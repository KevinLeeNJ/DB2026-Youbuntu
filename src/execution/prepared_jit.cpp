/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/prepared_jit.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "common/context.h"
#include "execution/execution_common.h"
#include "execution/parameter_frame.h"
#include "execution/prepared_plan_descriptor.h"
#include "execution/row_mutation.h"
#include "index/ix.h"
#include "system/sm.h"

#ifdef RMDB_ENABLE_JIT
#include "compiled/compiled_program.h"
#include "compiled/parameter_frame.h"
#include "compiled/program_runtime.h"
#include "compiled/program_verifier.h"
#include "jit/jit_types.h"
#endif

namespace {

void set_reason(std::string* reason, std::string message) {
    if (reason != nullptr) {
        *reason = std::move(message);
    }
}

#ifdef RMDB_ENABLE_JIT

using compiled::ExecStatus;
using compiled::Instruction;
using compiled::Opcode;
using compiled::ProgramKind;
using compiled::RuntimeValue;
using compiled::ValueType;

struct ParameterSource {
    std::size_t ordinal{0};
    ColType type{TYPE_INT};
    Value literal;
    ValueType native_type{ValueType::INT32};
};

struct LimitBinding {
    bool present{false};
    int limit{1};
    int offset{0};
    std::size_t limit_ordinal{0};
    std::size_t offset_ordinal{0};
};

struct PointSelectShape {
    const ProjectionPlan* projection{nullptr};
    const ScanPlan* scan{nullptr};
    std::vector<const Condition*> conditions;
    LimitBinding limit;
};

std::optional<ValueType> compiled_type(ColType type) {
    if (type == TYPE_INT) {
        return ValueType::INT32;
    }
    if (type == TYPE_STRING || type == TYPE_DATETIME) {
        return ValueType::BYTES;
    }
    return std::nullopt;
}

compiled::CompareOp compiled_op(CompOp op) {
    switch (op) {
    case OP_EQ:
        return compiled::CompareOp::EQ;
    case OP_NE:
        return compiled::CompareOp::NE;
    case OP_LT:
        return compiled::CompareOp::LT;
    case OP_GT:
        return compiled::CompareOp::GT;
    case OP_LE:
        return compiled::CompareOp::LE;
    case OP_GE:
        return compiled::CompareOp::GE;
    }
    throw InternalError("invalid comparison operator in prepared JIT");
}

bool inspect_point_select(const PreparedPlanDescriptor& descriptor, PointSelectShape* shape, std::string* reason) {
    if (descriptor.statement_kind() != PreparedStatementKind::Select || descriptor.dml_plan() == nullptr ||
        descriptor.dml_plan()->subplan_ == nullptr) {
        set_reason(reason, "statement is not a SELECT");
        return false;
    }

    const Plan* node = descriptor.dml_plan()->subplan_.get();
    while (node != nullptr) {
        switch (node->tag) {
        case T_Projection: {
            if (shape->projection != nullptr) {
                set_reason(reason, "multiple projections are not supported");
                return false;
            }
            shape->projection = static_cast<const ProjectionPlan*>(node);
            node = shape->projection->subplan_.get();
            break;
        }
        case T_Filter: {
            const auto* filter = static_cast<const FilterPlan*>(node);
            for (const auto& condition : filter->conds_) {
                shape->conditions.push_back(&condition);
            }
            node = filter->subplan_.get();
            break;
        }
        case T_Limit: {
            if (shape->limit.present) {
                set_reason(reason, "multiple LIMIT nodes are not supported");
                return false;
            }
            const auto* limit = static_cast<const LimitPlan*>(node);
            shape->limit = {true, limit->limit_, limit->offset_, limit->limit_parameter_ordinal_,
                            limit->offset_parameter_ordinal_};
            node = limit->subplan_.get();
            break;
        }
        case T_IndexScan:
            shape->scan = static_cast<const ScanPlan*>(node);
            node = nullptr;
            break;
        default:
            set_reason(reason, "SELECT is not a single index point lookup");
            return false;
        }
    }

    if (shape->projection == nullptr || shape->scan == nullptr || shape->scan->scan_backward_ ||
        shape->scan->index_col_names_.empty()) {
        set_reason(reason, "point SELECT requires projection over a forward index scan");
        return false;
    }
    for (const auto& condition : shape->scan->conds_) {
        shape->conditions.push_back(&condition);
    }
    if (shape->conditions.empty()) {
        set_reason(reason, "point SELECT has no predicates");
        return false;
    }
    return true;
}

const ColMeta* find_column(const std::vector<ColMeta>& columns, const TabCol& target) {
    auto position = std::find_if(columns.begin(), columns.end(), [&](const ColMeta& column) {
        return column.name == target.col_name && (target.tab_name.empty() || column.tab_name == target.tab_name);
    });
    return position == columns.end() ? nullptr : &*position;
}

std::size_t column_index(const std::vector<ColMeta>& columns, const ColMeta* column) {
    return static_cast<std::size_t>(column - columns.data());
}

std::size_t projected_tuple_size(const std::vector<ColMeta>& columns) {
    std::size_t size = 0;
    for (const auto& column : columns) {
        size = std::max(size, static_cast<std::size_t>(column.offset + column.len));
        if (column.null_byte >= 0) {
            size = std::max(size, static_cast<std::size_t>(column.null_byte + 1));
        }
    }
    return size;
}

std::vector<ColMeta> projection_sources(const PointSelectShape& shape, const PreparedPlanDescriptor& descriptor,
                                        std::string* reason) {
    std::vector<ColMeta> sources;
    const auto& columns = shape.scan->cols_;
    if (shape.projection->is_select_star_) {
        sources = columns;
    } else {
        sources.reserve(shape.projection->select_items_.size());
        for (const auto& item : shape.projection->select_items_) {
            if (item.expr.type != QueryExprType::COLUMN) {
                set_reason(reason, "computed projection is not supported");
                return {};
            }
            const ColMeta* source = find_column(columns, item.expr.col);
            if (source == nullptr) {
                set_reason(reason, "projection column is not in the point-scan tuple");
                return {};
            }
            sources.push_back(*source);
        }
    }
    if (sources.empty() || sources.size() != descriptor.result_schema().size()) {
        set_reason(reason, "projection shape does not match the prepared result schema");
        return {};
    }
    return sources;
}

struct ProgramBuild {
    std::shared_ptr<const compiled::CompiledProgram> program;
    PreparedStatementKind statement_kind{PreparedStatementKind::Unsupported};
    std::vector<ParameterSource> parameter_sources;
    std::string table_name;
    std::string index_name;
    std::vector<ColMeta> index_columns;
    std::vector<ColMeta> predicate_columns;
    std::vector<ColMeta> source_columns;
    std::vector<ColMeta> result_columns;
    std::vector<std::string> output_names;
    LimitBinding limit;
    TabMeta table_meta;
    std::vector<Condition> mutation_conditions;
    std::vector<SetClause> mutation_set_clauses;
    std::vector<BoundMutationCondition> bound_mutation_conditions;
    std::vector<BoundMutationSetClause> bound_mutation_set_clauses;
    std::vector<bool> affected_index_bitmap;
    std::vector<std::string> mutation_index_names;
    std::uint64_t catalog_generation{0};
    std::size_t output_size{0};
};

std::optional<ProgramBuild> build_insert_program(const PreparedPlanDescriptor& descriptor, SmManager* sm_manager,
                                                 std::string* reason) {
    const DMLPlan* dml = descriptor.dml_plan();
    if (dml == nullptr || dml->tag != T_Insert || dml->subplan_ != nullptr || dml->tab_name_.empty()) {
        set_reason(reason, "statement is not a direct INSERT");
        return std::nullopt;
    }
    auto& table = sm_manager->db_.get_table(dml->tab_name_);
    auto file = sm_manager->fhs_.find(dml->tab_name_);
    if (dml->values_.size() != table.cols.size() || file == sm_manager->fhs_.end()) {
        set_reason(reason, "INSERT values do not match current table metadata");
        return std::nullopt;
    }
    const int record_size = file->second->get_file_hdr().record_size;
    if (record_size <= 0 || static_cast<std::uint64_t>(record_size) > compiled::MAX_PROGRAM_VALUE_BYTES) {
        set_reason(reason, "INSERT tuple exceeds the JIT frame limit");
        return std::nullopt;
    }

    compiled::TupleLayout layout;
    layout.byte_size = static_cast<std::uint32_t>(record_size);
    layout.columns.reserve(table.cols.size());
    std::vector<compiled::ParameterDesc> parameter_descs;
    std::vector<ParameterSource> parameter_sources;
    std::vector<compiled::RegisterDesc> registers = {
        {ValueType::TUPLE, 0, 0},
        {ValueType::ROW_HANDLE, compiled::kNoOperand, 0},
    };
    std::vector<Instruction> instructions;
    parameter_descs.reserve(table.cols.size());
    parameter_sources.reserve(table.cols.size());
    for (std::size_t i = 0; i < table.cols.size(); ++i) {
        const ColMeta& column = table.cols[i];
        const Value& value = dml->values_[i];
        if (value.is_null ||
            (column.type != value.type && !((column.type == TYPE_STRING || column.type == TYPE_DATETIME) &&
                                            (value.type == TYPE_STRING || value.type == TYPE_DATETIME)))) {
            set_reason(reason, "INSERT JIT requires non-NULL values with exact column types");
            return std::nullopt;
        }
        const ValueType type = column.type == TYPE_INT ? ValueType::INT32 : ValueType::BYTES;
        const std::uint32_t width = type == ValueType::BYTES ? static_cast<std::uint32_t>(column.len) : 0;
        layout.columns.push_back(
            {type, static_cast<std::uint32_t>(column.offset), static_cast<std::uint32_t>(column.len)});
        parameter_descs.push_back({type, width, static_cast<std::int32_t>(value.parameter_ordinal)});
        parameter_sources.push_back({value.parameter_ordinal, value.type, value, type});
        const std::uint32_t value_register = static_cast<std::uint32_t>(registers.size());
        registers.push_back({type, compiled::kNoOperand, width});
        instructions.push_back({Opcode::LOAD_PARAM, value_register, compiled::kNoOperand, compiled::kNoOperand,
                                static_cast<std::uint32_t>(i)});
        instructions.push_back(
            {Opcode::STORE_COLUMN, 0, value_register, compiled::kNoOperand, static_cast<std::uint32_t>(i)});
    }
    instructions.push_back({Opcode::INSERT_ROW, 1, 0});
    instructions.push_back({Opcode::HALT});

    auto program = std::make_shared<const compiled::CompiledProgram>(
        compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, ProgramKind::POINT_INSERT,
        descriptor.catalog_generation(), std::move(parameter_descs), std::move(registers),
        std::vector<compiled::TupleLayout>{std::move(layout)}, std::move(instructions));
    const auto verification = compiled::VerifyProgram(*program);
    if (!verification) {
        set_reason(reason, verification.error);
        return std::nullopt;
    }
    ProgramBuild result;
    result.program = std::move(program);
    result.statement_kind = PreparedStatementKind::Insert;
    result.parameter_sources = std::move(parameter_sources);
    result.table_name = dml->tab_name_;
    result.table_meta = table;
    result.catalog_generation = descriptor.catalog_generation();
    return result;
}

std::optional<ProgramBuild> build_program(const PreparedPlanDescriptor& descriptor, SmManager* sm_manager,
                                          std::string* reason) {
    PointSelectShape shape;
    if (sm_manager == nullptr || !descriptor.eligible() || descriptor.dml_plan() == nullptr) {
        return std::nullopt;
    }
    if (descriptor.statement_kind() == PreparedStatementKind::Insert) {
        return build_insert_program(descriptor, sm_manager, reason);
    }

    const DMLPlan* dml = descriptor.dml_plan();
    const bool is_update = descriptor.statement_kind() == PreparedStatementKind::Update;
    if (is_update) {
        if (dml->tag != T_Update || dml->subplan_ == nullptr || dml->subplan_->tag != T_IndexScan ||
            dml->set_clauses_.empty()) {
            set_reason(reason, "UPDATE is not a single index point mutation");
            return std::nullopt;
        }
        shape.scan = static_cast<const ScanPlan*>(dml->subplan_.get());
        for (const auto& condition : dml->conds_) {
            shape.conditions.push_back(&condition);
        }
        if (shape.scan->scan_backward_ || shape.scan->index_col_names_.empty() || shape.conditions.empty()) {
            set_reason(reason, "point UPDATE requires a forward complete-key index scan");
            return std::nullopt;
        }
    } else if (!inspect_point_select(descriptor, &shape, reason)) {
        return std::nullopt;
    }

    auto& table = sm_manager->db_.get_table(shape.scan->tab_name_);
    auto index = table.get_index_meta(shape.scan->index_col_names_);
    if (index == table.indexes.end() || index->cols.size() != shape.scan->index_col_names_.size()) {
        set_reason(reason, "prepared index metadata is unavailable");
        return std::nullopt;
    }

    std::vector<std::size_t> key_conditions;
    key_conditions.reserve(index->cols.size());
    std::vector<bool> used(shape.conditions.size(), false);
    for (const auto& index_column : index->cols) {
        std::size_t match = shape.conditions.size();
        for (std::size_t i = 0; i < shape.conditions.size(); ++i) {
            const Condition& condition = *shape.conditions[i];
            if (!used[i] && condition.is_rhs_val && condition.op == OP_EQ &&
                condition.lhs_col.col_name == index_column.name &&
                (condition.lhs_col.tab_name.empty() || condition.lhs_col.tab_name == shape.scan->tab_name_)) {
                match = i;
                break;
            }
        }
        if (match == shape.conditions.size()) {
            set_reason(reason, "index equality key is incomplete");
            return std::nullopt;
        }
        used[match] = true;
        key_conditions.push_back(match);
    }

    std::vector<compiled::ParameterDesc> parameter_descs;
    std::vector<ParameterSource> parameter_sources;
    std::vector<ColMeta> predicate_columns;
    parameter_descs.reserve(shape.conditions.size());
    parameter_sources.reserve(shape.conditions.size());
    predicate_columns.reserve(shape.conditions.size());
    for (const Condition* condition : shape.conditions) {
        if (!condition->is_rhs_val || condition->rhs_val.is_null) {
            set_reason(reason, "column predicates and NULL literals are not supported");
            return std::nullopt;
        }
        const ColMeta* column = find_column(shape.scan->cols_, condition->lhs_col);
        const auto type = column == nullptr ? std::optional<ValueType>{} : compiled_type(column->type);
        const auto rhs_type = compiled_type(condition->rhs_val.type);
        if (column == nullptr || !type.has_value() || !rhs_type.has_value() || *type != *rhs_type) {
            set_reason(reason, "predicate requires unsupported FLOAT or mixed-type evaluation");
            return std::nullopt;
        }
        const std::uint32_t max_length = *type == ValueType::BYTES ? static_cast<std::uint32_t>(column->len) : 0;
        parameter_descs.push_back({*type, max_length, static_cast<std::int32_t>(condition->rhs_val.parameter_ordinal)});
        parameter_sources.push_back(
            {condition->rhs_val.parameter_ordinal, condition->rhs_val.type, condition->rhs_val, *type});
        predicate_columns.push_back(*column);
    }

    std::vector<ColMeta> sources;
    if (!is_update) {
        sources = projection_sources(shape, descriptor, reason);
        if (sources.empty()) {
            return std::nullopt;
        }
    }
    auto fh_position = sm_manager->fhs_.find(shape.scan->tab_name_);
    if (fh_position == sm_manager->fhs_.end()) {
        set_reason(reason, "prepared table handle is unavailable");
        return std::nullopt;
    }
    const int record_size = fh_position->second->get_file_hdr().record_size;
    if (record_size <= 0 || static_cast<std::uint64_t>(record_size) > compiled::MAX_PROGRAM_VALUE_BYTES) {
        set_reason(reason, "point tuple exceeds the JIT frame limit");
        return std::nullopt;
    }

    compiled::TupleLayout layout;
    layout.byte_size = static_cast<std::uint32_t>(record_size);
    layout.columns.reserve(shape.scan->cols_.size());
    for (const auto& column : shape.scan->cols_) {
        // FLOAT remains opaque bytes in this phase. No old FLOAT64 instruction
        // is allowed to read or modify the current binary32 storage format.
        const ValueType type = column.type == TYPE_INT ? ValueType::INT32 : ValueType::BYTES;
        layout.columns.push_back(
            {type, static_cast<std::uint32_t>(column.offset), static_cast<std::uint32_t>(column.len)});
    }

    std::vector<compiled::RegisterDesc> registers = {
        {ValueType::TUPLE, 0, 0},
        {ValueType::POINT_KEY, compiled::kNoOperand, 0},
        {ValueType::ROW_HANDLE, compiled::kNoOperand, 0},
        {ValueType::TUPLE, 0, 0},
    };
    if (is_update) {
        registers.push_back({ValueType::PREPARED_UPDATE, compiled::kNoOperand, 0});
    }
    std::vector<Instruction> instructions;
    for (std::size_t condition_index : key_conditions) {
        const ColMeta* column = find_column(shape.scan->cols_, shape.conditions[condition_index]->lhs_col);
        const ValueType type = *compiled_type(column->type);
        const std::uint32_t value_register = static_cast<std::uint32_t>(registers.size());
        registers.push_back(
            {type, compiled::kNoOperand, type == ValueType::BYTES ? static_cast<std::uint32_t>(column->len) : 0});
        instructions.push_back({Opcode::LOAD_PARAM, value_register, compiled::kNoOperand, compiled::kNoOperand,
                                static_cast<std::uint32_t>(condition_index)});
        instructions.push_back({Opcode::STORE_COLUMN, 0, value_register, compiled::kNoOperand,
                                static_cast<std::uint32_t>(column_index(shape.scan->cols_, column))});
    }
    instructions.push_back({Opcode::MAKE_POINT_KEY, 1, 0, compiled::kNoOperand, 0});
    instructions.push_back({Opcode::POINT_LOOKUP, 3, 1, 2});

    std::vector<std::size_t> no_match_jumps;
    for (std::size_t i = 0; i < shape.conditions.size(); ++i) {
        const ColMeta* column = find_column(shape.scan->cols_, shape.conditions[i]->lhs_col);
        const ValueType type = *compiled_type(column->type);
        const std::uint32_t lhs = static_cast<std::uint32_t>(registers.size());
        registers.push_back(
            {type, compiled::kNoOperand, type == ValueType::BYTES ? static_cast<std::uint32_t>(column->len) : 0});
        const std::uint32_t rhs = static_cast<std::uint32_t>(registers.size());
        registers.push_back(
            {type, compiled::kNoOperand, type == ValueType::BYTES ? static_cast<std::uint32_t>(column->len) : 0});
        const std::uint32_t result = static_cast<std::uint32_t>(registers.size());
        registers.push_back({ValueType::BOOL, compiled::kNoOperand, 0});
        instructions.push_back({Opcode::LOAD_COLUMN, lhs, 3, compiled::kNoOperand,
                                static_cast<std::uint32_t>(column_index(shape.scan->cols_, column))});
        instructions.push_back(
            {Opcode::LOAD_PARAM, rhs, compiled::kNoOperand, compiled::kNoOperand, static_cast<std::uint32_t>(i)});
        instructions.push_back(
            {Opcode::COMPARE, result, lhs, rhs, compiled::kNoOperand, compiled_op(shape.conditions[i]->op)});
        no_match_jumps.push_back(instructions.size());
        instructions.push_back(
            {Opcode::JUMP_IF_FALSE, compiled::kNoOperand, result, compiled::kNoOperand, compiled::kNoOperand});
    }
    if (is_update) {
        instructions.push_back({Opcode::PREPARE_UPDATE, 4, 2, 3});
        instructions.push_back({Opcode::COMMIT_UPDATE, compiled::kNoOperand, 4, 3});
    } else {
        instructions.push_back({Opcode::EMIT_ROW, compiled::kNoOperand, 3});
    }
    const std::uint32_t halt = static_cast<std::uint32_t>(instructions.size());
    instructions.push_back({Opcode::HALT});
    for (std::size_t jump : no_match_jumps) {
        instructions[jump].aux = halt;
    }

    auto program = std::make_shared<const compiled::CompiledProgram>(
        compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION,
        is_update ? ProgramKind::POINT_UPDATE : ProgramKind::POINT_SELECT, descriptor.catalog_generation(),
        std::move(parameter_descs), std::move(registers), std::vector<compiled::TupleLayout>{std::move(layout)},
        std::move(instructions));
    const auto verification = compiled::VerifyProgram(*program);
    if (!verification) {
        set_reason(reason, verification.error);
        return std::nullopt;
    }

    ProgramBuild result;
    result.program = std::move(program);
    result.statement_kind = descriptor.statement_kind();
    result.parameter_sources = std::move(parameter_sources);
    result.table_name = shape.scan->tab_name_;
    result.index_name = sm_manager->get_ix_manager()->get_index_name(result.table_name, index->cols);
    result.index_columns = index->cols;
    result.predicate_columns = std::move(predicate_columns);
    result.source_columns = sources;
    result.result_columns = descriptor.result_schema();
    result.output_names = descriptor.output_names();
    result.limit = shape.limit;
    if (is_update) {
        result.table_meta = table;
        result.mutation_conditions = dml->conds_;
        result.mutation_set_clauses = dml->set_clauses_;
        result.bound_mutation_conditions = BindMutationConditions(result.table_meta, result.mutation_conditions);
        result.bound_mutation_set_clauses = BindMutationSetClauses(result.table_meta, result.mutation_set_clauses);
        result.affected_index_bitmap.assign(result.table_meta.indexes.size(), false);
        result.mutation_index_names.reserve(result.table_meta.indexes.size());
        for (std::size_t index_index = 0; index_index < result.table_meta.indexes.size(); ++index_index) {
            const auto& mutation_index = result.table_meta.indexes[index_index];
            result.mutation_index_names.push_back(
                sm_manager->get_ix_manager()->get_index_name(result.table_name, mutation_index.cols));
            for (const auto& set_clause : result.mutation_set_clauses) {
                if (std::any_of(mutation_index.cols.begin(), mutation_index.cols.end(),
                                [&](const ColMeta& column) { return column.name == set_clause.lhs.col_name; })) {
                    result.affected_index_bitmap[index_index] = true;
                    break;
                }
            }
        }
    }
    result.catalog_generation = descriptor.catalog_generation();
    result.output_size = projected_tuple_size(result.result_columns);
    return result;
}

bool same_rid(const std::vector<Rid>& candidates, const Rid& rid) {
    return std::find(candidates.begin(), candidates.end(), rid) != candidates.end();
}

#endif

} // namespace

#ifdef RMDB_ENABLE_JIT

struct PreparedJitProgram::Impl {
    std::shared_ptr<const compiled::CompiledProgram> program;
    jit::JitCode code;
    PreparedStatementKind statement_kind{PreparedStatementKind::Unsupported};
    std::vector<ParameterSource> parameter_sources;
    std::string table_name;
    std::string index_name;
    std::vector<ColMeta> index_columns;
    std::vector<ColMeta> predicate_columns;
    std::vector<ColMeta> source_columns;
    std::vector<ColMeta> result_columns;
    std::vector<std::string> output_names;
    LimitBinding limit;
    TabMeta table_meta;
    std::vector<Condition> mutation_conditions;
    std::vector<SetClause> mutation_set_clauses;
    std::vector<BoundMutationCondition> bound_mutation_conditions;
    std::vector<BoundMutationSetClause> bound_mutation_set_clauses;
    std::vector<bool> affected_index_bitmap;
    std::vector<std::string> mutation_index_names;
    std::uint64_t catalog_generation{0};
    std::size_t output_size{0};
};

namespace {

class PointRuntime final : public compiled::ProgramRuntime {
public:
    PointRuntime(const PreparedJitProgram::Impl& program, SmManager* sm_manager, Context* context,
                 const std::vector<Condition>* mutation_conditions, const std::vector<SetClause>* mutation_set_clauses)
        : program_(program), sm_manager_(sm_manager), context_(context), mutation_conditions_(mutation_conditions),
          mutation_set_clauses_(mutation_set_clauses) {}

    ExecStatus MakePointKey(std::uint32_t index_id, const RuntimeValue& value, RuntimeValue* key) noexcept override {
        if (index_id != 0 || key == nullptr || value.type != ValueType::TUPLE || !value.initialized ||
            value.tuple.size() != program_.program->tuple_layouts()[0].byte_size) {
            return ExecStatus::FALLBACK;
        }
        try {
            key->type = ValueType::POINT_KEY;
            key->bytes.clear();
            for (const auto& column : program_.index_columns) {
                if (column.offset < 0 || column.len <= 0 ||
                    static_cast<std::size_t>(column.offset + column.len) > value.tuple.size()) {
                    return ExecStatus::FALLBACK;
                }
                key->bytes.append(reinterpret_cast<const char*>(value.tuple.data() + column.offset), column.len);
            }
            key->opaque = 0;
            key->initialized = true;
            return ExecStatus::OK;
        } catch (...) {
            pending_exception_ = std::current_exception();
            return ExecStatus::ERROR;
        }
    }

    ExecStatus PointLookup(const RuntimeValue& key, RuntimeValue* row, RuntimeValue* tuple) noexcept override {
        if (sm_manager_ == nullptr || row == nullptr || tuple == nullptr || key.type != ValueType::POINT_KEY ||
            !key.initialized || sm_manager_->get_catalog_generation() != program_.catalog_generation) {
            return ExecStatus::FALLBACK;
        }
        try {
            auto table = sm_manager_->fhs_.find(program_.table_name);
            auto index = sm_manager_->ihs_.find(program_.index_name);
            if (table == sm_manager_->fhs_.end() || index == sm_manager_->ihs_.end()) {
                return ExecStatus::FALLBACK;
            }
            const auto lookup = index->second->lookup_unique(key.bytes.data());
            if (lookup.status == UniqueLookupStatus::Duplicate) {
                return ExecStatus::FALLBACK;
            }

            std::vector<Rid> candidates;
            if (lookup.status == UniqueLookupStatus::Unique) {
                candidates.push_back(lookup.rid);
            }
            const std::vector<char> owned_key(key.bytes.begin(), key.bytes.end());
            for (const Rid& candidate :
                 sm_manager_->get_historical_index_key_rids(program_.table_name, program_.index_name, owned_key)) {
                if (!same_rid(candidates, candidate)) {
                    candidates.push_back(candidate);
                }
            }

            std::unique_ptr<RmRecord> visible;
            Rid visible_rid{};
            for (const Rid& candidate : candidates) {
                auto record = GetVisibleRecord(table->second.get(), candidate, context_);
                if (record == nullptr || !record_matches_key(*record, key.bytes)) {
                    continue;
                }
                if (visible != nullptr) {
                    return ExecStatus::FALLBACK;
                }
                visible = std::move(record);
                visible_rid = candidate;
            }
            if (visible == nullptr || predicate_is_null(*visible)) {
                return ExecStatus::NO_MATCH_RESULT;
            }

            row->type = ValueType::ROW_HANDLE;
            row->opaque = (static_cast<std::uint64_t>(static_cast<std::uint32_t>(visible_rid.page_no)) << 32U) |
                          static_cast<std::uint32_t>(visible_rid.slot_no);
            row->initialized = true;
            tuple->type = ValueType::TUPLE;
            tuple->tuple.assign(reinterpret_cast<const std::uint8_t*>(visible->data),
                                reinterpret_cast<const std::uint8_t*>(visible->data + visible->size));
            tuple->initialized = true;
            return ExecStatus::OK;
        } catch (const TransactionAbortException&) {
            pending_exception_ = std::current_exception();
            return ExecStatus::TXN_ABORT;
        } catch (...) {
            pending_exception_ = std::current_exception();
            return ExecStatus::ERROR;
        }
    }

    ExecStatus EmitRow(const RuntimeValue& tuple) noexcept override {
        if (tuple.type != ValueType::TUPLE || !tuple.initialized || emitted_row_.has_value()) {
            return ExecStatus::ERROR;
        }
        try {
            std::vector<char> output(program_.output_size, 0);
            for (std::size_t i = 0; i < program_.source_columns.size(); ++i) {
                const ColMeta& source = program_.source_columns[i];
                const ColMeta& target = program_.result_columns[i];
                if (source.len != target.len || source.offset < 0 || target.offset < 0 ||
                    static_cast<std::size_t>(source.offset + source.len) > tuple.tuple.size() ||
                    static_cast<std::size_t>(target.offset + target.len) > output.size()) {
                    return ExecStatus::FALLBACK;
                }
                std::memcpy(output.data() + target.offset, tuple.tuple.data() + source.offset, source.len);
                if (is_null(reinterpret_cast<const char*>(tuple.tuple.data()), source)) {
                    set_null(output.data(), target);
                }
            }
            emitted_row_ = std::move(output);
            return ExecStatus::OK;
        } catch (...) {
            pending_exception_ = std::current_exception();
            return ExecStatus::ERROR;
        }
    }

    ExecStatus PrepareUpdate(const RuntimeValue& row, RuntimeValue* current_tuple,
                             RuntimeValue* prepared) noexcept override {
        if (program_.statement_kind != PreparedStatementKind::Update || current_tuple == nullptr ||
            prepared == nullptr || row.type != ValueType::ROW_HANDLE || !row.initialized ||
            current_tuple->type != ValueType::TUPLE ||
            current_tuple->tuple.size() != program_.program->tuple_layouts()[0].byte_size) {
            return ExecStatus::FALLBACK;
        }
        prepared->type = ValueType::PREPARED_UPDATE;
        prepared->opaque = row.opaque;
        prepared->initialized = true;
        current_tuple->initialized = true;
        return ExecStatus::OK;
    }

    ExecStatus CommitUpdate(const RuntimeValue& prepared, const RuntimeValue& proposed_tuple) noexcept override {
        if (program_.statement_kind != PreparedStatementKind::Update || sm_manager_ == nullptr || context_ == nullptr ||
            mutation_conditions_ == nullptr || mutation_set_clauses_ == nullptr ||
            prepared.type != ValueType::PREPARED_UPDATE || !prepared.initialized ||
            proposed_tuple.type != ValueType::TUPLE || !proposed_tuple.initialized ||
            proposed_tuple.tuple.size() != program_.program->tuple_layouts()[0].byte_size) {
            return ExecStatus::FALLBACK;
        }
        try {
            auto table = sm_manager_->fhs_.find(program_.table_name);
            if (table == sm_manager_->fhs_.end() ||
                program_.mutation_index_names.size() != program_.table_meta.indexes.size()) {
                return ExecStatus::FALLBACK;
            }
            std::vector<RowMutationIndex> indexes;
            indexes.reserve(program_.table_meta.indexes.size());
            for (std::size_t i = 0; i < program_.table_meta.indexes.size(); ++i) {
                auto handle = sm_manager_->ihs_.find(program_.mutation_index_names[i]);
                if (handle == sm_manager_->ihs_.end()) {
                    return ExecStatus::FALLBACK;
                }
                indexes.push_back(
                    {&program_.table_meta.indexes[i], handle->second.get(), program_.mutation_index_names[i]});
            }
            const Rid rid{static_cast<int>(static_cast<std::uint32_t>(prepared.opaque >> 32U)),
                          static_cast<int>(static_cast<std::uint32_t>(prepared.opaque))};
            RmRecord visible(static_cast<int>(proposed_tuple.tuple.size()));
            std::memcpy(visible.data, proposed_tuple.tuple.data(), proposed_tuple.tuple.size());
            UpdateRuntimeInfo info{sm_manager_,
                                   &program_.table_name,
                                   &program_.table_meta,
                                   table->second.get(),
                                   mutation_conditions_,
                                   &program_.bound_mutation_conditions,
                                   &indexes,
                                   mutation_set_clauses_,
                                   &program_.bound_mutation_set_clauses,
                                   &program_.affected_index_bitmap};
            (void)RowMutationEngine::UpdateOne(rid, visible, info, context_);
            return ExecStatus::OK;
        } catch (const TransactionAbortException&) {
            pending_exception_ = std::current_exception();
            return ExecStatus::TXN_ABORT;
        } catch (...) {
            pending_exception_ = std::current_exception();
            return ExecStatus::ERROR;
        }
    }

    ExecStatus InsertRow(const RuntimeValue& tuple, RuntimeValue* row) noexcept override {
        if (program_.statement_kind != PreparedStatementKind::Insert || sm_manager_ == nullptr || context_ == nullptr ||
            row == nullptr || tuple.type != ValueType::TUPLE || !tuple.initialized ||
            tuple.tuple.size() != program_.program->tuple_layouts()[0].byte_size) {
            return ExecStatus::FALLBACK;
        }
        try {
            auto file = sm_manager_->fhs_.find(program_.table_name);
            if (file == sm_manager_->fhs_.end()) {
                return ExecStatus::FALLBACK;
            }
            RmRecord record(static_cast<int>(tuple.tuple.size()));
            std::memcpy(record.data, tuple.tuple.data(), tuple.tuple.size());
            const Rid rid = RowInsertEngine::InsertOne(sm_manager_, program_.table_name, program_.table_meta,
                                                       file->second.get(), record, context_);
            row->type = ValueType::ROW_HANDLE;
            row->opaque = (static_cast<std::uint64_t>(static_cast<std::uint32_t>(rid.page_no)) << 32U) |
                          static_cast<std::uint32_t>(rid.slot_no);
            row->initialized = true;
            return ExecStatus::OK;
        } catch (const TransactionAbortException&) {
            pending_exception_ = std::current_exception();
            return ExecStatus::TXN_ABORT;
        } catch (...) {
            pending_exception_ = std::current_exception();
            return ExecStatus::ERROR;
        }
    }

    const std::optional<std::vector<char>>& emitted_row() const noexcept {
        return emitted_row_;
    }

    void Rethrow() const {
        if (pending_exception_ != nullptr) {
            std::rethrow_exception(pending_exception_);
        }
        throw RMDBError(error_message().empty() ? "prepared JIT execution failed" : error_message());
    }

private:
    bool record_matches_key(const RmRecord& record, const std::string& key) const {
        std::size_t key_offset = 0;
        for (const auto& column : program_.index_columns) {
            if (is_null(record.data, column) || column.offset < 0 || column.len <= 0 ||
                static_cast<std::size_t>(column.offset + column.len) > static_cast<std::size_t>(record.size) ||
                key_offset + static_cast<std::size_t>(column.len) > key.size() ||
                std::memcmp(record.data + column.offset, key.data() + key_offset, column.len) != 0) {
                return false;
            }
            key_offset += static_cast<std::size_t>(column.len);
        }
        return key_offset == key.size();
    }

    bool predicate_is_null(const RmRecord& record) const {
        return std::any_of(program_.predicate_columns.begin(), program_.predicate_columns.end(),
                           [&](const ColMeta& column) { return is_null(record.data, column); });
    }

    const PreparedJitProgram::Impl& program_;
    SmManager* sm_manager_;
    Context* context_;
    const std::vector<Condition>* mutation_conditions_;
    const std::vector<SetClause>* mutation_set_clauses_;
    std::optional<std::vector<char>> emitted_row_;
    std::exception_ptr pending_exception_;
};

std::optional<compiled::ParameterValue> bind_parameter(const ParameterSource& source,
                                                       const ParameterFrame& parameters) {
    Value value = source.ordinal == 0 ? source.literal : parameters.bind(source.ordinal, source.type);
    if (value.is_null) {
        return std::nullopt;
    }
    if (value.type == TYPE_INT) {
        return compiled::ParameterValue::Int(value.int_val);
    }
    if (value.type == TYPE_FLOAT && source.native_type == ValueType::BYTES) {
        std::string bytes(sizeof(float), '\0');
        std::memcpy(bytes.data(), &value.float_val, sizeof(value.float_val));
        return compiled::ParameterValue::Bytes(std::move(bytes));
    }
    if (value.type == TYPE_STRING || value.type == TYPE_DATETIME) {
        return compiled::ParameterValue::Bytes(value.str_val);
    }
    return std::nullopt;
}

std::vector<Condition> bind_mutation_conditions(const std::vector<Condition>& templates,
                                                const ParameterFrame& parameters) {
    std::vector<Condition> conditions = templates;
    for (auto& condition : conditions) {
        if (condition.is_rhs_val && condition.rhs_val.parameter_ordinal != 0) {
            condition.rhs_val = parameters.bind(condition.rhs_val.parameter_ordinal, condition.rhs_val.type);
        }
    }
    return conditions;
}

std::vector<SetClause> bind_mutation_set_clauses(const PreparedJitProgram::Impl& program,
                                                 const ParameterFrame& parameters) {
    std::vector<SetClause> clauses = program.mutation_set_clauses;
    for (auto& clause : clauses) {
        if (clause.rhs.parameter_ordinal != 0) {
            clause.rhs = parameters.bind(clause.rhs.parameter_ordinal, clause.rhs.type);
        }
        for (auto& term : clause.additional_terms) {
            if (term.rhs.parameter_ordinal != 0) {
                term.rhs = parameters.bind(term.rhs.parameter_ordinal, term.rhs.type);
            }
        }
        auto column = std::find_if(program.table_meta.cols.begin(), program.table_meta.cols.end(),
                                   [&](const ColMeta& candidate) { return candidate.name == clause.lhs.col_name; });
        if (column == program.table_meta.cols.end()) {
            throw InternalError("prepared JIT UPDATE column metadata is unavailable");
        }
        const auto validate = [&](const Value& value) {
            if (!value.is_null && (column->type == TYPE_STRING || column->type == TYPE_DATETIME) &&
                value.str_val.size() > static_cast<std::size_t>(column->len)) {
                throw StringOverflowError();
            }
            if (!value.is_null && value.type == TYPE_FLOAT && !std::isfinite(value.float_val)) {
                throw RMDBError("prepared FLOAT parameter must be finite");
            }
        };
        validate(clause.rhs);
        for (const auto& term : clause.additional_terms) {
            validate(term.rhs);
        }
    }
    return clauses;
}

bool should_emit_for_limit(const LimitBinding& binding, const ParameterFrame& parameters) {
    if (!binding.present) {
        return true;
    }
    int limit = binding.limit;
    int offset = binding.offset;
    if (binding.limit_ordinal != 0) {
        const Value value = parameters.bind(binding.limit_ordinal, TYPE_INT);
        if (value.is_null || value.int_val < 0) {
            throw RMDBError("LIMIT must be a non-NULL, non-negative INT32");
        }
        limit = value.int_val;
    }
    if (binding.offset_ordinal != 0) {
        const Value value = parameters.bind(binding.offset_ordinal, TYPE_INT);
        if (value.is_null || value.int_val < 0) {
            throw RMDBError("OFFSET must be a non-NULL, non-negative INT32");
        }
        offset = value.int_val;
    }
    if (limit < 0 || offset < 0 || limit > std::numeric_limits<int>::max() - offset) {
        throw RMDBError("LIMIT plus OFFSET exceeds INT32 range");
    }
    return limit != 0 && offset == 0;
}

} // namespace

struct PreparedJitCompiler::Impl {
    mutable std::mutex mutex;
    jit::JitRuntime runtime;
    PreparedJitCompilerStats stats;
};

#else

struct PreparedJitProgram::Impl {};
struct PreparedJitCompiler::Impl {
    PreparedJitCompilerStats stats;
};

#endif

PreparedJitProgram::PreparedJitProgram(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
PreparedJitProgram::~PreparedJitProgram() = default;

PreparedJitExecutionStatus PreparedJitProgram::Execute(const ParameterFrame& parameters, SmManager* sm_manager,
                                                       Context* context, QueryResultSink* result_sink) const {
#ifdef RMDB_ENABLE_JIT
    if (impl_ == nullptr || sm_manager == nullptr || context == nullptr || result_sink == nullptr ||
        sm_manager->get_catalog_generation() != impl_->catalog_generation ||
        (context->txn_ != nullptr && context->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE)) {
        return PreparedJitExecutionStatus::Fallback;
    }
    std::vector<compiled::ParameterValue> values;
    values.reserve(impl_->parameter_sources.size());
    for (const auto& source : impl_->parameter_sources) {
        auto value = bind_parameter(source, parameters);
        if (!value.has_value()) {
            return PreparedJitExecutionStatus::Fallback;
        }
        values.push_back(std::move(*value));
    }
    std::string error;
    auto frame = compiled::ParameterFrame::Bind(impl_->program->parameters(), std::move(values), &error);
    if (!frame.has_value()) {
        return PreparedJitExecutionStatus::Fallback;
    }

    if (impl_->statement_kind == PreparedStatementKind::Select && !should_emit_for_limit(impl_->limit, parameters)) {
        result_sink->begin_query(impl_->result_columns, impl_->output_names);
        return PreparedJitExecutionStatus::Handled;
    }

    std::vector<Condition> mutation_conditions;
    std::vector<SetClause> mutation_set_clauses;
    if (impl_->statement_kind == PreparedStatementKind::Update) {
        mutation_conditions = bind_mutation_conditions(impl_->mutation_conditions, parameters);
        mutation_set_clauses = bind_mutation_set_clauses(*impl_, parameters);
    }
    PointRuntime runtime(*impl_, sm_manager, context,
                         impl_->statement_kind == PreparedStatementKind::Update ? &mutation_conditions : nullptr,
                         impl_->statement_kind == PreparedStatementKind::Update ? &mutation_set_clauses : nullptr);
    const ExecStatus status = impl_->code.invoke_program(&runtime, &*frame);
    if (status == ExecStatus::FALLBACK) {
        return PreparedJitExecutionStatus::Fallback;
    }
    if (status == ExecStatus::TXN_ABORT || status == ExecStatus::ERROR) {
        runtime.Rethrow();
    }
    if (impl_->statement_kind == PreparedStatementKind::Select) {
        result_sink->begin_query(impl_->result_columns, impl_->output_names);
        if (status == ExecStatus::OK && runtime.emitted_row().has_value()) {
            const auto& row = *runtime.emitted_row();
            result_sink->append_row(impl_->result_columns, row.data(), row.size());
        }
    }
    return PreparedJitExecutionStatus::Handled;
#else
    (void)parameters;
    (void)sm_manager;
    (void)context;
    (void)result_sink;
    return PreparedJitExecutionStatus::Fallback;
#endif
}

bool PreparedJitProgram::is_query() const noexcept {
#ifdef RMDB_ENABLE_JIT
    return impl_ != nullptr && impl_->statement_kind == PreparedStatementKind::Select;
#else
    return false;
#endif
}

std::size_t PreparedJitProgram::code_size() const noexcept {
#ifdef RMDB_ENABLE_JIT
    return impl_ == nullptr ? 0 : impl_->code.code_size();
#else
    return 0;
#endif
}

PreparedJitCompiler::PreparedJitCompiler() : impl_(std::make_unique<Impl>()) {}
PreparedJitCompiler::~PreparedJitCompiler() = default;

std::unique_ptr<PreparedJitProgram> PreparedJitCompiler::Compile(const PreparedPlanDescriptor& descriptor,
                                                                 SmManager* sm_manager, std::string* reason) {
#ifdef RMDB_ENABLE_JIT
    auto build = build_program(descriptor, sm_manager, reason);
    std::lock_guard<std::mutex> guard(impl_->mutex);
    ++impl_->stats.attempts;
    if (!build.has_value()) {
        ++impl_->stats.unsupported;
        return nullptr;
    }
    auto compiled = impl_->runtime.compile_program(*build->program);
    if (!compiled) {
        ++impl_->stats.failures;
        set_reason(reason, compiled.error);
        return nullptr;
    }
    auto program = std::make_unique<PreparedJitProgram::Impl>();
    program->program = std::move(build->program);
    program->code = std::move(compiled.code);
    program->statement_kind = build->statement_kind;
    program->parameter_sources = std::move(build->parameter_sources);
    program->table_name = std::move(build->table_name);
    program->index_name = std::move(build->index_name);
    program->index_columns = std::move(build->index_columns);
    program->predicate_columns = std::move(build->predicate_columns);
    program->source_columns = std::move(build->source_columns);
    program->result_columns = std::move(build->result_columns);
    program->output_names = std::move(build->output_names);
    program->limit = build->limit;
    program->table_meta = std::move(build->table_meta);
    program->mutation_conditions = std::move(build->mutation_conditions);
    program->mutation_set_clauses = std::move(build->mutation_set_clauses);
    program->bound_mutation_conditions = std::move(build->bound_mutation_conditions);
    program->bound_mutation_set_clauses = std::move(build->bound_mutation_set_clauses);
    program->affected_index_bitmap = std::move(build->affected_index_bitmap);
    program->mutation_index_names = std::move(build->mutation_index_names);
    program->catalog_generation = build->catalog_generation;
    program->output_size = build->output_size;
    ++impl_->stats.compiled;
    impl_->stats.code_bytes += program->code.code_size();
    if (reason != nullptr) {
        reason->clear();
    }
    return std::unique_ptr<PreparedJitProgram>(new PreparedJitProgram(std::move(program)));
#else
    (void)descriptor;
    (void)sm_manager;
    set_reason(reason, "JIT support is disabled at build time");
    ++impl_->stats.unsupported;
    return nullptr;
#endif
}

PreparedJitCompilerStats PreparedJitCompiler::stats() const {
#ifdef RMDB_ENABLE_JIT
    std::lock_guard<std::mutex> guard(impl_->mutex);
#endif
    return impl_->stats;
}

PreparedJitCompiler& prepared_jit_compiler() {
    static PreparedJitCompiler compiler;
    return compiler;
}
