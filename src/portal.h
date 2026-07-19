/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#ifdef private
#pragma push_macro("private")
#undef private
#define RMDB_PORTAL_RESTORE_PRIVATE
#endif

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#ifdef RMDB_PORTAL_RESTORE_PRIVATE
#pragma pop_macro("private")
#undef RMDB_PORTAL_RESTORE_PRIVATE
#endif
#include "execution/executor_abstract.h"
#include "execution/executor_aggregate.h"
#include "execution/executor_delete.h"
#include "execution/executor_filter.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_index_skip_scan.h"
#include "execution/prepared_select_descriptor.h"
#include "execution/executor_insert.h"
#include "execution/runtime/point_lookup_runtime.h"
#include "execution/executor_limit.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_union.h"
#include "execution/executor_update.h"
#include "execution/execution_sort.h"
#include "execution/runtime/database_program_runtime.h"
#include "compiled/bytecode_interpreter.h"
#include "compiled/parameter_frame.h"
#include "compiled/program_cache.h"
#include "common/common.h"
#include "optimizer/plan.h"

typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_EXPLAIN_ANALYZE,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;

struct PortalStmt {
    portalTag tag;

    std::vector<std::string> output_names;
    std::unique_ptr<AbstractExecutor> root;
    std::unique_ptr<Plan> plan;
    BoundPlan bound_plan;
    struct CompiledSelectSpec {
        std::shared_ptr<const compiled::CompiledProgram> program;
        std::vector<compiled::ParameterValue> parameters;
        DatabaseProgramBindings bindings;
        std::vector<ColMeta> output_cols;
        std::vector<std::string> output_captions;
        compiled::ProgramBindingTemplate template_bindings;
        std::vector<compiled::LexicalParameterDesc> template_parameters;
        compiled::ProgramTemplatePtr compiled_template;
    };
    std::shared_ptr<CompiledSelectSpec> compiled_select;
    struct CompiledMutationSpec {
        std::shared_ptr<const compiled::CompiledProgram> program;
        std::vector<compiled::ParameterValue> parameters;
        DatabaseProgramBindings bindings;
        std::string table_name;
        std::vector<Condition> conditions;
        std::vector<SetClause> set_clauses;
        std::vector<BoundMutationCondition> bound_conditions;
        std::vector<BoundMutationSetClause> bound_set_clauses;
        std::vector<RowMutationIndex> indexes;
        std::vector<bool> affected_index_bitmap;
        UpdateRuntimeInfo update_info{};
        DeleteRuntimeInfo delete_info{};
        InsertRuntimeInfo insert_info{};
        compiled::ProgramBindingTemplate template_bindings;
        std::vector<compiled::LexicalParameterDesc> template_parameters;
        compiled::ProgramTemplatePtr compiled_template;
    };
    std::shared_ptr<CompiledMutationSpec> compiled_mutation;
    compiled::ProgramTemplatePtr compiled_template;

    const Plan* execution_plan() const {
        return bound_plan ? bound_plan.get() : plan.get();
    }

    Plan* mutable_plan() const {
        return plan.get();
    }

    PortalStmt(portalTag tag_, std::vector<std::string> output_names_, std::unique_ptr<AbstractExecutor> root_,
               std::unique_ptr<Plan> plan_)
        : tag(tag_), output_names(std::move(output_names_)), root(std::move(root_)), plan(std::move(plan_)) {}

    PortalStmt(portalTag tag_, std::vector<std::string> output_names_, std::unique_ptr<AbstractExecutor> root_,
               BoundPlan plan_)
        : tag(tag_), output_names(std::move(output_names_)), root(std::move(root_)), bound_plan(std::move(plan_)) {}
};

class Portal {
private:
    SmManager* sm_manager_;
    compiled::ProgramTemplateCache owned_template_cache_;
    compiled::ProgramTemplateCache* template_cache_;

    static bool point_program_cache_enabled() {
        const char* value = std::getenv("ENABLE_POINT_PROGRAM_CACHE");
        return value != nullptr && std::string(value) == "1";
    }

    void record_template_fallback() {
        if (point_program_cache_enabled()) {
            template_cache_->RecordFallback();
        }
    }

    void record_template_handled() {
        if (point_program_cache_enabled()) {
            template_cache_->RecordHandled();
        }
    }

    static compiled::ValueType compiled_type(ColType type) {
        switch (type) {
        case TYPE_INT:
            return compiled::ValueType::INT32;
        case TYPE_FLOAT:
            return compiled::ValueType::FLOAT64;
        case TYPE_STRING:
        case TYPE_DATETIME:
            return compiled::ValueType::BYTES;
        default:
            throw InternalError("unsupported point SELECT column type");
        }
    }

    static compiled::CompareOp compiled_op(CompOp op) {
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
        throw InternalError("unsupported point SELECT comparison");
    }

    static compiled::ParameterValue compiled_value(const Value& value) {
        switch (value.type) {
        case TYPE_INT:
            return compiled::ParameterValue::Int(value.int_val);
        case TYPE_FLOAT:
            return compiled::ParameterValue::Float(value.float_val);
        case TYPE_STRING:
        case TYPE_DATETIME:
            return compiled::ParameterValue::Bytes(value.str_val);
        default:
            throw InternalError("unsupported point SELECT literal");
        }
    }

    static bool point_select_enabled() {
        const char* value = std::getenv("ENABLE_POINT_SELECT_INTERPRETER");
        return value != nullptr && std::string(value) == "1";
    }

    std::shared_ptr<PortalStmt::CompiledSelectSpec> build_compiled_select(const DMLPlan& dml, Context* context) {
        if (!point_select_enabled() || (context != nullptr && context->txn_ != nullptr &&
                                        context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED)) {
            return nullptr;
        }
        auto projection = dynamic_cast<const ProjectionPlan*>(dml.subplan_.get());
        if (projection == nullptr || projection->subplan_ == nullptr ||
            (!projection->is_select_star_ && projection->select_items_.empty())) {
            return nullptr;
        }
        auto scan = dynamic_cast<const ScanPlan*>(projection->subplan_.get());
        if (scan == nullptr || scan->tag != T_IndexScan || scan->index_col_names_.empty() || scan->conds_.empty()) {
            return nullptr;
        }
        auto& table = sm_manager_->db_.get_table(scan->tab_name_);
        auto index_it = std::find_if(table.indexes.begin(), table.indexes.end(), [&](const IndexMeta& index) {
            if (index.cols.size() != scan->index_col_names_.size()) {
                return false;
            }
            for (size_t i = 0; i < index.cols.size(); ++i) {
                if (index.cols[i].name != scan->index_col_names_[i]) {
                    return false;
                }
            }
            return true;
        });
        if (index_it == table.indexes.end()) {
            return nullptr;
        }
        std::vector<size_t> key_positions;
        key_positions.reserve(index_it->cols.size());
        std::vector<bool> used(scan->conds_.size(), false);
        for (const auto& index_col : index_it->cols) {
            size_t found = scan->conds_.size();
            for (size_t i = 0; i < scan->conds_.size(); ++i) {
                const auto& condition = scan->conds_[i];
                if (!used[i] && condition.is_rhs_val && condition.op == OP_EQ &&
                    condition.lhs_col.tab_name == scan->tab_name_ && condition.lhs_col.col_name == index_col.name) {
                    found = i;
                    break;
                }
            }
            if (found == scan->conds_.size()) {
                return nullptr;
            }
            used[found] = true;
            key_positions.push_back(found);
        }

        std::vector<ColMeta> output_cols;
        std::vector<std::string> captions;
        if (projection->is_select_star_) {
            output_cols = scan->cols_;
            captions.reserve(output_cols.size());
            for (const auto& col : output_cols) {
                captions.push_back(col.name);
            }
        } else {
            for (const auto& item : projection->select_items_) {
                if (item.expr.type != QueryExprType::COLUMN || item.expr.col.tab_name != scan->tab_name_) {
                    return nullptr;
                }
                auto col = std::find_if(scan->cols_.begin(), scan->cols_.end(), [&](const ColMeta& candidate) {
                    return candidate.name == item.expr.col.col_name && candidate.tab_name == item.expr.col.tab_name;
                });
                if (col == scan->cols_.end()) {
                    return nullptr;
                }
                ColMeta selected = *col;
                const std::string name = item.output_name.empty()
                                             ? (item.alias.empty() ? item.expr.col.col_name : item.alias)
                                             : item.output_name;
                selected.name = name;
                selected.tab_name.clear();
                output_cols.push_back(selected);
                captions.push_back(name);
            }
            if (output_cols.empty()) {
                return nullptr;
            }
        }

        const uint32_t tuple_size = static_cast<uint32_t>(scan->len_);
        compiled::TupleLayout layout;
        layout.byte_size = tuple_size;
        layout.columns.reserve(scan->cols_.size());
        for (const auto& col : scan->cols_) {
            layout.columns.push_back(
                {compiled_type(col.type), static_cast<uint32_t>(col.offset), static_cast<uint32_t>(col.len)});
        }
        std::vector<compiled::ParameterDesc> params;
        std::vector<compiled::ParameterValue> values;
        auto parameter_max_length = [&](const Condition& condition) -> uint32_t {
            const auto type = compiled_type(condition.rhs_val.type);
            if (type != compiled::ValueType::BYTES) {
                return 0;
            }
            const auto col = std::find_if(scan->cols_.begin(), scan->cols_.end(), [&](const ColMeta& candidate) {
                return candidate.tab_name == condition.lhs_col.tab_name && candidate.name == condition.lhs_col.col_name;
            });
            return col == scan->cols_.end() ? 0U : static_cast<uint32_t>(col->len);
        };
        for (const auto& condition : scan->conds_) {
            if (!condition.is_rhs_val || condition.lhs_col.tab_name != scan->tab_name_) {
                return nullptr;
            }
            params.push_back({compiled_type(condition.rhs_val.type), parameter_max_length(condition),
                              condition.rhs_val.lexical_slot});
            values.push_back(compiled_value(condition.rhs_val));
        }

        std::vector<compiled::RegisterDesc> regs;
        regs.push_back({compiled::ValueType::TUPLE, 0, 0});
        regs.push_back({compiled::ValueType::POINT_KEY, compiled::kNoOperand, 0});
        regs.push_back({compiled::ValueType::ROW_HANDLE, compiled::kNoOperand, 0});
        regs.push_back({compiled::ValueType::TUPLE, 0, 0});
        std::vector<compiled::Instruction> code;
        for (size_t i = 0; i < key_positions.size(); ++i) {
            const size_t condition_pos = key_positions[i];
            const auto col_it = std::find_if(scan->cols_.begin(), scan->cols_.end(), [&](const ColMeta& col) {
                return col.name == scan->conds_[condition_pos].lhs_col.col_name;
            });
            if (col_it == scan->cols_.end()) {
                return nullptr;
            }
            const uint32_t value_reg = static_cast<uint32_t>(regs.size());
            regs.push_back({compiled_type(scan->conds_[condition_pos].rhs_val.type), compiled::kNoOperand,
                            parameter_max_length(scan->conds_[condition_pos])});
            code.push_back({compiled::Opcode::LOAD_PARAM, value_reg, compiled::kNoOperand, compiled::kNoOperand,
                            static_cast<uint32_t>(condition_pos)});
            const size_t col_index = static_cast<size_t>(col_it - scan->cols_.begin());
            code.push_back(
                {compiled::Opcode::STORE_COLUMN, 0, value_reg, compiled::kNoOperand, static_cast<uint32_t>(col_index)});
        }
        code.push_back({compiled::Opcode::MAKE_POINT_KEY, 1, 0, compiled::kNoOperand, 0});
        code.push_back({compiled::Opcode::POINT_LOOKUP, 3, 1, 2});
        std::vector<size_t> jump_positions;
        for (size_t i = 0; i < scan->conds_.size(); ++i) {
            const auto& condition = scan->conds_[i];
            auto col_it = std::find_if(scan->cols_.begin(), scan->cols_.end(),
                                       [&](const ColMeta& col) { return col.name == condition.lhs_col.col_name; });
            if (col_it == scan->cols_.end()) {
                return nullptr;
            }
            const uint32_t lhs_reg = static_cast<uint32_t>(regs.size());
            regs.push_back(
                {compiled_type(col_it->type), compiled::kNoOperand,
                 compiled_type(col_it->type) == compiled::ValueType::BYTES ? static_cast<uint32_t>(col_it->len) : 0});
            const uint32_t rhs_reg = static_cast<uint32_t>(regs.size());
            regs.push_back(
                {compiled_type(condition.rhs_val.type), compiled::kNoOperand, parameter_max_length(condition)});
            const uint32_t bool_reg = static_cast<uint32_t>(regs.size());
            regs.push_back({compiled::ValueType::BOOL, compiled::kNoOperand, 0});
            code.push_back({compiled::Opcode::LOAD_COLUMN, lhs_reg, 3, compiled::kNoOperand,
                            static_cast<uint32_t>(col_it - scan->cols_.begin())});
            code.push_back({compiled::Opcode::LOAD_PARAM, rhs_reg, compiled::kNoOperand, compiled::kNoOperand,
                            static_cast<uint32_t>(i)});
            code.push_back({compiled::Opcode::COMPARE, bool_reg, lhs_reg, rhs_reg, compiled::kNoOperand,
                            compiled_op(condition.op)});
            jump_positions.push_back(code.size());
            code.push_back({compiled::Opcode::JUMP_IF_FALSE, compiled::kNoOperand, bool_reg, compiled::kNoOperand,
                            compiled::kNoOperand});
        }
        code.push_back({compiled::Opcode::EMIT_ROW, compiled::kNoOperand, 3});
        const uint32_t halt_pc = static_cast<uint32_t>(code.size());
        code.push_back({compiled::Opcode::HALT});
        for (size_t jump : jump_positions) {
            code[jump].aux = halt_pc;
        }

        auto spec = std::make_shared<PortalStmt::CompiledSelectSpec>();
        spec->program = std::make_shared<compiled::CompiledProgram>(
            compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, compiled::ProgramKind::POINT_SELECT,
            sm_manager_->get_catalog_generation(), std::move(params), std::move(regs),
            std::vector<compiled::TupleLayout>{std::move(layout)}, std::move(code));
        spec->parameters = std::move(values);
        PointIndexRuntimeBinding binding;
        binding.table_name = scan->tab_name_;
        binding.index_col_names = scan->index_col_names_;
        for (const auto& index_col : index_it->cols) {
            auto col_it = std::find_if(scan->cols_.begin(), scan->cols_.end(),
                                       [&](const ColMeta& col) { return col.name == index_col.name; });
            if (col_it == scan->cols_.end()) {
                return nullptr;
            }
            binding.tuple_offsets.push_back(static_cast<uint32_t>(col_it->offset));
        }
        spec->bindings.catalog_generation = sm_manager_->get_catalog_generation();
        spec->bindings.point_indexes.push_back(std::move(binding));
        spec->output_cols = std::move(output_cols);
        spec->output_captions = std::move(captions);
        spec->template_bindings.table.table_name = scan->tab_name_;
        spec->template_bindings.table.tuple_width = tuple_size;
        for (const auto& col : scan->cols_) {
            spec->template_bindings.table.columns.push_back({scan->tab_name_, col.name, compiled_type(col.type),
                                                             static_cast<uint32_t>(col.offset),
                                                             static_cast<uint32_t>(col.len)});
        }
        compiled::TemplateIndexDesc template_index;
        template_index.table_name = scan->tab_name_;
        template_index.index_name = sm_manager_->get_ix_manager()->get_index_name(scan->tab_name_, index_it->cols);
        template_index.column_names = scan->index_col_names_;
        template_index.tuple_offsets = spec->bindings.point_indexes.front().tuple_offsets;
        spec->template_bindings.point_indexes.push_back(std::move(template_index));
        for (size_t i = 0; i < spec->output_cols.size(); ++i) {
            const auto& selected = spec->output_cols[i];
            auto source = std::find_if(scan->cols_.begin(), scan->cols_.end(), [&](const ColMeta& col) {
                return col.offset == selected.offset && col.len == selected.len && col.type == selected.type;
            });
            if (source == scan->cols_.end()) {
                return nullptr;
            }
            spec->template_bindings.output_columns.push_back(
                {{scan->tab_name_, source->name, compiled_type(source->type), static_cast<uint32_t>(source->offset),
                  static_cast<uint32_t>(source->len)},
                 spec->output_captions[i]});
        }
        for (const auto& condition : scan->conds_) {
            spec->template_bindings.conditions.push_back({{condition.lhs_col.tab_name, condition.lhs_col.col_name},
                                                          compiled_op(condition.op),
                                                          true,
                                                          {},
                                                          condition.rhs_val.lexical_slot});
        }
        for (size_t i = 0; i < spec->program->parameters().size(); ++i) {
            const auto& parameter = spec->program->parameters()[i];
            spec->template_parameters.push_back(
                {static_cast<uint32_t>(i), parameter.lexical_slot, parameter.type, parameter.max_length});
        }
        if (point_program_cache_enabled() && context != nullptr && context->has_statement_template_identity_) {
            compiled::ProgramTemplateIdentity identity{
                parser::TokenShapeKey{context->statement_shape_high_, context->statement_shape_low_,
                                      context->statement_shape_size_},
                sm_manager_->get_catalog_generation(), context->statement_template_generation_,
                context->planner_generation_, compiled::ProgramKind::POINT_SELECT};
            std::string error;
            auto program_template = compiled::ProgramTemplate::Create(
                std::move(identity), spec->program, spec->template_parameters, spec->template_bindings, &error);
            if (program_template != nullptr) {
                compiled::ProgramCacheKey key{
                    program_template->identity().token_shape, program_template->identity().statement_generation,
                    program_template->identity().planner_generation, program_template->identity().catalog_generation,
                    program_template->identity().kind};
                spec->compiled_template = template_cache_->Publish(key, std::move(program_template));
            }
        }
        return spec;
    }

    static bool point_mutation_interpreter_enabled() {
        const char* value = std::getenv("ENABLE_POINT_MUTATION_INTERPRETER");
        return value != nullptr && std::string(value) == "1";
    }

    void publish_mutation_template(PortalStmt::CompiledMutationSpec* spec, const DMLPlan& dml, const TabMeta& table,
                                   Context* context) {
        if (spec == nullptr || spec->program == nullptr || !point_program_cache_enabled() || context == nullptr ||
            !context->has_statement_template_identity_) {
            return;
        }
        auto& bindings = spec->template_bindings;
        bindings.table.table_name = dml.tab_name_;
        bindings.table.tuple_width =
            static_cast<uint32_t>(spec->insert_info.fh != nullptr ? spec->insert_info.fh->get_file_hdr().record_size
                                                                  : table.cols.back().offset + table.cols.back().len);
        for (const auto& col : table.cols) {
            bindings.table.columns.push_back({dml.tab_name_, col.name, compiled_type(col.type),
                                              static_cast<uint32_t>(col.offset), static_cast<uint32_t>(col.len)});
        }
        const auto make_index = [&](const IndexMeta& index) {
            compiled::TemplateIndexDesc result;
            result.table_name = dml.tab_name_;
            result.index_name = sm_manager_->get_ix_manager()->get_index_name(dml.tab_name_, index.cols);
            for (const auto& col : index.cols) {
                result.column_names.push_back(col.name);
                result.tuple_offsets.push_back(static_cast<uint32_t>(col.offset));
            }
            return result;
        };
        if (dml.point_access_.has_value()) {
            auto point = std::find_if(table.indexes.begin(), table.indexes.end(), [&](const IndexMeta& index) {
                if (index.cols.size() != dml.point_access_->index_cols.size()) {
                    return false;
                }
                for (size_t i = 0; i < index.cols.size(); ++i) {
                    if (index.cols[i].name != dml.point_access_->index_cols[i]) {
                        return false;
                    }
                }
                return true;
            });
            if (point == table.indexes.end()) {
                return;
            }
            bindings.point_indexes.push_back(make_index(*point));
        }
        for (const auto& index : table.indexes) {
            bindings.mutation_indexes.push_back(make_index(index));
        }

        auto add_lexical = [&](uint32_t program_parameter, const Value& value, uint32_t max_length) -> bool {
            if (value.lexical_slot < 0) {
                return false;
            }
            auto existing = std::find_if(spec->template_parameters.begin(), spec->template_parameters.end(),
                                         [&](const auto& item) { return item.lexical_slot == value.lexical_slot; });
            if (existing != spec->template_parameters.end()) {
                return existing->program_parameter == program_parameter || program_parameter == compiled::kNoOperand;
            }
            spec->template_parameters.push_back(
                {program_parameter, value.lexical_slot, compiled_type(value.type), max_length});
            return true;
        };
        for (size_t i = 0; i < spec->program->parameters().size(); ++i) {
            const auto& parameter = spec->program->parameters()[i];
            if (parameter.lexical_slot < 0) {
                return;
            }
            spec->template_parameters.push_back(
                {static_cast<uint32_t>(i), parameter.lexical_slot, parameter.type, parameter.max_length});
        }
        for (const auto& condition : dml.conds_) {
            compiled::TemplateConditionDesc item;
            item.lhs = {condition.lhs_col.tab_name, condition.lhs_col.col_name};
            item.op = compiled_op(condition.op);
            item.rhs_is_parameter = condition.is_rhs_val;
            if (condition.is_rhs_val) {
                item.rhs_lexical_slot = condition.rhs_val.lexical_slot;
                auto col = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& candidate) {
                    return candidate.name == condition.lhs_col.col_name;
                });
                if (col == table.cols.end() ||
                    !add_lexical(compiled::kNoOperand, condition.rhs_val,
                                 compiled_type(condition.rhs_val.type) == compiled::ValueType::BYTES
                                     ? static_cast<uint32_t>(col->len)
                                     : 0)) {
                    return;
                }
            } else {
                item.rhs_column = {condition.rhs_col.tab_name, condition.rhs_col.col_name};
            }
            bindings.conditions.push_back(std::move(item));
        }
        for (const auto& set_clause : dml.set_clauses_) {
            compiled::TemplateSetDesc item;
            item.lhs = {set_clause.lhs.tab_name, set_clause.lhs.col_name};
            item.rhs_is_column = set_clause.is_self_ref;
            if (set_clause.is_self_ref) {
                item.rhs_column = {set_clause.rhs_col.tab_name, set_clause.rhs_col.col_name};
            }
            item.op = set_clause.op == UpdateOp::SELF_ADD   ? compiled::TemplateSetOp::ADD
                      : set_clause.op == UpdateOp::SELF_SUB ? compiled::TemplateSetOp::SUB
                      : set_clause.op == UpdateOp::SELF_MUL ? compiled::TemplateSetOp::MUL
                      : set_clause.op == UpdateOp::SELF_DIV ? compiled::TemplateSetOp::DIV
                                                            : compiled::TemplateSetOp::ASSIGNMENT;
            if (!set_clause.is_self_ref || set_clause.op != UpdateOp::ASSIGNMENT) {
                item.rhs_lexical_slot = set_clause.rhs.lexical_slot;
            }
            bindings.set_clauses.push_back(std::move(item));
        }
        for (const auto& value : dml.values_) {
            bindings.insert_value_slots.push_back(value.lexical_slot);
        }
        bindings.affected_mutation_indexes = spec->affected_index_bitmap;

        compiled::ProgramTemplateIdentity identity{
            parser::TokenShapeKey{context->statement_shape_high_, context->statement_shape_low_,
                                  context->statement_shape_size_},
            sm_manager_->get_catalog_generation(), context->statement_template_generation_,
            context->planner_generation_, spec->program->kind()};
        std::string error;
        auto program_template = compiled::ProgramTemplate::Create(std::move(identity), spec->program,
                                                                  spec->template_parameters, bindings, &error);
        if (program_template == nullptr) {
            return;
        }
        compiled::ProgramCacheKey key{
            program_template->identity().token_shape, program_template->identity().statement_generation,
            program_template->identity().planner_generation, program_template->identity().catalog_generation,
            program_template->identity().kind};
        spec->compiled_template = template_cache_->Publish(key, std::move(program_template));
    }

    std::shared_ptr<PortalStmt::CompiledMutationSpec> build_compiled_mutation(const DMLPlan& dml, Context* context) {
        if (!point_mutation_interpreter_enabled() ||
            (context != nullptr && context->txn_ != nullptr &&
             context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED)) {
            return nullptr;
        }
        if (dml.tag != T_Insert && dml.tag != T_Delete && dml.tag != T_Update) {
            return nullptr;
        }

        auto& table = sm_manager_->db_.get_table(dml.tab_name_);
        auto* fh = sm_manager_->fhs_.at(dml.tab_name_).get();
        if (table.cols.empty() || fh == nullptr) {
            return nullptr;
        }

        auto spec = std::make_shared<PortalStmt::CompiledMutationSpec>();
        spec->table_name = dml.tab_name_;
        spec->conditions = dml.conds_;
        spec->set_clauses = dml.set_clauses_;
        spec->indexes.reserve(table.indexes.size());
        for (const auto& index : table.indexes) {
            std::string index_name = sm_manager_->get_ix_manager()->get_index_name(dml.tab_name_, index.cols);
            auto handle = sm_manager_->ihs_.find(index_name);
            if (handle == sm_manager_->ihs_.end()) {
                return nullptr;
            }
            spec->indexes.push_back(RowMutationIndex{&index, handle->second.get(), std::move(index_name)});
        }

        compiled::TupleLayout layout;
        layout.byte_size = static_cast<uint32_t>(fh->get_file_hdr().record_size);
        layout.columns.reserve(table.cols.size());
        for (const auto& col : table.cols) {
            layout.columns.push_back(
                {compiled_type(col.type), static_cast<uint32_t>(col.offset), static_cast<uint32_t>(col.len)});
        }

        std::vector<compiled::ParameterDesc> params;
        std::vector<compiled::ParameterValue> values;
        std::vector<compiled::RegisterDesc> regs;
        std::vector<compiled::Instruction> code;
        auto append_parameter = [&](const Value& value, const ColMeta& target) -> uint32_t {
            const uint32_t parameter = static_cast<uint32_t>(params.size());
            const auto type = compiled_type(value.type);
            params.push_back(
                {type, type == compiled::ValueType::BYTES ? static_cast<uint32_t>(target.len) : 0, value.lexical_slot});
            values.push_back(compiled_value(value));
            return parameter;
        };
        auto append_value_register = [&](const Value& value, const ColMeta& target) -> uint32_t {
            const uint32_t reg = static_cast<uint32_t>(regs.size());
            const auto type = compiled_type(value.type);
            regs.push_back({type, compiled::kNoOperand,
                            type == compiled::ValueType::BYTES ? static_cast<uint32_t>(target.len) : 0});
            return reg;
        };

        if (dml.tag == T_Insert) {
            if (dml.values_.size() != table.cols.size()) {
                return nullptr;
            }
            regs.push_back({compiled::ValueType::TUPLE, 0, 0});
            regs.push_back({compiled::ValueType::ROW_HANDLE, compiled::kNoOperand, 0});
            for (size_t i = 0; i < dml.values_.size(); ++i) {
                const uint32_t parameter = append_parameter(dml.values_[i], table.cols[i]);
                const uint32_t value_reg = append_value_register(dml.values_[i], table.cols[i]);
                code.push_back(
                    {compiled::Opcode::LOAD_PARAM, value_reg, compiled::kNoOperand, compiled::kNoOperand, parameter});
                code.push_back(
                    {compiled::Opcode::STORE_COLUMN, 0, value_reg, compiled::kNoOperand, static_cast<uint32_t>(i)});
            }
            code.push_back({compiled::Opcode::INSERT_ROW, 1, 0});
            code.push_back({compiled::Opcode::HALT});

            spec->insert_info = InsertRuntimeInfo{sm_manager_, &spec->table_name, &table, fh, &spec->indexes};
            spec->bindings.catalog_generation = sm_manager_->get_catalog_generation();
            spec->bindings.insert_info = &spec->insert_info;
            spec->program = std::make_shared<compiled::CompiledProgram>(
                compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, compiled::ProgramKind::POINT_INSERT,
                sm_manager_->get_catalog_generation(), std::move(params), std::move(regs),
                std::vector<compiled::TupleLayout>{std::move(layout)}, std::move(code));
            spec->parameters = std::move(values);
            publish_mutation_template(spec.get(), dml, table, context);
            return spec;
        }

        if (!dml.point_access_.has_value() || dml.subplan_ == nullptr) {
            return nullptr;
        }
        const auto& access = *dml.point_access_;
        auto index_it = std::find_if(table.indexes.begin(), table.indexes.end(), [&](const IndexMeta& index) {
            if (index.cols.size() != access.index_cols.size()) {
                return false;
            }
            for (size_t i = 0; i < index.cols.size(); ++i) {
                if (index.cols[i].name != access.index_cols[i]) {
                    return false;
                }
            }
            return true;
        });
        if (index_it == table.indexes.end() || access.condition_positions.size() != index_it->cols.size()) {
            return nullptr;
        }

        regs.push_back({compiled::ValueType::TUPLE, 0, 0});
        regs.push_back({compiled::ValueType::POINT_KEY, compiled::kNoOperand, 0});
        regs.push_back({compiled::ValueType::ROW_HANDLE, compiled::kNoOperand, 0});
        regs.push_back({compiled::ValueType::TUPLE, 0, 0});
        for (size_t i = 0; i < access.condition_positions.size(); ++i) {
            const size_t condition_pos = access.condition_positions[i];
            if (condition_pos >= dml.conds_.size()) {
                return nullptr;
            }
            const auto& condition = dml.conds_[condition_pos];
            if (!condition.is_rhs_val || condition.op != OP_EQ || condition.lhs_col.tab_name != dml.tab_name_ ||
                condition.lhs_col.col_name != index_it->cols[i].name) {
                return nullptr;
            }
            auto col = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& candidate) {
                return candidate.name == condition.lhs_col.col_name;
            });
            if (col == table.cols.end()) {
                return nullptr;
            }
            const uint32_t parameter = append_parameter(condition.rhs_val, *col);
            const uint32_t value_reg = append_value_register(condition.rhs_val, *col);
            code.push_back(
                {compiled::Opcode::LOAD_PARAM, value_reg, compiled::kNoOperand, compiled::kNoOperand, parameter});
            code.push_back({compiled::Opcode::STORE_COLUMN, 0, value_reg, compiled::kNoOperand,
                            static_cast<uint32_t>(col - table.cols.begin())});
        }
        code.push_back({compiled::Opcode::MAKE_POINT_KEY, 1, 0, compiled::kNoOperand, 0});
        code.push_back({compiled::Opcode::POINT_LOOKUP, 3, 1, 2});

        if (dml.tag == T_Delete) {
            code.push_back({compiled::Opcode::DELETE_ROW, compiled::kNoOperand, 2});
        } else {
            const uint32_t current_tuple_reg = static_cast<uint32_t>(regs.size());
            regs.push_back({compiled::ValueType::TUPLE, 0, 0});
            const uint32_t prepared_reg = static_cast<uint32_t>(regs.size());
            regs.push_back({compiled::ValueType::PREPARED_UPDATE, compiled::kNoOperand, 0});
            const uint32_t proposed_tuple_reg = static_cast<uint32_t>(regs.size());
            regs.push_back({compiled::ValueType::TUPLE, 0, 0});
            code.push_back({compiled::Opcode::PREPARE_UPDATE, prepared_reg, 2, current_tuple_reg});
            code.push_back({compiled::Opcode::COPY_TUPLE, proposed_tuple_reg, current_tuple_reg});
            for (const auto& set_clause : dml.set_clauses_) {
                auto lhs = std::find_if(table.cols.begin(), table.cols.end(),
                                        [&](const ColMeta& col) { return col.name == set_clause.lhs.col_name; });
                if (lhs == table.cols.end()) {
                    return nullptr;
                }
                const uint32_t lhs_index = static_cast<uint32_t>(lhs - table.cols.begin());
                if (set_clause.is_self_ref) {
                    auto rhs = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& col) {
                        return col.name == set_clause.rhs_col.col_name;
                    });
                    if (rhs == table.cols.end()) {
                        return nullptr;
                    }
                    const uint32_t base_reg = static_cast<uint32_t>(regs.size());
                    regs.push_back(
                        {compiled_type(rhs->type), compiled::kNoOperand,
                         compiled_type(rhs->type) == compiled::ValueType::BYTES ? static_cast<uint32_t>(rhs->len) : 0});
                    code.push_back({compiled::Opcode::LOAD_COLUMN, base_reg, current_tuple_reg, compiled::kNoOperand,
                                    static_cast<uint32_t>(rhs - table.cols.begin())});
                    if (set_clause.op == UpdateOp::ASSIGNMENT) {
                        code.push_back({compiled::Opcode::STORE_COLUMN, proposed_tuple_reg, base_reg,
                                        compiled::kNoOperand, lhs_index});
                        continue;
                    }
                    if ((lhs->type != TYPE_INT && lhs->type != TYPE_FLOAT) ||
                        (rhs->type != TYPE_INT && rhs->type != TYPE_FLOAT) ||
                        (lhs->type == TYPE_INT && rhs->type != TYPE_INT) || set_clause.rhs.type == TYPE_STRING ||
                        set_clause.rhs.type == TYPE_DATETIME) {
                        return nullptr;
                    }
                    const uint32_t delta_param = append_parameter(set_clause.rhs, *lhs);
                    const uint32_t delta_reg = append_value_register(set_clause.rhs, *lhs);
                    code.push_back({compiled::Opcode::LOAD_PARAM, delta_reg, compiled::kNoOperand, compiled::kNoOperand,
                                    delta_param});
                    const uint32_t result_reg = static_cast<uint32_t>(regs.size());
                    regs.push_back({compiled_type(lhs->type), compiled::kNoOperand, 0});
                    compiled::Opcode arithmetic = compiled::Opcode::ADD;
                    if (set_clause.op == UpdateOp::SELF_SUB) {
                        arithmetic = compiled::Opcode::SUB;
                    } else if (set_clause.op == UpdateOp::SELF_MUL) {
                        arithmetic = compiled::Opcode::MUL;
                    } else if (set_clause.op == UpdateOp::SELF_DIV) {
                        arithmetic = compiled::Opcode::DIV;
                    }
                    code.push_back({arithmetic, result_reg, base_reg, delta_reg});
                    code.push_back({compiled::Opcode::STORE_COLUMN, proposed_tuple_reg, result_reg,
                                    compiled::kNoOperand, lhs_index});
                } else {
                    const uint32_t parameter = append_parameter(set_clause.rhs, *lhs);
                    const uint32_t value_reg = append_value_register(set_clause.rhs, *lhs);
                    code.push_back({compiled::Opcode::LOAD_PARAM, value_reg, compiled::kNoOperand, compiled::kNoOperand,
                                    parameter});
                    code.push_back({compiled::Opcode::STORE_COLUMN, proposed_tuple_reg, value_reg, compiled::kNoOperand,
                                    lhs_index});
                }
            }
            code.push_back({compiled::Opcode::COMMIT_UPDATE, compiled::kNoOperand, prepared_reg, proposed_tuple_reg});
        }
        code.push_back({compiled::Opcode::HALT});

        PointIndexRuntimeBinding point_binding;
        point_binding.table_name = dml.tab_name_;
        point_binding.index_col_names = access.index_cols;
        for (const auto& index_col : index_it->cols) {
            point_binding.tuple_offsets.push_back(static_cast<uint32_t>(index_col.offset));
        }
        spec->bound_conditions = BindMutationConditions(table, spec->conditions);
        spec->bindings.catalog_generation = sm_manager_->get_catalog_generation();
        spec->bindings.point_indexes.push_back(std::move(point_binding));
        compiled::ProgramKind program_kind = compiled::ProgramKind::POINT_DELETE;
        if (dml.tag == T_Delete) {
            spec->delete_info = DeleteRuntimeInfo{sm_manager_,       &spec->table_name,       &table,        fh,
                                                  &spec->conditions, &spec->bound_conditions, &spec->indexes};
            spec->bindings.delete_info = &spec->delete_info;
        } else {
            spec->bound_set_clauses = BindMutationSetClauses(table, spec->set_clauses);
            spec->affected_index_bitmap.assign(table.indexes.size(), false);
            for (size_t index_no = 0; index_no < table.indexes.size(); ++index_no) {
                for (const auto& set_clause : spec->set_clauses) {
                    if (std::any_of(table.indexes[index_no].cols.begin(), table.indexes[index_no].cols.end(),
                                    [&](const ColMeta& col) { return col.name == set_clause.lhs.col_name; })) {
                        spec->affected_index_bitmap[index_no] = true;
                        break;
                    }
                }
            }
            spec->update_info = UpdateRuntimeInfo{sm_manager_,
                                                  &spec->table_name,
                                                  &table,
                                                  fh,
                                                  &spec->conditions,
                                                  &spec->bound_conditions,
                                                  &spec->indexes,
                                                  &spec->set_clauses,
                                                  &spec->bound_set_clauses,
                                                  &spec->affected_index_bitmap};
            spec->bindings.update_info = &spec->update_info;
            program_kind = compiled::ProgramKind::POINT_UPDATE;
        }
        spec->program = std::make_shared<compiled::CompiledProgram>(
            compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION, program_kind,
            sm_manager_->get_catalog_generation(), std::move(params), std::move(regs),
            std::vector<compiled::TupleLayout>{std::move(layout)}, std::move(code));
        spec->parameters = std::move(values);
        publish_mutation_template(spec.get(), dml, table, context);
        return spec;
    }

    static bool point_dml_enabled() {
        static const bool enabled = [] {
            const char* value = std::getenv("ENABLE_POINT_DML");
            return value == nullptr || std::string(value) != "0";
        }();
        return enabled;
    }

    // nullopt means the point path is not safe and the caller must build the
    // original scan executor. A value with no RID is a proven no-match result.
    std::optional<std::optional<Rid>> resolve_point_rid(const DMLPlan& plan, Context* context) const {
        if (!point_dml_enabled() || !plan.point_access_.has_value()) {
            return std::nullopt;
        }
        const auto& path = *plan.point_access_;
        PointLookupRequest request{&plan.tab_name_, &path.index_cols, &plan.conds_, &path.condition_positions};
        const auto result = PointLookupRuntime::Lookup(request, sm_manager_, context);
        if (result.status == PointLookupStatus::FALLBACK) {
            return std::nullopt;
        }
        return result.rid;
    }

    struct ExecutorQueryExpr {
        QueryExprType type = QueryExprType::COLUMN;
        TabCol col;
        AggExpr agg;
        Value val;
        Value value;
        std::string display_name;
    };

    struct ExecutorSelectItem {
        ExecutorQueryExpr expr;
        std::string alias;
        std::string display_name;
        std::string output_name;
    };

    struct ExecutorHavingCondition {
        ExecutorQueryExpr lhs;
        CompOp op = OP_EQ;
        bool is_rhs_val = false;
        bool is_rhs_value = false;
        ExecutorQueryExpr rhs_expr;
        Value rhs_val;
    };

    class CountingExecutor : public AbstractExecutor {
    private:
        std::unique_ptr<AbstractExecutor> inner_;
        size_t* runtime_rows_;
        bool counting_enabled_ = true;
        bool current_counted_ = false;

        void count_current_if_available() {
            current_counted_ = false;
            if (!counting_enabled_ || inner_->is_end() || !inner_->current()) {
                return;
            }
            ++*runtime_rows_;
            current_counted_ = true;
        }

    public:
        CountingExecutor(std::unique_ptr<AbstractExecutor> inner, size_t* runtime_rows) {
            inner_ = std::move(inner);
            runtime_rows_ = runtime_rows;
            context_ = inner_->context_;
        }

        size_t tupleLen() const override {
            return inner_->tupleLen();
        }

        const std::vector<ColMeta>& cols() const override {
            return inner_->cols();
        }

        std::string getType() override {
            return inner_->getType();
        }

        void beginTuple() override {
            inner_->beginTuple();
            count_current_if_available();
        }

        void nextTuple() override {
            inner_->nextTuple();
            count_current_if_available();
        }

        bool is_end() const override {
            return inner_->is_end();
        }

        Rid& rid() override {
            return inner_->rid();
        }

        std::unique_ptr<RmRecord> Next() override {
            auto rec = inner_->Next();
            if (rec != nullptr && counting_enabled_ && !current_counted_) {
                ++*runtime_rows_;
            }
            current_counted_ = false;
            return rec;
        }

        TupleView current() const override {
            return inner_->current();
        }

        ColMeta get_col_offset(const TabCol& target) override {
            return inner_->get_col_offset(target);
        }

        void set_counting_enabled(bool enabled) override {
            counting_enabled_ = enabled;
            inner_->set_counting_enabled(enabled);
        }

        void set_key_conditions(std::vector<Condition> key_conds) override {
            inner_->set_key_conditions(std::move(key_conds));
        }

        void bind_lookup_key(const TabCol& target, LookupKeyView key) override {
            inner_->bind_lookup_key(target, key);
        }

        std::string scan_table_name() const override {
            return inner_->scan_table_name();
        }

        std::string_view scan_table_name_view() const override {
            return inner_->scan_table_name_view();
        }

        std::vector<Condition> scan_conditions() const override {
            return inner_->scan_conditions();
        }

        const std::vector<Condition>& scan_conditions_ref() const override {
            return inner_->scan_conditions_ref();
        }

        void record_current_read_for_ssi() override {
            inner_->record_current_read_for_ssi();
        }
    };

    static ExecutorQueryExpr to_executor_query_expr(const QueryExpr& expr) {
        ExecutorQueryExpr executor_expr;
        executor_expr.type = expr.type;
        executor_expr.col = expr.col;
        executor_expr.agg = expr.agg;
        executor_expr.val = expr.value;
        executor_expr.value = expr.value;
        executor_expr.display_name = expr.display_name;
        return executor_expr;
    }

    static Value bind_value(const Value& value, const PlanLiteralOverlay* literals) {
        if (literals == nullptr || value.lexical_slot < 0) {
            return value;
        }
        const Value* bound = literals->Find(value.lexical_slot);
        if (bound == nullptr) {
            throw InternalError("cached plan literal binding is missing");
        }
        return *bound;
    }

    static std::vector<Condition> bind_conditions(const std::vector<Condition>& conditions,
                                                  const PlanLiteralOverlay* literals) {
        std::vector<Condition> bound = conditions;
        for (auto& condition : bound) {
            if (condition.is_rhs_val) {
                condition.rhs_val = bind_value(condition.rhs_val, literals);
            }
        }
        return bound;
    }

    static std::vector<SetClause> bind_set_clauses(const std::vector<SetClause>& clauses,
                                                   const PlanLiteralOverlay* literals) {
        std::vector<SetClause> bound = clauses;
        for (auto& clause : bound) {
            clause.rhs = bind_value(clause.rhs, literals);
        }
        return bound;
    }

    static std::vector<ExecutorSelectItem> to_executor_select_items(const std::vector<SelectItem>& select_items) {
        std::vector<ExecutorSelectItem> executor_items;
        executor_items.reserve(select_items.size());
        for (const auto& item : select_items) {
            ExecutorSelectItem executor_item;
            executor_item.expr = to_executor_query_expr(item.expr);
            executor_item.alias = item.alias;
            executor_item.display_name = !item.output_name.empty()
                                             ? item.output_name
                                             : (!item.alias.empty() ? item.alias : item.expr.display_name);
            executor_item.output_name = item.output_name;
            executor_items.push_back(std::move(executor_item));
        }
        return executor_items;
    }

    static std::vector<ExecutorHavingCondition>
    to_executor_having_conds(const std::vector<HavingCondition>& having_conds) {
        std::vector<ExecutorHavingCondition> executor_conds;
        executor_conds.reserve(having_conds.size());
        for (const auto& cond : having_conds) {
            ExecutorHavingCondition executor_cond;
            executor_cond.lhs = to_executor_query_expr(cond.lhs);
            executor_cond.op = cond.op;
            executor_cond.is_rhs_val = cond.is_rhs_val;
            executor_cond.is_rhs_value = cond.is_rhs_val;
            executor_cond.rhs_expr = to_executor_query_expr(cond.rhs_expr);
            executor_cond.rhs_val = cond.rhs_val;
            executor_conds.push_back(std::move(executor_cond));
        }
        return executor_conds;
    }

    static bool same_tab_col(const TabCol& lhs, const TabCol& rhs) {
        return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
    }

    static bool same_query_expr(const QueryExpr& lhs, const QueryExpr& rhs) {
        if (lhs.type != rhs.type) {
            return false;
        }
        switch (lhs.type) {
        case QueryExprType::COLUMN:
            return same_tab_col(lhs.col, rhs.col);
        case QueryExprType::VALUE:
            return false;
        case QueryExprType::AGGREGATE:
            return lhs.agg.type == rhs.agg.type && lhs.agg.is_star == rhs.agg.is_star &&
                   (lhs.agg.is_star || same_tab_col(lhs.agg.col, rhs.agg.col));
        }
        return false;
    }

    static std::string get_select_item_output_name(const SelectItem& item) {
        if (!item.output_name.empty()) {
            return item.output_name;
        }
        if (!item.alias.empty()) {
            return item.alias;
        }
        if (!item.expr.display_name.empty()) {
            return item.expr.display_name;
        }
        if (item.expr.type == QueryExprType::AGGREGATE) {
            return item.expr.agg.display_name;
        }
        return item.expr.col.col_name;
    }

    static std::vector<OrderByItem> bind_sort_output_names(const SortPlan& plan) {
        auto order_by_items = plan.order_by_items_;
        auto* projection = dynamic_cast<ProjectionPlan*>(plan.subplan_.get());
        if (projection == nullptr) {
            return order_by_items;
        }

        for (auto& item : order_by_items) {
            if (!item.order_name.empty()) {
                continue;
            }
            auto pos = std::find_if(
                projection->select_items_.begin(), projection->select_items_.end(),
                [&](const SelectItem& select_item) { return same_query_expr(select_item.expr, item.expr); });
            if (pos != projection->select_items_.end()) {
                item.order_name = get_select_item_output_name(*pos);
            }
        }
        return order_by_items;
    }

    static std::vector<std::string> build_projection_output_names(const ProjectionPlan& plan) {
        if (!plan.output_names_.empty()) {
            return plan.output_names_;
        }

        std::vector<std::string> output_names;
        output_names.reserve(plan.select_items_.size());
        for (const auto& item : plan.select_items_) {
            if (!item.output_name.empty()) {
                output_names.push_back(item.output_name);
            } else if (!item.alias.empty()) {
                output_names.push_back(item.alias);
            } else if (!item.expr.display_name.empty()) {
                output_names.push_back(item.expr.display_name);
            } else if (item.expr.type == QueryExprType::AGGREGATE) {
                output_names.push_back(item.expr.agg.display_name);
            } else {
                output_names.push_back(item.expr.col.col_name);
            }
        }
        return output_names;
    }

    static std::vector<std::string> build_aggregate_output_names(const AggregatePlan& plan) {
        std::vector<std::string> output_names;
        output_names.reserve(plan.group_by_cols_.size() + plan.agg_exprs_.size());
        for (const auto& group_col : plan.group_by_cols_) {
            output_names.push_back(group_col.col_name);
        }
        for (const auto& agg_expr : plan.agg_exprs_) {
            output_names.push_back(agg_expr.display_name);
        }
        return output_names;
    }

    std::vector<std::string> get_plan_output_names(const Plan* plan) const {
        switch (plan->tag) {
        case T_Projection:
            return build_projection_output_names(*static_cast<const ProjectionPlan*>(plan));
        case T_Sort:
            return get_plan_output_names(static_cast<const SortPlan*>(plan)->subplan_.get());
        case T_Limit:
            return get_plan_output_names(static_cast<const LimitPlan*>(plan)->subplan_.get());
        case T_Aggregate:
            return build_aggregate_output_names(*static_cast<const AggregatePlan*>(plan));
        case T_Union: {
            auto union_plan = static_cast<const UnionPlan*>(plan);
            if (!union_plan->output_names_.empty()) {
                return union_plan->output_names_;
            }
            std::vector<std::string> output_names;
            output_names.reserve(union_plan->cols_.size());
            for (const auto& col : union_plan->cols_) {
                output_names.push_back(col.name);
            }
            return output_names;
        }
        case T_SeqScan:
        case T_IndexSkipScan:
        case T_IndexScan: {
            std::vector<std::string> output_names;
            const auto& cols = static_cast<const ScanPlan*>(plan)->cols_;
            output_names.reserve(cols.size());
            for (const auto& col : cols) {
                output_names.push_back(col.name);
            }
            return output_names;
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto join_plan = static_cast<const JoinPlan*>(plan);
            auto output_names = get_plan_output_names(join_plan->left_.get());
            auto right_output_names = get_plan_output_names(join_plan->right_.get());
            output_names.insert(output_names.end(), right_output_names.begin(), right_output_names.end());
            return output_names;
        }
        default:
            return {};
        }
    }

    static std::string display_table(const Plan& plan, const std::string& table_name) {
        auto pos = plan.table_name_to_display_.find(table_name);
        if (pos == plan.table_name_to_display_.end()) {
            return table_name;
        }
        return pos->second;
    }

    static std::string display_col(const Plan& plan, const TabCol& col) {
        return display_table(plan, col.tab_name) + "." + col.col_name;
    }

    static std::string comp_op_to_string(CompOp op) {
        switch (op) {
        case OP_EQ:
            return "=";
        case OP_NE:
            return "<>";
        case OP_LT:
            return "<";
        case OP_GT:
            return ">";
        case OP_LE:
            return "<=";
        case OP_GE:
            return ">=";
        }
        throw InternalError("Unexpected comparison operator");
    }

    static std::string value_to_string(const Value& val) {
        switch (val.type) {
        case TYPE_INT:
            return std::to_string(val.int_val);
        case TYPE_FLOAT: {
            std::ostringstream out;
            out << std::fixed << std::setprecision(6) << val.float_val;
            auto str = out.str();
            while (str.size() > 2 && str.back() == '0' && str[str.size() - 2] != '.') {
                str.pop_back();
            }
            return str;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            return "'" + val.str_val + "'";
        }
        throw InternalError("Unexpected value type");
    }

    static std::string condition_to_string(const Plan& plan, const Condition& cond) {
        std::string result = display_col(plan, cond.lhs_col) + comp_op_to_string(cond.op);
        if (cond.is_rhs_val) {
            result += cond.rhs_display.empty() ? value_to_string(cond.rhs_val) : cond.rhs_display;
        } else {
            result += display_col(plan, cond.rhs_col);
        }
        return result;
    }

    static std::string join_strings(std::vector<std::string> values) {
        std::sort(values.begin(), values.end());
        std::ostringstream out;
        for (size_t i = 0; i < values.size(); ++i) {
            if (i != 0) {
                out << ", ";
            }
            out << values[i];
        }
        return out.str();
    }

    static std::vector<std::string> condition_strings(const Plan& plan, const std::vector<Condition>& conds) {
        std::vector<std::string> values;
        values.reserve(conds.size());
        for (const auto& cond : conds) {
            values.push_back(condition_to_string(plan, cond));
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    static std::vector<std::string> condition_strings(const Plan& plan, const std::vector<Condition>& conds,
                                                      const PlanLiteralOverlay* literals) {
        auto bound = bind_conditions(conds, literals);
        for (auto& condition : bound) {
            if (condition.is_rhs_val && condition.rhs_val.lexical_slot >= 0) {
                condition.rhs_display.clear();
            }
        }
        return condition_strings(plan, bound);
    }

    static void collect_tables(const Plan* plan, std::set<std::string>& tables) {
        switch (plan->tag) {
        case T_SeqScan:
        case T_IndexSkipScan:
        case T_IndexScan:
            tables.insert(static_cast<const ScanPlan*>(plan)->tab_name_);
            break;
        case T_Filter:
            collect_tables(static_cast<const FilterPlan*>(plan)->subplan_.get(), tables);
            break;
        case T_Projection:
            collect_tables(static_cast<const ProjectionPlan*>(plan)->subplan_.get(), tables);
            break;
        case T_NestLoop:
        case T_SortMerge: {
            auto join = static_cast<const JoinPlan*>(plan);
            collect_tables(join->left_.get(), tables);
            collect_tables(join->right_.get(), tables);
            break;
        }
        default:
            break;
        }
    }

    static void reset_runtime_rows(Plan* plan) {
        if (plan == nullptr) {
            return;
        }
        plan->runtime_rows_ = 0;
        switch (plan->tag) {
        case T_Filter:
            reset_runtime_rows(static_cast<FilterPlan*>(plan)->subplan_.get());
            break;
        case T_Projection:
            reset_runtime_rows(static_cast<ProjectionPlan*>(plan)->subplan_.get());
            break;
        case T_NestLoop:
        case T_SortMerge: {
            auto join = static_cast<JoinPlan*>(plan);
            reset_runtime_rows(join->left_.get());
            reset_runtime_rows(join->right_.get());
            break;
        }
        default:
            break;
        }
    }

    static std::vector<std::string> projection_columns(const ProjectionPlan& plan) {
        if (plan.is_select_star_) {
            return {"*"};
        }
        std::vector<std::string> cols;
        cols.reserve(plan.select_items_.size());
        for (const auto& item : plan.select_items_) {
            if (item.expr.type == QueryExprType::COLUMN) {
                cols.push_back(display_col(plan, item.expr.col));
            }
        }
        std::sort(cols.begin(), cols.end());
        return cols;
    }

    static void render_explain_plan(Plan* plan, int depth, std::ostringstream& out) {
        out << std::string(static_cast<size_t>(depth), '\t');
        switch (plan->tag) {
        case T_SeqScan: {
            auto scan = static_cast<ScanPlan*>(plan);
            out << "Scan(table=" << scan->tab_name_ << ", type=SeqScan, rows=" << plan->runtime_rows_ << ")\n";
            break;
        }
        case T_IndexScan: {
            auto scan = static_cast<ScanPlan*>(plan);
            out << "Scan(table=" << scan->tab_name_ << ", type=IndexScan, using_index=(" << scan->index_col_names_[0]
                << "), rows=" << plan->runtime_rows_ << ")\n";
            break;
        }
        case T_IndexSkipScan: {
            auto scan = static_cast<ScanPlan*>(plan);
            out << "Scan(table=" << scan->tab_name_ << ", type=IndexSkipScan, using_index=("
                << scan->index_col_names_[0] << "), rows=" << plan->runtime_rows_ << ")\n";
            break;
        }
        case T_Filter: {
            auto filter = static_cast<FilterPlan*>(plan);
            out << "Filter(condition=[" << join_strings(condition_strings(*plan, filter->conds_))
                << "], rows=" << plan->runtime_rows_ << ")\n";
            render_explain_plan(filter->subplan_.get(), depth + 1, out);
            break;
        }
        case T_Projection: {
            auto projection = static_cast<ProjectionPlan*>(plan);
            out << "Project(columns=[" << join_strings(projection_columns(*projection))
                << "], rows=" << plan->runtime_rows_ << ")\n";
            render_explain_plan(projection->subplan_.get(), depth + 1, out);
            break;
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto join = static_cast<JoinPlan*>(plan);
            std::set<std::string> table_set;
            collect_tables(plan, table_set);
            std::vector<std::string> tables(table_set.begin(), table_set.end());
            out << "Join(tables=[" << join_strings(std::move(tables)) << "], condition=["
                << join_strings(condition_strings(*plan, join->conds_)) << "], rows=" << plan->runtime_rows_ << ")\n";
            render_explain_plan(join->left_.get(), depth + 1, out);
            render_explain_plan(join->right_.get(), depth + 1, out);
            break;
        }
        default:
            break;
        }
    }

    static void render_bound_explain_plan(const Plan* plan, const PlanLiteralOverlay* literals,
                                          const BoundPlan::RuntimeState& runtime, int depth, std::ostringstream& out) {
        const auto found = runtime.rows.find(plan);
        const size_t rows = found == runtime.rows.end() ? 0 : found->second;
        out << std::string(static_cast<size_t>(depth), '\t');
        switch (plan->tag) {
        case T_SeqScan: {
            const auto* scan = static_cast<const ScanPlan*>(plan);
            out << "Scan(table=" << scan->tab_name_ << ", type=SeqScan, rows=" << rows << ")\n";
            break;
        }
        case T_IndexScan:
        case T_IndexSkipScan: {
            const auto* scan = static_cast<const ScanPlan*>(plan);
            out << "Scan(table=" << scan->tab_name_
                << ", type=" << (plan->tag == T_IndexScan ? "IndexScan" : "IndexSkipScan") << ", using_index=("
                << scan->index_col_names_[0] << "), rows=" << rows << ")\n";
            break;
        }
        case T_Filter: {
            const auto* filter = static_cast<const FilterPlan*>(plan);
            out << "Filter(condition=[" << join_strings(condition_strings(*plan, filter->conds_, literals))
                << "], rows=" << rows << ")\n";
            render_bound_explain_plan(filter->subplan_.get(), literals, runtime, depth + 1, out);
            break;
        }
        case T_Projection: {
            const auto* projection = static_cast<const ProjectionPlan*>(plan);
            out << "Project(columns=[" << join_strings(projection_columns(*projection)) << "], rows=" << rows << ")\n";
            render_bound_explain_plan(projection->subplan_.get(), literals, runtime, depth + 1, out);
            break;
        }
        case T_NestLoop:
        case T_SortMerge: {
            const auto* join = static_cast<const JoinPlan*>(plan);
            std::set<std::string> table_set;
            collect_tables(plan, table_set);
            std::vector<std::string> tables(table_set.begin(), table_set.end());
            out << "Join(tables=[" << join_strings(std::move(tables)) << "], condition=["
                << join_strings(condition_strings(*plan, join->conds_, literals)) << "], rows=" << rows << ")\n";
            render_bound_explain_plan(join->left_.get(), literals, runtime, depth + 1, out);
            render_bound_explain_plan(join->right_.get(), literals, runtime, depth + 1, out);
            break;
        }
        default:
            break;
        }
    }

    static void append_to_context(const std::string& text, Context* context) {
        if (context == nullptr || context->data_send_ == nullptr || context->offset_ == nullptr) {
            return;
        }
        memcpy(context->data_send_ + *(context->offset_), text.c_str(), text.size());
        *(context->offset_) += static_cast<int>(text.size());
    }

    void write_explain_output(const std::string& text, Context* context) {
        append_to_context(text, context);
        if (sm_manager_->output_file_enabled_) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << text;
            outfile.close();
        }
    }

    static std::unique_ptr<AbstractExecutor> maybe_count(std::unique_ptr<AbstractExecutor> executor, Plan* plan,
                                                         bool count_rows) {
        if (!count_rows) {
            return executor;
        }
        return std::make_unique<CountingExecutor>(std::move(executor), &plan->runtime_rows_);
    }

    static std::unique_ptr<AbstractExecutor> maybe_count(std::unique_ptr<AbstractExecutor> executor, const Plan* plan,
                                                         bool count_rows, BoundPlan::RuntimeState* runtime) {
        if (!count_rows) {
            return executor;
        }
        return std::make_unique<CountingExecutor>(std::move(executor), &runtime->rows[plan]);
    }

public:
    explicit Portal(SmManager* sm_manager, compiled::ProgramTemplateCache* template_cache = nullptr)
        : sm_manager_(sm_manager),
          template_cache_(template_cache == nullptr ? &owned_template_cache_ : template_cache) {}
    ~Portal() {}

    bool run_prepared_select(const PreparedSelectDescriptor& descriptor, const parser::OwnedTokenStream& lexical,
                             QlManager* ql, Context* context) {
        auto root = descriptor.Instantiate(lexical, sm_manager_, context);
        if (root == nullptr) {
            return false;
        }
        ql->select_from(std::move(root), descriptor.output_names(), context);
        return true;
    }

    compiled::ProgramCacheStats point_program_cache_stats() const {
        return template_cache_->Stats();
    }

    // 将查询执行计划转换成对应的算子树
    std::unique_ptr<PortalStmt> start(std::unique_ptr<Plan> plan, Context* context) {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        switch (plan->tag) {
        case T_Help:
        case T_ShowTable:
        case T_ShowIndex:
        case T_DescTable:
        case T_Transaction_begin:
        case T_Transaction_commit:
        case T_Transaction_abort:
        case T_Transaction_rollback:
        case T_StaticCheckpoint:
            return std::make_unique<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_SetKnob:
        case T_SetTransaction:
        case T_SetOutputFile:
        case T_LoadData: {
            phase_metrics::ScopedSample metrics_sample(
                phase_metrics::Phase::PORTAL_INSTANTIATE,
                phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
            return std::make_unique<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        }
        case T_CreateTable:
        case T_DropTable:
        case T_CreateIndex:
        case T_DropIndex: {
            phase_metrics::ScopedSample metrics_sample(
                phase_metrics::Phase::PORTAL_INSTANTIATE,
                phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
            return std::make_unique<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        }
        case T_select:
        case T_ExplainAnalyze:
        case T_Update:
        case T_Delete:
        case T_Insert: {
            auto* x = static_cast<DMLPlan*>(plan.get());
            switch (x->tag) {
            case T_select: {
                phase_metrics::ScopedSample metrics_sample(
                    phase_metrics::Phase::PORTAL_INSTANTIATE,
                    phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                std::vector<std::string> output_names = get_plan_output_names(x->subplan_.get());
                auto compiled_select = build_compiled_select(*x, context);
                std::unique_ptr<AbstractExecutor> root;
                if (compiled_select == nullptr) {
                    root = convert_plan_executor(x->subplan_.get(), context, false, true);
                }
                auto portal_stmt = std::make_unique<PortalStmt>(PORTAL_ONE_SELECT, std::move(output_names),
                                                                std::move(root), std::move(plan));
                portal_stmt->compiled_template =
                    compiled_select == nullptr ? nullptr : compiled_select->compiled_template;
                portal_stmt->compiled_select = std::move(compiled_select);
                return portal_stmt;
            }
            case T_ExplainAnalyze: {
                phase_metrics::ScopedSample metrics_sample(
                    phase_metrics::Phase::PORTAL_INSTANTIATE,
                    phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                reset_runtime_rows(x->subplan_.get());
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), context, true);
                return std::make_unique<PortalStmt>(PORTAL_EXPLAIN_ANALYZE, std::vector<std::string>(), std::move(root),
                                                    std::move(plan));
            }

            case T_Update: {
                if (auto compiled_mutation = build_compiled_mutation(*x, context); compiled_mutation != nullptr) {
                    auto portal_stmt =
                        std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                     std::unique_ptr<AbstractExecutor>(), std::move(plan));
                    portal_stmt->compiled_template = compiled_mutation->compiled_template;
                    portal_stmt->compiled_mutation = std::move(compiled_mutation);
                    return portal_stmt;
                }
                std::vector<Rid> rids;
                const bool compiled_program = x->compiled_point_program_ != nullptr;
                auto point_rid = point_dml_enabled() ? resolve_point_rid(*x, context) : std::nullopt;
                if (point_rid.has_value()) {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::PORTAL_INSTANTIATE,
                        phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<UpdateExecutor>(sm_manager_, x->tab_name_, x->set_clauses_, x->conds_,
                                                         PointMutationTarget{*point_rid}, context, true);
                    return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                        std::move(root), std::move(plan));
                }
                std::unique_ptr<AbstractExecutor> scan;
                {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::PORTAL_INSTANTIATE,
                        phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                    if (compiled_program) {
                        // Duplicate/non-unique lookup or a visibility ambiguity
                        // falls back to the original scan semantics.
                        auto fallback_plan =
                            std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name_, x->conds_,
                                                       x->compiled_point_program_->index_col_names);
                        scan = convert_plan_executor(fallback_plan.get(), context, false, true);
                    } else {
                        scan = convert_plan_executor(x->subplan_.get(), context, false, true);
                    }
                }
                {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::EXECUTOR, phase_metrics::sample_rate(phase_metrics::Phase::EXECUTOR));
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                }
                phase_metrics::ScopedSample metrics_sample(
                    phase_metrics::Phase::PORTAL_INSTANTIATE,
                    phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                    sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }
            case T_Delete: {
                if (auto compiled_mutation = build_compiled_mutation(*x, context); compiled_mutation != nullptr) {
                    auto portal_stmt =
                        std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                     std::unique_ptr<AbstractExecutor>(), std::move(plan));
                    portal_stmt->compiled_template = compiled_mutation->compiled_template;
                    portal_stmt->compiled_mutation = std::move(compiled_mutation);
                    return portal_stmt;
                }
                std::vector<Rid> rids;
                const bool compiled_program = x->compiled_point_program_ != nullptr;
                auto point_rid = point_dml_enabled() ? resolve_point_rid(*x, context) : std::nullopt;
                if (point_rid.has_value()) {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::PORTAL_INSTANTIATE,
                        phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                    std::unique_ptr<AbstractExecutor> root = std::make_unique<DeleteExecutor>(
                        sm_manager_, x->tab_name_, x->conds_, PointMutationTarget{*point_rid}, context, true);
                    return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                        std::move(root), std::move(plan));
                }
                std::unique_ptr<AbstractExecutor> scan;
                {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::PORTAL_INSTANTIATE,
                        phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                    if (compiled_program) {
                        auto fallback_plan =
                            std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name_, x->conds_,
                                                       x->compiled_point_program_->index_col_names);
                        scan = convert_plan_executor(fallback_plan.get(), context, false, true);
                    } else {
                        scan = convert_plan_executor(x->subplan_.get(), context, false, true);
                    }
                }
                {
                    phase_metrics::ScopedSample metrics_sample(
                        phase_metrics::Phase::EXECUTOR, phase_metrics::sample_rate(phase_metrics::Phase::EXECUTOR));
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                }

                phase_metrics::ScopedSample metrics_sample(
                    phase_metrics::Phase::PORTAL_INSTANTIATE,
                    phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }

            case T_Insert: {
                if (auto compiled_mutation = build_compiled_mutation(*x, context); compiled_mutation != nullptr) {
                    auto portal_stmt =
                        std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                     std::unique_ptr<AbstractExecutor>(), std::move(plan));
                    portal_stmt->compiled_template = compiled_mutation->compiled_template;
                    portal_stmt->compiled_mutation = std::move(compiled_mutation);
                    return portal_stmt;
                }
                phase_metrics::ScopedSample metrics_sample(
                    phase_metrics::Phase::PORTAL_INSTANTIATE,
                    phase_metrics::sample_rate(phase_metrics::Phase::PORTAL_INSTANTIATE));
                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }

            default:
                throw InternalError("Unexpected field type");
                break;
            }
        }
        default:
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    std::unique_ptr<PortalStmt> start(BoundPlan plan, Context* context) {
        if (!plan) {
            return nullptr;
        }
        switch (plan->tag) {
        case T_Help:
        case T_ShowTable:
        case T_ShowIndex:
        case T_DescTable:
        case T_Transaction_begin:
        case T_Transaction_commit:
        case T_Transaction_abort:
        case T_Transaction_rollback:
        case T_SetKnob:
        case T_SetTransaction:
        case T_SetOutputFile:
        case T_LoadData:
        case T_StaticCheckpoint:
            return std::make_unique<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_CreateTable:
        case T_DropTable:
        case T_CreateIndex:
        case T_DropIndex:
            return std::make_unique<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        default:
            break;
        }
        const auto* dml = dynamic_cast<const DMLPlan*>(plan.get());
        if (dml == nullptr) {
            throw InternalError("cached plan is not a DML statement");
        }
        const auto* literals = plan.literals.get();
        switch (dml->tag) {
        case T_select: {
            std::vector<std::string> output_names = get_plan_output_names(dml->subplan_.get());
            auto root = convert_bound_plan_executor(dml->subplan_.get(), context, literals, plan.runtime.get());
            return std::make_unique<PortalStmt>(PORTAL_ONE_SELECT, std::move(output_names), std::move(root),
                                                std::move(plan));
        }
        case T_ExplainAnalyze: {
            auto root = convert_bound_plan_executor(dml->subplan_.get(), context, literals, plan.runtime.get(), true);
            return std::make_unique<PortalStmt>(PORTAL_EXPLAIN_ANALYZE, std::vector<std::string>(), std::move(root),
                                                std::move(plan));
        }
        case T_Insert: {
            std::vector<Value> values;
            values.reserve(dml->values_.size());
            for (const auto& value : dml->values_) {
                values.push_back(bind_value(value, literals));
            }
            auto root = std::make_unique<InsertExecutor>(sm_manager_, dml->tab_name_, std::move(values), context);
            return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(), std::move(root),
                                                std::move(plan));
        }
        case T_Update:
        case T_Delete: {
            const auto conditions = bind_conditions(dml->conds_, literals);
            std::vector<Rid> rids;
            auto scan = convert_bound_plan_executor(dml->subplan_.get(), context, literals, plan.runtime.get());
            for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                rids.push_back(scan->rid());
            }
            std::unique_ptr<AbstractExecutor> root;
            if (dml->tag == T_Delete) {
                root =
                    std::make_unique<DeleteExecutor>(sm_manager_, dml->tab_name_, conditions, std::move(rids), context);
            } else {
                root = std::make_unique<UpdateExecutor>(sm_manager_, dml->tab_name_,
                                                        bind_set_clauses(dml->set_clauses_, literals), conditions,
                                                        std::move(rids), context);
            }
            return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(), std::move(root),
                                                std::move(plan));
        }
        default:
            throw InternalError("cached plan kind is unsupported");
        }
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::unique_ptr<PortalStmt> portal, QlManager* ql, txn_id_t* txn_id, Context* context) {
        switch (portal->tag) {
        case PORTAL_ONE_SELECT: {
            auto run_legacy_select = [&]() {
                if (portal->root == nullptr && portal->plan != nullptr) {
                    auto* dml = static_cast<DMLPlan*>(portal->plan.get());
                    portal->root = convert_plan_executor(dml->subplan_.get(), context, false, true);
                }
                ql->select_from(std::move(portal->root), std::move(portal->output_names), context);
            };
            if (portal->compiled_select != nullptr && portal->plan != nullptr) {
                auto& spec = *portal->compiled_select;
                ResultSink sink(sm_manager_, context, spec.output_cols, spec.output_captions);
                spec.bindings.result_sink = &sink;
                DatabaseProgramRuntime runtime(sm_manager_, context, spec.bindings);
                std::string bind_error;
                auto frame = compiled::ParameterFrame::Bind(spec.program->parameters(), spec.parameters, &bind_error);
                if (frame.has_value()) {
                    const auto status = compiled::Interpret(*spec.program, *frame, &runtime);
                    if (status == compiled::ExecStatus::FALLBACK) {
                        record_template_fallback();
                        run_legacy_select();
                        break;
                    }
                    if (status == compiled::ExecStatus::ERROR && runtime.fallback_allowed() &&
                        !runtime.has_pending_exception()) {
                        record_template_fallback();
                        run_legacy_select();
                        break;
                    }
                    if (status != compiled::ExecStatus::OK) {
                        if (status == compiled::ExecStatus::NO_MATCH_RESULT) {
                            runtime.FinishResult();
                        } else {
                            runtime.RethrowPending();
                        }
                    } else {
                        runtime.FinishResult();
                    }
                    if (runtime.has_pending_exception()) {
                        runtime.RethrowPending();
                    }
                    record_template_handled();
                    break;
                }
            }
            if (portal->compiled_select != nullptr) {
                record_template_fallback();
            }
            run_legacy_select();
            break;
        }

        case PORTAL_EXPLAIN_ANALYZE: {
            for (portal->root->beginTuple(); !portal->root->is_end(); portal->root->nextTuple()) {
                (void)portal->root->Next();
            }
            std::ostringstream out;
            if (portal->bound_plan) {
                const auto& dml = static_cast<const DMLPlan&>(*portal->bound_plan);
                render_bound_explain_plan(dml.subplan_.get(), portal->bound_plan.literals.get(),
                                          *portal->bound_plan.runtime, 0, out);
            } else {
                auto* dml = static_cast<DMLPlan*>(portal->plan.get());
                render_explain_plan(dml->subplan_.get(), 0, out);
            }
            write_explain_output(out.str(), context);
            break;
        }

        case PORTAL_DML_WITHOUT_SELECT: {
            if (portal->compiled_mutation != nullptr && portal->plan != nullptr) {
                auto& spec = *portal->compiled_mutation;
                DatabaseProgramRuntime runtime(sm_manager_, context, spec.bindings);
                std::string bind_error;
                auto frame = compiled::ParameterFrame::Bind(spec.program->parameters(), spec.parameters, &bind_error);
                auto run_legacy_mutation = [&]() {
                    auto* dml = static_cast<DMLPlan*>(portal->plan.get());
                    if (dml->tag == T_Insert) {
                        auto root =
                            std::make_unique<InsertExecutor>(sm_manager_, dml->tab_name_, dml->values_, context);
                        ql->run_dml(std::move(root));
                        return;
                    }
                    auto scan = convert_plan_executor(dml->subplan_.get(), context, false, true);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                    if (dml->tag == T_Delete) {
                        auto root =
                            std::make_unique<DeleteExecutor>(sm_manager_, dml->tab_name_, dml->conds_, rids, context);
                        ql->run_dml(std::move(root));
                    } else if (dml->tag == T_Update) {
                        auto root = std::make_unique<UpdateExecutor>(sm_manager_, dml->tab_name_, dml->set_clauses_,
                                                                     dml->conds_, rids, context);
                        ql->run_dml(std::move(root));
                    }
                };
                if (frame.has_value()) {
                    const auto status = compiled::Interpret(*spec.program, *frame, &runtime);
                    if (status == compiled::ExecStatus::FALLBACK ||
                        (status == compiled::ExecStatus::ERROR && runtime.fallback_allowed() &&
                         !runtime.has_pending_exception())) {
                        record_template_fallback();
                        run_legacy_mutation();
                        break;
                    }
                    if (status == compiled::ExecStatus::TXN_ABORT || status == compiled::ExecStatus::ERROR) {
                        runtime.RethrowPending();
                    }
                    record_template_handled();
                    break;
                }
                record_template_fallback();
                run_legacy_mutation();
                break;
            }
            ql->run_dml(std::move(portal->root));
            break;
        }
        case PORTAL_MULTI_QUERY: {
            ql->run_mutli_query(portal->execution_plan(), context);
            break;
        }
        case PORTAL_CMD_UTILITY: {
            ql->run_cmd_utility(portal->execution_plan(), txn_id, context);
            break;
        }
        default: {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_bound_plan_executor(const Plan* plan, Context* context,
                                                                  const PlanLiteralOverlay* literals,
                                                                  BoundPlan::RuntimeState* runtime,
                                                                  bool count_rows = false) {
        switch (plan->tag) {
        case T_Projection: {
            const auto* projection = static_cast<const ProjectionPlan*>(plan);
            auto child =
                convert_bound_plan_executor(projection->subplan_.get(), context, literals, runtime, count_rows);
            std::unique_ptr<AbstractExecutor> executor;
            if (projection->preserve_col_names_) {
                std::vector<TabCol> cols;
                cols.reserve(projection->select_items_.size());
                for (const auto& item : projection->select_items_) {
                    cols.push_back(item.expr.col);
                }
                executor = std::make_unique<ProjectionExecutor>(std::move(child), cols);
            } else {
                executor = std::make_unique<ProjectionExecutor>(std::move(child),
                                                                to_executor_select_items(projection->select_items_));
            }
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_Filter: {
            const auto* filter = static_cast<const FilterPlan*>(plan);
            auto executor = std::make_unique<FilterExecutor>(
                convert_bound_plan_executor(filter->subplan_.get(), context, literals, runtime, count_rows),
                bind_conditions(filter->conds_, literals));
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_Aggregate: {
            const auto* aggregate = static_cast<const AggregatePlan*>(plan);
            auto having = aggregate->having_conds_;
            for (auto& condition : having) {
                if (condition.is_rhs_val) {
                    condition.rhs_val = bind_value(condition.rhs_val, literals);
                }
                if (condition.lhs.type == QueryExprType::VALUE) {
                    condition.lhs.value = bind_value(condition.lhs.value, literals);
                }
                if (!condition.is_rhs_val && condition.rhs_expr.type == QueryExprType::VALUE) {
                    condition.rhs_expr.value = bind_value(condition.rhs_expr.value, literals);
                }
            }
            auto executor = std::make_unique<AggregateExecutor>(
                convert_bound_plan_executor(aggregate->subplan_.get(), context, literals, runtime, count_rows),
                aggregate->group_by_cols_, aggregate->agg_exprs_, to_executor_having_conds(having), context);
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_SeqScan:
        case T_IndexSkipScan:
        case T_IndexScan: {
            const auto* scan = static_cast<const ScanPlan*>(plan);
            auto conditions = bind_conditions(scan->conds_, literals);
            std::unique_ptr<AbstractExecutor> executor;
            if (scan->tag == T_SeqScan) {
                executor =
                    std::make_unique<SeqScanExecutor>(sm_manager_, scan->tab_name_, std::move(conditions), context);
            } else if (scan->tag == T_IndexSkipScan) {
                executor = std::make_unique<IndexSkipScanExecutor>(sm_manager_, scan->tab_name_, std::move(conditions),
                                                                   scan->index_col_names_, context);
            } else {
                executor = std::make_unique<IndexScanExecutor>(
                    sm_manager_, scan->tab_name_, std::move(conditions), scan->index_col_names_, context,
                    scan->scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
            }
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_NestLoop:
        case T_SortMerge: {
            const auto* join = static_cast<const JoinPlan*>(plan);
            auto executor = std::make_unique<NestedLoopJoinExecutor>(
                convert_bound_plan_executor(join->left_.get(), context, literals, runtime, count_rows),
                convert_bound_plan_executor(join->right_.get(), context, literals, runtime, count_rows),
                bind_conditions(join->conds_, literals), join->inlj_left_col_, join->inlj_right_col_,
                join->inlj_index_col_name_);
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_Sort: {
            const auto* sort = static_cast<const SortPlan*>(plan);
            auto executor = std::make_unique<SortExecutor>(
                convert_bound_plan_executor(sort->subplan_.get(), context, literals, runtime, count_rows),
                bind_sort_output_names(*sort), sort->limit_);
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_Limit: {
            const auto* limit = static_cast<const LimitPlan*>(plan);
            auto executor = std::make_unique<LimitExecutor>(
                convert_bound_plan_executor(limit->subplan_.get(), context, literals, runtime, count_rows),
                static_cast<size_t>(limit->limit_));
            return maybe_count(std::move(executor), plan, count_rows, runtime);
        }
        case T_Union: {
            const auto* union_plan = static_cast<const UnionPlan*>(plan);
            std::vector<std::unique_ptr<AbstractExecutor>> branches;
            branches.reserve(union_plan->branches_.size());
            for (const auto& branch : union_plan->branches_) {
                branches.push_back(convert_bound_plan_executor(branch.get(), context, literals, runtime, count_rows));
            }
            return maybe_count(std::make_unique<UnionExecutor>(std::move(branches), union_plan->cols_), plan,
                               count_rows, runtime);
        }
        default:
            return nullptr;
        }
    }

    std::unique_ptr<AbstractExecutor> convert_plan_executor(Plan* plan, Context* context, bool count_rows = false,
                                                            bool consume_plan_data = false) {
        consume_plan_data = consume_plan_data && !count_rows;
        switch (plan->tag) {
        case T_Projection: {
            auto x = static_cast<ProjectionPlan*>(plan);
            std::unique_ptr<AbstractExecutor> subplan =
                convert_plan_executor(x->subplan_.get(), context, count_rows, consume_plan_data);
            std::unique_ptr<AbstractExecutor> executor;
            if (x->preserve_col_names_) {
                std::vector<TabCol> cols;
                cols.reserve(x->select_items_.size());
                for (const auto& item : x->select_items_) {
                    cols.push_back(item.expr.col);
                }
                executor = std::make_unique<ProjectionExecutor>(std::move(subplan), cols);
            } else {
                auto select_items = to_executor_select_items(x->select_items_);
                executor = std::make_unique<ProjectionExecutor>(std::move(subplan), select_items);
            }
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Filter: {
            auto x = static_cast<FilterPlan*>(plan);
            auto conds = consume_plan_data ? std::move(x->conds_) : x->conds_;
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<FilterExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows, consume_plan_data), std::move(conds));
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Aggregate: {
            auto x = static_cast<AggregatePlan*>(plan);
            auto having_conds = to_executor_having_conds(x->having_conds_);
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<AggregateExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows, consume_plan_data), x->group_by_cols_,
                x->agg_exprs_, having_conds, context);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_SeqScan:
        case T_IndexSkipScan:
        case T_IndexScan: {
            auto x = static_cast<ScanPlan*>(plan);
            auto tab_name = consume_plan_data ? std::move(x->tab_name_) : x->tab_name_;
            auto conds = consume_plan_data ? std::move(x->conds_) : x->conds_;
            auto index_col_names = consume_plan_data ? std::move(x->index_col_names_) : x->index_col_names_;
            std::unique_ptr<AbstractExecutor> executor;
            if (x->tag == T_SeqScan) {
                executor =
                    std::make_unique<SeqScanExecutor>(sm_manager_, std::move(tab_name), std::move(conds), context);
            } else if (x->tag == T_IndexSkipScan) {
                executor = std::make_unique<IndexSkipScanExecutor>(sm_manager_, std::move(tab_name), std::move(conds),
                                                                   std::move(index_col_names), context);
            } else {
                executor = std::make_unique<IndexScanExecutor>(
                    sm_manager_, std::move(tab_name), std::move(conds), std::move(index_col_names), context,
                    x->scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
            }
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto x = static_cast<JoinPlan*>(plan);
            std::unique_ptr<AbstractExecutor> left =
                convert_plan_executor(x->left_.get(), context, count_rows, consume_plan_data);
            std::unique_ptr<AbstractExecutor> right =
                convert_plan_executor(x->right_.get(), context, count_rows, consume_plan_data);
            auto conds = consume_plan_data ? std::move(x->conds_) : x->conds_;
            auto inlj_left_col = consume_plan_data ? std::move(x->inlj_left_col_) : x->inlj_left_col_;
            auto inlj_right_col = consume_plan_data ? std::move(x->inlj_right_col_) : x->inlj_right_col_;
            std::unique_ptr<AbstractExecutor> join = std::make_unique<NestedLoopJoinExecutor>(
                std::move(left), std::move(right), std::move(conds), std::move(inlj_left_col),
                std::move(inlj_right_col), x->inlj_index_col_name_);
            return maybe_count(std::move(join), plan, count_rows);
        }
        case T_Sort: {
            auto x = static_cast<SortPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<SortExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows, consume_plan_data),
                bind_sort_output_names(*x), x->limit_);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Limit: {
            auto x = static_cast<LimitPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<LimitExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows, consume_plan_data),
                static_cast<size_t>(x->limit_));
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Union: {
            auto x = static_cast<UnionPlan*>(plan);
            std::vector<std::unique_ptr<AbstractExecutor>> branches;
            branches.reserve(x->branches_.size());
            for (const auto& branch_plan : x->branches_) {
                branches.push_back(convert_plan_executor(branch_plan.get(), context, count_rows, consume_plan_data));
            }
            auto cols = consume_plan_data ? std::move(x->cols_) : x->cols_;
            return maybe_count(std::make_unique<UnionExecutor>(std::move(branches), std::move(cols)), plan, count_rows);
        }
        default:
            break;
        }
        return nullptr;
    }
};
