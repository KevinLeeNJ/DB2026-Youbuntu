/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/runtime/program_dispatcher.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "compiled/bytecode_interpreter.h"
#include "compiled/parameter_frame.h"
#include "execution/result_sink.h"
#include "execution/runtime/database_program_runtime.h"
#include "system/sm.h"

namespace {

std::optional<compiled::ParameterValue> BindParameter(const compiled::LexicalParameterDesc& descriptor,
                                                      const parser::LexicalParam& parameter) {
    switch (descriptor.type) {
    case compiled::ValueType::INT32:
        if (parameter.type == parser::TokenType::VALUE_INT) {
            return compiled::ParameterValue::Int(static_cast<int32_t>(parameter.int_value));
        }
        if (parameter.type == parser::TokenType::VALUE_FLOAT && std::isfinite(parameter.float_value) &&
            parameter.float_value >= static_cast<double>(std::numeric_limits<int32_t>::min()) &&
            parameter.float_value <= static_cast<double>(std::numeric_limits<int32_t>::max())) {
            return compiled::ParameterValue::Int(static_cast<int32_t>(parameter.float_value));
        }
        return std::nullopt;
    case compiled::ValueType::FLOAT64:
        if (parameter.type == parser::TokenType::VALUE_INT) {
            return compiled::ParameterValue::Float(static_cast<double>(parameter.int_value));
        }
        if (parameter.type == parser::TokenType::VALUE_FLOAT) {
            return compiled::ParameterValue::Float(parameter.float_value);
        }
        return std::nullopt;
    case compiled::ValueType::BOOL:
        if (parameter.type == parser::TokenType::VALUE_BOOL) {
            return compiled::ParameterValue::Bool(parameter.bool_value);
        }
        return std::nullopt;
    case compiled::ValueType::BYTES:
        if (parameter.type == parser::TokenType::VALUE_STRING && parameter.text.size() <= descriptor.max_length) {
            return compiled::ParameterValue::Bytes(parameter.text);
        }
        return std::nullopt;
    default:
        return std::nullopt;
    }
}

std::optional<std::vector<compiled::ParameterValue>> BindProgramParameters(
    const compiled::ProgramTemplate& program_template, const parser::OwnedTokenStream& lexical) {
    std::vector<compiled::ParameterValue> values;
    values.reserve(program_template.program().parameters().size());
    for (uint32_t program_parameter = 0; program_parameter < program_template.program().parameters().size();
         ++program_parameter) {
        auto mapping = std::find_if(program_template.lexical_parameters().begin(),
                                    program_template.lexical_parameters().end(), [&](const auto& descriptor) {
                                        return descriptor.program_parameter == program_parameter;
                                    });
        if (mapping == program_template.lexical_parameters().end() || mapping->lexical_slot < 0 ||
            static_cast<size_t>(mapping->lexical_slot) >= lexical.parameters.size()) {
            return std::nullopt;
        }
        auto value = BindParameter(*mapping, lexical.parameters[static_cast<size_t>(mapping->lexical_slot)]);
        if (!value.has_value()) {
            return std::nullopt;
        }
        values.push_back(std::move(*value));
    }
    return values;
}

compiled::ValueType LiveType(ColType type) {
    if (type == TYPE_INT) {
        return compiled::ValueType::INT32;
    }
    if (type == TYPE_FLOAT) {
        return compiled::ValueType::FLOAT64;
    }
    return compiled::ValueType::BYTES;
}

CompOp LiveCompare(compiled::CompareOp op) {
    switch (op) {
    case compiled::CompareOp::EQ:
        return OP_EQ;
    case compiled::CompareOp::NE:
        return OP_NE;
    case compiled::CompareOp::LT:
        return OP_LT;
    case compiled::CompareOp::GT:
        return OP_GT;
    case compiled::CompareOp::LE:
        return OP_LE;
    case compiled::CompareOp::GE:
        return OP_GE;
    }
    throw InternalError("invalid cached comparison operator");
}

Value BindRuntimeValue(const compiled::LexicalParameterDesc& descriptor, const parser::LexicalParam& parameter,
                       ColType preferred_type = TYPE_STRING) {
    Value result;
    auto bound = BindParameter(descriptor, parameter);
    if (!bound.has_value()) {
        throw InternalError("cached lexical parameter type mismatch");
    }
    if (descriptor.type == compiled::ValueType::INT32) {
        result.set_int(bound->value().int_value);
    } else if (descriptor.type == compiled::ValueType::FLOAT64) {
        result.set_float(bound->value().float_value);
    } else if (descriptor.type == compiled::ValueType::BYTES) {
        result.set_str(bound->value().bytes);
        result.type = preferred_type == TYPE_DATETIME ? TYPE_DATETIME : TYPE_STRING;
    } else {
        throw InternalError("cached runtime parameter type is unsupported");
    }
    result.lexical_slot = descriptor.lexical_slot;
    return result;
}

struct RuntimeBindingState {
    std::string table_name;
    std::vector<Condition> conditions;
    std::vector<SetClause> set_clauses;
    std::vector<BoundMutationCondition> bound_conditions;
    std::vector<BoundMutationSetClause> bound_set_clauses;
    std::vector<RowMutationIndex> indexes;
    std::vector<bool> affected_indexes;
    UpdateRuntimeInfo update_info{};
    DeleteRuntimeInfo delete_info{};
    InsertRuntimeInfo insert_info{};
};

bool ValidateLiveBindings(const compiled::ProgramTemplate& program_template, SmManager* sm_manager,
                          DatabaseProgramBindings* bindings, std::vector<ColMeta>* output_cols,
                          std::vector<std::string>* captions, const parser::OwnedTokenStream& lexical,
                          RuntimeBindingState* state) {
    if (sm_manager == nullptr || program_template.identity().catalog_generation != sm_manager->get_catalog_generation()) {
        return false;
    }
    const auto& description = program_template.bindings();
    if (!sm_manager->db_.is_table(description.table.table_name)) {
        return false;
    }
    auto& table = sm_manager->db_.get_table(description.table.table_name);
    if (table.cols.size() != description.table.columns.size()) {
        return false;
    }
    for (size_t i = 0; i < table.cols.size(); ++i) {
        const auto& live = table.cols[i];
        const auto& cached = description.table.columns[i];
        if (live.name != cached.column_name || live.tab_name != cached.table_name || LiveType(live.type) != cached.type ||
            live.offset != static_cast<int>(cached.offset) || live.len != static_cast<int>(cached.width)) {
            return false;
        }
    }
    for (const auto& cached_index : description.point_indexes) {
        auto index = std::find_if(table.indexes.begin(), table.indexes.end(), [&](const IndexMeta& live) {
            if (live.cols.size() != cached_index.column_names.size()) {
                return false;
            }
            for (size_t i = 0; i < live.cols.size(); ++i) {
                if (live.cols[i].name != cached_index.column_names[i] ||
                    live.cols[i].offset != static_cast<int>(cached_index.tuple_offsets[i])) {
                    return false;
                }
            }
            return true;
        });
        if (index == table.indexes.end()) {
            return false;
        }
        const std::string live_name =
            sm_manager->get_ix_manager()->get_index_name(description.table.table_name, index->cols);
        if (live_name != cached_index.index_name || sm_manager->ihs_.find(live_name) == sm_manager->ihs_.end()) {
            return false;
        }
        bindings->point_indexes.push_back(
            {description.table.table_name, cached_index.column_names, cached_index.tuple_offsets});
    }
    state->table_name = description.table.table_name;
    if (program_template.identity().kind != compiled::ProgramKind::POINT_SELECT &&
        description.mutation_indexes.size() != table.indexes.size()) {
        return false;
    }
    for (size_t i = 0; i < description.mutation_indexes.size(); ++i) {
        const auto& cached = description.mutation_indexes[i];
        const auto& live = table.indexes[i];
        if (live.cols.size() != cached.column_names.size()) {
            return false;
        }
        for (size_t j = 0; j < live.cols.size(); ++j) {
            if (live.cols[j].name != cached.column_names[j] ||
                live.cols[j].offset != static_cast<int>(cached.tuple_offsets[j])) {
                return false;
            }
        }
        const std::string index_name = sm_manager->get_ix_manager()->get_index_name(state->table_name, live.cols);
        auto handle = sm_manager->ihs_.find(index_name);
        if (index_name != cached.index_name || handle == sm_manager->ihs_.end()) {
            return false;
        }
        state->indexes.push_back({&live, handle->second.get(), index_name});
    }
    for (const auto& output : description.output_columns) {
        ColMeta col;
        col.tab_name = output.source.table_name;
        col.name = output.source.column_name;
        col.type = output.source.type == compiled::ValueType::INT32     ? TYPE_INT
                   : output.source.type == compiled::ValueType::FLOAT64 ? TYPE_FLOAT
                                                                        : TYPE_STRING;
        col.offset = static_cast<int>(output.source.offset);
        col.len = static_cast<int>(output.source.width);
        output_cols->push_back(std::move(col));
        captions->push_back(output.caption);
    }
    bindings->catalog_generation = program_template.identity().catalog_generation;
    auto* fh = sm_manager->fhs_.at(state->table_name).get();
    for (const auto& cached : description.conditions) {
        Condition condition;
        condition.lhs_col = {cached.lhs.table_name, cached.lhs.column_name};
        condition.op = LiveCompare(cached.op);
        condition.is_rhs_val = cached.rhs_is_parameter;
        if (cached.rhs_is_parameter) {
            const auto* descriptor = program_template.FindLexicalParameter(cached.rhs_lexical_slot);
            if (descriptor == nullptr || static_cast<size_t>(cached.rhs_lexical_slot) >= lexical.parameters.size()) {
                return false;
            }
            condition.rhs_val =
                BindRuntimeValue(*descriptor, lexical.parameters[static_cast<size_t>(cached.rhs_lexical_slot)],
                                 table.get_col(cached.lhs.column_name)->type);
        } else {
            condition.rhs_col = {cached.rhs_column.table_name, cached.rhs_column.column_name};
        }
        state->conditions.push_back(std::move(condition));
    }
    for (const auto& cached : description.set_clauses) {
        SetClause set_clause;
        set_clause.lhs = {cached.lhs.table_name, cached.lhs.column_name};
        set_clause.is_self_ref = cached.rhs_is_column;
        if (cached.rhs_is_column) {
            set_clause.rhs_col = {cached.rhs_column.table_name, cached.rhs_column.column_name};
        }
        set_clause.op = cached.op == compiled::TemplateSetOp::ADD   ? UpdateOp::SELF_ADD
                        : cached.op == compiled::TemplateSetOp::SUB ? UpdateOp::SELF_SUB
                        : cached.op == compiled::TemplateSetOp::MUL ? UpdateOp::SELF_MUL
                        : cached.op == compiled::TemplateSetOp::DIV ? UpdateOp::SELF_DIV
                                                                   : UpdateOp::ASSIGNMENT;
        if (cached.rhs_lexical_slot >= 0) {
            const auto* descriptor = program_template.FindLexicalParameter(cached.rhs_lexical_slot);
            if (descriptor == nullptr || static_cast<size_t>(cached.rhs_lexical_slot) >= lexical.parameters.size()) {
                return false;
            }
            set_clause.rhs =
                BindRuntimeValue(*descriptor, lexical.parameters[static_cast<size_t>(cached.rhs_lexical_slot)],
                                 table.get_col(cached.lhs.column_name)->type);
        }
        state->set_clauses.push_back(std::move(set_clause));
    }
    state->bound_conditions = BindMutationConditions(table, state->conditions);
    if (program_template.identity().kind == compiled::ProgramKind::POINT_SELECT) {
        return !bindings->point_indexes.empty() && !output_cols->empty();
    }
    if (program_template.identity().kind == compiled::ProgramKind::POINT_INSERT) {
        state->insert_info = {sm_manager, &state->table_name, &table, fh, &state->indexes};
        bindings->insert_info = &state->insert_info;
        return true;
    }
    if (bindings->point_indexes.empty()) {
        return false;
    }
    if (program_template.identity().kind == compiled::ProgramKind::POINT_DELETE) {
        state->delete_info = {sm_manager, &state->table_name, &table, fh, &state->conditions,
                              &state->bound_conditions, &state->indexes};
        bindings->delete_info = &state->delete_info;
        return true;
    }
    state->bound_set_clauses = BindMutationSetClauses(table, state->set_clauses);
    state->affected_indexes = description.affected_mutation_indexes;
    state->update_info = {sm_manager, &state->table_name, &table, fh, &state->conditions,
                          &state->bound_conditions, &state->indexes, &state->set_clauses,
                          &state->bound_set_clauses, &state->affected_indexes};
    bindings->update_info = &state->update_info;
    return true;
}

} // namespace

ProgramDispatchStatus DispatchCachedPointProgram(const ProgramDispatchRequest& request) {
    if (request.cache == nullptr || request.lexical == nullptr || request.sm_manager == nullptr ||
        request.context == nullptr || !*request.lexical) {
        return ProgramDispatchStatus::MISS;
    }
    compiled::ProgramTemplatePtr program_template = request.program_template;
    if (program_template == nullptr) {
        program_template = request.cache->LookupAny(request.lexical->key, request.statement_generation,
                                                    request.planner_generation,
                                                    request.sm_manager->get_catalog_generation());
    }
    if (program_template == nullptr) {
        return ProgramDispatchStatus::MISS;
    }
    if (program_template->identity().kind != compiled::ProgramKind::POINT_SELECT &&
        (request.context->txn_ == nullptr ||
         request.context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED)) {
        request.cache->RecordFallback();
        return ProgramDispatchStatus::FALLBACK;
    }
    auto values = BindProgramParameters(*program_template, *request.lexical);
    DatabaseProgramBindings bindings;
    std::vector<ColMeta> output_cols;
    std::vector<std::string> captions;
    RuntimeBindingState state;
    bool valid_bindings = false;
    try {
        valid_bindings = values.has_value() &&
                         ValidateLiveBindings(*program_template, request.sm_manager, &bindings, &output_cols,
                                              &captions, *request.lexical, &state);
    } catch (...) {
        valid_bindings = false;
    }
    if (!valid_bindings) {
        request.cache->RecordFallback();
        return ProgramDispatchStatus::FALLBACK;
    }
    std::string bind_error;
    auto frame = compiled::ParameterFrame::Bind(program_template->program().parameters(), *values, &bind_error);
    if (!frame.has_value()) {
        request.cache->RecordFallback();
        return ProgramDispatchStatus::FALLBACK;
    }
    std::unique_ptr<ResultSink> sink;
    if (program_template->identity().kind == compiled::ProgramKind::POINT_SELECT) {
        sink = std::make_unique<ResultSink>(request.sm_manager, request.context, output_cols, captions);
        bindings.result_sink = sink.get();
    }
    DatabaseProgramRuntime runtime(request.sm_manager, request.context, std::move(bindings));
    const auto status = compiled::Interpret(program_template->program(), *frame, &runtime);
    if (status == compiled::ExecStatus::FALLBACK ||
        (status == compiled::ExecStatus::ERROR && runtime.fallback_allowed() && !runtime.has_pending_exception())) {
        request.cache->RecordFallback();
        return ProgramDispatchStatus::FALLBACK;
    }
    if (status == compiled::ExecStatus::TXN_ABORT || status == compiled::ExecStatus::ERROR) {
        runtime.RethrowPending();
    }
    if (program_template->identity().kind == compiled::ProgramKind::POINT_SELECT) {
        if (runtime.FinishResult() != compiled::ExecStatus::OK) {
            runtime.RethrowPending();
        }
    }
    request.cache->RecordHandled();
    return ProgramDispatchStatus::HANDLED;
}
