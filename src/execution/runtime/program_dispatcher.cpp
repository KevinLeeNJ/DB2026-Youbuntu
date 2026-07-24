/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/runtime/program_dispatcher.h"

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "compiled/bytecode_interpreter.h"
#include "common/phase_metrics.h"
#include "compiled/parameter_frame.h"
#include "execution/result_sink.h"
#include "execution/runtime/database_program_runtime.h"
#include "system/sm.h"
#ifdef RMDB_ENABLE_JIT
#include "jit/point_program_jit_manager.h"
#endif

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

std::optional<std::vector<compiled::ParameterValue>>
BindProgramParameters(const compiled::ProgramTemplate& program_template, const parser::OwnedTokenStream& lexical) {
    phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::PARAMETER_BIND,
                                               phase_metrics::sample_rate(phase_metrics::Phase::PARAMETER_BIND));
    std::vector<compiled::ParameterValue> values;
    values.reserve(program_template.program().parameters().size());
    for (uint32_t program_parameter = 0; program_parameter < program_template.program().parameters().size();
         ++program_parameter) {
        const auto* mapping = program_template.FindProgramParameter(program_parameter);
        if (mapping == nullptr || mapping->lexical_slot < 0 ||
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
    UpdateRuntimeInfo update_info{};
    DeleteRuntimeInfo delete_info{};
    InsertRuntimeInfo insert_info{};
    BoundPointProgramPtr bound_point_program;
};

struct BoundPointProgramCacheEntry {
    std::weak_ptr<const compiled::ProgramTemplate> owner;
    BoundPointProgramPtr bound;
};

std::mutex bound_point_program_cache_latch;
std::unordered_map<const compiled::ProgramTemplate*, BoundPointProgramCacheEntry> bound_point_program_cache;

BoundPointProgramPtr BuildBoundPointProgram(const compiled::ProgramTemplate& program_template, SmManager* sm_manager) {
    if (sm_manager == nullptr ||
        program_template.identity().catalog_generation != sm_manager->get_catalog_generation()) {
        return nullptr;
    }

    const auto& description = program_template.bindings();
    auto bound = std::make_shared<BoundPointProgram>();
    bound->owner = sm_manager;
    bound->catalog_generation = program_template.identity().catalog_generation;
    bound->table_name = description.table.table_name;
    bound->table = &sm_manager->db_.get_table(bound->table_name);
    bound->fh = sm_manager->fhs_.at(bound->table_name).get();

    if (program_template.identity().kind != compiled::ProgramKind::POINT_SELECT &&
        description.mutation_indexes.size() != bound->table->indexes.size()) {
        return nullptr;
    }

    bound->point_indexes.reserve(description.point_indexes.size());
    for (const auto& cached_index : description.point_indexes) {
        auto index_it = bound->table->get_index_meta(cached_index.column_names);
        if (index_it == bound->table->indexes.end() || cached_index.tuple_offsets.size() != index_it->cols.size()) {
            return nullptr;
        }
        bound->point_indexes.push_back({bound->table_name, cached_index.column_names, cached_index.tuple_offsets,
                                        cached_index.index_name, &*index_it});
    }

    bound->mutation_indexes.reserve(description.mutation_indexes.size());
    for (size_t i = 0; i < description.mutation_indexes.size(); ++i) {
        const auto& cached = description.mutation_indexes[i];
        const auto& live = bound->table->indexes.at(i);
        auto handle = sm_manager->ihs_.find(cached.index_name);
        if (handle == sm_manager->ihs_.end()) {
            return nullptr;
        }
        bound->mutation_indexes.push_back({&live, handle->second.get(), cached.index_name});
    }

    bound->output_cols.reserve(description.output_columns.size());
    bound->captions.reserve(description.output_columns.size());
    for (const auto& output : description.output_columns) {
        ColMeta col;
        col.tab_name = output.source.table_name;
        col.name = output.source.column_name;
        col.type = output.source.type == compiled::ValueType::INT32     ? TYPE_INT
                   : output.source.type == compiled::ValueType::FLOAT64 ? TYPE_FLOAT
                                                                        : TYPE_STRING;
        col.offset = static_cast<int>(output.source.offset);
        col.len = static_cast<int>(output.source.width);
        bound->output_cols.push_back(std::move(col));
        bound->captions.push_back(output.caption);
    }
    bound->affected_indexes = description.affected_mutation_indexes;

    if (program_template.identity().kind == compiled::ProgramKind::POINT_SELECT) {
        if (bound->point_indexes.empty() || bound->output_cols.empty()) {
            return nullptr;
        }
    } else if (program_template.identity().kind != compiled::ProgramKind::POINT_INSERT &&
               bound->point_indexes.empty()) {
        return nullptr;
    }

    return bound;
}

BoundPointProgramPtr GetBoundPointProgram(const std::shared_ptr<const compiled::ProgramTemplate>& program_template,
                                          SmManager* sm_manager) {
    if (program_template == nullptr || sm_manager == nullptr) {
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(bound_point_program_cache_latch);
    const auto key = program_template.get();
    auto it = bound_point_program_cache.find(key);
    if (it != bound_point_program_cache.end()) {
        const auto cached_owner = it->second.owner.lock();
        if (cached_owner == program_template && it->second.bound != nullptr && it->second.bound->owner == sm_manager &&
            it->second.bound->catalog_generation == sm_manager->get_catalog_generation()) {
            return it->second.bound;
        }
        if (cached_owner == nullptr || cached_owner != program_template) {
            bound_point_program_cache.erase(it);
        }
    }

    auto bound = BuildBoundPointProgram(*program_template, sm_manager);
    if (bound == nullptr) {
        return nullptr;
    }
    bound_point_program_cache[key] = BoundPointProgramCacheEntry{program_template, bound};
    if (bound_point_program_cache.size() > 512) {
        for (auto cache_it = bound_point_program_cache.begin(); cache_it != bound_point_program_cache.end();) {
            if (cache_it->second.owner.expired()) {
                cache_it = bound_point_program_cache.erase(cache_it);
            } else {
                ++cache_it;
            }
        }
    }
    return bound;
}

bool ValidateLiveBindings(const compiled::ProgramTemplate& program_template, const BoundPointProgramPtr& bound,
                          SmManager* sm_manager, DatabaseProgramBindings* bindings, std::vector<ColMeta>* output_cols,
                          std::vector<std::string>* captions, const parser::OwnedTokenStream& lexical,
                          RuntimeBindingState* state) {
    if (sm_manager == nullptr || bound == nullptr || bound->owner != sm_manager ||
        bound->catalog_generation != sm_manager->get_catalog_generation() ||
        program_template.identity().catalog_generation != bound->catalog_generation || bound->table == nullptr ||
        bound->fh == nullptr) {
        return false;
    }
    const auto& description = program_template.bindings();
    bindings->bound_point_program = bound;
    bindings->catalog_generation = bound->catalog_generation;
    state->bound_point_program = bound;
    state->conditions.reserve(description.conditions.size());
    state->set_clauses.reserve(description.set_clauses.size());
    state->bound_conditions.reserve(description.conditions.size());
    state->bound_set_clauses.reserve(description.set_clauses.size());
    *output_cols = bound->output_cols;
    *captions = bound->captions;
    state->table_name = bound->table_name;
    auto& table = *bound->table;
    auto* fh = bound->fh;
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
        return !bound->point_indexes.empty() && !output_cols->empty();
    }
    if (program_template.identity().kind == compiled::ProgramKind::POINT_INSERT) {
        state->insert_info = {sm_manager, &bound->table_name, bound->table, fh, &bound->mutation_indexes};
        bindings->insert_info = &state->insert_info;
        return true;
    }
    if (bound->point_indexes.empty()) {
        return false;
    }
    if (program_template.identity().kind == compiled::ProgramKind::POINT_DELETE) {
        state->delete_info = {sm_manager,
                              &bound->table_name,
                              bound->table,
                              fh,
                              &state->conditions,
                              &state->bound_conditions,
                              &bound->mutation_indexes};
        bindings->delete_info = &state->delete_info;
        return true;
    }
    state->bound_set_clauses = BindMutationSetClauses(table, state->set_clauses);
    state->update_info = {sm_manager,
                          &bound->table_name,
                          bound->table,
                          fh,
                          &state->conditions,
                          &state->bound_conditions,
                          &bound->mutation_indexes,
                          &state->set_clauses,
                          &state->bound_set_clauses,
                          &bound->affected_indexes};
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
        program_template =
            request.cache->LookupAny(request.lexical->key, request.statement_generation, request.planner_generation,
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
    BoundPointProgramPtr bound_point_program;
    bool valid_bindings = false;
    try {
        phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::PROGRAM_BIND,
                                                   phase_metrics::sample_rate(phase_metrics::Phase::PROGRAM_BIND));
        bound_point_program = GetBoundPointProgram(program_template, request.sm_manager);
        valid_bindings =
            values.has_value() && ValidateLiveBindings(*program_template, bound_point_program, request.sm_manager,
                                                       &bindings, &output_cols, &captions, *request.lexical, &state);
    } catch (...) {
        valid_bindings = false;
    }
    if (!valid_bindings) {
        request.cache->RecordFallback();
        return ProgramDispatchStatus::FALLBACK;
    }
    std::string bind_error;
    auto frame =
        compiled::ParameterFrame::Bind(program_template->program().parameters(), std::move(*values), &bind_error);
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
    compiled::ExecStatus status;
#ifdef RMDB_ENABLE_JIT
    const char* point_jit_flag = std::getenv(rmdb_config::kPointProgramJitEnv);
    const bool point_jit_enabled =
        point_jit_flag != nullptr && std::string(point_jit_flag) == "1" && request.point_jit_manager != nullptr;
    const auto& identity = program_template->identity();
    const bool identity_current = identity.token_shape == request.lexical->key &&
                                  identity.statement_generation == request.statement_generation &&
                                  identity.planner_generation == request.planner_generation &&
                                  identity.catalog_generation == request.sm_manager->get_catalog_generation();
    auto native_code =
        point_jit_enabled
            ? request.point_jit_manager->AcquireCurrent(program_template, rmdb_config::jit_mode, identity_current)
            : std::shared_ptr<const jit::JitCode>{};
    if (native_code != nullptr) {
        status = native_code->invoke_program(&runtime, &*frame);
        request.point_jit_manager->RecordNativeExecution();
    } else {
        const auto started = std::chrono::steady_clock::now();
        status = compiled::Interpret(program_template->program(), *frame, &runtime);
        const auto elapsed = std::chrono::steady_clock::now() - started;
        if (point_jit_enabled) {
            request.point_jit_manager->ObserveInterpreter(
                program_template,
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()),
                rmdb_config::jit_mode);
        }
    }
#else
    status = compiled::Interpret(program_template->program(), *frame, &runtime);
#endif
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
