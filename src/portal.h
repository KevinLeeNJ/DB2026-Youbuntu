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
#include <fstream>
#include <iomanip>
#include <limits>
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
#include "execution/executor_insert.h"
#include "execution/executor_limit.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_union.h"
#include "execution/executor_update.h"
#include "execution/execution_sort.h"
#include "execution/parameter_frame.h"
#include "execution/prepared_plan_descriptor.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "record_printer.h" // BUFFER_LENGTH 截断语义与 RecordPrinter 共用

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

    PortalStmt(portalTag tag_, std::vector<std::string> output_names_, std::unique_ptr<AbstractExecutor> root_,
               std::unique_ptr<Plan> plan_)
        : tag(tag_), output_names(std::move(output_names_)), root(std::move(root_)), plan(std::move(plan_)) {}

    PortalStmt(portalTag tag_, std::vector<std::string> output_names_, std::unique_ptr<AbstractExecutor> root_)
        : tag(tag_), output_names(std::move(output_names_)), root(std::move(root_)) {}
};

class Portal {
private:
    SmManager* sm_manager_;

    static void write_point_key_part(char* dest, const Value& value, ColType type, int length) {
        memset(dest, 0, length);
        switch (type) {
        case TYPE_INT: {
            const int converted = value.type == TYPE_FLOAT ? static_cast<int>(value.float_val) : value.int_val;
            memcpy(dest, &converted, length);
            break;
        }
        case TYPE_FLOAT: {
            const float converted = value.type == TYPE_INT ? static_cast<float>(value.int_val) : value.float_val;
            write_float(dest, converted);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memcpy(dest, value.str_val.data(), std::min(static_cast<size_t>(length), value.str_val.size()));
            break;
        }
    }

    static void write_point_key_part(char* dest, const Value& value, const ColMeta& col) {
        write_point_key_part(dest, value, col.type, col.len);
    }

    // nullopt means the point path is not safe and the caller must build the
    // original scan executor. A value with no RID is a proven no-match result.
    std::optional<std::optional<Rid>> resolve_point_rid(const DMLPlan& plan, Context* context) const {
        if (!plan.point_access_.has_value()) {
            return std::nullopt;
        }
        if (context != nullptr) {
            const IsolationLevel isolation_level =
                context->txn_ == nullptr ? context->isolation_level_ : context->txn_->get_isolation_level();
            if (isolation_level != IsolationLevel::READ_COMMITTED) {
                return std::nullopt;
            }
        }

        const auto& path = *plan.point_access_;
        auto& tab = sm_manager_->db_.get_table(plan.tab_name_);
        auto index_it = tab.get_index_meta(path.index_cols);
        if (index_it == tab.indexes.end()) {
            return std::nullopt;
        }
        const IndexMeta& index = *index_it;
        const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(plan.tab_name_, index.cols);
        auto ih_it = sm_manager_->ihs_.find(index_name);
        if (ih_it == sm_manager_->ihs_.end()) {
            return std::nullopt;
        }

        std::vector<char> key(index.col_tot_len);
        int key_offset = 0;
        for (size_t i = 0; i < index.cols.size(); ++i) {
            const auto& condition = plan.conds_[path.condition_positions[i]];
            write_point_key_part(key.data() + key_offset, condition.rhs_val, index.cols[i]);
            key_offset += index.cols[i].len;
        }

        const auto lookup = ih_it->second->lookup_unique(key.data());
        if (lookup.status == UniqueLookupStatus::Duplicate) {
            return std::nullopt;
        }
        std::optional<Rid> point_rid;
        if (lookup.status == UniqueLookupStatus::Unique) {
            point_rid = lookup.rid;
        }
        if (context != nullptr && context->txn_ != nullptr && context->txn_mgr_ != nullptr &&
            sm_manager_->has_historical_index_keys(plan.tab_name_, index_name)) {
            std::optional<Rid> historical_rid;
            for (const Rid& candidate_rid :
                 sm_manager_->get_historical_index_key_rids(plan.tab_name_, index_name, key)) {
                if (!historical_rid.has_value()) {
                    historical_rid = candidate_rid;
                } else if (*historical_rid != candidate_rid) {
                    return std::nullopt;
                }
            }
            if (historical_rid.has_value()) {
                if (point_rid.has_value() && *point_rid != *historical_rid) {
                    return std::nullopt;
                }
                point_rid = historical_rid;
            }
        }
        return point_rid;
    }

    enum class PreparedPointUpdateResolutionKind {
        NoCandidate,
        NoVisible,
        Visible,
        Fallback,
    };

    struct PreparedPointUpdateResolution {
        PreparedPointUpdateResolutionKind kind = PreparedPointUpdateResolutionKind::Fallback;
        std::optional<Rid> rid;
        std::unique_ptr<RmRecord> visible_record;
    };

    PreparedPointUpdateResolution resolve_prepared_point_update(const PreparedUpdateExecutable& executable,
                                                                const std::vector<Condition>& conditions,
                                                                Context* context) const {
        if (!executable.point_update.has_value() || context == nullptr || context->txn_ == nullptr ||
            context->txn_mgr_ == nullptr || context->lock_mgr_ == nullptr ||
            context->txn_->get_isolation_level() != IsolationLevel::SNAPSHOT_ISOLATION) {
            return {};
        }
        const auto& point = *executable.point_update;
        if (point.index == nullptr || point.index_handle == nullptr || executable.scan.table_handle == nullptr ||
            point.key_length <= 0 || point.key_parts.size() != point.index->cols.size()) {
            return {};
        }

        std::vector<char> key(point.key_length);
        for (const auto& part : point.key_parts) {
            if (part.condition_index >= conditions.size() || part.key_offset < 0 || part.target_length <= 0 ||
                part.key_offset + part.target_length > point.key_length) {
                return {};
            }
            const Condition& condition = conditions[part.condition_index];
            if (!condition.is_rhs_val || condition.null_test != NullTest::NONE || condition.op != OP_EQ) {
                return {};
            }
            if (condition.rhs_val.is_null) {
                return {PreparedPointUpdateResolutionKind::NoCandidate, std::nullopt, nullptr};
            }
            write_point_key_part(key.data() + part.key_offset, condition.rhs_val, part.target_type, part.target_length);
        }

        std::vector<Rid> candidates;
        const auto lookup = point.index_handle->lookup_unique(key.data());
        if (lookup.status == UniqueLookupStatus::Duplicate) {
            return {};
        }
        if (lookup.status == UniqueLookupStatus::Unique) {
            candidates.push_back(lookup.rid);
        }
        for (const Rid& historical :
             sm_manager_->get_historical_index_key_rids(executable.scan.table_name, point.index_name, key)) {
            if (std::find(candidates.begin(), candidates.end(), historical) == candidates.end()) {
                candidates.push_back(historical);
            }
        }
        if (candidates.empty()) {
            return {PreparedPointUpdateResolutionKind::NoCandidate, std::nullopt, nullptr};
        }
        if (candidates.size() != 1) {
            return {};
        }

        RowMutationRuntimeInfo info{
            sm_manager_, &executable.scan.table_name,  executable.scan.table, executable.scan.table_handle,
            &conditions, &executable.bound_conditions, &executable.indexes};
        std::optional<Rid> target;
        std::unique_ptr<RmRecord> visible_record;
        for (const Rid& candidate : candidates) {
            auto visible = GetVisibleRecord(executable.scan.table_handle, candidate, context);
            if (visible == nullptr || !RowMutationEngine::MatchesTarget(*visible, info)) {
                continue;
            }
            if (target.has_value() && *target != candidate) {
                return {};
            }
            target = candidate;
            visible_record = std::move(visible);
        }
        if (!target.has_value()) {
            return {PreparedPointUpdateResolutionKind::NoVisible, std::nullopt, nullptr};
        }
        return {PreparedPointUpdateResolutionKind::Visible, target, std::move(visible_record)};
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
        Plan* plan_;
        bool counting_enabled_ = true;
        bool current_counted_ = false;

        void count_current_if_available() {
            current_counted_ = false;
            if (!counting_enabled_ || inner_->is_end() || !inner_->current()) {
                return;
            }
            ++plan_->runtime_rows_;
            current_counted_ = true;
        }

    public:
        CountingExecutor(std::unique_ptr<AbstractExecutor> inner, Plan* plan) {
            inner_ = std::move(inner);
            plan_ = plan;
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
                ++plan_->runtime_rows_;
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

        void set_lookup_key(const TabCol& target, const char* key, size_t len) override {
            inner_->set_lookup_key(target, key, len);
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

    Value bind_prepared_value(const Value& value, const ParameterFrame& parameters,
                              std::optional<int> raw_length = std::nullopt) const {
        if (value.parameter_ordinal == 0) {
            return value;
        }
        Value bound = parameters.bind(value.parameter_ordinal, value.type);
        if (raw_length.has_value() && !bound.is_null) {
            bound.init_raw(*raw_length);
        }
        return bound;
    }

    std::vector<Condition> bind_prepared_conditions(const std::vector<Condition>& conditions,
                                                    const ParameterFrame& parameters) const {
        std::vector<Condition> bound = conditions;
        for (auto& condition : bound) {
            if (!condition.is_rhs_val || condition.rhs_val.parameter_ordinal == 0) {
                continue;
            }
            auto& table = sm_manager_->db_.get_table(condition.lhs_col.tab_name);
            const auto column = table.get_col(condition.lhs_col.col_name);
            condition.rhs_val = bind_prepared_value(condition.rhs_val, parameters, column->len);
        }
        return bound;
    }

    static std::vector<Condition> bind_prepared_conditions(const std::vector<PreparedConditionBinding>& bindings,
                                                           const ParameterFrame& parameters) {
        std::vector<Condition> conditions;
        conditions.reserve(bindings.size());
        for (const auto& binding : bindings) {
            Condition condition = binding.condition;
            if (condition.is_rhs_val && condition.rhs_val.parameter_ordinal != 0) {
                condition.rhs_val = parameters.bind(condition.rhs_val.parameter_ordinal, condition.rhs_val.type);
                if (!condition.rhs_val.is_null) {
                    condition.rhs_val.init_raw(binding.rhs_raw_length);
                }
            }
            conditions.push_back(std::move(condition));
        }
        return conditions;
    }

    QueryExpr bind_prepared_query_expr(const QueryExpr& expression, const ParameterFrame& parameters) const {
        QueryExpr bound = expression;
        if (bound.type == QueryExprType::VALUE && bound.value.parameter_ordinal != 0) {
            bound.value = bind_prepared_value(bound.value, parameters);
        }
        return bound;
    }

    std::vector<SelectItem> bind_prepared_select_items(const std::vector<SelectItem>& items,
                                                       const ParameterFrame& parameters) const {
        std::vector<SelectItem> bound = items;
        for (auto& item : bound) {
            item.expr = bind_prepared_query_expr(item.expr, parameters);
        }
        return bound;
    }

    std::vector<Value> bind_prepared_insert_values(const DMLPlan& dml, const ParameterFrame& parameters) const {
        const auto& table = sm_manager_->db_.get_table(dml.tab_name_);
        if (dml.values_.size() != table.cols.size()) {
            throw InvalidValueCountError();
        }
        std::vector<Value> values;
        values.reserve(dml.values_.size());
        for (std::size_t i = 0; i < dml.values_.size(); ++i) {
            const auto& column = table.cols[i];
            Value value = dml.values_[i].parameter_ordinal == 0
                              ? dml.values_[i]
                              : parameters.bind(dml.values_[i].parameter_ordinal, column.type);
            if (!value.is_null) {
                if ((column.type == TYPE_STRING || column.type == TYPE_DATETIME) &&
                    value.str_val.size() > static_cast<std::size_t>(column.len)) {
                    throw StringOverflowError();
                }
                if (column.type == TYPE_FLOAT && !std::isfinite(value.float_val)) {
                    throw RMDBError("prepared FLOAT parameter must be finite");
                }
            } else {
                value.type = column.type;
            }
            values.push_back(std::move(value));
        }
        return values;
    }

    static std::vector<Value> bind_prepared_insert_values(const PreparedInsertExecutable& executable,
                                                          const ParameterFrame& parameters) {
        std::vector<Value> values;
        values.reserve(executable.values.size());
        for (const auto& binding : executable.values) {
            Value value = binding.parameter_ordinal == 0 ? binding.constant
                                                         : parameters.bind(binding.parameter_ordinal, binding.type);
            if (!value.is_null) {
                if ((binding.type == TYPE_STRING || binding.type == TYPE_DATETIME) &&
                    value.str_val.size() > static_cast<std::size_t>(binding.length)) {
                    throw StringOverflowError();
                }
                if (binding.type == TYPE_FLOAT && !std::isfinite(value.float_val)) {
                    throw RMDBError("prepared FLOAT parameter must be finite");
                }
            } else {
                value.type = binding.type;
            }
            values.push_back(std::move(value));
        }
        return values;
    }

    std::vector<SetClause> bind_prepared_set_clauses(const DMLPlan& dml, const ParameterFrame& parameters) const {
        auto& table = sm_manager_->db_.get_table(dml.tab_name_);
        std::vector<SetClause> clauses = dml.set_clauses_;
        for (auto& clause : clauses) {
            const auto column = table.get_col(clause.lhs.col_name);
            if (clause.rhs.parameter_ordinal != 0) {
                clause.rhs = parameters.bind(clause.rhs.parameter_ordinal, clause.rhs.type);
            }
            for (auto& term : clause.additional_terms) {
                if (term.rhs.parameter_ordinal != 0) {
                    term.rhs = parameters.bind(term.rhs.parameter_ordinal, term.rhs.type);
                }
            }
            const auto validate = [&](const Value& value) {
                if (value.is_null) {
                    return;
                }
                if ((column->type == TYPE_STRING || column->type == TYPE_DATETIME) &&
                    value.str_val.size() > static_cast<std::size_t>(column->len)) {
                    throw StringOverflowError();
                }
                if (value.type == TYPE_FLOAT && !std::isfinite(value.float_val)) {
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

    static std::vector<SetClause> bind_prepared_set_clauses(const std::vector<PreparedSetClauseBinding>& bindings,
                                                            const ParameterFrame& parameters) {
        std::vector<SetClause> clauses;
        clauses.reserve(bindings.size());
        for (const auto& binding : bindings) {
            SetClause clause = binding.clause;
            if (clause.rhs.parameter_ordinal != 0) {
                clause.rhs = parameters.bind(clause.rhs.parameter_ordinal, clause.rhs.type);
            }
            for (auto& term : clause.additional_terms) {
                if (term.rhs.parameter_ordinal != 0) {
                    term.rhs = parameters.bind(term.rhs.parameter_ordinal, term.rhs.type);
                }
            }
            const auto validate = [&](const Value& value) {
                if (value.is_null) {
                    return;
                }
                if ((binding.target_type == TYPE_STRING || binding.target_type == TYPE_DATETIME) &&
                    value.str_val.size() > static_cast<std::size_t>(binding.target_length)) {
                    throw StringOverflowError();
                }
                if (value.type == TYPE_FLOAT && !std::isfinite(value.float_val)) {
                    throw RMDBError("prepared FLOAT parameter must be finite");
                }
            };
            validate(clause.rhs);
            for (const auto& term : clause.additional_terms) {
                validate(term.rhs);
            }
            clauses.push_back(std::move(clause));
        }
        return clauses;
    }

    std::unique_ptr<AbstractExecutor> make_prepared_scan_executor(const PreparedScanExecutable& scan,
                                                                  const ParameterFrame& parameters, Context* context) {
        auto conditions = bind_prepared_conditions(scan.conditions, parameters);
        std::unique_ptr<AbstractExecutor> executor;
        if (!scan.uses_index) {
            executor = std::make_unique<SeqScanExecutor>(scan.sm_manager, scan.table_name, *scan.table,
                                                         scan.table_handle, std::move(conditions), context);
        } else {
            executor = std::make_unique<IndexScanExecutor>(
                scan.sm_manager, scan.table_name, *scan.table, scan.table_handle, *scan.index, scan.index_handle,
                scan.index_name, std::move(conditions), context,
                scan.scan_backward ? ScanDirection::Backward : ScanDirection::Forward);
        }
        return executor;
    }

    std::unique_ptr<AbstractExecutor> make_prepared_select_executor(const PreparedSelectExecutable& executable,
                                                                    const ParameterFrame& parameters,
                                                                    Context* context) {
        auto root = make_prepared_scan_executor(executable.scan, parameters, context);
        for (const auto& layer : executable.layers) {
            switch (layer.kind) {
            case PreparedSelectLayerKind::Filter:
                root = std::make_unique<FilterExecutor>(std::move(root),
                                                        bind_prepared_conditions(layer.conditions, parameters));
                break;
            case PreparedSelectLayerKind::Projection:
                root = std::make_unique<ProjectionExecutor>(std::move(root), layer.projection_ordinals,
                                                            layer.projection_names);
                break;
            case PreparedSelectLayerKind::Limit: {
                int runtime_limit = layer.limit;
                int runtime_offset = layer.offset;
                if (layer.limit_parameter_ordinal != 0) {
                    const Value value = parameters.bind(layer.limit_parameter_ordinal, TYPE_INT);
                    if (value.is_null || value.int_val < 0) {
                        throw RMDBError("LIMIT must be a non-NULL, non-negative INT32");
                    }
                    runtime_limit = value.int_val;
                }
                if (layer.offset_parameter_ordinal != 0) {
                    const Value value = parameters.bind(layer.offset_parameter_ordinal, TYPE_INT);
                    if (value.is_null || value.int_val < 0) {
                        throw RMDBError("OFFSET must be a non-NULL, non-negative INT32");
                    }
                    runtime_offset = value.int_val;
                }
                if (runtime_limit < 0 || runtime_offset < 0 ||
                    runtime_limit > std::numeric_limits<int>::max() - runtime_offset) {
                    throw RMDBError("LIMIT plus OFFSET exceeds INT32 range");
                }
                root = std::make_unique<LimitExecutor>(std::move(root), static_cast<std::size_t>(runtime_limit),
                                                       static_cast<std::size_t>(runtime_offset));
                break;
            }
            }
        }
        return root;
    }

    std::unique_ptr<AbstractExecutor> convert_prepared_plan_executor(const Plan* plan, const ParameterFrame& parameters,
                                                                     Context* context) {
        switch (plan->tag) {
        case T_Projection: {
            const auto* projection = static_cast<const ProjectionPlan*>(plan);
            auto child = convert_prepared_plan_executor(projection->subplan_.get(), parameters, context);
            std::unique_ptr<AbstractExecutor> executor;
            if (projection->preserve_col_names_) {
                std::vector<TabCol> columns;
                columns.reserve(projection->select_items_.size());
                for (const auto& item : projection->select_items_) {
                    columns.push_back(item.expr.col);
                }
                executor = std::make_unique<ProjectionExecutor>(std::move(child), columns);
            } else {
                executor = std::make_unique<ProjectionExecutor>(
                    std::move(child),
                    to_executor_select_items(bind_prepared_select_items(projection->select_items_, parameters)));
            }
            return executor;
        }
        case T_Filter: {
            const auto* filter = static_cast<const FilterPlan*>(plan);
            auto executor = std::make_unique<FilterExecutor>(
                convert_prepared_plan_executor(filter->subplan_.get(), parameters, context),
                bind_prepared_conditions(filter->conds_, parameters));
            return executor;
        }
        case T_SeqScan:
        case T_IndexScan: {
            const auto* scan = static_cast<const ScanPlan*>(plan);
            auto conditions = bind_prepared_conditions(scan->conds_, parameters);
            std::unique_ptr<AbstractExecutor> executor;
            if (scan->tag == T_SeqScan) {
                executor =
                    std::make_unique<SeqScanExecutor>(sm_manager_, scan->tab_name_, std::move(conditions), context);
            } else {
                executor = std::make_unique<IndexScanExecutor>(
                    sm_manager_, scan->tab_name_, std::move(conditions), scan->index_col_names_, context,
                    scan->scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
            }
            return executor;
        }
        case T_Sort: {
            const auto* sort = static_cast<const SortPlan*>(plan);
            auto order_by = bind_sort_output_names(*sort);
            for (auto& item : order_by) {
                item.expr = bind_prepared_query_expr(item.expr, parameters);
            }
            auto executor = std::make_unique<SortExecutor>(
                convert_prepared_plan_executor(sort->subplan_.get(), parameters, context), std::move(order_by),
                sort->limit_);
            return executor;
        }
        case T_Limit: {
            const auto* limit = static_cast<const LimitPlan*>(plan);
            int runtime_limit = limit->limit_;
            int runtime_offset = limit->offset_;
            if (limit->limit_parameter_ordinal_ != 0) {
                const Value value = parameters.bind(limit->limit_parameter_ordinal_, TYPE_INT);
                if (value.is_null || value.int_val < 0) {
                    throw RMDBError("LIMIT must be a non-NULL, non-negative INT32");
                }
                runtime_limit = value.int_val;
            }
            if (limit->offset_parameter_ordinal_ != 0) {
                const Value value = parameters.bind(limit->offset_parameter_ordinal_, TYPE_INT);
                if (value.is_null || value.int_val < 0) {
                    throw RMDBError("OFFSET must be a non-NULL, non-negative INT32");
                }
                runtime_offset = value.int_val;
            }
            if (runtime_limit < 0 || runtime_offset < 0 ||
                runtime_limit > std::numeric_limits<int>::max() - runtime_offset) {
                throw RMDBError("LIMIT plus OFFSET exceeds INT32 range");
            }
            auto executor = std::make_unique<LimitExecutor>(
                convert_prepared_plan_executor(limit->subplan_.get(), parameters, context),
                static_cast<std::size_t>(runtime_limit), static_cast<std::size_t>(runtime_offset));
            return executor;
        }
        default:
            throw InternalError("unsupported prepared plan shape");
        }
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
        case T_NestLoop: {
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

    static void collect_tables(Plan* plan, std::set<std::string>& tables) {
        switch (plan->tag) {
        case T_SeqScan:
        case T_IndexSkipScan:
        case T_IndexScan:
            tables.insert(static_cast<ScanPlan*>(plan)->tab_name_);
            break;
        case T_Filter:
            collect_tables(static_cast<FilterPlan*>(plan)->subplan_.get(), tables);
            break;
        case T_Projection:
            collect_tables(static_cast<ProjectionPlan*>(plan)->subplan_.get(), tables);
            break;
        case T_NestLoop: {
            auto join = static_cast<JoinPlan*>(plan);
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
        case T_NestLoop: {
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
        case T_NestLoop: {
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

    // data_send_ 是固定 BUFFER_LENGTH 字节的发送缓冲，写满必须截断。语义与
    // RecordPrinter 完全一致：尾部保留 RECORD_COUNT_LENGTH 给后续的
    // "Total record(s)" 行，写不下时置 ellipsis_ 让客户端看到 "... ..." 标记。
    // （宽表的 EXPLAIN ANALYZE 计划树可以轻松超过 8 KB，这里少一个长度判断就是
    //   一次堆越界写：连接线程静默死亡、服务端日志无任何记录。）
    static void append_to_context(const std::string& text, Context* context) {
        if (context == nullptr || context->data_send_ == nullptr || context->offset_ == nullptr) {
            return;
        }
        const int offset = *(context->offset_);
        const int remaining = static_cast<int>(BUFFER_LENGTH) - RECORD_COUNT_LENGTH - offset;
        if (remaining <= 0) {
            context->ellipsis_ = true;
            return;
        }
        const int written = std::min(static_cast<int>(text.size()), remaining);
        memcpy(context->data_send_ + offset, text.c_str(), static_cast<size_t>(written));
        *(context->offset_) = offset + written;
        if (written < static_cast<int>(text.size())) {
            context->ellipsis_ = true;
        }
    }

    void write_explain_output(const std::string& text, Context* context) {
        append_to_context(text, context);
        const bool output_file_enabled = context != nullptr && context->output_file_enabled_ != nullptr
                                             ? *context->output_file_enabled_
                                             : sm_manager_->output_file_enabled_;
        if (output_file_enabled) {
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
        return std::make_unique<CountingExecutor>(std::move(executor), plan);
    }

public:
    Portal(SmManager* sm_manager) : sm_manager_(sm_manager) {}
    ~Portal() {}

    std::pair<std::vector<std::string>, std::vector<ColMeta>> inspect_select_plan(Plan* plan, Context* context) {
        if (plan == nullptr || plan->tag != T_select) {
            throw InternalError("prepared SELECT inspection requires a SELECT plan");
        }
        auto* select = static_cast<DMLPlan*>(plan);
        auto root = convert_plan_executor(select->subplan_.get(), context);
        return {get_plan_output_names(select->subplan_.get()), root->cols()};
    }

    std::unique_ptr<PortalStmt> start_prepared(const PreparedPlanDescriptor& descriptor,
                                               const ParameterFrame& parameters, Context* context) {
        if (!descriptor.eligible() || descriptor.dml_plan() == nullptr) {
            throw InternalError("prepared descriptor is not eligible for execution");
        }
        if (parameters.size() != descriptor.parameter_layout().size()) {
            throw RMDBError("prepared parameter count does not match descriptor");
        }
        const auto* dml = descriptor.dml_plan();
        switch (descriptor.statement_kind()) {
        case PreparedStatementKind::Select: {
            if (const auto* executable = descriptor.select_executable();
                executable != nullptr && executable->scan.sm_manager == sm_manager_ &&
                sm_manager_->get_catalog_generation() == descriptor.catalog_generation()) {
                auto root = make_prepared_select_executor(*executable, parameters, context);
                return std::make_unique<PortalStmt>(PORTAL_ONE_SELECT, descriptor.output_names(), std::move(root));
            }
            auto root = convert_prepared_plan_executor(dml->subplan_.get(), parameters, context);
            return std::make_unique<PortalStmt>(PORTAL_ONE_SELECT, descriptor.output_names(), std::move(root));
        }
        case PreparedStatementKind::Insert: {
            if (const auto* executable = descriptor.insert_executable();
                executable != nullptr && executable->sm_manager == sm_manager_ &&
                sm_manager_->get_catalog_generation() == descriptor.catalog_generation()) {
                auto root = std::make_unique<InsertExecutor>(
                    *executable, bind_prepared_insert_values(*executable, parameters), context);
                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>{},
                                                    std::move(root));
            }
            auto root = std::make_unique<InsertExecutor>(sm_manager_, dml->tab_name_,
                                                         bind_prepared_insert_values(*dml, parameters), context);
            return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>{}, std::move(root));
        }
        case PreparedStatementKind::Update: {
            if (const auto* executable = descriptor.update_executable();
                executable != nullptr && executable->scan.sm_manager == sm_manager_ &&
                descriptor.database_identity() == sm_manager_->get_database_identity_under_catalog_guard() &&
                sm_manager_->get_catalog_generation() == descriptor.catalog_generation()) {
                auto conditions = bind_prepared_conditions(executable->conditions, parameters);
                auto point = resolve_prepared_point_update(*executable, conditions, context);
                if (point.kind != PreparedPointUpdateResolutionKind::Fallback) {
                    auto root = std::make_unique<UpdateExecutor>(
                        *executable, bind_prepared_set_clauses(executable->set_clauses, parameters),
                        std::move(conditions), PointMutationTarget{point.rid}, std::move(point.visible_record),
                        context);
                    return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>{},
                                                        std::move(root));
                }
                auto scan = make_prepared_scan_executor(executable->scan, parameters, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                auto root = std::make_unique<UpdateExecutor>(
                    *executable, bind_prepared_set_clauses(executable->set_clauses, parameters), std::move(conditions),
                    std::move(rids), context);
                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>{},
                                                    std::move(root));
            }
            if (dml->compiled_point_program_ != nullptr || dml->subplan_ == nullptr) {
                throw InternalError("compiled point UPDATE is not eligible for prepared runtime");
            }
            auto scan = convert_prepared_plan_executor(dml->subplan_.get(), parameters, context);
            std::vector<Rid> rids;
            for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                rids.push_back(scan->rid());
            }
            auto root = std::make_unique<UpdateExecutor>(
                sm_manager_, dml->tab_name_, bind_prepared_set_clauses(*dml, parameters),
                bind_prepared_conditions(dml->conds_, parameters), std::move(rids), context);
            return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>{}, std::move(root));
        }
        case PreparedStatementKind::Unsupported:
            break;
        }
        throw InternalError("unsupported prepared statement kind");
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
        case T_SetTransaction:
        case T_SetOutputFile:
        case T_LoadData:
            return std::make_unique<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_CreateTable:
        case T_DropTable:
        case T_CreateIndex:
        case T_DropIndex:
            return std::make_unique<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_select:
        case T_ExplainAnalyze:
        case T_Update:
        case T_Delete:
        case T_Insert: {
            auto* x = static_cast<DMLPlan*>(plan.get());
            switch (x->tag) {
            case T_select: {
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), context);
                std::vector<std::string> output_names = get_plan_output_names(x->subplan_.get());
                return std::make_unique<PortalStmt>(PORTAL_ONE_SELECT, std::move(output_names), std::move(root),
                                                    std::move(plan));
            }
            case T_ExplainAnalyze: {
                reset_runtime_rows(x->subplan_.get());
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), context, true);
                return std::make_unique<PortalStmt>(PORTAL_EXPLAIN_ANALYZE, std::vector<std::string>(), std::move(root),
                                                    std::move(plan));
            }

            case T_Update: {
                std::vector<Rid> rids;
                const bool compiled_program = x->compiled_point_program_ != nullptr;
                auto point_rid = resolve_point_rid(*x, context);
                if (point_rid.has_value()) {
                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<UpdateExecutor>(sm_manager_, x->tab_name_, x->set_clauses_, x->conds_,
                                                         PointMutationTarget{*point_rid}, context, true);
                    return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                        std::move(root), std::move(plan));
                }
                std::unique_ptr<AbstractExecutor> scan;
                if (compiled_program) {
                    // Duplicate/non-unique lookup or a visibility ambiguity
                    // falls back to the original scan semantics.
                    auto fallback_plan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name_, x->conds_,
                                                                    x->compiled_point_program_->index_col_names);
                    scan = convert_plan_executor(fallback_plan.get(), context);
                } else {
                    scan = convert_plan_executor(x->subplan_.get(), context);
                }
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                    sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }
            case T_Delete: {
                std::vector<Rid> rids;
                const bool compiled_program = x->compiled_point_program_ != nullptr;
                auto point_rid = resolve_point_rid(*x, context);
                if (point_rid.has_value()) {
                    std::unique_ptr<AbstractExecutor> root = std::make_unique<DeleteExecutor>(
                        sm_manager_, x->tab_name_, x->conds_, PointMutationTarget{*point_rid}, context, true);
                    return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                        std::move(root), std::move(plan));
                }
                std::unique_ptr<AbstractExecutor> scan;
                if (compiled_program) {
                    auto fallback_plan = std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name_, x->conds_,
                                                                    x->compiled_point_program_->index_col_names);
                    scan = convert_plan_executor(fallback_plan.get(), context);
                } else {
                    scan = convert_plan_executor(x->subplan_.get(), context);
                }
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }

                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }

            case T_Insert: {
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

    // 遍历算子树并执行算子生成执行结果
    void run(std::unique_ptr<PortalStmt> portal, QlManager* ql, txn_id_t* txn_id, Context* context) {
        switch (portal->tag) {
        case PORTAL_ONE_SELECT: {
            ql->select_from(std::move(portal->root), std::move(portal->output_names), context);
            break;
        }

        case PORTAL_EXPLAIN_ANALYZE: {
            for (portal->root->beginTuple(); !portal->root->is_end(); portal->root->nextTuple()) {
                (void)portal->root->Next();
            }
            auto* dml = static_cast<DMLPlan*>(portal->plan.get());
            std::ostringstream out;
            render_explain_plan(dml->subplan_.get(), 0, out);
            write_explain_output(out.str(), context);
            break;
        }

        case PORTAL_DML_WITHOUT_SELECT: {
            ql->run_dml(std::move(portal->root));
            break;
        }
        case PORTAL_MULTI_QUERY: {
            ql->run_mutli_query(portal->plan.get(), context);
            break;
        }
        case PORTAL_CMD_UTILITY: {
            ql->run_cmd_utility(portal->plan.get(), txn_id, context);
            break;
        }
        default: {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(Plan* plan, Context* context, bool count_rows = false) {
        switch (plan->tag) {
        case T_Projection: {
            auto x = static_cast<ProjectionPlan*>(plan);
            std::unique_ptr<AbstractExecutor> subplan = convert_plan_executor(x->subplan_.get(), context, count_rows);
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
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<FilterExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows), x->conds_);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Aggregate: {
            auto x = static_cast<AggregatePlan*>(plan);
            auto having_conds = to_executor_having_conds(x->having_conds_);
            std::unique_ptr<AbstractExecutor> executor =
                std::make_unique<AggregateExecutor>(convert_plan_executor(x->subplan_.get(), context, count_rows),
                                                    x->group_by_cols_, x->agg_exprs_, having_conds, context);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_SeqScan:
        case T_IndexSkipScan:
        case T_IndexScan: {
            auto x = static_cast<ScanPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor;
            if (x->tag == T_SeqScan) {
                executor = std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            } else if (x->tag == T_IndexSkipScan) {
                executor = std::make_unique<IndexSkipScanExecutor>(sm_manager_, x->tab_name_, x->conds_,
                                                                   x->index_col_names_, context);
            } else {
                executor = std::make_unique<IndexScanExecutor>(
                    sm_manager_, x->tab_name_, x->conds_, x->index_col_names_, context,
                    x->scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
            }
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_NestLoop: {
            auto x = static_cast<JoinPlan*>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_.get(), context, count_rows);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_.get(), context, count_rows);
            std::unique_ptr<AbstractExecutor> join = std::make_unique<NestedLoopJoinExecutor>(
                std::move(left), std::move(right), x->conds_, x->inlj_left_col_, x->inlj_right_col_,
                x->inlj_index_col_name_);
            return maybe_count(std::move(join), plan, count_rows);
        }
        case T_Sort: {
            auto x = static_cast<SortPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<SortExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows), bind_sort_output_names(*x), x->limit_);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Limit: {
            auto x = static_cast<LimitPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor =
                std::make_unique<LimitExecutor>(convert_plan_executor(x->subplan_.get(), context, count_rows),
                                                static_cast<size_t>(x->limit_), static_cast<size_t>(x->offset_));
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Union: {
            auto x = static_cast<UnionPlan*>(plan);
            std::vector<std::unique_ptr<AbstractExecutor>> branches;
            branches.reserve(x->branches_.size());
            for (const auto& branch_plan : x->branches_) {
                branches.push_back(convert_plan_executor(branch_plan.get(), context, count_rows));
            }
            auto executor = std::make_unique<UnionExecutor>(std::move(branches), x->cols_);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        default:
            break;
        }
        return nullptr;
    }
};
