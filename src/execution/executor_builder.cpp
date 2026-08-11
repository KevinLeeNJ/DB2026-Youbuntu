/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution/executor_builder.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <utility>

#include "execution/executor_aggregate.h"
#include "execution/executor_filter.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_index_skip_scan.h"
#include "execution/executor_limit.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_union.h"
#include "execution/execution_sort.h"
#include "execution/parameter_frame.h"
#include "optimizer/plan.h"

namespace {

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
    const Plan* plan_;
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
    CountingExecutor(std::unique_ptr<AbstractExecutor> inner, const Plan& plan)
        : inner_(std::move(inner)), plan_(&plan) {
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

std::unique_ptr<AbstractExecutor> maybe_count(std::unique_ptr<AbstractExecutor> executor, const Plan& plan,
                                              bool count_rows) {
    return count_rows ? std::make_unique<CountingExecutor>(std::move(executor), plan) : std::move(executor);
}

ExecutorQueryExpr to_executor_query_expr(const QueryExpr& expr) {
    return {expr.type, expr.col, expr.agg, expr.value, expr.value, expr.display_name};
}

std::vector<ExecutorSelectItem> to_executor_select_items(const std::vector<SelectItem>& select_items) {
    std::vector<ExecutorSelectItem> executor_items;
    executor_items.reserve(select_items.size());
    for (const auto& item : select_items) {
        executor_items.push_back(
            {to_executor_query_expr(item.expr), item.alias,
             !item.output_name.empty() ? item.output_name : (!item.alias.empty() ? item.alias : item.expr.display_name),
             item.output_name});
    }
    return executor_items;
}

std::vector<ExecutorHavingCondition> to_executor_having_conds(const std::vector<HavingCondition>& having_conds) {
    std::vector<ExecutorHavingCondition> executor_conds;
    executor_conds.reserve(having_conds.size());
    for (const auto& cond : having_conds) {
        executor_conds.push_back({to_executor_query_expr(cond.lhs), cond.op, cond.is_rhs_val, cond.is_rhs_val,
                                  to_executor_query_expr(cond.rhs_expr), cond.rhs_val});
    }
    return executor_conds;
}

bool same_tab_col(const TabCol& lhs, const TabCol& rhs) {
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}

bool same_query_expr(const QueryExpr& lhs, const QueryExpr& rhs) {
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

std::string get_select_item_output_name(const SelectItem& item) {
    if (!item.output_name.empty())
        return item.output_name;
    if (!item.alias.empty())
        return item.alias;
    if (!item.expr.display_name.empty())
        return item.expr.display_name;
    return item.expr.type == QueryExprType::AGGREGATE ? item.expr.agg.display_name : item.expr.col.col_name;
}

std::vector<OrderByItem> bind_sort_output_names(const SortPlan& plan) {
    auto order_by_items = plan.order_by_items_;
    const auto* projection = dynamic_cast<const ProjectionPlan*>(plan.subplan_.get());
    if (projection == nullptr)
        return order_by_items;
    for (auto& item : order_by_items) {
        if (!item.order_name.empty())
            continue;
        auto pos =
            std::find_if(projection->select_items_.begin(), projection->select_items_.end(),
                         [&](const SelectItem& select_item) { return same_query_expr(select_item.expr, item.expr); });
        if (pos != projection->select_items_.end())
            item.order_name = get_select_item_output_name(*pos);
    }
    return order_by_items;
}

Value bind_prepared_value(const Value& value, const ParameterFrame& parameters,
                          std::optional<int> raw_length = std::nullopt) {
    if (value.parameter_ordinal == 0)
        return value;
    Value bound = parameters.bind(value.parameter_ordinal, value.type);
    if (raw_length.has_value() && !bound.is_null)
        bound.init_raw(*raw_length);
    return bound;
}

std::vector<Condition> bind_prepared_conditions(const std::vector<Condition>& conditions, SmManager& sm_manager,
                                                const ParameterFrame* parameters) {
    if (parameters == nullptr)
        return conditions;
    std::vector<Condition> bound = conditions;
    for (auto& condition : bound) {
        if (!condition.is_rhs_val || condition.rhs_val.parameter_ordinal == 0)
            continue;
        const auto column = sm_manager.db_.get_table(condition.lhs_col.tab_name).get_col(condition.lhs_col.col_name);
        condition.rhs_val = bind_prepared_value(condition.rhs_val, *parameters, column->len);
    }
    return bound;
}

QueryExpr bind_prepared_query_expr(const QueryExpr& expression, const ParameterFrame* parameters) {
    QueryExpr bound = expression;
    if (parameters != nullptr && bound.type == QueryExprType::VALUE && bound.value.parameter_ordinal != 0) {
        bound.value = bind_prepared_value(bound.value, *parameters);
    }
    return bound;
}

std::vector<SelectItem> bind_prepared_select_items(const std::vector<SelectItem>& items,
                                                   const ParameterFrame* parameters) {
    if (parameters == nullptr)
        return items;
    std::vector<SelectItem> bound = items;
    for (auto& item : bound)
        item.expr = bind_prepared_query_expr(item.expr, parameters);
    return bound;
}

std::vector<HavingCondition> bind_prepared_having_conds(const std::vector<HavingCondition>& conditions,
                                                        const ParameterFrame* parameters) {
    if (parameters == nullptr)
        return conditions;
    std::vector<HavingCondition> bound = conditions;
    for (auto& condition : bound) {
        condition.lhs = bind_prepared_query_expr(condition.lhs, parameters);
        condition.rhs_expr = bind_prepared_query_expr(condition.rhs_expr, parameters);
        if (condition.is_rhs_val)
            condition.rhs_val = bind_prepared_value(condition.rhs_val, *parameters);
    }
    return bound;
}

std::pair<size_t, size_t> bind_limit(const LimitPlan& plan, const ParameterFrame* parameters) {
    int runtime_limit = plan.limit_;
    int runtime_offset = plan.offset_;
    if (parameters != nullptr && plan.limit_parameter_ordinal_ != 0) {
        const Value value = parameters->bind(plan.limit_parameter_ordinal_, TYPE_INT);
        if (value.is_null || value.int_val < 0)
            throw RMDBError("LIMIT must be a non-NULL, non-negative INT32");
        runtime_limit = value.int_val;
    }
    if (parameters != nullptr && plan.offset_parameter_ordinal_ != 0) {
        const Value value = parameters->bind(plan.offset_parameter_ordinal_, TYPE_INT);
        if (value.is_null || value.int_val < 0)
            throw RMDBError("OFFSET must be a non-NULL, non-negative INT32");
        runtime_offset = value.int_val;
    }
    if (runtime_limit < 0 || runtime_offset < 0 || runtime_limit > std::numeric_limits<int>::max() - runtime_offset) {
        throw RMDBError("LIMIT plus OFFSET exceeds INT32 range");
    }
    return {static_cast<size_t>(runtime_limit), static_cast<size_t>(runtime_offset)};
}

} // namespace

std::unique_ptr<AbstractExecutor> BuildExecutorTree(const Plan& plan, SmManager& sm_manager, Context* context,
                                                    const ParameterFrame* parameters, bool count_rows) {
    switch (plan.tag) {
    case T_Projection: {
        const auto& x = static_cast<const ProjectionPlan&>(plan);
        auto child = BuildExecutorTree(*x.subplan_, sm_manager, context, parameters, count_rows);
        std::unique_ptr<AbstractExecutor> executor;
        if (x.preserve_col_names_) {
            std::vector<TabCol> cols;
            cols.reserve(x.select_items_.size());
            for (const auto& item : x.select_items_)
                cols.push_back(item.expr.col);
            executor = std::make_unique<ProjectionExecutor>(std::move(child), cols);
        } else {
            executor = std::make_unique<ProjectionExecutor>(
                std::move(child), to_executor_select_items(bind_prepared_select_items(x.select_items_, parameters)));
        }
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_Filter: {
        const auto& x = static_cast<const FilterPlan&>(plan);
        return maybe_count(std::make_unique<FilterExecutor>(
                               BuildExecutorTree(*x.subplan_, sm_manager, context, parameters, count_rows),
                               bind_prepared_conditions(x.conds_, sm_manager, parameters)),
                           plan, count_rows);
    }
    case T_Aggregate: {
        const auto& x = static_cast<const AggregatePlan&>(plan);
        return maybe_count(std::make_unique<AggregateExecutor>(
                               BuildExecutorTree(*x.subplan_, sm_manager, context, parameters, count_rows),
                               x.group_by_cols_, x.agg_exprs_,
                               to_executor_having_conds(bind_prepared_having_conds(x.having_conds_, parameters)),
                               context),
                           plan, count_rows);
    }
    case T_SeqScan:
    case T_IndexSkipScan:
    case T_IndexScan: {
        const auto& x = static_cast<const ScanPlan&>(plan);
        const auto conditions = bind_prepared_conditions(x.conds_, sm_manager, parameters);
        std::unique_ptr<AbstractExecutor> executor;
        if (x.tag == T_SeqScan) {
            executor = std::make_unique<SeqScanExecutor>(&sm_manager, x.tab_name_, conditions, context);
        } else if (x.tag == T_IndexSkipScan) {
            executor = std::make_unique<IndexSkipScanExecutor>(&sm_manager, x.tab_name_, conditions, x.index_col_names_,
                                                               context);
        } else {
            executor = std::make_unique<IndexScanExecutor>(
                &sm_manager, x.tab_name_, conditions, x.index_col_names_, context,
                x.scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
        }
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_NestLoop: {
        const auto& x = static_cast<const JoinPlan&>(plan);
        return maybe_count(std::make_unique<NestedLoopJoinExecutor>(
                               BuildExecutorTree(*x.left_, sm_manager, context, parameters, count_rows),
                               BuildExecutorTree(*x.right_, sm_manager, context, parameters, count_rows),
                               bind_prepared_conditions(x.conds_, sm_manager, parameters), x.inlj_left_col_,
                               x.inlj_right_col_, x.inlj_index_col_name_),
                           plan, count_rows);
    }
    case T_Sort: {
        const auto& x = static_cast<const SortPlan&>(plan);
        auto order_by = bind_sort_output_names(x);
        for (auto& item : order_by)
            item.expr = bind_prepared_query_expr(item.expr, parameters);
        return maybe_count(
            std::make_unique<SortExecutor>(BuildExecutorTree(*x.subplan_, sm_manager, context, parameters, count_rows),
                                           std::move(order_by), x.limit_),
            plan, count_rows);
    }
    case T_Limit: {
        const auto& x = static_cast<const LimitPlan&>(plan);
        const auto [limit, offset] = bind_limit(x, parameters);
        return maybe_count(
            std::make_unique<LimitExecutor>(BuildExecutorTree(*x.subplan_, sm_manager, context, parameters, count_rows),
                                            limit, offset),
            plan, count_rows);
    }
    case T_Union: {
        const auto& x = static_cast<const UnionPlan&>(plan);
        std::vector<std::unique_ptr<AbstractExecutor>> branches;
        branches.reserve(x.branches_.size());
        for (const auto& branch : x.branches_)
            branches.push_back(BuildExecutorTree(*branch, sm_manager, context, parameters, count_rows));
        return maybe_count(std::make_unique<UnionExecutor>(std::move(branches), x.cols_), plan, count_rows);
    }
    default:
        throw InternalError("unsupported query plan shape");
    }
}
