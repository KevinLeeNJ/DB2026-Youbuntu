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

#include "planner.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <sstream>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

namespace {

bool same_tab_col(const TabCol& lhs, const TabCol& rhs) {
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}
bool same_agg_expr(const AggExpr& lhs, const AggExpr& rhs) {
    return lhs.type == rhs.type && lhs.is_star == rhs.is_star && lhs.is_distinct == rhs.is_distinct &&
           lhs.display_name == rhs.display_name && (lhs.is_star || same_tab_col(lhs.col, rhs.col));
}

ScanPlan* find_order_preserving_scan(Plan* plan) {
    if (plan == nullptr) {
        return nullptr;
    }
    switch (plan->tag) {
    case T_Projection:
        return find_order_preserving_scan(static_cast<ProjectionPlan*>(plan)->subplan_.get());
    case T_Filter:
        return find_order_preserving_scan(static_cast<FilterPlan*>(plan)->subplan_.get());
    case T_IndexScan:
        return static_cast<ScanPlan*>(plan);
    default:
        return nullptr;
    }
}

bool has_equality_constraint(const ScanPlan& scan, const std::string& col_name) {
    return std::any_of(scan.conds_.begin(), scan.conds_.end(), [&](const Condition& cond) {
        return is_indexable_value_condition(cond) && cond.op == OP_EQ && cond.lhs_col.col_name == col_name;
    });
}

bool configure_index_order(Plan* plan, const std::vector<OrderByItem>& order_by_items) {
    if (order_by_items.size() != 1) {
        return false;
    }
    const auto& order = order_by_items.front();
    if (order.expr.type != QueryExprType::COLUMN) {
        return false;
    }

    ScanPlan* scan = find_order_preserving_scan(plan);
    if (scan == nullptr || scan->index_col_names_.empty()) {
        return false;
    }

    size_t order_pos = scan->index_col_names_.size();
    for (size_t i = 0; i < scan->index_col_names_.size(); ++i) {
        if (scan->index_col_names_[i] == order.expr.col.col_name) {
            order_pos = i;
            break;
        }
    }
    if (order_pos == scan->index_col_names_.size()) {
        return false;
    }
    for (size_t i = 0; i < order_pos; ++i) {
        if (!has_equality_constraint(*scan, scan->index_col_names_[i])) {
            return false;
        }
    }

    scan->scan_backward_ = order.is_desc;
    return true;
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
               lhs.agg.is_distinct == rhs.agg.is_distinct &&
               (lhs.agg.is_star || same_tab_col(lhs.agg.col, rhs.agg.col));
    }
    return false;
}
void attach_display_names(Plan* plan, const std::unordered_map<std::string, std::string>& table_name_to_display) {
    if (plan == nullptr) {
        return;
    }
    plan->table_name_to_display_ = table_name_to_display;
    switch (plan->tag) {
    case T_Filter:
        attach_display_names(static_cast<FilterPlan*>(plan)->subplan_.get(), table_name_to_display);
        break;
    case T_Projection:
        attach_display_names(static_cast<ProjectionPlan*>(plan)->subplan_.get(), table_name_to_display);
        break;
    case T_NestLoop: {
        auto* join = static_cast<JoinPlan*>(plan);
        attach_display_names(join->left_.get(), table_name_to_display);
        attach_display_names(join->right_.get(), table_name_to_display);
        break;
    }
    default:
        break;
    }
}

void append_agg_expr_if_needed(std::vector<AggExpr>& agg_exprs, const QueryExpr& expr) {
    if (expr.type != QueryExprType::AGGREGATE) {
        return;
    }
    auto pos = std::find_if(agg_exprs.begin(), agg_exprs.end(),
                            [&](const AggExpr& agg_expr) { return same_agg_expr(agg_expr, expr.agg); });
    if (pos == agg_exprs.end()) {
        agg_exprs.push_back(expr.agg);
    }
}

std::vector<AggExpr> collect_aggregate_exprs(const Query& query) {
    std::vector<AggExpr> agg_exprs;
    agg_exprs.reserve(query.select_items.size() + query.having_conds.size() * 2);

    for (const auto& item : query.select_items) {
        append_agg_expr_if_needed(agg_exprs, item.expr);
    }
    for (const auto& cond : query.having_conds) {
        append_agg_expr_if_needed(agg_exprs, cond.lhs);
        if (!cond.is_rhs_val) {
            append_agg_expr_if_needed(agg_exprs, cond.rhs_expr);
        }
    }
    return agg_exprs;
}

bool needs_aggregate_plan(const Query& query) {
    return query.has_aggregate || !query.group_by_cols.empty() || !query.having_conds.empty();
}

std::string get_select_item_output_name(const SelectItem& item) {
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
std::vector<OrderByItem> bind_order_by_output_names(const Query& query) {
    auto order_by_items = query.order_by_items;
    for (auto& item : order_by_items) {
        if (!item.order_name.empty()) {
            continue;
        }
        auto pos =
            std::find_if(query.select_items.begin(), query.select_items.end(),
                         [&](const SelectItem& select_item) { return same_query_expr(select_item.expr, item.expr); });
        if (pos != query.select_items.end()) {
            item.order_name = get_select_item_output_name(*pos);
        }
    }
    return order_by_items;
}

} // namespace

std::unique_ptr<Plan> Planner::generate_sort_plan(const Query* query, std::unique_ptr<Plan> plan) {
    if (query->order_by_items.empty()) {
        return plan;
    }
    if (configure_index_order(plan.get(), query->order_by_items)) {
        plan->order_satisfied_ = true;
        return plan;
    }
    // top-k must keep the rows discarded by OFFSET, otherwise nothing is left to skip over
    const bool parameterized_limit = query->limit_parameter_ordinal != 0 || query->offset_parameter_ordinal != 0;
    const long long top_k = static_cast<long long>(query->limit) + query->offset;
    int sort_limit = (query->has_limit && !parameterized_limit && top_k <= std::numeric_limits<int>::max())
                         ? static_cast<int>(top_k)
                         : -1;
    return std::make_unique<SortPlan>(T_Sort, std::move(plan), bind_order_by_output_names(*query), sort_limit);
}

std::unique_ptr<Plan> Planner::generate_limit_plan(const Query* query, std::unique_ptr<Plan> plan) {
    if (!query->has_limit) {
        return plan;
    }
    // A sort below already truncated the stream to limit rows, unless OFFSET still has to be skipped.
    const bool parameterized_limit = query->limit_parameter_ordinal != 0 || query->offset_parameter_ordinal != 0;
    if (!parameterized_limit && !query->order_by_items.empty() && !plan->order_satisfied_ && query->offset == 0) {
        return plan;
    }
    return std::make_unique<LimitPlan>(T_Limit, std::move(plan), query->limit, query->offset,
                                       query->limit_parameter_ordinal, query->offset_parameter_ordinal);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::unique_ptr<Plan> Planner::generate_select_plan(std::unique_ptr<Query> query, Context* context) {
    // 逻辑优化
    query = logical_optimization(std::move(query), context);

    // scan / join
    std::unique_ptr<Plan> plannerRoot = physical_optimization(query.get(), context);

    // aggregate / group by / having
    const bool has_aggregate_plan = needs_aggregate_plan(*query);
    if (has_aggregate_plan) {
        plannerRoot = std::make_unique<AggregatePlan>(T_Aggregate, std::move(plannerRoot), query->group_by_cols,
                                                      collect_aggregate_exprs(*query), query->having_conds);
    }

    // An ungrouped query may ORDER BY columns that are not projected, so its sort has to run below
    // the projection where those columns are still visible.
    if (!has_aggregate_plan) {
        plannerRoot = generate_sort_plan(query.get(), std::move(plannerRoot));
    }

    // final select projection
    const bool order_satisfied = plannerRoot->order_satisfied_;
    plannerRoot = std::make_unique<ProjectionPlan>(T_Projection, std::move(plannerRoot), query->select_items,
                                                   query->output_names, false, query->has_select_star);
    plannerRoot->order_satisfied_ = order_satisfied;
    attach_display_names(plannerRoot.get(), query->table_name_to_display);

    // final order by
    if (has_aggregate_plan) {
        plannerRoot = generate_sort_plan(query.get(), std::move(plannerRoot));
    }

    // final limit
    plannerRoot = generate_limit_plan(query.get(), std::move(plannerRoot));

    return plannerRoot;
}

std::unique_ptr<Plan> Planner::generate_union_plan(std::unique_ptr<Query> query, Context* context) {
    std::vector<std::unique_ptr<Plan>> branch_plans;
    branch_plans.reserve(query->union_branches.size());
    for (auto& branch_query : query->union_branches) {
        branch_plans.push_back(generate_select_plan(std::move(branch_query), context));
    }

    std::unique_ptr<Plan> plannerRoot =
        std::make_unique<UnionPlan>(T_Union, std::move(branch_plans), query->union_cols, query->output_names);

    if (!query->order_by_items.empty()) {
        plannerRoot = std::make_unique<SortPlan>(T_Sort, std::move(plannerRoot), query->order_by_items);
    }
    return plannerRoot;
}
