/* Copyright (c) 2023 Renmin University of China
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
#include <map>
#include <memory>
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
    return lhs.type == rhs.type && lhs.is_star == rhs.is_star && lhs.display_name == rhs.display_name &&
           (lhs.is_star || same_tab_col(lhs.col, rhs.col));
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

std::string condition_sort_key(const Condition& cond) {
    auto col_key = [](const TabCol& col) { return col.tab_name + "." + col.col_name; };
    std::string key = col_key(cond.lhs_col);
    switch (cond.op) {
    case OP_EQ:
        key += "=";
        break;
    case OP_NE:
        key += "<>";
        break;
    case OP_LT:
        key += "<";
        break;
    case OP_GT:
        key += ">";
        break;
    case OP_LE:
        key += "<=";
        break;
    case OP_GE:
        key += ">=";
        break;
    }
    if (cond.is_rhs_val) {
        if (!cond.rhs_display.empty()) {
            key += cond.rhs_display;
        } else {
            switch (cond.rhs_val.type) {
            case TYPE_INT:
                key += std::to_string(cond.rhs_val.int_val);
                break;
            case TYPE_FLOAT:
                key += std::to_string(cond.rhs_val.float_val);
                break;
            case TYPE_STRING:
                key += cond.rhs_val.str_val;
                break;
            }
        }
    } else {
        key += col_key(cond.rhs_col);
    }
    return key;
}

SelectItem make_column_select_item(const TabCol& col) {
    SelectItem item;
    item.expr.type = QueryExprType::COLUMN;
    item.expr.col = col;
    item.expr.display_name = col.col_name;
    return item;
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
    case T_NestLoop:
    case T_SortMerge: {
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

// Rebuild the right plan tree, replacing the SeqScan leaf with new_scan (IndexScan).
// The tree structure is: [Projection -> [Filter ->]] ScanPlan
std::unique_ptr<Plan> rebuild_right_plan_with_index(std::unique_ptr<Plan> plan, std::unique_ptr<Plan> new_scan) {
    if (plan->tag == T_SeqScan) {
        return new_scan;
    }
    if (plan->tag == T_Filter) {
        auto* filter = static_cast<FilterPlan*>(plan.get());
        auto rebuilt_sub = rebuild_right_plan_with_index(std::move(filter->subplan_), std::move(new_scan));
        return std::make_unique<FilterPlan>(T_Filter, std::move(rebuilt_sub), filter->conds_);
    }
    if (plan->tag == T_Projection) {
        auto* proj = static_cast<ProjectionPlan*>(plan.get());
        auto rebuilt_sub = rebuild_right_plan_with_index(std::move(proj->subplan_), std::move(new_scan));
        return std::make_unique<ProjectionPlan>(T_Projection, std::move(rebuilt_sub), proj->select_items_,
                                                proj->output_names_, proj->preserve_col_names_, proj->is_select_star_);
    }
    return plan;
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

// 使用最左匹配原则选择索引：等值前缀后最多接一个范围列，其他条件留给执行器过滤。
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                             std::vector<std::string>& index_col_names) {
    index_col_names.clear();
    if (curr_conds.empty()) {
        return false;
    }
    TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        return false;
    }

    for (auto& cond : curr_conds) {
        if (cond.lhs_col.tab_name != tab_name && !cond.is_rhs_val && cond.rhs_col.tab_name == tab_name) {
            std::swap(cond.lhs_col, cond.rhs_col);
            cond.op = swap_comp_op(cond.op);
        }
    }

    int best_index = -1;
    int best_prefix_len = 0;
    int best_condition_count = 0;
    std::vector<int> best_condition_order;

    for (size_t index_no = 0; index_no < tab.indexes.size(); ++index_no) {
        const auto& index = tab.indexes[index_no];
        std::vector<int> condition_order;
        bool used_range = false;
        int prefix_len = 0;

        for (const auto& index_col : index.cols) {
            std::vector<int> eq_conds;
            std::vector<int> range_conds;
            for (size_t cond_no = 0; cond_no < curr_conds.size(); ++cond_no) {
                const auto& cond = curr_conds[cond_no];
                if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name || cond.lhs_col.col_name != index_col.name ||
                    cond.op == OP_NE) {
                    continue;
                }
                if (cond.op == OP_EQ) {
                    eq_conds.push_back(static_cast<int>(cond_no));
                } else {
                    range_conds.push_back(static_cast<int>(cond_no));
                }
            }

            if (!eq_conds.empty() && !used_range) {
                condition_order.insert(condition_order.end(), eq_conds.begin(), eq_conds.end());
                ++prefix_len;
                continue;
            }
            if (!range_conds.empty() && !used_range) {
                condition_order.insert(condition_order.end(), range_conds.begin(), range_conds.end());
                ++prefix_len;
                used_range = true;
            }
            break;
        }

        if (prefix_len > best_prefix_len ||
            (prefix_len == best_prefix_len && static_cast<int>(condition_order.size()) > best_condition_count)) {
            best_index = static_cast<int>(index_no);
            best_prefix_len = prefix_len;
            best_condition_count = static_cast<int>(condition_order.size());
            best_condition_order = std::move(condition_order);
        }
    }

    if (best_index < 0 || best_prefix_len == 0) {
        return false;
    }

    const auto& best_meta = tab.indexes[best_index];
    for (const auto& col : best_meta.cols) {
        index_col_names.push_back(col.name);
    }

    std::vector<bool> used(curr_conds.size(), false);
    std::vector<Condition> reordered;
    reordered.reserve(curr_conds.size());
    for (int cond_no : best_condition_order) {
        if (!used[cond_no]) {
            reordered.push_back(curr_conds[cond_no]);
            used[cond_no] = true;
        }
    }
    for (size_t cond_no = 0; cond_no < curr_conds.size(); ++cond_no) {
        if (!used[cond_no]) {
            reordered.push_back(curr_conds[cond_no]);
        }
    }
    curr_conds = std::move(reordered);
    return true;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition>& conds, std::string tab_names) {
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    solved_conds.reserve(conds.size());
    auto it = conds.begin();
    while (it != conds.end()) {
        if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) ||
            (it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0)) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition* cond, Plan* plan) {
    switch (plan->tag) {
    case T_SeqScan:
    case T_IndexScan: {
        auto* x = static_cast<ScanPlan*>(plan);
        if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0) {
            return 2;
        } else {
            return 0;
        }
    }
    case T_NestLoop:
    case T_SortMerge: {
        auto* x = static_cast<JoinPlan*>(plan);
        int left_res = push_conds(cond, x->left_.get());
        // 条件已经下推到左子节点
        if (left_res == 3) {
            return 3;
        }
        int right_res = push_conds(cond, x->right_.get());
        // 条件已经下推到右子节点
        if (right_res == 3) {
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if (left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if (left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_comp_op(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    default:
        break;
    }
    return false;
}

std::unique_ptr<Plan> pop_scan(std::vector<int>& scantbl, std::string table, std::vector<std::string>& joined_tables,
                               std::vector<std::unique_ptr<Plan>>& plans) {
    for (size_t i = 0; i < plans.size(); i++) {
        auto* x = static_cast<ScanPlan*>(plans[i].get());
        if (x->tab_name_.compare(table) == 0) {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return std::move(plans[i]);
        }
    }
    return nullptr;
}

std::unique_ptr<Query> Planner::logical_optimization(std::unique_ptr<Query> query, Context* context) {
    (void)context;
    std::stable_sort(query->conds.begin(), query->conds.end(), [](const Condition& lhs, const Condition& rhs) {
        return condition_sort_key(lhs) < condition_sort_key(rhs);
    });

    return query;
}

std::unique_ptr<Plan> Planner::physical_optimization(Query* query, Context* context) {
    (void)context;
    std::map<std::string, std::vector<Condition>> table_filters;
    std::vector<Condition> join_conds;
    for (const auto& cond : query->conds) {
        if (cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name) {
            table_filters[cond.lhs_col.tab_name].push_back(cond);
        } else {
            join_conds.push_back(cond);
        }
    }

    std::map<std::string, std::set<TabCol>> needed_cols;
    if (query->tables.size() > 1 && !query->has_select_star && !needs_aggregate_plan(*query)) {
        for (const auto& item : query->select_items) {
            if (item.expr.type == QueryExprType::COLUMN) {
                needed_cols[item.expr.col.tab_name].insert(item.expr.col);
            }
        }
        for (const auto& cond : query->conds) {
            if (!cond.is_rhs_val) {
                needed_cols[cond.lhs_col.tab_name].insert(cond.lhs_col);
                needed_cols[cond.rhs_col.tab_name].insert(cond.rhs_col);
            }
        }
    }

    auto plan_tables = query->tables;

    std::vector<std::unique_ptr<Plan>> table_plans;
    table_plans.reserve(plan_tables.size());
    for (const auto& table : plan_tables) {
        std::vector<std::string> index_col_names;
        std::unique_ptr<Plan> table_plan =
            std::make_unique<ScanPlan>(T_SeqScan, sm_manager_, table, std::vector<Condition>(), index_col_names);

        auto filter_pos = table_filters.find(table);
        if (filter_pos != table_filters.end() && !filter_pos->second.empty()) {
            std::stable_sort(filter_pos->second.begin(), filter_pos->second.end(),
                             [](const Condition& lhs, const Condition& rhs) {
                                 return condition_sort_key(lhs) < condition_sort_key(rhs);
                             });
            table_plan = std::make_unique<FilterPlan>(T_Filter, std::move(table_plan), filter_pos->second);
        }

        auto needed_pos = needed_cols.find(table);
        if (needed_pos != needed_cols.end() && !needed_pos->second.empty()) {
            std::vector<SelectItem> pushdown_items;
            pushdown_items.reserve(needed_pos->second.size());
            for (const auto& col : needed_pos->second) {
                pushdown_items.push_back(make_column_select_item(col));
            }
            table_plan = std::make_unique<ProjectionPlan>(T_Projection, std::move(table_plan),
                                                          std::move(pushdown_items), std::vector<std::string>(), true);
        }

        table_plans.push_back(std::move(table_plan));
    }

    if (table_plans.empty()) {
        throw InternalError("SELECT has no table plan");
    }
    if (table_plans.size() == 1) {
        attach_display_names(table_plans[0].get(), query->table_name_to_display);
        return std::move(table_plans[0]);
    }

    std::unique_ptr<Plan> joined = std::move(table_plans[0]);
    std::set<std::string> joined_tables{plan_tables[0]};
    for (size_t i = 1; i < table_plans.size(); ++i) {
        const auto& next_table = plan_tables[i];
        std::vector<Condition> curr_join_conds;
        for (const auto& cond : join_conds) {
            bool lhs_joined = joined_tables.find(cond.lhs_col.tab_name) != joined_tables.end();
            bool rhs_joined = joined_tables.find(cond.rhs_col.tab_name) != joined_tables.end();
            if ((lhs_joined && cond.rhs_col.tab_name == next_table) ||
                (rhs_joined && cond.lhs_col.tab_name == next_table)) {
                curr_join_conds.push_back(cond);
            }
        }
        std::stable_sort(curr_join_conds.begin(), curr_join_conds.end(),
                         [](const Condition& lhs, const Condition& rhs) {
                             return condition_sort_key(lhs) < condition_sort_key(rhs);
                         });

        // INLJ detection: find if right table has index on a join column
        TabCol inlj_left_col;
        TabCol inlj_right_col;
        std::string inlj_index_col_name;
        if (!curr_join_conds.empty()) {
            TabMeta& next_tab = sm_manager_->db_.get_table(next_table);
            for (const auto& cond : curr_join_conds) {
                if (cond.op != OP_EQ || cond.is_rhs_val) {
                    continue;
                }
                // Identify which side is right table, which is left side
                TabCol right_col, left_col;
                if (cond.rhs_col.tab_name == next_table) {
                    right_col = cond.rhs_col;
                    left_col = cond.lhs_col;
                } else if (cond.lhs_col.tab_name == next_table) {
                    right_col = cond.lhs_col;
                    left_col = cond.rhs_col;
                } else {
                    continue;
                }
                // Check if left_col belongs to already-joined tables
                if (joined_tables.find(left_col.tab_name) == joined_tables.end()) {
                    continue;
                }
                // Check if right_col has an index on the right table
                if (next_tab.is_index({right_col.col_name})) {
                    inlj_right_col = right_col;
                    inlj_left_col = left_col;
                    inlj_index_col_name = right_col.col_name;
                    break; // only use the first matching index
                }
            }
        }

        std::unique_ptr<Plan> right_plan = std::move(table_plans[i]);
        if (!inlj_index_col_name.empty()) {
            // Replace right plan's SeqScan with IndexScan
            std::unique_ptr<Plan> new_scan =
                std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, next_table, std::vector<Condition>(),
                                           std::vector<std::string>{inlj_index_col_name});
            right_plan = rebuild_right_plan_with_index(std::move(right_plan), std::move(new_scan));
        }

        if (!query->is_explain_analyze && curr_join_conds.empty()) {
            joined = std::make_unique<JoinPlan>(T_NestLoop, std::move(right_plan), std::move(joined), curr_join_conds);
        } else {
            auto join_plan =
                std::make_unique<JoinPlan>(T_NestLoop, std::move(joined), std::move(right_plan), curr_join_conds);
            if (!inlj_index_col_name.empty()) {
                join_plan->inlj_left_col_ = inlj_left_col;
                join_plan->inlj_right_col_ = inlj_right_col;
                join_plan->inlj_index_col_name_ = inlj_index_col_name;
            }
            joined = std::move(join_plan);
        }
        joined_tables.insert(next_table);
    }

    attach_display_names(joined.get(), query->table_name_to_display);
    return joined;
}

std::unique_ptr<Plan> Planner::make_one_rel(Query* query) {
    std::vector<std::string> tables = query->tables;
    // // Scan table , 生成表算子列表tab_nodes
    std::vector<std::unique_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        if (index_exist == false) { // 该表没有索引
            index_col_names.clear();
            table_scan_executors[i] =
                std::make_unique<ScanPlan>(T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
        } else { // 存在索引
            table_scan_executors[i] =
                std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
    }
    // 只有一个表，不需要join。
    if (tables.size() == 1) {
        return std::move(table_scan_executors[0]);
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::unique_ptr<Plan> table_join_executors;

    std::vector<int> scantbl(tables.size(), -1);
    // 当前 AST 将 JOIN 条件统一放入 query->conds，这里按条件生成连接计划。
    if (conds.size() >= 1) {
        // 有连接条件

        // 根据连接条件，生成第一层join
        std::vector<std::string> joined_tables;
        joined_tables.reserve(tables.size());
        auto it = conds.begin();
        while (it != conds.end()) {
            std::unique_ptr<Plan> left, right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
            std::vector<Condition> join_conds{*it};
            // 建立join
            //  判断使用哪种join方式
            if (enable_nestedloop_join && enable_sortmerge_join) {
                // 默认nested loop join
                table_join_executors =
                    std::make_unique<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            } else if (enable_nestedloop_join) {
                table_join_executors =
                    std::make_unique<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            } else if (enable_sortmerge_join) {
                table_join_executors =
                    std::make_unique<JoinPlan>(T_SortMerge, std::move(left), std::move(right), join_conds);
            } else {
                // error
                throw RMDBError("No join executor selected!");
            }

            // table_join_executors = std::make_unique<JoinPlan>(T_NestLoop, std::move(left), std::move(right),
            // join_conds);
            it = conds.erase(it);
            break;
        }
        // 根据连接条件，生成第2-n层join
        it = conds.begin();
        while (it != conds.end()) {
            std::unique_ptr<Plan> left_need_to_join_executors = nullptr;
            std::unique_ptr<Plan> right_need_to_join_executors = nullptr;
            bool isneedreverse = false;
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
                left_need_to_join_executors =
                    pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            }
            if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
                right_need_to_join_executors =
                    pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
                isneedreverse = true;
            }

            if (left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr) {
                std::vector<Condition> join_conds{*it};
                std::unique_ptr<Plan> temp_join_executors =
                    std::make_unique<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                               std::move(right_need_to_join_executors), join_conds);
                table_join_executors =
                    std::make_unique<JoinPlan>(T_NestLoop, std::move(temp_join_executors),
                                               std::move(table_join_executors), std::vector<Condition>());
            } else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
                if (isneedreverse) {
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_comp_op(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{*it};
                table_join_executors = std::make_unique<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                                                  std::move(table_join_executors), join_conds);
            } else {
                push_conds(&(*it), table_join_executors.get());
            }
            it = conds.erase(it);
        }
    } else {
        table_join_executors = std::move(table_scan_executors[0]);
        scantbl[0] = 1;
    }

    // 连接剩余表
    for (size_t i = 0; i < tables.size(); i++) {
        if (scantbl[i] == -1) {
            table_join_executors =
                std::make_unique<JoinPlan>(T_NestLoop, std::move(table_scan_executors[i]),
                                           std::move(table_join_executors), std::vector<Condition>());
        }
    }

    return table_join_executors;
}

std::unique_ptr<Plan> Planner::generate_sort_plan(const Query* query, std::unique_ptr<Plan> plan) {
    if (query->order_by_items.empty()) {
        return plan;
    }
    int sort_limit = query->has_limit ? query->limit : -1;
    return std::make_unique<SortPlan>(T_Sort, std::move(plan), bind_order_by_output_names(*query), sort_limit);
}

std::unique_ptr<Plan> Planner::generate_limit_plan(const Query* query, std::unique_ptr<Plan> plan) {
    if (!query->has_limit) {
        return plan;
    }
    if (!query->order_by_items.empty()) {
        return plan;
    }
    return std::make_unique<LimitPlan>(T_Limit, std::move(plan), query->limit);
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
    if (needs_aggregate_plan(*query)) {
        plannerRoot = std::make_unique<AggregatePlan>(T_Aggregate, std::move(plannerRoot), query->group_by_cols,
                                                      collect_aggregate_exprs(*query), query->having_conds);
    }

    // final select projection
    plannerRoot = std::make_unique<ProjectionPlan>(T_Projection, std::move(plannerRoot), query->select_items,
                                                   query->output_names, false, query->has_select_star);
    attach_display_names(plannerRoot.get(), query->table_name_to_display);

    // final order by
    plannerRoot = generate_sort_plan(query.get(), std::move(plannerRoot));

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

// 生成DDL语句和DML语句的查询执行计划
std::unique_ptr<Plan> Planner::do_planner(std::unique_ptr<Query> query, Context* context) {
    std::unique_ptr<Plan> plannerRoot;
    auto* parse = query->parse.get();
    if (parse == nullptr) {
        throw InternalError("Unexpected null AST root");
    }
    switch (parse->type) {
    case ast::AstType::CreateTable: {
        auto x = static_cast<const ast::CreateTable*>(parse);
        // create table;
        std::vector<ColDef> col_defs;
        col_defs.reserve(x->fields.size());
        for (auto& field : x->fields) {
            if (field->type == ast::AstType::ColDef) {
                auto sv_col_def = static_cast<const ast::ColDef*>(field.get());
                ColDef col_def = {.name = sv_col_def->col_name,
                                  .type = interp_sv_type(sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_unique<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
        break;
    }
    case ast::AstType::DropTable: {
        auto x = static_cast<const ast::DropTable*>(parse);
        // drop table;
        plannerRoot =
            std::make_unique<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
        break;
    }
    case ast::AstType::CreateIndex: {
        auto x = static_cast<const ast::CreateIndex*>(parse);
        // create index;
        plannerRoot = std::make_unique<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
        break;
    }
    case ast::AstType::DropIndex: {
        auto x = static_cast<const ast::DropIndex*>(parse);
        // drop index
        plannerRoot = std::make_unique<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
        break;
    }
    case ast::AstType::InsertStmt: {
        auto x = static_cast<const ast::InsertStmt*>(parse);
        // insert;
        plannerRoot = std::make_unique<DMLPlan>(T_Insert, std::unique_ptr<Plan>(), x->tab_name, query->values,
                                                std::vector<Condition>(), std::vector<SetClause>());
        break;
    }
    case ast::AstType::DeleteStmt: {
        auto x = static_cast<const ast::DeleteStmt*>(parse);
        // delete;
        // 生成表扫描方式
        std::unique_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false) { // 该表没有索引
            index_col_names.clear();
            table_scan_executors =
                std::make_unique<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else { // 存在索引
            table_scan_executors =
                std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }

        plannerRoot = std::make_unique<DMLPlan>(T_Delete, std::move(table_scan_executors), x->tab_name,
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
        break;
    }
    case ast::AstType::UpdateStmt: {
        auto x = static_cast<const ast::UpdateStmt*>(parse);
        // update;
        // 生成表扫描方式
        std::unique_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false) { // 该表没有索引
            index_col_names.clear();
            table_scan_executors =
                std::make_unique<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else { // 存在索引
            table_scan_executors =
                std::make_unique<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        plannerRoot = std::make_unique<DMLPlan>(T_Update, std::move(table_scan_executors), x->tab_name,
                                                std::vector<Value>(), query->conds, query->set_clauses);
        break;
    }
    case ast::AstType::SelectStmt: {
        // 生成select语句的查询执行计划
        std::unique_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_unique<DMLPlan>(T_select, std::move(projection), std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
        break;
    }
    case ast::AstType::ExplainAnalyze: {
        std::unique_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot =
            std::make_unique<DMLPlan>(T_ExplainAnalyze, std::move(projection), std::string(), std::vector<Value>(),
                                      std::vector<Condition>(), std::vector<SetClause>());
        break;
    }
    case ast::AstType::SelectFromUnionStmt: {
        std::unique_ptr<Plan> union_plan = generate_union_plan(std::move(query), context);
        plannerRoot = std::make_unique<DMLPlan>(T_select, std::move(union_plan), std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
        break;
    }
    default:
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
