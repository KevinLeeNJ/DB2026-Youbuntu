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
#include <memory>

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
        auto pos = std::find_if(query.select_items.begin(), query.select_items.end(), [&](const SelectItem& select_item) {
            return same_query_expr(select_item.expr, item.expr);
        });
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

int push_conds(Condition* cond, std::shared_ptr<Plan> plan) {
    switch (plan->tag) {
    case T_SeqScan:
    case T_IndexScan: {
        auto x = std::static_pointer_cast<ScanPlan>(plan);
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
        auto x = std::static_pointer_cast<JoinPlan>(plan);
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if (left_res == 3) {
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
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

std::shared_ptr<Plan> pop_scan(std::vector<int>& scantbl, std::string table, std::vector<std::string>& joined_tables,
                               std::vector<std::shared_ptr<Plan>> plans) {
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::static_pointer_cast<ScanPlan>(plans[i]);
        if (x->tab_name_.compare(table) == 0) {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context* context) {

    // TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context* context) {
    std::shared_ptr<Plan> plan = make_one_rel(query);

    // 其他物理优化

    return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query) {
    auto x = std::static_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;
    // // Scan table , 生成表算子列表tab_nodes
    std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        if (index_exist == false) { // 该表没有索引
            index_col_names.clear();
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tables[i], curr_conds, index_col_names);
        } else { // 存在索引
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tables[i], curr_conds, index_col_names);
        }
    }
    // 只有一个表，不需要join。
    if (tables.size() == 1) {
        return table_scan_executors[0];
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::shared_ptr<Plan> table_join_executors;

    std::vector<int> scantbl(tables.size(), -1);
    // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
    if (conds.size() >= 1) {
        // 有连接条件

        // 根据连接条件，生成第一层join
        std::vector<std::string> joined_tables;
        joined_tables.reserve(tables.size());
        auto it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left, right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
            std::vector<Condition> join_conds{*it};
            // 建立join
            //  判断使用哪种join方式
            if (enable_nestedloop_join && enable_sortmerge_join) {
                // 默认nested loop join
                table_join_executors =
                    std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            } else if (enable_nestedloop_join) {
                table_join_executors =
                    std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            } else if (enable_sortmerge_join) {
                table_join_executors =
                    std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right), join_conds);
            } else {
                // error
                throw RMDBError("No join executor selected!");
            }

            // table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right),
            // join_conds);
            it = conds.erase(it);
            break;
        }
        // 根据连接条件，生成第2-n层join
        it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
            std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
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
                std::shared_ptr<Plan> temp_join_executors =
                    std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                               std::move(right_need_to_join_executors), join_conds);
                table_join_executors =
                    std::make_shared<JoinPlan>(T_NestLoop, std::move(temp_join_executors),
                                               std::move(table_join_executors), std::vector<Condition>());
            } else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
                if (isneedreverse) {
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_comp_op(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{*it};
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                                                  std::move(table_join_executors), join_conds);
            } else {
                push_conds(&(*it), table_join_executors);
            }
            it = conds.erase(it);
        }
    } else {
        table_join_executors = table_scan_executors[0];
        scantbl[0] = 1;
    }

    // 连接剩余表
    for (size_t i = 0; i < tables.size(); i++) {
        if (scantbl[i] == -1) {
            table_join_executors =
                std::make_shared<JoinPlan>(T_NestLoop, std::move(table_scan_executors[i]),
                                           std::move(table_join_executors), std::vector<Condition>());
        }
    }

    return table_join_executors;
}

std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    if (query->order_by_items.empty()) {
        return plan;
    }
    int sort_limit = query->has_limit ? query->limit : -1;
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), bind_order_by_output_names(*query), sort_limit);
}

std::shared_ptr<Plan> Planner::generate_limit_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    if (!query->has_limit) {
        return plan;
    }
    if (!query->order_by_items.empty()) {
        return plan;
    }
    return std::make_shared<LimitPlan>(T_Limit, std::move(plan), query->limit);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context* context) {
    // 逻辑优化
    query = logical_optimization(std::move(query), context);

    // scan / join
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);

    // aggregate / group by / having
    if (needs_aggregate_plan(*query)) {
        plannerRoot = std::make_shared<AggregatePlan>(T_Aggregate, std::move(plannerRoot), query->group_by_cols,
                                                      collect_aggregate_exprs(*query), query->having_conds);
    }

    // final select projection
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot), query->select_items,
                                                   query->output_names);

    // final order by
    plannerRoot = generate_sort_plan(query, std::move(plannerRoot));

    // final limit
    plannerRoot = generate_limit_plan(query, std::move(plannerRoot));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context* context) {
    std::shared_ptr<Plan> plannerRoot;
    switch (query->parse->type) {
    case ast::AstType::CreateTable: {
        auto x = std::static_pointer_cast<ast::CreateTable>(query->parse);
        // create table;
        std::vector<ColDef> col_defs;
        col_defs.reserve(x->fields.size());
        for (auto& field : x->fields) {
            if (field->type == ast::AstType::ColDef) {
                auto sv_col_def = std::static_pointer_cast<ast::ColDef>(field);
                ColDef col_def = {.name = sv_col_def->col_name,
                                  .type = interp_sv_type(sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
        break;
    }
    case ast::AstType::DropTable: {
        auto x = std::static_pointer_cast<ast::DropTable>(query->parse);
        // drop table;
        plannerRoot =
            std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
        break;
    }
    case ast::AstType::CreateIndex: {
        auto x = std::static_pointer_cast<ast::CreateIndex>(query->parse);
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
        break;
    }
    case ast::AstType::DropIndex: {
        auto x = std::static_pointer_cast<ast::DropIndex>(query->parse);
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
        break;
    }
    case ast::AstType::InsertStmt: {
        auto x = std::static_pointer_cast<ast::InsertStmt>(query->parse);
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(), x->tab_name, query->values,
                                                std::vector<Condition>(), std::vector<SetClause>());
        break;
    }
    case ast::AstType::DeleteStmt: {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(query->parse);
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false) { // 该表没有索引
            index_col_names.clear();
            table_scan_executors =
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else { // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name, std::vector<Value>(),
                                                query->conds, std::vector<SetClause>());
        break;
    }
    case ast::AstType::UpdateStmt: {
        auto x = std::static_pointer_cast<ast::UpdateStmt>(query->parse);
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);

        if (index_exist == false) { // 该表没有索引
            index_col_names.clear();
            table_scan_executors =
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else { // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name, std::vector<Value>(),
                                                query->conds, query->set_clauses);
        break;
    }
    case ast::AstType::SelectStmt: {
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
        break;
    }
    default:
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
