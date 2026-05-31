/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>
#include "execution/executor_abstract.h"
#include "execution/executor_aggregate.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_limit.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "execution/execution_sort.h"
#include "common/common.h"
#include "optimizer/plan.h"

typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;

struct PortalStmt {
    portalTag tag;

    std::vector<std::string> output_names;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;

    PortalStmt(portalTag tag_, std::vector<std::string> output_names_, std::unique_ptr<AbstractExecutor> root_,
               std::shared_ptr<Plan> plan_)
        : tag(tag_), output_names(std::move(output_names_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

class Portal {
private:
    SmManager* sm_manager_;

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
        auto projection = std::dynamic_pointer_cast<ProjectionPlan>(plan.subplan_);
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

    std::vector<std::string> get_plan_output_names(const std::shared_ptr<Plan>& plan) const {
        switch (plan->tag) {
        case T_Projection:
            return build_projection_output_names(*std::static_pointer_cast<ProjectionPlan>(plan));
        case T_Sort:
            return get_plan_output_names(std::static_pointer_cast<SortPlan>(plan)->subplan_);
        case T_Limit:
            return get_plan_output_names(std::static_pointer_cast<LimitPlan>(plan)->subplan_);
        case T_Aggregate:
            return build_aggregate_output_names(*std::static_pointer_cast<AggregatePlan>(plan));
        case T_SeqScan:
        case T_IndexScan: {
            std::vector<std::string> output_names;
            const auto& cols = std::static_pointer_cast<ScanPlan>(plan)->cols_;
            output_names.reserve(cols.size());
            for (const auto& col : cols) {
                output_names.push_back(col.name);
            }
            return output_names;
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto join_plan = std::static_pointer_cast<JoinPlan>(plan);
            auto output_names = get_plan_output_names(join_plan->left_);
            auto right_output_names = get_plan_output_names(join_plan->right_);
            output_names.insert(output_names.end(), right_output_names.begin(), right_output_names.end());
            return output_names;
        }
        default:
            return {};
        }
    }

public:
    Portal(SmManager* sm_manager) : sm_manager_(sm_manager) {}
    ~Portal() {}

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context* context) {
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
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        case T_SetKnob:
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        case T_CreateTable:
        case T_DropTable:
        case T_CreateIndex:
        case T_DropIndex:
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), plan);
        case T_select:
        case T_Update:
        case T_Delete:
        case T_Insert: {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            switch (x->tag) {
            case T_select: {
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_, context);
                std::vector<std::string> output_names = get_plan_output_names(x->subplan_);
                return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(output_names), std::move(root), plan);
            }

            case T_Update: {
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                    sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), plan);
            }
            case T_Delete: {
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }

                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), plan);
            }

            case T_Insert: {
                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

                return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), plan);
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
    void run(std::shared_ptr<PortalStmt> portal, QlManager* ql, txn_id_t* txn_id, Context* context) {
        switch (portal->tag) {
        case PORTAL_ONE_SELECT: {
            ql->select_from(std::move(portal->root), std::move(portal->output_names), context);
            break;
        }

        case PORTAL_DML_WITHOUT_SELECT: {
            ql->run_dml(std::move(portal->root));
            break;
        }
        case PORTAL_MULTI_QUERY: {
            ql->run_mutli_query(portal->plan, context);
            break;
        }
        case PORTAL_CMD_UTILITY: {
            ql->run_cmd_utility(portal->plan, txn_id, context);
            break;
        }
        default: {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context* context) {
        switch (plan->tag) {
        case T_Projection: {
            auto x = std::static_pointer_cast<ProjectionPlan>(plan);
            auto select_items = to_executor_select_items(x->select_items_);
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), select_items);
        }
        case T_Aggregate: {
            auto x = std::static_pointer_cast<AggregatePlan>(plan);
            auto having_conds = to_executor_having_conds(x->having_conds_);
            return std::make_unique<AggregateExecutor>(convert_plan_executor(x->subplan_, context), x->group_by_cols_,
                                                       x->agg_exprs_, having_conds);
        }
        case T_SeqScan:
        case T_IndexScan: {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            if (x->tag == T_SeqScan) {
                return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            } else {
                return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_col_names_,
                                                           context);
            }
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto x = std::static_pointer_cast<JoinPlan>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join =
                std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
            return join;
        }
        case T_Sort: {
            auto x = std::static_pointer_cast<SortPlan>(plan);
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context),
                                                  bind_sort_output_names(*x), x->limit_);
        }
        case T_Limit: {
            auto x = std::static_pointer_cast<LimitPlan>(plan);
            return std::make_unique<LimitExecutor>(convert_plan_executor(x->subplan_, context),
                                                   static_cast<size_t>(x->limit_));
        }
        default:
            break;
        }
        return nullptr;
    }
};
