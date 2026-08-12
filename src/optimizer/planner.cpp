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

std::optional<PointAccessPath> find_point_access_path(SmManager* sm_manager, const std::string& tab_name,
                                                      const std::vector<Condition>& conditions) {
    const auto& tab = sm_manager->db_.get_table(tab_name);
    for (const auto& index : tab.indexes) {
        PointAccessPath path;
        path.index_cols.reserve(index.cols.size());
        path.condition_positions.reserve(index.cols.size());
        bool complete = true;

        for (const auto& index_col : index.cols) {
            size_t match = conditions.size();
            for (size_t i = 0; i < conditions.size(); ++i) {
                const auto& condition = conditions[i];
                if (condition.lhs_col.tab_name == tab_name && condition.lhs_col.col_name == index_col.name &&
                    is_indexable_value_condition(condition) && condition.op == OP_EQ) {
                    if (match != conditions.size()) {
                        complete = false;
                        break;
                    }
                    match = i;
                }
            }
            if (!complete || match == conditions.size()) {
                complete = false;
                break;
            }
            path.index_cols.push_back(index_col.name);
            path.condition_positions.push_back(match);
        }

        if (complete) {
            return path;
        }
    }
    return std::nullopt;
}

bool is_lock_only_self_assignment_update(const std::string& tab_name, const std::vector<SetClause>& set_clauses,
                                         const std::optional<PointAccessPath>& point_access) {
    if (!point_access.has_value() || set_clauses.empty()) {
        return false;
    }
    return std::all_of(set_clauses.begin(), set_clauses.end(), [&](const SetClause& clause) {
        return clause.lhs.tab_name == tab_name && clause.is_self_ref && clause.op == UpdateOp::ASSIGNMENT &&
               clause.rhs_col.tab_name == tab_name && clause.rhs_col.col_name == clause.lhs.col_name &&
               clause.additional_terms.empty();
    });
}

} // namespace

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
        std::vector<std::string> index_col_names;
        PlanTag scan_tag = choose_scan_plan_tag(x->tab_name, query->conds, index_col_names);
        if (scan_tag == T_SeqScan) {
            index_col_names.clear();
        }
        // choose_scan_plan_tag may reorder equality predicates to match the
        // index column order; compile against this final condition vector.
        std::optional<PointAccessPath> point_access = find_point_access_path(sm_manager_, x->tab_name, query->conds);
        table_scan_executors =
            std::make_unique<ScanPlan>(scan_tag, sm_manager_, x->tab_name, query->conds, index_col_names);

        auto dml = std::make_unique<DMLPlan>(T_Delete, std::move(table_scan_executors), x->tab_name,
                                             std::vector<Value>(), query->conds, std::vector<SetClause>());
        dml->point_access_ = std::move(point_access);
        plannerRoot = std::move(dml);
        break;
    }
    case ast::AstType::UpdateStmt: {
        auto x = static_cast<const ast::UpdateStmt*>(parse);
        // update;
        // 生成表扫描方式
        std::unique_ptr<Plan> table_scan_executors;
        std::vector<std::string> index_col_names;
        PlanTag scan_tag = choose_scan_plan_tag(x->tab_name, query->conds, index_col_names);
        if (scan_tag == T_SeqScan) {
            index_col_names.clear();
        }
        std::optional<PointAccessPath> point_access = find_point_access_path(sm_manager_, x->tab_name, query->conds);
        table_scan_executors =
            std::make_unique<ScanPlan>(scan_tag, sm_manager_, x->tab_name, query->conds, index_col_names);
        auto dml = std::make_unique<DMLPlan>(T_Update, std::move(table_scan_executors), x->tab_name,
                                             std::vector<Value>(), query->conds, query->set_clauses);
        if (is_lock_only_self_assignment_update(x->tab_name, query->set_clauses, point_access)) {
            dml->update_execution_mode_ = UpdateExecutionMode::LockOnlySelfAssignment;
        }
        dml->point_access_ = std::move(point_access);
        plannerRoot = std::move(dml);
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
