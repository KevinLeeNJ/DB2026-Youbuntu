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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "parser/ast.h"

#include "parser/parser.h"

typedef enum PlanTag {
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_ShowIndex,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_SetKnob,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_Filter,
    T_NestLoop,
    T_SortMerge, // sort merge join
    T_Sort,
    T_Projection,
    T_Aggregate,
    T_Limit,
    T_Union,
    T_ExplainAnalyze,
    T_SetTransaction,
    T_StaticCheckpoint
} PlanTag;

// 查询执行计划
class Plan {
public:
    PlanTag tag;
    size_t runtime_rows_ = 0;
    std::unordered_map<std::string, std::string> table_name_to_display_;
    virtual ~Plan() = default;
};

class ScanPlan : public Plan {
public:
    ScanPlan(PlanTag tag, SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds,
             std::vector<std::string> index_col_names) {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta& tab = sm_manager->db_.get_table(tab_name_);
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        fed_conds_ = conds_;
        index_col_names_ = index_col_names;
    }
    ~ScanPlan() {}
    // 以下变量同ScanExecutor中的变量
    std::string tab_name_;
    std::vector<ColMeta> cols_;
    std::vector<Condition> conds_;
    size_t len_;
    std::vector<Condition> fed_conds_;
    std::vector<std::string> index_col_names_;
};

class JoinPlan : public Plan {
public:
    JoinPlan(PlanTag tag, std::unique_ptr<Plan> left, std::unique_ptr<Plan> right, std::vector<Condition> conds) {
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = INNER_JOIN;
    }
    ~JoinPlan() {}
    // 左节点
    std::unique_ptr<Plan> left_;
    // 右节点
    std::unique_ptr<Plan> right_;
    // 连接条件
    std::vector<Condition> conds_;
    // future TODO: 后续可以支持的连接类型
    // INLJ binding: set by planner when right table join column has an index
    TabCol inlj_left_col_;            // left-side column providing lookup key (empty tab_name = NLJ mode)
    TabCol inlj_right_col_;           // right (inner) table's indexed column
    std::string inlj_index_col_name_; // convenience: index column name for ScanPlan::index_col_names_
    JoinType type;
};

class FilterPlan : public Plan {
public:
    FilterPlan(PlanTag tag, std::unique_ptr<Plan> subplan, std::vector<Condition> conds) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        conds_ = std::move(conds);
    }
    ~FilterPlan() {}
    std::unique_ptr<Plan> subplan_;
    std::vector<Condition> conds_;
};

class ProjectionPlan : public Plan {
public:
    ProjectionPlan(PlanTag tag, std::unique_ptr<Plan> subplan, std::vector<SelectItem> select_items,
                   std::vector<std::string> output_names, bool preserve_col_names = false,
                   bool is_select_star = false) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        select_items_ = std::move(select_items);
        output_names_ = std::move(output_names);
        preserve_col_names_ = preserve_col_names;
        is_select_star_ = is_select_star;
    }
    ~ProjectionPlan() {}
    std::unique_ptr<Plan> subplan_;
    std::vector<SelectItem> select_items_;
    std::vector<std::string> output_names_;
    bool preserve_col_names_ = false;
    bool is_select_star_ = false;
};

class AggregatePlan : public Plan {
public:
    AggregatePlan(PlanTag tag, std::unique_ptr<Plan> subplan, std::vector<TabCol> group_by_cols,
                  std::vector<AggExpr> agg_exprs, std::vector<HavingCondition> having_conds) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        group_by_cols_ = std::move(group_by_cols);
        agg_exprs_ = std::move(agg_exprs);
        having_conds_ = std::move(having_conds);
    }
    ~AggregatePlan() {}
    std::unique_ptr<Plan> subplan_;
    std::vector<TabCol> group_by_cols_;
    std::vector<AggExpr> agg_exprs_;
    std::vector<HavingCondition> having_conds_;
};

class SortPlan : public Plan {
public:
    SortPlan(PlanTag tag, std::unique_ptr<Plan> subplan, std::vector<OrderByItem> order_by_items, int limit = -1) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        order_by_items_ = std::move(order_by_items);
        limit_ = limit;
    }
    ~SortPlan() {}
    std::unique_ptr<Plan> subplan_;
    std::vector<OrderByItem> order_by_items_;
    int limit_ = -1;
};

class LimitPlan : public Plan {
public:
    LimitPlan(PlanTag tag, std::unique_ptr<Plan> subplan, int limit) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        limit_ = limit;
    }
    ~LimitPlan() {}
    std::unique_ptr<Plan> subplan_;
    int limit_;
};

class UnionPlan : public Plan {
public:
    UnionPlan(PlanTag tag, std::vector<std::unique_ptr<Plan>> branches, std::vector<ColMeta> cols,
              std::vector<std::string> output_names) {
        Plan::tag = tag;
        branches_ = std::move(branches);
        cols_ = std::move(cols);
        output_names_ = std::move(output_names);
    }
    ~UnionPlan() {}
    std::vector<std::unique_ptr<Plan>> branches_;
    std::vector<ColMeta> cols_;
    std::vector<std::string> output_names_;
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan {
public:
    DMLPlan(PlanTag tag, std::unique_ptr<Plan> subplan, std::string tab_name, std::vector<Value> values,
            std::vector<Condition> conds, std::vector<SetClause> set_clauses) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        tab_name_ = std::move(tab_name);
        values_ = std::move(values);
        conds_ = std::move(conds);
        set_clauses_ = std::move(set_clauses);
    }
    ~DMLPlan() {}
    std::unique_ptr<Plan> subplan_;
    std::string tab_name_;
    std::vector<Value> values_;
    std::vector<Condition> conds_;
    std::vector<SetClause> set_clauses_;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan {
public:
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols) {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        cols_ = std::move(cols);
        tab_col_names_ = std::move(col_names);
    }
    ~DDLPlan() {}
    std::string tab_name_;
    std::vector<std::string> tab_col_names_;
    std::vector<ColDef> cols_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan {
public:
    OtherPlan(PlanTag tag, std::string tab_name) {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
    }
    ~OtherPlan() {}
    std::string tab_name_;
};

// Set Knob Plan
class SetKnobPlan : public Plan {
public:
    SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
        Plan::tag = T_SetKnob;
        set_knob_type_ = knob_type;
        bool_value_ = bool_value;
    }
    ast::SetKnobType set_knob_type_;
    bool bool_value_;
};

// Set Transaction Isolation Level Plan
class SetTransactionPlan : public Plan {
public:
    explicit SetTransactionPlan(ast::IsolationLevelType level) {
        Plan::tag = T_SetTransaction;
        isolation_level_ = level;
    }
    ast::IsolationLevelType isolation_level_;
};

class plannerInfo {
public:
    const ast::SelectStmt* parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::unique_ptr<Plan> plan;
    std::vector<std::unique_ptr<Plan>> table_scan_executors;
    std::vector<SetClause> set_clauses;
    plannerInfo(const ast::SelectStmt* parse_) : parse(parse_) {}
};
