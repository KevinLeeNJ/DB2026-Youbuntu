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

#include <cassert>
#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "parser/parser.h"
#include "system/sm.h"
#include "common/common.h"

class Query {
public:
    std::unique_ptr<ast::TreeNode> parse;
    // where条件
    std::vector<Condition> conds;
    // 递归 WHERE 表达式；简单 AND/比较条件仍会同步保存在 conds 中供索引和连接优化使用
    std::shared_ptr<QueryExpr> where_expr;
    // 投影列
    std::vector<TabCol> cols;
    // 聚合/分组查询的执行期输出描述
    std::vector<SelectItem> select_items;
    std::vector<TabCol> group_by_cols;
    std::vector<HavingCondition> having_conds;
    std::vector<OrderByItem> order_by_items;
    bool has_limit = false;
    int limit = 0;
    bool has_offset = false;
    int offset = 0;
    bool has_distinct = false;
    bool has_aggregate = false;
    bool has_select_star = false;
    std::vector<std::string> output_names;
    bool is_union = false;
    std::vector<std::unique_ptr<Query>> union_branches;
    std::vector<ColMeta> union_cols;
    std::string union_alias;
    std::vector<bool> union_all;
    std::vector<QuerySetOperator> set_operators;
    std::vector<JoinType> join_types;
    std::vector<std::vector<Condition>> join_on_conds;
    // 表名
    std::vector<std::string> tables;
    std::vector<std::string> table_display_names;
    std::unordered_map<std::string, std::string> table_alias_to_name;
    std::unordered_map<std::string, std::string> table_name_to_display;
    bool is_explain_analyze = false;
    bool is_set_transaction = false;
    ast::IsolationLevelType set_isolation_level;
    // update 的set 值
    std::vector<SetClause> set_clauses;
    // insert 的values值
    std::vector<Value> values;
    std::vector<std::string> insert_col_names;
    std::shared_ptr<Query> insert_query;

    Query() {}
};

class Analyze {
private:
    SmManager* sm_manager_;

public:
    Analyze(SmManager* sm_manager) : sm_manager_(sm_manager) {}
    ~Analyze() {}

    std::unique_ptr<Query> do_analyze(std::unique_ptr<ast::TreeNode> root);

private:
    TabCol check_column(const std::vector<ColMeta>& all_cols, TabCol target);
    void get_all_cols(const std::vector<std::string>& tab_names, std::vector<ColMeta>& all_cols);
    void get_clause(const std::vector<std::unique_ptr<ast::BinaryExpr>>& sv_conds, std::vector<Condition>& conds);
    void check_clause(const std::vector<std::string>& tab_names, std::vector<Condition>& conds);
    Value convert_sv_value(const ast::Value* sv_val);
    CompOp convert_sv_comp_op(ast::SvCompOp op);
    void cast_value(Value& val, ColType to);

    bool can_cast(ColType lhs_type, ColType rhs_type) {
        if (lhs_type == rhs_type) {
            return true;
        }
        if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)) {
            return true;
        }
        if ((lhs_type == TYPE_STRING && rhs_type == TYPE_DATETIME) ||
            (lhs_type == TYPE_DATETIME && rhs_type == TYPE_STRING)) {
            return true;
        }
        return false;
    }

    std::unique_ptr<Query> analyze_select_stmt(const ast::SelectStmt* select,
                                               std::unique_ptr<ast::TreeNode> owner = nullptr,
                                               const std::vector<ColMeta>& outer_cols = {},
                                               const std::unordered_map<std::string, std::string>& outer_aliases = {});
    std::unique_ptr<Query> analyze_select_from_union_stmt(const ast::SelectFromUnionStmt* select,
                                                          std::unique_ptr<ast::TreeNode> owner,
                                                          const std::vector<ColMeta>& outer_cols = {},
                                                          const std::unordered_map<std::string, std::string>&
                                                              outer_aliases = {});
    std::unique_ptr<Query> analyze_union_stmt(const ast::UnionStmt* union_stmt, std::string alias,
                                              const std::vector<std::unique_ptr<ast::OrderByItem>>& order_by_items,
                                              bool has_limit, int limit, bool has_offset, int offset,
                                              std::unique_ptr<ast::TreeNode> owner,
                                              const std::vector<ColMeta>& outer_cols,
                                              const std::unordered_map<std::string, std::string>& outer_aliases);
    QueryExpr convert_ast_expr(const ast::Expr* expr, const std::string& context_name,
                               const std::vector<ColMeta>& outer_cols,
                               const std::unordered_map<std::string, std::string>& outer_aliases);
    std::unique_ptr<Query> analyze_subquery(const ast::TreeNode* root, const std::vector<ColMeta>& outer_cols,
                                            const std::unordered_map<std::string, std::string>& outer_aliases);
    std::vector<ColMeta> get_query_output_metas(const Query& query);
    ColMeta make_union_col_meta(const ColMeta& current, const ColMeta& next);
    void validate_union_order_by(Query& query);
};
