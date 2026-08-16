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

#include "analyze.h"
#include "analyze_expr_internal.h"
#include "analyze_select_internal.h"

#include <algorithm>
#include <functional>
#include <unordered_set>
#include <utility>

using namespace analyze_internal;

namespace {

UpdateOp convert_update_op(ast::SetOp op) {
    switch (op) {
    case ast::SetOp::SELF_ADD:
        return UpdateOp::SELF_ADD;
    case ast::SetOp::SELF_SUB:
        return UpdateOp::SELF_SUB;
    case ast::SetOp::SELF_MUL:
        return UpdateOp::SELF_MUL;
    case ast::SetOp::SELF_DIV:
        return UpdateOp::SELF_DIV;
    case ast::SetOp::ASSIGNMENT:
        return UpdateOp::ASSIGNMENT;
    }
    throw InternalError("Unexpected UPDATE operator");
}

WindowFuncType convert_ast_window_func_type(ast::WindowFuncType type) {
    switch (type) {
    case ast::WindowFuncType::ROW_NUMBER:
        return WindowFuncType::ROW_NUMBER;
    case ast::WindowFuncType::RANK:
        return WindowFuncType::RANK;
    case ast::WindowFuncType::DENSE_RANK:
        return WindowFuncType::DENSE_RANK;
    case ast::WindowFuncType::LAG:
        return WindowFuncType::LAG;
    case ast::WindowFuncType::LEAD:
        return WindowFuncType::LEAD;
    case ast::WindowFuncType::SUM:
        return WindowFuncType::SUM;
    case ast::WindowFuncType::AVG:
        return WindowFuncType::AVG;
    }
    throw InternalError("Unexpected AST window function type");
}

ScalarFuncType convert_ast_scalar_func_type(ast::ScalarFuncType type) {
    switch (type) {
    case ast::ScalarFuncType::ABS:
        return ScalarFuncType::ABS;
    case ast::ScalarFuncType::LENGTH:
        return ScalarFuncType::LENGTH;
    case ast::ScalarFuncType::COALESCE:
        return ScalarFuncType::COALESCE;
    case ast::ScalarFuncType::LOWER:
        return ScalarFuncType::LOWER;
    case ast::ScalarFuncType::UPPER:
        return ScalarFuncType::UPPER;
    case ast::ScalarFuncType::TRIM:
        return ScalarFuncType::TRIM;
    case ast::ScalarFuncType::ROUND:
        return ScalarFuncType::ROUND;
    case ast::ScalarFuncType::NULLIF:
        return ScalarFuncType::NULLIF;
    }
    throw InternalError("Unexpected AST scalar function type");
}

void resolve_alias(TabCol& col, const Query& query) {
    if (col.tab_name.empty()) {
        return;
    }
    auto alias_pos = query.table_alias_to_name.find(col.tab_name);
    if (alias_pos != query.table_alias_to_name.end()) {
        col.tab_name = alias_pos->second;
    }
}

void resolve_alias(QueryExpr& expr, const Query& query) {
    if (expr.type == QueryExprType::COLUMN) {
        resolve_alias(expr.col, query);
    } else if (expr.type == QueryExprType::AGGREGATE && !expr.agg.is_star) {
        resolve_alias(expr.agg.col, query);
    }
    for (auto& window_arg : expr.window_args) {
        if (window_arg != nullptr) {
            resolve_alias(*window_arg, query);
        }
    }
    for (auto& partition_expr : expr.window_partition_by) {
        if (partition_expr != nullptr) {
            resolve_alias(*partition_expr, query);
        }
    }
    for (auto& order_expr : expr.window_order_by) {
        if (order_expr != nullptr) {
            resolve_alias(*order_expr, query);
        }
    }
    if (expr.lhs != nullptr) {
        resolve_alias(*expr.lhs, query);
    }
    if (expr.rhs != nullptr) {
        resolve_alias(*expr.rhs, query);
    }
    if (expr.rhs_upper != nullptr) {
        resolve_alias(*expr.rhs_upper, query);
    }
    for (auto& operand : expr.operands) {
        if (operand != nullptr) {
            resolve_alias(*operand, query);
        }
    }
    for (auto& clause : expr.case_when) {
        if (clause.first != nullptr) {
            resolve_alias(*clause.first, query);
        }
        if (clause.second != nullptr) {
            resolve_alias(*clause.second, query);
        }
    }
    if (expr.else_expr != nullptr) {
        resolve_alias(*expr.else_expr, query);
    }
    for (auto& value : expr.rhs_values) {
        if (value != nullptr) {
            resolve_alias(*value, query);
        }
    }
}

void resolve_aliases(Query& query) {
    for (auto& item : query.select_items) {
        resolve_alias(item.expr, query);
    }
    for (auto& col : query.group_by_cols) {
        resolve_alias(col, query);
    }
    for (auto& cond : query.having_conds) {
        resolve_alias(cond.lhs, query);
        if (!cond.is_rhs_val) {
            resolve_alias(cond.rhs_expr, query);
        }
    }
    if (query.having_expr != nullptr) {
        resolve_alias(*query.having_expr, query);
    }
    for (auto& item : query.order_by_items) {
        resolve_alias(item.expr, query);
    }
    for (auto& cond : query.conds) {
        resolve_alias(cond.lhs_col, query);
        if (!cond.is_rhs_val && cond.op != OP_IN && cond.op != OP_BETWEEN) {
            resolve_alias(cond.rhs_col, query);
        }
    }
    for (auto& join_conds : query.join_on_conds) {
        for (auto& cond : join_conds) {
            resolve_alias(cond.lhs_col, query);
            if (!cond.is_rhs_val && cond.op != OP_IN && cond.op != OP_BETWEEN) {
                resolve_alias(cond.rhs_col, query);
            }
        }
    }
    if (query.where_expr != nullptr) {
        resolve_alias(*query.where_expr, query);
    }
    for (auto& join_exprs : query.join_on_exprs) {
        for (auto& join_expr : join_exprs) {
            if (join_expr != nullptr) {
                resolve_alias(*join_expr, query);
            }
        }
    }
}

void resolve_local_unqualified_columns(QueryExpr& expr, const std::vector<ColMeta>& local_cols) {
    if (expr.type == QueryExprType::COLUMN && expr.col.tab_name.empty()) {
        try {
            resolve_column_meta(local_cols, expr.col);
        } catch (const ColumnNotFoundError&) {
            // An unresolved unqualified name may belong to a correlated outer scope.
        }
    } else if (expr.type == QueryExprType::AGGREGATE && !expr.agg.is_star && expr.agg.col.tab_name.empty()) {
        try {
            resolve_column_meta(local_cols, expr.agg.col);
        } catch (const ColumnNotFoundError&) {
            // An unresolved unqualified name may belong to a correlated outer scope.
        }
    }
    for (auto& window_arg : expr.window_args) {
        if (window_arg != nullptr) {
            resolve_local_unqualified_columns(*window_arg, local_cols);
        }
    }
    for (auto& partition_expr : expr.window_partition_by) {
        if (partition_expr != nullptr) {
            resolve_local_unqualified_columns(*partition_expr, local_cols);
        }
    }
    for (auto& order_expr : expr.window_order_by) {
        if (order_expr != nullptr) {
            resolve_local_unqualified_columns(*order_expr, local_cols);
        }
    }
    if (expr.lhs != nullptr) {
        resolve_local_unqualified_columns(*expr.lhs, local_cols);
    }
    if (expr.rhs != nullptr) {
        resolve_local_unqualified_columns(*expr.rhs, local_cols);
    }
    if (expr.rhs_upper != nullptr) {
        resolve_local_unqualified_columns(*expr.rhs_upper, local_cols);
    }
    for (auto& operand : expr.operands) {
        resolve_local_unqualified_columns(*operand, local_cols);
    }
    for (auto& clause : expr.case_when) {
        resolve_local_unqualified_columns(*clause.first, local_cols);
        resolve_local_unqualified_columns(*clause.second, local_cols);
    }
    if (expr.else_expr != nullptr) {
        resolve_local_unqualified_columns(*expr.else_expr, local_cols);
    }
    for (auto& value : expr.rhs_values) {
        resolve_local_unqualified_columns(*value, local_cols);
    }
}

void resolve_local_unqualified_columns(Query& query, const std::vector<ColMeta>& local_cols) {
    for (auto& item : query.select_items) {
        resolve_local_unqualified_columns(item.expr, local_cols);
    }
    for (auto& cond : query.having_conds) {
        resolve_local_unqualified_columns(cond.lhs, local_cols);
        if (!cond.is_rhs_val) {
            resolve_local_unqualified_columns(cond.rhs_expr, local_cols);
        }
    }
    if (query.having_expr != nullptr) {
        resolve_local_unqualified_columns(*query.having_expr, local_cols);
    }
    for (auto& item : query.order_by_items) {
        resolve_local_unqualified_columns(item.expr, local_cols);
    }
    if (query.where_expr != nullptr) {
        resolve_local_unqualified_columns(*query.where_expr, local_cols);
    }
    for (auto& join_exprs : query.join_on_exprs) {
        for (auto& join_expr : join_exprs) {
            if (join_expr != nullptr) {
                resolve_local_unqualified_columns(*join_expr, local_cols);
            }
        }
    }
}

bool is_outer_column(const ast::Col* column, const std::vector<ColMeta>& outer_cols,
                     const std::unordered_map<std::string, std::string>& outer_aliases) {
    if (column == nullptr) {
        return false;
    }
    if (outer_aliases.find(column->tab_name) != outer_aliases.end()) {
        return true;
    }
    return !column->tab_name.empty() && std::any_of(outer_cols.begin(), outer_cols.end(), [&](const ColMeta& col) {
        return col.tab_name == column->tab_name;
    });
}

bool is_legacy_condition_expression(const ast::Expr* expression, const std::vector<ColMeta>& outer_cols,
                                    const std::unordered_map<std::string, std::string>& outer_aliases) {
    if (expression == nullptr) {
        return false;
    }
    if (auto logical = dynamic_cast<const ast::LogicalExpr*>(expression); logical != nullptr) {
        return logical->op == ast::LogicalOp::AND && !logical->operands.empty() &&
               std::all_of(logical->operands.begin(), logical->operands.end(), [&](const auto& operand) {
                   return is_legacy_condition_expression(operand.get(), outer_cols, outer_aliases);
               });
    }

    auto condition = dynamic_cast<const ast::BinaryExpr*>(expression);
    if (condition == nullptr || condition->quantifier != ast::Quantifier::NONE) {
        return false;
    }
    auto lhs = dynamic_cast<const ast::Col*>(condition->lhs.get());
    if (lhs == nullptr || is_outer_column(lhs, outer_cols, outer_aliases)) {
        return false;
    }
    if (condition->op == ast::SV_OP_IS_NULL || condition->op == ast::SV_OP_IS_NOT_NULL) {
        return condition->rhs == nullptr;
    }
    if (condition->op == ast::SV_OP_EXISTS) {
        return false;
    }
    if (!std::all_of(condition->rhs_list.begin(), condition->rhs_list.end(),
                     [](const auto& value) { return dynamic_cast<const ast::Value*>(value.get()) != nullptr; })) {
        return false;
    }
    if (std::any_of(condition->rhs_list.begin(), condition->rhs_list.end(), [](const auto& value) {
            auto literal = dynamic_cast<const ast::Value*>(value.get());
            return literal != nullptr && literal->type == ast::AstType::NullLit;
        })) {
        return false;
    }
    if (condition->rhs_upper != nullptr && dynamic_cast<const ast::Value*>(condition->rhs_upper.get()) == nullptr) {
        return false;
    }
    if (auto rhs_value = dynamic_cast<const ast::Value*>(condition->rhs.get());
        rhs_value != nullptr && rhs_value->type == ast::AstType::NullLit) {
        return false;
    }
    if (condition->rhs == nullptr) {
        return condition->op == ast::SV_OP_IN || condition->op == ast::SV_OP_BETWEEN;
    }
    if (dynamic_cast<const ast::Value*>(condition->rhs.get()) != nullptr) {
        return true;
    }
    auto rhs_col = dynamic_cast<const ast::Col*>(condition->rhs.get());
    return rhs_col != nullptr && !is_outer_column(rhs_col, outer_cols, outer_aliases);
}

void resolve_local_condition_columns(std::vector<Condition>& conditions, const std::vector<ColMeta>& local_cols) {
    for (auto& condition : conditions) {
        if (condition.lhs_col.tab_name.empty()) {
            resolve_column_meta(local_cols, condition.lhs_col);
        }
        if (!condition.is_rhs_val && condition.op != OP_IN && condition.op != OP_BETWEEN &&
            condition.op != OP_IS_NULL && condition.op != OP_IS_NOT_NULL && condition.op != OP_EXISTS &&
            condition.rhs_col.tab_name.empty()) {
            try {
                resolve_column_meta(local_cols, condition.rhs_col);
            } catch (const ColumnNotFoundError&) {
                // A qualified outer reference is already non-empty; only an unresolved
                // unqualified reference can reach this path and is handled by the full expression.
            }
        }
    }
}

void populate_table_refs(Query& query, const std::vector<ast::TableRef>& table_refs) {
    query.tables.clear();
    query.table_display_names.clear();
    query.table_alias_to_name.clear();
    query.table_name_to_display.clear();
    query.tables.reserve(table_refs.size());
    query.table_display_names.reserve(table_refs.size());

    for (const auto& ref : table_refs) {
        const std::string& table_name = ref.table_name;
        const std::string& display_name = ref.alias.empty() ? ref.table_name : ref.alias;
        query.tables.push_back(table_name);
        query.table_display_names.push_back(display_name);
        query.table_name_to_display[table_name] = display_name;
        if (display_name != table_name) {
            query.table_alias_to_name[display_name] = table_name;
        }
    }
}

} // namespace

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {unique_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {unique_ptr<Query>} Query
 */
std::unique_ptr<Query> Analyze::do_analyze(std::unique_ptr<ast::TreeNode> parse) {
    auto* root = parse.get();
    if (root == nullptr) {
        throw InternalError("Unexpected null AST root");
    }
    if (root->type == ast::AstType::SelectStmt) {
        return analyze_select_stmt(static_cast<const ast::SelectStmt*>(root), std::move(parse));
    }
    if (root->type == ast::AstType::ExplainAnalyze) {
        auto explain = static_cast<const ast::ExplainAnalyze*>(root);
        auto query = analyze_select_stmt(explain->select.get());
        query->is_explain_analyze = true;
        query->parse = std::move(parse);
        return query;
    }
    if (root->type == ast::AstType::SelectFromUnionStmt) {
        return analyze_select_from_union_stmt(static_cast<const ast::SelectFromUnionStmt*>(root), std::move(parse));
    }
    if (root->type == ast::AstType::UnionStmt) {
        return analyze_union_stmt(static_cast<const ast::UnionStmt*>(root), "", {}, false, 0, false, 0,
                                  std::move(parse), {}, {});
    }

    if (root->type == ast::AstType::SetTransaction) {
        auto x = static_cast<const ast::SetTransaction*>(root);
        auto query = std::make_unique<Query>();
        query->is_set_transaction = true;
        query->set_isolation_level = x->isolation_level_;
        query->parse = std::move(parse);
        return query;
    }

    auto query = std::make_unique<Query>();
    switch (root->type) {
    case ast::AstType::UpdateStmt: {
        auto x = static_cast<const ast::UpdateStmt*>(root);
        if (!sm_manager_->db_.is_table(x->tab_name)) {
            throw TableNotFoundError(x->tab_name);
        }
        std::vector<ColMeta> all_cols;
        get_all_cols({x->tab_name}, all_cols);
        query->set_clauses.reserve(x->set_clauses.size());
        for (const auto& set_clause : x->set_clauses) {
            SetClause clause;
            clause.lhs = {.tab_name = x->tab_name, .col_name = set_clause->col_name};
            clause.op = convert_update_op(set_clause->op);
            if (set_clause->val != nullptr) {
                clause.rhs = convert_sv_value(set_clause->val.get());
            }
            clause.is_self_ref = set_clause->is_self_ref;
            if (set_clause->rhs_expr != nullptr) {
                if (auto value = dynamic_cast<const ast::Value*>(set_clause->rhs_expr.get()); value != nullptr) {
                    clause.rhs = convert_sv_value(value);
                } else {
                    auto rhs_expr = convert_ast_expr(set_clause->rhs_expr.get(), "UPDATE", all_cols, {});
                    if (rhs_expr.type == QueryExprType::SUBQUERY && rhs_expr.subquery->output_cols.size() != 1) {
                        throw RMDBError("Scalar subquery must return exactly one column");
                    }
                    clause.rhs_expr = std::make_shared<QueryExpr>(std::move(rhs_expr));
                }
            }
            clause.lhs = {.tab_name = x->tab_name, .col_name = set_clause->col_name};
            clause.lhs = check_column(all_cols, clause.lhs);
            if (set_clause->is_self_ref) {
                clause.rhs_col = {.tab_name = set_clause->rhs_col->tab_name.empty() ? x->tab_name
                                                                                    : set_clause->rhs_col->tab_name,
                                  .col_name = set_clause->rhs_col->col_name};
                clause.rhs_col = check_column(all_cols, clause.rhs_col);
            }
            query->set_clauses.push_back(clause);
        }
        get_clause(x->conds, query->conds);
        if (x->where_expr != nullptr && !is_legacy_condition_expression(x->where_expr.get(), {}, {})) {
            query->where_expr =
                std::make_shared<QueryExpr>(convert_ast_expr(x->where_expr.get(), "WHERE", all_cols, {}));
            if (!is_boolean_query_expr(*query->where_expr)) {
                throw RMDBError("WHERE expression must be boolean");
            }
        }
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstType::DeleteStmt: {
        auto x = static_cast<const ast::DeleteStmt*>(root);
        if (!sm_manager_->db_.is_table(x->tab_name)) {
            throw TableNotFoundError(x->tab_name);
        }
        get_clause(x->conds, query->conds);
        if (x->where_expr != nullptr && !is_legacy_condition_expression(x->where_expr.get(), {}, {})) {
            std::vector<ColMeta> all_cols;
            get_all_cols({x->tab_name}, all_cols);
            query->where_expr =
                std::make_shared<QueryExpr>(convert_ast_expr(x->where_expr.get(), "WHERE", all_cols, {}));
            if (!is_boolean_query_expr(*query->where_expr)) {
                throw RMDBError("WHERE expression must be boolean");
            }
        }
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstType::InsertStmt: {
        auto x = static_cast<const ast::InsertStmt*>(root);
        if (!sm_manager_->db_.is_table(x->tab_name)) {
            throw TableNotFoundError(x->tab_name);
        }
        const auto& table = sm_manager_->db_.get_table(x->tab_name);
        query->insert_col_names = x->col_names;
        if (x->select != nullptr) {
            query->insert_query = std::shared_ptr<Query>(analyze_subquery(x->select.get(), {}, {}));
            const auto& source_cols = query->insert_query->output_cols;
            if (query->insert_col_names.empty()) {
                if (source_cols.size() != table.cols.size()) {
                    throw InvalidValueCountError();
                }
            } else {
                if (query->insert_col_names.size() != table.cols.size() ||
                    query->insert_col_names.size() != source_cols.size()) {
                    throw InvalidValueCountError();
                }
                std::unordered_set<std::string> seen;
                for (const auto& name : query->insert_col_names) {
                    if (!seen.insert(name).second) {
                        throw RMDBError("Duplicate INSERT target column: " + name);
                    }
                    if (std::find_if(table.cols.begin(), table.cols.end(),
                                     [&](const ColMeta& col) { return col.name == name; }) == table.cols.end()) {
                        throw ColumnNotFoundError(name);
                    }
                }
            }
            break;
        }
        if (x->col_names.empty()) {
            if (x->vals.size() != table.cols.size()) {
                throw InvalidValueCountError();
            }
            query->values.reserve(x->vals.size());
            for (const auto& sv_val : x->vals) {
                query->values.push_back(convert_sv_value(sv_val.get()));
            }
        } else {
            if (x->col_names.size() != table.cols.size() || x->col_names.size() != x->vals.size()) {
                throw InvalidValueCountError();
            }
            query->values.resize(table.cols.size());
            std::vector<bool> filled(table.cols.size(), false);
            for (size_t value_idx = 0; value_idx < x->col_names.size(); ++value_idx) {
                auto col = std::find_if(table.cols.begin(), table.cols.end(),
                                        [&](const ColMeta& meta) { return meta.name == x->col_names[value_idx]; });
                if (col == table.cols.end()) {
                    throw ColumnNotFoundError(x->col_names[value_idx]);
                }
                size_t col_idx = static_cast<size_t>(col - table.cols.begin());
                if (filled[col_idx]) {
                    throw RMDBError("Duplicate INSERT target column: " + x->col_names[value_idx]);
                }
                filled[col_idx] = true;
                query->values[col_idx] = convert_sv_value(x->vals[value_idx].get());
            }
        }
        break;
    }
    default:
        break;
    }
    query->parse = std::move(parse);
    return query;
}

std::unique_ptr<Query> Analyze::analyze_select_stmt(const ast::SelectStmt* x, std::unique_ptr<ast::TreeNode> owner,
                                                    const std::vector<ColMeta>& outer_cols,
                                                    const std::unordered_map<std::string, std::string>& outer_aliases) {
    auto query = std::make_unique<Query>();
    populate_table_refs(*query, x->tabs);
    for (const auto& [alias, table] : outer_aliases) {
        query->table_alias_to_name.emplace(alias, table);
    }

    for (const auto& tab_name : query->tables) {
        if (!sm_manager_->db_.is_table(tab_name)) {
            throw TableNotFoundError(tab_name);
        }
    }

    std::vector<ColMeta> all_cols;
    get_all_cols(query->tables, all_cols);
    std::unordered_set<std::string> natural_duplicate_columns;
    for (const auto& join : x->jointree) {
        if (join->join_type != NATURAL_JOIN) {
            continue;
        }
        const auto& left_meta = sm_manager_->db_.get_table(join->left.table_name);
        const auto& right_meta = sm_manager_->db_.get_table(join->right.table_name);
        for (const auto& left_col : left_meta.cols) {
            auto right_col = std::find_if(right_meta.cols.begin(), right_meta.cols.end(),
                                          [&](const ColMeta& col) { return col.name == left_col.name; });
            if (right_col != right_meta.cols.end()) {
                natural_duplicate_columns.insert(join->right.table_name + "." + right_col->name);
            }
        }
    }
    std::vector<ColMeta> visible_cols;
    visible_cols.reserve(all_cols.size());
    for (const auto& col : all_cols) {
        if (natural_duplicate_columns.find(col.tab_name + "." + col.name) == natural_duplicate_columns.end()) {
            visible_cols.push_back(col);
        }
    }
    std::vector<ColMeta> scope_cols = all_cols;
    scope_cols.insert(scope_cols.end(), outer_cols.begin(), outer_cols.end());
    auto converter = [this, &scope_cols, &query](const ast::Expr* expr, const std::string& context_name) {
        return convert_ast_expr(expr, context_name, scope_cols, query->table_alias_to_name);
    };

    populate_select_items_from_ast(*query, *x, visible_cols, converter);
    populate_group_by_from_ast(*query, *x);
    populate_having_from_ast(*query, *x, converter);
    if (x->having_expr != nullptr && x->having_conds.empty()) {
        query->having_expr = std::make_shared<QueryExpr>(converter(x->having_expr.get(), "HAVING"));
    }
    populate_order_by_from_ast(*query, *x, converter);
    populate_limit_from_ast(*query, *x);
    if (x->where_expr != nullptr) {
        if (!is_legacy_condition_expression(x->where_expr.get(), outer_cols, outer_aliases)) {
            query->where_expr = std::make_shared<QueryExpr>(
                convert_ast_expr(x->where_expr.get(), "WHERE", scope_cols, query->table_alias_to_name));
            if (!is_boolean_query_expr(*query->where_expr)) {
                throw RMDBError("WHERE expression must be boolean");
            }
        }
    }
    query->conds.clear();
    query->join_types.reserve(x->jointree.size());
    query->join_right_tables.reserve(x->jointree.size());
    query->join_on_conds.reserve(x->jointree.size());
    query->join_on_exprs.reserve(x->jointree.size());
    for (const auto& join : x->jointree) {
        query->join_types.push_back(join->join_type);
        query->join_right_tables.push_back(join->right.table_name);

        std::vector<Condition> join_conds;
        std::vector<std::shared_ptr<QueryExpr>> join_exprs;
        if (join->condition != nullptr &&
            !is_legacy_condition_expression(join->condition.get(), outer_cols, outer_aliases)) {
            join_exprs.push_back(std::make_shared<QueryExpr>(
                convert_ast_expr(join->condition.get(), "JOIN ON", scope_cols, query->table_alias_to_name)));
        } else {
            for (const auto& raw_condition : join->conds) {
                if (is_legacy_condition_expression(raw_condition.get(), outer_cols, outer_aliases)) {
                    append_clause(raw_condition.get(), join_conds);
                    continue;
                }
                join_exprs.push_back(std::make_shared<QueryExpr>(
                    convert_ast_expr(raw_condition.get(), "JOIN ON", scope_cols, query->table_alias_to_name)));
            }
        }
        for (auto& condition : join_conds) {
            condition.is_join_on = true;
            query->conds.push_back(condition);
        }
        query->join_on_conds.push_back(std::move(join_conds));
        query->join_on_exprs.push_back(std::move(join_exprs));
    }

    std::vector<Condition> where_conds;
    get_legacy_clauses(x->where_expr.get(), where_conds);
    query->conds.insert(query->conds.end(), where_conds.begin(), where_conds.end());
    resolve_aliases(*query);
    resolve_local_condition_columns(query->conds, all_cols);
    for (auto& join_conds : query->join_on_conds) {
        resolve_local_condition_columns(join_conds, all_cols);
    }
    query->conds.erase(
        std::remove_if(query->conds.begin(), query->conds.end(),
                       [&](const Condition& cond) {
                           auto is_local_table = [&](const std::string& table) {
                               return std::find(query->tables.begin(), query->tables.end(), table) !=
                                      query->tables.end();
                           };
                           auto is_outer_table = [&](const std::string& table) {
                               return outer_aliases.find(table) != outer_aliases.end() ||
                                      std::any_of(outer_cols.begin(), outer_cols.end(),
                                                  [&](const ColMeta& col) { return col.tab_name == table; });
                           };
                           if (!is_local_table(cond.lhs_col.tab_name)) {
                               return is_outer_table(cond.lhs_col.tab_name);
                           }
                           return !cond.is_rhs_val && cond.op != OP_IS_NULL && cond.op != OP_IS_NOT_NULL &&
                                  !is_local_table(cond.rhs_col.tab_name) && is_outer_table(cond.rhs_col.tab_name);
                       }),
        query->conds.end());
    check_clause(query->tables, query->conds);
    for (auto& join_conds : query->join_on_conds) {
        check_clause(query->tables, join_conds);
    }
    for (auto& join_exprs : query->join_on_exprs) {
        for (auto& join_expr : join_exprs) {
            resolve_local_unqualified_columns(*join_expr, visible_cols);
            normalize_query_expr(*join_expr, scope_cols);
            if (!is_boolean_query_expr(*join_expr) || infer_expr_type(*join_expr, scope_cols) != TYPE_INT) {
                throw RMDBError("JOIN ON expression must be boolean");
            }
        }
    }
    for (size_t join_idx = 0; join_idx < x->jointree.size(); ++join_idx) {
        const auto& join = x->jointree[join_idx];
        if (join->join_type != NATURAL_JOIN) {
            continue;
        }
        const auto& left_meta = sm_manager_->db_.get_table(join->left.table_name);
        const auto& right_meta = sm_manager_->db_.get_table(join->right.table_name);
        for (const auto& left_col : left_meta.cols) {
            auto right_col = std::find_if(right_meta.cols.begin(), right_meta.cols.end(),
                                          [&](const ColMeta& col) { return col.name == left_col.name; });
            if (right_col == right_meta.cols.end()) {
                continue;
            }
            Condition condition;
            condition.lhs_col = {.tab_name = left_meta.name, .col_name = left_col.name};
            condition.rhs_col = {.tab_name = right_meta.name, .col_name = right_col->name};
            condition.op = OP_EQ;
            condition.is_rhs_val = false;
            condition.is_join_on = true;
            query->join_on_conds[join_idx].push_back(std::move(condition));
        }
    }
    resolve_local_unqualified_columns(*query, visible_cols);
    for (auto& group_col : query->group_by_cols) {
        if (group_col.tab_name.empty()) {
            try {
                resolve_column_meta(visible_cols, group_col);
            } catch (const ColumnNotFoundError&) {
                // Correlated GROUP BY references are rejected by normal validation.
            }
        }
    }
    validate_select_query(*query, scope_cols);
    query->output_cols = get_query_output_metas(*query, scope_cols);
    query->parse = std::move(owner);
    return query;
}

std::vector<ColMeta> Analyze::get_query_output_metas(const Query& query, const std::vector<ColMeta>& scope_cols) {
    if (query.is_union) {
        return query.union_cols;
    }

    std::vector<ColMeta> result;
    result.reserve(query.select_items.size());
    size_t offset = 0;
    for (size_t i = 0; i < query.select_items.size(); ++i) {
        const auto& item = query.select_items[i];
        ColType type = infer_expr_type(item.expr, scope_cols);
        std::function<int(const QueryExpr&)> expression_len = [&](const QueryExpr& expr) -> int {
            ColType expr_type = infer_expr_type(expr, scope_cols);
            if (expr_type == TYPE_INT) {
                return sizeof(int);
            }
            if (expr_type == TYPE_FLOAT) {
                return sizeof(double);
            }
            if (expr.type == QueryExprType::COLUMN) {
                TabCol resolved_col = expr.col;
                return resolve_column_meta(scope_cols, resolved_col)->len;
            }
            if (expr.type == QueryExprType::VALUE) {
                return std::max<int>(1, expr.value.str_val.size());
            }
            if (expr.type == QueryExprType::CASE_EXPR) {
                int len = 1;
                for (const auto& clause : expr.case_when) {
                    len = std::max(len, expression_len(*clause.second));
                }
                return expr.else_expr == nullptr ? len : std::max(len, expression_len(*expr.else_expr));
            }
            if (expr.type == QueryExprType::SUBQUERY && expr.subquery != nullptr &&
                expr.subquery->output_cols.size() == 1) {
                return expr.subquery->output_cols[0].len;
            }
            if (expr.type == QueryExprType::WINDOW && !expr.window_args.empty()) {
                return expression_len(*expr.window_args.front());
            }
            if (expr.type == QueryExprType::SCALAR_FUNCTION) {
                if (expr.scalar_func == ScalarFuncType::NULLIF && !expr.operands.empty()) {
                    return expression_len(*expr.operands.front());
                }
                int len = 1;
                for (const auto& arg : expr.operands) {
                    len = std::max(len, expression_len(*arg));
                }
                return len;
            }
            return 1;
        };
        int len = expression_len(item.expr);

        ColMeta col;
        col.tab_name.clear();
        col.name = query.output_names[i];
        col.type = type;
        col.len = len;
        col.offset = static_cast<int>(offset);
        offset += static_cast<size_t>(len);
        result.push_back(std::move(col));
    }
    return result;
}

ColMeta Analyze::make_union_col_meta(const ColMeta& current, const ColMeta& next) {
    ColMeta result = current;
    result.tab_name.clear();

    if (current.type == next.type) {
        if (current.type == TYPE_STRING || current.type == TYPE_DATETIME) {
            result.len = std::max(current.len, next.len);
        }
        return result;
    }

    if ((current.type == TYPE_INT && next.type == TYPE_FLOAT) ||
        (current.type == TYPE_FLOAT && next.type == TYPE_INT)) {
        result.type = TYPE_FLOAT;
        result.len = sizeof(double);
        return result;
    }

    throw IncompatibleTypeError(coltype2str(current.type), coltype2str(next.type));
}

void Analyze::validate_union_order_by(Query& query) {
    for (auto& item : query.order_by_items) {
        if (item.output_ordinal >= 0) {
            int ordinal = item.output_ordinal;
            if (ordinal <= 0 || static_cast<size_t>(ordinal) > query.union_cols.size()) {
                throw RMDBError("ORDER BY position is out of range");
            }
            item.expr = make_column_expr({.tab_name = "", .col_name = query.union_cols[ordinal - 1].name});
            item.order_name = query.union_cols[ordinal - 1].name;
        }
        if (item.expr.type != QueryExprType::COLUMN) {
            throw RMDBError("ORDER BY must reference Union output columns");
        }
        if (!item.expr.col.tab_name.empty() && item.expr.col.tab_name != query.union_alias) {
            throw ColumnNotFoundError(item.expr.col.tab_name + "." + item.expr.col.col_name);
        }
        std::string name = !item.order_name.empty() ? item.order_name : item.expr.col.col_name;
        auto pos = std::find_if(query.union_cols.begin(), query.union_cols.end(),
                                [&](const ColMeta& col) { return col.name == name; });
        if (pos == query.union_cols.end()) {
            throw ColumnNotFoundError(name);
        }

        item.expr = make_column_expr({.tab_name = "", .col_name = pos->name});
        item.expr.display_name = pos->name;
        item.order_name = pos->name;
    }
}

std::unique_ptr<Query>
Analyze::analyze_select_from_union_stmt(const ast::SelectFromUnionStmt* x, std::unique_ptr<ast::TreeNode> owner,
                                        const std::vector<ColMeta>& outer_cols,
                                        const std::unordered_map<std::string, std::string>& outer_aliases) {
    return analyze_union_stmt(x->union_stmt.get(), x->alias, x->order_by_items, x->has_limit, x->limit, x->has_offset,
                              x->offset, std::move(owner), outer_cols, outer_aliases);
}

std::unique_ptr<Query> Analyze::analyze_union_stmt(const ast::UnionStmt* union_stmt, std::string alias,
                                                   const std::vector<std::unique_ptr<ast::OrderByItem>>& order_by_items,
                                                   bool has_limit, int limit, bool has_offset, int query_offset,
                                                   std::unique_ptr<ast::TreeNode> owner,
                                                   const std::vector<ColMeta>& outer_cols,
                                                   const std::unordered_map<std::string, std::string>& outer_aliases) {
    if (union_stmt == nullptr || union_stmt->branches.size() < 2) {
        throw RMDBError("UNION requires at least two SELECT branches");
    }

    auto query = std::make_unique<Query>();
    query->is_union = true;
    query->parse = std::move(owner);
    query->union_alias = std::move(alias);
    auto converter = [this, &outer_cols, &outer_aliases](const ast::Expr* expr, const std::string& context_name) {
        return convert_ast_expr(expr, context_name, outer_cols, outer_aliases);
    };
    query->order_by_items.reserve(order_by_items.size());
    for (const auto& raw_item : order_by_items) {
        OrderByItem item;
        item.expr = converter(raw_item->expr.get(), "ORDER BY");
        item.output_ordinal = raw_item->output_ordinal;
        item.is_desc = raw_item->orderby_dir == ast::OrderBy_DESC;
        item.nulls_order = static_cast<int>(raw_item->nulls_order);
        if (item.expr.type == QueryExprType::COLUMN && item.expr.col.tab_name.empty()) {
            item.order_name = item.expr.col.col_name;
        }
        query->order_by_items.push_back(std::move(item));
    }
    query->has_limit = has_limit;
    query->limit = limit;
    query->has_offset = has_offset;
    query->offset = query_offset;
    if (query->has_limit && query->limit < 0) {
        throw RMDBError("LIMIT must be non-negative");
    }
    if (query->has_offset && query->offset < 0) {
        throw RMDBError("OFFSET must be non-negative");
    }

    query->union_branches.reserve(union_stmt->branches.size());
    query->union_all = union_stmt->union_all;
    for (const auto& op : union_stmt->operators) {
        switch (op) {
        case ast::SetOperator::UNION:
            query->set_operators.push_back(QuerySetOperator::UNION);
            break;
        case ast::SetOperator::INTERSECT:
            query->set_operators.push_back(QuerySetOperator::INTERSECT);
            break;
        case ast::SetOperator::EXCEPT:
            query->set_operators.push_back(QuerySetOperator::EXCEPT);
            break;
        }
    }
    for (const auto& branch : union_stmt->branches) {
        auto branch_query = analyze_select_stmt(branch.get(), nullptr, outer_cols, outer_aliases);
        if (!branch_query->order_by_items.empty() || branch_query->has_limit || branch_query->has_offset) {
            throw RMDBError("UNION branches do not support ORDER BY or LIMIT");
        }
        query->union_branches.push_back(std::move(branch_query));
    }

    query->union_cols = query->union_branches.front()->output_cols;
    std::vector<bool> union_col_is_null;
    union_col_is_null.reserve(query->union_cols.size());
    for (const auto& item : query->union_branches.front()->select_items) {
        union_col_is_null.push_back(is_null_query_expr(item.expr));
    }
    for (size_t branch_idx = 1; branch_idx < query->union_branches.size(); ++branch_idx) {
        const auto& branch = *query->union_branches[branch_idx];
        const auto& branch_cols = branch.output_cols;
        if (branch_cols.size() != query->union_cols.size()) {
            throw RMDBError("UNION branches must return the same number of columns");
        }
        for (size_t col_idx = 0; col_idx < query->union_cols.size(); ++col_idx) {
            const bool branch_is_null = is_null_query_expr(branch.select_items[col_idx].expr);
            if (union_col_is_null[col_idx]) {
                if (!branch_is_null) {
                    std::string output_name = query->union_cols[col_idx].name;
                    query->union_cols[col_idx] = branch_cols[col_idx];
                    query->union_cols[col_idx].tab_name.clear();
                    query->union_cols[col_idx].name = std::move(output_name);
                }
            } else if (!branch_is_null) {
                query->union_cols[col_idx] = make_union_col_meta(query->union_cols[col_idx], branch_cols[col_idx]);
            }
            union_col_is_null[col_idx] = union_col_is_null[col_idx] && branch_is_null;
        }
    }

    size_t offset = 0;
    query->output_names.clear();
    query->output_names.reserve(query->union_cols.size());
    for (auto& col : query->union_cols) {
        col.offset = static_cast<int>(offset);
        offset += static_cast<size_t>(col.len);
        query->output_names.push_back(col.name);
    }
    query->output_cols = query->union_cols;

    validate_union_order_by(*query);
    return query;
}

QueryExpr Analyze::convert_ast_expr(const ast::Expr* expr, const std::string& context_name,
                                    const std::vector<ColMeta>& outer_cols,
                                    const std::unordered_map<std::string, std::string>& outer_aliases) {
    if (expr == nullptr) {
        throw InternalError("Unexpected null expression node");
    }

    if (auto col = dynamic_cast<const ast::Col*>(expr); col != nullptr) {
        return make_column_expr({.tab_name = col->tab_name, .col_name = col->col_name});
    }
    if (auto value = dynamic_cast<const ast::Value*>(expr); value != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::VALUE;
        result.value = convert_ast_value_node(value);
        return result;
    }
    if (auto agg = dynamic_cast<const ast::AggExpr*>(expr); agg != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::AGGREGATE;
        result.agg.type = convert_ast_agg_type(agg->func);
        result.agg.is_star = agg->is_star;
        result.agg.is_distinct = agg->is_distinct;
        if (!agg->is_star) {
            if (agg->col == nullptr) {
                throw InternalError("Unexpected null aggregate argument");
            }
            result.agg.col = {.tab_name = agg->col->tab_name, .col_name = agg->col->col_name};
        }
        result.agg.display_name = build_agg_display_name(result.agg);
        result.display_name = result.agg.display_name;
        return result;
    }
    if (auto function = dynamic_cast<const ast::ScalarFuncExpr*>(expr); function != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::SCALAR_FUNCTION;
        result.scalar_func = convert_ast_scalar_func_type(function->func);
        result.display_name = scalar_func_to_string(result.scalar_func) + "(...)";
        result.operands.reserve(function->args.size());
        for (const auto& arg : function->args) {
            result.operands.push_back(
                std::make_shared<QueryExpr>(convert_ast_expr(arg.get(), context_name, outer_cols, outer_aliases)));
        }
        return result;
    }
    if (auto window = dynamic_cast<const ast::WindowExpr*>(expr); window != nullptr) {
        if (context_name != "SELECT" && context_name != "ORDER BY") {
            if (context_name == "WINDOW ARGUMENT" || context_name == "WINDOW PARTITION BY" ||
                context_name == "WINDOW ORDER BY") {
                throw RMDBError("nested window functions are not supported");
            }
            throw RMDBError("Window functions are not allowed in " + context_name);
        }

        QueryExpr result;
        result.type = QueryExprType::WINDOW;
        result.window_func = convert_ast_window_func_type(window->func);
        result.window_args.reserve(window->args.size());
        for (const auto& arg : window->args) {
            result.window_args.push_back(
                std::make_shared<QueryExpr>(convert_ast_expr(arg.get(), "WINDOW ARGUMENT", outer_cols, outer_aliases)));
        }
        result.window_partition_by.reserve(window->partition_by.size());
        for (const auto& partition_expr : window->partition_by) {
            result.window_partition_by.push_back(std::make_shared<QueryExpr>(
                convert_ast_expr(partition_expr.get(), "WINDOW PARTITION BY", outer_cols, outer_aliases)));
        }
        result.window_order_by.reserve(window->order_by.size());
        result.window_order_desc.reserve(window->order_by.size());
        result.window_nulls_order.reserve(window->order_by.size());
        for (const auto& order_item : window->order_by) {
            result.window_order_by.push_back(std::make_shared<QueryExpr>(
                convert_ast_expr(order_item->expr.get(), "WINDOW ORDER BY", outer_cols, outer_aliases)));
            result.window_order_desc.push_back(order_item->orderby_dir == ast::OrderBy_DESC);
            result.window_nulls_order.push_back(static_cast<int>(order_item->nulls_order));
        }
        result.display_name = window_func_to_string(result.window_func) + "(...) OVER";
        return result;
    }
    if (auto arithmetic = dynamic_cast<const ast::ArithmeticExpr*>(expr); arithmetic != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::ARITHMETIC;
        switch (arithmetic->op) {
        case ast::ArithmeticOp::ADD:
            result.arithmetic_op = QueryArithmeticOp::ADD;
            break;
        case ast::ArithmeticOp::SUB:
            result.arithmetic_op = QueryArithmeticOp::SUB;
            break;
        case ast::ArithmeticOp::MUL:
            result.arithmetic_op = QueryArithmeticOp::MUL;
            break;
        case ast::ArithmeticOp::DIV:
            result.arithmetic_op = QueryArithmeticOp::DIV;
            break;
        }
        result.lhs = std::make_shared<QueryExpr>(
            convert_ast_expr(arithmetic->lhs.get(), context_name, outer_cols, outer_aliases));
        result.rhs = std::make_shared<QueryExpr>(
            convert_ast_expr(arithmetic->rhs.get(), context_name, outer_cols, outer_aliases));
        return result;
    }
    if (auto logical = dynamic_cast<const ast::LogicalExpr*>(expr); logical != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::LOGICAL;
        switch (logical->op) {
        case ast::LogicalOp::AND:
            result.logical_op = QueryLogicalOp::AND;
            break;
        case ast::LogicalOp::OR:
            result.logical_op = QueryLogicalOp::OR;
            break;
        case ast::LogicalOp::NOT:
            result.logical_op = QueryLogicalOp::NOT;
            break;
        }
        result.operands.reserve(logical->operands.size());
        for (const auto& operand : logical->operands) {
            result.operands.push_back(
                std::make_shared<QueryExpr>(convert_ast_expr(operand.get(), context_name, outer_cols, outer_aliases)));
        }
        return result;
    }
    if (auto case_expr = dynamic_cast<const ast::CaseExpr*>(expr); case_expr != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::CASE_EXPR;
        result.display_name = "CASE";
        result.case_when.reserve(case_expr->when_clauses.size());
        for (const auto& clause : case_expr->when_clauses) {
            result.case_when.emplace_back(std::make_shared<QueryExpr>(convert_ast_expr(
                                              clause.condition.get(), context_name, outer_cols, outer_aliases)),
                                          std::make_shared<QueryExpr>(convert_ast_expr(
                                              clause.result.get(), context_name, outer_cols, outer_aliases)));
        }
        if (case_expr->else_expr != nullptr) {
            result.else_expr = std::make_shared<QueryExpr>(
                convert_ast_expr(case_expr->else_expr.get(), context_name, outer_cols, outer_aliases));
        }
        return result;
    }
    if (auto binary = dynamic_cast<const ast::BinaryExpr*>(expr); binary != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::PREDICATE;
        result.predicate_op = convert_sv_comp_op(binary->op);
        result.negated = binary->negated;
        switch (binary->quantifier) {
        case ast::Quantifier::NONE:
            result.quantifier = QueryQuantifier::NONE;
            break;
        case ast::Quantifier::ANY:
            result.quantifier = QueryQuantifier::ANY;
            break;
        case ast::Quantifier::ALL:
            result.quantifier = QueryQuantifier::ALL;
            break;
        }
        if (binary->lhs != nullptr) {
            result.lhs = std::make_shared<QueryExpr>(
                convert_ast_expr(binary->lhs.get(), context_name, outer_cols, outer_aliases));
        }
        if (binary->rhs != nullptr) {
            result.rhs = std::make_shared<QueryExpr>(
                convert_ast_expr(binary->rhs.get(), context_name, outer_cols, outer_aliases));
            if ((result.predicate_op == OP_IN || result.quantifier != QueryQuantifier::NONE) &&
                result.rhs->type == QueryExprType::SUBQUERY && result.rhs->subquery->output_cols.size() != 1) {
                throw RMDBError("Subquery predicate must return exactly one column");
            }
        }
        if (binary->rhs_upper != nullptr) {
            result.rhs_upper = std::make_shared<QueryExpr>(
                convert_ast_expr(binary->rhs_upper.get(), context_name, outer_cols, outer_aliases));
        }
        result.rhs_values.reserve(binary->rhs_list.size());
        for (const auto& rhs_value : binary->rhs_list) {
            result.rhs_values.push_back(std::make_shared<QueryExpr>(
                convert_ast_expr(rhs_value.get(), context_name, outer_cols, outer_aliases)));
        }
        return result;
    }
    if (auto subquery = dynamic_cast<const ast::SubqueryExpr*>(expr); subquery != nullptr) {
        QueryExpr result;
        result.type = QueryExprType::SUBQUERY;
        auto query = analyze_subquery(subquery->query.get(), outer_cols, outer_aliases);
        result.subquery = std::shared_ptr<Query>(std::move(query));
        result.display_name = "SUBQUERY";
        return result;
    }

    throw InternalError(context_name + " contains an unsupported expression type");
}

std::unique_ptr<Query> Analyze::analyze_subquery(const ast::TreeNode* root, const std::vector<ColMeta>& outer_cols,
                                                 const std::unordered_map<std::string, std::string>& outer_aliases) {
    if (root == nullptr) {
        throw InternalError("Unexpected null subquery");
    }
    if (root->type == ast::AstType::SelectStmt) {
        return analyze_select_stmt(static_cast<const ast::SelectStmt*>(root), nullptr, outer_cols, outer_aliases);
    }
    if (root->type == ast::AstType::SelectFromUnionStmt) {
        return analyze_select_from_union_stmt(static_cast<const ast::SelectFromUnionStmt*>(root), nullptr, outer_cols,
                                              outer_aliases);
    }
    if (root->type == ast::AstType::UnionStmt) {
        return analyze_union_stmt(static_cast<const ast::UnionStmt*>(root), "", {}, false, 0, false, 0, nullptr,
                                  outer_cols, outer_aliases);
    }
    throw RMDBError("Subquery must be a SELECT statement");
}

TabCol Analyze::check_column(const std::vector<ColMeta>& all_cols, TabCol target) {
    resolve_column_meta(all_cols, target);
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string>& tab_names, std::vector<ColMeta>& all_cols) {
    for (const auto& sel_tab_name : tab_names) {
        const auto& sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::unique_ptr<ast::BinaryExpr>>& sv_conds, std::vector<Condition>& conds) {
    conds.clear();
    conds.reserve(sv_conds.size());
    for (const auto& expr : sv_conds) {
        append_clause(expr.get(), conds);
    }
}

void Analyze::append_clause(const ast::BinaryExpr* expr, std::vector<Condition>& conds) {
    if (expr == nullptr || dynamic_cast<const ast::Col*>(expr->lhs.get()) == nullptr) {
        return;
    }
    if (expr->op != ast::SV_OP_IS_NULL && expr->op != ast::SV_OP_IS_NOT_NULL && expr->op != ast::SV_OP_EXISTS) {
        if (std::any_of(expr->rhs_list.begin(), expr->rhs_list.end(), [](const auto& raw_value) {
                auto value = dynamic_cast<const ast::Value*>(raw_value.get());
                return value == nullptr || value->type == ast::AstType::NullLit;
            })) {
            return;
        }
        if (expr->rhs_upper != nullptr && dynamic_cast<const ast::Value*>(expr->rhs_upper.get()) == nullptr) {
            return;
        }
        if (expr->rhs != nullptr && dynamic_cast<const ast::Value*>(expr->rhs.get()) == nullptr &&
            dynamic_cast<const ast::Col*>(expr->rhs.get()) == nullptr) {
            return;
        }
        if (auto rhs_value = dynamic_cast<const ast::Value*>(expr->rhs.get());
            rhs_value != nullptr && rhs_value->type == ast::AstType::NullLit) {
            return;
        }
    }

    Condition cond;
    cond.lhs_col = extract_ast_column(expr->lhs, "WHERE");
    cond.op = convert_sv_comp_op(expr->op);
    cond.negated = expr->negated;

    if (cond.op == OP_IS_NULL || cond.op == OP_IS_NOT_NULL || cond.op == OP_EXISTS) {
        conds.push_back(cond);
        return;
    }

    for (const auto& raw_value : expr->rhs_list) {
        auto rhs_val = dynamic_cast<const ast::Value*>(raw_value.get());
        if (rhs_val == nullptr) {
            return;
        }
        cond.rhs_vals.push_back(convert_sv_value(rhs_val));
    }

    if (!cond.rhs_vals.empty()) {
        cond.is_rhs_val = true;
    } else if (auto rhs_val = dynamic_cast<const ast::Value*>(expr->rhs.get()); rhs_val != nullptr) {
        cond.is_rhs_val = true;
        cond.rhs_val = convert_sv_value(rhs_val);
        cond.rhs_display = rhs_val->display_text;
    } else if (auto rhs_col = dynamic_cast<const ast::Col*>(expr->rhs.get()); rhs_col != nullptr) {
        cond.is_rhs_val = false;
        cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
    } else {
        return;
    }
    if (expr->rhs_upper != nullptr) {
        auto rhs_upper = dynamic_cast<const ast::Value*>(expr->rhs_upper.get());
        if (rhs_upper == nullptr) {
            return;
        }
        cond.rhs_upper = convert_sv_value(rhs_upper);
        cond.has_rhs_upper = true;
    }
    conds.push_back(cond);
}

void Analyze::get_legacy_clauses(const ast::Expr* expression, std::vector<Condition>& conds) {
    if (expression == nullptr) {
        return;
    }
    if (auto logical = dynamic_cast<const ast::LogicalExpr*>(expression); logical != nullptr) {
        if (logical->op != ast::LogicalOp::AND) {
            return;
        }
        for (const auto& operand : logical->operands) {
            get_legacy_clauses(operand.get(), conds);
        }
        return;
    }
    if (auto binary = dynamic_cast<const ast::BinaryExpr*>(expression); binary != nullptr) {
        append_clause(binary, conds);
    }
}

void Analyze::check_clause(const std::vector<std::string>& tab_names, std::vector<Condition>& conds) {
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);

    for (auto& cond : conds) {
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        auto lhs_col = sm_manager_->db_.get_table(cond.lhs_col.tab_name).get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;

        if (cond.op == OP_IS_NULL || cond.op == OP_IS_NOT_NULL || cond.op == OP_EXISTS) {
            continue;
        }

        if (cond.op == OP_LIKE && lhs_type != TYPE_STRING && lhs_type != TYPE_DATETIME) {
            throw IncompatibleTypeError(coltype2str(lhs_type), "string");
        }

        ColType rhs_type;
        if (cond.op == OP_IN) {
            if (cond.rhs_vals.empty()) {
                throw RMDBError("IN list must not be empty");
            }
            for (auto& rhs_val : cond.rhs_vals) {
                rhs_type = rhs_val.type;
                if (!can_cast(lhs_type, rhs_type)) {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                }
                cast_value(rhs_val, lhs_type);
                rhs_val.init_raw(lhs_col->len);
            }
            continue;
        }
        if (cond.op == OP_BETWEEN) {
            if (!cond.has_rhs_upper) {
                throw RMDBError("BETWEEN requires two bounds");
            }
            for (Value* bound : {&cond.rhs_val, &cond.rhs_upper}) {
                rhs_type = bound->type;
                if (!can_cast(lhs_type, rhs_type)) {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                }
                cast_value(*bound, lhs_type);
                bound->init_raw(lhs_col->len);
            }
            continue;
        }
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            if (!can_cast(lhs_type, rhs_type)) {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }
            cast_value(cond.rhs_val, lhs_type);
            cond.rhs_val.init_raw(lhs_col->len);
            continue;
        } else {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
            auto rhs_col = sm_manager_->db_.get_table(cond.rhs_col.tab_name).get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
            if (cond.op == OP_LIKE && rhs_type != TYPE_STRING && rhs_type != TYPE_DATETIME) {
                throw IncompatibleTypeError(coltype2str(rhs_type), "string");
            }
        }

        if (!can_cast(lhs_type, rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

Value Analyze::convert_sv_value(const ast::Value* sv_val) {
    return convert_ast_value_node(sv_val);
}

void Analyze::cast_value(Value& val, ColType to) {
    if (to == TYPE_FLOAT && val.type == TYPE_INT) {
        val.set_float(static_cast<double>(val.int_val));
        return;
    }
    if (to == TYPE_INT && val.type == TYPE_FLOAT) {
        val.set_int(checked_int_cast(val.float_val));
        return;
    }
    if (to == TYPE_DATETIME && val.type == TYPE_STRING) {
        val.type = TYPE_DATETIME;
        return;
    }
    if (to == TYPE_STRING && val.type == TYPE_DATETIME) {
        val.type = TYPE_STRING;
        return;
    }
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    switch (op) {
    case ast::SV_OP_EQ:
        return OP_EQ;
    case ast::SV_OP_NE:
        return OP_NE;
    case ast::SV_OP_LT:
        return OP_LT;
    case ast::SV_OP_GT:
        return OP_GT;
    case ast::SV_OP_LE:
        return OP_LE;
    case ast::SV_OP_GE:
        return OP_GE;
    case ast::SV_OP_LIKE:
        return OP_LIKE;
    case ast::SV_OP_IN:
        return OP_IN;
    case ast::SV_OP_BETWEEN:
        return OP_BETWEEN;
    case ast::SV_OP_IS_NULL:
        return OP_IS_NULL;
    case ast::SV_OP_IS_NOT_NULL:
        return OP_IS_NOT_NULL;
    case ast::SV_OP_EXISTS:
        return OP_EXISTS;
    }
    throw InternalError("Unexpected comparison operator");
}
