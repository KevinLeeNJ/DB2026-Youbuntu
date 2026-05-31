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

#include "analyze_expr_internal.h"

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace analyze_internal {

// =============================================================================
// Template traits for AST node type detection at compile time
// =============================================================================

template <typename T, typename = void> struct has_select_items_member : std::false_type {};

template <typename T>
struct has_select_items_member<T, std::void_t<decltype(std::declval<T>().select_items)>> : std::true_type {};

template <typename T, typename = void> struct has_group_by_cols_member : std::false_type {};

template <typename T>
struct has_group_by_cols_member<T, std::void_t<decltype(std::declval<T>().group_by_cols)>> : std::true_type {};

template <typename T, typename = void> struct has_having_conds_member : std::false_type {};

template <typename T>
struct has_having_conds_member<T, std::void_t<decltype(std::declval<T>().having_conds)>> : std::true_type {};

template <typename T, typename = void> struct has_order_by_items_member : std::false_type {};

template <typename T>
struct has_order_by_items_member<T, std::void_t<decltype(std::declval<T>().order_by_items)>> : std::true_type {};

template <typename T, typename = void> struct has_has_limit_member : std::false_type {};

template <typename T>
struct has_has_limit_member<T, std::void_t<decltype(std::declval<T>().has_limit)>> : std::true_type {};

template <typename T, typename = void> struct has_limit_member : std::false_type {};

template <typename T> struct has_limit_member<T, std::void_t<decltype(std::declval<T>().limit)>> : std::true_type {};

template <typename T, typename = void> struct has_has_select_star_member : std::false_type {};

template <typename T>
struct has_has_select_star_member<T, std::void_t<decltype(std::declval<T>().has_select_star)>> : std::true_type {};

template <typename T, typename = void> struct has_order_member : std::false_type {};

template <typename T> struct has_order_member<T, std::void_t<decltype(std::declval<T>().order)>> : std::true_type {};

// =============================================================================
// Non-template function declarations (forward declarations needed by templates)
// =============================================================================

void append_star_projection(Query& query, const std::vector<ColMeta>& all_cols);
void rebuild_select_outputs(Query& query, const std::vector<ColMeta>& all_cols);
void validate_having(Query& query, const std::vector<ColMeta>& all_cols);
void validate_order_by(Query& query, const std::vector<ColMeta>& all_cols);
void validate_group_by(Query& query);
void validate_select_without_group_by(const Query& query);
void validate_select_query(Query& query, const std::vector<ColMeta>& all_cols);

// =============================================================================
// Template function definitions (must be in header for instantiation)
// =============================================================================

template <typename ExprPtrT> TabCol extract_ast_column(const ExprPtrT& expr_node, const std::string& clause_name) {
    auto col = std::dynamic_pointer_cast<ast::Col>(expr_node);
    if (col == nullptr) {
        throw RMDBError(clause_name + " clause does not allow aggregate expressions");
    }
    return {.tab_name = col->tab_name, .col_name = col->col_name};
}

template <typename ExprPtrT>
QueryExpr convert_simple_ast_expr(const ExprPtrT& expr_node, const std::string& context_name) {
    if (expr_node == nullptr) {
        throw InternalError("Unexpected null expression node");
    }
    if (auto col = std::dynamic_pointer_cast<ast::Col>(expr_node); col != nullptr) {
        return make_column_expr({.tab_name = col->tab_name, .col_name = col->col_name});
    }
    if (auto val = std::dynamic_pointer_cast<ast::Value>(expr_node); val != nullptr) {
        QueryExpr expr;
        expr.type = QueryExprType::VALUE;
        expr.value = convert_ast_value_node(val);
        return expr;
    }
    if (auto agg = std::dynamic_pointer_cast<ast::AggExpr>(expr_node); agg != nullptr) {
        QueryExpr expr;
        expr.type = QueryExprType::AGGREGATE;
        expr.agg.type = convert_ast_agg_type(agg->func);
        expr.agg.is_star = agg->is_star;
        if (!agg->is_star) {
            if (agg->col == nullptr) {
                throw InternalError("Unexpected null aggregate argument");
            }
            expr.agg.col = {.tab_name = agg->col->tab_name, .col_name = agg->col->col_name};
        }
        expr.agg.display_name = build_agg_display_name(expr.agg);
        expr.display_name = expr.agg.display_name;
        return expr;
    }

    throw InternalError(context_name + " contains an unsupported expression type");
}

template <typename SelectStmtT>
void populate_select_items_from_ast(Query& query, const SelectStmtT& stmt, const std::vector<ColMeta>& all_cols) {
    query.select_items.clear();
    query.has_select_star = false;

    if constexpr (has_select_items_member<SelectStmtT>::value) {
        if constexpr (has_has_select_star_member<SelectStmtT>::value) {
            query.has_select_star = stmt.has_select_star;
        }
        if (stmt.select_items.empty()) {
            query.has_select_star = true;
            append_star_projection(query, all_cols);
            return;
        }
        for (const auto& raw_item : stmt.select_items) {
            if (raw_item == nullptr) {
                throw InternalError("Unexpected null select item");
            }
            SelectItem item;
            item.expr = convert_simple_ast_expr(raw_item->expr, "SELECT");
            item.alias = raw_item->alias;
            query.select_items.push_back(std::move(item));
        }
        return;
    }

    query.has_select_star = stmt.cols.empty();
    if (query.has_select_star) {
        append_star_projection(query, all_cols);
        return;
    }
    for (const auto& raw_col : stmt.cols) {
        if (raw_col == nullptr) {
            throw InternalError("Unexpected null select column");
        }
        SelectItem item;
        item.expr = make_column_expr({.tab_name = raw_col->tab_name, .col_name = raw_col->col_name});
        query.select_items.push_back(std::move(item));
    }
}

template <typename SelectStmtT> void populate_group_by_from_ast(Query& query, const SelectStmtT& stmt) {
    query.group_by_cols.clear();
    if constexpr (has_group_by_cols_member<SelectStmtT>::value) {
        query.group_by_cols.reserve(stmt.group_by_cols.size());
        for (const auto& raw_col : stmt.group_by_cols) {
            if (raw_col == nullptr) {
                throw InternalError("Unexpected null GROUP BY column");
            }
            query.group_by_cols.push_back({.tab_name = raw_col->tab_name, .col_name = raw_col->col_name});
        }
    }
}

template <typename SelectStmtT> void populate_having_from_ast(Query& query, const SelectStmtT& stmt) {
    query.having_conds.clear();
    if constexpr (has_having_conds_member<SelectStmtT>::value) {
        query.having_conds.reserve(stmt.having_conds.size());
        for (const auto& raw_cond : stmt.having_conds) {
            if (raw_cond == nullptr) {
                throw InternalError("Unexpected null HAVING condition");
            }
            HavingCondition cond;
            cond.lhs = convert_simple_ast_expr(raw_cond->lhs, "HAVING");
            cond.op = static_cast<CompOp>(0); // overwritten below
            switch (raw_cond->op) {
            case ast::SV_OP_EQ:
                cond.op = OP_EQ;
                break;
            case ast::SV_OP_NE:
                cond.op = OP_NE;
                break;
            case ast::SV_OP_LT:
                cond.op = OP_LT;
                break;
            case ast::SV_OP_GT:
                cond.op = OP_GT;
                break;
            case ast::SV_OP_LE:
                cond.op = OP_LE;
                break;
            case ast::SV_OP_GE:
                cond.op = OP_GE;
                break;
            }
            if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(raw_cond->rhs); rhs_val != nullptr) {
                cond.is_rhs_val = true;
                cond.rhs_val = convert_ast_value_node(rhs_val);
            } else {
                cond.is_rhs_val = false;
                cond.rhs_expr = convert_simple_ast_expr(raw_cond->rhs, "HAVING");
            }
            query.having_conds.push_back(std::move(cond));
        }
    }
}

template <typename SelectStmtT> void populate_order_by_from_ast(Query& query, const SelectStmtT& stmt) {
    query.order_by_items.clear();
    if constexpr (has_order_by_items_member<SelectStmtT>::value) {
        query.order_by_items.reserve(stmt.order_by_items.size());
        for (const auto& raw_item : stmt.order_by_items) {
            if (raw_item == nullptr) {
                throw InternalError("Unexpected null ORDER BY item");
            }
            OrderByItem item;
            item.expr = convert_simple_ast_expr(raw_item->expr, "ORDER BY");
            item.is_desc = raw_item->orderby_dir == ast::OrderBy_DESC;
            if (item.expr.type == QueryExprType::COLUMN && item.expr.col.tab_name.empty()) {
                item.order_name = item.expr.col.col_name;
            }
            query.order_by_items.push_back(std::move(item));
        }
        return;
    }

    if constexpr (has_order_member<SelectStmtT>::value) {
        if (stmt.order != nullptr) {
            OrderByItem item;
            item.expr =
                make_column_expr({.tab_name = stmt.order->cols->tab_name, .col_name = stmt.order->cols->col_name});
            item.is_desc = stmt.order->orderby_dir == ast::OrderBy_DESC;
            if (item.expr.col.tab_name.empty()) {
                item.order_name = item.expr.col.col_name;
            }
            query.order_by_items.push_back(std::move(item));
        }
    }
}

template <typename SelectStmtT> void populate_limit_from_ast(Query& query, const SelectStmtT& stmt) {
    query.has_limit = false;
    query.limit = 0;
    if constexpr (has_has_limit_member<SelectStmtT>::value && has_limit_member<SelectStmtT>::value) {
        query.has_limit = stmt.has_limit;
        query.limit = stmt.limit;
    }
}

} // namespace analyze_internal
