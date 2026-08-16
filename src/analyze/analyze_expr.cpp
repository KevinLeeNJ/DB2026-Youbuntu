/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze_expr_internal.h"

namespace analyze_internal {

Value convert_ast_value_node(const ast::Value* sv_val) {
    Value val;
    switch (sv_val->type) {
    case ast::AstType::IntLit: {
        auto int_lit = static_cast<const ast::IntLit*>(sv_val);
        val.set_int(int_lit->val);
        break;
    }
    case ast::AstType::FloatLit: {
        auto float_lit = static_cast<const ast::FloatLit*>(sv_val);
        val.set_float(float_lit->val);
        break;
    }
    case ast::AstType::StringLit: {
        auto str_lit = static_cast<const ast::StringLit*>(sv_val);
        val.set_str(str_lit->val);
        break;
    }
    case ast::AstType::BoolLit: {
        auto bool_lit = static_cast<const ast::BoolLit*>(sv_val);
        val.set_int(bool_lit->val ? 1 : 0);
        break;
    }
    case ast::AstType::NullLit:
        val.set_null();
        break;
    default:
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

const ColMeta* resolve_column_meta(const std::vector<ColMeta>& all_cols, TabCol& target) {
    if (target.tab_name.empty()) {
        const ColMeta* found = nullptr;
        for (const auto& col : all_cols) {
            if (col.name != target.col_name) {
                continue;
            }
            if (found != nullptr) {
                throw AmbiguousColumnError(target.col_name);
            }
            found = &col;
        }
        if (found == nullptr) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = found->tab_name;
        return found;
    }

    for (const auto& col : all_cols) {
        if (col.tab_name == target.tab_name && col.name == target.col_name) {
            return &col;
        }
    }
    throw ColumnNotFoundError(target.col_name);
}

bool can_cast_types(ColType lhs_type, ColType rhs_type) {
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

bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

bool is_groupable_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT || type == TYPE_STRING || type == TYPE_DATETIME;
}

std::string agg_type_to_string(AggType type) {
    switch (type) {
    case AggType::COUNT:
        return "COUNT";
    case AggType::MAX:
        return "MAX";
    case AggType::MIN:
        return "MIN";
    case AggType::SUM:
        return "SUM";
    case AggType::AVG:
        return "AVG";
    }
    throw InternalError("Unexpected aggregate type");
}

AggType convert_ast_agg_type(ast::AggFuncType type) {
    switch (type) {
    case ast::AGG_COUNT:
        return AggType::COUNT;
    case ast::AGG_MAX:
        return AggType::MAX;
    case ast::AGG_MIN:
        return AggType::MIN;
    case ast::AGG_SUM:
        return AggType::SUM;
    case ast::AGG_AVG:
        return AggType::AVG;
    }
    throw InternalError("Unexpected aggregate function type");
}

bool same_tab_col(const TabCol& lhs, const TabCol& rhs) {
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}

bool contains_group_col(const std::vector<TabCol>& group_by_cols, const TabCol& col) {
    for (const auto& group_col : group_by_cols) {
        if (same_tab_col(group_col, col)) {
            return true;
        }
    }
    return false;
}

QueryExpr make_column_expr(TabCol col, std::string display_name) {
    QueryExpr expr;
    expr.type = QueryExprType::COLUMN;
    expr.col = std::move(col);
    expr.display_name = display_name.empty() ? expr.col.col_name : std::move(display_name);
    return expr;
}

std::string build_agg_display_name(const AggExpr& agg) {
    std::string name = agg_type_to_string(agg.type) + "(";
    if (agg.is_star) {
        name += "*";
    } else {
        if (agg.is_distinct) {
            name += "DISTINCT ";
        }
        name += agg.col.col_name;
    }
    name += ")";
    return name;
}

void validate_agg_expr(AggExpr& agg, const std::vector<ColMeta>& all_cols) {
    if (agg.is_star) {
        if (agg.type != AggType::COUNT) {
            throw RMDBError("Only COUNT(*) is supported for '*' aggregate arguments");
        }
        agg.display_name = build_agg_display_name(agg);
        return;
    }

    TabCol resolved_col = agg.col;
    const ColMeta* col_meta = resolve_column_meta(all_cols, resolved_col);
    agg.col = resolved_col;

    switch (agg.type) {
    case AggType::COUNT:
        if (col_meta->type != TYPE_INT && col_meta->type != TYPE_FLOAT && col_meta->type != TYPE_STRING &&
            col_meta->type != TYPE_DATETIME) {
            throw RMDBError("COUNT(col) only supports int, float, and string columns");
        }
        break;
    case AggType::MAX:
    case AggType::MIN:
        if (col_meta->type != TYPE_INT && col_meta->type != TYPE_FLOAT && col_meta->type != TYPE_STRING &&
            col_meta->type != TYPE_DATETIME) {
            throw RMDBError(agg_type_to_string(agg.type) + " only supports int, float, and string columns");
        }
        break;
    case AggType::SUM:
    case AggType::AVG:
        if (!is_numeric_type(col_meta->type)) {
            throw RMDBError(agg_type_to_string(agg.type) + " only supports int and float columns");
        }
        break;
    }
    agg.display_name = build_agg_display_name(agg);
}

std::string window_func_to_string(WindowFuncType type) {
    switch (type) {
    case WindowFuncType::ROW_NUMBER:
        return "ROW_NUMBER";
    case WindowFuncType::RANK:
        return "RANK";
    case WindowFuncType::DENSE_RANK:
        return "DENSE_RANK";
    case WindowFuncType::LAG:
        return "LAG";
    case WindowFuncType::LEAD:
        return "LEAD";
    case WindowFuncType::SUM:
        return "SUM";
    case WindowFuncType::AVG:
        return "AVG";
    }
    throw InternalError("Unexpected window function type");
}

void validate_window_expr(QueryExpr& expr, const std::vector<ColMeta>& all_cols) {
    const auto function_name = window_func_to_string(expr.window_func);
    for (const auto& arg : expr.window_args) {
        if (arg == nullptr) {
            throw InternalError("Window function is missing an argument");
        }
        if (arg->type == QueryExprType::WINDOW) {
            throw RMDBError("nested window functions are not supported");
        }
    }
    for (const auto& partition_expr : expr.window_partition_by) {
        if (partition_expr == nullptr) {
            throw InternalError("Window PARTITION BY is missing an expression");
        }
        if (partition_expr->type == QueryExprType::WINDOW) {
            throw RMDBError("nested window functions are not supported");
        }
        (void)infer_expr_type(*partition_expr, all_cols);
    }
    if (expr.window_order_by.size() != expr.window_order_desc.size() ||
        expr.window_order_by.size() != expr.window_nulls_order.size()) {
        throw InternalError("Window ORDER BY metadata is inconsistent");
    }
    for (const auto& order_expr : expr.window_order_by) {
        if (order_expr == nullptr) {
            throw InternalError("Window ORDER BY is missing an expression");
        }
        if (order_expr->type == QueryExprType::WINDOW) {
            throw RMDBError("nested window functions are not supported");
        }
        (void)infer_expr_type(*order_expr, all_cols);
    }

    switch (expr.window_func) {
    case WindowFuncType::ROW_NUMBER:
    case WindowFuncType::RANK:
    case WindowFuncType::DENSE_RANK:
        if (!expr.window_args.empty()) {
            throw RMDBError(function_name + " does not accept arguments");
        }
        break;
    case WindowFuncType::LAG:
    case WindowFuncType::LEAD: {
        if (expr.window_args.empty() || expr.window_args.size() > 3) {
            throw RMDBError(function_name + " requires one to three arguments");
        }
        const ColType value_type = infer_expr_type(*expr.window_args.front(), all_cols);
        if (expr.window_args.size() >= 2) {
            const auto& offset = *expr.window_args[1];
            if (offset.type != QueryExprType::VALUE || offset.value.is_null || offset.value.type != TYPE_INT) {
                throw RMDBError("window offset must be a non-negative integer literal");
            }
            if (offset.value.int_val < 0) {
                throw RMDBError("window offset must be non-negative");
            }
        }
        if (expr.window_args.size() == 3) {
            const ColType default_type = infer_expr_type(*expr.window_args[2], all_cols);
            if (expr.window_args[2]->type != QueryExprType::VALUE ||
                (!expr.window_args[2]->value.is_null && !can_cast_types(value_type, default_type))) {
                throw IncompatibleTypeError(coltype2str(value_type), coltype2str(default_type));
            }
        }
        break;
    }
    case WindowFuncType::SUM:
    case WindowFuncType::AVG:
        if (expr.window_args.size() != 1) {
            throw RMDBError(function_name + " window function requires one argument");
        }
        if (!is_numeric_type(infer_expr_type(*expr.window_args.front(), all_cols))) {
            throw RMDBError(function_name + " window function requires a numeric expression");
        }
        break;
    }
}

void normalize_query_expr(QueryExpr& expr, const std::vector<ColMeta>& all_cols) {
    switch (expr.type) {
    case QueryExprType::COLUMN: {
        TabCol resolved_col = expr.col;
        resolve_column_meta(all_cols, resolved_col);
        expr.col = resolved_col;
        if (expr.display_name.empty()) {
            expr.display_name = expr.col.col_name;
        }
        break;
    }
    case QueryExprType::AGGREGATE:
        validate_agg_expr(expr.agg, all_cols);
        expr.display_name = expr.agg.display_name;
        break;
    case QueryExprType::WINDOW:
        for (auto& arg : expr.window_args) {
            if (arg == nullptr) {
                throw InternalError("Window function is missing an argument");
            }
            normalize_query_expr(*arg, all_cols);
        }
        for (auto& partition_expr : expr.window_partition_by) {
            if (partition_expr == nullptr) {
                throw InternalError("Window PARTITION BY is missing an expression");
            }
            normalize_query_expr(*partition_expr, all_cols);
        }
        for (auto& order_expr : expr.window_order_by) {
            if (order_expr == nullptr) {
                throw InternalError("Window ORDER BY is missing an expression");
            }
            normalize_query_expr(*order_expr, all_cols);
        }
        validate_window_expr(expr, all_cols);
        break;
    case QueryExprType::VALUE:
        break;
    case QueryExprType::ARITHMETIC:
        if (expr.lhs == nullptr || expr.rhs == nullptr) {
            throw InternalError("Arithmetic expression is missing an operand");
        }
        normalize_query_expr(*expr.lhs, all_cols);
        normalize_query_expr(*expr.rhs, all_cols);
        break;
    case QueryExprType::LOGICAL:
        for (auto& operand : expr.operands) {
            if (operand == nullptr) {
                throw InternalError("Logical expression is missing an operand");
            }
            normalize_query_expr(*operand, all_cols);
        }
        break;
    case QueryExprType::CASE_EXPR:
        for (auto& clause : expr.case_when) {
            if (clause.first == nullptr || clause.second == nullptr) {
                throw InternalError("CASE expression is missing an operand");
            }
            normalize_query_expr(*clause.first, all_cols);
            normalize_query_expr(*clause.second, all_cols);
        }
        if (expr.else_expr != nullptr) {
            normalize_query_expr(*expr.else_expr, all_cols);
        }
        break;
    case QueryExprType::PREDICATE:
        if (expr.lhs != nullptr) {
            normalize_query_expr(*expr.lhs, all_cols);
        }
        if (expr.rhs != nullptr) {
            normalize_query_expr(*expr.rhs, all_cols);
        }
        if (expr.rhs_upper != nullptr) {
            normalize_query_expr(*expr.rhs_upper, all_cols);
        }
        for (auto& value : expr.rhs_values) {
            normalize_query_expr(*value, all_cols);
        }
        break;
    case QueryExprType::SUBQUERY:
        if (expr.subquery == nullptr) {
            throw InternalError("Subquery expression is missing its query");
        }
        break;
    }
}

ColType infer_expr_type(const QueryExpr& expr, const std::vector<ColMeta>& all_cols) {
    switch (expr.type) {
    case QueryExprType::COLUMN: {
        TabCol resolved_col = expr.col;
        const ColMeta* col_meta = resolve_column_meta(all_cols, resolved_col);
        return col_meta->type;
    }
    case QueryExprType::VALUE:
        return expr.value.type;
    case QueryExprType::AGGREGATE:
        switch (expr.agg.type) {
        case AggType::COUNT:
            return TYPE_INT;
        case AggType::AVG:
        case AggType::SUM:
            return TYPE_FLOAT;
        case AggType::MAX:
        case AggType::MIN: {
            if (expr.agg.is_star) {
                throw InternalError("Unexpected '*' argument for non-COUNT aggregate");
            }
            TabCol resolved_col = expr.agg.col;
            const ColMeta* col_meta = resolve_column_meta(all_cols, resolved_col);
            return col_meta->type;
        }
        }
    case QueryExprType::WINDOW:
        switch (expr.window_func) {
        case WindowFuncType::ROW_NUMBER:
        case WindowFuncType::RANK:
        case WindowFuncType::DENSE_RANK:
            return TYPE_INT;
        case WindowFuncType::LAG:
        case WindowFuncType::LEAD:
        case WindowFuncType::SUM:
            if (expr.window_args.empty()) {
                throw InternalError("Window function is missing its value expression");
            }
            return infer_expr_type(*expr.window_args.front(), all_cols);
        case WindowFuncType::AVG:
            return TYPE_FLOAT;
        }
    case QueryExprType::ARITHMETIC: {
        if (expr.lhs == nullptr || expr.rhs == nullptr) {
            throw InternalError("Arithmetic expression is missing an operand");
        }
        ColType lhs_type = infer_expr_type(*expr.lhs, all_cols);
        ColType rhs_type = infer_expr_type(*expr.rhs, all_cols);
        if (!is_numeric_type(lhs_type) || !is_numeric_type(rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        return lhs_type == TYPE_FLOAT || rhs_type == TYPE_FLOAT ? TYPE_FLOAT : TYPE_INT;
    }
    case QueryExprType::LOGICAL:
    case QueryExprType::PREDICATE:
        return TYPE_INT;
    case QueryExprType::CASE_EXPR: {
        ColType result_type = TYPE_INT;
        bool have_result = false;
        for (const auto& clause : expr.case_when) {
            ColType clause_type = infer_expr_type(*clause.second, all_cols);
            if (!have_result) {
                result_type = clause_type;
                have_result = true;
            } else if (result_type != clause_type) {
                if (!can_cast_types(result_type, clause_type)) {
                    throw IncompatibleTypeError(coltype2str(result_type), coltype2str(clause_type));
                }
                if (result_type == TYPE_INT && clause_type == TYPE_FLOAT) {
                    result_type = TYPE_FLOAT;
                }
            }
        }
        if (expr.else_expr != nullptr) {
            ColType else_type = infer_expr_type(*expr.else_expr, all_cols);
            if (!have_result) {
                result_type = else_type;
                have_result = true;
            } else if (result_type != else_type) {
                if (!can_cast_types(result_type, else_type)) {
                    throw IncompatibleTypeError(coltype2str(result_type), coltype2str(else_type));
                }
                if (result_type == TYPE_INT && else_type == TYPE_FLOAT) {
                    result_type = TYPE_FLOAT;
                }
            }
        }
        return result_type;
    }
    case QueryExprType::SUBQUERY:
        if (expr.subquery == nullptr || expr.subquery->union_cols.empty() && expr.subquery->select_items.size() != 1) {
            throw RMDBError("Scalar subquery must return exactly one column");
        }
        if (expr.subquery->is_union) {
            if (expr.subquery->union_cols.size() != 1) {
                throw RMDBError("Scalar subquery must return exactly one column");
            }
            return expr.subquery->union_cols[0].type;
        }
        return infer_expr_type(expr.subquery->select_items.front().expr, all_cols);
    }
    throw InternalError("Unexpected query expression type");
}

bool same_query_expr(const QueryExpr& lhs, const QueryExpr& rhs) {
    if (lhs.type != rhs.type) {
        return false;
    }
    switch (lhs.type) {
    case QueryExprType::COLUMN:
        return same_tab_col(lhs.col, rhs.col);
    case QueryExprType::VALUE:
        if (lhs.value.type != rhs.value.type || lhs.value.is_null != rhs.value.is_null) {
            return false;
        }
        if (lhs.value.is_null) {
            return true;
        }
        switch (lhs.value.type) {
        case TYPE_INT:
            return lhs.value.int_val == rhs.value.int_val;
        case TYPE_FLOAT:
            return lhs.value.float_val == rhs.value.float_val;
        case TYPE_STRING:
        case TYPE_DATETIME:
            return lhs.value.str_val == rhs.value.str_val;
        }
        return false;
    case QueryExprType::AGGREGATE:
        return lhs.agg.type == rhs.agg.type && lhs.agg.is_star == rhs.agg.is_star &&
               lhs.agg.is_distinct == rhs.agg.is_distinct &&
               (lhs.agg.is_star || same_tab_col(lhs.agg.col, rhs.agg.col));
    case QueryExprType::WINDOW:
        if (lhs.window_func != rhs.window_func || lhs.window_args.size() != rhs.window_args.size() ||
            lhs.window_partition_by.size() != rhs.window_partition_by.size() ||
            lhs.window_order_by.size() != rhs.window_order_by.size() ||
            lhs.window_order_desc != rhs.window_order_desc || lhs.window_nulls_order != rhs.window_nulls_order) {
            return false;
        }
        for (size_t i = 0; i < lhs.window_args.size(); ++i) {
            if (lhs.window_args[i] == nullptr || rhs.window_args[i] == nullptr ||
                !same_query_expr(*lhs.window_args[i], *rhs.window_args[i])) {
                return false;
            }
        }
        for (size_t i = 0; i < lhs.window_partition_by.size(); ++i) {
            if (lhs.window_partition_by[i] == nullptr || rhs.window_partition_by[i] == nullptr ||
                !same_query_expr(*lhs.window_partition_by[i], *rhs.window_partition_by[i])) {
                return false;
            }
        }
        for (size_t i = 0; i < lhs.window_order_by.size(); ++i) {
            if (lhs.window_order_by[i] == nullptr || rhs.window_order_by[i] == nullptr ||
                !same_query_expr(*lhs.window_order_by[i], *rhs.window_order_by[i])) {
                return false;
            }
        }
        return true;
    case QueryExprType::ARITHMETIC:
        return lhs.arithmetic_op == rhs.arithmetic_op && lhs.lhs != nullptr && lhs.rhs != nullptr &&
               rhs.lhs != nullptr && rhs.rhs != nullptr && same_query_expr(*lhs.lhs, *rhs.lhs) &&
               same_query_expr(*lhs.rhs, *rhs.rhs);
    case QueryExprType::LOGICAL:
        if (lhs.logical_op != rhs.logical_op || lhs.operands.size() != rhs.operands.size()) {
            return false;
        }
        for (size_t i = 0; i < lhs.operands.size(); ++i) {
            if (lhs.operands[i] == nullptr || rhs.operands[i] == nullptr ||
                !same_query_expr(*lhs.operands[i], *rhs.operands[i])) {
                return false;
            }
        }
        return true;
    case QueryExprType::CASE_EXPR:
    case QueryExprType::PREDICATE:
    case QueryExprType::SUBQUERY:
        return lhs.display_name == rhs.display_name;
    }
    return false;
}

bool contains_window_expr(const QueryExpr& expr) {
    if (expr.type == QueryExprType::WINDOW) {
        return true;
    }
    if (expr.lhs != nullptr && contains_window_expr(*expr.lhs)) {
        return true;
    }
    if (expr.rhs != nullptr && contains_window_expr(*expr.rhs)) {
        return true;
    }
    if (expr.rhs_upper != nullptr && contains_window_expr(*expr.rhs_upper)) {
        return true;
    }
    for (const auto& operand : expr.operands) {
        if (operand != nullptr && contains_window_expr(*operand)) {
            return true;
        }
    }
    for (const auto& clause : expr.case_when) {
        if ((clause.first != nullptr && contains_window_expr(*clause.first)) ||
            (clause.second != nullptr && contains_window_expr(*clause.second))) {
            return true;
        }
    }
    if (expr.else_expr != nullptr && contains_window_expr(*expr.else_expr)) {
        return true;
    }
    for (const auto& value : expr.rhs_values) {
        if (value != nullptr && contains_window_expr(*value)) {
            return true;
        }
    }
    return false;
}

bool contains_select_expr(const Query& query, const QueryExpr& expr) {
    for (const auto& item : query.select_items) {
        if (same_query_expr(item.expr, expr)) {
            return true;
        }
    }
    return false;
}

const SelectItem* find_output_item_by_name(const Query& query, const std::string& name) {
    const SelectItem* found = nullptr;
    for (const auto& item : query.select_items) {
        if (item.output_name != name) {
            continue;
        }
        if (found != nullptr) {
            throw AmbiguousColumnError(name);
        }
        found = &item;
    }
    return found;
}

bool having_uses_plain_column(const HavingCondition& cond) {
    if (cond.lhs.type == QueryExprType::COLUMN) {
        return true;
    }
    return !cond.is_rhs_val && cond.rhs_expr.type == QueryExprType::COLUMN;
}

} // namespace analyze_internal
