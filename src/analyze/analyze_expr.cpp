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

#include <cmath>

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
        if (!std::isfinite(float_lit->val)) {
            throw RMDBError("FLOAT value must be finite");
        }
        val.set_float(float_lit->val);
        break;
    }
    case ast::AstType::StringLit: {
        auto str_lit = static_cast<const ast::StringLit*>(sv_val);
        val.set_str(str_lit->val);
        break;
    }
    case ast::AstType::NullLit:
        val.set_null();
        break;
    case ast::AstType::Parameter: {
        const auto* parameter = static_cast<const ast::Parameter*>(sv_val);
        if (parameter->ordinal == 0 || !parameter->declared_type.has_value()) {
            throw InternalError("Prepared parameter is missing its declared type");
        }
        switch (*parameter->declared_type) {
        case ast::SV_TYPE_INT:
            val.set_int(0);
            break;
        case ast::SV_TYPE_FLOAT:
            val.set_float(0.0f);
            break;
        case ast::SV_TYPE_STRING:
            val.set_str("");
            break;
        case ast::SV_TYPE_DATETIME:
            val.set_str("");
            val.type = TYPE_DATETIME;
            break;
        case ast::SV_TYPE_BOOL:
            throw InternalError("BOOL prepared parameters are not supported");
        }
        val.parameter_ordinal = parameter->ordinal;
        break;
    }
    case ast::AstType::BoolLit:
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
        if (agg.is_distinct) {
            throw RMDBError("COUNT(DISTINCT *) is not supported");
        }
        if (agg.type != AggType::COUNT) {
            throw RMDBError("Only COUNT(*) is supported for '*' aggregate arguments");
        }
        agg.display_name = build_agg_display_name(agg);
        return;
    }

    TabCol resolved_col = agg.col;
    const ColMeta* col_meta = resolve_column_meta(all_cols, resolved_col);
    agg.col = resolved_col;

    if (agg.is_distinct && agg.type != AggType::COUNT) {
        throw RMDBError("DISTINCT is only supported for COUNT");
    }

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
    case QueryExprType::VALUE:
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
        return false;
    case QueryExprType::AGGREGATE:
        return lhs.agg.type == rhs.agg.type && lhs.agg.is_star == rhs.agg.is_star &&
               lhs.agg.is_distinct == rhs.agg.is_distinct &&
               (lhs.agg.is_star || same_tab_col(lhs.agg.col, rhs.agg.col));
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
