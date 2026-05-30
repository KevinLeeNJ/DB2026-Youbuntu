/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

#include <type_traits>
#include <utility>

namespace {

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

Value convert_ast_value_node(const std::shared_ptr<ast::Value>& sv_val) {
    Value val;
    switch (sv_val->type) {
    case ast::AstType::IntLit: {
        auto int_lit = std::static_pointer_cast<ast::IntLit>(sv_val);
        val.set_int(int_lit->val);
        break;
    }
    case ast::AstType::FloatLit: {
        auto float_lit = std::static_pointer_cast<ast::FloatLit>(sv_val);
        val.set_float(float_lit->val);
        break;
    }
    case ast::AstType::StringLit: {
        auto str_lit = std::static_pointer_cast<ast::StringLit>(sv_val);
        val.set_str(str_lit->val);
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
    return false;
}

bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

bool is_groupable_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT || type == TYPE_STRING;
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

QueryExpr make_column_expr(TabCol col, std::string display_name = "") {
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
        if (col_meta->type != TYPE_INT && col_meta->type != TYPE_FLOAT && col_meta->type != TYPE_STRING) {
            throw RMDBError("COUNT(col) only supports int, float, and string columns");
        }
        break;
    case AggType::MAX:
    case AggType::MIN:
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

void rebuild_select_outputs(Query& query, const std::vector<ColMeta>& all_cols) {
    query.cols.clear();
    query.output_names.clear();
    query.has_aggregate = false;

    for (auto& item : query.select_items) {
        normalize_query_expr(item.expr, all_cols);
        if (item.expr.type == QueryExprType::COLUMN) {
            query.cols.push_back(item.expr.col);
        }
        if (item.expr.type == QueryExprType::AGGREGATE) {
            query.has_aggregate = true;
        }
        if (item.output_name.empty()) {
            item.output_name = item.alias.empty() ? item.expr.display_name : item.alias;
        }
        query.output_names.push_back(item.output_name);
    }
}

void validate_having(Query& query, const std::vector<ColMeta>& all_cols) {
    for (auto& cond : query.having_conds) {
        normalize_query_expr(cond.lhs, all_cols);
        query.has_aggregate = query.has_aggregate || cond.lhs.type == QueryExprType::AGGREGATE;

        ColType lhs_type = infer_expr_type(cond.lhs, all_cols);
        ColType rhs_type = TYPE_INT;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
        } else {
            normalize_query_expr(cond.rhs_expr, all_cols);
            query.has_aggregate = query.has_aggregate || cond.rhs_expr.type == QueryExprType::AGGREGATE;
            rhs_type = infer_expr_type(cond.rhs_expr, all_cols);
        }

        if (!can_cast_types(lhs_type, rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

void validate_order_by(Query& query, const std::vector<ColMeta>& all_cols) {
    for (auto& item : query.order_by_items) {
        if (!item.order_name.empty()) {
            const SelectItem* output_item = find_output_item_by_name(query, item.order_name);
            if (output_item != nullptr) {
                item.expr = output_item->expr;
            }
        }

        normalize_query_expr(item.expr, all_cols);
        query.has_aggregate = query.has_aggregate || item.expr.type == QueryExprType::AGGREGATE;

        if (item.expr.type == QueryExprType::VALUE) {
            throw RMDBError("ORDER BY does not support literal expressions");
        }

        if (!contains_select_expr(query, item.expr)) {
            throw RMDBError("ORDER BY must reference output columns or aliases");
        }
    }
}

void validate_group_by(Query& query) {
    if (query.group_by_cols.empty()) {
        return;
    }

    for (const auto& item : query.select_items) {
        if (item.expr.type == QueryExprType::COLUMN && !contains_group_col(query.group_by_cols, item.expr.col)) {
            throw RMDBError("SELECT list contains a non-aggregated column that is not in GROUP BY");
        }
    }

    for (const auto& cond : query.having_conds) {
        if (cond.lhs.type == QueryExprType::COLUMN && !contains_group_col(query.group_by_cols, cond.lhs.col)) {
            throw RMDBError("HAVING contains a non-aggregated column that is not in GROUP BY");
        }
        if (!cond.is_rhs_val && cond.rhs_expr.type == QueryExprType::COLUMN &&
            !contains_group_col(query.group_by_cols, cond.rhs_expr.col)) {
            throw RMDBError("HAVING contains a non-aggregated column that is not in GROUP BY");
        }
    }
}

void validate_select_without_group_by(const Query& query) {
    if (!query.has_aggregate) {
        return;
    }

    bool has_plain_col = false;
    bool has_agg_col = false;
    for (const auto& item : query.select_items) {
        has_plain_col = has_plain_col || item.expr.type == QueryExprType::COLUMN;
        has_agg_col = has_agg_col || item.expr.type == QueryExprType::AGGREGATE;
    }

    if (has_plain_col && has_agg_col) {
        throw RMDBError("SELECT list cannot mix aggregate and non-aggregate columns without GROUP BY");
    }

    for (const auto& cond : query.having_conds) {
        if (having_uses_plain_column(cond)) {
            throw RMDBError("HAVING cannot reference plain columns without GROUP BY");
        }
    }
}

void validate_select_query(Query& query, const std::vector<ColMeta>& all_cols) {
    for (auto& group_col : query.group_by_cols) {
        const ColMeta* col_meta = resolve_column_meta(all_cols, group_col);
        if (!is_groupable_type(col_meta->type)) {
            throw RMDBError("GROUP BY only supports int, float, and string columns");
        }
    }

    rebuild_select_outputs(query, all_cols);
    validate_having(query, all_cols);
    validate_order_by(query, all_cols);

    if (!query.having_conds.empty() && query.group_by_cols.empty() && !query.has_aggregate) {
        throw RMDBError("HAVING requires GROUP BY or aggregate expressions");
    }

    if (query.has_select_star && (query.has_aggregate || !query.group_by_cols.empty() || !query.having_conds.empty())) {
        throw RMDBError("SELECT * cannot be combined with aggregate, GROUP BY, or HAVING");
    }

    if (!query.group_by_cols.empty()) {
        validate_group_by(query);
    } else {
        validate_select_without_group_by(query);
    }

    if (query.has_limit && query.limit < 0) {
        throw RMDBError("LIMIT must be non-negative");
    }
}

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

void append_star_projection(Query& query, const std::vector<ColMeta>& all_cols) {
    for (const auto& col : all_cols) {
        SelectItem item;
        item.expr = make_column_expr({.tab_name = col.tab_name, .col_name = col.name});
        query.select_items.push_back(std::move(item));
    }
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

} // namespace

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    std::shared_ptr<Query> query = std::make_shared<Query>();
    switch (parse->type) {
    case ast::AstType::SelectStmt: {
        auto x = std::static_pointer_cast<ast::SelectStmt>(parse);
        query->tables = x->tabs;

        for (const auto& tab_name : query->tables) {
            if (!sm_manager_->db_.is_table(tab_name)) {
                throw TableNotFoundError(tab_name);
            }
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);

        populate_select_items_from_ast(*query, *x, all_cols);
        populate_group_by_from_ast(*query, *x);
        populate_having_from_ast(*query, *x);
        populate_order_by_from_ast(*query, *x);
        populate_limit_from_ast(*query, *x);

        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        validate_select_query(*query, all_cols);
        break;
    }
    case ast::AstType::UpdateStmt: {
        auto x = std::static_pointer_cast<ast::UpdateStmt>(parse);
        query->set_clauses.reserve(x->set_clauses.size());
        for (auto set_clause : x->set_clauses) {
            SetClause clause;
            clause.lhs = {.tab_name = x->tab_name, .col_name = set_clause->col_name};
            clause.rhs = convert_sv_value(set_clause->val);
            query->set_clauses.push_back(clause);
        }
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstType::DeleteStmt: {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(parse);
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstType::InsertStmt: {
        auto x = std::static_pointer_cast<ast::InsertStmt>(parse);
        query->values.reserve(x->vals.size());
        for (auto& sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
        break;
    }
    default:
        break;
    }
    query->parse = std::move(parse);
    return query;
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

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>>& sv_conds, std::vector<Condition>& conds) {
    conds.clear();
    conds.reserve(sv_conds.size());
    for (const auto& expr : sv_conds) {
        Condition cond;
        cond.lhs_col = extract_ast_column(expr->lhs, "WHERE");
        cond.op = convert_sv_comp_op(expr->op);

        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs); rhs_val != nullptr) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs); rhs_col != nullptr) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        } else {
            throw RMDBError("WHERE clause does not allow aggregate expressions");
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string>& tab_names, std::vector<Condition>& conds) {
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);

    for (auto& cond : conds) {
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        auto lhs_col = sm_manager_->db_.get_table(cond.lhs_col.tab_name).get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;

        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
            auto rhs_col = sm_manager_->db_.get_table(cond.rhs_col.tab_name).get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }

        if (!can_cast(lhs_type, rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value>& sv_val) {
    return convert_ast_value_node(sv_val);
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
    }
    throw InternalError("Unexpected comparison operator");
}
