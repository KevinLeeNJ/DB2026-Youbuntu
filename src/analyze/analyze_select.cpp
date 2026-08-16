/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze_select_internal.h"

namespace analyze_internal {

void append_star_projection(Query& query, const std::vector<ColMeta>& all_cols) {
    for (const auto& col : all_cols) {
        SelectItem item;
        item.expr = make_column_expr({.tab_name = col.tab_name, .col_name = col.name});
        query.select_items.push_back(std::move(item));
    }
}

void rebuild_select_outputs(Query& query, const std::vector<ColMeta>& all_cols) {
    query.cols.clear();
    query.output_names.clear();
    query.has_aggregate = false;
    query.has_window = false;

    for (auto& item : query.select_items) {
        normalize_query_expr(item.expr, all_cols);
        if (item.expr.type == QueryExprType::COLUMN) {
            query.cols.push_back(item.expr.col);
        }
        if (item.expr.type == QueryExprType::AGGREGATE) {
            query.has_aggregate = true;
        }
        query.has_window = query.has_window || contains_window_expr(item.expr);
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
        if (cond.op == OP_LIKE && lhs_type != TYPE_STRING && lhs_type != TYPE_DATETIME) {
            throw IncompatibleTypeError(coltype2str(lhs_type), "string");
        }
        if (cond.op == OP_IN) {
            if (cond.rhs_vals.empty()) {
                throw RMDBError("HAVING IN list must not be empty");
            }
            for (const auto& rhs_val : cond.rhs_vals) {
                if (!can_cast_types(lhs_type, rhs_val.type)) {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_val.type));
                }
            }
            continue;
        }
        if (cond.op == OP_BETWEEN) {
            if (!cond.has_rhs_upper) {
                throw RMDBError("HAVING BETWEEN requires two bounds");
            }
            for (const auto* bound : {&cond.rhs_val, &cond.rhs_upper}) {
                if (!can_cast_types(lhs_type, bound->type)) {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(bound->type));
                }
            }
            continue;
        }
        ColType rhs_type = TYPE_INT;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
        } else {
            normalize_query_expr(cond.rhs_expr, all_cols);
            query.has_aggregate = query.has_aggregate || cond.rhs_expr.type == QueryExprType::AGGREGATE;
            rhs_type = infer_expr_type(cond.rhs_expr, all_cols);
        }

        if (cond.op == OP_LIKE && rhs_type != TYPE_STRING && rhs_type != TYPE_DATETIME) {
            throw IncompatibleTypeError(coltype2str(rhs_type), "string");
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
        query.has_window = query.has_window || contains_window_expr(item.expr);

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
    if (query.has_offset && query.offset < 0) {
        throw RMDBError("OFFSET must be non-negative");
    }
}

} // namespace analyze_internal
