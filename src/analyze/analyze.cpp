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
    for (auto& item : query.order_by_items) {
        resolve_alias(item.expr, query);
    }
    for (auto& cond : query.conds) {
        resolve_alias(cond.lhs_col, query);
        if (!cond.is_rhs_val) {
            resolve_alias(cond.rhs_col, query);
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
        query->set_clauses.reserve(x->set_clauses.size());
        for (const auto& set_clause : x->set_clauses) {
            SetClause clause;
            clause.lhs = {.tab_name = x->tab_name, .col_name = set_clause->col_name};
            clause.op = convert_update_op(set_clause->op);
            if (set_clause->val != nullptr) {
                clause.rhs = convert_sv_value(set_clause->val.get());
            }
            clause.is_self_ref = set_clause->is_self_ref;
            if (set_clause->is_self_ref) {
                clause.rhs_col = {.tab_name = set_clause->rhs_col->tab_name.empty() ? x->tab_name
                                                                                    : set_clause->rhs_col->tab_name,
                                  .col_name = set_clause->rhs_col->col_name};
                std::vector<ColMeta> all_cols;
                get_all_cols({x->tab_name}, all_cols);
                clause.rhs_col = check_column(all_cols, clause.rhs_col);
            }
            query->set_clauses.push_back(clause);
        }
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstType::DeleteStmt: {
        auto x = static_cast<const ast::DeleteStmt*>(root);
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstType::InsertStmt: {
        auto x = static_cast<const ast::InsertStmt*>(root);
        query->values.reserve(x->vals.size());
        for (auto& sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val.get()));
        }
        break;
    }
    default:
        break;
    }
    query->parse = std::move(parse);
    return query;
}

std::unique_ptr<Query> Analyze::analyze_select_stmt(const ast::SelectStmt* x, std::unique_ptr<ast::TreeNode> owner) {
    auto query = std::make_unique<Query>();
    populate_table_refs(*query, x->tabs);

    for (const auto& tab_name : query->tables) {
        if (!catalog_->is_table(tab_name)) {
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
    resolve_aliases(*query);
    check_clause(query->tables, query->conds);
    validate_select_query(*query, all_cols);
    query->parse = std::move(owner);
    return query;
}

std::vector<ColMeta> Analyze::get_query_output_metas(const Query& query) {
    std::vector<ColMeta> all_cols;
    get_all_cols(query.tables, all_cols);

    std::vector<ColMeta> result;
    result.reserve(query.select_items.size());
    size_t offset = 0;
    for (size_t i = 0; i < query.select_items.size(); ++i) {
        const auto& item = query.select_items[i];
        ColType type = infer_expr_type(item.expr, all_cols);
        int len = sizeof(int);
        if (type == TYPE_FLOAT) {
            len = sizeof(float);
        } else if (type == TYPE_STRING || type == TYPE_DATETIME) {
            if (item.expr.type != QueryExprType::COLUMN) {
                throw RMDBError("UNION only supports string output from columns");
            }
            TabCol resolved_col = item.expr.col;
            const ColMeta* col_meta = resolve_column_meta(all_cols, resolved_col);
            len = col_meta->len;
        }

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
        result.len = sizeof(float);
        return result;
    }

    throw IncompatibleTypeError(coltype2str(current.type), coltype2str(next.type));
}

void Analyze::validate_union_order_by(Query& query) {
    for (auto& item : query.order_by_items) {
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

std::unique_ptr<Query> Analyze::analyze_select_from_union_stmt(const ast::SelectFromUnionStmt* x,
                                                               std::unique_ptr<ast::TreeNode> owner) {
    if (x->union_stmt == nullptr || x->union_stmt->branches.size() < 2) {
        throw RMDBError("UNION requires at least two SELECT branches");
    }

    auto query = std::make_unique<Query>();
    query->is_union = true;
    query->parse = std::move(owner);
    query->union_alias = x->alias;
    populate_order_by_from_ast(*query, *x);

    query->union_branches.reserve(x->union_stmt->branches.size());
    for (const auto& branch : x->union_stmt->branches) {
        auto branch_query = analyze_select_stmt(branch.get());
        if (!branch_query->order_by_items.empty() || branch_query->has_limit) {
            throw RMDBError("UNION branches do not support ORDER BY or LIMIT");
        }
        query->union_branches.push_back(std::move(branch_query));
    }

    query->union_cols = get_query_output_metas(*query->union_branches.front());
    for (size_t branch_idx = 1; branch_idx < query->union_branches.size(); ++branch_idx) {
        auto branch_cols = get_query_output_metas(*query->union_branches[branch_idx]);
        if (branch_cols.size() != query->union_cols.size()) {
            throw RMDBError("UNION branches must return the same number of columns");
        }
        for (size_t col_idx = 0; col_idx < query->union_cols.size(); ++col_idx) {
            query->union_cols[col_idx] = make_union_col_meta(query->union_cols[col_idx], branch_cols[col_idx]);
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

    validate_union_order_by(*query);
    return query;
}

TabCol Analyze::check_column(const std::vector<ColMeta>& all_cols, TabCol target) {
    resolve_column_meta(all_cols, target);
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string>& tab_names, std::vector<ColMeta>& all_cols) {
    for (const auto& sel_tab_name : tab_names) {
        const auto& sel_tab_cols = catalog_->get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::unique_ptr<ast::BinaryExpr>>& sv_conds, std::vector<Condition>& conds) {
    conds.clear();
    conds.reserve(sv_conds.size());
    for (const auto& expr : sv_conds) {
        Condition cond;
        cond.lhs_col = extract_ast_column(expr->lhs, "WHERE");
        cond.op = convert_sv_comp_op(expr->op);

        if (auto rhs_val = dynamic_cast<const ast::Value*>(expr->rhs.get()); rhs_val != nullptr) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
            cond.rhs_display = rhs_val->display_text;
        } else if (auto rhs_col = dynamic_cast<const ast::Col*>(expr->rhs.get()); rhs_col != nullptr) {
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
        auto lhs_col = catalog_->get_table(cond.lhs_col.tab_name).get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;

        ColType rhs_type;
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
            auto rhs_col = catalog_->get_table(cond.rhs_col.tab_name).get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
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
        val.set_float(static_cast<float>(val.int_val));
        return;
    }
    if (to == TYPE_INT && val.type == TYPE_FLOAT) {
        val.set_int(static_cast<int>(val.float_val));
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
    }
    throw InternalError("Unexpected comparison operator");
}
