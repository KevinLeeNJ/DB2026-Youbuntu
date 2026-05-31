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
#include "analyze_expr_internal.h"
#include "analyze_select_internal.h"

#include <algorithm>

using namespace analyze_internal;

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    if (parse->type == ast::AstType::SelectStmt) {
        return analyze_select_stmt(std::static_pointer_cast<ast::SelectStmt>(parse));
    }
    if (parse->type == ast::AstType::SelectFromUnionStmt) {
        return analyze_select_from_union_stmt(std::static_pointer_cast<ast::SelectFromUnionStmt>(parse));
    }

    std::shared_ptr<Query> query = std::make_shared<Query>();
    switch (parse->type) {
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

std::shared_ptr<Query> Analyze::analyze_select_stmt(const std::shared_ptr<ast::SelectStmt>& x) {
    std::shared_ptr<Query> query = std::make_shared<Query>();
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
    query->parse = x;
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
        } else if (type == TYPE_STRING) {
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
        if (current.type == TYPE_STRING) {
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

std::shared_ptr<Query> Analyze::analyze_select_from_union_stmt(const std::shared_ptr<ast::SelectFromUnionStmt>& x) {
    if (x->union_stmt == nullptr || x->union_stmt->branches.size() < 2) {
        throw RMDBError("UNION requires at least two SELECT branches");
    }

    auto query = std::make_shared<Query>();
    query->is_union = true;
    query->parse = x;
    query->union_alias = x->alias;
    populate_order_by_from_ast(*query, *x);

    query->union_branches.reserve(x->union_stmt->branches.size());
    for (const auto& branch : x->union_stmt->branches) {
        auto branch_query = analyze_select_stmt(branch);
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
