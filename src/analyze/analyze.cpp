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

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    std::shared_ptr<Query> query = std::make_shared<Query>();
    switch (parse->kind) {
    case ast::AstNodeKind::SelectStmt: {
        auto x = std::static_pointer_cast<ast::SelectStmt>(parse);
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */

        // 处理target list，再target list中添加上表名，例如 a.id
        query->cols.reserve(x->cols.size());
        for (auto& sv_sel_col : x->cols) {
            TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
            query->cols.push_back(sel_col);
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty()) {
            // select all columns
            query->cols.reserve(all_cols.size());
            for (auto& col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto& sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col); // 列元数据校验
            }
        }
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
        break;
    }
    case ast::AstNodeKind::UpdateStmt: {
        /** TODO: */
        break;
    }
    case ast::AstNodeKind::DeleteStmt: {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(parse);
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
        break;
    }
    case ast::AstNodeKind::InsertStmt: {
        auto x = std::static_pointer_cast<ast::InsertStmt>(parse);
        // 处理insert 的values值
        query->values.reserve(x->vals.size());
        for (auto& sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
        break;
    }
    default:
        // do nothing
        break;
    }
    query->parse = std::move(parse);
    return query;
}

TabCol Analyze::check_column(const std::vector<ColMeta>& all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto& col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        // 显式指定了表名，校验列在该表中确实存在
        bool found = false;
        for (auto& col : all_cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string>& tab_names, std::vector<ColMeta>& all_cols) {
    for (auto& sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto& sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>>& sv_conds, std::vector<Condition>& conds) {
    conds.clear();
    conds.reserve(sv_conds.size());
    for (auto& expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        switch (expr->rhs->kind) {
        case ast::AstNodeKind::IntLit:
        case ast::AstNodeKind::FloatLit:
        case ast::AstNodeKind::StringLit:
        case ast::AstNodeKind::BoolLit: {
            auto rhs_val = std::static_pointer_cast<ast::Value>(expr->rhs);
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
            break;
        }
        case ast::AstNodeKind::Col: {
            auto rhs_col = std::static_pointer_cast<ast::Col>(expr->rhs);
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
            break;
        }
        default:
            throw InternalError("Unexpected rhs expression type in where clause");
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string>& tab_names, std::vector<Condition>& conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto& cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta& lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta& rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (lhs_type != rhs_type) {
            // 允许 INT 和 FLOAT 之间的隐式类型转换（实际比较在 eval_conds 中统一提升为 FLOAT）
            if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)) {
                // 不做截断转换，交给 eval_conds 处理
            } else {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }
        }
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value>& sv_val) {
    Value val;
    switch (sv_val->kind) {
    case ast::AstNodeKind::IntLit: {
        auto int_lit = std::static_pointer_cast<ast::IntLit>(sv_val);
        val.set_int(int_lit->val);
        break;
    }
    case ast::AstNodeKind::FloatLit: {
        auto float_lit = std::static_pointer_cast<ast::FloatLit>(sv_val);
        val.set_float(float_lit->val);
        break;
    }
    case ast::AstNodeKind::StringLit: {
        auto str_lit = std::static_pointer_cast<ast::StringLit>(sv_val);
        val.set_str(str_lit->val);
        break;
    }
    case ast::AstNodeKind::BoolLit: {
        auto bool_lit = std::static_pointer_cast<ast::BoolLit>(sv_val);
        val.set_int(bool_lit->val ? 1 : 0);
        break;
    }
    default:
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    static const std::unordered_map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}
