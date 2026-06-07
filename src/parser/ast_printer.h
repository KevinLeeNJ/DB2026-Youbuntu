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

#include "ast.h"
#include <cassert>
#include <iostream>
#include <map>

namespace ast {

class TreePrinter {
public:
    static void print(const std::unique_ptr<TreeNode>& node) {
        print_node(node.get(), 0);
    }

    static void print(const TreeNode* node) {
        print_node(node, 0);
    }

private:
    static std::string offset2string(int offset) {
        return std::string(offset, ' ');
    }

    template <typename T> static void print_val(const T& val, int offset) {
        std::cout << offset2string(offset) << val << '\n';
    }

    template <typename T> static void print_val_list(const std::vector<T>& vals, int offset) {
        std::cout << offset2string(offset) << "LIST\n";
        offset += 2;
        for (auto& val : vals) {
            print_val(val, offset);
        }
    }

    static std::string type2str(SvType type) {
        static std::map<SvType, std::string> m{
            {SV_TYPE_INT, "INT"},
            {SV_TYPE_FLOAT, "FLOAT"},
            {SV_TYPE_STRING, "STRING"},
        };
        return m.at(type);
    }

    static std::string op2str(SvCompOp op) {
        static std::map<SvCompOp, std::string> m{
            {SV_OP_EQ, "=="}, {SV_OP_NE, "!="}, {SV_OP_LT, "<"}, {SV_OP_GT, ">"}, {SV_OP_LE, "<="}, {SV_OP_GE, ">="},
        };
        return m.at(op);
    }

    static std::string agg_func2str(AggFuncType func) {
        static std::map<AggFuncType, std::string> m{
            {AGG_COUNT, "COUNT"}, {AGG_MAX, "MAX"}, {AGG_MIN, "MIN"}, {AGG_SUM, "SUM"}, {AGG_AVG, "AVG"},
        };
        return m.at(func);
    }

    static std::string orderby_dir2str(OrderByDir dir) {
        static std::map<OrderByDir, std::string> m{
            {OrderBy_DEFAULT, "DEFAULT"},
            {OrderBy_ASC, "ASC"},
            {OrderBy_DESC, "DESC"},
        };
        return m.at(dir);
    }

    template <typename T> static void print_node_list(const std::vector<T>& nodes, int offset) {
        std::cout << offset2string(offset);
        offset += 2;
        std::cout << "LIST\n";
        for (auto& node : nodes) {
            print_node(node, offset);
        }
    }

    template <typename T> static void print_node(const std::unique_ptr<T>& node, int offset) {
        print_node(node.get(), offset);
    }

    static void print_node(const TreeNode* node, int offset) {
        std::cout << offset2string(offset);
        offset += 2;
        switch (node->type) {
        case AstType::Help:
            std::cout << "HELP\n";
            break;
        case AstType::ShowTables:
            std::cout << "SHOW_TABLES\n";
            break;
        case AstType::ShowIndex: {
            auto x = static_cast<const ShowIndex*>(node);
            std::cout << "SHOW_INDEX\n";
            print_val(x->tab_name, offset);
            break;
        }
        case AstType::CreateTable: {
            auto x = static_cast<const CreateTable*>(node);
            std::cout << "CREATE_TABLE\n";
            print_val(x->tab_name, offset);
            print_node_list(x->fields, offset);
            break;
        }
        case AstType::DropTable: {
            auto x = static_cast<const DropTable*>(node);
            std::cout << "DROP_TABLE\n";
            print_val(x->tab_name, offset);
            break;
        }
        case AstType::DescTable: {
            auto x = static_cast<const DescTable*>(node);
            std::cout << "DESC_TABLE\n";
            print_val(x->tab_name, offset);
            break;
        }
        case AstType::CreateIndex: {
            auto x = static_cast<const CreateIndex*>(node);
            std::cout << "CREATE_INDEX\n";
            print_val(x->tab_name, offset);
            // print_val(x->col_name, offset);
            for (auto col_name : x->col_names)
                print_val(col_name, offset);
            break;
        }
        case AstType::DropIndex: {
            auto x = static_cast<const DropIndex*>(node);
            std::cout << "DROP_INDEX\n";
            print_val(x->tab_name, offset);
            // print_val(x->col_name, offset);
            for (auto col_name : x->col_names)
                print_val(col_name, offset);
            break;
        }
        case AstType::StaticCheckpoint: {
            std::cout << "STATIC_CHECKPOINT\n";
            break;
        }
        case AstType::ColDef: {
            auto x = static_cast<const ColDef*>(node);
            std::cout << "COL_DEF\n";
            print_val(x->col_name, offset);
            print_node(x->type_len, offset);
            break;
        }
        case AstType::Col: {
            auto x = static_cast<const Col*>(node);
            std::cout << "COL\n";
            print_val(x->tab_name, offset);
            print_val(x->col_name, offset);
            break;
        }
        case AstType::AggExpr: {
            auto x = static_cast<const AggExpr*>(node);
            std::cout << "AGG_EXPR\n";
            print_val(agg_func2str(x->func), offset);
            print_val(x->is_star, offset);
            if (x->col != nullptr) {
                print_node(x->col, offset);
            }
            break;
        }
        case AstType::SelectItem: {
            auto x = static_cast<const SelectItem*>(node);
            std::cout << "SELECT_ITEM\n";
            print_node(x->expr, offset);
            print_val(x->alias, offset);
            break;
        }
        case AstType::TypeLen: {
            auto x = static_cast<const TypeLen*>(node);
            std::cout << "TYPE_LEN\n";
            print_val(type2str(x->type), offset);
            print_val(x->len, offset);
            break;
        }
        case AstType::IntLit: {
            auto x = static_cast<const IntLit*>(node);
            std::cout << "INT_LIT\n";
            print_val(x->val, offset);
            break;
        }
        case AstType::FloatLit: {
            auto x = static_cast<const FloatLit*>(node);
            std::cout << "FLOAT_LIT\n";
            print_val(x->val, offset);
            break;
        }
        case AstType::StringLit: {
            auto x = static_cast<const StringLit*>(node);
            std::cout << "STRING_LIT\n";
            print_val(x->val, offset);
            break;
        }
        case AstType::BoolLit: {
            auto x = static_cast<const BoolLit*>(node);
            std::cout << "BOOL_LIT\n";
            print_val(x->val, offset);
            break;
        }
        case AstType::SetClause: {
            auto x = static_cast<const SetClause*>(node);
            std::cout << "SET_CLAUSE\n";
            print_val(x->col_name, offset);
            print_node(x->val, offset);
            break;
        }
        case AstType::BinaryExpr: {
            auto x = static_cast<const BinaryExpr*>(node);
            std::cout << "BINARY_EXPR\n";
            print_node(x->lhs, offset);
            print_val(op2str(x->op), offset);
            print_node(x->rhs, offset);
            break;
        }
        case AstType::HavingExpr: {
            auto x = static_cast<const HavingExpr*>(node);
            std::cout << "HAVING_EXPR\n";
            print_node(x->lhs, offset);
            print_val(op2str(x->op), offset);
            print_node(x->rhs, offset);
            break;
        }
        case AstType::OrderBy: {
            auto x = static_cast<const OrderBy*>(node);
            std::cout << "ORDER_BY\n";
            print_node(x->cols, offset);
            print_val(orderby_dir2str(x->orderby_dir), offset);
            break;
        }
        case AstType::OrderByItem: {
            auto x = static_cast<const OrderByItem*>(node);
            std::cout << "ORDER_BY_ITEM\n";
            print_node(x->expr, offset);
            print_val(orderby_dir2str(x->orderby_dir), offset);
            break;
        }
        case AstType::InsertStmt: {
            auto x = static_cast<const InsertStmt*>(node);
            std::cout << "INSERT\n";
            print_val(x->tab_name, offset);
            print_node_list(x->vals, offset);
            break;
        }
        case AstType::DeleteStmt: {
            auto x = static_cast<const DeleteStmt*>(node);
            std::cout << "DELETE\n";
            print_val(x->tab_name, offset);
            print_node_list(x->conds, offset);
            break;
        }
        case AstType::UpdateStmt: {
            auto x = static_cast<const UpdateStmt*>(node);
            std::cout << "UPDATE\n";
            print_val(x->tab_name, offset);
            print_node_list(x->set_clauses, offset);
            print_node_list(x->conds, offset);
            break;
        }
        case AstType::JoinExpr:
            assert(0);
            break;
        case AstType::SelectStmt: {
            auto x = static_cast<const SelectStmt*>(node);
            std::cout << "SELECT\n";
            print_val(x->has_select_star, offset);
            print_node_list(x->select_items, offset);
            print_val_list(x->tabs, offset);
            print_node_list(x->conds, offset);
            print_node_list(x->group_by_cols, offset);
            print_node_list(x->having_conds, offset);
            print_node_list(x->order_by_items, offset);
            print_val(x->has_limit, offset);
            if (x->has_limit) {
                print_val(x->limit, offset);
            }
            break;
        }
        case AstType::UnionStmt: {
            auto x = static_cast<const UnionStmt*>(node);
            std::cout << "UNION\n";
            print_node_list(x->branches, offset);
            break;
        }
        case AstType::SelectFromUnionStmt: {
            auto x = static_cast<const SelectFromUnionStmt*>(node);
            std::cout << "SELECT_FROM_UNION\n";
            print_node(x->union_stmt, offset);
            print_val(x->alias, offset);
            print_node_list(x->order_by_items, offset);
            break;
        }
        case AstType::ExplainAnalyze: {
            auto x = static_cast<const ExplainAnalyze*>(node);
            std::cout << "EXPLAIN_ANALYZE\n";
            print_node(x->select, offset);
            break;
        }
        case AstType::TxnBegin:
            std::cout << "BEGIN\n";
            break;
        case AstType::TxnCommit:
            std::cout << "COMMIT\n";
            break;
        case AstType::TxnAbort:
            std::cout << "ABORT\n";
            break;
        case AstType::TxnRollback:
            std::cout << "ROLLBACK\n";
            break;
        case AstType::SetStmt: {
            auto x = static_cast<const SetStmt*>(node);
            std::cout << "SET\n";
            print_val(x->set_knob_type_ == EnableNestLoop ? "ENABLE_NESTLOOP" : "ENABLE_SORTMERGE", offset);
            print_val(x->bool_val_, offset);
            break;
        }
        case AstType::SetTransaction: {
            auto x = static_cast<const SetTransaction*>(node);
            std::cout << "SET_TRANSACTION ISOLATION LEVEL "
                      << (x->isolation_level_ == IsolationLevelType::SNAPSHOT_ISOLATION ? "SNAPSHOT ISOLATION"
                                                                                        : "SERIALIZABLE")
                      << "\n";
            break;
        }
        }
    }
};

} // namespace ast
