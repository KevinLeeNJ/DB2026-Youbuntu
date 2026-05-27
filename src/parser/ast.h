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

#include <vector>
#include <string>
#include <memory>

enum JoinType { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN };
namespace ast {

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL };

enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum SetKnobType { EnableNestLoop, EnableSortMerge };

enum class AstNodeKind {
    Help,
    ShowTables,
    TxnBegin,
    TxnCommit,
    TxnAbort,
    TxnRollback,
    TypeLen,
    ColDef,
    CreateTable,
    DropTable,
    DescTable,
    CreateIndex,
    DropIndex,
    IntLit,
    FloatLit,
    StringLit,
    BoolLit,
    Col,
    SetClause,
    BinaryExpr,
    OrderBy,
    InsertStmt,
    DeleteStmt,
    UpdateStmt,
    SelectStmt,
    SetStmt,
    JoinExpr,
};

// Base class for tree nodes
struct TreeNode {
    AstNodeKind kind;
    TreeNode() : kind(AstNodeKind::Help) {} // intermediate base classes use this
    TreeNode(AstNodeKind k) : kind(k) {}
    virtual ~TreeNode() = default; // enable polymorphism
};

struct Help : public TreeNode {
    Help() : TreeNode(AstNodeKind::Help) {}
};

struct ShowTables : public TreeNode {
    ShowTables() : TreeNode(AstNodeKind::ShowTables) {}
};

struct TxnBegin : public TreeNode {
    TxnBegin() : TreeNode(AstNodeKind::TxnBegin) {}
};

struct TxnCommit : public TreeNode {
    TxnCommit() : TreeNode(AstNodeKind::TxnCommit) {}
};

struct TxnAbort : public TreeNode {
    TxnAbort() : TreeNode(AstNodeKind::TxnAbort) {}
};

struct TxnRollback : public TreeNode {
    TxnRollback() : TreeNode(AstNodeKind::TxnRollback) {}
};

struct TypeLen : public TreeNode {
    SvType type;
    int len;

    TypeLen(SvType type_, int len_) : TreeNode(AstNodeKind::TypeLen), type(type_), len(len_) {}
};

struct Field : public TreeNode {
protected:
    Field(AstNodeKind k) : TreeNode(k) {}
};

struct ColDef : public Field {
    std::string col_name;
    std::shared_ptr<TypeLen> type_len;

    ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_)
        : Field(AstNodeKind::ColDef), col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
};

struct CreateTable : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Field>> fields;

    CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_)
        : TreeNode(AstNodeKind::CreateTable), tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
    std::string tab_name;

    DropTable(std::string tab_name_) : TreeNode(AstNodeKind::DropTable), tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
    std::string tab_name;

    DescTable(std::string tab_name_) : TreeNode(AstNodeKind::DescTable), tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    CreateIndex(std::string tab_name_, std::vector<std::string> col_names_)
        : TreeNode(AstNodeKind::CreateIndex), tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct DropIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    DropIndex(std::string tab_name_, std::vector<std::string> col_names_)
        : TreeNode(AstNodeKind::DropIndex), tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct Expr : public TreeNode {
protected:
    Expr(AstNodeKind k) : TreeNode(k) {}
};

struct Value : public Expr {
protected:
    Value(AstNodeKind k) : Expr(k) {}
};

struct IntLit : public Value {
    int val;

    IntLit(int val_) : Value(AstNodeKind::IntLit), val(val_) {}
};

struct FloatLit : public Value {
    float val;

    FloatLit(float val_) : Value(AstNodeKind::FloatLit), val(val_) {}
};

struct StringLit : public Value {
    std::string val;

    StringLit(std::string val_) : Value(AstNodeKind::StringLit), val(std::move(val_)) {}
};

struct BoolLit : public Value {
    bool val;

    BoolLit(bool val_) : Value(AstNodeKind::BoolLit), val(val_) {}
};

struct Col : public Expr {
    std::string tab_name;
    std::string col_name;

    Col(std::string tab_name_, std::string col_name_)
        : Expr(AstNodeKind::Col), tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}
};

struct SetClause : public TreeNode {
    std::string col_name;
    std::shared_ptr<Value> val;

    SetClause(std::string col_name_, std::shared_ptr<Value> val_)
        : TreeNode(AstNodeKind::SetClause), col_name(std::move(col_name_)), val(std::move(val_)) {}
};

struct BinaryExpr : public TreeNode {
    std::shared_ptr<Col> lhs;
    SvCompOp op;
    std::shared_ptr<Expr> rhs;

    BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_)
        : TreeNode(AstNodeKind::BinaryExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderBy : public TreeNode {
    std::shared_ptr<Col> cols;
    OrderByDir orderby_dir;
    OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_)
        : TreeNode(AstNodeKind::OrderBy), cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
};

struct InsertStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<Value>> vals;

    InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_)
        : TreeNode(AstNodeKind::InsertStmt), tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct DeleteStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_)
        : TreeNode(AstNodeKind::DeleteStmt), tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::shared_ptr<SetClause>> set_clauses;
    std::vector<std::shared_ptr<BinaryExpr>> conds;

    UpdateStmt(std::string tab_name_, std::vector<std::shared_ptr<SetClause>> set_clauses_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_)
        : TreeNode(AstNodeKind::UpdateStmt), tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)),
          conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
    std::string left;
    std::string right;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    JoinType type;

    JoinExpr(std::string left_, std::string right_, std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_)
        : TreeNode(AstNodeKind::JoinExpr), left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)),
          type(type_) {}
};

struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<Col>> cols;
    std::vector<std::string> tabs;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    std::vector<std::shared_ptr<JoinExpr>> jointree;

    bool has_sort;
    std::shared_ptr<OrderBy> order;

    SelectStmt(std::vector<std::shared_ptr<Col>> cols_, std::vector<std::string> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_, std::shared_ptr<OrderBy> order_)
        : TreeNode(AstNodeKind::SelectStmt), cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
          order(std::move(order_)) {
        has_sort = (bool)order;
    }
};

// set enable_nestloop
struct SetStmt : public TreeNode {
    SetKnobType set_knob_type_;
    bool bool_val_;

    SetStmt(SetKnobType& type, bool bool_value)
        : TreeNode(AstNodeKind::SetStmt), set_knob_type_(type), bool_val_(bool_value) {}
};

// Semantic value
struct SemValue {
    int sv_int;
    float sv_float;
    std::string sv_str;
    bool sv_bool;
    OrderByDir sv_orderby_dir;
    std::vector<std::string> sv_strs;

    std::shared_ptr<TreeNode> sv_node;

    SvCompOp sv_comp_op;

    std::shared_ptr<TypeLen> sv_type_len;

    std::shared_ptr<Field> sv_field;
    std::vector<std::shared_ptr<Field>> sv_fields;

    std::shared_ptr<Expr> sv_expr;

    std::shared_ptr<Value> sv_val;
    std::vector<std::shared_ptr<Value>> sv_vals;

    std::shared_ptr<Col> sv_col;
    std::vector<std::shared_ptr<Col>> sv_cols;

    std::shared_ptr<SetClause> sv_set_clause;
    std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

    std::shared_ptr<BinaryExpr> sv_cond;
    std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

    std::shared_ptr<OrderBy> sv_orderby;

    SetKnobType sv_setKnobType;
};

extern std::shared_ptr<ast::TreeNode> parse_tree;

} // namespace ast

#define YYSTYPE ast::SemValue
