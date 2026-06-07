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

#include <memory>
#include <string>
#include <utility>
#include <vector>

enum JoinType { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN };
namespace ast {

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL };

enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum AggFuncType { AGG_COUNT, AGG_MAX, AGG_MIN, AGG_SUM, AGG_AVG };

enum SetKnobType { EnableNestLoop, EnableSortMerge };

enum class IsolationLevelType { SNAPSHOT_ISOLATION, SERIALIZABLE };

enum class AstType {
    Help,
    ShowTables,
    ShowIndex,
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
    AggExpr,
    SelectItem,
    HavingExpr,
    OrderByItem,
    SetClause,
    BinaryExpr,
    OrderBy,
    InsertStmt,
    DeleteStmt,
    UpdateStmt,
    JoinExpr,
    SelectStmt,
    UnionStmt,
    SelectFromUnionStmt,
    ExplainAnalyze,
    SetStmt,
    SetTransaction,
    StaticCheckpoint
};

// Base class for tree nodes
struct TreeNode {
    explicit TreeNode(AstType type_) : type(type_) {}
    virtual ~TreeNode() = default; // enable polymorphism

    AstType type;
};

struct Help : public TreeNode {
    Help() : TreeNode(AstType::Help) {}
};

struct ShowTables : public TreeNode {
    ShowTables() : TreeNode(AstType::ShowTables) {}
};

struct ShowIndex : public TreeNode {
    std::string tab_name;

    ShowIndex(std::string tab_name_) : TreeNode(AstType::ShowIndex), tab_name(std::move(tab_name_)) {}
};

struct TxnBegin : public TreeNode {
    TxnBegin() : TreeNode(AstType::TxnBegin) {}
};

struct TxnCommit : public TreeNode {
    TxnCommit() : TreeNode(AstType::TxnCommit) {}
};

struct TxnAbort : public TreeNode {
    TxnAbort() : TreeNode(AstType::TxnAbort) {}
};

struct TxnRollback : public TreeNode {
    TxnRollback() : TreeNode(AstType::TxnRollback) {}
};

struct TypeLen : public TreeNode {
    SvType type;
    int len;

    TypeLen(SvType type_, int len_) : TreeNode(AstType::TypeLen), type(type_), len(len_) {}
};

struct Field : public TreeNode {
protected:
    explicit Field(AstType type_) : TreeNode(type_) {}
};

struct ColDef : public Field {
    std::string col_name;
    std::unique_ptr<TypeLen> type_len;

    ColDef(std::string col_name_, std::unique_ptr<TypeLen> type_len_)
        : Field(AstType::ColDef), col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
};

struct CreateTable : public TreeNode {
    std::string tab_name;
    std::vector<std::unique_ptr<Field>> fields;

    CreateTable(std::string tab_name_, std::vector<std::unique_ptr<Field>> fields_)
        : TreeNode(AstType::CreateTable), tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
};

struct DropTable : public TreeNode {
    std::string tab_name;

    DropTable(std::string tab_name_) : TreeNode(AstType::DropTable), tab_name(std::move(tab_name_)) {}
};

struct DescTable : public TreeNode {
    std::string tab_name;

    DescTable(std::string tab_name_) : TreeNode(AstType::DescTable), tab_name(std::move(tab_name_)) {}
};

struct CreateIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    CreateIndex(std::string tab_name_, std::vector<std::string> col_names_)
        : TreeNode(AstType::CreateIndex), tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct DropIndex : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;

    DropIndex(std::string tab_name_, std::vector<std::string> col_names_)
        : TreeNode(AstType::DropIndex), tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
};

struct StaticCheckpoint : public TreeNode {
    StaticCheckpoint() : TreeNode(AstType::StaticCheckpoint) {}
};

struct TableRef {
    std::string table_name;
    std::string alias;

    TableRef(std::string table_name_, std::string alias_ = "")
        : table_name(std::move(table_name_)), alias(std::move(alias_)) {}
};

struct Expr : public TreeNode {
protected:
    explicit Expr(AstType type_) : TreeNode(type_) {}
};

struct Value : public Expr {
    std::string display_text;

protected:
    explicit Value(AstType type_, std::string display_text_ = "")
        : Expr(type_), display_text(std::move(display_text_)) {}
};

struct IntLit : public Value {
    int val;

    IntLit(int val_, std::string display_text_ = "") : Value(AstType::IntLit, std::move(display_text_)), val(val_) {}
};

struct FloatLit : public Value {
    float val;

    FloatLit(float val_, std::string display_text_ = "")
        : Value(AstType::FloatLit, std::move(display_text_)), val(val_) {}
};

struct StringLit : public Value {
    std::string val;

    StringLit(std::string val_, std::string display_text_ = "")
        : Value(AstType::StringLit, std::move(display_text_)), val(std::move(val_)) {}
};

struct BoolLit : public Value {
    bool val;

    BoolLit(bool val_, std::string display_text_ = "") : Value(AstType::BoolLit, std::move(display_text_)), val(val_) {}
};

struct Col : public Expr {
    std::string tab_name;
    std::string col_name;

    Col(std::string tab_name_, std::string col_name_)
        : Expr(AstType::Col), tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}
};

inline std::unique_ptr<Col> clone_col(const Col& col) {
    return std::make_unique<Col>(col.tab_name, col.col_name);
}

struct AggExpr : public Expr {
    AggFuncType func;
    bool is_star;
    std::unique_ptr<Col> col;

    AggExpr(AggFuncType func_, bool is_star_, std::unique_ptr<Col> col_)
        : Expr(AstType::AggExpr), func(func_), is_star(is_star_), col(std::move(col_)) {}
};

struct SelectItem : public TreeNode {
    std::unique_ptr<Expr> expr;
    std::string alias;

    SelectItem(std::unique_ptr<Expr> expr_, std::string alias_)
        : TreeNode(AstType::SelectItem), expr(std::move(expr_)), alias(std::move(alias_)) {}
};

struct SetClause : public TreeNode {
    std::string col_name;
    std::unique_ptr<Value> val;

    SetClause(std::string col_name_, std::unique_ptr<Value> val_)
        : TreeNode(AstType::SetClause), col_name(std::move(col_name_)), val(std::move(val_)) {}
};

struct BinaryExpr : public TreeNode {
    std::unique_ptr<Col> lhs;
    SvCompOp op;
    std::unique_ptr<Expr> rhs;

    BinaryExpr(std::unique_ptr<Col> lhs_, SvCompOp op_, std::unique_ptr<Expr> rhs_)
        : TreeNode(AstType::BinaryExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct HavingExpr : public TreeNode {
    std::unique_ptr<Expr> lhs;
    SvCompOp op;
    std::unique_ptr<Expr> rhs;

    HavingExpr(std::unique_ptr<Expr> lhs_, SvCompOp op_, std::unique_ptr<Expr> rhs_)
        : TreeNode(AstType::HavingExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderBy : public TreeNode {
    std::unique_ptr<Col> cols;
    OrderByDir orderby_dir;
    OrderBy(std::unique_ptr<Col> cols_, OrderByDir orderby_dir_)
        : TreeNode(AstType::OrderBy), cols(std::move(cols_)), orderby_dir(orderby_dir_) {}
};

struct OrderByItem : public TreeNode {
    std::unique_ptr<Expr> expr;
    OrderByDir orderby_dir;

    OrderByItem(std::unique_ptr<Expr> expr_, OrderByDir orderby_dir_)
        : TreeNode(AstType::OrderByItem), expr(std::move(expr_)), orderby_dir(orderby_dir_) {}
};

struct InsertStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::unique_ptr<Value>> vals;

    InsertStmt(std::string tab_name_, std::vector<std::unique_ptr<Value>> vals_)
        : TreeNode(AstType::InsertStmt), tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
};

struct DeleteStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::unique_ptr<BinaryExpr>> conds;

    DeleteStmt(std::string tab_name_, std::vector<std::unique_ptr<BinaryExpr>> conds_)
        : TreeNode(AstType::DeleteStmt), tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::unique_ptr<SetClause>> set_clauses;
    std::vector<std::unique_ptr<BinaryExpr>> conds;

    UpdateStmt(std::string tab_name_, std::vector<std::unique_ptr<SetClause>> set_clauses_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_)
        : TreeNode(AstType::UpdateStmt), tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)),
          conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
    std::string left;
    std::string right;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    JoinType type;

    JoinExpr(std::string left_, std::string right_, std::vector<std::unique_ptr<BinaryExpr>> conds_, JoinType type_)
        : TreeNode(AstType::JoinExpr), left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)),
          type(type_) {}
};

struct SelectStmt : public TreeNode {
    std::vector<std::unique_ptr<Col>> cols;
    std::vector<std::unique_ptr<SelectItem>> select_items;
    std::vector<TableRef> tabs;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    std::vector<std::unique_ptr<JoinExpr>> jointree;

    bool has_select_star;
    std::vector<std::unique_ptr<Col>> group_by_cols;
    std::vector<std::unique_ptr<HavingExpr>> having_conds;
    bool has_sort;
    std::unique_ptr<OrderBy> order;
    std::vector<std::unique_ptr<OrderByItem>> order_by_items;
    bool has_limit;
    int limit;

    SelectStmt(std::vector<std::unique_ptr<Col>> cols_, std::vector<TableRef> tabs_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_, std::unique_ptr<OrderBy> order_)
        : TreeNode(AstType::SelectStmt), cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
          has_select_star(cols.empty()), order(std::move(order_)), has_limit(false), limit(0) {
        for (const auto& col : cols) {
            select_items.push_back(std::make_unique<SelectItem>(clone_col(*col), ""));
        }
        if (order != nullptr) {
            order_by_items.push_back(std::make_unique<OrderByItem>(clone_col(*order->cols), order->orderby_dir));
        }
        has_sort = !order_by_items.empty();
    }

    // Backward compatibility: accepts vector<string> and converts to vector<TableRef>
    SelectStmt(std::vector<std::unique_ptr<Col>> cols_, std::vector<std::string> tab_names_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_, std::unique_ptr<OrderBy> order_)
        : TreeNode(AstType::SelectStmt), cols(std::move(cols_)), conds(std::move(conds_)),
          has_select_star(cols.empty()), order(std::move(order_)), has_limit(false), limit(0) {
        tabs.reserve(tab_names_.size());
        for (auto& name : tab_names_) {
            tabs.emplace_back(std::move(name));
        }
        for (const auto& col : cols) {
            select_items.push_back(std::make_unique<SelectItem>(clone_col(*col), ""));
        }
        if (order != nullptr) {
            order_by_items.push_back(std::make_unique<OrderByItem>(clone_col(*order->cols), order->orderby_dir));
        }
        has_sort = !order_by_items.empty();
    }

    SelectStmt(std::vector<std::unique_ptr<SelectItem>> select_items_, std::vector<TableRef> tabs_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_, std::vector<std::unique_ptr<Col>> group_by_cols_,
               std::vector<std::unique_ptr<HavingExpr>> having_conds_,
               std::vector<std::unique_ptr<OrderByItem>> order_by_items_, bool has_limit_, int limit_,
               bool has_select_star_)
        : TreeNode(AstType::SelectStmt), select_items(std::move(select_items_)), tabs(std::move(tabs_)),
          conds(std::move(conds_)), has_select_star(has_select_star_), group_by_cols(std::move(group_by_cols_)),
          having_conds(std::move(having_conds_)), has_sort(!order_by_items_.empty()),
          order_by_items(std::move(order_by_items_)), has_limit(has_limit_), limit(limit_) {
        for (const auto& item : select_items) {
            if (item == nullptr) {
                continue;
            }
            auto col = dynamic_cast<Col*>(item->expr.get());
            if (col != nullptr) {
                cols.push_back(clone_col(*col));
            }
        }
        if (order_by_items.size() == 1) {
            auto order_col = dynamic_cast<Col*>(order_by_items.front()->expr.get());
            if (order_col != nullptr) {
                order = std::make_unique<OrderBy>(clone_col(*order_col), order_by_items.front()->orderby_dir);
            }
        }
    }
};

struct UnionStmt : public TreeNode {
    std::vector<std::unique_ptr<SelectStmt>> branches;

    explicit UnionStmt(std::vector<std::unique_ptr<SelectStmt>> branches_)
        : TreeNode(AstType::UnionStmt), branches(std::move(branches_)) {}
};

struct SelectFromUnionStmt : public TreeNode {
    std::unique_ptr<UnionStmt> union_stmt;
    std::string alias;
    std::vector<std::unique_ptr<OrderByItem>> order_by_items;
    bool has_sort;

    SelectFromUnionStmt(std::unique_ptr<UnionStmt> union_stmt_, std::string alias_,
                        std::vector<std::unique_ptr<OrderByItem>> order_by_items_)
        : TreeNode(AstType::SelectFromUnionStmt), union_stmt(std::move(union_stmt_)), alias(std::move(alias_)),
          order_by_items(std::move(order_by_items_)), has_sort(!order_by_items.empty()) {}
};

struct ExplainAnalyze : public TreeNode {
    std::unique_ptr<SelectStmt> select;

    explicit ExplainAnalyze(std::unique_ptr<SelectStmt> select_)
        : TreeNode(AstType::ExplainAnalyze), select(std::move(select_)) {}
};

// set enable_nestloop
struct SetStmt : public TreeNode {
    SetKnobType set_knob_type_;
    bool bool_val_;

    SetStmt(SetKnobType& type, bool bool_value)
        : TreeNode(AstType::SetStmt), set_knob_type_(type), bool_val_(bool_value) {}
};

// set transaction isolation level
struct SetTransaction : public TreeNode {
    IsolationLevelType isolation_level_;

    explicit SetTransaction(IsolationLevelType level) : TreeNode(AstType::SetTransaction), isolation_level_(level) {}
};

struct FromClause {
    std::vector<TableRef> tables;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
};

} // namespace ast
