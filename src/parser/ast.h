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
#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

enum JoinType { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN };
namespace ast {

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL, SV_TYPE_DATETIME };

// SV_OP_IS_NULL / SV_OP_IS_NOT_NULL 不是二元比较，rhs 恒为 NullLit 占位。
// 它们与 `col = NULL`（恒假）语义不同，因此不能复用 SV_OP_EQ / SV_OP_NE。
enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE, SV_OP_IS_NULL, SV_OP_IS_NOT_NULL };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum AggFuncType { AGG_COUNT, AGG_MAX, AGG_MIN, AGG_SUM, AGG_AVG };

enum SetKnobType { EnableNestLoop, EnableSortMerge };

enum class SetOp { SELF_ADD, SELF_SUB, SELF_MUL, SELF_DIV, ASSIGNMENT };

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
    NullLit,
    Parameter,
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
    StaticCheckpoint,
    SetOutputFile,
    LoadStmt
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

/* SQL NULL 字面量。没有值，只有类型标记；display_text 固定为 "NULL"。 */
struct NullLit : public Value {
    NullLit() : Value(AstType::NullLit, "NULL") {}
};

struct Parameter : public Value {
    std::size_t ordinal;
    std::optional<SvType> declared_type;

    explicit Parameter(std::size_t ordinal_, std::optional<SvType> declared_type_ = std::nullopt)
        : Value(AstType::Parameter), ordinal(ordinal_), declared_type(declared_type_) {}
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
    bool is_distinct;
    std::unique_ptr<Col> col;

    AggExpr(AggFuncType func_, bool is_star_, bool is_distinct_, std::unique_ptr<Col> col_)
        : Expr(AstType::AggExpr), func(func_), is_star(is_star_), is_distinct(is_distinct_), col(std::move(col_)) {}

    AggExpr(AggFuncType func_, bool is_star_, std::unique_ptr<Col> col_)
        : AggExpr(func_, is_star_, false, std::move(col_)) {}
};

struct SelectItem : public TreeNode {
    std::unique_ptr<Expr> expr;
    std::string alias;

    SelectItem(std::unique_ptr<Expr> expr_, std::string alias_)
        : TreeNode(AstType::SelectItem), expr(std::move(expr_)), alias(std::move(alias_)) {}
};

struct UpdateTerm {
    std::unique_ptr<Value> val;
    SetOp op;

    UpdateTerm(std::unique_ptr<Value> val_, SetOp op_) : val(std::move(val_)), op(op_) {}
};

struct SetClause : public TreeNode {
    std::string col_name;
    std::unique_ptr<Value> val;
    std::unique_ptr<Col> rhs_col;
    bool is_self_ref;
    SetOp op;
    std::vector<UpdateTerm> additional_terms;

    SetClause(std::string col_name_, std::unique_ptr<Value> val_)
        : TreeNode(AstType::SetClause), col_name(std::move(col_name_)), val(std::move(val_)), is_self_ref(false),
          op(SetOp::ASSIGNMENT) {}

    SetClause(std::string col_name_, std::unique_ptr<Col> rhs_col_, std::unique_ptr<Value> val_, SetOp op_,
              std::vector<UpdateTerm> additional_terms_ = {})
        : TreeNode(AstType::SetClause), col_name(std::move(col_name_)), val(std::move(val_)),
          rhs_col(std::move(rhs_col_)), is_self_ref(true), op(op_), additional_terms(std::move(additional_terms_)) {}
};

struct BinaryExpr : public TreeNode {
    std::unique_ptr<Expr> lhs;
    SvCompOp op;
    std::unique_ptr<Expr> rhs;

    BinaryExpr(std::unique_ptr<Expr> lhs_, SvCompOp op_, std::unique_ptr<Expr> rhs_)
        : TreeNode(AstType::BinaryExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

inline std::unique_ptr<Expr> clone_expr(const Expr& expr);

inline std::unique_ptr<BinaryExpr> clone_binary_expr(const BinaryExpr& expr) {
    return std::make_unique<BinaryExpr>(clone_expr(*expr.lhs), expr.op, clone_expr(*expr.rhs));
}

struct HavingExpr : public TreeNode {
    std::unique_ptr<Expr> lhs;
    SvCompOp op;
    std::unique_ptr<Expr> rhs;

    HavingExpr(std::unique_ptr<Expr> lhs_, SvCompOp op_, std::unique_ptr<Expr> rhs_)
        : TreeNode(AstType::HavingExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
};

struct OrderByItem : public TreeNode {
    std::unique_ptr<Expr> expr;
    OrderByDir orderby_dir;

    OrderByItem(std::unique_ptr<Expr> expr_, OrderByDir orderby_dir_)
        : TreeNode(AstType::OrderByItem), expr(std::move(expr_)), orderby_dir(orderby_dir_) {}
};

inline std::unique_ptr<OrderByItem> clone_order_by_item(const OrderByItem& item) {
    return std::make_unique<OrderByItem>(clone_expr(*item.expr), item.orderby_dir);
}

struct OrderBy : public TreeNode {
    std::vector<std::unique_ptr<OrderByItem>> items;

    explicit OrderBy(std::vector<std::unique_ptr<OrderByItem>> items_)
        : TreeNode(AstType::OrderBy), items(std::move(items_)) {}
};

inline std::unique_ptr<Expr> clone_expr(const Expr& expr) {
    switch (expr.type) {
    case AstType::Col: {
        auto& col = static_cast<const Col&>(expr);
        return clone_col(col);
    }
    case AstType::AggExpr: {
        auto& agg = static_cast<const AggExpr&>(expr);
        return std::make_unique<AggExpr>(agg.func, agg.is_star, agg.is_distinct,
                                         agg.col == nullptr ? nullptr : clone_col(*agg.col));
    }
    case AstType::IntLit: {
        auto& lit = static_cast<const IntLit&>(expr);
        return std::make_unique<IntLit>(lit.val, lit.display_text);
    }
    case AstType::FloatLit: {
        auto& lit = static_cast<const FloatLit&>(expr);
        return std::make_unique<FloatLit>(lit.val, lit.display_text);
    }
    case AstType::StringLit: {
        auto& lit = static_cast<const StringLit&>(expr);
        return std::make_unique<StringLit>(lit.val, lit.display_text);
    }
    case AstType::BoolLit: {
        auto& lit = static_cast<const BoolLit&>(expr);
        return std::make_unique<BoolLit>(lit.val, lit.display_text);
    }
    case AstType::NullLit:
        return std::make_unique<NullLit>();
    case AstType::Parameter: {
        auto& parameter = static_cast<const Parameter&>(expr);
        return std::make_unique<Parameter>(parameter.ordinal, parameter.declared_type);
    }
    default:
        throw std::logic_error("unsupported expression type for AST clone");
    }
}

inline std::unique_ptr<Value> clone_value(const Value& value) {
    switch (value.type) {
    case AstType::IntLit: {
        const auto& x = static_cast<const IntLit&>(value);
        return std::make_unique<IntLit>(x.val, x.display_text);
    }
    case AstType::FloatLit: {
        const auto& x = static_cast<const FloatLit&>(value);
        return std::make_unique<FloatLit>(x.val, x.display_text);
    }
    case AstType::StringLit: {
        const auto& x = static_cast<const StringLit&>(value);
        return std::make_unique<StringLit>(x.val, x.display_text);
    }
    case AstType::BoolLit: {
        const auto& x = static_cast<const BoolLit&>(value);
        return std::make_unique<BoolLit>(x.val, x.display_text);
    }
    case AstType::NullLit:
        return std::make_unique<NullLit>();
    case AstType::Parameter: {
        const auto& x = static_cast<const Parameter&>(value);
        return std::make_unique<Parameter>(x.ordinal, x.declared_type);
    }
    default:
        throw std::logic_error("unsupported value type for AST clone");
    }
}

inline std::unique_ptr<Value> clone_bound_value(const Value& value,
                                                const std::vector<std::unique_ptr<Value>>& bindings) {
    if (value.type != AstType::Parameter)
        return clone_value(value);
    const auto& parameter = static_cast<const Parameter&>(value);
    if (parameter.ordinal == 0 || parameter.ordinal > bindings.size() || bindings[parameter.ordinal - 1] == nullptr) {
        throw std::logic_error("unbound AST parameter");
    }
    if (bindings[parameter.ordinal - 1]->type == AstType::Parameter &&
        static_cast<const Parameter&>(*bindings[parameter.ordinal - 1]).ordinal != parameter.ordinal) {
        throw std::logic_error("mismatched AST parameter binding");
    }
    return clone_value(*bindings[parameter.ordinal - 1]);
}

inline std::unique_ptr<Expr> clone_expr_bound(const Expr& expr, const std::vector<std::unique_ptr<Value>>& bindings) {
    if (expr.type == AstType::Parameter || expr.type == AstType::IntLit || expr.type == AstType::FloatLit ||
        expr.type == AstType::StringLit || expr.type == AstType::BoolLit || expr.type == AstType::NullLit) {
        return clone_bound_value(static_cast<const Value&>(expr), bindings);
    }
    if (expr.type == AstType::Col)
        return clone_col(static_cast<const Col&>(expr));
    if (expr.type == AstType::AggExpr) {
        const auto& x = static_cast<const AggExpr&>(expr);
        return std::make_unique<AggExpr>(x.func, x.is_star, x.is_distinct, x.col ? clone_col(*x.col) : nullptr);
    }
    throw std::logic_error("unsupported expression for AST bind");
}

inline std::unique_ptr<BinaryExpr> clone_binary_bound(const BinaryExpr& x,
                                                      const std::vector<std::unique_ptr<Value>>& b) {
    return std::make_unique<BinaryExpr>(clone_expr_bound(*x.lhs, b), x.op, clone_expr_bound(*x.rhs, b));
}

inline std::unique_ptr<HavingExpr> clone_having_bound(const HavingExpr& x,
                                                      const std::vector<std::unique_ptr<Value>>& b) {
    return std::make_unique<HavingExpr>(clone_expr_bound(*x.lhs, b), x.op, clone_expr_bound(*x.rhs, b));
}

inline std::unique_ptr<OrderByItem> clone_order_bound(const OrderByItem& x,
                                                      const std::vector<std::unique_ptr<Value>>& b) {
    return std::make_unique<OrderByItem>(clone_expr_bound(*x.expr, b), x.orderby_dir);
}

inline std::unique_ptr<TreeNode> clone_bound_tree(const TreeNode& root, const std::vector<std::unique_ptr<Value>>& b);

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
    TableRef left;
    TableRef right;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    JoinType join_type;

    JoinExpr(TableRef left_, TableRef right_, std::vector<std::unique_ptr<BinaryExpr>> conds_, JoinType type_)
        : TreeNode(AstType::JoinExpr), left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)),
          join_type(type_) {}
};

struct SelectStmt : public TreeNode {
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
    bool limit_is_parameter;
    std::size_t limit_parameter;
    int offset;
    bool offset_is_parameter;
    std::size_t offset_parameter;

    SelectStmt(std::vector<std::unique_ptr<SelectItem>> select_items_, std::vector<TableRef> tabs_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_, std::vector<std::unique_ptr<Col>> group_by_cols_,
               std::vector<std::unique_ptr<HavingExpr>> having_conds_,
               std::vector<std::unique_ptr<OrderByItem>> order_by_items_, bool has_limit_, int limit_,
               bool has_select_star_, std::vector<std::unique_ptr<JoinExpr>> jointree_ = {},
               bool limit_is_parameter_ = false, std::size_t limit_parameter_ = 0, int offset_ = 0,
               bool offset_is_parameter_ = false, std::size_t offset_parameter_ = 0)
        : TreeNode(AstType::SelectStmt), select_items(std::move(select_items_)), tabs(std::move(tabs_)),
          conds(std::move(conds_)), jointree(std::move(jointree_)), has_select_star(has_select_star_),
          group_by_cols(std::move(group_by_cols_)), having_conds(std::move(having_conds_)),
          has_sort(!order_by_items_.empty()), order_by_items(std::move(order_by_items_)), has_limit(has_limit_),
          limit(limit_), limit_is_parameter(limit_is_parameter_), limit_parameter(limit_parameter_), offset(offset_),
          offset_is_parameter(offset_is_parameter_), offset_parameter(offset_parameter_) {
        if (!order_by_items.empty()) {
            std::vector<std::unique_ptr<OrderByItem>> order_items;
            order_items.reserve(order_by_items.size());
            for (const auto& item : order_by_items) {
                order_items.push_back(clone_order_by_item(*item));
            }
            order = std::make_unique<OrderBy>(std::move(order_items));
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
    std::unique_ptr<OrderBy> order;
    std::vector<std::unique_ptr<OrderByItem>> order_by_items;
    bool has_sort;

    SelectFromUnionStmt(std::unique_ptr<UnionStmt> union_stmt_, std::string alias_,
                        std::vector<std::unique_ptr<OrderByItem>> order_by_items_)
        : TreeNode(AstType::SelectFromUnionStmt), union_stmt(std::move(union_stmt_)), alias(std::move(alias_)),
          order_by_items(std::move(order_by_items_)), has_sort(!order_by_items.empty()) {
        if (!order_by_items.empty()) {
            std::vector<std::unique_ptr<OrderByItem>> order_items;
            order_items.reserve(order_by_items.size());
            for (const auto& item : order_by_items) {
                order_items.push_back(clone_order_by_item(*item));
            }
            order = std::make_unique<OrderBy>(std::move(order_items));
        }
    }
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

// set output_file on|off
struct SetOutputFile : public TreeNode {
    bool enable_;

    explicit SetOutputFile(bool enable) : TreeNode(AstType::SetOutputFile), enable_(enable) {}
};

// load file_name into table_name
struct LoadStmt : public TreeNode {
    std::string file_name_;
    std::string tab_name_;

    LoadStmt(std::string file_name, std::string tab_name)
        : TreeNode(AstType::LoadStmt), file_name_(std::move(file_name)), tab_name_(std::move(tab_name)) {}
};

inline std::unique_ptr<SetClause> clone_set_bound(const SetClause& x, const std::vector<std::unique_ptr<Value>>& b) {
    std::vector<UpdateTerm> additional_terms;
    additional_terms.reserve(x.additional_terms.size());
    for (const auto& term : x.additional_terms) {
        additional_terms.emplace_back(clone_bound_value(*term.val, b), term.op);
    }
    return std::make_unique<SetClause>(
        x.col_name, x.rhs_col ? std::make_unique<Col>(x.rhs_col->tab_name, x.rhs_col->col_name) : nullptr,
        x.val ? clone_bound_value(*x.val, b) : nullptr, x.op, std::move(additional_terms));
}

inline std::unique_ptr<JoinExpr> clone_join_bound(const JoinExpr& x, const std::vector<std::unique_ptr<Value>>& b) {
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    for (const auto& cond : x.conds)
        conds.push_back(clone_binary_bound(*cond, b));
    return std::make_unique<JoinExpr>(x.left, x.right, std::move(conds), x.join_type);
}

inline std::unique_ptr<SelectStmt> clone_select_bound(const SelectStmt& x,
                                                      const std::vector<std::unique_ptr<Value>>& b) {
    std::vector<std::unique_ptr<SelectItem>> items;
    for (const auto& item : x.select_items)
        items.push_back(std::make_unique<SelectItem>(clone_expr_bound(*item->expr, b), item->alias));
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    for (const auto& cond : x.conds)
        conds.push_back(clone_binary_bound(*cond, b));
    std::vector<std::unique_ptr<Col>> groups;
    for (const auto& col : x.group_by_cols)
        groups.push_back(clone_col(*col));
    std::vector<std::unique_ptr<HavingExpr>> having;
    for (const auto& cond : x.having_conds)
        having.push_back(clone_having_bound(*cond, b));
    std::vector<std::unique_ptr<OrderByItem>> order;
    for (const auto& item : x.order_by_items)
        order.push_back(clone_order_bound(*item, b));
    std::vector<std::unique_ptr<JoinExpr>> joins;
    for (const auto& join : x.jointree)
        joins.push_back(clone_join_bound(*join, b));
    int limit = x.limit;
    bool limit_is_parameter = false;
    std::size_t limit_parameter = 0;
    if (x.limit_is_parameter) {
        if (x.limit_parameter == 0 || x.limit_parameter > b.size() || b[x.limit_parameter - 1] == nullptr)
            throw std::logic_error("LIMIT parameter must be INT32");
        const auto* binding = b[x.limit_parameter - 1].get();
        if (binding->type == AstType::IntLit) {
            limit = static_cast<const IntLit*>(binding)->val;
            if (limit < 0)
                throw std::logic_error("LIMIT parameter must be non-negative");
        } else if (binding->type == AstType::Parameter) {
            const auto* parameter = static_cast<const Parameter*>(binding);
            if (!parameter->declared_type.has_value() || parameter->declared_type != SV_TYPE_INT)
                throw std::logic_error("LIMIT parameter must be INT32");
            limit = 0;
            limit_is_parameter = true;
            limit_parameter = parameter->ordinal;
        } else {
            throw std::logic_error("LIMIT parameter must be INT32");
        }
    }
    int offset = x.offset;
    bool offset_is_parameter = false;
    std::size_t offset_parameter = 0;
    if (x.offset_is_parameter) {
        if (x.offset_parameter == 0 || x.offset_parameter > b.size() || b[x.offset_parameter - 1] == nullptr)
            throw std::logic_error("OFFSET parameter must be INT32");
        const auto* binding = b[x.offset_parameter - 1].get();
        if (binding->type == AstType::IntLit) {
            offset = static_cast<const IntLit*>(binding)->val;
            if (offset < 0)
                throw std::logic_error("OFFSET parameter must be non-negative");
        } else if (binding->type == AstType::Parameter) {
            const auto* parameter = static_cast<const Parameter*>(binding);
            if (!parameter->declared_type.has_value() || parameter->declared_type != SV_TYPE_INT)
                throw std::logic_error("OFFSET parameter must be INT32");
            offset = 0;
            offset_is_parameter = true;
            offset_parameter = parameter->ordinal;
        } else {
            throw std::logic_error("OFFSET parameter must be INT32");
        }
    }
    if (!limit_is_parameter && limit < 0)
        throw std::logic_error("LIMIT must be non-negative");
    if (!offset_is_parameter && offset < 0)
        throw std::logic_error("OFFSET must be non-negative");
    if (!limit_is_parameter && !offset_is_parameter &&
        static_cast<long long>(limit) + static_cast<long long>(offset) > std::numeric_limits<int>::max()) {
        throw std::logic_error("LIMIT plus OFFSET exceeds INT32");
    }
    return std::make_unique<SelectStmt>(std::move(items), x.tabs, std::move(conds), std::move(groups),
                                        std::move(having), std::move(order), x.has_limit, limit, x.has_select_star,
                                        std::move(joins), limit_is_parameter, limit_parameter, offset,
                                        offset_is_parameter, offset_parameter);
}

inline std::unique_ptr<TreeNode> clone_bound_tree(const TreeNode& root, const std::vector<std::unique_ptr<Value>>& b) {
    switch (root.type) {
    case AstType::InsertStmt: {
        const auto& x = static_cast<const InsertStmt&>(root);
        std::vector<std::unique_ptr<Value>> vals;
        for (const auto& v : x.vals)
            vals.push_back(clone_bound_value(*v, b));
        return std::make_unique<InsertStmt>(x.tab_name, std::move(vals));
    }
    case AstType::DeleteStmt: {
        const auto& x = static_cast<const DeleteStmt&>(root);
        std::vector<std::unique_ptr<BinaryExpr>> conds;
        for (const auto& c : x.conds)
            conds.push_back(clone_binary_bound(*c, b));
        return std::make_unique<DeleteStmt>(x.tab_name, std::move(conds));
    }
    case AstType::UpdateStmt: {
        const auto& x = static_cast<const UpdateStmt&>(root);
        std::vector<std::unique_ptr<SetClause>> sets, conds_dummy;
        for (const auto& s : x.set_clauses) {
            std::vector<UpdateTerm> additional_terms;
            additional_terms.reserve(s->additional_terms.size());
            for (const auto& term : s->additional_terms) {
                additional_terms.emplace_back(clone_bound_value(*term.val, b), term.op);
            }
            std::unique_ptr<SetClause> copy;
            if (s->is_self_ref)
                copy = std::make_unique<SetClause>(
                    s->col_name, std::make_unique<Col>(s->rhs_col->tab_name, s->rhs_col->col_name),
                    s->val ? clone_bound_value(*s->val, b) : nullptr, s->op, std::move(additional_terms));
            else
                copy = std::make_unique<SetClause>(s->col_name, s->val ? clone_bound_value(*s->val, b) : nullptr);
            sets.push_back(std::move(copy));
        }
        std::vector<std::unique_ptr<BinaryExpr>> conds;
        for (const auto& c : x.conds)
            conds.push_back(clone_binary_bound(*c, b));
        return std::make_unique<UpdateStmt>(x.tab_name, std::move(sets), std::move(conds));
    }
    case AstType::SelectStmt:
        return clone_select_bound(static_cast<const SelectStmt&>(root), b);
    case AstType::TxnBegin:
        return std::make_unique<TxnBegin>();
    case AstType::TxnCommit:
        return std::make_unique<TxnCommit>();
    case AstType::TxnAbort:
        return std::make_unique<TxnAbort>();
    case AstType::TxnRollback:
        return std::make_unique<TxnRollback>();
    default:
        throw std::logic_error("prepared statement type is not supported");
    }
}

} // namespace ast
