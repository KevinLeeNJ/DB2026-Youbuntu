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

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

enum JoinType { INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, CROSS_JOIN, NATURAL_JOIN };
namespace ast {

enum SvType { SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BOOL, SV_TYPE_DATETIME };

enum SvCompOp {
    SV_OP_EQ,
    SV_OP_NE,
    SV_OP_LT,
    SV_OP_GT,
    SV_OP_LE,
    SV_OP_GE,
    SV_OP_LIKE,
    SV_OP_IN,
    SV_OP_BETWEEN,
    SV_OP_IS_NULL,
    SV_OP_IS_NOT_NULL,
    SV_OP_EXISTS
};

enum class Quantifier { NONE, ANY, ALL };

enum class LogicalOp { AND, OR, NOT };

enum class ArithmeticOp { ADD, SUB, MUL, DIV };

enum class NullsOrder { DEFAULT, FIRST, LAST };

enum class SetOperator { UNION, INTERSECT, EXCEPT };

enum OrderByDir { OrderBy_DEFAULT, OrderBy_ASC, OrderBy_DESC };

enum AggFuncType { AGG_COUNT, AGG_MAX, AGG_MIN, AGG_SUM, AGG_AVG };

enum class WindowFuncType { ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM, AVG };

enum class ScalarFuncType { ABS, LENGTH, COALESCE, LOWER, UPPER, TRIM, ROUND, NULLIF };

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
    Col,
    AggExpr,
    ScalarFuncExpr,
    WindowExpr,
    ArithmeticExpr,
    LogicalExpr,
    CaseExpr,
    SubqueryExpr,
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
    double val;

    FloatLit(double val_, std::string display_text_ = "")
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

struct NullLit : public Value {
    explicit NullLit(std::string display_text_ = "NULL") : Value(AstType::NullLit, std::move(display_text_)) {}
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

    AggExpr(AggFuncType func_, bool is_star_, std::unique_ptr<Col> col_, bool is_distinct_ = false)
        : Expr(AstType::AggExpr), func(func_), is_star(is_star_), is_distinct(is_distinct_), col(std::move(col_)) {}
};

struct ScalarFuncExpr : public Expr {
    ScalarFuncType func;
    std::vector<std::unique_ptr<Expr>> args;

    ScalarFuncExpr(ScalarFuncType func_, std::vector<std::unique_ptr<Expr>> args_)
        : Expr(AstType::ScalarFuncExpr), func(func_), args(std::move(args_)) {}
};

struct ArithmeticExpr : public Expr {
    ArithmeticOp op;
    std::unique_ptr<Expr> lhs;
    std::unique_ptr<Expr> rhs;

    ArithmeticExpr(ArithmeticOp op_, std::unique_ptr<Expr> lhs_, std::unique_ptr<Expr> rhs_)
        : Expr(AstType::ArithmeticExpr), op(op_), lhs(std::move(lhs_)), rhs(std::move(rhs_)) {}
};

struct LogicalExpr : public Expr {
    LogicalOp op;
    std::vector<std::unique_ptr<Expr>> operands;

    LogicalExpr(LogicalOp op_, std::vector<std::unique_ptr<Expr>> operands_)
        : Expr(AstType::LogicalExpr), op(op_), operands(std::move(operands_)) {}
};

struct CaseWhen {
    std::unique_ptr<Expr> condition;
    std::unique_ptr<Expr> result;

    CaseWhen(std::unique_ptr<Expr> condition_, std::unique_ptr<Expr> result_)
        : condition(std::move(condition_)), result(std::move(result_)) {}
};

struct CaseExpr : public Expr {
    std::vector<CaseWhen> when_clauses;
    std::unique_ptr<Expr> else_expr;

    CaseExpr(std::vector<CaseWhen> when_clauses_, std::unique_ptr<Expr> else_expr_)
        : Expr(AstType::CaseExpr), when_clauses(std::move(when_clauses_)), else_expr(std::move(else_expr_)) {}
};

// The query is kept as a TreeNode to avoid a circular definition between AST expressions and SelectStmt.
struct SubqueryExpr : public Expr {
    std::unique_ptr<TreeNode> query;

    explicit SubqueryExpr(std::unique_ptr<TreeNode> query_) : Expr(AstType::SubqueryExpr), query(std::move(query_)) {}
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
    std::unique_ptr<Col> rhs_col;
    std::unique_ptr<Expr> rhs_expr;
    bool is_self_ref;
    SetOp op;

    SetClause(std::string col_name_, std::unique_ptr<Value> val_)
        : TreeNode(AstType::SetClause), col_name(std::move(col_name_)), val(std::move(val_)), is_self_ref(false),
          op(SetOp::ASSIGNMENT) {}

    SetClause(std::string col_name_, std::unique_ptr<Col> rhs_col_, std::unique_ptr<Value> val_, SetOp op_)
        : TreeNode(AstType::SetClause), col_name(std::move(col_name_)), val(std::move(val_)),
          rhs_col(std::move(rhs_col_)), is_self_ref(true), op(op_) {}

    SetClause(std::string col_name_, std::unique_ptr<Expr> rhs_expr_)
        : TreeNode(AstType::SetClause), col_name(std::move(col_name_)), rhs_expr(std::move(rhs_expr_)),
          is_self_ref(false), op(SetOp::ASSIGNMENT) {}
};

struct BinaryExpr : public Expr {
    std::unique_ptr<Expr> lhs;
    SvCompOp op;
    std::unique_ptr<Expr> rhs;
    std::unique_ptr<Expr> rhs_upper;
    std::vector<std::unique_ptr<Expr>> rhs_list;
    bool negated = false;
    Quantifier quantifier = Quantifier::NONE;

    BinaryExpr(std::unique_ptr<Expr> lhs_, SvCompOp op_, std::unique_ptr<Expr> rhs_, bool negated_ = false)
        : Expr(AstType::BinaryExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)), negated(negated_) {}
};

inline std::unique_ptr<Expr> clone_expr(const Expr& expr);

inline std::unique_ptr<BinaryExpr> clone_binary_expr(const BinaryExpr& expr) {
    auto result = std::make_unique<BinaryExpr>(expr.lhs == nullptr ? nullptr : clone_expr(*expr.lhs), expr.op,
                                               expr.rhs == nullptr ? nullptr : clone_expr(*expr.rhs), expr.negated);
    result->quantifier = expr.quantifier;
    result->rhs_upper = expr.rhs_upper == nullptr ? nullptr : clone_expr(*expr.rhs_upper);
    result->rhs_list.reserve(expr.rhs_list.size());
    for (const auto& value : expr.rhs_list) {
        result->rhs_list.push_back(clone_expr(*value));
    }
    return result;
}

struct HavingExpr : public TreeNode {
    std::unique_ptr<Expr> lhs;
    SvCompOp op;
    std::unique_ptr<Expr> rhs;
    std::unique_ptr<Expr> rhs_upper;
    std::vector<std::unique_ptr<Expr>> rhs_list;
    bool negated = false;

    HavingExpr(std::unique_ptr<Expr> lhs_, SvCompOp op_, std::unique_ptr<Expr> rhs_, bool negated_ = false)
        : TreeNode(AstType::HavingExpr), lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)), negated(negated_) {}
};

struct OrderByItem : public TreeNode {
    std::unique_ptr<Expr> expr;
    OrderByDir orderby_dir;
    NullsOrder nulls_order;
    int output_ordinal;

    OrderByItem(std::unique_ptr<Expr> expr_, OrderByDir orderby_dir_, NullsOrder nulls_order_ = NullsOrder::DEFAULT,
                int output_ordinal_ = -1)
        : TreeNode(AstType::OrderByItem), expr(std::move(expr_)), orderby_dir(orderby_dir_), nulls_order(nulls_order_),
          output_ordinal(output_ordinal_) {}
};

struct WindowExpr : public Expr {
    WindowFuncType func;
    std::vector<std::unique_ptr<Expr>> args;
    std::vector<std::unique_ptr<Expr>> partition_by;
    std::vector<std::unique_ptr<OrderByItem>> order_by;

    WindowExpr(WindowFuncType func_, std::vector<std::unique_ptr<Expr>> args_,
               std::vector<std::unique_ptr<Expr>> partition_by_, std::vector<std::unique_ptr<OrderByItem>> order_by_)
        : Expr(AstType::WindowExpr), func(func_), args(std::move(args_)), partition_by(std::move(partition_by_)),
          order_by(std::move(order_by_)) {}
};

inline std::unique_ptr<OrderByItem> clone_order_by_item(const OrderByItem& item) {
    return std::make_unique<OrderByItem>(clone_expr(*item.expr), item.orderby_dir, item.nulls_order,
                                         item.output_ordinal);
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
        return std::make_unique<AggExpr>(agg.func, agg.is_star, agg.col == nullptr ? nullptr : clone_col(*agg.col),
                                         agg.is_distinct);
    }
    case AstType::ScalarFuncExpr: {
        auto& function = static_cast<const ScalarFuncExpr&>(expr);
        std::vector<std::unique_ptr<Expr>> args;
        args.reserve(function.args.size());
        for (const auto& arg : function.args) {
            args.push_back(clone_expr(*arg));
        }
        return std::make_unique<ScalarFuncExpr>(function.func, std::move(args));
    }
    case AstType::WindowExpr: {
        auto& window = static_cast<const WindowExpr&>(expr);
        std::vector<std::unique_ptr<Expr>> args;
        args.reserve(window.args.size());
        for (const auto& arg : window.args) {
            args.push_back(clone_expr(*arg));
        }
        std::vector<std::unique_ptr<Expr>> partition_by;
        partition_by.reserve(window.partition_by.size());
        for (const auto& expr : window.partition_by) {
            partition_by.push_back(clone_expr(*expr));
        }
        std::vector<std::unique_ptr<OrderByItem>> order_by;
        order_by.reserve(window.order_by.size());
        for (const auto& item : window.order_by) {
            order_by.push_back(clone_order_by_item(*item));
        }
        return std::make_unique<WindowExpr>(window.func, std::move(args), std::move(partition_by), std::move(order_by));
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
    case AstType::NullLit: {
        auto& lit = static_cast<const NullLit&>(expr);
        return std::make_unique<NullLit>(lit.display_text);
    }
    case AstType::ArithmeticExpr: {
        auto& arithmetic = static_cast<const ArithmeticExpr&>(expr);
        return std::make_unique<ArithmeticExpr>(arithmetic.op, clone_expr(*arithmetic.lhs),
                                                clone_expr(*arithmetic.rhs));
    }
    case AstType::LogicalExpr: {
        auto& logical = static_cast<const LogicalExpr&>(expr);
        std::vector<std::unique_ptr<Expr>> operands;
        operands.reserve(logical.operands.size());
        for (const auto& operand : logical.operands) {
            operands.push_back(clone_expr(*operand));
        }
        return std::make_unique<LogicalExpr>(logical.op, std::move(operands));
    }
    case AstType::CaseExpr: {
        auto& case_expr = static_cast<const CaseExpr&>(expr);
        std::vector<CaseWhen> clauses;
        clauses.reserve(case_expr.when_clauses.size());
        for (const auto& clause : case_expr.when_clauses) {
            clauses.emplace_back(clone_expr(*clause.condition), clone_expr(*clause.result));
        }
        return std::make_unique<CaseExpr>(std::move(clauses),
                                          case_expr.else_expr == nullptr ? nullptr : clone_expr(*case_expr.else_expr));
    }
    case AstType::BinaryExpr: {
        return clone_binary_expr(static_cast<const BinaryExpr&>(expr));
    }
    case AstType::SubqueryExpr:
        throw std::logic_error("subquery expressions cannot be cloned without a query-tree clone");
    default:
        throw std::logic_error("unsupported expression type for AST clone");
    }
}

struct InsertStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::string> col_names;
    std::vector<std::unique_ptr<Value>> vals;
    std::unique_ptr<TreeNode> select;

    InsertStmt(std::string tab_name_, std::vector<std::string> col_names_, std::vector<std::unique_ptr<Value>> vals_)
        : TreeNode(AstType::InsertStmt), tab_name(std::move(tab_name_)), col_names(std::move(col_names_)),
          vals(std::move(vals_)) {}

    InsertStmt(std::string tab_name_, std::vector<std::string> col_names_, std::unique_ptr<TreeNode> select_)
        : TreeNode(AstType::InsertStmt), tab_name(std::move(tab_name_)), col_names(std::move(col_names_)),
          select(std::move(select_)) {}
};

struct DeleteStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    std::unique_ptr<Expr> where_expr;

    DeleteStmt(std::string tab_name_, std::vector<std::unique_ptr<BinaryExpr>> conds_)
        : TreeNode(AstType::DeleteStmt), tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
};

struct UpdateStmt : public TreeNode {
    std::string tab_name;
    std::vector<std::unique_ptr<SetClause>> set_clauses;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    std::unique_ptr<Expr> where_expr;

    UpdateStmt(std::string tab_name_, std::vector<std::unique_ptr<SetClause>> set_clauses_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_)
        : TreeNode(AstType::UpdateStmt), tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)),
          conds(std::move(conds_)) {}
};

struct JoinExpr : public TreeNode {
    TableRef left;
    TableRef right;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    std::unique_ptr<Expr> condition;
    JoinType join_type;

    JoinExpr(TableRef left_, TableRef right_, std::vector<std::unique_ptr<BinaryExpr>> conds_, JoinType type_,
             std::unique_ptr<Expr> condition_ = nullptr)
        : TreeNode(AstType::JoinExpr), left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)),
          condition(std::move(condition_)), join_type(type_) {}
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
    bool has_distinct;
    bool has_offset;
    int offset;
    std::unique_ptr<Expr> where_expr;
    std::unique_ptr<Expr> having_expr;

    SelectStmt(std::vector<std::unique_ptr<SelectItem>> select_items_, std::vector<TableRef> tabs_,
               std::vector<std::unique_ptr<BinaryExpr>> conds_, std::vector<std::unique_ptr<Col>> group_by_cols_,
               std::vector<std::unique_ptr<HavingExpr>> having_conds_,
               std::vector<std::unique_ptr<OrderByItem>> order_by_items_, bool has_limit_, int limit_,
               bool has_select_star_, std::vector<std::unique_ptr<JoinExpr>> jointree_ = {}, bool has_distinct_ = false,
               bool has_offset_ = false, int offset_ = 0, std::unique_ptr<Expr> where_expr_ = nullptr,
               std::unique_ptr<Expr> having_expr_ = nullptr)
        : TreeNode(AstType::SelectStmt), select_items(std::move(select_items_)), tabs(std::move(tabs_)),
          conds(std::move(conds_)), jointree(std::move(jointree_)), has_select_star(has_select_star_),
          group_by_cols(std::move(group_by_cols_)), having_conds(std::move(having_conds_)),
          has_sort(!order_by_items_.empty()), order_by_items(std::move(order_by_items_)), has_limit(has_limit_),
          limit(limit_), has_distinct(has_distinct_), has_offset(has_offset_), offset(offset_),
          where_expr(std::move(where_expr_)), having_expr(std::move(having_expr_)) {
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
    std::vector<bool> union_all;
    std::vector<SetOperator> operators;

    explicit UnionStmt(std::vector<std::unique_ptr<SelectStmt>> branches_, std::vector<bool> union_all_ = {},
                       std::vector<SetOperator> operators_ = {})
        : TreeNode(AstType::UnionStmt), branches(std::move(branches_)), union_all(std::move(union_all_)),
          operators(std::move(operators_)) {}
};

struct SelectFromUnionStmt : public TreeNode {
    std::unique_ptr<UnionStmt> union_stmt;
    std::string alias;
    std::unique_ptr<OrderBy> order;
    std::vector<std::unique_ptr<OrderByItem>> order_by_items;
    bool has_sort;
    bool has_limit = false;
    int limit = 0;
    bool has_offset = false;
    int offset = 0;

    SelectFromUnionStmt(std::unique_ptr<UnionStmt> union_stmt_, std::string alias_,
                        std::vector<std::unique_ptr<OrderByItem>> order_by_items_, bool has_limit_ = false,
                        int limit_ = 0, bool has_offset_ = false, int offset_ = 0)
        : TreeNode(AstType::SelectFromUnionStmt), union_stmt(std::move(union_stmt_)), alias(std::move(alias_)),
          order_by_items(std::move(order_by_items_)), has_sort(!order_by_items.empty()), has_limit(has_limit_),
          limit(limit_), has_offset(has_offset_), offset(offset_) {
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

} // namespace ast
