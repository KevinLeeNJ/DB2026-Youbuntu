#undef NDEBUG

#define private public
#include "parser/ast.h"
#include "parser/ast_printer.h"
#undef private

#include <sstream>
#include <iostream>

#include "gtest/gtest.h"

// =============================================================================
// AST node kind 测试 — 验证每个节点的构造函数正确设置 kind
// =============================================================================

// --- 简单叶子节点 ---

TEST(AstKindTest, help_node) {
    auto node = std::make_shared<ast::Help>();
    EXPECT_EQ(node->kind, ast::AstNodeKind::Help);
}

TEST(AstKindTest, show_tables_node) {
    auto node = std::make_shared<ast::ShowTables>();
    EXPECT_EQ(node->kind, ast::AstNodeKind::ShowTables);
}

TEST(AstKindTest, txn_begin_node) {
    auto node = std::make_shared<ast::TxnBegin>();
    EXPECT_EQ(node->kind, ast::AstNodeKind::TxnBegin);
}

TEST(AstKindTest, txn_commit_node) {
    auto node = std::make_shared<ast::TxnCommit>();
    EXPECT_EQ(node->kind, ast::AstNodeKind::TxnCommit);
}

TEST(AstKindTest, txn_abort_node) {
    auto node = std::make_shared<ast::TxnAbort>();
    EXPECT_EQ(node->kind, ast::AstNodeKind::TxnAbort);
}

TEST(AstKindTest, txn_rollback_node) {
    auto node = std::make_shared<ast::TxnRollback>();
    EXPECT_EQ(node->kind, ast::AstNodeKind::TxnRollback);
}

// --- TypeLen ---

TEST(AstKindTest, type_len_node) {
    auto node = std::make_shared<ast::TypeLen>(ast::SV_TYPE_INT, 4);
    EXPECT_EQ(node->kind, ast::AstNodeKind::TypeLen);
}

// --- ColDef ---

TEST(AstKindTest, col_def_node) {
    auto type_len = std::make_shared<ast::TypeLen>(ast::SV_TYPE_INT, 4);
    auto node = std::make_shared<ast::ColDef>("id", type_len);
    EXPECT_EQ(node->kind, ast::AstNodeKind::ColDef);
}

// --- DDL 语句 ---

TEST(AstKindTest, create_table_node) {
    auto type_len = std::make_shared<ast::TypeLen>(ast::SV_TYPE_INT, 4);
    auto col_def = std::make_shared<ast::ColDef>("id", type_len);
    std::vector<std::shared_ptr<ast::Field>> fields = {col_def};
    auto node = std::make_shared<ast::CreateTable>("t", fields);
    EXPECT_EQ(node->kind, ast::AstNodeKind::CreateTable);
}

TEST(AstKindTest, drop_table_node) {
    auto node = std::make_shared<ast::DropTable>("t");
    EXPECT_EQ(node->kind, ast::AstNodeKind::DropTable);
}

TEST(AstKindTest, desc_table_node) {
    auto node = std::make_shared<ast::DescTable>("t");
    EXPECT_EQ(node->kind, ast::AstNodeKind::DescTable);
}

TEST(AstKindTest, create_index_node) {
    auto node = std::make_shared<ast::CreateIndex>("t", std::vector<std::string>{"a", "b"});
    EXPECT_EQ(node->kind, ast::AstNodeKind::CreateIndex);
}

TEST(AstKindTest, drop_index_node) {
    auto node = std::make_shared<ast::DropIndex>("t", std::vector<std::string>{"a"});
    EXPECT_EQ(node->kind, ast::AstNodeKind::DropIndex);
}

// --- 表达式节点 ---

TEST(AstKindTest, int_lit_node) {
    auto node = std::make_shared<ast::IntLit>(42);
    EXPECT_EQ(node->kind, ast::AstNodeKind::IntLit);
}

TEST(AstKindTest, float_lit_node) {
    auto node = std::make_shared<ast::FloatLit>(3.14f);
    EXPECT_EQ(node->kind, ast::AstNodeKind::FloatLit);
}

TEST(AstKindTest, string_lit_node) {
    auto node = std::make_shared<ast::StringLit>("hello");
    EXPECT_EQ(node->kind, ast::AstNodeKind::StringLit);
}

TEST(AstKindTest, bool_lit_node) {
    auto node = std::make_shared<ast::BoolLit>(true);
    EXPECT_EQ(node->kind, ast::AstNodeKind::BoolLit);
}

TEST(AstKindTest, col_node) {
    auto node = std::make_shared<ast::Col>("t", "x");
    EXPECT_EQ(node->kind, ast::AstNodeKind::Col);
}

// --- SetClause ---

TEST(AstKindTest, set_clause_node) {
    auto val = std::make_shared<ast::IntLit>(10);
    auto node = std::make_shared<ast::SetClause>("x", val);
    EXPECT_EQ(node->kind, ast::AstNodeKind::SetClause);
}

// --- BinaryExpr ---

TEST(AstKindTest, binary_expr_node) {
    auto lhs = std::make_shared<ast::Col>("t", "x");
    auto rhs = std::make_shared<ast::IntLit>(10);
    auto node = std::make_shared<ast::BinaryExpr>(lhs, ast::SV_OP_EQ, rhs);
    EXPECT_EQ(node->kind, ast::AstNodeKind::BinaryExpr);
}

// --- OrderBy ---

TEST(AstKindTest, order_by_node) {
    auto col = std::make_shared<ast::Col>("t", "x");
    auto node = std::make_shared<ast::OrderBy>(col, ast::OrderBy_ASC);
    EXPECT_EQ(node->kind, ast::AstNodeKind::OrderBy);
}

// --- DML 语句 ---

TEST(AstKindTest, insert_stmt_node) {
    auto val = std::make_shared<ast::IntLit>(1);
    std::vector<std::shared_ptr<ast::Value>> vals = {val};
    auto node = std::make_shared<ast::InsertStmt>("t", vals);
    EXPECT_EQ(node->kind, ast::AstNodeKind::InsertStmt);
}

TEST(AstKindTest, delete_stmt_node) {
    std::vector<std::shared_ptr<ast::BinaryExpr>> conds;
    auto node = std::make_shared<ast::DeleteStmt>("t", conds);
    EXPECT_EQ(node->kind, ast::AstNodeKind::DeleteStmt);
}

TEST(AstKindTest, update_stmt_node) {
    auto val = std::make_shared<ast::IntLit>(10);
    auto set_clause = std::make_shared<ast::SetClause>("x", val);
    std::vector<std::shared_ptr<ast::SetClause>> set_clauses = {set_clause};
    std::vector<std::shared_ptr<ast::BinaryExpr>> conds;
    auto node = std::make_shared<ast::UpdateStmt>("t", set_clauses, conds);
    EXPECT_EQ(node->kind, ast::AstNodeKind::UpdateStmt);
}

TEST(AstKindTest, select_stmt_node) {
    auto col = std::make_shared<ast::Col>("t", "x");
    std::vector<std::shared_ptr<ast::Col>> cols = {col};
    std::vector<std::string> tabs = {"t"};
    std::vector<std::shared_ptr<ast::BinaryExpr>> conds;
    auto node = std::make_shared<ast::SelectStmt>(cols, tabs, conds, nullptr);
    EXPECT_EQ(node->kind, ast::AstNodeKind::SelectStmt);
}

// --- SetStmt ---

TEST(AstKindTest, set_stmt_node) {
    auto type = ast::SetKnobType::EnableNestLoop;
    auto node = std::make_shared<ast::SetStmt>(type, true);
    EXPECT_EQ(node->kind, ast::AstNodeKind::SetStmt);
}

// --- JoinExpr ---

TEST(AstKindTest, join_expr_node) {
    std::vector<std::shared_ptr<ast::BinaryExpr>> conds;
    auto node = std::make_shared<ast::JoinExpr>("t1", "t2", conds, INNER_JOIN);
    EXPECT_EQ(node->kind, ast::AstNodeKind::JoinExpr);
}

// =============================================================================
// TreePrinter::print 冒烟测试 — 验证不会崩溃
// =============================================================================

class TreePrinterSmokeTest : public ::testing::Test {
protected:
    void SetUp() override {
        old_buf_ = std::cout.rdbuf(buffer_.rdbuf());
    }

    void TearDown() override {
        std::cout.rdbuf(old_buf_);
    }

    std::stringstream buffer_;

private:
    std::streambuf* old_buf_;
};

TEST_F(TreePrinterSmokeTest, print_select_stmt) {
    auto col = std::make_shared<ast::Col>("t", "x");
    std::vector<std::shared_ptr<ast::Col>> cols = {col};
    std::vector<std::string> tabs = {"t"};
    std::vector<std::shared_ptr<ast::BinaryExpr>> conds;
    auto node = std::make_shared<ast::SelectStmt>(cols, tabs, conds, nullptr);
    EXPECT_NO_THROW(ast::TreePrinter::print(node));
}

TEST_F(TreePrinterSmokeTest, print_insert_stmt) {
    auto val = std::make_shared<ast::IntLit>(1);
    std::vector<std::shared_ptr<ast::Value>> vals = {val};
    auto node = std::make_shared<ast::InsertStmt>("t", vals);
    EXPECT_NO_THROW(ast::TreePrinter::print(node));
}

TEST_F(TreePrinterSmokeTest, print_create_table) {
    auto type_len = std::make_shared<ast::TypeLen>(ast::SV_TYPE_INT, 4);
    auto col_def = std::make_shared<ast::ColDef>("id", type_len);
    std::vector<std::shared_ptr<ast::Field>> fields = {col_def};
    auto node = std::make_shared<ast::CreateTable>("t", fields);
    EXPECT_NO_THROW(ast::TreePrinter::print(node));
}

TEST_F(TreePrinterSmokeTest, print_binary_expr) {
    auto lhs = std::make_shared<ast::Col>("t", "x");
    auto rhs = std::make_shared<ast::IntLit>(10);
    auto node = std::make_shared<ast::BinaryExpr>(lhs, ast::SV_OP_EQ, rhs);
    EXPECT_NO_THROW(ast::TreePrinter::print(node));
}
