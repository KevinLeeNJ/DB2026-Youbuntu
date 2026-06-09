#include "parser/parser.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

template <typename T> const T* as_node(const std::unique_ptr<ast::TreeNode>& node) {
    auto typed = dynamic_cast<const T*>(node.get());
    EXPECT_NE(typed, nullptr);
    return typed;
}

std::unique_ptr<ast::TreeNode> parse_ok(const std::string& sql) {
    auto node = ast::parse_sql(sql);
    EXPECT_NE(node, nullptr) << sql;
    return node;
}

void expect_parse_error(const std::string& sql) {
    EXPECT_THROW((void)ast::parse_sql(sql), std::runtime_error) << sql;
}

} // namespace

TEST(ParserTest, ParsesUtilityStatements) {
    EXPECT_EQ(ast::parse_sql("").get(), nullptr);
    EXPECT_EQ(ast::parse_sql("exit;").get(), nullptr);

    auto help = parse_ok("help;");
    EXPECT_EQ(help->type, ast::AstType::Help);

    auto show_tables = parse_ok("show tables;");
    EXPECT_EQ(show_tables->type, ast::AstType::ShowTables);

    auto show_index = parse_ok("show index from tb;");
    auto show_index_node = as_node<ast::ShowIndex>(show_index);
    ASSERT_NE(show_index_node, nullptr);
    EXPECT_EQ(show_index_node->tab_name, "tb");
}

TEST(ParserTest, ParsesDdlAndDmlStatements) {
    EXPECT_EQ(parse_ok("desc tb;")->type, ast::AstType::DescTable);
    EXPECT_EQ(parse_ok("create table tb (a int, b float, c char(4));")->type, ast::AstType::CreateTable);
    EXPECT_EQ(parse_ok("drop table tb;")->type, ast::AstType::DropTable);
    EXPECT_EQ(parse_ok("create index tb(a, b, c);")->type, ast::AstType::CreateIndex);
    EXPECT_EQ(parse_ok("drop index tb(b);")->type, ast::AstType::DropIndex);
    EXPECT_EQ(parse_ok("insert into tb values (1, 3.14, 'pi');")->type, ast::AstType::InsertStmt);
    EXPECT_EQ(parse_ok("delete from tb where a = 1;")->type, ast::AstType::DeleteStmt);
    EXPECT_EQ(parse_ok("update tb set a = 1, b = 2.2, c = 'xyz' where x = 2 and y < 1.1;")->type,
              ast::AstType::UpdateStmt);
}

TEST(ParserTest, ParsesSelectFeaturesUsedByCompetition) {
    auto select_star = parse_ok("select * from tb;");
    auto select_star_node = as_node<ast::SelectStmt>(select_star);
    ASSERT_NE(select_star_node, nullptr);
    EXPECT_TRUE(select_star_node->has_select_star);
    EXPECT_EQ(select_star_node->tabs.size(), 1);
    EXPECT_EQ(select_star_node->tabs[0].table_name, "tb");

    auto select_join = parse_ok("select x.a, y.b from x join y where x.a = y.b and c = d;");
    auto select_join_node = as_node<ast::SelectStmt>(select_join);
    ASSERT_NE(select_join_node, nullptr);
    EXPECT_FALSE(select_join_node->has_select_star);
    EXPECT_EQ(select_join_node->tabs.size(), 2);
    EXPECT_EQ(select_join_node->conds.size(), 2);
    ASSERT_EQ(select_join_node->jointree.size(), 1);
    EXPECT_EQ(select_join_node->jointree[0]->type, ast::AstType::JoinExpr);
    EXPECT_EQ(select_join_node->jointree[0]->left.table_name, "x");
    EXPECT_EQ(select_join_node->jointree[0]->right.table_name, "y");

    auto aggregate = parse_ok(
        "select count(*), sum(a) as total from tb group by b having count(*) > 1 order by total desc limit 10;");
    auto aggregate_node = as_node<ast::SelectStmt>(aggregate);
    ASSERT_NE(aggregate_node, nullptr);
    EXPECT_EQ(aggregate_node->select_items.size(), 2);
    EXPECT_EQ(aggregate_node->group_by_cols.size(), 1);
    EXPECT_EQ(aggregate_node->having_conds.size(), 1);
    EXPECT_EQ(aggregate_node->having_conds[0]->type, ast::AstType::HavingExpr);
    EXPECT_EQ(aggregate_node->order_by_items.size(), 1);
    ASSERT_NE(aggregate_node->order, nullptr);
    EXPECT_EQ(aggregate_node->order->type, ast::AstType::OrderBy);
    EXPECT_EQ(aggregate_node->order->items.size(), 1);
    EXPECT_TRUE(aggregate_node->has_limit);
    EXPECT_EQ(aggregate_node->limit, 10);
}

TEST(ParserTest, ParsesUnionWrapperAndExplainAnalyze) {
    auto union_wrapper = parse_ok("select * from (select a from t1 union select a from t2) as u order by a;");
    auto union_node = as_node<ast::SelectFromUnionStmt>(union_wrapper);
    ASSERT_NE(union_node, nullptr);
    EXPECT_EQ(union_node->alias, "u");
    ASSERT_NE(union_node->union_stmt, nullptr);
    EXPECT_EQ(union_node->union_stmt->branches.size(), 2);
    EXPECT_EQ(union_node->order_by_items.size(), 1);
    ASSERT_NE(union_node->order, nullptr);
    EXPECT_EQ(union_node->order->items.size(), 1);

    auto explain = parse_ok("explain analyze select * from tb;");
    EXPECT_EQ(explain->type, ast::AstType::ExplainAnalyze);
}

TEST(ParserTest, ParsesKnobsAndTransactionStatements) {
    EXPECT_EQ(parse_ok("set enable_nestloop = true;")->type, ast::AstType::SetStmt);
    EXPECT_EQ(parse_ok("set transaction isolation level snapshot isolation;")->type, ast::AstType::SetTransaction);
    EXPECT_EQ(parse_ok("set transaction isolation level serializable;")->type, ast::AstType::SetTransaction);
    EXPECT_EQ(parse_ok("begin;")->type, ast::AstType::TxnBegin);
    EXPECT_EQ(parse_ok("commit;")->type, ast::AstType::TxnCommit);
    EXPECT_EQ(parse_ok("abort;")->type, ast::AstType::TxnAbort);
    EXPECT_EQ(parse_ok("rollback;")->type, ast::AstType::TxnRollback);
    EXPECT_EQ(parse_ok("create static_checkpoint;")->type, ast::AstType::StaticCheckpoint);
}

TEST(ParserTest, RejectsMalformedStatements) {
    expect_parse_error("select * from tb");
    expect_parse_error("select from tb;");
    expect_parse_error("insert into tb values (1, );");
    expect_parse_error("set enable_nestloop = maybe;");
}

TEST(ParserTest, SelectStmtUsesSelectItemsAsSingleProjectionContract) {
    auto parsed = parse_ok("select a, sum(b) as total from tb group by a order by total desc limit 5;");
    auto select = as_node<ast::SelectStmt>(parsed);
    ASSERT_NE(select, nullptr);

    EXPECT_FALSE(select->has_select_star);
    ASSERT_EQ(select->select_items.size(), 2);
    EXPECT_EQ(select->select_items[0]->expr->type, ast::AstType::Col);
    EXPECT_EQ(select->select_items[1]->expr->type, ast::AstType::AggExpr);
    EXPECT_EQ(select->select_items[1]->alias, "total");
    EXPECT_EQ(select->order_by_items.size(), 1);
    EXPECT_TRUE(select->has_limit);
    EXPECT_EQ(select->limit, 5);
}

TEST(ParserTest, UsesBinaryExprForWhereAndHavingExprForHavingConditions) {
    auto where_parsed = parse_ok("select * from tb where a = 1 and b = c;");
    auto where_select = as_node<ast::SelectStmt>(where_parsed);
    ASSERT_NE(where_select, nullptr);
    ASSERT_EQ(where_select->conds.size(), 2);
    EXPECT_EQ(where_select->conds[0]->type, ast::AstType::BinaryExpr);
    EXPECT_EQ(where_select->conds[0]->lhs->type, ast::AstType::Col);
    EXPECT_EQ(where_select->conds[0]->rhs->type, ast::AstType::IntLit);

    auto having_parsed = parse_ok("select count(*) from tb group by a having count(*) > 1;");
    auto having_select = as_node<ast::SelectStmt>(having_parsed);
    ASSERT_NE(having_select, nullptr);
    ASSERT_EQ(having_select->having_conds.size(), 1);
    EXPECT_EQ(having_select->having_conds[0]->type, ast::AstType::HavingExpr);
    EXPECT_EQ(having_select->having_conds[0]->lhs->type, ast::AstType::AggExpr);
    EXPECT_EQ(having_select->having_conds[0]->rhs->type, ast::AstType::IntLit);
}

TEST(ParserTest, SelectRoutingKeepsUnionWrapperBehavior) {
    EXPECT_EQ(parse_ok("select * from tb;")->type, ast::AstType::SelectStmt);
    EXPECT_EQ(parse_ok("select a from tb;")->type, ast::AstType::SelectStmt);
    EXPECT_EQ(parse_ok("select * from (select a from t1 union select a from t2) as u;")->type,
              ast::AstType::SelectFromUnionStmt);
    EXPECT_EQ(parse_ok("explain analyze select a from tb;")->type, ast::AstType::ExplainAnalyze);
}

TEST(ParserTest, ParsesOptionalWhereAndNegativeLiterals) {
    auto no_where = parse_ok("delete from tb;");
    auto delete_stmt = as_node<ast::DeleteStmt>(no_where);
    ASSERT_NE(delete_stmt, nullptr);
    EXPECT_TRUE(delete_stmt->conds.empty());

    auto negative_insert = parse_ok("insert into tb values (-1, -2.5, true);");
    auto insert_stmt = as_node<ast::InsertStmt>(negative_insert);
    ASSERT_NE(insert_stmt, nullptr);
    ASSERT_EQ(insert_stmt->vals.size(), 3);
    auto int_lit = dynamic_cast<const ast::IntLit*>(insert_stmt->vals[0].get());
    auto float_lit = dynamic_cast<const ast::FloatLit*>(insert_stmt->vals[1].get());
    ASSERT_NE(int_lit, nullptr);
    ASSERT_NE(float_lit, nullptr);
    EXPECT_EQ(int_lit->val, -1);
    EXPECT_FLOAT_EQ(float_lit->val, -2.5F);
}

TEST(ParserTest, RejectsUnterminatedBlockComment) {
    expect_parse_error("select * from tb /* missing close ;");
}

TEST(ParserTest, ParsesIntMinBoundary) {
    auto parsed = parse_ok("insert into tb values (-2147483648);");
    auto insert_stmt = as_node<ast::InsertStmt>(parsed);
    ASSERT_NE(insert_stmt, nullptr);
    ASSERT_EQ(insert_stmt->vals.size(), 1);
    auto int_lit = dynamic_cast<const ast::IntLit*>(insert_stmt->vals[0].get());
    ASSERT_NE(int_lit, nullptr);
    EXPECT_EQ(int_lit->val, std::numeric_limits<int>::min());
}

TEST(ParserTest, RejectsIntegerOverflow) {
    expect_parse_error("insert into tb values (2147483648);");
    expect_parse_error("insert into tb values (-2147483649);");
}
