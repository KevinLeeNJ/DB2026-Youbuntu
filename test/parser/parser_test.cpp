/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

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

TEST(ParserTest, ParserAndLexerErrorsUsePublicType) {
    EXPECT_THROW((void)ast::parse_sql("select from;"), ast::ParseError);
    EXPECT_THROW((void)ast::parse_sql("select @;"), ast::ParseError);
}

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

TEST(ParserTest, ParsesSelfReferentialUpdateSetClauses) {
    auto parsed = parse_ok("update score_tab set score = score + 5, bonus = score_tab.bonus - 0.5 where id < 3;");
    auto update = as_node<ast::UpdateStmt>(parsed);
    ASSERT_NE(update, nullptr);
    ASSERT_EQ(update->set_clauses.size(), 2);

    const auto* score_clause = update->set_clauses[0].get();
    EXPECT_TRUE(score_clause->is_self_ref);
    EXPECT_EQ(score_clause->op, ast::SetOp::SELF_ADD);
    EXPECT_EQ(score_clause->col_name, "score");
    ASSERT_NE(score_clause->rhs_col, nullptr);
    EXPECT_EQ(score_clause->rhs_col->tab_name, "");
    EXPECT_EQ(score_clause->rhs_col->col_name, "score");
    auto score_delta = dynamic_cast<const ast::IntLit*>(score_clause->val.get());
    ASSERT_NE(score_delta, nullptr);
    EXPECT_EQ(score_delta->val, 5);

    const auto* bonus_clause = update->set_clauses[1].get();
    EXPECT_TRUE(bonus_clause->is_self_ref);
    EXPECT_EQ(bonus_clause->op, ast::SetOp::SELF_SUB);
    EXPECT_EQ(bonus_clause->col_name, "bonus");
    ASSERT_NE(bonus_clause->rhs_col, nullptr);
    EXPECT_EQ(bonus_clause->rhs_col->tab_name, "score_tab");
    EXPECT_EQ(bonus_clause->rhs_col->col_name, "bonus");
    auto bonus_delta = dynamic_cast<const ast::FloatLit*>(bonus_clause->val.get());
    ASSERT_NE(bonus_delta, nullptr);
    EXPECT_DOUBLE_EQ(bonus_delta->val, 0.5);
}

TEST(ParserTest, ParsesCompoundAssignmentUpdateSetClauses) {
    // col += num / col -= num 在解析期脱糖为 col = col ± num 的自引用节点
    auto parsed = parse_ok("update score_tab set score += 5, bonus -= 0.5 where id < 3;");
    auto update = as_node<ast::UpdateStmt>(parsed);
    ASSERT_NE(update, nullptr);
    ASSERT_EQ(update->set_clauses.size(), 2);

    // score += 5  脱糖为 score = score + 5
    const auto* score_clause = update->set_clauses[0].get();
    EXPECT_TRUE(score_clause->is_self_ref);
    EXPECT_EQ(score_clause->op, ast::SetOp::SELF_ADD);
    EXPECT_EQ(score_clause->col_name, "score");
    ASSERT_NE(score_clause->rhs_col, nullptr);
    EXPECT_EQ(score_clause->rhs_col->tab_name, "");
    EXPECT_EQ(score_clause->rhs_col->col_name, "score");
    auto score_delta = dynamic_cast<const ast::IntLit*>(score_clause->val.get());
    ASSERT_NE(score_delta, nullptr);
    EXPECT_EQ(score_delta->val, 5);

    // bonus -= 0.5  脱糖为 bonus = bonus - 0.5
    const auto* bonus_clause = update->set_clauses[1].get();
    EXPECT_TRUE(bonus_clause->is_self_ref);
    EXPECT_EQ(bonus_clause->op, ast::SetOp::SELF_SUB);
    EXPECT_EQ(bonus_clause->col_name, "bonus");
    ASSERT_NE(bonus_clause->rhs_col, nullptr);
    EXPECT_EQ(bonus_clause->rhs_col->tab_name, "");
    EXPECT_EQ(bonus_clause->rhs_col->col_name, "bonus");
    auto bonus_delta = dynamic_cast<const ast::FloatLit*>(bonus_clause->val.get());
    ASSERT_NE(bonus_delta, nullptr);
    EXPECT_DOUBLE_EQ(bonus_delta->val, 0.5);
}

TEST(ParserTest, FoldsConstantArithmeticExpressions) {
    // 括号常量算术:折叠成 IntLit,display_text 保留原始表达式文本
    {
        auto parsed = parse_ok("select * from t where id >= (100-20);");
        auto sel = as_node<ast::SelectStmt>(parsed);
        ASSERT_FALSE(sel->conds.empty());
        auto lit = dynamic_cast<const ast::IntLit*>(sel->conds.front()->rhs.get());
        ASSERT_NE(lit, nullptr);
        EXPECT_EQ(lit->val, 80);
        EXPECT_EQ(lit->display_text, "(100-20)");
    }
    // 嵌套括号
    {
        auto parsed = parse_ok("select * from t where id >= ((10+5)*2);");
        auto sel = as_node<ast::SelectStmt>(parsed);
        auto lit = dynamic_cast<const ast::IntLit*>(sel->conds.front()->rhs.get());
        ASSERT_NE(lit, nullptr);
        EXPECT_EQ(lit->val, 30);
        EXPECT_EQ(lit->display_text, "((10+5)*2)");
    }
    // 无括号算术(保留运算符两侧空格)
    {
        auto parsed = parse_ok("select * from t where id >= 100 - 20;");
        auto sel = as_node<ast::SelectStmt>(parsed);
        auto lit = dynamic_cast<const ast::IntLit*>(sel->conds.front()->rhs.get());
        ASSERT_NE(lit, nullptr);
        EXPECT_EQ(lit->val, 80);
        EXPECT_EQ(lit->display_text, "100 - 20");
    }
    // int 与 float 混合提升为 float
    {
        auto parsed = parse_ok("select * from t where x > 5.0 + 3;");
        auto sel = as_node<ast::SelectStmt>(parsed);
        auto lit = dynamic_cast<const ast::FloatLit*>(sel->conds.front()->rhs.get());
        ASSERT_NE(lit, nullptr);
        EXPECT_DOUBLE_EQ(lit->val, 8.0);
    }
    // 括号外接算术
    {
        auto parsed = parse_ok("select * from t where id >= (3-1)*2;");
        auto sel = as_node<ast::SelectStmt>(parsed);
        auto lit = dynamic_cast<const ast::IntLit*>(sel->conds.front()->rhs.get());
        ASSERT_NE(lit, nullptr);
        EXPECT_EQ(lit->val, 4);
        EXPECT_EQ(lit->display_text, "(3-1)*2");
    }
    // 纯常量、字符串不受影响(回归)
    {
        auto parsed = parse_ok("select * from t where id >= 5;");
        auto sel = as_node<ast::SelectStmt>(parsed);
        auto lit = dynamic_cast<const ast::IntLit*>(sel->conds.front()->rhs.get());
        ASSERT_NE(lit, nullptr);
        EXPECT_EQ(lit->val, 5);
    }
    {
        auto parsed = parse_ok("select * from t where x = 'abc';");
        auto sel = as_node<ast::SelectStmt>(parsed);
        EXPECT_EQ(sel->conds.front()->rhs->type, ast::AstType::StringLit);
    }
    // 除零、溢出、非数值、列参与算术 均应解析失败
    expect_parse_error("select * from t where id >= (100/0);");
    expect_parse_error("select * from t where id >= (2000000000+2000000000);");
    expect_parse_error("select * from t where id >= ('a'-1);");
    expect_parse_error("select * from t where id >= (col-20);");
}

TEST(ParserTest, ParsesRushdbCompatibleUpdateSetOperators) {
    auto parsed =
        parse_ok("update score_tab set score = score * 2, ratio = score_tab.ratio / 4, untouched = untouched where "
                 "id < 3;");
    auto update = as_node<ast::UpdateStmt>(parsed);
    ASSERT_NE(update, nullptr);
    ASSERT_EQ(update->set_clauses.size(), 3);

    const auto* mul_clause = update->set_clauses[0].get();
    EXPECT_TRUE(mul_clause->is_self_ref);
    EXPECT_EQ(mul_clause->op, ast::SetOp::SELF_MUL);
    EXPECT_EQ(mul_clause->col_name, "score");
    ASSERT_NE(mul_clause->rhs_col, nullptr);
    EXPECT_EQ(mul_clause->rhs_col->col_name, "score");
    auto mul_value = dynamic_cast<const ast::IntLit*>(mul_clause->val.get());
    ASSERT_NE(mul_value, nullptr);
    EXPECT_EQ(mul_value->val, 2);

    const auto* div_clause = update->set_clauses[1].get();
    EXPECT_TRUE(div_clause->is_self_ref);
    EXPECT_EQ(div_clause->op, ast::SetOp::SELF_DIV);
    EXPECT_EQ(div_clause->col_name, "ratio");
    ASSERT_NE(div_clause->rhs_col, nullptr);
    EXPECT_EQ(div_clause->rhs_col->tab_name, "score_tab");
    EXPECT_EQ(div_clause->rhs_col->col_name, "ratio");
    auto div_value = dynamic_cast<const ast::IntLit*>(div_clause->val.get());
    ASSERT_NE(div_value, nullptr);
    EXPECT_EQ(div_value->val, 4);

    const auto* bare_clause = update->set_clauses[2].get();
    EXPECT_TRUE(bare_clause->is_self_ref);
    EXPECT_EQ(bare_clause->op, ast::SetOp::ASSIGNMENT);
    EXPECT_EQ(bare_clause->col_name, "untouched");
    ASSERT_NE(bare_clause->rhs_col, nullptr);
    EXPECT_EQ(bare_clause->rhs_col->col_name, "untouched");
    EXPECT_EQ(bare_clause->val, nullptr);
}

TEST(ParserTest, ParsesDatetimeTypeAsFixedLengthColumn) {
    auto parsed = parse_ok("create table orders (id int, created_at datetime);");
    auto create = as_node<ast::CreateTable>(parsed);
    ASSERT_NE(create, nullptr);
    ASSERT_EQ(create->fields.size(), 2);
    auto col = dynamic_cast<const ast::ColDef*>(create->fields[1].get());
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->col_name, "created_at");
    EXPECT_EQ(col->type_len->type, ast::SV_TYPE_DATETIME);
    EXPECT_EQ(col->type_len->len, 19);
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
    expect_parse_error("update tb set a = b where x = 1;");
    EXPECT_EQ(parse_ok("update tb set a = a where x = 1;")->type, ast::AstType::UpdateStmt);
    expect_parse_error("update tb set a = b * 'bad' where x = 1;");
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

TEST(ParserTest, ParsesKeywordExpansionForms) {
    auto distinct_parse = parse_ok("select distinct name from people;");
    auto distinct = as_node<ast::SelectStmt>(distinct_parse);
    ASSERT_NE(distinct, nullptr);
    EXPECT_TRUE(distinct->has_distinct);

    auto like_parse = parse_ok("select name from people where name not like 'A%';");
    auto like = as_node<ast::SelectStmt>(like_parse);
    ASSERT_NE(like, nullptr);
    ASSERT_EQ(like->conds.size(), 1);
    EXPECT_EQ(like->conds[0]->op, ast::SV_OP_LIKE);
    EXPECT_TRUE(like->conds[0]->negated);

    auto in_parse = parse_ok("select id from people where id not in (2, 4);");
    auto in = as_node<ast::SelectStmt>(in_parse);
    ASSERT_NE(in, nullptr);
    ASSERT_EQ(in->conds.size(), 1);
    EXPECT_EQ(in->conds[0]->op, ast::SV_OP_IN);
    EXPECT_TRUE(in->conds[0]->negated);
    EXPECT_EQ(in->conds[0]->rhs_list.size(), 2);

    auto between_parse = parse_ok("select id from people where id between 2 and 4;");
    auto between = as_node<ast::SelectStmt>(between_parse);
    ASSERT_NE(between, nullptr);
    ASSERT_EQ(between->conds.size(), 1);
    EXPECT_EQ(between->conds[0]->op, ast::SV_OP_BETWEEN);
    ASSERT_NE(between->conds[0]->rhs_upper, nullptr);

    auto having_parse = parse_ok("select count(*) from people group by id having count(*) not between 1 and 2;");
    auto having = as_node<ast::SelectStmt>(having_parse);
    ASSERT_NE(having, nullptr);
    ASSERT_EQ(having->having_conds.size(), 1);
    EXPECT_EQ(having->having_conds[0]->op, ast::SV_OP_BETWEEN);
    EXPECT_TRUE(having->having_conds[0]->negated);

    auto right_join_parse = parse_ok("select * from left_t right join right_t on left_t.id = right_t.id;");
    auto right_join = as_node<ast::SelectStmt>(right_join_parse);
    ASSERT_NE(right_join, nullptr);
    ASSERT_EQ(right_join->jointree.size(), 1);
    EXPECT_EQ(right_join->jointree[0]->join_type, RIGHT_JOIN);

    auto full_join_parse = parse_ok("select * from left_t full outer join right_t on left_t.id = right_t.id;");
    auto full_join = as_node<ast::SelectStmt>(full_join_parse);
    ASSERT_NE(full_join, nullptr);
    ASSERT_EQ(full_join->jointree.size(), 1);
    EXPECT_EQ(full_join->jointree[0]->join_type, FULL_JOIN);

    auto union_wrapper_parse = parse_ok("select * from (select id from left_t union all select id from right_t) as u;");
    auto union_wrapper = as_node<ast::SelectFromUnionStmt>(union_wrapper_parse);
    ASSERT_NE(union_wrapper, nullptr);
    ASSERT_NE(union_wrapper->union_stmt, nullptr);
    ASSERT_EQ(union_wrapper->union_stmt->union_all.size(), 1);
    EXPECT_TRUE(union_wrapper->union_stmt->union_all[0]);

    auto pagination_parse = parse_ok("select id from people order by id offset 1 limit 2;");
    auto pagination = as_node<ast::SelectStmt>(pagination_parse);
    ASSERT_NE(pagination, nullptr);
    EXPECT_TRUE(pagination->has_offset);
    EXPECT_EQ(pagination->offset, 1);
    EXPECT_TRUE(pagination->has_limit);
    EXPECT_EQ(pagination->limit, 2);

    expect_parse_error("select id from people where id in (); ");
    expect_parse_error("select id from people where id between 1; ");
    expect_parse_error("select id from people where id like; ");
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
    EXPECT_DOUBLE_EQ(float_lit->val, -2.5);
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

TEST(ParserTest, ParsesSetOutputFile) {
    auto off = parse_ok("set output_file off");
    auto off_node = as_node<ast::SetOutputFile>(off);
    ASSERT_NE(off_node, nullptr);
    EXPECT_FALSE(off_node->enable_);

    auto on = parse_ok("set output_file on");
    auto on_node = as_node<ast::SetOutputFile>(on);
    ASSERT_NE(on_node, nullptr);
    EXPECT_TRUE(on_node->enable_);

    // trailing semicolon is tolerated (optional)
    auto on_semi = parse_ok("set output_file on;");
    EXPECT_EQ(on_semi->type, ast::AstType::SetOutputFile);

    // normal SET statements still require a semicolon
    EXPECT_NO_THROW((void)parse_ok("set enable_nestloop = true;"));
    EXPECT_NO_THROW((void)parse_ok("set transaction isolation level serializable;"));
}

TEST(ParserTest, ParsesLoadStmt) {
    auto node = parse_ok("load ../../src/test/performance_test/table_data/warehouse.csv into warehouse;");
    auto load = as_node<ast::LoadStmt>(node);
    ASSERT_NE(load, nullptr);
    EXPECT_EQ(load->file_name_, "../../src/test/performance_test/table_data/warehouse.csv");
    EXPECT_EQ(load->tab_name_, "warehouse");

    auto node2 = parse_ok("load ./x.csv into t;");
    auto load2 = as_node<ast::LoadStmt>(node2);
    ASSERT_NE(load2, nullptr);
    EXPECT_EQ(load2->file_name_, "./x.csv");
    EXPECT_EQ(load2->tab_name_, "t");
}
