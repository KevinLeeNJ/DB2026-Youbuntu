/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO ANY WARRANTY OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <gtest/gtest.h>

#include "cache/statement_template_cache.h"
#include "parser/parser.h"
#include "parser/token_stream.h"

TEST(StatementTemplateTest, NormalizesLiteralValuesButOwnsIdentifiersAndStrings) {
    auto first = parser::normalize_sql("select id from users where id = 7 and name = 'alice';");
    auto second = parser::normalize_sql(" SELECT id FROM users WHERE id = 99 AND name = 'bob' ; ");
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first.key, second.key);
    ASSERT_EQ(first.parameters.size(), 2U);
    EXPECT_EQ(first.parameters[0].type, parser::TokenType::VALUE_INT);
    EXPECT_EQ(first.parameters[1].type, parser::TokenType::VALUE_STRING);
    EXPECT_EQ(first.parameters[1].text, "alice");
    EXPECT_EQ(first.tokens[1].text, "id");
    EXPECT_EQ(first.tokens[3].text, "users");
}

TEST(StatementTemplateTest, IdentifierAndMalformedShapeDoNotAlias) {
    auto users = parser::normalize_sql("select id from users where id = 1;");
    auto orders = parser::normalize_sql("select id from orders where id = 1;");
    ASSERT_TRUE(users);
    ASSERT_TRUE(orders);
    EXPECT_NE(users.key, orders.key);
    auto malformed = parser::normalize_sql("select /* unterminated");
    EXPECT_FALSE(malformed);
    EXPECT_FALSE(malformed.error.empty());
}

TEST(StatementTemplateTest, LightweightNormalizationKeepsBindingDataWithoutOwnedTokens) {
    auto shape = parser::normalize_sql("select id from users where id = 7 limit 1;", false);
    ASSERT_TRUE(shape);
    EXPECT_TRUE(shape.tokens.empty());
    ASSERT_EQ(shape.parameters.size(), 2U);
    EXPECT_EQ(shape.parameters[0].int_value, 7);
    EXPECT_EQ(shape.parameters[1].int_value, 1);
    EXPECT_TRUE(shape.template_unsupported);
}

TEST(StatementTemplateTest, CacheChecksGenerationAndEvictsByCapacity) {
    auto shape = parser::normalize_sql("select id from users where id = 1;");
    ASSERT_TRUE(shape);
    cache::StatementTemplateCache cache(1);
    EXPECT_FALSE(cache.lookup(shape.key, 3));
    cache.publish(shape.key, 3);
    EXPECT_TRUE(cache.lookup(shape.key, 3));
    EXPECT_FALSE(cache.lookup(shape.key, 4));
    auto second = parser::normalize_sql("select id from orders where id = 2;");
    cache.publish(second.key, 3);
    EXPECT_FALSE(cache.lookup(shape.key, 3));
    EXPECT_EQ(cache.stats().evictions, 1U);
}

TEST(StatementTemplateTest, ClonesStatementSkeletonWithFreshOwnership) {
    auto parsed = ast::parse_sql("update users set score = score + 1 where id = 7;");
    ASSERT_NE(parsed, nullptr);
    auto clone = ast::clone_tree(*parsed);
    ASSERT_NE(clone, nullptr);
    ASSERT_EQ(clone->type, ast::AstType::UpdateStmt);
    auto* original_update = static_cast<ast::UpdateStmt*>(parsed.get());
    auto* clone_update = static_cast<ast::UpdateStmt*>(clone.get());
    ASSERT_NE(original_update->set_clauses[0]->val, clone_update->set_clauses[0]->val);
    original_update->tab_name = "changed";
    EXPECT_EQ(clone_update->tab_name, "users");
}

TEST(StatementTemplateTest, BindsCurrentLiteralValuesOnParameterizedAstHit) {
    auto first_shape = parser::normalize_sql("update users set score = 7 where id = 1;");
    auto second_shape = parser::normalize_sql("update users set score = 99 where id = 42;");
    ASSERT_TRUE(first_shape);
    ASSERT_TRUE(second_shape);
    ASSERT_EQ(first_shape.key, second_shape.key);
    auto first_ast = ast::parse_sql("update users set score = 7 where id = 1;");
    cache::StatementTemplateCache cache;
    cache.publish(first_shape.key, 1, std::shared_ptr<const ast::TreeNode>(std::move(first_ast)));
    auto rebound = cache.lookup_ast(second_shape.key, 1, &second_shape);
    ASSERT_NE(rebound, nullptr);
    auto* update = static_cast<ast::UpdateStmt*>(rebound.get());
    EXPECT_EQ(static_cast<ast::IntLit*>(update->set_clauses[0]->val.get())->val, 99);
    EXPECT_EQ(static_cast<ast::IntLit*>(update->conds[0]->rhs.get())->val, 42);
}

TEST(StatementTemplateTest, FullLookupReturnsStatementTypeAndFreshPlanWithOneCacheProbe) {
    auto shape = parser::normalize_sql("begin;");
    ASSERT_TRUE(shape);
    auto parsed = ast::parse_sql("begin;");
    auto plan = std::make_unique<OtherPlan>(T_Transaction_begin, "");
    SmManager sm_manager{nullptr, nullptr, nullptr, nullptr};

    cache::StatementTemplateCache cache;
    cache.publish(shape.key, 1, std::shared_ptr<const ast::TreeNode>(std::move(parsed)), nullptr,
                  std::shared_ptr<const Plan>(std::move(plan)));

    auto first = cache.lookup_full(shape.key, 1, &sm_manager, &shape);
    auto second = cache.lookup_full(shape.key, 1, &sm_manager, &shape);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first.statement_type, ast::AstType::TxnBegin);
    EXPECT_EQ(first.plan->tag, T_Transaction_begin);
    EXPECT_EQ(second.statement_type, ast::AstType::TxnBegin);
    EXPECT_NE(first.plan.get(), second.plan.get());
    EXPECT_EQ(cache.stats().lookups, 2U);
    EXPECT_EQ(cache.stats().hits, 2U);
}

TEST(StatementTemplateTest, FullLookupBindsCurrentInsertValues) {
    auto first_shape = parser::normalize_sql("insert into users values (1, 'alice');");
    auto second_shape = parser::normalize_sql("insert into users values (42, 'bob');");
    ASSERT_TRUE(first_shape);
    ASSERT_TRUE(second_shape);
    ASSERT_EQ(first_shape.key, second_shape.key);

    auto parsed = ast::parse_sql("insert into users values (1, 'alice');");
    ast::assign_literal_slots(*parsed);
    Value id;
    id.set_int(1);
    id.lexical_slot = 0;
    Value name;
    name.set_str("alice");
    name.lexical_slot = 1;
    auto plan = std::make_unique<DMLPlan>(T_Insert, nullptr, "users", std::vector<Value>{id, name},
                                          std::vector<Condition>{}, std::vector<SetClause>{});
    SmManager sm_manager{nullptr, nullptr, nullptr, nullptr};

    cache::StatementTemplateCache cache;
    cache.publish(first_shape.key, 1, std::shared_ptr<const ast::TreeNode>(std::move(parsed)), nullptr,
                  std::shared_ptr<const Plan>(std::move(plan)));
    auto rebound = cache.lookup_full(second_shape.key, 1, &sm_manager, &second_shape);
    ASSERT_TRUE(rebound);
    const auto& insert = static_cast<const DMLPlan&>(*rebound.plan);
    ASSERT_EQ(insert.values_.size(), 2U);
    EXPECT_EQ(insert.values_[0].int_val, 42);
    EXPECT_EQ(insert.values_[1].str_val, "bob");
}
