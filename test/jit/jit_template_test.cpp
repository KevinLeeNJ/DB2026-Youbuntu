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
