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
#include "execution/prepared_select_descriptor.h"
#include "parser/parser.h"
#include "parser/token_stream.h"
#include "record/rm_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

namespace {

class StatementTemplateDatabaseTest : public ::testing::Test {
protected:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<RmManager> rm_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::unique_ptr<SmManager> sm_manager;
    std::string db_name;

    void SetUp() override {
        db_name = "statement_template_db_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
        rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
        sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(),
                                                 ix_manager.get());
        if (sm_manager->is_dir(db_name)) {
            sm_manager->drop_db(db_name);
        }
        sm_manager->create_db(db_name);
        sm_manager->open_db(db_name);
        sm_manager->create_table("users", {{"id", TYPE_INT, 4}}, nullptr);
        sm_manager->create_index("users", {"id"}, nullptr);
    }

    void TearDown() override {
        sm_manager->close_db();
        sm_manager->drop_db(db_name);
    }

    std::unique_ptr<Plan> select_plan(PlanTag scan_tag) {
        Condition condition;
        condition.lhs_col = {"users", "id"};
        condition.op = OP_EQ;
        condition.is_rhs_val = true;
        condition.rhs_val.set_int(1);
        condition.rhs_val.lexical_slot = 0;
        auto scan = std::make_unique<ScanPlan>(scan_tag, sm_manager.get(), "users", std::vector<Condition>{condition},
                                               std::vector<std::string>{"id"});
        SelectItem item;
        item.expr.type = QueryExprType::COLUMN;
        item.expr.col = {"users", "id"};
        auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(scan), std::vector<SelectItem>{item},
                                                           std::vector<std::string>{"id"}, true);
        return std::make_unique<DMLPlan>(T_select, std::move(projection), "users", std::vector<Value>{},
                                         std::vector<Condition>{}, std::vector<SetClause>{});
    }
};

} // namespace

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

TEST(StatementTemplateTest, StreamingShapeDigestIsDeterministicAndTracksCanonicalSize) {
    const std::string long_identifier(4096, 'x');
    const std::string sql = "select " + long_identifier + " from users where id = 1;";
    auto first = parser::normalize_sql(sql, false);
    auto second = parser::normalize_sql(sql, false);
    auto changed = parser::normalize_sql("select " + long_identifier + "y from users where id = 1;", false);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    ASSERT_TRUE(changed);
    EXPECT_EQ(first.key, second.key);
    EXPECT_GT(first.key.canonical_size, long_identifier.size());
    EXPECT_NE(first.key, changed.key);
    EXPECT_EQ(first.key.canonical_size + 1, changed.key.canonical_size);
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

TEST(StatementTemplateTest, FullLookupSharesImmutablePlanWithRequestLocalState) {
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
    EXPECT_EQ(first.plan.get(), second.plan.get());
    EXPECT_NE(first.plan.literals.get(), second.plan.literals.get());
    EXPECT_NE(first.plan.runtime.get(), second.plan.runtime.get());
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
    ASSERT_NE(rebound.plan.literals->Find(insert.values_[0].lexical_slot), nullptr);
    ASSERT_NE(rebound.plan.literals->Find(insert.values_[1].lexical_slot), nullptr);
    EXPECT_EQ(rebound.plan.literals->Find(insert.values_[0].lexical_slot)->int_val, 42);
    EXPECT_EQ(rebound.plan.literals->Find(insert.values_[1].lexical_slot)->str_val, "bob");
    EXPECT_EQ(insert.values_[0].int_val, 1);
    EXPECT_EQ(insert.values_[1].str_val, "alice");
}

TEST(StatementTemplateTest, ConcurrentDmlLookupsKeepLiteralOverlaysIsolated) {
    struct Case {
        std::string first_sql;
        std::string second_sql;
        std::unique_ptr<Plan> plan;
        std::vector<int> slots;
    };
    Value insert_id;
    insert_id.set_int(1);
    insert_id.lexical_slot = 0;
    Condition delete_condition;
    delete_condition.lhs_col = {"users", "id"};
    delete_condition.op = OP_EQ;
    delete_condition.is_rhs_val = true;
    delete_condition.rhs_val.set_int(1);
    delete_condition.rhs_val.lexical_slot = 0;
    SetClause self_reference;
    self_reference.lhs = {"users", "score"};
    self_reference.is_self_ref = true;
    self_reference.rhs_col = {"users", "score"};
    self_reference.op = UpdateOp::SELF_ADD;
    self_reference.rhs.set_int(1);
    self_reference.rhs.lexical_slot = 0;
    Condition update_condition = delete_condition;
    update_condition.rhs_val.lexical_slot = 1;

    std::vector<Case> cases;
    cases.push_back({"insert into users values (1);",
                     "insert into users values (9);",
                     std::make_unique<DMLPlan>(T_Insert, nullptr, "users", std::vector<Value>{insert_id},
                                               std::vector<Condition>{}, std::vector<SetClause>{}),
                     {0}});
    cases.push_back({"delete from users where id = 1;",
                     "delete from users where id = 9;",
                     std::make_unique<DMLPlan>(T_Delete, nullptr, "users", std::vector<Value>{},
                                               std::vector<Condition>{delete_condition}, std::vector<SetClause>{}),
                     {0}});
    cases.push_back(
        {"update users set score = score + 1 where id = 1;",
         "update users set score = score + 1 where id = 9;",
         std::make_unique<DMLPlan>(T_Update, nullptr, "users", std::vector<Value>{},
                                   std::vector<Condition>{update_condition}, std::vector<SetClause>{self_reference}),
         {1}});

    for (auto& test_case : cases) {
        auto first = parser::normalize_sql(test_case.first_sql);
        auto second = parser::normalize_sql(test_case.second_sql);
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
        ASSERT_EQ(first.key, second.key);
        auto parsed = ast::parse_sql(test_case.first_sql);
        ast::assign_literal_slots(*parsed);
        cache::StatementTemplateCache cache;
        cache.publish(first.key, 1, std::shared_ptr<const ast::TreeNode>(std::move(parsed)), nullptr,
                      std::shared_ptr<const Plan>(std::move(test_case.plan)));

        BoundPlan first_plan;
        BoundPlan second_plan;
        std::thread first_thread([&] { first_plan = cache.lookup_full(first.key, 1, nullptr, &first).plan; });
        std::thread second_thread([&] { second_plan = cache.lookup_full(second.key, 1, nullptr, &second).plan; });
        first_thread.join();
        second_thread.join();
        ASSERT_TRUE(first_plan);
        ASSERT_TRUE(second_plan);
        EXPECT_EQ(first_plan.get(), second_plan.get());
        EXPECT_NE(first_plan.literals.get(), second_plan.literals.get());
        ASSERT_NE(first_plan.literals->Find(test_case.slots[0]), nullptr);
        ASSERT_NE(second_plan.literals->Find(test_case.slots[0]), nullptr);
        EXPECT_EQ(first_plan.literals->Find(test_case.slots[0])->int_val, 1);
        EXPECT_EQ(second_plan.literals->Find(test_case.slots[0])->int_val, 9);
    }
}

TEST(StatementTemplateTest, LiteralFreeDmlPlanLookupKeepsEmptyOverlay) {
    auto shape = parser::normalize_sql("delete from users;");
    auto parsed = ast::parse_sql("delete from users;");
    cache::StatementTemplateCache cache;
    cache.publish(
        shape.key, 1, std::shared_ptr<const ast::TreeNode>(std::move(parsed)), nullptr,
        std::shared_ptr<const Plan>(std::make_unique<DMLPlan>(T_Delete, nullptr, "users", std::vector<Value>{},
                                                              std::vector<Condition>{}, std::vector<SetClause>{})));
    auto lookup = cache.lookup_full(shape.key, 1, nullptr, &shape);
    ASSERT_TRUE(lookup.plan);
    EXPECT_TRUE(lookup.plan.literals->values.empty());
}

TEST_F(StatementTemplateDatabaseTest, ReplacingPlanAtomicallyClearsUnsupportedPreparedDescriptor) {
    auto shape = parser::normalize_sql("select id from users where id = 1;");
    ASSERT_TRUE(shape);
    auto parsed = ast::parse_sql("select id from users where id = 1;");
    ast::assign_literal_slots(*parsed);

    auto prepared_plan = select_plan(T_IndexScan);
    auto prepared = PreparedSelectDescriptor::Build(*prepared_plan, sm_manager.get());
    ASSERT_NE(prepared, nullptr);
    cache::StatementTemplateCache cache;
    cache.publish(shape.key, 1, std::shared_ptr<const ast::TreeNode>(std::move(parsed)), nullptr,
                  std::shared_ptr<const Plan>(std::move(prepared_plan)), prepared);

    auto unsupported_plan = select_plan(T_SeqScan);
    EXPECT_EQ(PreparedSelectDescriptor::Build(*unsupported_plan, sm_manager.get()), nullptr);
    cache.publish(shape.key, 1, nullptr, nullptr, std::shared_ptr<const Plan>(std::move(unsupported_plan)), nullptr);

    auto lookup = cache.lookup_full(shape.key, 1, sm_manager.get(), &shape);
    EXPECT_EQ(lookup.prepared_select, nullptr);
    ASSERT_TRUE(lookup.plan);
    const auto& select = static_cast<const DMLPlan&>(*lookup.plan);
    const auto& projection = static_cast<const ProjectionPlan&>(*select.subplan_);
    EXPECT_EQ(projection.subplan_->tag, T_SeqScan);
}
