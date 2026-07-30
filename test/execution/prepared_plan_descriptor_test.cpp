/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <gtest/gtest.h>

#include <memory>
#include <type_traits>
#include <utility>

#include "execution/prepared_plan_descriptor.h"

namespace {

Condition parameter_condition(std::size_t ordinal, ColType type) {
    Condition condition;
    condition.lhs_col = {"items", "id"};
    condition.op = OP_EQ;
    condition.is_rhs_val = true;
    condition.rhs_val.type = type;
    condition.rhs_val.parameter_ordinal = ordinal;
    return condition;
}

Value parameter_value(std::size_t ordinal, ColType type) {
    Value value;
    value.type = type;
    value.parameter_ordinal = ordinal;
    return value;
}

ColMeta int_result_column() {
    ColMeta column;
    column.tab_name = "items";
    column.name = "id";
    column.type = TYPE_INT;
    column.len = sizeof(int);
    return column;
}

class PreparedPlanDescriptorTest : public ::testing::Test {
protected:
    SmManager sm_manager_{nullptr, nullptr, nullptr, nullptr};

    void SetUp() override {
        TabMeta table;
        table.name = "items";
        table.cols.push_back(int_result_column());
        sm_manager_.db_.SetTabMeta("items", table);
    }

    std::unique_ptr<Plan> make_scan(std::vector<Condition> conditions = {}) {
        return std::make_unique<ScanPlan>(T_SeqScan, &sm_manager_, "items", std::move(conditions),
                                          std::vector<std::string>{});
    }

    static std::unique_ptr<Plan> make_select(std::unique_ptr<Plan> child) {
        return std::make_unique<DMLPlan>(T_select, std::move(child), "", std::vector<Value>{}, std::vector<Condition>{},
                                         std::vector<SetClause>{});
    }

    static std::unique_ptr<Plan> make_insert(std::vector<Value> values) {
        return std::make_unique<DMLPlan>(T_Insert, nullptr, "items", std::move(values), std::vector<Condition>{},
                                         std::vector<SetClause>{});
    }

    std::unique_ptr<Plan> make_update(std::vector<Condition> conditions, std::vector<SetClause> set_clauses,
                                      PlanTag scan_tag = T_SeqScan) {
        auto scan = std::make_unique<ScanPlan>(scan_tag, &sm_manager_, "items", conditions, std::vector<std::string>{});
        return std::make_unique<DMLPlan>(T_Update, std::move(scan), "items", std::vector<Value>{},
                                         std::move(conditions), std::move(set_clauses));
    }

    static std::unique_ptr<const PreparedPlanDescriptor> build(std::unique_ptr<Plan> plan,
                                                               std::vector<std::string> names = {"id"},
                                                               std::vector<ColMeta> schema = {int_result_column()}) {
        return PreparedPlanDescriptor::Build(std::move(plan), PreparedStatementKind::Select, std::move(names),
                                             std::move(schema), "db-instance-17", 42);
    }
};

static_assert(std::is_same_v<decltype(std::declval<const PreparedPlanDescriptor&>().plan()), const Plan*>);
static_assert(std::is_same_v<decltype(std::declval<const PreparedPlanDescriptor&>().dml_plan()), const DMLPlan*>);
static_assert(std::is_same_v<decltype(std::declval<const PreparedPlanDescriptor&>().parameter_layout()),
                             const std::vector<PreparedParameterSlot>&>);
static_assert(std::is_same_v<decltype(std::declval<const PreparedPlanDescriptor&>().insert_executable()),
                             const PreparedInsertExecutable*>);
static_assert(
    std::is_same_v<decltype(PreparedPlanDescriptor::Build(nullptr, PreparedStatementKind::Select, {}, {}, "", 0)),
                   std::unique_ptr<const PreparedPlanDescriptor>>);
static_assert(!std::is_copy_constructible_v<PreparedPlanDescriptor>);
static_assert(!std::is_move_constructible_v<PreparedPlanDescriptor>);

TEST_F(PreparedPlanDescriptorTest, supported_select_retains_immutable_topology_and_metadata) {
    std::vector<Condition> conditions{parameter_condition(1, TYPE_INT), parameter_condition(1, TYPE_INT)};
    auto child = std::make_unique<FilterPlan>(T_Filter, make_scan(conditions), conditions);
    child = std::make_unique<FilterPlan>(
        T_Filter,
        std::make_unique<SortPlan>(T_Sort, std::make_unique<LimitPlan>(T_Limit, std::move(child), 10, 2),
                                   std::vector<OrderByItem>{}),
        std::vector<Condition>{});
    auto projection = std::make_unique<ProjectionPlan>(T_Projection, std::move(child), std::vector<SelectItem>{},
                                                       std::vector<std::string>{"id"});

    auto descriptor = build(make_select(std::move(projection)));

    ASSERT_TRUE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::None);
    ASSERT_NE(descriptor->plan(), nullptr);
    EXPECT_EQ(descriptor->plan()->tag, T_select);
    ASSERT_EQ(descriptor->parameter_layout().size(), 1);
    EXPECT_EQ(descriptor->parameter_layout()[0].ordinal, 1);
    EXPECT_EQ(descriptor->parameter_layout()[0].type, TYPE_INT);
    EXPECT_FALSE(descriptor->limit_offset_layout().limit_ordinal.has_value());
    EXPECT_FALSE(descriptor->limit_offset_layout().offset_ordinal.has_value());
}

TEST_F(PreparedPlanDescriptorTest, unsupported_node_rejects_whole_descriptor) {
    auto aggregate =
        std::make_unique<AggregatePlan>(T_Aggregate, make_scan({parameter_condition(1, TYPE_INT)}),
                                        std::vector<TabCol>{}, std::vector<AggExpr>{}, std::vector<HavingCondition>{});

    auto descriptor = build(make_select(std::move(aggregate)));

    EXPECT_FALSE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::UnsupportedShape);
    EXPECT_TRUE(descriptor->parameter_layout().empty());
}

TEST_F(PreparedPlanDescriptorTest, repeated_dense_parameters_share_one_typed_slot) {
    auto descriptor =
        build(make_select(make_scan({parameter_condition(1, TYPE_STRING), parameter_condition(1, TYPE_STRING),
                                     parameter_condition(2, TYPE_FLOAT)})));

    ASSERT_TRUE(descriptor->eligible());
    ASSERT_EQ(descriptor->parameter_layout().size(), 2);
    EXPECT_EQ(descriptor->parameter_layout()[0].ordinal, 1);
    EXPECT_EQ(descriptor->parameter_layout()[0].type, TYPE_STRING);
    EXPECT_EQ(descriptor->parameter_layout()[1].ordinal, 2);
    EXPECT_EQ(descriptor->parameter_layout()[1].type, TYPE_FLOAT);
}

TEST_F(PreparedPlanDescriptorTest, repeated_parameter_type_mismatch_is_unsupported) {
    auto descriptor =
        build(make_select(make_scan({parameter_condition(1, TYPE_INT), parameter_condition(1, TYPE_STRING)})));

    EXPECT_FALSE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::InvalidParameterLayout);
    EXPECT_TRUE(descriptor->parameter_layout().empty());
}

TEST_F(PreparedPlanDescriptorTest, parameter_ordinal_hole_is_invalid) {
    auto descriptor = build(make_select(make_scan({parameter_condition(2, TYPE_INT)})));

    EXPECT_FALSE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::InvalidParameterLayout);
    EXPECT_TRUE(descriptor->parameter_layout().empty());
}

TEST_F(PreparedPlanDescriptorTest, limit_and_offset_ordinals_are_independent_and_dense) {
    auto limit = std::make_unique<LimitPlan>(T_Limit, make_scan({parameter_condition(1, TYPE_STRING)}), 0, 0, 2, 3);

    auto descriptor = build(make_select(std::move(limit)));

    ASSERT_TRUE(descriptor->eligible());
    ASSERT_EQ(descriptor->parameter_layout().size(), 3);
    EXPECT_EQ(descriptor->parameter_layout()[1].type, TYPE_INT);
    EXPECT_EQ(descriptor->parameter_layout()[2].type, TYPE_INT);
    EXPECT_EQ(descriptor->limit_offset_layout().limit_ordinal, 2);
    EXPECT_EQ(descriptor->limit_offset_layout().offset_ordinal, 3);
}

TEST_F(PreparedPlanDescriptorTest, identity_generation_and_result_schema_round_trip) {
    auto descriptor = build(make_select(make_scan()));

    EXPECT_EQ(descriptor->statement_kind(), PreparedStatementKind::Select);
    EXPECT_EQ(descriptor->database_identity(), "db-instance-17");
    EXPECT_EQ(descriptor->catalog_generation(), 42);
    ASSERT_EQ(descriptor->output_names().size(), 1);
    EXPECT_EQ(descriptor->output_names()[0], "id");
    ASSERT_EQ(descriptor->result_schema().size(), 1);
    EXPECT_EQ(descriptor->result_schema()[0].type, TYPE_INT);
    EXPECT_EQ(descriptor->result_schema()[0].len, static_cast<int>(sizeof(int)));
}

TEST_F(PreparedPlanDescriptorTest, unique_owner_retains_immutable_descriptor) {
    auto dictionary_owner = build(make_select(make_scan()));
    const PreparedPlanDescriptor& operation_borrow = *dictionary_owner;

    ASSERT_TRUE(operation_borrow.eligible());
    ASSERT_NE(operation_borrow.plan(), nullptr);
    EXPECT_EQ(operation_borrow.database_identity(), "db-instance-17");
}

TEST(PreparedPlanDescriptorStandaloneTest, null_plan_is_safely_ineligible) {
    auto descriptor = PreparedPlanDescriptor::Build(nullptr, PreparedStatementKind::Select, {}, {}, "db", 1);

    EXPECT_FALSE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::NullPlan);
    EXPECT_EQ(descriptor->plan(), nullptr);
}

TEST_F(PreparedPlanDescriptorTest, insert_preserves_values_and_collects_repeated_char_float_parameters) {
    auto descriptor = PreparedPlanDescriptor::Build(
        make_insert({parameter_value(1, TYPE_STRING), parameter_value(2, TYPE_FLOAT), parameter_value(1, TYPE_STRING)}),
        PreparedStatementKind::Insert, {}, {}, "db-instance-17", 42);

    ASSERT_TRUE(descriptor->eligible());
    EXPECT_EQ(descriptor->statement_kind(), PreparedStatementKind::Insert);
    ASSERT_EQ(descriptor->parameter_layout().size(), 2);
    EXPECT_EQ(descriptor->parameter_layout()[0].type, TYPE_STRING);
    EXPECT_EQ(descriptor->parameter_layout()[1].type, TYPE_FLOAT);
    ASSERT_NE(descriptor->dml_plan(), nullptr);
    EXPECT_EQ(descriptor->dml_plan()->tab_name_, "items");
    ASSERT_EQ(descriptor->dml_plan()->values_.size(), 3);
    EXPECT_EQ(descriptor->dml_plan()->values_[2].parameter_ordinal, 1);
    // Synthetic descriptors without open generation-scoped handles retain the
    // existing generic prepared runtime instead of caching invalid pointers.
    EXPECT_EQ(descriptor->insert_executable(), nullptr);
}

TEST_F(PreparedPlanDescriptorTest, update_collects_conditions_self_reference_and_additional_terms) {
    SetClause set_clause;
    set_clause.lhs = {"items", "id"};
    set_clause.is_self_ref = true;
    set_clause.rhs_col = {"items", "id"};
    set_clause.op = UpdateOp::SELF_ADD;
    set_clause.rhs = parameter_value(2, TYPE_INT);
    set_clause.additional_terms.push_back({parameter_value(3, TYPE_FLOAT), UpdateOp::SELF_ADD});
    set_clause.additional_terms.push_back({parameter_value(2, TYPE_INT), UpdateOp::SELF_SUB});
    auto update = make_update({parameter_condition(1, TYPE_INT)}, {set_clause}, T_IndexScan);
    auto* mutable_dml = static_cast<DMLPlan*>(update.get());
    mutable_dml->point_access_ = PointAccessPath{{"id"}, {0}};
    auto descriptor =
        PreparedPlanDescriptor::Build(std::move(update), PreparedStatementKind::Update, {}, {}, "db-instance-17", 42);

    ASSERT_TRUE(descriptor->eligible());
    EXPECT_EQ(descriptor->statement_kind(), PreparedStatementKind::Update);
    ASSERT_EQ(descriptor->parameter_layout().size(), 3);
    EXPECT_EQ(descriptor->parameter_layout()[0].type, TYPE_INT);
    EXPECT_EQ(descriptor->parameter_layout()[1].type, TYPE_INT);
    EXPECT_EQ(descriptor->parameter_layout()[2].type, TYPE_FLOAT);
    ASSERT_NE(descriptor->dml_plan(), nullptr);
    ASSERT_TRUE(descriptor->dml_plan()->point_access_.has_value());
    EXPECT_EQ(descriptor->dml_plan()->point_access_->index_cols, std::vector<std::string>{"id"});
    ASSERT_NE(descriptor->dml_plan()->subplan_, nullptr);
    EXPECT_EQ(descriptor->dml_plan()->subplan_->tag, T_IndexScan);
    ASSERT_EQ(descriptor->dml_plan()->set_clauses_.size(), 1);
    EXPECT_EQ(descriptor->dml_plan()->set_clauses_[0].additional_terms.size(), 2);
}

TEST_F(PreparedPlanDescriptorTest, update_rejects_non_scan_subplan_as_whole_shape) {
    SetClause set_clause;
    set_clause.lhs = {"items", "id"};
    set_clause.rhs = parameter_value(1, TYPE_INT);
    auto filter =
        std::make_unique<FilterPlan>(T_Filter, make_scan(), std::vector<Condition>{parameter_condition(2, TYPE_INT)});
    auto update = std::make_unique<DMLPlan>(T_Update, std::move(filter), "items", std::vector<Value>{},
                                            std::vector<Condition>{}, std::vector<SetClause>{set_clause});

    auto descriptor = PreparedPlanDescriptor::Build(std::move(update), PreparedStatementKind::Update, {}, {}, "db", 1);

    EXPECT_FALSE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::UnsupportedShape);
    EXPECT_TRUE(descriptor->parameter_layout().empty());
}

TEST_F(PreparedPlanDescriptorTest, delete_is_never_eligible_as_update) {
    auto delete_plan = std::make_unique<DMLPlan>(T_Delete, make_scan(), "items", std::vector<Value>{},
                                                 std::vector<Condition>{}, std::vector<SetClause>{});

    auto descriptor =
        PreparedPlanDescriptor::Build(std::move(delete_plan), PreparedStatementKind::Update, {}, {}, "db", 1);

    EXPECT_FALSE(descriptor->eligible());
    EXPECT_EQ(descriptor->fallback_reason(), PreparedPlanFallbackReason::UnsupportedStatement);
}

TEST_F(PreparedPlanDescriptorTest, insert_rejects_holes_and_repeated_type_mismatches) {
    auto hole = PreparedPlanDescriptor::Build(make_insert({parameter_value(2, TYPE_INT)}),
                                              PreparedStatementKind::Insert, {}, {}, "db", 1);
    auto mismatch =
        PreparedPlanDescriptor::Build(make_insert({parameter_value(1, TYPE_INT), parameter_value(1, TYPE_FLOAT)}),
                                      PreparedStatementKind::Insert, {}, {}, "db", 1);

    EXPECT_FALSE(hole->eligible());
    EXPECT_EQ(hole->fallback_reason(), PreparedPlanFallbackReason::InvalidParameterLayout);
    EXPECT_FALSE(mismatch->eligible());
    EXPECT_EQ(mismatch->fallback_reason(), PreparedPlanFallbackReason::InvalidParameterLayout);
}

} // namespace
