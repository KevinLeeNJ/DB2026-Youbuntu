#undef NDEBUG

#define private public
#include "optimizer/planner.h"
#undef private

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace {

TabMeta make_grade_tab() {
    TabMeta tab;
    tab.name = "grade";
    tab.cols = {
        {.tab_name = "grade", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false},
        {.tab_name = "grade", .name = "score", .type = TYPE_INT, .len = 4, .offset = 4, .index = false},
    };
    return tab;
}

QueryExpr make_col_expr(const std::string& col_name) {
    QueryExpr expr;
    expr.type = QueryExprType::COLUMN;
    expr.col = {.tab_name = "grade", .col_name = col_name};
    expr.display_name = col_name;
    return expr;
}

QueryExpr make_agg_expr(AggType type, const std::string& col_name, const std::string& display_name) {
    QueryExpr expr;
    expr.type = QueryExprType::AGGREGATE;
    expr.agg.type = type;
    expr.agg.is_star = false;
    expr.agg.col = {.tab_name = "grade", .col_name = col_name};
    expr.agg.display_name = display_name;
    expr.display_name = display_name;
    return expr;
}

QueryExpr make_count_star_expr() {
    QueryExpr expr;
    expr.type = QueryExprType::AGGREGATE;
    expr.agg.type = AggType::COUNT;
    expr.agg.is_star = true;
    expr.agg.display_name = "COUNT(*)";
    expr.display_name = "COUNT(*)";
    return expr;
}

std::shared_ptr<Query> make_aggregate_query(bool with_sort, bool with_limit) {
    auto query = std::make_shared<Query>();
    query->parse =
        std::make_unique<ast::SelectStmt>(std::vector<std::unique_ptr<ast::Col>>{}, std::vector<std::string>{"grade"},
                                          std::vector<std::unique_ptr<ast::BinaryExpr>>{}, nullptr);
    query->tables = {"grade"};
    query->has_aggregate = true;
    query->group_by_cols = {{.tab_name = "grade", .col_name = "id"}};

    SelectItem group_item;
    group_item.expr = make_col_expr("id");
    group_item.output_name = "id";
    query->select_items.push_back(group_item);

    SelectItem max_item;
    max_item.expr = make_agg_expr(AggType::MAX, "score", "MAX(score)");
    max_item.alias = "max_score";
    max_item.output_name = "max_score";
    query->select_items.push_back(max_item);

    HavingCondition max_having;
    max_having.lhs = make_agg_expr(AggType::MAX, "score", "MAX(score)");
    max_having.op = OP_GT;
    max_having.is_rhs_val = true;
    max_having.rhs_val.set_int(90);
    query->having_conds.push_back(max_having);

    HavingCondition count_having;
    count_having.lhs = make_count_star_expr();
    count_having.op = OP_GT;
    count_having.is_rhs_val = true;
    count_having.rhs_val.set_int(1);
    query->having_conds.push_back(count_having);

    query->output_names = {"id", "max_score"};

    if (with_sort) {
        OrderByItem order_by;
        order_by.expr = make_agg_expr(AggType::MAX, "score", "MAX(score)");
        order_by.is_desc = true;
        query->order_by_items.push_back(order_by);
    }
    query->has_limit = with_limit;
    query->limit = with_limit ? 5 : 0;
    return query;
}

} // namespace

class PlannerAggregateTest : public ::testing::Test {
protected:
    SmManager sm_manager_{nullptr, nullptr, nullptr, nullptr};
    Planner planner_{&sm_manager_};

    void SetUp() override {
        sm_manager_.db_.SetTabMeta("grade", make_grade_tab());
    }
};

TEST_F(PlannerAggregateTest, generate_select_plan_builds_aggregate_projection_shape) {
    auto query = make_aggregate_query(false, false);

    auto plan = planner_.generate_select_plan(query, nullptr);

    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->tag, T_Projection);
    auto projection = std::static_pointer_cast<ProjectionPlan>(plan);
    ASSERT_NE(projection->subplan_, nullptr);
    EXPECT_EQ(projection->subplan_->tag, T_Aggregate);
    auto aggregate = std::static_pointer_cast<AggregatePlan>(projection->subplan_);
    ASSERT_NE(aggregate->subplan_, nullptr);
    EXPECT_EQ(aggregate->subplan_->tag, T_SeqScan);
    ASSERT_EQ(aggregate->group_by_cols_.size(), 1);
    EXPECT_EQ(aggregate->group_by_cols_[0].col_name, "id");
    ASSERT_EQ(aggregate->agg_exprs_.size(), 2);
    EXPECT_EQ(aggregate->agg_exprs_[0].display_name, "MAX(score)");
    EXPECT_EQ(aggregate->agg_exprs_[1].display_name, "COUNT(*)");
}

TEST_F(PlannerAggregateTest, generate_select_plan_pushes_limit_into_sort_over_projection_and_aggregate) {
    auto query = make_aggregate_query(true, true);

    auto plan = planner_.generate_select_plan(query, nullptr);

    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->tag, T_Sort);
    auto sort = std::static_pointer_cast<SortPlan>(plan);
    EXPECT_EQ(sort->limit_, 5);
    ASSERT_NE(sort->subplan_, nullptr);
    EXPECT_EQ(sort->subplan_->tag, T_Projection);
    auto projection = std::static_pointer_cast<ProjectionPlan>(sort->subplan_);
    ASSERT_NE(projection->subplan_, nullptr);
    EXPECT_EQ(projection->subplan_->tag, T_Aggregate);
    auto aggregate = std::static_pointer_cast<AggregatePlan>(projection->subplan_);
    EXPECT_EQ(aggregate->subplan_->tag, T_SeqScan);
}
