/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

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

ColMeta make_int_col(const std::string& tab_name, const std::string& name, int offset) {
    return {.tab_name = tab_name, .name = name, .type = TYPE_INT, .len = 4, .offset = offset, .index = false};
}

IndexMeta make_index(const std::string& tab_name, const std::vector<ColMeta>& cols) {
    IndexMeta index;
    index.tab_name = tab_name;
    index.col_num = static_cast<int>(cols.size());
    index.col_tot_len = 0;
    index.cols = cols;
    for (const auto& col : cols) {
        index.col_tot_len += col.len;
    }
    return index;
}

TabMeta make_stock_tab() {
    TabMeta tab;
    tab.name = "stock";
    tab.cols = {
        make_int_col("stock", "s_w_id", 0),
        make_int_col("stock", "s_i_id", 4),
        make_int_col("stock", "s_quantity", 8),
    };
    tab.indexes.push_back(make_index("stock", {tab.cols[0], tab.cols[1]}));
    return tab;
}

TabMeta make_order_line_tab() {
    TabMeta tab;
    tab.name = "order_line";
    tab.cols = {
        make_int_col("order_line", "ol_w_id", 0),  make_int_col("order_line", "ol_d_id", 4),
        make_int_col("order_line", "ol_o_id", 8),  make_int_col("order_line", "ol_number", 12),
        make_int_col("order_line", "ol_i_id", 16),
    };
    tab.indexes.push_back(make_index("order_line", {tab.cols[0], tab.cols[1], tab.cols[2], tab.cols[3]}));
    return tab;
}

Condition value_cond(const std::string& tab_name, const std::string& col_name, CompOp op, int value) {
    Condition cond;
    cond.lhs_col = {.tab_name = tab_name, .col_name = col_name};
    cond.op = op;
    cond.is_rhs_val = true;
    cond.rhs_val.set_int(value);
    return cond;
}

Condition join_cond(const std::string& lhs_tab, const std::string& lhs_col, const std::string& rhs_tab,
                    const std::string& rhs_col) {
    Condition cond;
    cond.lhs_col = {.tab_name = lhs_tab, .col_name = lhs_col};
    cond.op = OP_EQ;
    cond.is_rhs_val = false;
    cond.rhs_col = {.tab_name = rhs_tab, .col_name = rhs_col};
    return cond;
}

std::unique_ptr<Query> make_aggregate_query(bool with_sort, bool with_limit) {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::SelectStmt>(
        std::vector<std::unique_ptr<ast::SelectItem>>{}, std::vector<ast::TableRef>{ast::TableRef("grade", "")},
        std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::vector<std::unique_ptr<ast::Col>>{},
        std::vector<std::unique_ptr<ast::HavingExpr>>{}, std::vector<std::unique_ptr<ast::OrderByItem>>{}, false, 0,
        true);
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

std::unique_ptr<Query> make_stock_level_query() {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::SelectStmt>(
        std::vector<std::unique_ptr<ast::SelectItem>>{},
        std::vector<ast::TableRef>{ast::TableRef("stock", ""), ast::TableRef("order_line", "")},
        std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::vector<std::unique_ptr<ast::Col>>{},
        std::vector<std::unique_ptr<ast::HavingExpr>>{}, std::vector<std::unique_ptr<ast::OrderByItem>>{}, false, 0,
        true);
    query->tables = {"stock", "order_line"};
    query->has_aggregate = true;
    query->select_items.push_back({.expr = make_count_star_expr(), .alias = "", .output_name = "COUNT(*)"});
    query->output_names = {"COUNT(*)"};
    query->conds = {
        value_cond("stock", "s_w_id", OP_EQ, 1),          join_cond("stock", "s_i_id", "order_line", "ol_i_id"),
        value_cond("order_line", "ol_w_id", OP_EQ, 1),    value_cond("order_line", "ol_d_id", OP_EQ, 1),
        value_cond("order_line", "ol_o_id", OP_GE, 2981), value_cond("order_line", "ol_o_id", OP_LT, 3001),
        value_cond("stock", "s_quantity", OP_LT, 15),
    };
    return query;
}

std::unique_ptr<Query> make_order_line_suffix_lookup_query() {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::SelectStmt>(
        std::vector<std::unique_ptr<ast::SelectItem>>{}, std::vector<ast::TableRef>{ast::TableRef("order_line", "")},
        std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::vector<std::unique_ptr<ast::Col>>{},
        std::vector<std::unique_ptr<ast::HavingExpr>>{}, std::vector<std::unique_ptr<ast::OrderByItem>>{}, false, 0,
        true);
    query->tables = {"order_line"};
    query->has_aggregate = true;
    query->select_items.push_back({.expr = make_count_star_expr(), .alias = "", .output_name = "COUNT(*)"});
    query->output_names = {"COUNT(*)"};
    query->conds = {
        value_cond("order_line", "ol_d_id", OP_EQ, 1),
        value_cond("order_line", "ol_o_id", OP_EQ, 3001),
    };
    return query;
}

} // namespace

class PlannerAggregateTest : public ::testing::Test {
protected:
    SmManager sm_manager_{nullptr, nullptr, nullptr, nullptr};
    SchemaManager schema_manager_{&sm_manager_};
    Planner planner_{&schema_manager_};

    void SetUp() override {
        schema_manager_.db().SetTabMeta("grade", make_grade_tab());
    }
};

TEST_F(PlannerAggregateTest, generate_select_plan_builds_aggregate_projection_shape) {
    auto query = make_aggregate_query(false, false);

    auto plan = planner_.generate_select_plan(std::move(query), nullptr);

    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->tag, T_Projection);
    auto* projection = static_cast<ProjectionPlan*>(plan.get());
    ASSERT_NE(projection->subplan_, nullptr);
    EXPECT_EQ(projection->subplan_->tag, T_Aggregate);
    auto* aggregate = static_cast<AggregatePlan*>(projection->subplan_.get());
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

    auto plan = planner_.generate_select_plan(std::move(query), nullptr);

    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->tag, T_Sort);
    auto* sort = static_cast<SortPlan*>(plan.get());
    EXPECT_EQ(sort->limit_, 5);
    ASSERT_NE(sort->subplan_, nullptr);
    EXPECT_EQ(sort->subplan_->tag, T_Projection);
    auto* projection = static_cast<ProjectionPlan*>(sort->subplan_.get());
    ASSERT_NE(projection->subplan_, nullptr);
    EXPECT_EQ(projection->subplan_->tag, T_Aggregate);
    auto* aggregate = static_cast<AggregatePlan*>(projection->subplan_.get());
    EXPECT_EQ(aggregate->subplan_->tag, T_SeqScan);
}

TEST_F(PlannerAggregateTest, stock_level_join_starts_from_order_line_and_uses_stock_inlj) {
    schema_manager_.db().SetTabMeta("stock", make_stock_tab());
    schema_manager_.db().SetTabMeta("order_line", make_order_line_tab());
    auto query = make_stock_level_query();

    auto plan = planner_.generate_select_plan(std::move(query), nullptr);

    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->tag, T_Projection);
    auto* projection = static_cast<ProjectionPlan*>(plan.get());
    ASSERT_EQ(projection->subplan_->tag, T_Aggregate);
    auto* aggregate = static_cast<AggregatePlan*>(projection->subplan_.get());
    ASSERT_EQ(aggregate->subplan_->tag, T_NestLoop);
    auto* join = static_cast<JoinPlan*>(aggregate->subplan_.get());
    ASSERT_EQ(join->left_->tag, T_IndexScan);
    auto* left_scan = static_cast<ScanPlan*>(join->left_.get());
    EXPECT_EQ(left_scan->tab_name_, "order_line");
    ASSERT_EQ(join->right_->tag, T_IndexScan);
    auto* right_scan = static_cast<ScanPlan*>(join->right_.get());
    EXPECT_EQ(right_scan->tab_name_, "stock");
    EXPECT_EQ(right_scan->index_col_names_, (std::vector<std::string>{"s_w_id", "s_i_id"}));
    EXPECT_EQ(join->inlj_left_col_.tab_name, "order_line");
    EXPECT_EQ(join->inlj_left_col_.col_name, "ol_i_id");
    EXPECT_EQ(join->inlj_right_col_.tab_name, "stock");
    EXPECT_EQ(join->inlj_right_col_.col_name, "s_i_id");
}

TEST_F(PlannerAggregateTest, suffix_equality_on_composite_index_uses_skip_scan) {
    schema_manager_.db().SetTabMeta("order_line", make_order_line_tab());
    auto query = make_order_line_suffix_lookup_query();

    auto plan = planner_.generate_select_plan(std::move(query), nullptr);

    ASSERT_NE(plan, nullptr);
    ASSERT_EQ(plan->tag, T_Projection);
    auto* projection = static_cast<ProjectionPlan*>(plan.get());
    ASSERT_EQ(projection->subplan_->tag, T_Aggregate);
    auto* aggregate = static_cast<AggregatePlan*>(projection->subplan_.get());
    ASSERT_EQ(aggregate->subplan_->tag, T_IndexSkipScan);
    auto* scan = static_cast<ScanPlan*>(aggregate->subplan_.get());
    EXPECT_EQ(scan->index_col_names_, (std::vector<std::string>{"ol_w_id", "ol_d_id", "ol_o_id", "ol_number"}));
}
