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

QueryExpr make_agg_expr(AggType type, const std::string& col_name, const std::string& display_name,
                        bool is_distinct = false) {
    QueryExpr expr;
    expr.type = QueryExprType::AGGREGATE;
    expr.agg.type = type;
    expr.agg.is_star = false;
    expr.agg.is_distinct = is_distinct;
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

TabMeta make_exact_outer_tab() {
    TabMeta tab;
    tab.name = "qz7";
    tab.cols = {make_int_col(tab.name, "k", 0), make_int_col(tab.name, "v", 4)};
    tab.indexes.push_back(make_index(tab.name, {tab.cols[0]}));
    return tab;
}

TabMeta make_parameterized_inner_tab(const std::string& tab_name, bool trailing_unbound_col) {
    TabMeta tab;
    tab.name = tab_name;
    tab.cols = {
        make_int_col(tab.name, "p", 0),
        make_int_col(tab.name, "q", 4),
        make_int_col(tab.name, "r", 8),
        make_int_col(tab.name, "s", 12),
    };
    if (!trailing_unbound_col) {
        tab.indexes.push_back(make_index(tab.name, {tab.cols[0]}));
        tab.indexes.push_back(make_index(tab.name, {tab.cols[0], tab.cols[1], tab.cols[2]}));
    } else {
        tab.indexes.push_back(make_index(tab.name, {tab.cols[0], tab.cols[1], tab.cols[2], tab.cols[3]}));
    }
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

TabMeta make_reverse_index_tab() {
    TabMeta tab;
    tab.name = "reverse_index";
    tab.cols = {make_int_col("reverse_index", "a", 0), make_int_col("reverse_index", "z", 4)};
    tab.indexes.push_back(make_index("reverse_index", {tab.cols[1], tab.cols[0]}));
    return tab;
}

TabMeta make_point_program_tab() {
    TabMeta tab;
    tab.name = "point_program";
    tab.cols = {make_int_col("point_program", "id", 0), make_int_col("point_program", "value", 4)};
    tab.indexes.push_back(make_index("point_program", {tab.cols[0]}));
    return tab;
}

std::unique_ptr<Query> make_point_update_query(int id, int value) {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::UpdateStmt>("point_program", std::vector<std::unique_ptr<ast::SetClause>>{},
                                                     std::vector<std::unique_ptr<ast::BinaryExpr>>{});
    query->conds = {value_cond("point_program", "id", OP_EQ, id)};
    SetClause set_clause;
    set_clause.lhs = {.tab_name = "point_program", .col_name = "value"};
    set_clause.rhs.set_int(value);
    query->set_clauses = {set_clause};
    return query;
}

std::unique_ptr<Query> make_point_chained_update_query(int id, int first, int second, UpdateOp second_op) {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::UpdateStmt>("point_program", std::vector<std::unique_ptr<ast::SetClause>>{},
                                                     std::vector<std::unique_ptr<ast::BinaryExpr>>{});
    query->conds = {value_cond("point_program", "id", OP_EQ, id)};
    SetClause set_clause;
    set_clause.lhs = {.tab_name = "point_program", .col_name = "value"};
    set_clause.is_self_ref = true;
    set_clause.rhs_col = {.tab_name = "point_program", .col_name = "value"};
    set_clause.op = UpdateOp::SELF_SUB;
    set_clause.rhs.set_int(first);
    UpdateTerm term;
    term.op = second_op;
    term.rhs.set_int(second);
    set_clause.additional_terms.push_back(term);
    query->set_clauses = {set_clause};
    return query;
}

std::unique_ptr<Query> make_point_delete_query(int id) {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::DeleteStmt>("point_program", std::vector<std::unique_ptr<ast::BinaryExpr>>{});
    query->conds = {value_cond("point_program", "id", OP_EQ, id)};
    return query;
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

std::unique_ptr<Query> make_parameterized_exact_join_query(const std::string& inner_table) {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::SelectStmt>(
        std::vector<std::unique_ptr<ast::SelectItem>>{},
        std::vector<ast::TableRef>{ast::TableRef(inner_table, ""), ast::TableRef("qz7", "")},
        std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::vector<std::unique_ptr<ast::Col>>{},
        std::vector<std::unique_ptr<ast::HavingExpr>>{}, std::vector<std::unique_ptr<ast::OrderByItem>>{}, false, 0,
        true);
    query->tables = {inner_table, "qz7"};
    query->has_aggregate = true;
    query->select_items.push_back({.expr = make_count_star_expr(), .alias = "", .output_name = "COUNT(*)"});
    query->output_names = {"COUNT(*)"};
    query->conds = {
        value_cond("qz7", "k", OP_EQ, 4),
        join_cond("qz7", "k", inner_table, "p"),
        value_cond(inner_table, "q", OP_EQ, 7),
        value_cond(inner_table, "r", OP_EQ, 9),
    };
    return query;
}

std::unique_ptr<Query> make_reverse_index_query(int a_value, int z_value) {
    auto query = std::make_unique<Query>();
    query->parse = std::make_unique<ast::SelectStmt>(
        std::vector<std::unique_ptr<ast::SelectItem>>{}, std::vector<ast::TableRef>{ast::TableRef("reverse_index", "")},
        std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::vector<std::unique_ptr<ast::Col>>{},
        std::vector<std::unique_ptr<ast::HavingExpr>>{}, std::vector<std::unique_ptr<ast::OrderByItem>>{}, false, 0,
        true);
    query->tables = {"reverse_index"};
    query->has_aggregate = true;
    query->select_items.push_back({.expr = make_count_star_expr(), .alias = "", .output_name = "COUNT(*)"});
    query->output_names = {"COUNT(*)"};
    query->conds = {value_cond("reverse_index", "a", OP_EQ, a_value), value_cond("reverse_index", "z", OP_EQ, z_value)};
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

TEST_F(PlannerAggregateTest, countAndCountDistinctUseSeparateAggregateStatesAndCacheShapes) {
    auto query = make_aggregate_query(false, false);
    query->group_by_cols.clear();
    query->having_conds.clear();
    query->select_items.clear();
    query->output_names.clear();

    SelectItem count_item;
    count_item.expr = make_agg_expr(AggType::COUNT, "score", "COUNT(score)");
    count_item.output_name = "COUNT(score)";
    query->select_items.push_back(count_item);
    query->output_names.push_back(count_item.output_name);

    SelectItem distinct_item;
    distinct_item.expr = make_agg_expr(AggType::COUNT, "score", "COUNT(DISTINCT score)", true);
    distinct_item.output_name = "COUNT(DISTINCT score)";
    query->select_items.push_back(distinct_item);
    query->output_names.push_back(distinct_item.output_name);

    auto plan = planner_.generate_select_plan(std::move(query), nullptr);
    ASSERT_NE(plan, nullptr);
    auto* projection = static_cast<ProjectionPlan*>(plan.get());
    auto* aggregate = static_cast<AggregatePlan*>(projection->subplan_.get());
    ASSERT_EQ(aggregate->agg_exprs_.size(), 2);
    EXPECT_FALSE(aggregate->agg_exprs_[0].is_distinct);
    EXPECT_TRUE(aggregate->agg_exprs_[1].is_distinct);

    auto normal_shape = make_aggregate_query(false, false);
    normal_shape->group_by_cols.clear();
    normal_shape->having_conds.clear();
    normal_shape->select_items.clear();
    normal_shape->select_items.push_back(count_item);
    auto distinct_shape = make_aggregate_query(false, false);
    distinct_shape->group_by_cols.clear();
    distinct_shape->having_conds.clear();
    distinct_shape->select_items.clear();
    distinct_shape->select_items.push_back(distinct_item);
    EXPECT_NE(planner_.make_physical_plan_cache_key(*normal_shape, 0),
              planner_.make_physical_plan_cache_key(*distinct_shape, 0));
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
    sm_manager_.db_.SetTabMeta("stock", make_stock_tab());
    sm_manager_.db_.SetTabMeta("order_line", make_order_line_tab());
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
    sm_manager_.db_.SetTabMeta("order_line", make_order_line_tab());
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

TEST_F(PlannerAggregateTest, exact_outer_lookup_enables_parameterized_composite_exact_lookup) {
    sm_manager_.db_.SetTabMeta("qz7", make_exact_outer_tab());
    sm_manager_.db_.SetTabMeta("mv3", make_parameterized_inner_tab("mv3", false));
    auto query = make_parameterized_exact_join_query("mv3");

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
    EXPECT_EQ(left_scan->tab_name_, "qz7");
    ASSERT_EQ(join->right_->tag, T_IndexScan);
    auto* right_scan = static_cast<ScanPlan*>(join->right_.get());
    EXPECT_EQ(right_scan->tab_name_, "mv3");
    EXPECT_EQ(right_scan->index_col_names_, (std::vector<std::string>{"p", "q", "r"}));
    EXPECT_EQ(join->inlj_left_col_.tab_name, "qz7");
    EXPECT_EQ(join->inlj_left_col_.col_name, "k");
    EXPECT_EQ(join->inlj_right_col_.tab_name, "mv3");
    EXPECT_EQ(join->inlj_right_col_.col_name, "p");
}

TEST_F(PlannerAggregateTest, incomplete_parameterized_key_keeps_existing_skip_scan_order) {
    sm_manager_.db_.SetTabMeta("qz7", make_exact_outer_tab());
    sm_manager_.db_.SetTabMeta("nx9", make_parameterized_inner_tab("nx9", true));
    auto query = make_parameterized_exact_join_query("nx9");

    auto plan = planner_.generate_select_plan(std::move(query), nullptr);

    ASSERT_NE(plan, nullptr);
    auto* projection = static_cast<ProjectionPlan*>(plan.get());
    auto* aggregate = static_cast<AggregatePlan*>(projection->subplan_.get());
    ASSERT_EQ(aggregate->subplan_->tag, T_NestLoop);
    auto* join = static_cast<JoinPlan*>(aggregate->subplan_.get());
    ASSERT_EQ(join->left_->tag, T_IndexSkipScan);
    EXPECT_EQ(static_cast<ScanPlan*>(join->left_.get())->tab_name_, "nx9");
    ASSERT_EQ(join->right_->tag, T_IndexScan);
    EXPECT_EQ(static_cast<ScanPlan*>(join->right_.get())->tab_name_, "qz7");
}

TEST_F(PlannerAggregateTest, physical_template_reuses_shape_without_reusing_literal_values) {
    if (!planner_.enable_physical_plan_cache_) {
        GTEST_SKIP() << "physical plan cache is disabled";
    }
    sm_manager_.db_.SetTabMeta("order_line", make_order_line_tab());
    auto first_query = make_order_line_suffix_lookup_query();
    auto second_query = make_order_line_suffix_lookup_query();
    second_query->conds[0].rhs_val.set_int(7);

    auto first_plan = planner_.generate_select_plan(std::move(first_query), nullptr);
    auto second_plan = planner_.generate_select_plan(std::move(second_query), nullptr);

    ASSERT_EQ(planner_.physical_plan_cache_.size(), 1);

    auto* first_projection = static_cast<ProjectionPlan*>(first_plan.get());
    auto* first_aggregate = static_cast<AggregatePlan*>(first_projection->subplan_.get());
    auto* first_scan = static_cast<ScanPlan*>(first_aggregate->subplan_.get());
    ASSERT_EQ(first_scan->conds_.size(), 2);
    EXPECT_EQ(first_scan->conds_[0].rhs_val.int_val, 1);

    auto* second_projection = static_cast<ProjectionPlan*>(second_plan.get());
    auto* second_aggregate = static_cast<AggregatePlan*>(second_projection->subplan_.get());
    auto* second_scan = static_cast<ScanPlan*>(second_aggregate->subplan_.get());
    ASSERT_EQ(second_scan->conds_.size(), 2);
    EXPECT_EQ(second_scan->conds_[0].rhs_val.int_val, 7);
}

TEST_F(PlannerAggregateTest, physical_plan_cache_can_be_disabled) {
    sm_manager_.db_.SetTabMeta("order_line", make_order_line_tab());
    planner_.enable_physical_plan_cache_ = false;

    auto query = make_order_line_suffix_lookup_query();
    ASSERT_NE(planner_.generate_select_plan(std::move(query), nullptr), nullptr);
    EXPECT_TRUE(planner_.physical_plan_cache_.empty());
}

TEST_F(PlannerAggregateTest, physical_template_is_invalidated_by_catalog_generation) {
    if (!planner_.enable_physical_plan_cache_) {
        GTEST_SKIP() << "physical plan cache is disabled";
    }
    sm_manager_.db_.SetTabMeta("order_line", make_order_line_tab());

    auto first_query = make_order_line_suffix_lookup_query();
    planner_.generate_select_plan(std::move(first_query), nullptr);
    ASSERT_EQ(planner_.physical_plan_cache_.size(), 1);

    sm_manager_.bump_catalog_generation();
    auto second_query = make_order_line_suffix_lookup_query();
    planner_.generate_select_plan(std::move(second_query), nullptr);

    EXPECT_EQ(planner_.physical_plan_cache_.size(), 1);
    EXPECT_EQ(planner_.physical_plan_cache_generation_, sm_manager_.get_catalog_generation());
}

TEST_F(PlannerAggregateTest, physical_template_reorders_current_index_conditions) {
    sm_manager_.db_.SetTabMeta("reverse_index", make_reverse_index_tab());

    auto first_query = make_reverse_index_query(2, 1);
    auto second_query = make_reverse_index_query(7, 8);
    auto first_plan = planner_.generate_select_plan(std::move(first_query), nullptr);
    auto second_plan = planner_.generate_select_plan(std::move(second_query), nullptr);

    auto* first_projection = static_cast<ProjectionPlan*>(first_plan.get());
    auto* first_aggregate = static_cast<AggregatePlan*>(first_projection->subplan_.get());
    auto* first_scan = static_cast<ScanPlan*>(first_aggregate->subplan_.get());
    ASSERT_EQ(first_scan->conds_.size(), 2);
    EXPECT_EQ(first_scan->conds_[0].lhs_col.col_name, "z");
    EXPECT_EQ(first_scan->conds_[0].rhs_val.int_val, 1);

    auto* second_projection = static_cast<ProjectionPlan*>(second_plan.get());
    auto* second_aggregate = static_cast<AggregatePlan*>(second_projection->subplan_.get());
    auto* second_scan = static_cast<ScanPlan*>(second_aggregate->subplan_.get());
    ASSERT_EQ(second_scan->conds_.size(), 2);
    EXPECT_EQ(second_scan->conds_[0].lhs_col.col_name, "z");
    EXPECT_EQ(second_scan->conds_[0].rhs_val.int_val, 8);
}

TEST_F(PlannerAggregateTest, compiled_point_program_hits_for_update_and_delete_shapes) {
    planner_.enable_compiled_point_program_cache_ = true;
    sm_manager_.db_.SetTabMeta("point_program", make_point_program_tab());

    auto first_update = planner_.do_planner(make_point_update_query(1, 10), nullptr);
    auto* first_update_dml = static_cast<DMLPlan*>(first_update.get());
    ASSERT_EQ(first_update_dml->compiled_point_program_, nullptr);
    ASSERT_NE(first_update_dml->subplan_, nullptr);

    auto second_update = planner_.do_planner(make_point_update_query(2, 20), nullptr);
    auto* second_update_dml = static_cast<DMLPlan*>(second_update.get());
    ASSERT_NE(second_update_dml->compiled_point_program_, nullptr);
    EXPECT_EQ(second_update_dml->compiled_point_program_->kind, PointProgramKind::Update);
    EXPECT_EQ(second_update_dml->compiled_point_program_->conditions[0].rhs_type, TYPE_INT);
    EXPECT_EQ(second_update_dml->subplan_, nullptr);

    auto first_delete = planner_.do_planner(make_point_delete_query(1), nullptr);
    auto* first_delete_dml = static_cast<DMLPlan*>(first_delete.get());
    ASSERT_EQ(first_delete_dml->compiled_point_program_, nullptr);
    ASSERT_NE(first_delete_dml->subplan_, nullptr);

    auto second_delete = planner_.do_planner(make_point_delete_query(2), nullptr);
    auto* second_delete_dml = static_cast<DMLPlan*>(second_delete.get());
    ASSERT_NE(second_delete_dml->compiled_point_program_, nullptr);
    EXPECT_EQ(second_delete_dml->compiled_point_program_->kind, PointProgramKind::Delete);
    EXPECT_EQ(second_delete_dml->subplan_, nullptr);
    EXPECT_EQ(planner_.point_program_cache_hits_.load(), 2U);
}

TEST_F(PlannerAggregateTest, compiled_point_program_keys_include_chained_update_shape_but_not_values) {
    planner_.enable_compiled_point_program_cache_ = true;
    sm_manager_.db_.SetTabMeta("point_program", make_point_program_tab());

    auto first = planner_.do_planner(make_point_chained_update_query(1, 1, 91, UpdateOp::SELF_ADD), nullptr);
    EXPECT_EQ(static_cast<DMLPlan*>(first.get())->compiled_point_program_, nullptr);

    auto same_shape = planner_.do_planner(make_point_chained_update_query(2, 7, 13, UpdateOp::SELF_ADD), nullptr);
    auto* same_shape_dml = static_cast<DMLPlan*>(same_shape.get());
    ASSERT_NE(same_shape_dml->compiled_point_program_, nullptr);
    ASSERT_EQ(same_shape_dml->compiled_point_program_->set_ops.size(), 1U);
    ASSERT_EQ(same_shape_dml->compiled_point_program_->set_ops[0].additional_terms.size(), 1U);
    EXPECT_EQ(same_shape_dml->compiled_point_program_->set_ops[0].additional_terms[0].first, UpdateOp::SELF_ADD);

    auto different_operator =
        planner_.do_planner(make_point_chained_update_query(3, 7, 13, UpdateOp::SELF_SUB), nullptr);
    EXPECT_EQ(static_cast<DMLPlan*>(different_operator.get())->compiled_point_program_, nullptr);
}

TEST_F(PlannerAggregateTest, compiled_point_program_is_invalidated_by_catalog_generation) {
    planner_.enable_compiled_point_program_cache_ = true;
    sm_manager_.db_.SetTabMeta("point_program", make_point_program_tab());

    planner_.do_planner(make_point_update_query(1, 10), nullptr);
    ASSERT_EQ(planner_.point_program_cache_.size(), 1U);
    const auto old_generation = sm_manager_.get_catalog_generation();

    sm_manager_.bump_catalog_generation();
    ASSERT_NE(sm_manager_.get_catalog_generation(), old_generation);

    auto after_ddl = planner_.do_planner(make_point_update_query(2, 20), nullptr);
    auto* after_ddl_dml = static_cast<DMLPlan*>(after_ddl.get());
    EXPECT_EQ(after_ddl_dml->compiled_point_program_, nullptr);
    EXPECT_NE(after_ddl_dml->subplan_, nullptr);
    EXPECT_EQ(planner_.point_program_cache_generation_, sm_manager_.get_catalog_generation());
    EXPECT_EQ(planner_.point_program_cache_.size(), 1U);
}
