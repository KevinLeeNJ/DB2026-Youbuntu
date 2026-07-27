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
#include "analyze/analyze.h"
#undef private

#include <memory>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/common.h"
#include "errors.h"
#include "gtest/gtest.h"
#include "parser/ast.h"

// =============================================================================
// convert_sv_value 测试
//
// NOTE: 所有测试都传入 nullptr 作为 SmManager*，因为当前测试只调用不依赖
// sm_manager_ 的方法（convert_sv_value, convert_sv_comp_op, check_column,
// get_clause）。如果需要测试 do_analyze 或 check_clause，需要 mock SmManager
// 来提供表和列的元数据。
// =============================================================================

TEST(AnalyzeConvertTest, convert_int_lit) {
    Analyze analyze(nullptr);
    auto sv_val = std::make_unique<ast::IntLit>(42);
    Value val = analyze.convert_sv_value(sv_val.get());
    EXPECT_EQ(val.type, TYPE_INT);
    EXPECT_EQ(val.int_val, 42);
}

TEST(AnalyzeConvertTest, convert_float_lit) {
    Analyze analyze(nullptr);
    auto sv_val = std::make_unique<ast::FloatLit>(3.14f);
    Value val = analyze.convert_sv_value(sv_val.get());
    EXPECT_EQ(val.type, TYPE_FLOAT);
    EXPECT_FLOAT_EQ(val.float_val, 3.14f);
}

TEST(AnalyzeConvertTest, rejects_non_finite_float_values) {
    Analyze analyze(nullptr);
    auto infinity = std::make_unique<ast::FloatLit>(std::numeric_limits<float>::infinity());
    auto nan = std::make_unique<ast::FloatLit>(std::numeric_limits<float>::quiet_NaN());
    EXPECT_THROW((void)analyze.convert_sv_value(infinity.get()), RMDBError);
    EXPECT_THROW((void)analyze.convert_sv_value(nan.get()), RMDBError);
}

TEST(AnalyzeConvertTest, convert_string_lit) {
    Analyze analyze(nullptr);
    auto sv_val = std::make_unique<ast::StringLit>("hello");
    Value val = analyze.convert_sv_value(sv_val.get());
    EXPECT_EQ(val.type, TYPE_STRING);
    EXPECT_EQ(val.str_val, "hello");
}

// =============================================================================
// convert_sv_comp_op 测试
// =============================================================================

TEST(AnalyzeConvertTest, convert_all_comp_ops) {
    Analyze analyze(nullptr);
    EXPECT_EQ(analyze.convert_sv_comp_op(ast::SV_OP_EQ), OP_EQ);
    EXPECT_EQ(analyze.convert_sv_comp_op(ast::SV_OP_NE), OP_NE);
    EXPECT_EQ(analyze.convert_sv_comp_op(ast::SV_OP_LT), OP_LT);
    EXPECT_EQ(analyze.convert_sv_comp_op(ast::SV_OP_GT), OP_GT);
    EXPECT_EQ(analyze.convert_sv_comp_op(ast::SV_OP_LE), OP_LE);
    EXPECT_EQ(analyze.convert_sv_comp_op(ast::SV_OP_GE), OP_GE);
}

// =============================================================================
// check_column 测试
// =============================================================================

class AnalyzeCheckColumnTest : public ::testing::Test {
protected:
    Analyze analyze_{nullptr};

    std::vector<ColMeta> make_cols() {
        return {
            {.tab_name = "t1", .name = "a", .type = TYPE_INT, .len = 4, .offset = 0, .index = false},
            {.tab_name = "t1", .name = "b", .type = TYPE_FLOAT, .len = 4, .offset = 4, .index = false},
            {.tab_name = "t2", .name = "a", .type = TYPE_INT, .len = 4, .offset = 0, .index = false},
        };
    }
};

TEST_F(AnalyzeCheckColumnTest, infer_table_name_success) {
    auto cols = make_cols();
    TabCol target = {.tab_name = "", .col_name = "b"};
    TabCol result = analyze_.check_column(cols, target);
    EXPECT_EQ(result.tab_name, "t1");
    EXPECT_EQ(result.col_name, "b");
}

TEST_F(AnalyzeCheckColumnTest, infer_table_name_not_found) {
    auto cols = make_cols();
    TabCol target = {.tab_name = "", .col_name = "nonexistent"};
    EXPECT_THROW(analyze_.check_column(cols, target), ColumnNotFoundError);
}

TEST_F(AnalyzeCheckColumnTest, infer_table_name_ambiguous) {
    auto cols = make_cols();
    TabCol target = {.tab_name = "", .col_name = "a"}; // 两个表都有列a
    EXPECT_THROW(analyze_.check_column(cols, target), AmbiguousColumnError);
}

TEST_F(AnalyzeCheckColumnTest, explicit_tab_and_col_exists) {
    auto cols = make_cols();
    TabCol target = {.tab_name = "t1", .col_name = "a"};
    // 列存在于指定表中，不应抛异常
    EXPECT_NO_THROW(analyze_.check_column(cols, target));
}

TEST_F(AnalyzeCheckColumnTest, explicit_tab_but_column_not_found) {
    // P1-4: 当前实现中，显式指定tab_name时没有校验列是否存在
    // 这个测试验证修复后应抛出 ColumnNotFoundError
    auto cols = make_cols();
    TabCol target = {.tab_name = "t1", .col_name = "nonexistent"};
    EXPECT_THROW(analyze_.check_column(cols, target), ColumnNotFoundError);
}

// =============================================================================
// get_clause 测试
// =============================================================================

TEST(AnalyzeGetClauseTest, single_cond_with_value) {
    Analyze analyze(nullptr);
    std::vector<std::unique_ptr<ast::BinaryExpr>> sv_conds;
    sv_conds.push_back(std::make_unique<ast::BinaryExpr>(std::make_unique<ast::Col>("t", "x"), ast::SV_OP_EQ,
                                                         std::make_unique<ast::IntLit>(10)));

    std::vector<Condition> conds;
    analyze.get_clause(sv_conds, conds);

    ASSERT_EQ(conds.size(), 1);
    EXPECT_EQ(conds[0].lhs_col.tab_name, "t");
    EXPECT_EQ(conds[0].lhs_col.col_name, "x");
    EXPECT_EQ(conds[0].op, OP_EQ);
    EXPECT_TRUE(conds[0].is_rhs_val);
    EXPECT_EQ(conds[0].rhs_val.type, TYPE_INT);
    EXPECT_EQ(conds[0].rhs_val.int_val, 10);
}

TEST(AnalyzeGetClauseTest, single_cond_with_col) {
    Analyze analyze(nullptr);
    std::vector<std::unique_ptr<ast::BinaryExpr>> sv_conds;
    sv_conds.push_back(std::make_unique<ast::BinaryExpr>(std::make_unique<ast::Col>("t1", "x"), ast::SV_OP_GT,
                                                         std::make_unique<ast::Col>("t2", "y")));

    std::vector<Condition> conds;
    analyze.get_clause(sv_conds, conds);

    ASSERT_EQ(conds.size(), 1);
    EXPECT_EQ(conds[0].lhs_col.col_name, "x");
    EXPECT_EQ(conds[0].op, OP_GT);
    EXPECT_FALSE(conds[0].is_rhs_val);
    EXPECT_EQ(conds[0].rhs_col.tab_name, "t2");
    EXPECT_EQ(conds[0].rhs_col.col_name, "y");
}

namespace {

TabMeta make_grade_tab() {
    TabMeta tab;
    tab.name = "grade";
    tab.cols = {
        {.tab_name = "grade", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false},
        {.tab_name = "grade", .name = "course", .type = TYPE_STRING, .len = 16, .offset = 4, .index = false},
        {.tab_name = "grade", .name = "score", .type = TYPE_INT, .len = 4, .offset = 20, .index = false},
    };
    return tab;
}

TabMeta make_update_tab() {
    TabMeta tab;
    tab.name = "t";
    tab.cols = {
        {.tab_name = "t", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false},
        {.tab_name = "t", .name = "f", .type = TYPE_FLOAT, .len = 4, .offset = 4, .index = false},
        {.tab_name = "t", .name = "s", .type = TYPE_STRING, .len = 8, .offset = 8, .index = false},
    };
    return tab;
}

std::unique_ptr<ast::AggExpr> make_ast_agg(ast::AggFuncType func, const std::string& col_name) {
    return std::make_unique<ast::AggExpr>(func, false, std::make_unique<ast::Col>("", col_name));
}

std::unique_ptr<ast::AggExpr> make_ast_count_star() {
    return std::make_unique<ast::AggExpr>(ast::AGG_COUNT, true, nullptr);
}

std::unique_ptr<ast::AggExpr> make_ast_count_distinct(const std::string& col_name) {
    return std::make_unique<ast::AggExpr>(ast::AGG_COUNT, false, true, std::make_unique<ast::Col>("", col_name));
}

template <typename... Items> std::vector<std::unique_ptr<ast::SelectItem>> select_items(Items&&... items) {
    std::vector<std::unique_ptr<ast::SelectItem>> result;
    (result.push_back(std::forward<Items>(items)), ...);
    return result;
}

template <typename... Cols> std::vector<std::unique_ptr<ast::Col>> group_cols(Cols&&... cols) {
    std::vector<std::unique_ptr<ast::Col>> result;
    (result.push_back(std::forward<Cols>(cols)), ...);
    return result;
}

template <typename... Conds> std::vector<std::unique_ptr<ast::HavingExpr>> having_conds(Conds&&... conds) {
    std::vector<std::unique_ptr<ast::HavingExpr>> result;
    (result.push_back(std::forward<Conds>(conds)), ...);
    return result;
}

std::unique_ptr<ast::SelectStmt> make_select_stmt(std::vector<std::unique_ptr<ast::SelectItem>> select_items,
                                                  std::vector<std::unique_ptr<ast::Col>> group_by_cols = {},
                                                  std::vector<std::unique_ptr<ast::HavingExpr>> having_conds = {},
                                                  std::vector<std::unique_ptr<ast::OrderByItem>> order_by_items = {}) {
    return std::make_unique<ast::SelectStmt>(std::move(select_items),
                                             std::vector<ast::TableRef>{ast::TableRef("grade", "")},
                                             std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::move(group_by_cols),
                                             std::move(having_conds), std::move(order_by_items), false, 0, false);
}

std::vector<std::unique_ptr<ast::OrderByItem>> order_by_col(const std::string& col_name) {
    std::vector<std::unique_ptr<ast::OrderByItem>> items;
    items.push_back(std::make_unique<ast::OrderByItem>(std::make_unique<ast::Col>("", col_name), ast::OrderBy_DEFAULT));
    return items;
}

} // namespace

class AnalyzeAggregateTest : public ::testing::Test {
protected:
    SmManager sm_manager_{nullptr, nullptr, nullptr, nullptr};
    Analyze analyze_{&sm_manager_};

    void SetUp() override {
        sm_manager_.db_.SetTabMeta("grade", make_grade_tab());
    }
};

class AnalyzeUpdateTest : public ::testing::Test {
protected:
    SmManager sm_manager_{nullptr, nullptr, nullptr, nullptr};
    Analyze analyze_{&sm_manager_};

    void SetUp() override {
        sm_manager_.db_.SetTabMeta("t", make_update_tab());
    }

    std::unique_ptr<ast::TreeNode> bind(const std::string& sql, std::vector<std::unique_ptr<ast::Value>> values) {
        auto template_tree = ast::parse_sql(sql);
        return ast::clone_bound_tree(*template_tree, values);
    }
};

TEST_F(AnalyzeUpdateTest, accepts_numeric_prepared_self_update_and_resolves_columns) {
    std::vector<std::unique_ptr<ast::Value>> values;
    values.push_back(std::make_unique<ast::FloatLit>(0.0F));
    values.push_back(std::make_unique<ast::IntLit>(0));

    auto query = analyze_.do_analyze(bind("UPDATE t SET f = f + $1 WHERE id = $2;", std::move(values)));

    ASSERT_EQ(query->set_clauses.size(), 1U);
    EXPECT_EQ(query->set_clauses[0].lhs.tab_name, "t");
    EXPECT_EQ(query->set_clauses[0].lhs.col_name, "f");
    EXPECT_EQ(query->set_clauses[0].rhs_col.tab_name, "t");
    EXPECT_EQ(query->set_clauses[0].rhs_col.col_name, "f");
    EXPECT_EQ(query->set_clauses[0].rhs.type, TYPE_FLOAT);
    EXPECT_EQ(query->conds.size(), 1U);
}

TEST_F(AnalyzeUpdateTest, rejects_char_parameter_for_prepared_numeric_delta) {
    std::vector<std::unique_ptr<ast::Value>> values;
    values.push_back(std::make_unique<ast::StringLit>(""));
    values.push_back(std::make_unique<ast::IntLit>(0));

    EXPECT_THROW((void)analyze_.do_analyze(bind("UPDATE t SET f = f + $1 WHERE id = $2;", std::move(values))),
                 IncompatibleTypeError);
}

TEST_F(AnalyzeUpdateTest, accepts_direct_column_assignment_and_resolves_both_columns) {
    auto query = analyze_.do_analyze(ast::parse_sql("UPDATE t SET f = id WHERE id = (1 + 1);"));

    ASSERT_EQ(query->set_clauses.size(), 1U);
    const auto& clause = query->set_clauses[0];
    EXPECT_EQ(clause.lhs.tab_name, "t");
    EXPECT_EQ(clause.lhs.col_name, "f");
    EXPECT_TRUE(clause.is_self_ref);
    EXPECT_EQ(clause.op, UpdateOp::ASSIGNMENT);
    EXPECT_EQ(clause.rhs_col.tab_name, "t");
    EXPECT_EQ(clause.rhs_col.col_name, "id");
    ASSERT_EQ(query->conds.size(), 1U);
    EXPECT_TRUE(query->conds[0].is_rhs_val);
    EXPECT_EQ(query->conds[0].rhs_val.type, TYPE_INT);
    EXPECT_EQ(query->conds[0].rhs_val.int_val, 2);
}

TEST_F(AnalyzeUpdateTest, accepts_chained_target_column_terms_and_rejects_ambiguous_or_incompatible_terms) {
    auto query = analyze_.do_analyze(ast::parse_sql("UPDATE t SET f = f - 1 + 91 WHERE id = 2;"));
    ASSERT_EQ(query->set_clauses.size(), 1U);
    const auto& clause = query->set_clauses[0];
    EXPECT_EQ(clause.op, UpdateOp::SELF_SUB);
    ASSERT_EQ(clause.additional_terms.size(), 1U);
    EXPECT_EQ(clause.additional_terms[0].op, UpdateOp::SELF_ADD);
    EXPECT_EQ(clause.additional_terms[0].rhs.type, TYPE_INT);
    EXPECT_EQ(clause.additional_terms[0].rhs.int_val, 91);

    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("UPDATE t SET f = id + 1 + 2 WHERE id = 2;")), RMDBError);

    std::vector<std::unique_ptr<ast::Value>> values;
    values.push_back(std::make_unique<ast::StringLit>("bad"));
    EXPECT_THROW((void)analyze_.do_analyze(bind("UPDATE t SET f = f - 1 + $1 WHERE id = 2;", std::move(values))),
                 IncompatibleTypeError);
}

TEST_F(AnalyzeUpdateTest, rejects_missing_lhs_and_non_numeric_self_update_columns) {
    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("UPDATE t SET missing = 1 WHERE id = 1;")),
                 ColumnNotFoundError);
    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("UPDATE t SET s = s + 1 WHERE id = 1;")),
                 IncompatibleTypeError);
    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("UPDATE t SET f = s + 1 WHERE id = 1;")),
                 IncompatibleTypeError);
}

TEST_F(AnalyzeUpdateTest, rejects_incompatible_plain_assignment_during_analysis) {
    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("UPDATE t SET f = 'bad' WHERE id = 1;")),
                 IncompatibleTypeError);
    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("UPDATE t SET s = f WHERE id = 1;")), IncompatibleTypeError);
}

TEST_F(AnalyzeAggregateTest, do_analyze_group_by_having_success) {
    auto stmt = make_select_stmt(
        select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "id"), ""),
                     std::make_unique<ast::SelectItem>(make_ast_agg(ast::AGG_MAX, "score"), "max_score")),
        group_cols(std::make_unique<ast::Col>("", "id")),
        having_conds(
            std::make_unique<ast::HavingExpr>(make_ast_count_star(), ast::SV_OP_GT, std::make_unique<ast::IntLit>(1))));

    auto query = analyze_.do_analyze(std::move(stmt));

    ASSERT_EQ(query->select_items.size(), 2);
    EXPECT_TRUE(query->has_aggregate);
    EXPECT_EQ(query->group_by_cols.size(), 1);
    EXPECT_EQ(query->group_by_cols[0].tab_name, "grade");
    EXPECT_EQ(query->group_by_cols[0].col_name, "id");
    EXPECT_EQ(query->select_items[0].expr.col.tab_name, "grade");
    EXPECT_EQ(query->select_items[1].expr.agg.col.tab_name, "grade");
    EXPECT_EQ(query->select_items[1].expr.agg.display_name, "MAX(score)");
    ASSERT_EQ(query->having_conds.size(), 1);
    EXPECT_EQ(query->having_conds[0].lhs.type, QueryExprType::AGGREGATE);
    EXPECT_EQ(query->having_conds[0].lhs.agg.display_name, "COUNT(*)");
    EXPECT_EQ(query->output_names, (std::vector<std::string>{"id", "max_score"}));
}

TEST_F(AnalyzeAggregateTest, do_analyze_preserves_count_distinct_and_resolves_column) {
    auto stmt = make_select_stmt(select_items(std::make_unique<ast::SelectItem>(make_ast_count_distinct("id"), "")));

    auto query = analyze_.do_analyze(std::move(stmt));

    ASSERT_EQ(query->select_items.size(), 1);
    const auto& aggregate = query->select_items[0].expr.agg;
    EXPECT_EQ(aggregate.type, AggType::COUNT);
    EXPECT_TRUE(aggregate.is_distinct);
    EXPECT_FALSE(aggregate.is_star);
    EXPECT_EQ(aggregate.col.tab_name, "grade");
    EXPECT_EQ(aggregate.display_name, "COUNT(DISTINCT id)");
    EXPECT_EQ(query->output_names, (std::vector<std::string>{"COUNT(DISTINCT id)"}));
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_select_column_not_in_group_by) {
    auto stmt =
        make_select_stmt(select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "course"), ""),
                                      std::make_unique<ast::SelectItem>(make_ast_agg(ast::AGG_MAX, "score"), "")),
                         group_cols(std::make_unique<ast::Col>("", "id")));

    try {
        (void)analyze_.do_analyze(std::move(stmt));
        FAIL() << "expected group-by validation failure";
    } catch (const RMDBError& err) {
        EXPECT_NE(std::string(err.what()).find("SELECT list contains a non-aggregated column that is not in GROUP BY"),
                  std::string::npos);
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_having_column_not_in_group_by) {
    auto stmt = make_select_stmt(
        select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "id"), ""),
                     std::make_unique<ast::SelectItem>(make_ast_agg(ast::AGG_MAX, "score"), "")),
        group_cols(std::make_unique<ast::Col>("", "id")),
        having_conds(std::make_unique<ast::HavingExpr>(std::make_unique<ast::Col>("", "score"), ast::SV_OP_GT,
                                                       std::make_unique<ast::IntLit>(90))));

    try {
        (void)analyze_.do_analyze(std::move(stmt));
        FAIL() << "expected having validation failure";
    } catch (const RMDBError& err) {
        EXPECT_NE(std::string(err.what()).find("HAVING contains a non-aggregated column that is not in GROUP BY"),
                  std::string::npos);
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_mixed_aggregate_without_group_by) {
    auto stmt =
        make_select_stmt(select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "id"), ""),
                                      std::make_unique<ast::SelectItem>(make_ast_agg(ast::AGG_MAX, "score"), "")));

    try {
        (void)analyze_.do_analyze(std::move(stmt));
        FAIL() << "expected aggregate mixing validation failure";
    } catch (const RMDBError& err) {
        EXPECT_NE(std::string(err.what())
                      .find("SELECT list cannot mix aggregate and non-aggregate columns without "
                            "GROUP BY"),
                  std::string::npos);
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_accepts_order_by_column_outside_select_list) {
    auto stmt =
        make_select_stmt(select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "id"), "")), {},
                         {}, order_by_col("score"));

    auto query = analyze_.do_analyze(std::move(stmt));

    ASSERT_EQ(query->order_by_items.size(), 1);
    EXPECT_EQ(query->order_by_items[0].expr.type, QueryExprType::COLUMN);
    EXPECT_EQ(query->order_by_items[0].expr.col.tab_name, "grade");
    EXPECT_EQ(query->order_by_items[0].expr.col.col_name, "score");
    EXPECT_EQ(query->output_names, (std::vector<std::string>{"id"}));
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_order_by_column_outside_select_list_when_grouped) {
    auto stmt =
        make_select_stmt(select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "id"), "")),
                         group_cols(std::make_unique<ast::Col>("", "id")), {}, order_by_col("score"));

    try {
        (void)analyze_.do_analyze(std::move(stmt));
        FAIL() << "expected order-by validation failure";
    } catch (const RMDBError& err) {
        EXPECT_NE(std::string(err.what()).find("ORDER BY must reference output columns or aliases"), std::string::npos);
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_order_by_unknown_column) {
    auto stmt =
        make_select_stmt(select_items(std::make_unique<ast::SelectItem>(std::make_unique<ast::Col>("", "id"), "")), {},
                         {}, order_by_col("nosuchcol"));

    EXPECT_THROW((void)analyze_.do_analyze(std::move(stmt)), ColumnNotFoundError);
}
