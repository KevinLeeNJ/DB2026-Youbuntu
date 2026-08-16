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
    auto sv_val = std::make_unique<ast::FloatLit>(3.14);
    Value val = analyze.convert_sv_value(sv_val.get());
    EXPECT_EQ(val.type, TYPE_FLOAT);
    EXPECT_DOUBLE_EQ(val.float_val, 3.14);
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
            {.tab_name = "t1", .name = "b", .type = TYPE_FLOAT, .len = 8, .offset = 4, .index = false},
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

std::unique_ptr<ast::AggExpr> make_ast_agg(ast::AggFuncType func, const std::string& col_name) {
    return std::make_unique<ast::AggExpr>(func, false, std::make_unique<ast::Col>("", col_name));
}

std::unique_ptr<ast::AggExpr> make_ast_count_star() {
    return std::make_unique<ast::AggExpr>(ast::AGG_COUNT, true, nullptr);
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
                                                  std::vector<std::unique_ptr<ast::HavingExpr>> having_conds = {}) {
    return std::make_unique<ast::SelectStmt>(
        std::move(select_items), std::vector<ast::TableRef>{ast::TableRef("grade", "")},
        std::vector<std::unique_ptr<ast::BinaryExpr>>{}, std::move(group_by_cols), std::move(having_conds),
        std::vector<std::unique_ptr<ast::OrderByItem>>{}, false, 0, false);
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

TEST_F(AnalyzeAggregateTest, do_analyze_accepts_expanded_predicates) {
    const std::vector<std::string> sqls = {
        "select id from grade where id in (1, 3);",
        "select id from grade where id not in (2, 4);",
        "select course from grade where course like 'Data%';",
        "select course from grade where course not like 'C%';",
        "select id from grade where id between 1 and 2;",
        "select id from grade where id not between 1 and 2;",
    };

    for (const auto& sql : sqls) {
        auto parse = ast::parse_sql(sql);
        EXPECT_NO_THROW((void)analyze_.do_analyze(std::move(parse))) << sql;
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_like_on_numeric_column) {
    auto parse = ast::parse_sql("select id from grade where id like '1%';");
    EXPECT_THROW((void)analyze_.do_analyze(std::move(parse)), RMDBError);

    parse = ast::parse_sql("select id from grade where id like course;");
    EXPECT_THROW((void)analyze_.do_analyze(std::move(parse)), RMDBError);
}

TEST_F(AnalyzeAggregateTest, do_analyze_builds_recursive_expression_tree) {
    auto parse = ast::parse_sql("select case when score >= 90 then 'A' else 'F' end as band from grade "
                                "where score is not null and (id = 1 or id = 2);");

    EXPECT_NO_THROW({
        auto query = analyze_.do_analyze(std::move(parse));
        ASSERT_NE(query, nullptr);
        ASSERT_EQ(query->select_items.size(), 1);
        EXPECT_EQ(query->select_items[0].alias, "band");
    });
}

TEST_F(AnalyzeAggregateTest, do_analyze_converts_window_expression) {
    auto parse =
        ast::parse_sql("select row_number() over (partition by course order by score desc) as row_num from grade;");

    EXPECT_NO_THROW({
        auto query = analyze_.do_analyze(std::move(parse));
        ASSERT_NE(query, nullptr);
        ASSERT_EQ(query->select_items.size(), 1);
        EXPECT_EQ(query->select_items[0].alias, "row_num");
        EXPECT_TRUE(query->has_window);
        EXPECT_EQ(query->select_items[0].expr.type, QueryExprType::WINDOW);
        EXPECT_EQ(query->select_items[0].expr.window_partition_by.size(), 1);
        EXPECT_EQ(query->select_items[0].expr.window_order_by.size(), 1);
    });
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_window_expression_in_where) {
    auto parse = ast::parse_sql("select id from grade where row_number() over () > 1;");

    try {
        (void)analyze_.do_analyze(std::move(parse));
        FAIL() << "expected window expression context validation failure";
    } catch (const RMDBError& err) {
        EXPECT_NE(std::string(err.what()).find("Window functions are not allowed in WHERE"), std::string::npos);
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_invalid_window_arguments) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"select sum(course) over () from grade;", "SUM window function requires a numeric expression"},
        {"select lag(score, -1) over () from grade;", "window offset must be non-negative"},
        {"select sum(row_number() over ()) over () from grade;", "nested window functions are not supported"},
    };

    for (const auto& [sql, message] : cases) {
        auto parse = ast::parse_sql(sql);
        try {
            (void)analyze_.do_analyze(std::move(parse));
            FAIL() << "expected window argument validation failure for: " << sql;
        } catch (const RMDBError& err) {
            EXPECT_NE(std::string(err.what()).find(message), std::string::npos) << sql;
        }
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_accepts_scalar_functions) {
    const std::vector<std::string> sqls = {
        "select abs(score), length(course), coalesce(NULL, course, 'unknown') from grade;",
        "select lower(course), upper(course), trim(course), round(score), nullif(score, 0), "
        "char_length(course) from grade;",
        "select id, length(course) as len from grade where abs(score) >= 60 order by len;",
        "select id from grade where score > some (select score from grade);",
        "select id, course from grade order by 2 desc, 1;",
        "update grade set score = abs(score) where id = 1;",
    };

    for (const auto& sql : sqls) {
        auto parse = ast::parse_sql(sql);
        EXPECT_NO_THROW((void)analyze_.do_analyze(std::move(parse))) << sql;
    }

    auto parse = ast::parse_sql("select abs(score) as magnitude, coalesce(NULL, course) as label from grade;");
    auto query = analyze_.do_analyze(std::move(parse));
    ASSERT_NE(query, nullptr);
    ASSERT_EQ(query->select_items.size(), 2);
    EXPECT_EQ(query->select_items[0].expr.type, QueryExprType::SCALAR_FUNCTION);
    EXPECT_EQ(query->select_items[0].expr.scalar_func, ScalarFuncType::ABS);
    EXPECT_EQ(query->output_cols[0].type, TYPE_INT);
    EXPECT_EQ(query->output_cols[1].type, TYPE_STRING);
}

TEST_F(AnalyzeAggregateTest, do_analyze_rejects_invalid_scalar_functions) {
    const std::vector<std::string> sqls = {
        "select abs(course) from grade;",     "select length(score) from grade;",
        "select abs(score, id) from grade;",  "select length() from grade;",
        "select coalesce(score) from grade;", "select coalesce(score, course) from grade;",
        "select lower(score) from grade;",    "select upper(score) from grade;",
        "select trim(score) from grade;",     "select round(course) from grade;",
        "select nullif(score) from grade;",   "select nullif(score, course) from grade;",
        "select id from grade order by 0;",   "select id from grade order by 2;",
    };

    for (const auto& sql : sqls) {
        auto parse = ast::parse_sql(sql);
        EXPECT_THROW((void)analyze_.do_analyze(std::move(parse)), RMDBError) << sql;
    }
}

TEST_F(AnalyzeAggregateTest, do_analyze_distinguishes_order_by_positions_from_constant_expressions) {
    auto positional = analyze_.do_analyze(ast::parse_sql("select id, course from grade order by 2 desc, 1;"));
    ASSERT_EQ(positional->order_by_items.size(), 2);
    EXPECT_EQ(positional->order_by_items[0].expr.type, QueryExprType::COLUMN);
    EXPECT_EQ(positional->order_by_items[0].expr.col.col_name, "course");
    EXPECT_EQ(positional->order_by_items[1].expr.type, QueryExprType::COLUMN);
    EXPECT_EQ(positional->order_by_items[1].expr.col.col_name, "id");

    EXPECT_THROW((void)analyze_.do_analyze(ast::parse_sql("select id, course from grade order by 1 + 1;")), RMDBError);
}
