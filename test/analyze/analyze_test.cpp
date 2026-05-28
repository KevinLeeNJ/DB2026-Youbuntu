#undef NDEBUG

#define private public
#include "analyze/analyze.h"
#undef private

#include <memory>
#include <string>
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
    auto sv_val = std::make_shared<ast::IntLit>(42);
    Value val = analyze.convert_sv_value(sv_val);
    EXPECT_EQ(val.type, TYPE_INT);
    EXPECT_EQ(val.int_val, 42);
}

TEST(AnalyzeConvertTest, convert_float_lit) {
    Analyze analyze(nullptr);
    auto sv_val = std::make_shared<ast::FloatLit>(3.14f);
    Value val = analyze.convert_sv_value(sv_val);
    EXPECT_EQ(val.type, TYPE_FLOAT);
    EXPECT_EQ(val.float_val, 3.14f);
}

TEST(AnalyzeConvertTest, convert_string_lit) {
    Analyze analyze(nullptr);
    auto sv_val = std::make_shared<ast::StringLit>("hello");
    Value val = analyze.convert_sv_value(sv_val);
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
    auto lhs = std::make_shared<ast::Col>("t", "x");
    auto rhs = std::make_shared<ast::IntLit>(10);
    auto expr = std::make_shared<ast::BinaryExpr>(lhs, ast::SV_OP_EQ, rhs);
    std::vector<std::shared_ptr<ast::BinaryExpr>> sv_conds = {expr};

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
    auto lhs = std::make_shared<ast::Col>("t1", "x");
    auto rhs = std::make_shared<ast::Col>("t2", "y");
    auto expr = std::make_shared<ast::BinaryExpr>(lhs, ast::SV_OP_GT, rhs);
    std::vector<std::shared_ptr<ast::BinaryExpr>> sv_conds = {expr};

    std::vector<Condition> conds;
    analyze.get_clause(sv_conds, conds);

    ASSERT_EQ(conds.size(), 1);
    EXPECT_EQ(conds[0].lhs_col.col_name, "x");
    EXPECT_EQ(conds[0].op, OP_GT);
    EXPECT_FALSE(conds[0].is_rhs_val);
    EXPECT_EQ(conds[0].rhs_col.tab_name, "t2");
    EXPECT_EQ(conds[0].rhs_col.col_name, "y");
}
