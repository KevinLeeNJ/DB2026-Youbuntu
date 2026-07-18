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

#include <array>
#include <cstring>
#include <limits>
#include <random>

#include "execution/executor_abstract.h"
#include "jit/jit_ir.h"

namespace {

jit::JitTupleLayout single_int_layout(uint32_t offset = 0) {
    ColMeta column{"t", "v", TYPE_INT, static_cast<int>(sizeof(int32_t)), static_cast<int>(offset), false};
    return {offset + static_cast<uint32_t>(sizeof(int32_t)), {column}};
}

Condition int_condition(CompOp op, int value) {
    Condition condition;
    condition.lhs_col = {"t", "v"};
    condition.op = op;
    condition.is_rhs_val = true;
    condition.rhs_val.set_int(value);
    return condition;
}

class PredicateOracleExecutor final : public AbstractExecutor {
public:
    explicit PredicateOracleExecutor(std::vector<ColMeta> columns) : columns_(std::move(columns)) {}

    bool matches(const Condition& condition, const RmRecord& record) {
        return compare(condition, record);
    }

    Rid& rid() override {
        return rid_;
    }

    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto column = get_col(columns_, target);
        return *column;
    }

private:
    std::vector<ColMeta> columns_;
    Rid rid_{};
};

} // namespace

TEST(JitIrTest, ParameterizesLiteralsWithoutChangingTheCodeKey) {
    auto first =
        jit::build_predicate_program(T_SeqScan, {int_condition(OP_GE, 7)}, single_int_layout(), std::nullopt, 101);
    auto second =
        jit::build_predicate_program(T_SeqScan, {int_condition(OP_GE, -19)}, single_int_layout(), std::nullopt, 101);
    ASSERT_TRUE(first) << first.error;
    ASSERT_TRUE(second) << second.error;
    EXPECT_EQ(first.program->key, second.program->key);
    ASSERT_EQ(first.params.values.size(), 1U);
    ASSERT_EQ(second.params.values.size(), 1U);
    EXPECT_EQ(first.params.values[0].int_value, 7);
    EXPECT_EQ(second.params.values[0].int_value, -19);

    Condition wrong_type = int_condition(OP_GE, 7);
    wrong_type.rhs_val.set_float(7.0);
    EXPECT_FALSE(jit::bind_parameters(*first.program, {wrong_type}));
}

TEST(JitIrTest, KeyIncludesPlanLayoutAndCatalogGeneration) {
    auto base =
        jit::build_predicate_program(T_SeqScan, {int_condition(OP_EQ, 1)}, single_int_layout(), std::nullopt, 11);
    auto offset =
        jit::build_predicate_program(T_SeqScan, {int_condition(OP_EQ, 1)}, single_int_layout(8), std::nullopt, 11);
    auto filter =
        jit::build_predicate_program(T_Filter, {int_condition(OP_EQ, 1)}, single_int_layout(), std::nullopt, 11);
    auto generation =
        jit::build_predicate_program(T_SeqScan, {int_condition(OP_EQ, 1)}, single_int_layout(), std::nullopt, 12);
    ASSERT_TRUE(base) << base.error;
    ASSERT_TRUE(offset) << offset.error;
    ASSERT_TRUE(filter) << filter.error;
    ASSERT_TRUE(generation) << generation.error;
    EXPECT_NE(base.program->key, offset.program->key);
    EXPECT_NE(base.program->key, filter.program->key);
    EXPECT_NE(base.program->key, generation.program->key);
}

TEST(JitIrTest, InterpreterMatchesNumericPredicateSemanticsForOneHundredThousandTuples) {
    std::mt19937_64 random(20260718);
    std::uniform_int_distribution<int> values(-1000000, 1000000);
    std::uniform_int_distribution<int> operators(0, 5);
    const std::array<CompOp, 6> all_operators{OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE};
    const auto layout = single_int_layout();
    PredicateOracleExecutor oracle(layout.columns);
    RmRecord record(sizeof(double));

    for (int iteration = 0; iteration < 100000; ++iteration) {
        const int lhs = values(random);
        const int rhs = values(random);
        const CompOp op = all_operators[operators(random)];
        const Condition condition = int_condition(op, rhs);
        auto result = jit::build_predicate_program(T_SeqScan, {condition}, layout, std::nullopt, 77);
        ASSERT_TRUE(result) << result.error;
        std::array<char, sizeof(int32_t)> tuple{};
        std::memcpy(tuple.data(), &lhs, sizeof(lhs));
        std::memcpy(record.data, tuple.data(), tuple.size());
        jit::JitCallFrame frame{tuple.data(),
                                static_cast<uint32_t>(tuple.size()),
                                nullptr,
                                0,
                                result.params.values.data(),
                                static_cast<uint32_t>(result.params.values.size()),
                                false};
        ASSERT_EQ(jit::interpret_predicate(*result.program, &frame), jit::JitStatus::OK);
        EXPECT_EQ(frame.match, oracle.matches(condition, record)) << "iteration=" << iteration;
    }
}

TEST(JitIrTest, InterpreterSupportsStringParametersAndRejectsMalformedPrograms) {
    ColMeta column{"t", "name", TYPE_STRING, 8, 0, false};
    Condition condition;
    condition.lhs_col = {"t", "name"};
    condition.op = OP_LT;
    condition.is_rhs_val = true;
    condition.rhs_val.set_str("zen");
    auto result = jit::build_predicate_program(T_Filter, {condition}, {8, {column}}, std::nullopt, 9);
    ASSERT_TRUE(result) << result.error;
    std::array<char, 8> tuple{};
    std::memcpy(tuple.data(), "ant", 3);
    jit::JitCallFrame frame{tuple.data(),
                            static_cast<uint32_t>(tuple.size()),
                            nullptr,
                            0,
                            result.params.values.data(),
                            static_cast<uint32_t>(result.params.values.size()),
                            false};
    EXPECT_EQ(jit::interpret_predicate(*result.program, &frame), jit::JitStatus::OK);
    EXPECT_TRUE(frame.match);

    jit::JitProgram malformed = *result.program;
    malformed.predicates.front().lhs.offset = 100;
    EXPECT_FALSE(jit::verify_program(malformed));
    EXPECT_EQ(jit::interpret_predicate(malformed, &frame), jit::JitStatus::INVALID_INPUT);
}

TEST(JitIrTest, PreservesStringAndNanComparisonSemantics) {
    ColMeta string_column{"t", "name", TYPE_STRING, 8, 0, false};
    Condition first_string;
    first_string.lhs_col = {"t", "name"};
    first_string.op = OP_EQ;
    first_string.is_rhs_val = true;
    first_string.rhs_val.set_str("a");
    Condition second_string = first_string;
    second_string.rhs_val.set_str("longer");
    auto first = jit::build_predicate_program(T_Filter, {first_string}, {8, {string_column}}, std::nullopt, 10);
    auto second = jit::build_predicate_program(T_Filter, {second_string}, {8, {string_column}}, std::nullopt, 10);
    ASSERT_TRUE(first) << first.error;
    ASSERT_TRUE(second) << second.error;
    EXPECT_EQ(first.program->key, second.program->key);
    Condition too_long = first_string;
    too_long.rhs_val.set_str("much-longer-than-column");
    EXPECT_FALSE(jit::bind_parameters(*first.program, {too_long}));

    ColMeta float_column{"t", "v", TYPE_FLOAT, static_cast<int>(sizeof(double)), 0, false};
    Condition nan_condition;
    nan_condition.lhs_col = {"t", "v"};
    nan_condition.op = OP_NE;
    nan_condition.is_rhs_val = true;
    nan_condition.rhs_val.set_float(std::numeric_limits<double>::quiet_NaN());
    auto nan_program =
        jit::build_predicate_program(T_SeqScan, {nan_condition}, {sizeof(double), {float_column}}, std::nullopt, 10);
    ASSERT_TRUE(nan_program) << nan_program.error;
    const double value = 1.0;
    std::array<char, sizeof(double)> tuple{};
    std::memcpy(tuple.data(), &value, sizeof(value));
    jit::JitCallFrame frame{tuple.data(),
                            static_cast<uint32_t>(tuple.size()),
                            nullptr,
                            0,
                            nan_program.params.values.data(),
                            static_cast<uint32_t>(nan_program.params.values.size()),
                            false};
    EXPECT_EQ(jit::interpret_predicate(*nan_program.program, &frame), jit::JitStatus::OK);
    EXPECT_TRUE(frame.match);
}

TEST(JitIrTest, InterpretsJoinColumnsFromSeparateTuples) {
    ColMeta left_column{"left", "v", TYPE_INT, static_cast<int>(sizeof(int32_t)), 0, false};
    ColMeta right_column{"right", "v", TYPE_INT, static_cast<int>(sizeof(int32_t)), 0, false};
    Condition condition;
    condition.lhs_col = {"left", "v"};
    condition.op = OP_LT;
    condition.is_rhs_val = false;
    condition.rhs_col = {"right", "v"};
    auto program = jit::build_predicate_program(T_NestLoop, {condition}, {sizeof(int32_t), {left_column}},
                                                jit::JitTupleLayout{sizeof(int32_t), {right_column}}, 123);
    ASSERT_TRUE(program) << program.error;
    const int left = 1;
    const int right = 2;
    jit::JitCallFrame frame{reinterpret_cast<const char*>(&left),
                            sizeof(left),
                            reinterpret_cast<const char*>(&right),
                            sizeof(right),
                            nullptr,
                            0,
                            false};
    EXPECT_EQ(jit::interpret_predicate(*program.program, &frame), jit::JitStatus::OK);
    EXPECT_TRUE(frame.match);
}
