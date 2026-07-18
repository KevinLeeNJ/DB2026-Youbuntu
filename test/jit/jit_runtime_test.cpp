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

#include "jit/jit_ir.h"
#include "jit/jit_types.h"

namespace {

Condition condition(ColType type, CompOp op, int int_value = 0, double float_value = 0.0,
                    const std::string& string_value = {}) {
    Condition result;
    result.lhs_col = {"t", "v"};
    result.op = op;
    result.is_rhs_val = true;
    if (type == TYPE_INT) {
        result.rhs_val.set_int(int_value);
    } else if (type == TYPE_FLOAT) {
        result.rhs_val.set_float(float_value);
    } else {
        result.rhs_val.set_str(string_value);
    }
    return result;
}

jit::JitBuildResult make_program(ColType lhs_type, int len, const Condition& predicate) {
    ColMeta column{"t", "v", lhs_type, len, 0, false};
    return jit::build_predicate_program(T_SeqScan, {predicate}, {static_cast<uint32_t>(len), {column}}, std::nullopt,
                                        1);
}

void expect_same_result(jit::JitRuntime& runtime, jit::JitBuildResult result, const char* tuple, uint32_t tuple_len) {
    ASSERT_TRUE(result) << result.error;
    auto compiled = runtime.compile_predicate(*result.program);
    ASSERT_TRUE(compiled) << compiled.error;
    jit::JitCallFrame interpreted{
        tuple, tuple_len, nullptr, 0, result.params.values.data(), static_cast<uint32_t>(result.params.values.size()),
        false};
    jit::JitCallFrame generated = interpreted;
    ASSERT_EQ(jit::interpret_predicate(*result.program, &interpreted), jit::JitStatus::OK);
    ASSERT_EQ(compiled.code.invoke_predicate(&generated), jit::JitStatus::OK);
    EXPECT_EQ(generated.match, interpreted.match);
}

} // namespace

TEST(JitRuntimeTest, CompilesAndReleasesParameterizedAddFunction) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());

    for (int i = 0; i < 100; ++i) {
        auto result = runtime.compile_test_add_i32();
        ASSERT_TRUE(result) << result.error;
        EXPECT_GT(result.code.code_size(), 0U);
        EXPECT_EQ(result.code.test_add_i32(i, -3), i - 3);
        EXPECT_EQ(runtime.active_code_count(), 1U);
    }

    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(JitRuntimeTest, ReportsForcedFailuresWithoutPublishingCode) {
    jit::JitRuntime runtime;

    jit::JitCompileOptions compile_options;
    compile_options.force_compile_failure = true;
    auto compile_failure = runtime.compile_test_add_i32(compile_options);
    EXPECT_EQ(compile_failure.status, jit::JitStatus::COMPILE_ERROR);
    EXPECT_FALSE(compile_failure.code);

    jit::JitCompileOptions allocation_options;
    allocation_options.force_allocation_failure = true;
    auto allocation_failure = runtime.compile_test_add_i32(allocation_options);
    EXPECT_EQ(allocation_failure.status, jit::JitStatus::ALLOCATION_ERROR);
    EXPECT_FALSE(allocation_failure.code);
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(JitRuntimeTest, UnsupportedArchitectureFallsBackWithoutCode) {
    jit::JitRuntimeOptions options;
    options.force_unsupported_architecture = true;
    jit::JitRuntime runtime(options);
    EXPECT_FALSE(runtime.is_supported());

    auto result = runtime.compile_test_add_i32();
    EXPECT_EQ(result.status, jit::JitStatus::UNSUPPORTED_ARCHITECTURE);
    EXPECT_FALSE(result.code);
    EXPECT_EQ(runtime.active_code_count(), 0U);
}

TEST(JitRuntimeTest, PredicateCodeMatchesInterpreterForNumericOperatorsAndNan) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    const std::array<CompOp, 6> operators{OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE};
    const int lhs = 7;
    for (CompOp op : operators) {
        expect_same_result(runtime, make_program(TYPE_INT, sizeof(int), condition(TYPE_INT, op, 7)),
                           reinterpret_cast<const char*>(&lhs), sizeof(lhs));
        expect_same_result(runtime, make_program(TYPE_INT, sizeof(int), condition(TYPE_FLOAT, op, 7, 7.5)),
                           reinterpret_cast<const char*>(&lhs), sizeof(lhs));
    }

    const double nan = std::numeric_limits<double>::quiet_NaN();
    expect_same_result(runtime, make_program(TYPE_FLOAT, sizeof(double), condition(TYPE_FLOAT, OP_EQ, 0, nan)),
                       reinterpret_cast<const char*>(&nan), sizeof(nan));
    expect_same_result(runtime, make_program(TYPE_FLOAT, sizeof(double), condition(TYPE_FLOAT, OP_NE, 0, nan)),
                       reinterpret_cast<const char*>(&nan), sizeof(nan));
}

TEST(JitRuntimeTest, PredicateCodeUsesStringHelperWithFixedWidthSemantics) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    std::array<char, 8> tuple{};
    std::memcpy(tuple.data(), "ant", 3);
    expect_same_result(runtime, make_program(TYPE_STRING, tuple.size(), condition(TYPE_STRING, OP_LT, 0, 0.0, "zen")),
                       tuple.data(), static_cast<uint32_t>(tuple.size()));

    std::array<char, 8> no_terminator{'z', 'e', 'n', 'x', 'y', 'z', 'a', 'b'};
    expect_same_result(runtime,
                       make_program(TYPE_STRING, no_terminator.size(), condition(TYPE_STRING, OP_GT, 0, 0.0, "zen")),
                       no_terminator.data(), static_cast<uint32_t>(no_terminator.size()));
}

TEST(JitRuntimeTest, PredicateCodeDifferentiallyMatchesInterpreterForRandomTuplesAndJoinSource) {
    jit::JitRuntime runtime;
    ASSERT_TRUE(runtime.is_supported());
    const std::array<CompOp, 6> operators{OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE};
    std::mt19937 random(20260718);
    std::uniform_int_distribution<int> values(-1000000, 1000000);
    for (CompOp op : operators) {
        auto result = make_program(TYPE_INT, sizeof(int), condition(TYPE_INT, op, 17));
        ASSERT_TRUE(result) << result.error;
        auto compiled = runtime.compile_predicate(*result.program);
        ASSERT_TRUE(compiled) << compiled.error;
        for (int iteration = 0; iteration < 10000; ++iteration) {
            const int value = values(random);
            jit::JitCallFrame interpreted{reinterpret_cast<const char*>(&value),
                                          sizeof(value),
                                          nullptr,
                                          0,
                                          result.params.values.data(),
                                          static_cast<uint32_t>(result.params.values.size()),
                                          false};
            jit::JitCallFrame generated = interpreted;
            ASSERT_EQ(jit::interpret_predicate(*result.program, &interpreted), jit::JitStatus::OK);
            ASSERT_EQ(compiled.code.invoke_predicate(&generated), jit::JitStatus::OK);
            EXPECT_EQ(generated.match, interpreted.match) << "operation=" << op << " iteration=" << iteration;
        }
    }

    ColMeta left_column{"left", "v", TYPE_INT, static_cast<int>(sizeof(int)), 0, false};
    ColMeta right_column{"right", "v", TYPE_INT, static_cast<int>(sizeof(int)), 0, false};
    Condition join;
    join.lhs_col = {"left", "v"};
    join.op = OP_LT;
    join.is_rhs_val = false;
    join.rhs_col = {"right", "v"};
    auto join_result = jit::build_predicate_program(T_NestLoop, {join}, {sizeof(int), {left_column}},
                                                    jit::JitTupleLayout{sizeof(int), {right_column}}, 1);
    ASSERT_TRUE(join_result) << join_result.error;
    auto join_code = runtime.compile_predicate(*join_result.program);
    ASSERT_TRUE(join_code) << join_code.error;
    const int left = 3;
    const int right = 9;
    jit::JitCallFrame frame{reinterpret_cast<const char*>(&left),
                            sizeof(left),
                            reinterpret_cast<const char*>(&right),
                            sizeof(right),
                            nullptr,
                            0,
                            false};
    EXPECT_EQ(join_code.code.invoke_predicate(&frame), jit::JitStatus::OK);
    EXPECT_TRUE(frame.match);
}
