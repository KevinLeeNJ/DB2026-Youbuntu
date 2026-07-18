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

#include "jit/jit_types.h"

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
