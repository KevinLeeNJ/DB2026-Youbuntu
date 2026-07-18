/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <gtest/gtest.h>

#include <array>

#include "jit/jit_aggregate.h"

TEST(JitAggregateTest, UpdatesMultipleNumericAggregatesWithIntegerAndFloatInputs) {
    const std::vector<jit::AggregateDescriptor> descriptors{
        {jit::AggregateOp::COUNT, TYPE_INT, 0, sizeof(int), true},
        {jit::AggregateOp::SUM, TYPE_INT, 0, sizeof(int), false},
        {jit::AggregateOp::MIN, TYPE_INT, 0, sizeof(int), false},
        {jit::AggregateOp::MAX, TYPE_INT, 0, sizeof(int), false},
        {jit::AggregateOp::AVG, TYPE_INT, 0, sizeof(int), false},
    };
    jit::AggregateKernel kernel(descriptors);
    ASSERT_TRUE(kernel.valid());
    for (int value : {-4, 7, 12}) {
        ASSERT_EQ(kernel.update(reinterpret_cast<const char*>(&value), sizeof(value)), jit::JitStatus::OK);
    }
    const auto& slots = kernel.slots();
    ASSERT_EQ(slots.size(), descriptors.size());
    EXPECT_EQ(slots[0].count, 3);
    EXPECT_DOUBLE_EQ(slots[1].sum, 15.0);
    EXPECT_TRUE(slots[2].has_value);
    EXPECT_DOUBLE_EQ(slots[2].value, -4.0);
    EXPECT_DOUBLE_EQ(slots[3].value, 12.0);
    EXPECT_EQ(slots[4].count, 3);
    EXPECT_DOUBLE_EQ(slots[4].sum, 15.0);
}

TEST(JitAggregateTest, RejectsMalformedTupleWithoutChangingState) {
    jit::AggregateKernel kernel({{jit::AggregateOp::SUM, TYPE_FLOAT, 4, sizeof(double), false}});
    ASSERT_TRUE(kernel.valid());
    std::array<char, 4> tuple{};
    EXPECT_EQ(kernel.update(tuple.data(), tuple.size()), jit::JitStatus::INVALID_INPUT);
    EXPECT_EQ(kernel.slots()[0].count, 0);
    EXPECT_DOUBLE_EQ(kernel.slots()[0].sum, 0.0);
}
