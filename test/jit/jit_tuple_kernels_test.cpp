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
#include <cstring>
#include <limits>

#include "execution/execution_common.h"
#include "jit/jit_tuple_kernels.h"

namespace {

TabMeta tuple_table() {
    TabMeta table;
    table.name = "t";
    table.cols = {
        {"t", "i", TYPE_INT, 4, 0, false}, {"t", "f", TYPE_FLOAT, 8, 4, false}, {"t", "s", TYPE_STRING, 6, 12, false}};
    return table;
}

SetClause self_clause(UpdateOp op, ColType type, int integer, double floating) {
    SetClause clause;
    clause.lhs = {"t", "i"};
    clause.rhs_col = {"t", "i"};
    clause.is_self_ref = true;
    clause.op = op;
    if (type == TYPE_INT) {
        clause.rhs.set_int(integer);
    } else {
        clause.rhs.set_float(floating);
    }
    return clause;
}

} // namespace

TEST(JitTupleKernelTest, ProjectionPreservesReorderRepeatAndAdjacentSpans) {
    const std::vector<ColMeta> input{
        {"t", "a", TYPE_INT, 4, 0, false}, {"t", "b", TYPE_INT, 4, 4, false}, {"t", "c", TYPE_STRING, 4, 8, false}};
    std::vector<ColMeta> output{{"", "b", TYPE_INT, 4, 0, false},
                                {"", "a", TYPE_INT, 4, 4, false},
                                {"", "a2", TYPE_INT, 4, 8, false},
                                {"", "c", TYPE_STRING, 4, 12, false}};
    jit::ProjectionKernel kernel(output, {1, 0, 0, 2}, input);
    ASSERT_TRUE(kernel.valid());
    std::array<char, 12> source{};
    std::array<char, 16> destination{};
    write_unaligned(source.data(), 3);
    write_unaligned(source.data() + 4, -9);
    std::memcpy(source.data() + 8, "xy", 2);
    kernel.project(source.data(), destination.data());
    EXPECT_EQ(read_unaligned<int>(destination.data()), -9);
    EXPECT_EQ(read_unaligned<int>(destination.data() + 4), 3);
    EXPECT_EQ(read_unaligned<int>(destination.data() + 8), 3);
    EXPECT_EQ(std::memcmp(destination.data() + 12, "xy\0\0", 4), 0);

    std::vector<ColMeta> adjacent{{"", "a", TYPE_INT, 4, 0, false}, {"", "b", TYPE_INT, 4, 4, false}};
    jit::ProjectionKernel merged(adjacent, {0, 1}, input);
    std::array<char, 8> compact{};
    merged.project(source.data(), compact.data());
    EXPECT_EQ(std::memcmp(compact.data(), source.data(), compact.size()), 0);
}

TEST(JitTupleKernelTest, UpdateMatchesArithmeticCastStringAndDivisionRules) {
    const TabMeta table = tuple_table();
    std::array<char, 18> old_record{};
    write_unaligned(old_record.data(), -7);
    write_unaligned(old_record.data() + 4, 2.5);
    std::memcpy(old_record.data() + 12, "abcdef", 6);
    std::array<char, 18> updated = old_record;

    SetClause float_assignment;
    float_assignment.lhs = {"t", "f"};
    float_assignment.rhs_col = {"t", "i"};
    float_assignment.is_self_ref = true;
    float_assignment.op = UpdateOp::ASSIGNMENT;
    SetClause string_assignment;
    string_assignment.lhs = {"t", "s"};
    string_assignment.rhs.set_str("toolong");
    jit::UpdateKernel kernel(
        table, {self_clause(UpdateOp::SELF_MUL, TYPE_FLOAT, 0, -2.0), float_assignment, string_assignment});
    ASSERT_TRUE(kernel.valid());
    EXPECT_EQ(kernel.update(updated.data(), old_record.data()), jit::JitStatus::OK);
    EXPECT_EQ(read_unaligned<int>(updated.data()), 14);
    EXPECT_DOUBLE_EQ(read_unaligned<double>(updated.data() + 4), -7.0);
    EXPECT_EQ(std::memcmp(updated.data() + 12, "toolon", 6), 0);

    std::array<char, 18> unchanged = old_record;
    jit::UpdateKernel divide_zero(table, {self_clause(UpdateOp::SELF_DIV, TYPE_INT, 0, 0.0)});
    EXPECT_EQ(divide_zero.update(unchanged.data(), old_record.data()), jit::JitStatus::DIVISION_BY_ZERO);
    EXPECT_EQ(std::memcmp(unchanged.data(), old_record.data(), old_record.size()), 0);

    jit::UpdateKernel extrema(table, {self_clause(UpdateOp::SELF_ADD, TYPE_INT, std::numeric_limits<int>::max(), 0.0)});
    EXPECT_EQ(extrema.update(updated.data(), old_record.data()), jit::JitStatus::OK);
}
