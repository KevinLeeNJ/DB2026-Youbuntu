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
#include "execution/executor_aggregate.h"
#undef private

#include <cstdint>
#include <cstring>

#include "common/common.h"
#include "gtest/gtest.h"

namespace {

using CellValue = AggregateExecutor::CellValue;
using CellValueHash = AggregateExecutor::CellValueHash;
using GroupKey = AggregateExecutor::GroupKey;
using GroupKeyHash = AggregateExecutor::GroupKeyHash;

CellValue make_int_cell(int value) {
    CellValue cell;
    cell.type = TYPE_INT;
    cell.int_val = value;
    return cell;
}

CellValue make_float_cell(double value) {
    CellValue cell;
    cell.type = TYPE_FLOAT;
    cell.float_val = value;
    return cell;
}

CellValue make_string_cell(std::string value) {
    CellValue cell;
    cell.type = TYPE_STRING;
    cell.str_val = std::move(value);
    return cell;
}

} // namespace

TEST(ValueFloatTest, RawStorageKeepsBinary32BitPattern) {
    constexpr float input = 300000.01F;
    Value value;
    value.set_float(input);
    value.init_raw(sizeof(float));

    uint32_t expected_bits = 0;
    uint32_t actual_bits = 0;
    std::memcpy(&expected_bits, &input, sizeof(expected_bits));
    std::memcpy(&actual_bits, value.raw->data, sizeof(actual_bits));
    EXPECT_EQ(actual_bits, expected_bits);
    EXPECT_EQ(value.raw->size, static_cast<int>(sizeof(float)));
}

TEST(ValueFloatTest, RepeatedWritesRoundAtBinary32Precision) {
    float expected = 0.0F;
    constexpr float delta = 0.01F;
    for (int i = 0; i < 1000; ++i) {
        expected = static_cast<float>(expected + delta);
    }

    Value value;
    value.set_float(0.0F);
    for (int i = 0; i < 1000; ++i) {
        value.set_float(static_cast<float>(value.float_val + delta));
    }
    EXPECT_EQ(value.float_val, expected);
}

TEST(ExecutionScalarTest, CompareCellsNormalizesMixedNumericEquality) {
    EXPECT_EQ(AggregateExecutor::compare_cells(make_int_cell(7), make_float_cell(7.0f)), 0);
    EXPECT_EQ(AggregateExecutor::compare_cells(make_float_cell(7.0f), make_int_cell(7)), 0);
}

TEST(ExecutionScalarTest, CompareCellsOrdersMixedNumericValuesConsistently) {
    EXPECT_LT(AggregateExecutor::compare_cells(make_int_cell(6), make_float_cell(6.5f)), 0);
    EXPECT_GT(AggregateExecutor::compare_cells(make_float_cell(6.5f), make_int_cell(6)), 0);
}

TEST(ExecutionScalarTest, CompareCellsUsesLexicographicStringOrder) {
    EXPECT_LT(AggregateExecutor::compare_cells(make_string_cell("alpha"), make_string_cell("beta")), 0);
    EXPECT_GT(AggregateExecutor::compare_cells(make_string_cell("beta"), make_string_cell("alpha")), 0);
    EXPECT_EQ(AggregateExecutor::compare_cells(make_string_cell("same"), make_string_cell("same")), 0);
}

TEST(ExecutionScalarTest, CompareCellsRejectsIncompatibleTypes) {
    EXPECT_THROW(AggregateExecutor::compare_cells(make_string_cell("7"), make_int_cell(7)), IncompatibleTypeError);
}

TEST(ExecutionScalarTest, TrimStringStopsAtFirstNullByte) {
    const char raw[8] = {'a', 'b', '\0', 'x', 'y', 'z', '\0', '\0'};
    EXPECT_EQ(AggregateExecutor::trim_string(raw, sizeof(raw)), "ab");
}

TEST(ExecutionScalarTest, TrimStringKeepsFullFixedWidthStringWithoutPadding) {
    const char raw[4] = {'d', 'a', 't', 'a'};
    EXPECT_EQ(AggregateExecutor::trim_string(raw, sizeof(raw)), "data");
}

TEST(ExecutionScalarTest, HashesMatchForTrimmedEquivalentStrings) {
    CellValue lhs = make_string_cell("group");
    CellValue rhs = make_string_cell(AggregateExecutor::trim_string("group\0pad", 9));
    EXPECT_EQ(lhs, rhs);
    EXPECT_EQ(CellValueHash{}(lhs), CellValueHash{}(rhs));
}

TEST(ExecutionScalarTest, HashesTreatEquivalentMixedNumericValuesConsistently) {
    CellValue as_int = make_int_cell(42);
    CellValue as_float = make_float_cell(42.0f);

    EXPECT_EQ(AggregateExecutor::compare_cells(as_int, as_float), 0);
    EXPECT_EQ(CellValueHash{}(as_int), CellValueHash{}(as_float));

    GroupKey int_key{{as_int}};
    GroupKey float_key{{as_float}};
    EXPECT_EQ(GroupKeyHash{}(int_key), GroupKeyHash{}(float_key));
}
