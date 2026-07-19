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

#include "execution/execution_common.h"
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

TEST(ExecutionScalarTest, FlatGroupKeyTrimsFixedWidthStrings) {
    const char raw[8] = {'a', 'b', '\0', 'p', 'a', 'd', '\0', '\0'};
    ColMeta col;
    col.type = TYPE_STRING;
    col.offset = 0;
    col.len = sizeof(raw);
    const TupleView tuple{raw, static_cast<uint32_t>(sizeof(raw))};

    GroupKey from_tuple = AggregateExecutor::make_group_key(tuple, {col});
    GroupKey from_cell{{make_string_cell("ab")}};
    EXPECT_EQ(from_tuple, from_cell);
    EXPECT_EQ(GroupKeyHash{}(from_tuple), GroupKeyHash{}(from_cell));
}

TEST(ExecutionScalarTest, FlatGroupKeyCanonicalizesMixedNumericTypes) {
    const int raw_int = 42;
    ColMeta int_col;
    int_col.type = TYPE_INT;
    int_col.offset = 0;
    int_col.len = sizeof(raw_int);
    const TupleView int_tuple{reinterpret_cast<const char*>(&raw_int), static_cast<uint32_t>(sizeof(raw_int))};

    const double raw_float = 42.0;
    ColMeta float_col;
    float_col.type = TYPE_FLOAT;
    float_col.offset = 0;
    float_col.len = sizeof(raw_float);
    const TupleView float_tuple{reinterpret_cast<const char*>(&raw_float), static_cast<uint32_t>(sizeof(raw_float))};

    EXPECT_EQ(AggregateExecutor::make_group_key(int_tuple, {int_col}),
              AggregateExecutor::make_group_key(float_tuple, {float_col}));
}

TEST(ExecutionScalarTest, VisibilityWatermarkFastPathPreservesDeleteSemantics) {
    TupleMeta committed;
    committed.is_committed_ = true;
    committed.commit_ts_ = 10;
    EXPECT_TRUE(IsCommittedBeforeWatermark(committed, 11));
    EXPECT_FALSE(IsCommittedBeforeWatermark(committed, 10));

    committed.is_deleted_ = true;
    EXPECT_TRUE(IsCommittedBeforeWatermark(committed, 11));
}
