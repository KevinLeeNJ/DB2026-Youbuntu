#include "execution/cursor_test_helper.h"
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
#include "execution/execution_sort.h"
#undef private

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

ColMeta make_col(const std::string& tab_name, const std::string& name, ColType type, int len, int offset) {
    return ColMeta{tab_name, name, type, len, offset, false};
}

RmRecord make_sort_record(int group_id, int total_score) {
    RmRecord rec(sizeof(int) * 2);
    std::memcpy(rec.data, &group_id, sizeof(int));
    std::memcpy(rec.data + sizeof(int), &total_score, sizeof(int));
    return rec;
}

class VectorExecutor : public AbstractExecutor {
public:
    VectorExecutor(std::vector<ColMeta> cols, std::vector<RmRecord> records)
        : cols_(std::move(cols)), records_(std::move(records)) {
        len_ = 0;
        for (const auto& col : cols_) {
            len_ = std::max(len_, static_cast<size_t>(col.offset + col.len));
        }
    }

    void beginTuple() override {
        ++begin_calls_;
        cursor_ = 0;
    }

    void nextTuple() override {
        ++next_calls_;
        if (!is_end()) {
            ++cursor_;
        }
    }

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        return TupleView{records_[cursor_].data, static_cast<uint32_t>(records_[cursor_].size)};
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return cursor_ >= records_.size();
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : cols_) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
    }

    int begin_calls_ = 0;
    int next_calls_ = 0;

private:
    std::vector<ColMeta> cols_;
    std::vector<RmRecord> records_;
    size_t len_ = 0;
    size_t cursor_ = 0;
};

int read_int(const RmRecord& rec, int offset) {
    return *reinterpret_cast<const int*>(rec.data + offset);
}

} // namespace

TEST(SortExecutorFocusedTest, MaterializesOnlyOnceAndResetsCursorOnRebegin) {
    auto child = std::make_unique<VectorExecutor>(
        std::vector<ColMeta>{make_col("scores", "group_id", TYPE_INT, sizeof(int), 0),
                             make_col("scores", "total_score", TYPE_INT, sizeof(int), sizeof(int))},
        std::vector<RmRecord>{make_sort_record(1, 90), make_sort_record(2, 70), make_sort_record(3, 80)});
    auto* child_ptr = child.get();

    SortExecutor exec(std::move(child), TabCol{"scores", "total_score"}, false);

    exec.beginTuple();
    ASSERT_TRUE(exec.materialized_);
    ASSERT_EQ(exec.tuples_.size(), 3U);
    EXPECT_EQ(exec.cursor_, 0U);
    EXPECT_EQ(child_ptr->begin_calls_, 1);
    EXPECT_EQ(child_ptr->next_calls_, 3);
    EXPECT_EQ(child_ptr->next_calls_, 3);

    auto first = CopyCurrentTuple(exec);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(read_int(*first, 0), 2);
    EXPECT_EQ(read_int(*first, sizeof(int)), 70);

    exec.nextTuple();
    exec.beginTuple();
    EXPECT_EQ(exec.cursor_, 0U);
    EXPECT_EQ(child_ptr->begin_calls_, 1);
    EXPECT_EQ(child_ptr->next_calls_, 3);
    EXPECT_EQ(child_ptr->next_calls_, 3);

    auto replayed_first = CopyCurrentTuple(exec);
    ASSERT_NE(replayed_first, nullptr);
    EXPECT_EQ(read_int(*replayed_first, 0), 2);
    EXPECT_EQ(read_int(*replayed_first, sizeof(int)), 70);
}

TEST(SortExecutorFocusedTest, ResolvesAggregateAliasAndKeepsStableOrderForTies) {
    auto child = std::make_unique<VectorExecutor>(
        std::vector<ColMeta>{make_col("", "group_id", TYPE_INT, sizeof(int), 0),
                             make_col("", "total_score", TYPE_INT, sizeof(int), sizeof(int))},
        std::vector<RmRecord>{make_sort_record(3, 100), make_sort_record(1, 100), make_sort_record(2, 90)});

    OrderByItem order_by;
    order_by.expr.type = QueryExprType::AGGREGATE;
    order_by.expr.display_name = "total_score";
    order_by.expr.agg.display_name = "SUM(score)";
    order_by.is_desc = true;

    SortExecutor exec(std::move(child), std::vector<OrderByItem>{order_by});

    ASSERT_EQ(exec.sort_keys_.size(), 1U);
    EXPECT_EQ(exec.sort_keys_[0].col.name, "total_score");
    EXPECT_TRUE(exec.sort_keys_[0].is_desc);

    std::vector<std::pair<int, int>> rows;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = CopyCurrentTuple(exec);
        ASSERT_NE(rec, nullptr);
        rows.push_back({read_int(*rec, 0), read_int(*rec, sizeof(int))});
    }

    ASSERT_EQ(rows.size(), 3U);
    EXPECT_EQ(rows[0], (std::pair<int, int>{3, 100}));
    EXPECT_EQ(rows[1], (std::pair<int, int>{1, 100}));
    EXPECT_EQ(rows[2], (std::pair<int, int>{2, 90}));
}
