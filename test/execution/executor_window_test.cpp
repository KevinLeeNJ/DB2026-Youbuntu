/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
 */

#include "execution/executor_window.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

constexpr int kIntLen = sizeof(int);

std::vector<ColMeta> score_cols() {
    return {ColMeta{"scores", "grp", TYPE_INT, kIntLen, 0, false},
            ColMeta{"scores", "id", TYPE_INT, kIntLen, kIntLen, false},
            ColMeta{"scores", "score", TYPE_INT, kIntLen, kIntLen * 2, false}};
}

RmRecord make_score_record(int group, int id, int score) {
    RmRecord record(kIntLen * 3);
    std::memcpy(record.data, &group, kIntLen);
    std::memcpy(record.data + kIntLen, &id, kIntLen);
    std::memcpy(record.data + kIntLen * 2, &score, kIntLen);
    return record;
}

class VectorExecutor : public AbstractExecutor {
public:
    VectorExecutor(std::vector<ColMeta> cols, std::vector<RmRecord> records, std::vector<std::vector<bool>> nulls = {})
        : cols_(std::move(cols)), records_(std::move(records)), nulls_(std::move(nulls)) {
        if (nulls_.empty()) {
            nulls_.resize(records_.size(), std::vector<bool>(cols_.size(), false));
        }
    }

    void beginTuple() override {
        cursor_ = 0;
    }

    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(records_[cursor_]);
    }

    bool is_end() const override {
        return cursor_ >= records_.size();
    }

    Rid& rid() override {
        return rid_;
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return kIntLen * 3;
    }

    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : nulls_[cursor_];
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
            return (target.tab_name.empty() || col.tab_name == target.tab_name) && col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *pos;
    }

private:
    std::vector<ColMeta> cols_;
    std::vector<RmRecord> records_;
    std::vector<std::vector<bool>> nulls_;
    size_t cursor_ = 0;
    Rid rid_{};
};

std::shared_ptr<QueryExpr> column_expr(const std::string& name) {
    auto expr = std::make_shared<QueryExpr>();
    expr->type = QueryExprType::COLUMN;
    expr->col = {.tab_name = "scores", .col_name = name};
    return expr;
}

std::shared_ptr<QueryExpr> int_expr(int value) {
    auto expr = std::make_shared<QueryExpr>();
    expr->type = QueryExprType::VALUE;
    expr->value.set_int(value);
    return expr;
}

QueryExpr make_window(WindowFuncType func, std::vector<std::shared_ptr<QueryExpr>> args = {},
                      std::vector<std::shared_ptr<QueryExpr>> partition_by = {},
                      std::vector<std::shared_ptr<QueryExpr>> order_by = {}, std::vector<bool> order_desc = {}) {
    QueryExpr expr;
    expr.type = QueryExprType::WINDOW;
    expr.window_func = func;
    expr.window_args = std::move(args);
    expr.window_partition_by = std::move(partition_by);
    expr.window_order_by = std::move(order_by);
    expr.window_order_desc = std::move(order_desc);
    if (expr.window_order_desc.empty()) {
        expr.window_order_desc.assign(expr.window_order_by.size(), false);
    }
    expr.window_nulls_order.assign(expr.window_order_by.size(), 0);
    return expr;
}

int hidden_int(const WindowExecutor& executor, const RmRecord& record, size_t index) {
    const auto& col = executor.cols()[3 + index];
    return *reinterpret_cast<const int*>(record.data + col.offset);
}

double hidden_float(const WindowExecutor& executor, const RmRecord& record, size_t index) {
    const auto& col = executor.cols()[3 + index];
    return *reinterpret_cast<const double*>(record.data + col.offset);
}

} // namespace

TEST(WindowExecutorTest, RowNumberResetsPerPartition) {
    auto child = std::make_unique<VectorExecutor>(
        score_cols(), std::vector<RmRecord>{make_score_record(1, 2, 20), make_score_record(2, 4, 5),
                                            make_score_record(1, 1, 10), make_score_record(2, 5, 15)});
    auto window = make_window(WindowFuncType::ROW_NUMBER, {}, {column_expr("grp")}, {column_expr("score")});
    WindowExecutor executor(std::move(child), {std::move(window)});

    std::vector<int> row_numbers;
    for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
        auto record = executor.Next();
        ASSERT_NE(record, nullptr);
        row_numbers.push_back(hidden_int(executor, *record, 0));
    }

    EXPECT_EQ(row_numbers, (std::vector<int>{2, 1, 1, 2}));
}

TEST(WindowExecutorTest, RankAndDenseRankHandlePeers) {
    auto child = std::make_unique<VectorExecutor>(
        score_cols(),
        std::vector<RmRecord>{make_score_record(1, 1, 20), make_score_record(1, 2, 20), make_score_record(1, 3, 10)});
    auto rank = make_window(WindowFuncType::RANK, {}, {}, {column_expr("score")}, {true});
    auto dense_rank = make_window(WindowFuncType::DENSE_RANK, {}, {}, {column_expr("score")}, {true});
    WindowExecutor executor(std::move(child), {std::move(rank), std::move(dense_rank)});

    std::vector<std::pair<int, int>> ranks;
    for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
        auto record = executor.Next();
        ASSERT_NE(record, nullptr);
        ranks.emplace_back(hidden_int(executor, *record, 0), hidden_int(executor, *record, 1));
    }

    EXPECT_EQ(ranks, (std::vector<std::pair<int, int>>{{1, 1}, {1, 1}, {3, 2}}));
}

TEST(WindowExecutorTest, LagAndLeadUsePartitionBoundariesAndDefaults) {
    auto child = std::make_unique<VectorExecutor>(
        score_cols(),
        std::vector<RmRecord>{make_score_record(1, 1, 10), make_score_record(1, 2, 20), make_score_record(2, 3, 30)});
    auto lag = make_window(WindowFuncType::LAG, {column_expr("score"), int_expr(1), int_expr(0)}, {column_expr("grp")},
                           {column_expr("id")});
    auto lead = make_window(WindowFuncType::LEAD, {column_expr("score"), int_expr(1), int_expr(-1)},
                            {column_expr("grp")}, {column_expr("id")});
    WindowExecutor executor(std::move(child), {std::move(lag), std::move(lead)});

    std::vector<std::pair<int, int>> offsets;
    for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
        auto record = executor.Next();
        ASSERT_NE(record, nullptr);
        offsets.emplace_back(hidden_int(executor, *record, 0), hidden_int(executor, *record, 1));
    }

    EXPECT_EQ(offsets, (std::vector<std::pair<int, int>>{{0, 20}, {10, -1}, {0, -1}}));
}

TEST(WindowExecutorTest, OrderedSumIncludesTheCurrentPeerGroup) {
    auto child = std::make_unique<VectorExecutor>(
        score_cols(), std::vector<RmRecord>{make_score_record(1, 1, 10), make_score_record(2, 4, 5),
                                            make_score_record(1, 2, 20), make_score_record(1, 3, 20)});
    auto sum = make_window(WindowFuncType::SUM, {column_expr("score")}, {column_expr("grp")}, {column_expr("score")});
    WindowExecutor executor(std::move(child), {std::move(sum)});

    std::vector<int> sums;
    for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
        auto record = executor.Next();
        ASSERT_NE(record, nullptr);
        sums.push_back(hidden_int(executor, *record, 0));
    }

    EXPECT_EQ(sums, (std::vector<int>{10, 5, 50, 50}));
}

TEST(WindowExecutorTest, AvgIgnoresNullInputsAndReturnsNullForEmptyPartitions) {
    auto nulls = std::vector<std::vector<bool>>{
        {false, false, false}, {false, false, true}, {false, false, false}, {false, false, true}};
    auto child =
        std::make_unique<VectorExecutor>(score_cols(),
                                         std::vector<RmRecord>{make_score_record(1, 1, 10), make_score_record(1, 2, 0),
                                                               make_score_record(1, 3, 20), make_score_record(2, 4, 0)},
                                         std::move(nulls));
    auto avg = make_window(WindowFuncType::AVG, {column_expr("score")}, {column_expr("grp")});
    WindowExecutor executor(std::move(child), {std::move(avg)});

    std::vector<double> averages;
    std::vector<bool> result_nulls;
    for (executor.beginTuple(); !executor.is_end(); executor.nextTuple()) {
        auto record = executor.Next();
        ASSERT_NE(record, nullptr);
        averages.push_back(hidden_float(executor, *record, 0));
        result_nulls.push_back(executor.nulls().back());
    }

    EXPECT_EQ(result_nulls, (std::vector<bool>{false, false, false, true}));
    EXPECT_DOUBLE_EQ(averages[0], 15.0);
    EXPECT_DOUBLE_EQ(averages[1], 15.0);
    EXPECT_DOUBLE_EQ(averages[2], 15.0);
}
