#undef NDEBUG

#define private public
#include "execution/executor_aggregate.h"
#undef private

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

struct TestExecutorQueryExpr {
    QueryExprType type = QueryExprType::COLUMN;
    TabCol col;
    AggExpr agg;
    Value val;
    Value value;
    std::string display_name;
};

struct TestExecutorHavingCondition {
    TestExecutorQueryExpr lhs;
    CompOp op = OP_EQ;
    bool is_rhs_val = false;
    bool is_rhs_value = false;
    TestExecutorQueryExpr rhs_expr;
    Value rhs_val;
};

class FakeExecutor : public AbstractExecutor {
public:
    FakeExecutor(std::vector<ColMeta> cols, std::vector<RmRecord> rows)
        : cols_(std::move(cols)), rows_(std::move(rows)) {
        for (const auto& col : cols_) {
            tuple_len_ = std::max(tuple_len_, static_cast<size_t>(col.offset + col.len));
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

    bool is_end() const override {
        return cursor_ >= rows_.size();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(rows_[cursor_]);
    }

    Rid& rid() override {
        return rid_;
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return tuple_len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto it = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
            if (!target.tab_name.empty()) {
                return col.tab_name == target.tab_name && col.name == target.col_name;
            }
            return col.name == target.col_name;
        });
        if (it == cols_.end()) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *it;
    }

private:
    std::vector<ColMeta> cols_;
    std::vector<RmRecord> rows_;
    size_t tuple_len_ = 0;
    size_t cursor_ = 0;
    Rid rid_{};
};

ColMeta make_col(std::string tab_name, std::string col_name, ColType type, int len, int offset) {
    return ColMeta{std::move(tab_name), std::move(col_name), type, len, offset, false};
}

Value make_int_value(int value) {
    Value v;
    v.set_int(value);
    return v;
}

Value make_float_value(float value) {
    Value v;
    v.set_float(value);
    return v;
}

Value make_string_value(std::string value) {
    Value v;
    v.set_str(std::move(value));
    return v;
}

RmRecord make_record(const std::vector<ColMeta>& cols, const std::vector<Value>& values) {
    int tuple_len = 0;
    for (const auto& col : cols) {
        tuple_len = std::max(tuple_len, col.offset + col.len);
    }

    RmRecord rec(tuple_len);
    std::memset(rec.data, 0, tuple_len);
    for (size_t i = 0; i < cols.size(); ++i) {
        const auto& col = cols[i];
        const auto& value = values[i];
        char* dest = rec.data + col.offset;
        switch (col.type) {
        case TYPE_INT:
            *reinterpret_cast<int*>(dest) = value.int_val;
            break;
        case TYPE_FLOAT:
            *reinterpret_cast<float*>(dest) = value.float_val;
            break;
        case TYPE_STRING:
            std::memcpy(dest, value.str_val.data(), std::min<int>(col.len, value.str_val.size()));
            break;
        }
    }
    return rec;
}

std::unique_ptr<AbstractExecutor> make_child_executor(const std::vector<ColMeta>& cols,
                                                      const std::vector<std::vector<Value>>& rows) {
    std::vector<RmRecord> records;
    records.reserve(rows.size());
    for (const auto& row : rows) {
        records.push_back(make_record(cols, row));
    }
    return std::make_unique<FakeExecutor>(cols, std::move(records));
}

AggExpr make_count_star(std::string display_name = "COUNT(*)") {
    AggExpr agg;
    agg.type = AggType::COUNT;
    agg.is_star = true;
    agg.display_name = std::move(display_name);
    return agg;
}

AggExpr make_aggregate(AggType type, std::string col_name, std::string display_name) {
    AggExpr agg;
    agg.type = type;
    agg.col = {"t", std::move(col_name)};
    agg.display_name = std::move(display_name);
    return agg;
}

TestExecutorQueryExpr make_agg_expr(const AggExpr& agg) {
    TestExecutorQueryExpr expr;
    expr.type = QueryExprType::AGGREGATE;
    expr.agg = agg;
    expr.display_name = agg.display_name;
    return expr;
}

TestExecutorHavingCondition make_having_with_literal(const TestExecutorQueryExpr& lhs, CompOp op, const Value& rhs) {
    TestExecutorHavingCondition cond;
    cond.lhs = lhs;
    cond.op = op;
    cond.is_rhs_val = true;
    cond.is_rhs_value = true;
    cond.rhs_val = rhs;
    return cond;
}

int read_int(const RmRecord& rec, int offset) {
    return *reinterpret_cast<int*>(rec.data + offset);
}

float read_float(const RmRecord& rec, int offset) {
    return *reinterpret_cast<float*>(rec.data + offset);
}

std::string read_string(const RmRecord& rec, int offset, int len) {
    return std::string(rec.data + offset, strnlen(rec.data + offset, len));
}

} // namespace

TEST(AggregateExecutorTest, GroupsRowsAndComputesCountStarAndSum) {
    std::vector<ColMeta> cols = {
        make_col("t", "dept", TYPE_STRING, 8, 0),
        make_col("t", "score", TYPE_INT, 4, 8),
    };

    auto child = make_child_executor(cols, {
                                               {make_string_value("eng"), make_int_value(10)},
                                               {make_string_value("eng"), make_int_value(20)},
                                               {make_string_value("ops"), make_int_value(7)},
                                           });

    std::vector<TabCol> group_by = {{"t", "dept"}};
    std::vector<AggExpr> aggs = {make_count_star("cnt"), make_aggregate(AggType::SUM, "score", "total_score")};
    std::vector<TestExecutorHavingCondition> having;

    AggregateExecutor exec(std::move(child), group_by, aggs, having);

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    auto first = exec.Next();
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(read_string(*first, 0, 8), "eng");
    EXPECT_EQ(read_int(*first, 8), 2);
    EXPECT_EQ(read_int(*first, 12), 30);

    exec.nextTuple();
    ASSERT_FALSE(exec.is_end());
    auto second = exec.Next();
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(read_string(*second, 0, 8), "ops");
    EXPECT_EQ(read_int(*second, 8), 1);
    EXPECT_EQ(read_int(*second, 12), 7);

    exec.nextTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST(AggregateExecutorTest, HavingCanFilterOnAggregateResultAgainstIntegerLiteral) {
    std::vector<ColMeta> cols = {
        make_col("t", "dept", TYPE_STRING, 8, 0),
        make_col("t", "score", TYPE_INT, 4, 8),
    };

    auto child = make_child_executor(cols, {
                                               {make_string_value("eng"), make_int_value(10)},
                                               {make_string_value("eng"), make_int_value(20)},
                                               {make_string_value("ops"), make_int_value(7)},
                                               {make_string_value("ops"), make_int_value(9)},
                                           });

    AggExpr avg_score = make_aggregate(AggType::AVG, "score", "avg_score");
    std::vector<TabCol> group_by = {{"t", "dept"}};
    std::vector<AggExpr> aggs = {avg_score};
    std::vector<TestExecutorHavingCondition> having = {
        make_having_with_literal(make_agg_expr(avg_score), OP_GE, make_int_value(15)),
    };

    AggregateExecutor exec(std::move(child), group_by, aggs, having);

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    auto row = exec.Next();
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(read_string(*row, 0, 8), "eng");
    EXPECT_FLOAT_EQ(read_float(*row, 8), 15.0f);

    exec.nextTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST(AggregateExecutorTest, EmptyInputWithoutGroupByStillEmitsAggregateRow) {
    std::vector<ColMeta> cols = {
        make_col("t", "score", TYPE_INT, 4, 0),
    };

    auto child = make_child_executor(cols, {});
    std::vector<TabCol> group_by;
    std::vector<AggExpr> aggs = {
        make_count_star("cnt"),
        make_aggregate(AggType::SUM, "score", "sum_score"),
        make_aggregate(AggType::AVG, "score", "avg_score"),
    };
    std::vector<TestExecutorHavingCondition> having;

    AggregateExecutor exec(std::move(child), group_by, aggs, having);

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    auto row = exec.Next();
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(read_int(*row, 0), 0);
    EXPECT_EQ(read_int(*row, 4), 0);
    EXPECT_FLOAT_EQ(read_float(*row, 8), 0.0f);

    exec.nextTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST(AggregateExecutorTest, EmptyInputWithGroupByProducesNoRows) {
    std::vector<ColMeta> cols = {
        make_col("t", "dept", TYPE_STRING, 8, 0),
        make_col("t", "score", TYPE_INT, 4, 8),
    };

    auto child = make_child_executor(cols, {});
    std::vector<TabCol> group_by = {{"t", "dept"}};
    std::vector<AggExpr> aggs = {make_count_star("cnt")};
    std::vector<TestExecutorHavingCondition> having;

    AggregateExecutor exec(std::move(child), group_by, aggs, having);

    exec.beginTuple();
    EXPECT_TRUE(exec.is_end());
}

TEST(AggregateExecutorTest, CountStarCountsRowsWithoutReadingADataColumn) {
    std::vector<ColMeta> cols = {
        make_col("t", "dept", TYPE_STRING, 8, 0),
    };

    auto child = make_child_executor(cols, {
                                               {make_string_value("eng")},
                                               {make_string_value("ops")},
                                               {make_string_value("qa")},
                                           });

    std::vector<TabCol> group_by;
    std::vector<AggExpr> aggs = {make_count_star("cnt")};
    std::vector<TestExecutorHavingCondition> having;

    AggregateExecutor exec(std::move(child), group_by, aggs, having);

    exec.beginTuple();
    ASSERT_FALSE(exec.is_end());
    auto row = exec.Next();
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(read_int(*row, 0), 3);
}

TEST(AggregateExecutorTest, OutputSchemaMatchesGroupAndAggregateLayout) {
    std::vector<ColMeta> cols = {
        make_col("t", "dept", TYPE_STRING, 8, 0),
        make_col("t", "score", TYPE_INT, 4, 8),
        make_col("t", "bonus", TYPE_FLOAT, 4, 12),
    };

    auto child = make_child_executor(cols, {
                                               {make_string_value("eng"), make_int_value(10), make_float_value(1.5f)},
                                           });

    AggExpr max_score = make_aggregate(AggType::MAX, "score", "best_score");
    AggExpr avg_bonus = make_aggregate(AggType::AVG, "bonus", "avg_bonus");
    std::vector<TabCol> group_by = {{"t", "dept"}};
    std::vector<AggExpr> aggs = {max_score, avg_bonus};
    std::vector<TestExecutorHavingCondition> having;

    AggregateExecutor exec(std::move(child), group_by, aggs, having);

    ASSERT_EQ(exec.tupleLen(), 16);
    ASSERT_EQ(exec.cols().size(), 3);

    EXPECT_EQ(exec.cols()[0].tab_name, "t");
    EXPECT_EQ(exec.cols()[0].name, "dept");
    EXPECT_EQ(exec.cols()[0].type, TYPE_STRING);
    EXPECT_EQ(exec.cols()[0].offset, 0);

    EXPECT_TRUE(exec.cols()[1].tab_name.empty());
    EXPECT_EQ(exec.cols()[1].name, "best_score");
    EXPECT_EQ(exec.cols()[1].type, TYPE_INT);
    EXPECT_EQ(exec.cols()[1].offset, 8);

    EXPECT_TRUE(exec.cols()[2].tab_name.empty());
    EXPECT_EQ(exec.cols()[2].name, "avg_bonus");
    EXPECT_EQ(exec.cols()[2].type, TYPE_FLOAT);
    EXPECT_EQ(exec.cols()[2].offset, 12);
}
