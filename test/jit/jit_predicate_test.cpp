/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <vector>

#include "execution/executor_filter.h"
#include "jit/jit_predicate.h"

namespace {

class TupleSourceExecutor final : public AbstractExecutor {
public:
    TupleSourceExecutor(std::vector<ColMeta> columns, std::vector<RmRecord> records)
        : columns_(std::move(columns)), records_(std::move(records)) {}

    void beginTuple() override {
        position_ = 0;
    }

    void nextTuple() override {
        ++position_;
    }

    bool is_end() const override {
        return position_ >= records_.size();
    }

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        const auto& record = records_[position_];
        return {record.data, static_cast<uint32_t>(record.size)};
    }

    std::unique_ptr<RmRecord> Next() override {
        return is_end() ? nullptr : std::make_unique<RmRecord>(records_[position_]);
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    const std::vector<ColMeta>& cols() const override {
        return columns_;
    }

    size_t tupleLen() const override {
        return sizeof(int);
    }

    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& column : columns_) {
            if (column.tab_name == target.tab_name && column.name == target.col_name) {
                return column;
            }
        }
        throw ColumnNotFoundError(target.col_name);
    }

private:
    std::vector<ColMeta> columns_;
    std::vector<RmRecord> records_;
    size_t position_{0};
};

RmRecord int_record(int value) {
    RmRecord record(sizeof(value));
    std::memcpy(record.data, &value, sizeof(value));
    return record;
}

} // namespace

TEST(JitPredicateTest, ForceModeExecutesCachedPredicateWithExecutionLocalFrame) {
    std::atomic<uint64_t> generation{0};
    const auto saved_mode = rmdb_config::jit_mode;
    rmdb_config::jit_mode = rmdb_config::JitMode::FORCE;
    jit::initialize_predicate_jit([&] { return generation.load(); });
    {
        auto scope = jit::enter_predicate_jit_execution();
        ColMeta column{"t", "v", TYPE_INT, static_cast<int>(sizeof(int)), 0, false};
        Condition condition;
        condition.lhs_col = {"t", "v"};
        condition.op = OP_GE;
        condition.is_rhs_val = true;
        condition.rhs_val.set_int(7);
        jit::PredicateKernel kernel(T_SeqScan, {condition}, {sizeof(int), {column}}, std::nullopt, generation.load());
        ASSERT_TRUE(kernel);
        const int value = 9;
        const auto match = kernel.evaluate(reinterpret_cast<const char*>(&value), sizeof(value));
        ASSERT_TRUE(match.has_value());
        EXPECT_TRUE(*match);
        const auto hits_before = jit::predicate_jit_stats().cache_hits;
        for (int iteration = 0; iteration < 100; ++iteration) {
            const auto repeated = kernel.evaluate(reinterpret_cast<const char*>(&value), sizeof(value));
            ASSERT_TRUE(repeated.has_value());
            EXPECT_TRUE(*repeated);
        }
        EXPECT_EQ(jit::predicate_jit_stats().cache_hits, hits_before);

        jit::PredicateKernel second_kernel(T_SeqScan, {condition}, {sizeof(int), {column}}, std::nullopt,
                                           generation.load());
        const auto cached_match = second_kernel.evaluate(reinterpret_cast<const char*>(&value), sizeof(value));
        ASSERT_TRUE(cached_match.has_value());
        EXPECT_TRUE(*cached_match);
        EXPECT_EQ(jit::predicate_jit_stats().cache_hits, hits_before + 1);

        Context first_context(nullptr, nullptr, nullptr);
        first_context.has_statement_template_identity_ = true;
        first_context.statement_shape_high_ = 11;
        first_context.statement_shape_low_ = 22;
        first_context.statement_template_generation_ = 33;
        jit::PredicateKernel first_template_kernel(T_SeqScan, {condition}, {sizeof(int), {column}}, std::nullopt,
                                                   generation.load(), &first_context);
        const auto first_template_match =
            first_template_kernel.evaluate(reinterpret_cast<const char*>(&value), sizeof(value));
        ASSERT_TRUE(first_template_match.has_value());
        EXPECT_TRUE(*first_template_match);

        Context second_context(nullptr, nullptr, nullptr);
        second_context.has_statement_template_identity_ = true;
        second_context.statement_shape_high_ = 11;
        second_context.statement_shape_low_ = 22;
        second_context.statement_template_generation_ = 33;
        Condition rebound_condition = condition;
        rebound_condition.rhs_val.set_int(10);
        jit::PredicateKernel rebound_template_kernel(T_SeqScan, {rebound_condition}, {sizeof(int), {column}},
                                                     std::nullopt, generation.load(), &second_context);
        const auto rebound_template_match =
            rebound_template_kernel.evaluate(reinterpret_cast<const char*>(&value), sizeof(value));
        ASSERT_TRUE(rebound_template_match.has_value());
        EXPECT_FALSE(*rebound_template_match);

        auto child = std::make_unique<TupleSourceExecutor>(std::vector<ColMeta>{column},
                                                           std::vector<RmRecord>{int_record(4), int_record(9)});
        FilterExecutor filter(std::move(child), {condition});
        filter.beginTuple();
        ASSERT_FALSE(filter.is_end());
        const auto filtered = filter.current();
        int filtered_value = 0;
        std::memcpy(&filtered_value, filtered.data, sizeof(filtered_value));
        EXPECT_EQ(filtered_value, 9);
        EXPECT_GE(jit::predicate_jit_stats().cache_hits, 1U);
    }
    jit::shutdown_predicate_jit();
    rmdb_config::jit_mode = saved_mode;
}
