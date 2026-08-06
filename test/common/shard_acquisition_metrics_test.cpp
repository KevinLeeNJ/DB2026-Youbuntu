#include <gtest/gtest.h>

#include <mutex>

#include "common/shard_acquisition_metrics.h"

TEST(ShardAcquisitionMetricsTest, DisabledAcquisitionLeavesCountersZero) {
    ShardAcquisitionMetrics metrics;
    std::mutex latch;
    auto lock = metrics.acquire_exclusive(latch, 3);
    const auto snapshot = metrics.snapshot(3);
    EXPECT_EQ(snapshot.sampled_acquisitions, 0);
    EXPECT_EQ(snapshot.slow_acquisitions, 0);
    EXPECT_EQ(snapshot.sampled_elapsed_ns, 0);
    EXPECT_EQ(snapshot.sampled_max_ns, 0);
}

TEST(ShardAcquisitionMetricsTest, AlwaysSampleRecordsUncontendedAcquisition) {
    ShardAcquisitionMetrics metrics({true, 0, 0});
    std::mutex latch;
    auto lock = metrics.acquire_exclusive(latch, 7);
    const auto first = metrics.snapshot(7);
    lock.unlock();
    auto second_lock = metrics.acquire_exclusive(latch, 7);
    const auto second = metrics.snapshot(7);
    EXPECT_EQ(first.sampled_acquisitions, 1);
    EXPECT_EQ(first.slow_acquisitions, 1);
    EXPECT_GE(first.sampled_elapsed_ns, first.sampled_max_ns);
    EXPECT_EQ(second.sampled_acquisitions, 2);
    EXPECT_EQ(second.slow_acquisitions, 2);
    EXPECT_GE(second.sampled_elapsed_ns, first.sampled_elapsed_ns);
    EXPECT_GE(second.sampled_max_ns, first.sampled_max_ns);
}

TEST(ShardAcquisitionMetricsTest, ParsesOnlyStrictUnsignedConfiguration) {
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse(nullptr, nullptr).enabled);
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse("", nullptr).enabled);
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse("-1", nullptr).enabled);
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse("21", nullptr).enabled);
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse("18446744073709551616", nullptr).enabled);
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse("0", "-1").enabled);
    EXPECT_FALSE(ShardAcquisitionMetrics::Config::Parse("0", "18446744073709551616").enabled);
    const auto config = ShardAcquisitionMetrics::Config::Parse("20", "0");
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.sample_log2, 20);
    EXPECT_EQ(config.slow_ns, 0);
}

TEST(ShardAcquisitionMetricsTest, ConstructorDisablesOutOfRangePublicConfig) {
    ShardAcquisitionMetrics metrics({true, 255, 0});
    EXPECT_FALSE(metrics.enabled());
}
