#include <gtest/gtest.h>

#include "common/wal_flush_metrics.h"

TEST(WalFlushMetricsTest, EnabledConfigurationIsExactOneAndDefaultsOff) {
    EXPECT_FALSE(WalFlushMetrics::ParseEnabled(nullptr));
    EXPECT_FALSE(WalFlushMetrics::ParseEnabled(""));
    EXPECT_FALSE(WalFlushMetrics::ParseEnabled("0"));
    EXPECT_FALSE(WalFlushMetrics::ParseEnabled("01"));
    EXPECT_FALSE(WalFlushMetrics::ParseEnabled("true"));
    EXPECT_TRUE(WalFlushMetrics::ParseEnabled("1"));

    WalFlushMetrics disabled(false);
    EXPECT_FALSE(disabled.enabled());
    const auto snapshot = disabled.snapshot();
    EXPECT_EQ(snapshot.leader_requests, 0U);
    EXPECT_EQ(snapshot.pwrite.count, 0U);
}

TEST(WalFlushMetricsTest, CountsTimingsAndMaximumsAreCumulative) {
    WalFlushMetrics metrics(true);
    metrics.record_already_covered_fast_path();
    metrics.record_leader_request();
    metrics.record_leader_rotation();
    metrics.record_leader_rotation();
    metrics.record_leader_tenure(1);
    metrics.record_leader_tenure(3);
    metrics.record_follower_request();
    metrics.record_follower_wait(3);
    metrics.record_follower_wait(7);
    metrics.record_coalescing_wait(11);
    metrics.record_physical_flush_iteration();
    metrics.record_pwrite(128, 5);
    metrics.record_pwrite(64, 9);
    metrics.record_fdatasync(4);
    metrics.record_fdatasync(6);

    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.already_covered_fast_paths, 1U);
    EXPECT_EQ(snapshot.leader_requests, 1U);
    EXPECT_EQ(snapshot.leader_rotations, 2U);
    EXPECT_EQ(snapshot.max_batches_per_leader, 3U);
    EXPECT_EQ(snapshot.follower_requests, 1U);
    EXPECT_EQ(snapshot.follower_wait.count, 2U);
    EXPECT_EQ(snapshot.follower_wait.elapsed_ns, 10U);
    EXPECT_EQ(snapshot.follower_wait.max_ns, 7U);
    EXPECT_EQ(snapshot.coalescing_wait.count, 1U);
    EXPECT_EQ(snapshot.coalescing_wait.elapsed_ns, 11U);
    EXPECT_EQ(snapshot.physical_flush_iterations, 1U);
    EXPECT_EQ(snapshot.pwrite.count, 2U);
    EXPECT_EQ(snapshot.pwrite_bytes, 192U);
    EXPECT_EQ(snapshot.pwrite.elapsed_ns, 14U);
    EXPECT_EQ(snapshot.pwrite.max_ns, 9U);
    EXPECT_EQ(snapshot.fdatasync.count, 2U);
    EXPECT_EQ(snapshot.fdatasync.elapsed_ns, 10U);
    EXPECT_EQ(snapshot.fdatasync.max_ns, 6U);
}

TEST(WalFlushMetricsTest, CompletedBatchHistogramUsesDocumentedBoundaries) {
    WalFlushMetrics metrics(true);
    for (const size_t value : {0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U, 16U,
                               17U, 32U, 33U, 64U, 65U, 512U}) {
        metrics.record_completed_batch(value);
    }

    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.completed_batch_histogram[0], 1U);
    for (size_t exact = 1; exact <= 8; ++exact) {
        EXPECT_EQ(snapshot.completed_batch_histogram[exact], 1U) << "exact batch " << exact;
    }
    EXPECT_EQ(snapshot.completed_batch_histogram[9], 2U);
    EXPECT_EQ(snapshot.completed_batch_histogram[10], 2U);
    EXPECT_EQ(snapshot.completed_batch_histogram[11], 2U);
    EXPECT_EQ(snapshot.completed_batch_histogram[12], 2U);
}

TEST(WalFlushMetricsTest, InvalidAndResetLsnValuesDoNotCreateLagSamples) {
    WalFlushMetrics metrics(true);
    metrics.record_durable_lag(10, -1, 4);
    metrics.record_durable_lag(-1, 4, 4);
    metrics.record_durable_lag(10, 4, -1);
    metrics.record_durable_lag(10, 4, 10);

    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.durable_lag_samples, 1U);
    EXPECT_EQ(snapshot.durable_lag_before_sum, 6U);
    EXPECT_EQ(snapshot.durable_lag_before_max, 6U);
    EXPECT_EQ(snapshot.durable_lag_after_sum, 0U);
    EXPECT_EQ(snapshot.durable_lag_after_max, 0U);
}
