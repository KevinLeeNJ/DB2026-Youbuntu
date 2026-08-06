#include <gtest/gtest.h>

#include "common/checkpoint_phase_metrics.h"

TEST(CheckpointPhaseMetricsTest, ExactOneAndDefaultOff) {
    EXPECT_FALSE(CheckpointPhaseMetrics::ParseEnabled(nullptr));
    EXPECT_FALSE(CheckpointPhaseMetrics::ParseEnabled(""));
    EXPECT_FALSE(CheckpointPhaseMetrics::ParseEnabled("0"));
    EXPECT_FALSE(CheckpointPhaseMetrics::ParseEnabled("01"));
    EXPECT_TRUE(CheckpointPhaseMetrics::ParseEnabled("1"));
    CheckpointPhaseMetrics disabled(false);
    EXPECT_FALSE(disabled.enabled());
    disabled.clean_attempt();
    EXPECT_EQ(disabled.snapshot().clean_attempts, 0U);
}

TEST(CheckpointPhaseMetricsTest, CountersTimingsAndMaximaAreCumulative) {
    CheckpointPhaseMetrics metrics(true);
    metrics.clean_attempt();
    metrics.clean_success();
    metrics.fuzzy_attempt();
    metrics.fuzzy_failure();
    metrics.fuzzy_cancel();
    metrics.pages_marked(4);
    metrics.page_write(2, 8);
    metrics.page_write(3, 1);
    metrics.retry_deferral(false);
    metrics.retry_deferral(true);
    metrics.budget_yield_io();
    metrics.budget_yield_time();
    metrics.zero_progress_yield();
    metrics.record(CheckpointPhaseMetrics::Timing::PageWrite, 3);
    metrics.record(CheckpointPhaseMetrics::Timing::PageWrite, 7);
    metrics.record(CheckpointPhaseMetrics::Timing::CleanDataSync, 11);
    metrics.record(CheckpointPhaseMetrics::Timing::CleanMetaFlush, 13);
    metrics.record(CheckpointPhaseMetrics::Timing::FuzzyFinalPublish, 17);
    const auto s = metrics.snapshot();
    EXPECT_EQ(s.clean_attempts, 1U);
    EXPECT_EQ(s.clean_successes, 1U);
    EXPECT_EQ(s.fuzzy_attempts, 1U);
    EXPECT_EQ(s.fuzzy_failures, 1U);
    EXPECT_EQ(s.fuzzy_cancels, 1U);
    EXPECT_EQ(s.pages_marked, 4U);
    EXPECT_EQ(s.page_write_calls, 2U);
    EXPECT_EQ(s.pages_written, 5U);
    EXPECT_EQ(s.pages_remaining_max, 8U);
    EXPECT_EQ(s.retry_deferrals, 2U);
    EXPECT_EQ(s.deadline_deferrals, 1U);
    EXPECT_EQ(s.budget_yields_io, 1U);
    EXPECT_EQ(s.budget_yields_time, 1U);
    EXPECT_EQ(s.zero_progress_yields, 1U);
    const auto timing = s.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::PageWrite)];
    EXPECT_EQ(timing.count, 2U);
    EXPECT_EQ(timing.elapsed_ns, 10U);
    EXPECT_EQ(timing.max_ns, 7U);
    EXPECT_EQ(s.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::CleanDataSync)].elapsed_ns, 11U);
    EXPECT_EQ(s.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::CleanMetaFlush)].elapsed_ns, 13U);
    EXPECT_EQ(s.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::FuzzyFinalPublish)].elapsed_ns, 17U);
}
