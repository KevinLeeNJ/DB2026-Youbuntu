#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <type_traits>
#include <utility>

#include "common/transaction_phase_metrics.h"

using TransactionPhaseScope = TransactionPhaseMetrics::Scope;
static_assert(!std::is_copy_constructible_v<TransactionPhaseScope>);
static_assert(!std::is_copy_assignable_v<TransactionPhaseScope>);
static_assert(!std::is_move_constructible_v<TransactionPhaseScope>);
static_assert(!std::is_move_assignable_v<TransactionPhaseScope>);
static_assert(std::is_nothrow_destructible_v<TransactionPhaseScope>);

TEST(TransactionPhaseMetricsTest, ParsesExactOneOnly) {
    EXPECT_FALSE(TransactionPhaseMetrics::ParseEnabled(nullptr));
    EXPECT_FALSE(TransactionPhaseMetrics::ParseEnabled(""));
    EXPECT_FALSE(TransactionPhaseMetrics::ParseEnabled("0"));
    EXPECT_TRUE(TransactionPhaseMetrics::ParseEnabled("1"));
    EXPECT_FALSE(TransactionPhaseMetrics::ParseEnabled("01"));
    EXPECT_FALSE(TransactionPhaseMetrics::ParseEnabled("true"));
    EXPECT_FALSE(TransactionPhaseMetrics::ParseEnabled(" 1"));
}

TEST(TransactionPhaseMetricsTest, DisabledDoesNotRecord) {
    TransactionPhaseMetrics metrics(false);
    metrics.record(TransactionPhaseMetrics::Phase::RecordLockWait, 99);
    metrics.record_wait_for_graph(101, 128, 7, 3);
    metrics.record_owner_cleanup_terminal(true, 4, 1);
    metrics.record_inferred_successful_begin_commit_pair();

    EXPECT_EQ(metrics.snapshot(TransactionPhaseMetrics::Phase::RecordLockWait).count, 0);
    const auto graph = metrics.wait_for_graph_snapshot();
    EXPECT_EQ(graph.build.count, 0U);
    EXPECT_EQ(graph.shards.sum, 0U);
    EXPECT_EQ(graph.queues.sum, 0U);
    EXPECT_EQ(graph.edges.sum, 0U);
    const auto owner = metrics.owner_conflict_snapshot();
    EXPECT_EQ(owner.commit_cleanup_terminals, 0U);
    EXPECT_EQ(owner.observer_count, 0U);
    const auto read_only = metrics.read_only_wal_snapshot();
    EXPECT_EQ(read_only.inferred_successful_begin_commit_pairs, 0U);
}

TEST(TransactionPhaseMetricsTest, RecordsCumulativeAndMaximum) {
    TransactionPhaseMetrics metrics(true);
    metrics.record(TransactionPhaseMetrics::Phase::CommitWalWait, 4);
    metrics.record(TransactionPhaseMetrics::Phase::CommitWalWait, 9);
    const auto snapshot = metrics.snapshot(TransactionPhaseMetrics::Phase::CommitWalWait);
    EXPECT_EQ(snapshot.count, 2);
    EXPECT_EQ(snapshot.elapsed_ns, 13);
    EXPECT_EQ(snapshot.max_ns, 9);
    EXPECT_EQ(snapshot.histogram[0], 2);
}

TEST(TransactionPhaseMetricsTest, HistogramsUseDocumentedNanosecondBuckets) {
    TransactionPhaseMetrics metrics(true);
    constexpr std::array<uint64_t, TransactionPhaseMetrics::kHistogramBuckets> samples = {
        1'000,     4'000,      16'000,     64'000,      256'000,       1'000'000,
        4'000'000, 16'000'000, 64'000'000, 256'000'000, 1'000'000'000, 1'000'000'001,
    };
    for (const uint64_t sample : samples) {
        metrics.record(TransactionPhaseMetrics::Phase::CommitOwnerCleanup, sample);
    }
    const auto snapshot = metrics.snapshot(TransactionPhaseMetrics::Phase::CommitOwnerCleanup);
    EXPECT_EQ(snapshot.count, samples.size());
    for (const uint64_t bucket : snapshot.histogram) {
        EXPECT_EQ(bucket, 1U);
    }
}

TEST(TransactionPhaseMetricsTest, AggregatesWaitGraphOwnerTerminalAndReadOnlyWal) {
    TransactionPhaseMetrics metrics(true);
    metrics.record_wait_for_graph(19, 128, 7, 3);
    metrics.record_wait_for_graph(23, 128, 11, 5);
    const auto graph = metrics.wait_for_graph_snapshot();
    EXPECT_EQ(graph.build.count, 2U);
    EXPECT_EQ(graph.shards.sum, 256U);
    EXPECT_EQ(graph.shards.max, 128U);
    EXPECT_EQ(graph.queues.sum, 18U);
    EXPECT_EQ(graph.queues.max, 11U);
    EXPECT_EQ(graph.edges.sum, 8U);
    EXPECT_EQ(graph.edges.max, 5U);

    const uint64_t now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
    metrics.record_owner_cleanup_terminal(true, 4, now_ns);
    metrics.record_owner_cleanup_terminal(false, 2, now_ns);
    const auto owner = metrics.owner_conflict_snapshot();
    EXPECT_EQ(owner.commit_cleanup_terminals, 1U);
    EXPECT_EQ(owner.abort_cleanup_terminals, 1U);
    EXPECT_EQ(owner.observer_count, 6U);
    EXPECT_EQ(owner.observation_to_cleanup_terminal.count, 2U);

    metrics.record_inferred_successful_begin_commit_pair();
    const auto read_only = metrics.read_only_wal_snapshot();
    EXPECT_EQ(read_only.inferred_successful_begin_commit_pairs, 1U);
}

TEST(TransactionPhaseMetricsTest, ScopeFinishIsIdempotent) {
    TransactionPhaseMetrics metrics(true);
    TransactionPhaseMetrics::Scope scope(&metrics, TransactionPhaseMetrics::Phase::FrontierWait);
    scope.Finish();
    scope.Finish();
    EXPECT_EQ(metrics.snapshot(TransactionPhaseMetrics::Phase::FrontierWait).count, 1);
    TransactionPhaseMetrics disabled(false);
    TransactionPhaseMetrics::Scope disabled_scope(&disabled, TransactionPhaseMetrics::Phase::FrontierWait);
    disabled_scope.Finish();
    EXPECT_EQ(disabled.snapshot(TransactionPhaseMetrics::Phase::FrontierWait).count, 0);
}

TEST(TransactionPhaseMetricsTest, ScopeFactoryReturnsDirectPrvalue) {
    TransactionPhaseMetrics metrics(true);
    auto scope = metrics.scope(TransactionPhaseMetrics::Phase::ExecBatchWall);
    scope.Finish();
    EXPECT_EQ(metrics.snapshot(TransactionPhaseMetrics::Phase::ExecBatchWall).count, 1);
}
