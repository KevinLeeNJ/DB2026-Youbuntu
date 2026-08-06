#include <gtest/gtest.h>

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
    EXPECT_EQ(metrics.snapshot(TransactionPhaseMetrics::Phase::RecordLockWait).count, 0);
}

TEST(TransactionPhaseMetricsTest, RecordsCumulativeAndMaximum) {
    TransactionPhaseMetrics metrics(true);
    metrics.record(TransactionPhaseMetrics::Phase::CommitWalWait, 4);
    metrics.record(TransactionPhaseMetrics::Phase::CommitWalWait, 9);
    const auto snapshot = metrics.snapshot(TransactionPhaseMetrics::Phase::CommitWalWait);
    EXPECT_EQ(snapshot.count, 2);
    EXPECT_EQ(snapshot.elapsed_ns, 13);
    EXPECT_EQ(snapshot.max_ns, 9);
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
