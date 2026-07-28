#include <gtest/gtest.h>

#include "transaction/commit_timing_diagnostics.h"

TEST(CommitTimingDiagnosticsTest, IsDisabledByDefaultAndRejectsFalseValues) {
    EXPECT_FALSE(commit_timing_diagnostics::configured_enabled(nullptr));
    EXPECT_FALSE(commit_timing_diagnostics::configured_enabled(""));
    EXPECT_FALSE(commit_timing_diagnostics::configured_enabled("0"));
    EXPECT_FALSE(commit_timing_diagnostics::configured_enabled("false"));
    EXPECT_FALSE(commit_timing_diagnostics::configured_enabled("off"));
    EXPECT_TRUE(commit_timing_diagnostics::configured_enabled("1"));
}

TEST(CommitTimingDiagnosticsTest, AggregatesSamplesWithoutACollectorLock) {
    commit_timing_diagnostics::reset_for_test();

    commit_timing_diagnostics::Sample sample;
    sample.total_ns = 10;
    sample.prepare_publication_ns = 1;
    sample.timestamp_csn_ns = 2;
    sample.wal_ns = 3;
    sample.tuple_publication_ns = 4;
    sample.frontier_publication_ns = 5;
    sample.frontier_wait_ns = 6;
    sample.cleanup_ns = 7;

    commit_timing_diagnostics::observe(sample, false);
    commit_timing_diagnostics::observe(sample, true);

    const auto snapshot = commit_timing_diagnostics::snapshot();
    EXPECT_EQ(snapshot.invocations, 2);
    EXPECT_EQ(snapshot.failures, 1);
    EXPECT_EQ(snapshot.total_ns, 20);
    EXPECT_EQ(snapshot.prepare_publication_ns, 2);
    EXPECT_EQ(snapshot.timestamp_csn_ns, 4);
    EXPECT_EQ(snapshot.wal_ns, 6);
    EXPECT_EQ(snapshot.tuple_publication_ns, 8);
    EXPECT_EQ(snapshot.frontier_publication_ns, 10);
    EXPECT_EQ(snapshot.frontier_wait_ns, 12);
    EXPECT_EQ(snapshot.cleanup_ns, 14);
}

TEST(CommitTimingDiagnosticsTest, DisabledTemplatePathDoesNotRecord) {
    commit_timing_diagnostics::reset_for_test();

    commit_timing_diagnostics::OperationScope<false> scope;
    { commit_timing_diagnostics::StageTimer<false> timer(scope, commit_timing_diagnostics::Stage::WAL); }
    scope.finish();

    EXPECT_EQ(commit_timing_diagnostics::snapshot().invocations, 0);
}

TEST(CommitTimingDiagnosticsTest, UnwindingEnabledScopeRecordsFailure) {
    commit_timing_diagnostics::reset_for_test();

    {
        commit_timing_diagnostics::OperationScope<true> scope;
        commit_timing_diagnostics::StageTimer<true> timer(scope, commit_timing_diagnostics::Stage::CLEANUP);
    }

    const auto snapshot = commit_timing_diagnostics::snapshot();
    EXPECT_EQ(snapshot.invocations, 1);
    EXPECT_EQ(snapshot.failures, 1);
}
