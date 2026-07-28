#include <algorithm>

#include <gtest/gtest.h>

#include "execution/execution_timing_diagnostics.h"

namespace {

static_assert(noexcept(execution_timing_diagnostics::snapshots()));

TEST(ExecutionTimingDiagnosticsTest, IsDisabledByDefaultAndRejectsFalseValues) {
    EXPECT_FALSE(execution_timing_diagnostics::configured_enabled(nullptr));
    EXPECT_FALSE(execution_timing_diagnostics::configured_enabled(""));
    EXPECT_FALSE(execution_timing_diagnostics::configured_enabled("0"));
    EXPECT_FALSE(execution_timing_diagnostics::configured_enabled("false"));
    EXPECT_FALSE(execution_timing_diagnostics::configured_enabled("off"));
    EXPECT_TRUE(execution_timing_diagnostics::configured_enabled("1"));
}

TEST(ExecutionTimingDiagnosticsTest, AggregatesByStatementAndPlan) {
    constexpr std::uint16_t statement_id = 63001;
    constexpr std::uint64_t plan_hash = 90001;
    execution_timing_diagnostics::Sample first;
    first.route = execution_timing_diagnostics::Route::PreparedPlan;
    first.fallback_reason = execution_timing_diagnostics::FallbackReason::Shape;
    first.total_ns = 100;
    first.bind_ns = 5;
    first.clone_bind_ns = 10;
    first.analyze_ns = 20;
    first.plan_ns = 30;
    first.instantiate_ns = 15;
    first.portal_run_ns = 25;
    first.executor_constructed = 3;
    first.executor_reused = 1;
    execution_timing_diagnostics::Sample second = first;
    second.total_ns = 200;

    execution_timing_diagnostics::observe(statement_id, plan_hash, first, false);
    execution_timing_diagnostics::observe(statement_id, plan_hash, second, true);
    execution_timing_diagnostics::observe_abort(statement_id, plan_hash, first.route, first.fallback_reason);
    execution_timing_diagnostics::observe_error(statement_id, plan_hash, first.route, first.fallback_reason);

    const auto snapshots = execution_timing_diagnostics::snapshots();
    const auto found = std::find_if(snapshots.begin(), snapshots.end(), [](const auto& snapshot) {
        return snapshot.statement_id == statement_id && snapshot.plan_hash == plan_hash;
    });
    ASSERT_NE(found, snapshots.end());
    EXPECT_EQ(found->invocations, 2);
    EXPECT_EQ(found->failures, 1);
    EXPECT_EQ(found->aborts, 1);
    EXPECT_EQ(found->errors, 1);
    EXPECT_EQ(found->route, execution_timing_diagnostics::Route::PreparedPlan);
    EXPECT_EQ(found->fallback_reason, execution_timing_diagnostics::FallbackReason::Shape);
    EXPECT_EQ(found->total_ns, 300);
    EXPECT_EQ(found->bind_ns, 10);
    EXPECT_EQ(found->clone_bind_ns, 20);
    EXPECT_EQ(found->analyze_ns, 40);
    EXPECT_EQ(found->plan_ns, 60);
    EXPECT_EQ(found->instantiate_ns, 30);
    EXPECT_EQ(found->portal_run_ns, 50);
    EXPECT_EQ(found->executor_constructed, 6);
    EXPECT_EQ(found->executor_reused, 2);
}

TEST(ExecutionTimingDiagnosticsTest, SeparatesRoutesAndFallbackReasons) {
    constexpr std::uint16_t statement_id = 63003;
    constexpr std::uint64_t plan_hash = 90003;
    execution_timing_diagnostics::Sample fallback;
    fallback.fallback_reason = execution_timing_diagnostics::FallbackReason::Bind;
    execution_timing_diagnostics::Sample prepared;
    prepared.route = execution_timing_diagnostics::Route::PreparedRuntime;

    execution_timing_diagnostics::observe(statement_id, plan_hash, fallback, false);
    execution_timing_diagnostics::observe(statement_id, plan_hash, prepared, false);

    const auto snapshots = execution_timing_diagnostics::snapshots();
    const auto count = std::count_if(snapshots.begin(), snapshots.end(), [](const auto& snapshot) {
        return snapshot.statement_id == statement_id && snapshot.plan_hash == plan_hash;
    });
    EXPECT_EQ(count, 2);
    EXPECT_STREQ(execution_timing_diagnostics::route_name(prepared.route), "prepared-runtime");
    EXPECT_STREQ(execution_timing_diagnostics::fallback_reason_name(fallback.fallback_reason), "bind");
}

TEST(ExecutionTimingDiagnosticsTest, OperationScopeRecordsSuccessAndUnwindingFailure) {
    constexpr std::uint16_t statement_id = 63002;
    constexpr std::uint64_t plan_hash = 90002;
    {
        execution_timing_diagnostics::OperationScope scope(statement_id, plan_hash);
        scope.sample().bind_ns = 7;
        scope.sample().clone_bind_ns = 11;
        scope.finish();
    }
    {
        execution_timing_diagnostics::OperationScope scope(statement_id, plan_hash);
        scope.sample().analyze_ns = 17;
    }

    const auto snapshots = execution_timing_diagnostics::snapshots();
    const auto found = std::find_if(snapshots.begin(), snapshots.end(), [](const auto& snapshot) {
        return snapshot.statement_id == statement_id && snapshot.plan_hash == plan_hash;
    });
    ASSERT_NE(found, snapshots.end());
    EXPECT_EQ(found->invocations, 2);
    EXPECT_EQ(found->failures, 1);
    EXPECT_EQ(found->bind_ns, 7);
    EXPECT_EQ(found->clone_bind_ns, 11);
    EXPECT_EQ(found->analyze_ns, 17);
}

TEST(ExecutionTimingDiagnosticsTest, PreparedRouteFailureUsesSingleAttributionKey) {
    constexpr std::uint16_t statement_id = 63004;
    constexpr std::uint64_t plan_hash = 90004;
    {
        execution_timing_diagnostics::OperationScope scope(statement_id, plan_hash,
                                                           execution_timing_diagnostics::Route::PreparedRuntime,
                                                           execution_timing_diagnostics::FallbackReason::Bind);
        scope.sample().bind_ns = 11;
        scope.finish_abort();
    }
    {
        execution_timing_diagnostics::OperationScope scope(statement_id, plan_hash,
                                                           execution_timing_diagnostics::Route::PreparedRuntime,
                                                           execution_timing_diagnostics::FallbackReason::Bind);
        scope.finish_error();
    }

    const auto snapshots = execution_timing_diagnostics::snapshots();
    const auto found = std::find_if(snapshots.begin(), snapshots.end(), [](const auto& snapshot) {
        return snapshot.statement_id == statement_id && snapshot.plan_hash == plan_hash;
    });
    ASSERT_NE(found, snapshots.end());
    EXPECT_EQ(found->route, execution_timing_diagnostics::Route::PreparedRuntime);
    EXPECT_EQ(found->fallback_reason, execution_timing_diagnostics::FallbackReason::Bind);
    EXPECT_EQ(found->invocations, 2);
    EXPECT_EQ(found->failures, 2);
    EXPECT_EQ(found->aborts, 1);
    EXPECT_EQ(found->errors, 1);
    EXPECT_EQ(found->bind_ns, 11);
}

} // namespace
