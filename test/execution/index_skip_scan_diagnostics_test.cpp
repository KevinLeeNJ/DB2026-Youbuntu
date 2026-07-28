#include <gtest/gtest.h>

#include "execution/index_skip_scan_diagnostics.h"

namespace {

static_assert(noexcept(index_skip_scan_diagnostics::enabled()));
static_assert(noexcept(index_skip_scan_diagnostics::snapshots()));

TEST(IndexSkipScanDiagnosticsTest, StatementScopeRestoresNestedStatementId) {
    EXPECT_EQ(index_skip_scan_diagnostics::current_statement_id(), 0);
    EXPECT_EQ(index_skip_scan_diagnostics::current_plan_hash(), 0);
    {
        index_skip_scan_diagnostics::StatementScope outer(64001, 1001);
        EXPECT_EQ(index_skip_scan_diagnostics::current_statement_id(), 64001);
        EXPECT_EQ(index_skip_scan_diagnostics::current_plan_hash(), 1001);
        {
            index_skip_scan_diagnostics::StatementScope inner(64002, 1002);
            EXPECT_EQ(index_skip_scan_diagnostics::current_statement_id(), 64002);
            EXPECT_EQ(index_skip_scan_diagnostics::current_plan_hash(), 1002);
        }
        EXPECT_EQ(index_skip_scan_diagnostics::current_statement_id(), 64001);
        EXPECT_EQ(index_skip_scan_diagnostics::current_plan_hash(), 1001);
    }
    EXPECT_EQ(index_skip_scan_diagnostics::current_statement_id(), 0);
    EXPECT_EQ(index_skip_scan_diagnostics::current_plan_hash(), 0);
}

TEST(IndexSkipScanDiagnosticsTest, AggregatesBuildCountersByStatementAndAccessPath) {
    constexpr std::uint16_t statement_id = 64003;
    constexpr std::uint64_t plan_hash = 2001;
    index_skip_scan_diagnostics::observe_build(statement_id, plan_hash, "order_line", "order_line_ol_o_id", 2, 5, 3, 17,
                                               100);
    index_skip_scan_diagnostics::observe_build(statement_id, plan_hash, "order_line", "order_line_ol_o_id", 2, 7, 4, 23,
                                               250);

    const auto snapshots = index_skip_scan_diagnostics::snapshots();
    const auto found = std::find_if(snapshots.begin(), snapshots.end(), [](const auto& snapshot) {
        return snapshot.statement_id == statement_id && snapshot.plan_hash == plan_hash &&
               snapshot.table_name == "order_line" && snapshot.index_name == "order_line_ol_o_id" &&
               snapshot.prefix_column_count == 2;
    });
    ASSERT_NE(found, snapshots.end());
    EXPECT_EQ(found->invocations, 2);
    EXPECT_EQ(found->prefixes, 12);
    EXPECT_EQ(found->ranges, 7);
    EXPECT_EQ(found->descents, 40);
    EXPECT_EQ(found->build_ns, 350);
    EXPECT_EQ(found->build_ns_max, 250);
}

TEST(IndexSkipScanDiagnosticsTest, DeduplicatesPreparedPlanMappings) {
    const std::string mapping =
        "skipdiag_prepare statement_id=64004 root=Select node=IndexSkipScan table=t index_cols=a,b equality_mask=01";
    EXPECT_TRUE(index_skip_scan_diagnostics::register_prepared_plan(mapping));
    EXPECT_FALSE(index_skip_scan_diagnostics::register_prepared_plan(mapping));
}

} // namespace
