#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/transaction_abort_metrics.h"

static_assert(std::is_constructible_v<TransactionAbortException, txn_id_t, AbortReason>);
static_assert(std::is_constructible_v<TransactionAbortException, txn_id_t, AbortReason, AbortDetail, uint64_t>);

TEST(TransactionAbortMetricsTest, ParsesExactOneOnly) {
    EXPECT_FALSE(TransactionAbortMetrics::Config::ParseEnabled(nullptr));
    EXPECT_FALSE(TransactionAbortMetrics::Config::ParseEnabled(""));
    EXPECT_FALSE(TransactionAbortMetrics::Config::ParseEnabled("0"));
    EXPECT_FALSE(TransactionAbortMetrics::Config::ParseEnabled("01"));
    EXPECT_FALSE(TransactionAbortMetrics::Config::ParseEnabled("true"));
    EXPECT_TRUE(TransactionAbortMetrics::Config::ParseEnabled("1"));
}

TEST(TransactionAbortMetricsTest, DisabledDoesNotRecordAndMetadataIsBackwardCompatible) {
    TransactionAbortMetrics metrics({false});
    TransactionAbortException exception(7, AbortReason::WW_CONFLICT);
    EXPECT_EQ(exception.GetAbortDetail(), AbortDetail::UNKNOWN);
    EXPECT_EQ(exception.GetTriggeringTableRuntimeId(), 0u);
    metrics.record(exception);
    EXPECT_EQ(metrics.snapshot(AbortOrigin::EXEC_STREAM, AbortTxnMode::AUTOCOMMIT, DEFAULT_ISOLATION_LEVEL,
                               AbortOperation::OTHER, AbortReason::WW_CONFLICT, AbortDetail::UNKNOWN).count,
              0u);
}

TEST(TransactionAbortMetricsTest, ConcurrentRecordsPreserveGlobalAndTableOverflow) {
    TransactionAbortMetrics metrics({true});
    constexpr size_t kIds = TransactionAbortMetrics::kTableSlots + 1;
    std::vector<std::thread> threads;
    for (size_t id = 1; id <= kIds; ++id) {
        threads.emplace_back([&metrics, id] {
            TransactionAbortException exception(7, AbortReason::WW_CONFLICT, AbortDetail::WAITED_THEN_STALE, id);
            exception.SetObservation(AbortOrigin::EXEC_BATCH, AbortTxnMode::EXPLICIT, IsolationLevel::SNAPSHOT_ISOLATION,
                                     AbortOperation::UPDATE);
            metrics.record(exception);
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(metrics.snapshot(AbortOrigin::EXEC_BATCH, AbortTxnMode::EXPLICIT, IsolationLevel::SNAPSHOT_ISOLATION,
                               AbortOperation::UPDATE, AbortReason::WW_CONFLICT,
                               AbortDetail::WAITED_THEN_STALE).count,
              kIds);
    EXPECT_EQ(metrics.table_snapshot(TransactionAbortMetrics::kOverflowSlot, AbortReason::WW_CONFLICT,
                                     AbortDetail::WAITED_THEN_STALE).count,
              1u);
}

TEST(TransactionAbortMetricsTest, SameIdRacersConvergeAndInvalidEnumsAreIgnored) {
    TransactionAbortMetrics metrics({true});
    constexpr size_t kProducerCount = 32;
    std::mutex gate_latch;
    std::condition_variable ready_cv;
    std::condition_variable start_cv;
    size_t ready = 0;
    bool start = false;
    std::atomic<size_t> done{0};
    std::atomic<bool> observer_ready{false};
    std::atomic<bool> invalid_publication{false};
    std::thread observer([&] {
        {
            std::lock_guard<std::mutex> lock(gate_latch);
            observer_ready.store(true, std::memory_order_release);
        }
        ready_cv.notify_one();
        while (done.load(std::memory_order_acquire) != kProducerCount) {
            for (size_t slot = 0; slot < TransactionAbortMetrics::kTableSlots; ++slot) {
                const auto cell =
                    metrics.table_snapshot(slot, AbortReason::WW_CONFLICT, AbortDetail::IMMEDIATE_ACTIVE_OWNER);
                if (cell.count != 0 && cell.runtime_id != 73) invalid_publication.store(true);
            }
            if (metrics.table_snapshot(TransactionAbortMetrics::kUnknownSlot, AbortReason::WW_CONFLICT,
                                       AbortDetail::IMMEDIATE_ACTIVE_OWNER).count != 0 ||
                metrics.table_snapshot(TransactionAbortMetrics::kOverflowSlot, AbortReason::WW_CONFLICT,
                                       AbortDetail::IMMEDIATE_ACTIVE_OWNER).count != 0) {
                invalid_publication.store(true);
            }
            std::this_thread::yield();
        }
    });
    std::vector<std::thread> threads;
    for (size_t i = 0; i < kProducerCount; ++i) threads.emplace_back([&] {
        {
            std::unique_lock<std::mutex> lock(gate_latch);
            ++ready;
            ready_cv.notify_one();
            start_cv.wait(lock, [&] { return start; });
        }
        TransactionAbortException exception(1, AbortReason::WW_CONFLICT, AbortDetail::IMMEDIATE_ACTIVE_OWNER, 73);
        metrics.record(exception);
        done.fetch_add(1, std::memory_order_release);
    });
    bool all_ready = false;
    {
        std::unique_lock<std::mutex> lock(gate_latch);
        all_ready = ready_cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return ready == kProducerCount && observer_ready.load(std::memory_order_acquire);
        });
        start = true;
    }
    start_cv.notify_all();
    for (auto& thread : threads) thread.join();
    observer.join();
    ASSERT_TRUE(all_ready);
    uint64_t precise = 0;
    size_t matching_slots = 0;
    for (size_t slot = 0; slot < TransactionAbortMetrics::kTableSlots; ++slot) {
        const auto cell = metrics.table_snapshot(slot, AbortReason::WW_CONFLICT, AbortDetail::IMMEDIATE_ACTIVE_OWNER);
        if (cell.runtime_id == 73) { ++matching_slots; precise += cell.count; }
    }
    EXPECT_EQ(matching_slots, 1u);
    EXPECT_FALSE(invalid_publication.load());
    EXPECT_EQ(precise, kProducerCount);
    TransactionAbortException invalid(1, static_cast<AbortReason>(255), static_cast<AbortDetail>(255), 1);
    invalid.SetObservation(static_cast<AbortOrigin>(255), static_cast<AbortTxnMode>(255),
                           static_cast<IsolationLevel>(255), static_cast<AbortOperation>(255));
    metrics.record(invalid);
    EXPECT_EQ(metrics.snapshot(static_cast<AbortOrigin>(255), AbortTxnMode::AUTOCOMMIT, DEFAULT_ISOLATION_LEVEL,
                               AbortOperation::OTHER, AbortReason::WW_CONFLICT, AbortDetail::UNKNOWN).count,
              0u);
}

TEST(TransactionAbortMetricsTest, UnknownAndOverflowUseDistinctBuckets) {
    TransactionAbortMetrics metrics({true});
    TransactionAbortException unknown(1, AbortReason::WW_CONFLICT, AbortDetail::UNKNOWN, 0);
    metrics.record(unknown);
    for (size_t id = 1; id <= TransactionAbortMetrics::kTableSlots + 1; ++id) {
        TransactionAbortException exception(1, AbortReason::WW_CONFLICT, AbortDetail::UNKNOWN, id);
        metrics.record(exception);
    }
    EXPECT_EQ(metrics.table_snapshot(TransactionAbortMetrics::kUnknownSlot, AbortReason::WW_CONFLICT,
                                     AbortDetail::UNKNOWN).count, 1u);
    EXPECT_EQ(metrics.table_snapshot(TransactionAbortMetrics::kOverflowSlot, AbortReason::WW_CONFLICT,
                                     AbortDetail::UNKNOWN).count, 1u);
}
