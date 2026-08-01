/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/watermark.h"

TEST(LockManagerMetadataTest, DuplicateUnlockAfterOwnerHandoffDoesNotReleaseNewOwner) {
    LockManager lock_manager;
    Transaction owner(1001, IsolationLevel::READ_COMMITTED);
    Transaction waiter(1002, IsolationLevel::READ_COMMITTED);
    waiter.set_txn_mode(true);
    Rid rid{1, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    std::atomic<bool> waiter_started{false};
    std::atomic<bool> waiter_acquired{false};
    std::thread waiter_thread([&] {
        waiter_started.store(true);
        waiter_acquired.store(lock_manager.lock_exclusive_on_record(&waiter, rid, 42));
    });

    while (!waiter_started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(lock_manager.unlock(&owner, lock_id));
    waiter_thread.join();

    ASSERT_TRUE(waiter_acquired.load());
    EXPECT_FALSE(lock_manager.unlock(&owner, lock_id));
    EXPECT_EQ(waiter.get_lock_set()->count(lock_id), 1u);
    EXPECT_TRUE(lock_manager.unlock(&waiter, lock_id));
}

TEST(LockManagerMetadataTest, CancellingWaitingRecordLockLeavesQueueUsable) {
    LockManager lock_manager;
    Transaction owner(1011, IsolationLevel::READ_COMMITTED);
    Transaction cancelled(1012, IsolationLevel::READ_COMMITTED);
    Transaction next(1013, IsolationLevel::READ_COMMITTED);
    cancelled.set_txn_mode(true);
    next.set_txn_mode(true);
    Rid rid{2, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    std::atomic<bool> waiter_started{false};
    std::atomic<bool> waiter_acquired{true};
    std::thread waiter_thread([&] {
        waiter_started.store(true);
        waiter_acquired.store(lock_manager.lock_exclusive_on_record(&cancelled, rid, 42));
    });

    while (!waiter_started.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    lock_manager.cancel_transaction(&cancelled);
    waiter_thread.join();

    EXPECT_FALSE(waiter_acquired.load());
    EXPECT_EQ(cancelled.get_lock_set()->count(lock_id), 0u);
    ASSERT_TRUE(lock_manager.unlock(&owner, lock_id));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&next, rid, 42));
    EXPECT_TRUE(lock_manager.unlock(&next, lock_id));
}

TEST(LockManagerMetadataTest, CancellationDuringOwnerHandoffReleasesGrantedOwner) {
    LockManager lock_manager;
    Transaction owner(1021, IsolationLevel::READ_COMMITTED);
    Transaction waiter(1022, IsolationLevel::READ_COMMITTED);
    Transaction next(1023, IsolationLevel::READ_COMMITTED);
    const Rid rid{3, 0};
    const LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    bool handoff_entered = false;
    bool release_handoff = false;
    lock_manager.set_record_handoff_published_test_hook([&] {
        std::unique_lock<std::mutex> lock(hook_latch);
        handoff_entered = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_handoff; });
    });

    LockAcquireResult waiter_result = LockAcquireResult::Value::Granted;
    std::thread waiter_thread([&] { waiter_result = lock_manager.lock_exclusive_on_record(&waiter, rid, 42); });
    while (lock_manager.record_lock_observability().wait_enqueued != 1) {
        std::this_thread::yield();
    }
    std::thread owner_release([&] { EXPECT_TRUE(lock_manager.unlock(&owner, lock_id)); });
    {
        std::unique_lock<std::mutex> lock(hook_latch);
        hook_cv.wait(lock, [&] { return handoff_entered; });
    }
    std::thread canceller([&] { lock_manager.cancel_transaction(&waiter); });
    while (!waiter.is_lock_cancellation_requested()) {
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> lock(hook_latch);
        release_handoff = true;
    }
    hook_cv.notify_all();
    owner_release.join();
    canceller.join();
    waiter_thread.join();

    EXPECT_EQ(waiter_result.value(), LockAcquireResult::Value::Cancelled);
    EXPECT_TRUE(waiter.get_lock_set()->empty());
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&next, rid, 42));
    EXPECT_TRUE(lock_manager.unlock(&next, lock_id));
}

TEST(LockManagerMetadataTest, DeadlockVictimDuringOwnerHandoffReleasesGrantedOwner) {
    LockManager lock_manager;
    Transaction owner(1031, IsolationLevel::READ_COMMITTED);
    Transaction victim(1032, IsolationLevel::READ_COMMITTED);
    Transaction next(1033, IsolationLevel::READ_COMMITTED);
    const Rid rid{4, 0};
    const LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    bool pre_notify = false;
    bool release_notify = false;
    lock_manager.set_record_handoff_pre_notify_test_hook([&] {
        std::unique_lock<std::mutex> lock(hook_latch);
        pre_notify = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_notify; });
    });

    LockAcquireResult victim_result = LockAcquireResult::Value::Granted;
    std::thread victim_thread([&] { victim_result = lock_manager.lock_exclusive_on_record(&victim, rid, 42); });
    while (lock_manager.record_lock_observability().wait_enqueued != 1) {
        std::this_thread::yield();
    }
    std::thread owner_release([&] { EXPECT_TRUE(lock_manager.unlock(&owner, lock_id)); });
    {
        std::unique_lock<std::mutex> lock(hook_latch);
        hook_cv.wait(lock, [&] { return pre_notify; });
    }
    std::thread detector([&] { lock_manager.cancel_waiting_transaction_for_test(victim.get_transaction_id()); });
    detector.join();
    {
        std::lock_guard<std::mutex> lock(hook_latch);
        release_notify = true;
    }
    hook_cv.notify_all();
    owner_release.join();
    victim_thread.join();

    EXPECT_EQ(victim_result.value(), LockAcquireResult::Value::DeadlockVictim);
    EXPECT_TRUE(victim.is_lock_cancellation_requested());
    EXPECT_TRUE(victim.get_lock_set()->empty());
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&next, rid, 42));
    EXPECT_TRUE(lock_manager.unlock(&next, lock_id));
}

TEST(LockManagerMetadataTest, StaleCycleCancellationAfterHandoffCompletionIsNoOp) {
    LockManager lock_manager;
    Transaction owner(1034, IsolationLevel::READ_COMMITTED);
    Transaction waiter(1035, IsolationLevel::READ_COMMITTED);
    Transaction next(1036, IsolationLevel::READ_COMMITTED);
    const Rid rid{5, 0};
    const LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    bool check_entered = false;
    bool release_check = false;
    bool canceller_snapshot = false;
    bool release_canceller = false;
    lock_manager.set_record_handoff_checked_test_hook([&] {
        std::unique_lock<std::mutex> lock(hook_latch);
        check_entered = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_check; });
    });
    lock_manager.set_cycle_cancel_before_record_queue_test_hook([&] {
        std::unique_lock<std::mutex> lock(hook_latch);
        canceller_snapshot = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_canceller; });
    });

    LockAcquireResult waiter_result = LockAcquireResult::Value::Cancelled;
    std::thread waiter_thread([&] { waiter_result = lock_manager.lock_exclusive_on_record(&waiter, rid, 42); });
    while (lock_manager.record_lock_observability().wait_enqueued != 1) {
        std::this_thread::yield();
    }
    std::thread owner_release([&] { EXPECT_TRUE(lock_manager.unlock(&owner, lock_id)); });
    {
        std::unique_lock<std::mutex> lock(hook_latch);
        hook_cv.wait(lock, [&] { return check_entered; });
    }
    const auto victims_before = lock_manager.record_lock_observability().cycle_victims;
    std::thread canceller([&] { lock_manager.cancel_waiting_transaction_for_test(waiter.get_transaction_id()); });
    {
        std::unique_lock<std::mutex> lock(hook_latch);
        hook_cv.wait(lock, [&] { return canceller_snapshot; });
    }
    {
        std::lock_guard<std::mutex> lock(hook_latch);
        release_check = true;
    }
    hook_cv.notify_all();
    owner_release.join();
    waiter_thread.join();
    {
        std::lock_guard<std::mutex> lock(hook_latch);
        release_canceller = true;
    }
    hook_cv.notify_all();
    canceller.join();

    EXPECT_EQ(waiter_result.value(), LockAcquireResult::Value::Granted);
    EXPECT_FALSE(waiter.is_lock_cancellation_requested());
    EXPECT_EQ(lock_manager.record_lock_observability().cycle_victims, victims_before);
    EXPECT_EQ(waiter.get_lock_set()->count(lock_id), 1u);
    EXPECT_TRUE(lock_manager.unlock(&waiter, lock_id));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&next, rid, 42));
    EXPECT_TRUE(lock_manager.unlock(&next, lock_id));
}

TEST(LockManagerMetadataTest, CancelledTransactionCannotAcquireUnownedLocks) {
    LockManager lock_manager;
    Transaction cancelled(1044, IsolationLevel::READ_COMMITTED);
    const Rid rid{5, 1};
    const std::vector<char> unique_key{'k'};

    lock_manager.cancel_transaction(&cancelled);

    EXPECT_FALSE(lock_manager.lock_exclusive_on_record(&cancelled, rid, 42));
    EXPECT_FALSE(lock_manager.lock_exclusive_on_unique_key(&cancelled, 43, unique_key));
    EXPECT_TRUE(cancelled.get_lock_set()->empty());
    EXPECT_TRUE(cancelled.get_unique_key_lock_set()->empty());
}

TEST(LockManagerMetadataTest, UniqueHandoffPublishesBeforeAbortCanRetireWaiter) {
    LockManager lock_manager;
    TransactionManager transaction_manager(&lock_manager, nullptr);
    Transaction* owner = transaction_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
    Transaction* waiter = transaction_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
    Transaction* next = transaction_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
    const std::vector<char> key{'p', 'u', 'b'};
    constexpr int index_fd = 43;
    const txn_id_t waiter_id = waiter->get_transaction_id();
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(owner, index_fd, key));

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    bool published = false;
    bool release = false;
    lock_manager.set_unique_handoff_published_test_hook([&] {
        std::unique_lock<std::mutex> lock(hook_latch);
        published = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release; });
    });

    std::atomic<bool> waiter_result{false};
    std::thread waiter_thread([&] { waiter_result.store(lock_manager.lock_exclusive_on_unique_key(waiter, index_fd, key)); });
    while (lock_manager.unique_key_lock_observability().wait_enqueued != 1) {
        std::this_thread::yield();
    }
    const std::string lock_id = *owner->get_unique_key_lock_set()->begin();
    ASSERT_TRUE(lock_manager.unlock_unique_key(owner, lock_id));
    {
        std::unique_lock<std::mutex> lock(hook_latch);
        hook_cv.wait(lock, [&] { return published; });
    }
    auto abort_waiter = std::async(std::launch::async, [&] { transaction_manager.abort(waiter, nullptr); });
    while (!waiter->is_lock_cancellation_requested()) {
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> lock(hook_latch);
        release = true;
    }
    hook_cv.notify_all();
    waiter_thread.join();
    abort_waiter.get();

    EXPECT_TRUE(waiter_result.load());
    EXPECT_EQ(transaction_manager.get_transaction(waiter_id), nullptr);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(next, index_fd, key));
    EXPECT_TRUE(lock_manager.unlock_unique_key(next, lock_id));
    transaction_manager.abort(owner, nullptr);
    transaction_manager.abort(next, nullptr);
}

TEST(LockManagerMetadataTest, UniqueCycleCancellationPublishesFlagBeforeWakeup) {
    LockManager lock_manager;
    Transaction owner(1081, IsolationLevel::READ_COMMITTED);
    Transaction victim(1082, IsolationLevel::READ_COMMITTED);
    const std::vector<char> key{'w', 'a', 'k', 'e'};
    constexpr int index_fd = 44;
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&owner, index_fd, key));

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    bool removed = false;
    bool release_flag = false;
    lock_manager.set_cycle_cancel_before_flag_test_hook([&] {
        std::unique_lock<std::mutex> lock(hook_latch);
        removed = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_flag; });
    });
    std::atomic<bool> result{true};
    std::thread waiter([&] { result.store(lock_manager.lock_exclusive_on_unique_key(&victim, index_fd, key)); });
    while (lock_manager.unique_key_lock_observability().wait_enqueued != 1) std::this_thread::yield();
    std::thread detector([&] { lock_manager.cancel_waiting_transaction_for_test(victim.get_transaction_id()); });
    {
        std::unique_lock<std::mutex> lock(hook_latch);
        hook_cv.wait(lock, [&] { return removed; });
    }
    EXPECT_FALSE(victim.is_lock_cancellation_requested());
    {
        std::lock_guard<std::mutex> lock(hook_latch);
        release_flag = true;
    }
    hook_cv.notify_all();
    detector.join();
    waiter.join();
    EXPECT_FALSE(result.load());
    EXPECT_TRUE(victim.is_lock_cancellation_requested());
    EXPECT_TRUE(lock_manager.unlock_unique_key(&owner, *owner.get_unique_key_lock_set()->begin()));
}

TEST(LockManagerMetadataTest, SerializableFirstRecordConflictReturnsWriteConflictImmediately) {
    LockManager lock_manager;
    Transaction owner(1061, IsolationLevel::SERIALIZABLE);
    Transaction contender(1062, IsolationLevel::SERIALIZABLE);
    Rid rid{7, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    const LockAcquireResult result = lock_manager.lock_exclusive_on_record(&contender, rid, 42);
    const auto stats = lock_manager.record_lock_observability();
    EXPECT_EQ(result.value(), LockAcquireResult::Value::WriteConflict);
    EXPECT_EQ(stats.immediate_conflict, 1u);
    EXPECT_EQ(stats.wait_enqueued, 0u);
    EXPECT_EQ(stats.cycle_checks, 0u);
    EXPECT_EQ(owner.get_lock_set()->count(lock_id), 1u);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
    EXPECT_TRUE(contender.get_lock_set()->empty());
}

TEST(LockManagerMetadataTest, SiFirstRecordConflictWaitsOnce) {
    LockManager lock_manager;
    Transaction owner(1073, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction contender(1074, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction next(1075, IsolationLevel::SNAPSHOT_ISOLATION);
    const Rid rid{7, 0};
    const LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    LockAcquireResult contender_result = LockAcquireResult::Value::Cancelled;
    std::thread contender_thread([&] {
        contender_result = lock_manager.lock_exclusive_on_record(&contender, rid, 42);
    });

    while (lock_manager.record_lock_observability().wait_enqueued != 1) {
        std::this_thread::yield();
    }
    const auto queued = lock_manager.record_lock_observability();
    EXPECT_EQ(queued.immediate_conflict, 0u);
    EXPECT_EQ(queued.cycle_checks, 0u);

    ASSERT_TRUE(lock_manager.unlock(&owner, lock_id));
    contender_thread.join();

    EXPECT_EQ(contender_result.value(), LockAcquireResult::Value::Granted);
    EXPECT_EQ(contender.get_lock_set()->count(lock_id), 1u);
    EXPECT_TRUE(lock_manager.unlock(&contender, lock_id));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&next, rid, 42));
    EXPECT_TRUE(lock_manager.unlock(&next, lock_id));
}

TEST(WatermarkTest, ConcurrentReadTimestampUpdatesPreserveActiveReaders) {
    constexpr int reader_count = 16;
    constexpr timestamp_t initial_read_ts = 100;
    Watermark watermark(1000);

    for (int i = 0; i < reader_count; ++i) {
        watermark.AddTxn(initial_read_ts);
    }

    std::vector<std::thread> readers;
    for (int i = 0; i < reader_count; ++i) {
        readers.emplace_back([&, i] { watermark.UpdateTxnReadTs(initial_read_ts, 200 + i); });
    }
    for (auto& reader : readers) {
        reader.join();
    }

    EXPECT_EQ(watermark.GetWatermark(), 200);
    for (int i = 0; i < reader_count; ++i) {
        watermark.RemoveTxn(200 + i);
    }
    EXPECT_EQ(watermark.GetWatermark(), 1000);
}

TEST(WatermarkTest, StableSlotsHandleDuplicateReadTimestamps) {
    Watermark watermark(1000, 2);
    const size_t first = watermark.AddTxnSlot(100);
    const size_t second = watermark.AddTxnSlot(100);

    watermark.RemoveTxnSlot(first);
    EXPECT_EQ(watermark.GetWatermark(), 100);
    watermark.UpdateTxnReadTsSlot(second, 200);
    EXPECT_EQ(watermark.GetWatermark(), 200);
    watermark.RemoveTxnSlot(second);
    EXPECT_EQ(watermark.GetWatermark(), 1000);
}
