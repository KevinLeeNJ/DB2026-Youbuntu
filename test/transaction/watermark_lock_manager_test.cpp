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
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "transaction/concurrency/lock_manager.h"
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

TEST(LockManagerMetadataTest, SerializableFirstRecordConflictReturnsImmediately) {
    LockManager lock_manager;
    Transaction owner(1061, IsolationLevel::SERIALIZABLE);
    Transaction contender(1062, IsolationLevel::SERIALIZABLE);
    Rid rid{7, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    EXPECT_FALSE(lock_manager.lock_exclusive_on_record(&contender, rid, 42));
    EXPECT_TRUE(contender.get_lock_set()->empty());
    const auto stats = lock_manager.record_lock_observability();
    EXPECT_EQ(stats.immediate_conflict, 1u);
    EXPECT_EQ(stats.wait_enqueued, 0u);
    EXPECT_EQ(stats.wait_granted, 0u);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
}

TEST(LockManagerMetadataTest, SiFirstRecordConflictReturnsImmediatelyWithoutHandoff) {
    LockManager lock_manager;
    Transaction owner(1071, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction contender(1072, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid rid{8, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    EXPECT_FALSE(lock_manager.lock_exclusive_on_record(&contender, rid, 42));
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));

    EXPECT_EQ(contender.get_lock_set()->count(lock_id), 0u);
    const auto stats = lock_manager.record_lock_observability();
    EXPECT_EQ(stats.wait_enqueued, 0u);
    EXPECT_EQ(stats.wait_granted, 0u);
    EXPECT_EQ(stats.immediate_conflict, 1u);
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
