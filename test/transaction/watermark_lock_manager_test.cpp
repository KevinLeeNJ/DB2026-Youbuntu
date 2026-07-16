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
