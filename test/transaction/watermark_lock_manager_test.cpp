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
#include "transaction/transaction_manager.h"
#include "transaction/watermark.h"

TEST(UniqueKeyIdTest, InlineAndFallbackPreserveBinaryIdentity) {
    std::vector<char> inline_key(UniqueKeyId::INLINE_KEY_CAPACITY);
    std::vector<char> overflow_key(UniqueKeyId::INLINE_KEY_CAPACITY + 1);
    for (size_t index = 0; index < overflow_key.size(); ++index) {
        overflow_key[index] = static_cast<char>(index);
        if (index < inline_key.size()) {
            inline_key[index] = static_cast<char>(index);
        }
    }

    UniqueKeyId inline_id(7, inline_key);
    UniqueKeyId same_inline_id(7, inline_key);
    UniqueKeyId different_index_id(8, inline_key);
    UniqueKeyId overflow_id(7, overflow_key);
    UniqueKeyId same_overflow_id(7, overflow_key);
    overflow_key.back() = static_cast<char>(overflow_key.back() + 1);
    UniqueKeyId different_overflow_id(7, overflow_key);

    EXPECT_TRUE(inline_id.uses_inline_storage());
    EXPECT_FALSE(overflow_id.uses_inline_storage());
    EXPECT_EQ(inline_id, same_inline_id);
    EXPECT_EQ(UniqueKeyIdHash{}(inline_id), UniqueKeyIdHash{}(same_inline_id));
    EXPECT_FALSE(inline_id == different_index_id);
    EXPECT_EQ(overflow_id, same_overflow_id);
    EXPECT_EQ(UniqueKeyIdHash{}(overflow_id), UniqueKeyIdHash{}(same_overflow_id));
    EXPECT_FALSE(overflow_id == different_overflow_id);
}

TEST(LockManagerMetadataTest, DuplicateUniqueReservationsUseSingleTransactionEntry) {
    LockManager lock_manager;
    Transaction owner(900, IsolationLevel::READ_COMMITTED);
    const int index_fd = 31;
    const std::vector<char> inline_key{'a', '\0', 'b'};
    const std::vector<char> overflow_key(48, 'z');
    const UniqueKeyId inline_id(index_fd, inline_key);
    const UniqueKeyId overflow_id(index_fd, overflow_key);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&owner, index_fd, inline_key));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&owner, index_fd, inline_key));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&owner, index_fd, overflow_key));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(&owner, index_fd, overflow_key));
    EXPECT_EQ(owner.get_unique_key_lock_set()->size(), 2u);
    EXPECT_EQ(owner.get_unique_key_lock_set()->count(inline_id), 1u);
    EXPECT_EQ(owner.get_unique_key_lock_set()->count(overflow_id), 1u);

    EXPECT_TRUE(lock_manager.unlock_unique_key(&owner, inline_id));
    EXPECT_TRUE(lock_manager.unlock_unique_key(&owner, overflow_id));
    EXPECT_TRUE(owner.get_unique_key_lock_set()->empty());
}

TEST(LockManagerMetadataTest, AbortReleasesInlineAndFallbackUniqueReservations) {
    LockManager lock_manager;
    TransactionManager txn_manager(&lock_manager, nullptr);
    const int index_fd = 32;
    const std::vector<char> inline_key{'r', '\0', 'b'};
    const std::vector<char> overflow_key(64, 'q');

    Transaction* owner = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(owner, index_fd, inline_key));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_unique_key(owner, index_fd, overflow_key));
    txn_manager.abort(owner, nullptr);

    Transaction* next = txn_manager.begin(nullptr, nullptr, IsolationLevel::READ_COMMITTED);
    EXPECT_TRUE(lock_manager.lock_exclusive_on_unique_key(next, index_fd, inline_key));
    EXPECT_TRUE(lock_manager.lock_exclusive_on_unique_key(next, index_fd, overflow_key));
    txn_manager.abort(next, nullptr);
}

TEST(LockManagerMetadataTest, ReacquiringOwnedRecordLockIsIdempotent) {
    LockManager lock_manager;
    Transaction owner(1000, IsolationLevel::READ_COMMITTED);
    Transaction waiter(1001, IsolationLevel::READ_COMMITTED);
    waiter.set_txn_mode(true);
    Rid rid{0, 7};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    EXPECT_EQ(owner.get_lock_set()->count(lock_id), 1u);

    std::atomic<bool> waiter_started{false};
    std::atomic<bool> waiter_acquired{false};
    std::thread waiter_thread([&] {
        waiter_started.store(true, std::memory_order_release);
        waiter_acquired.store(lock_manager.lock_exclusive_on_record(&waiter, rid, 42), std::memory_order_release);
    });

    while (!waiter_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(waiter_acquired.load(std::memory_order_acquire));

    ASSERT_TRUE(lock_manager.unlock(&owner, lock_id));
    waiter_thread.join();
    ASSERT_TRUE(waiter_acquired.load(std::memory_order_acquire));
    EXPECT_TRUE(lock_manager.unlock(&waiter, lock_id));
}

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
