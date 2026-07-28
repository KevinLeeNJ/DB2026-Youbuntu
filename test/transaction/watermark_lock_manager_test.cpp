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
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/watermark.h"

namespace {

class ScopedEnvironmentValue {
public:
    ScopedEnvironmentValue(const char* name, const char* value) : name_(name) {
        if (const char* old_value = std::getenv(name_.c_str()); old_value != nullptr) {
            old_value_ = old_value;
        }
        setenv(name_.c_str(), value, 1);
    }

    ~ScopedEnvironmentValue() {
        if (old_value_.has_value()) {
            setenv(name_.c_str(), old_value_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

private:
    std::string name_;
    std::optional<std::string> old_value_;
};

} // namespace

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

TEST(LockManagerMetadataTest, SiFirstConflictBackoffRemainsBoundedAndUnqueued) {
    LockManager lock_manager(std::chrono::microseconds(2000));
    Transaction owner(1021, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction contender(1022, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid rid{3, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    const auto begin = std::chrono::steady_clock::now();
    EXPECT_FALSE(lock_manager.lock_exclusive_on_record(&contender, rid, 42));
    const auto elapsed = std::chrono::steady_clock::now() - begin;

    EXPECT_GE(elapsed, std::chrono::milliseconds(1));
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
    EXPECT_EQ(contender.get_lock_set()->count(lock_id), 0u);
    const auto stats = lock_manager.record_lock_observability();
    EXPECT_EQ(stats.immediate_conflict, 1u);
    EXPECT_EQ(stats.wait_enqueued, 0u);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
}

TEST(LockManagerMetadataTest, SiFirstConflictClaimsLockReleasedDuringBackoff) {
    LockManager lock_manager(std::chrono::microseconds(2000));
    Transaction owner(1031, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction contender(1032, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid rid{4, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    std::atomic<bool> contender_started{false};
    std::atomic<bool> contender_acquired{false};
    std::thread contender_thread([&] {
        contender_started.store(true, std::memory_order_release);
        contender_acquired.store(lock_manager.lock_exclusive_on_record(&contender, rid, 42), std::memory_order_release);
    });

    while (!contender_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (lock_manager.record_lock_observability().backoff_waits == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(lock_manager.record_lock_observability().backoff_waits, 1u);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
    contender_thread.join();

    EXPECT_TRUE(contender_acquired.load(std::memory_order_acquire));
    EXPECT_EQ(contender.get_lock_set()->count(lock_id), 1u);
    const auto stats = lock_manager.record_lock_observability();
    EXPECT_EQ(stats.immediate_conflict, 0u);
    EXPECT_EQ(stats.wait_enqueued, 0u);
    EXPECT_TRUE(lock_manager.unlock(&contender, lock_id));
}

TEST(LockManagerMetadataTest, CancelledSiBackoffDoesNotClaimReleasedLock) {
    LockManager lock_manager(std::chrono::microseconds(2000));
    Transaction owner(1041, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction cancelled(1042, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction next(1043, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid rid{5, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);

    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));
    std::atomic<bool> cancelled_started{false};
    std::atomic<bool> cancelled_acquired{true};
    std::thread cancelled_thread([&] {
        cancelled_started.store(true, std::memory_order_release);
        cancelled_acquired.store(lock_manager.lock_exclusive_on_record(&cancelled, rid, 42), std::memory_order_release);
    });

    while (!cancelled_started.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (lock_manager.record_lock_observability().backoff_waits == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(lock_manager.record_lock_observability().backoff_waits, 1u);
    lock_manager.cancel_transaction(&cancelled);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
    cancelled_thread.join();

    EXPECT_FALSE(cancelled_acquired.load(std::memory_order_acquire));
    EXPECT_EQ(cancelled.get_lock_set()->count(lock_id), 0u);
    EXPECT_TRUE(lock_manager.lock_exclusive_on_record(&next, rid, 42));
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

TEST(LockManagerMetadataTest, SiFirstLockWaitEnvironmentIsStrictAndFrozenPerInstance) {
    {
        ScopedEnvironmentValue invalid("RMDB_SI_FIRST_LOCK_WAIT", "true");
        EXPECT_THROW({ LockManager lock_manager; }, std::invalid_argument);
    }

    ScopedEnvironmentValue disabled("RMDB_SI_FIRST_LOCK_WAIT", "0");
    LockManager immediate_manager;
    setenv("RMDB_SI_FIRST_LOCK_WAIT", "1", 1);

    Transaction immediate_owner(1051, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction immediate_contender(1052, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid immediate_rid{6, 0};
    LockDataId immediate_lock_id(42, immediate_rid, LockDataType::RECORD);
    ASSERT_TRUE(immediate_manager.lock_exclusive_on_record(&immediate_owner, immediate_rid, 42));
    EXPECT_FALSE(immediate_manager.lock_exclusive_on_record(&immediate_contender, immediate_rid, 42));
    EXPECT_TRUE(immediate_manager.unlock(&immediate_owner, immediate_lock_id));

    ASSERT_EQ(unsetenv("RMDB_SI_FIRST_LOCK_WAIT"), 0);
    LockManager waiting_manager;
    setenv("RMDB_SI_FIRST_LOCK_WAIT", "0", 1);

    Transaction waiting_owner(1061, IsolationLevel::SERIALIZABLE);
    Transaction waiting_contender(1062, IsolationLevel::SERIALIZABLE);
    Rid waiting_rid{7, 0};
    LockDataId waiting_lock_id(42, waiting_rid, LockDataType::RECORD);
    ASSERT_TRUE(waiting_manager.lock_exclusive_on_record(&waiting_owner, waiting_rid, 42));

    std::atomic<bool> contender_finished{false};
    std::atomic<bool> contender_acquired{true};
    std::thread contender_thread([&] {
        contender_acquired.store(waiting_manager.lock_exclusive_on_record(&waiting_contender, waiting_rid, 42),
                                 std::memory_order_release);
        contender_finished.store(true, std::memory_order_release);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (waiting_manager.record_lock_observability().wait_enqueued == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(waiting_manager.record_lock_observability().wait_enqueued, 1u);
    EXPECT_EQ(waiting_manager.record_lock_observability().completion_waits, 1u);
    EXPECT_FALSE(contender_finished.load(std::memory_order_acquire));

    waiting_manager.cancel_transaction(&waiting_contender);
    contender_thread.join();
    EXPECT_FALSE(contender_acquired.load(std::memory_order_acquire));
    EXPECT_EQ(waiting_manager.record_lock_observability().completion_aborts, 0u);
    EXPECT_TRUE(waiting_manager.unlock(&waiting_owner, waiting_lock_id));
}

TEST(LockManagerMetadataTest, SiFirstCompletionWaitReturnsConflictInsteadOfTakingHandoff) {
    LockManager lock_manager(std::chrono::microseconds(0), true);
    Transaction owner(1071, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction contender(1072, IsolationLevel::SNAPSHOT_ISOLATION);
    Rid rid{8, 0};
    LockDataId lock_id(42, rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_manager.lock_exclusive_on_record(&owner, rid, 42));

    std::atomic<bool> contender_acquired{true};
    std::thread contender_thread([&] {
        contender_acquired.store(lock_manager.lock_exclusive_on_record(&contender, rid, 42), std::memory_order_release);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (lock_manager.record_lock_observability().completion_waits == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(lock_manager.record_lock_observability().completion_waits, 1u);
    EXPECT_TRUE(lock_manager.unlock(&owner, lock_id));
    contender_thread.join();

    EXPECT_FALSE(contender_acquired.load(std::memory_order_acquire));
    EXPECT_EQ(contender.get_lock_set()->count(lock_id), 0u);
    const auto stats = lock_manager.record_lock_observability();
    EXPECT_EQ(stats.completion_aborts, 1u);
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
