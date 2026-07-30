/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution/executor_insert.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record/rm.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

class BlockingPoint {
public:
    void Block() {
        std::unique_lock<std::mutex> lock(latch_);
        entered_ = true;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }

    bool WaitUntilEntered(std::chrono::milliseconds timeout = 2s) {
        std::unique_lock<std::mutex> lock(latch_);
        return cv_.wait_for(lock, timeout, [&] { return entered_; });
    }

    void Release() {
        std::lock_guard<std::mutex> lock(latch_);
        released_ = true;
        cv_.notify_all();
    }

private:
    std::mutex latch_;
    std::condition_variable cv_;
    bool entered_{false};
    bool released_{false};
};

class ScopedPublicationTestDir {
public:
    explicit ScopedPublicationTestDir(std::string dir)
        : old_path_(std::filesystem::current_path()), dir_(std::move(dir)) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directory(dir_);
        std::filesystem::current_path(dir_);
    }

    ~ScopedPublicationTestDir() {
        std::filesystem::current_path(old_path_);
        std::filesystem::remove_all(dir_);
    }

private:
    std::filesystem::path old_path_;
    std::filesystem::path dir_;
};

class ScopedEnvironmentValue {
public:
    ScopedEnvironmentValue(const char* name, const char* value) : name_(name) {
        if (const char* current = std::getenv(name); current != nullptr) {
            old_value_ = current;
        }
        EXPECT_EQ(setenv(name, value, 1), 0);
    }

    ~ScopedEnvironmentValue() {
        if (old_value_.has_value()) {
            EXPECT_EQ(setenv(name_.c_str(), old_value_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(name_.c_str()), 0);
        }
    }

private:
    std::string name_;
    std::optional<std::string> old_value_;
};

class CommitPublicationHelpingTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        test_dir_ = std::make_unique<ScopedPublicationTestDir>(std::string("commit_publication_") + info->name());
        disk_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(128, disk_.get());
        rm_mgr_ = std::make_unique<RmManager>(disk_.get(), bpm_.get());
        ix_mgr_ = std::make_unique<IxManager>(disk_.get(), bpm_.get());
        sm_mgr_ = std::make_unique<SmManager>(disk_.get(), bpm_.get(), rm_mgr_.get(), ix_mgr_.get());
        log_mgr_ = std::make_unique<LogManager>(disk_.get());
        lock_mgr_ = std::make_unique<LockManager>();
        sm_mgr_->create_db("db");
        sm_mgr_->open_db("db");
        sm_mgr_->create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
        sm_mgr_->create_index("t", {"id"}, nullptr);
        bpm_->set_log_manager(log_mgr_.get());
    }

    void TearDown() override {
        txn_mgr_.reset();
        sm_mgr_->close_db();
        sm_mgr_.reset();
        test_dir_.reset();
    }

    void MakeManager(TransactionManager::CommitPublicationTestHook hook = {}, bool helping = true) {
        TransactionManager::CommitPublicationTestOptions test_options;
        test_options.helping = helping;
        test_options.hook = std::move(hook);
        txn_mgr_ = std::make_unique<TransactionManager>(lock_mgr_.get(), sm_mgr_.get(),
                                                        ConcurrencyMode::TWO_PHASE_LOCKING, std::move(test_options));
    }

    static Value IntValue(int value) {
        Value result;
        result.set_int(value);
        return result;
    }

    void InsertRow(Transaction* txn, int id, int value) {
        Context context(lock_mgr_.get(), log_mgr_.get(), txn, nullptr, &offset_, txn_mgr_.get());
        InsertExecutor executor(sm_mgr_.get(), "t", {IntValue(id), IntValue(value)}, &context);
        executor.Next();
    }

    Rid OnlyRid() {
        RmScan scan(sm_mgr_->fhs_.at("t").get());
        EXPECT_FALSE(scan.is_end());
        return scan.rid();
    }

    void UpdateValue(Transaction* txn, const Rid& rid, int value) {
        Context context(lock_mgr_.get(), log_mgr_.get(), txn, nullptr, &offset_, txn_mgr_.get());
        SetClause set_clause;
        set_clause.lhs = TabCol{"t", "v"};
        set_clause.rhs = IntValue(value);
        UpdateExecutor executor(sm_mgr_.get(), "t", {set_clause}, {}, {rid}, &context);
        executor.Next();
    }

    int offset_{0};
    std::unique_ptr<ScopedPublicationTestDir> test_dir_;
    std::unique_ptr<DiskManager> disk_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_mgr_;
    std::unique_ptr<IxManager> ix_mgr_;
    std::unique_ptr<SmManager> sm_mgr_;
    std::unique_ptr<LogManager> log_mgr_;
    std::unique_ptr<LockManager> lock_mgr_;
    std::unique_ptr<TransactionManager> txn_mgr_;
};

TEST_F(CommitPublicationHelpingTest, LaterOwnerPublishesDurablePrefixAndReleasesRecordAndUniqueLocks) {
    BlockingPoint first_owner_after_wal;
    std::mutex observation_latch;
    std::condition_variable observation_cv;
    bool first_locks_released = false;
    std::thread::id first_helper;
    std::thread::id second_owner;

    MakeManager([&](std::string_view event, timestamp_t csn, lsn_t) {
        if (event == "after_wal_wait" && csn == 1) {
            first_owner_after_wal.Block();
        }
        if (event == "after_lock_release" && csn == 1) {
            std::lock_guard<std::mutex> lock(observation_latch);
            first_locks_released = true;
            first_helper = std::this_thread::get_id();
            observation_cv.notify_all();
        }
    });

    Transaction* first = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::READ_COMMITTED);
    const Rid rid{7, 3};
    const LockDataId record_lock(71, rid, LockDataType::RECORD);
    const std::vector<char> unique_key{'k', 'e', 'y'};
    ASSERT_TRUE(lock_mgr_->lock_exclusive_on_record(first, rid, 71));
    ASSERT_TRUE(lock_mgr_->lock_exclusive_on_unique_key(first, 91, unique_key));

    std::thread first_owner([&] { txn_mgr_->commit(first, log_mgr_.get()); });
    ASSERT_TRUE(first_owner_after_wal.WaitUntilEntered());

    Transaction* waiter = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::READ_COMMITTED);
    std::atomic<bool> waiter_has_both{false};
    std::thread waiter_thread([&] {
        const bool record_acquired = lock_mgr_->lock_exclusive_on_record(waiter, rid, 71);
        const bool unique_acquired = lock_mgr_->lock_exclusive_on_unique_key(waiter, 91, unique_key);
        waiter_has_both.store(record_acquired && unique_acquired, std::memory_order_release);
    });
    while (lock_mgr_->record_lock_observability().wait_enqueued == 0) {
        std::this_thread::yield();
    }

    Transaction* second = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::READ_COMMITTED);
    std::thread second_thread([&] {
        second_owner = std::this_thread::get_id();
        txn_mgr_->commit(second, log_mgr_.get());
    });

    {
        std::unique_lock<std::mutex> lock(observation_latch);
        ASSERT_TRUE(observation_cv.wait_for(lock, 2s, [&] { return first_locks_released; }));
    }
    waiter_thread.join();

    EXPECT_EQ(first_helper, second_owner);
    EXPECT_TRUE(first->get_lock_set()->empty());
    EXPECT_TRUE(first->get_unique_key_lock_set()->empty());
    EXPECT_TRUE(waiter_has_both.load(std::memory_order_acquire));
    EXPECT_EQ(waiter->get_lock_set()->count(record_lock), 1u);

    first_owner_after_wal.Release();
    first_owner.join();
    second_thread.join();
    txn_mgr_->abort(waiter, log_mgr_.get());
}

TEST_F(CommitPublicationHelpingTest, MissingLowerCsnAndInvertedLsnsDoNotAdvanceOrMissWakeup) {
    BlockingPoint first_after_csn;
    std::mutex observation_latch;
    std::condition_variable observation_cv;
    bool second_wal_durable = false;
    lsn_t first_lsn = INVALID_LSN;
    lsn_t second_lsn = INVALID_LSN;

    MakeManager([&](std::string_view event, timestamp_t csn, lsn_t lsn) {
        if (event == "after_csn_allocated" && csn == 1) {
            first_after_csn.Block();
        }
        if (event == "after_registered") {
            std::lock_guard<std::mutex> lock(observation_latch);
            if (csn == 1) {
                first_lsn = lsn;
            } else if (csn == 2) {
                second_lsn = lsn;
            }
        }
        if (event == "after_wal_wait" && csn == 2) {
            std::lock_guard<std::mutex> lock(observation_latch);
            second_wal_durable = true;
            observation_cv.notify_all();
        }
    });

    Transaction* first = txn_mgr_->begin(nullptr, log_mgr_.get());
    Transaction* second = txn_mgr_->begin(nullptr, log_mgr_.get());
    std::atomic<bool> first_done{false};
    std::atomic<bool> second_done{false};
    std::thread first_thread([&] {
        txn_mgr_->commit(first, log_mgr_.get());
        first_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(first_after_csn.WaitUntilEntered());

    std::thread second_thread([&] {
        txn_mgr_->commit(second, log_mgr_.get());
        second_done.store(true, std::memory_order_release);
    });
    {
        std::unique_lock<std::mutex> lock(observation_latch);
        ASSERT_TRUE(observation_cv.wait_for(lock, 2s, [&] { return second_wal_durable; }));
    }

    EXPECT_EQ(first->get_state(), TransactionState::COMMITTING);
    EXPECT_EQ(second->get_state(), TransactionState::COMMITTING);
    EXPECT_FALSE(first_done.load(std::memory_order_acquire));
    EXPECT_FALSE(second_done.load(std::memory_order_acquire));
    EXPECT_EQ(first_lsn, INVALID_LSN);
    ASSERT_NE(second_lsn, INVALID_LSN);
    EXPECT_GE(log_mgr_->get_durable_lsn(), second_lsn);

    first_after_csn.Release();
    first_thread.join();
    second_thread.join();

    EXPECT_TRUE(first_done.load(std::memory_order_acquire));
    EXPECT_TRUE(second_done.load(std::memory_order_acquire));
    EXPECT_GT(first_lsn, second_lsn);
}

TEST_F(CommitPublicationHelpingTest, WalCompletionWakesLeaderBeforeOwnerEntersPublication) {
    BlockingPoint first_after_csn;
    BlockingPoint first_after_registered;
    BlockingPoint leader_waiting_for_first_request;
    BlockingPoint leader_waiting_for_first_wal;
    BlockingPoint first_after_wal;
    std::mutex observation_latch;
    std::condition_variable observation_cv;
    bool first_published = false;

    MakeManager([&](std::string_view event, timestamp_t csn, lsn_t) {
        if (event == "after_csn_allocated" && csn == 1) {
            first_after_csn.Block();
        }
        if (event == "after_registered" && csn == 1) {
            first_after_registered.Block();
        }
        if (event == "leader_waiting_for_request" && csn == 1) {
            leader_waiting_for_first_request.Block();
        }
        if (event == "leader_waiting_for_wal" && csn == 1) {
            leader_waiting_for_first_wal.Block();
        }
        if (event == "after_wal_wait" && csn == 1) {
            first_after_wal.Block();
        }
        if (event == "after_lock_release" && csn == 1) {
            std::lock_guard<std::mutex> lock(observation_latch);
            first_published = true;
            observation_cv.notify_all();
        }
    });

    Transaction* first = txn_mgr_->begin(nullptr, log_mgr_.get());
    Transaction* second = txn_mgr_->begin(nullptr, log_mgr_.get());
    std::thread first_thread([&] { txn_mgr_->commit(first, log_mgr_.get()); });
    ASSERT_TRUE(first_after_csn.WaitUntilEntered());

    std::thread second_thread([&] { txn_mgr_->commit(second, log_mgr_.get()); });
    ASSERT_TRUE(leader_waiting_for_first_request.WaitUntilEntered());
    first_after_csn.Release();
    ASSERT_TRUE(first_after_registered.WaitUntilEntered());
    leader_waiting_for_first_request.Release();
    ASSERT_TRUE(leader_waiting_for_first_wal.WaitUntilEntered());

    first_after_registered.Release();
    ASSERT_TRUE(first_after_wal.WaitUntilEntered());
    leader_waiting_for_first_wal.Release();

    bool published_before_owner_entered = false;
    {
        std::unique_lock<std::mutex> lock(observation_latch);
        published_before_owner_entered = observation_cv.wait_for(lock, 2s, [&] { return first_published; });
    }

    first_after_wal.Release();
    first_thread.join();
    second_thread.join();

    EXPECT_TRUE(published_before_owner_entered);
    EXPECT_EQ(first->get_state(), TransactionState::COMMITTED);
    EXPECT_EQ(second->get_state(), TransactionState::COMMITTED);
}

TEST_F(CommitPublicationHelpingTest, WaiterTakesOverAfterBoundedLeaderStops) {
    BlockingPoint first_after_lock_release;
    std::mutex observation_latch;
    std::condition_variable observation_cv;
    bool second_wal_durable = false;

    MakeManager([&](std::string_view event, timestamp_t csn, lsn_t) {
        if (event == "after_lock_release" && csn == 1) {
            first_after_lock_release.Block();
        }
        if (event == "after_wal_wait" && csn == 2) {
            std::lock_guard<std::mutex> lock(observation_latch);
            second_wal_durable = true;
            observation_cv.notify_all();
        }
    });

    Transaction* first = txn_mgr_->begin(nullptr, log_mgr_.get());
    std::thread first_thread([&] { txn_mgr_->commit(first, log_mgr_.get()); });
    ASSERT_TRUE(first_after_lock_release.WaitUntilEntered());

    Transaction* second = txn_mgr_->begin(nullptr, log_mgr_.get());
    std::atomic<bool> second_done{false};
    std::thread second_thread([&] {
        txn_mgr_->commit(second, log_mgr_.get());
        second_done.store(true, std::memory_order_release);
    });
    {
        std::unique_lock<std::mutex> lock(observation_latch);
        ASSERT_TRUE(observation_cv.wait_for(lock, 2s, [&] { return second_wal_durable; }));
    }
    EXPECT_FALSE(second_done.load(std::memory_order_acquire));

    first_after_lock_release.Release();
    first_thread.join();
    second_thread.join();

    EXPECT_EQ(first->get_state(), TransactionState::COMMITTED);
    EXPECT_EQ(second->get_state(), TransactionState::COMMITTED);
    EXPECT_TRUE(second_done.load(std::memory_order_acquire));
}

TEST_F(CommitPublicationHelpingTest, HigherAllocatedCsnDoesNotExtendLowerLeaderTarget) {
    BlockingPoint first_after_registered;
    BlockingPoint second_after_csn;
    std::mutex done_latch;
    std::condition_variable done_cv;
    bool first_done = false;

    MakeManager([&](std::string_view event, timestamp_t csn, lsn_t) {
        if (event == "after_registered" && csn == 1) {
            first_after_registered.Block();
        }
        if (event == "after_csn_allocated" && csn == 2) {
            second_after_csn.Block();
        }
    });

    Transaction* first = txn_mgr_->begin(nullptr, log_mgr_.get());
    std::thread first_thread([&] {
        txn_mgr_->commit(first, log_mgr_.get());
        {
            std::lock_guard<std::mutex> lock(done_latch);
            first_done = true;
        }
        done_cv.notify_all();
    });
    ASSERT_TRUE(first_after_registered.WaitUntilEntered());

    Transaction* second = txn_mgr_->begin(nullptr, log_mgr_.get());
    std::thread second_thread([&] { txn_mgr_->commit(second, log_mgr_.get()); });
    ASSERT_TRUE(second_after_csn.WaitUntilEntered());

    first_after_registered.Release();
    bool first_returned_while_second_unregistered = false;
    {
        std::unique_lock<std::mutex> lock(done_latch);
        first_returned_while_second_unregistered = done_cv.wait_for(lock, 2s, [&] { return first_done; });
    }

    second_after_csn.Release();
    first_thread.join();
    second_thread.join();

    EXPECT_TRUE(first_returned_while_second_unregistered);
    EXPECT_EQ(first->get_state(), TransactionState::COMMITTED);
    EXPECT_EQ(second->get_state(), TransactionState::COMMITTED);
}

TEST_F(CommitPublicationHelpingTest, LockReleasePrecedesOwnerCleanupButPinKeepsTxnAndCheckpointActive) {
    BlockingPoint after_lock_release;
    MakeManager([&](std::string_view event, timestamp_t csn, lsn_t) {
        if (event == "after_lock_release" && csn == 1) {
            after_lock_release.Block();
        }
    });

    Transaction* txn = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::SERIALIZABLE);
    const txn_id_t txn_id = txn->get_transaction_id();
    const Rid rid{9, 4};
    ASSERT_TRUE(lock_mgr_->lock_exclusive_on_record(txn, rid, 73));

    std::thread owner([&] { txn_mgr_->commit(txn, log_mgr_.get()); });
    ASSERT_TRUE(after_lock_release.WaitUntilEntered());

    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
    EXPECT_TRUE(txn->get_lock_set()->empty());
    EXPECT_TRUE(txn->has_commit_publication_pin());
    EXPECT_EQ(txn_mgr_->get_transaction(txn_id), txn);
    txn_mgr_->PruneSsiState();
    EXPECT_EQ(txn_mgr_->get_transaction(txn_id), txn);
    txn_mgr_->commit(txn, log_mgr_.get());
    txn_mgr_->commit(txn, log_mgr_.get());
    EXPECT_EQ(txn_mgr_->get_transaction(txn_id), txn);

    txn_mgr_->block_new_transactions_for_checkpoint();
    EXPECT_FALSE(txn_mgr_->wait_active_transactions_drained_for_checkpoint(20ms));
    after_lock_release.Release();
    owner.join();
    EXPECT_TRUE(txn_mgr_->wait_active_transactions_drained_for_checkpoint(2s));
    txn_mgr_->unblock_new_transactions_after_checkpoint();
}

TEST_F(CommitPublicationHelpingTest, StaleSnapshotStillAbortsAfterHelperReleasesCommittedWriterLock) {
    MakeManager();
    Transaction* initial = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::SNAPSHOT_ISOLATION);
    InsertRow(initial, 1, 10);
    txn_mgr_->commit(initial, log_mgr_.get());
    const Rid rid = OnlyRid();

    Transaction* stale = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction* winner = txn_mgr_->begin(nullptr, log_mgr_.get(), IsolationLevel::SNAPSHOT_ISOLATION);
    UpdateValue(winner, rid, 20);
    txn_mgr_->commit(winner, log_mgr_.get());

    EXPECT_THROW(UpdateValue(stale, rid, 30), TransactionAbortException);
    txn_mgr_->abort(stale, log_mgr_.get());
}

TEST_F(CommitPublicationHelpingTest, OwnerCleanupFailureAfterPublicationIsFailStop) {
    EXPECT_EXIT(
        {
            MakeManager([](std::string_view event, timestamp_t, lsn_t) {
                if (event == "before_owner_cleanup") {
                    throw std::runtime_error("injected owner cleanup failure");
                }
            });
            Transaction* txn = txn_mgr_->begin(nullptr, log_mgr_.get());
            txn_mgr_->commit(txn, log_mgr_.get());
            std::_Exit(0);
        },
        ::testing::ExitedWithCode(134), "FATAL: COMMIT WAL/publication failed");
}

TEST_F(CommitPublicationHelpingTest, LegacyEnvironmentCannotDisableDefaultHelping) {
    ScopedEnvironmentValue legacy_disable("RMDB_COMMIT_PUBLICATION_HELPING", "0");
    txn_mgr_ = std::make_unique<TransactionManager>(lock_mgr_.get(), sm_mgr_.get());
    EXPECT_TRUE(txn_mgr_->commit_publication_helping_enabled_for_test());

    Transaction* txn = txn_mgr_->begin(nullptr, log_mgr_.get());
    txn_mgr_->commit(txn, log_mgr_.get());

    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST_F(CommitPublicationHelpingTest, ExplicitTestOnlyDisabledPathRetainsDirectPublicationOracle) {
    MakeManager({}, false);
    Transaction* txn = txn_mgr_->begin(nullptr, log_mgr_.get());
    txn_mgr_->commit(txn, log_mgr_.get());

    EXPECT_EQ(log_mgr_->get_commit_count(), 1u);
    EXPECT_GE(log_mgr_->get_durable_lsn(), 0);
}

TEST_F(CommitPublicationHelpingTest, ProcessCrashModePublishesThroughPersistLsn) {
    log_mgr_ = std::make_unique<LogManager>(disk_.get(), DurabilityMode::PROCESS_CRASH);
    bpm_->set_log_manager(log_mgr_.get());
    MakeManager();

    Transaction* txn = txn_mgr_->begin(nullptr, log_mgr_.get());
    txn_mgr_->commit(txn, log_mgr_.get());

    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
    EXPECT_GE(log_mgr_->get_persist_lsn(), txn->get_prev_lsn());
    EXPECT_EQ(log_mgr_->get_durable_lsn(), INVALID_LSN);
}

} // namespace
