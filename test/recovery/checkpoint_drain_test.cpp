/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "index/ix.h"
#include "record/rm.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>

namespace {

class ScopedDrainTestDir {
public:
    explicit ScopedDrainTestDir(std::string dir) : old_path_(std::filesystem::current_path()), dir_(std::move(dir)) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directory(dir_);
        std::filesystem::current_path(dir_);
    }

    ~ScopedDrainTestDir() {
        std::filesystem::current_path(old_path_);
        std::filesystem::remove_all(dir_);
    }

private:
    std::filesystem::path old_path_;
    std::filesystem::path dir_;
};

class DrainTestDb {
public:
    explicit DrainTestDb(const std::string& db_name)
        : bpm_(64, &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_), sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_),
          log_mgr_(&disk_) {
        sm_mgr_.create_db(db_name);
        sm_mgr_.open_db(db_name);
        sm_mgr_.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
    }

    ~DrainTestDb() {
        sm_mgr_.close_db();
    }

    DiskManager disk_;
    BufferPoolManager bpm_;
    RmManager rm_mgr_;
    IxManager ix_mgr_;
    SmManager sm_mgr_;
    LogManager log_mgr_;
};

} // namespace

// An idle-in-transaction client must not be able to park the checkpoint thread
// inside its "block new transactions" window. If it can, every new transaction
// in the process — including the liveness probe — stalls forever.
TEST(CheckpointDrainTest, IdleTransactionDoesNotStallNewTransactionsForever) {
    ScopedDrainTestDir test_dir("checkpoint_drain_root");
    DrainTestDb db("checkpoint_drain_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    // A client that ran BEGIN and then went silent.
    Transaction* idle_txn = txn_mgr.begin(nullptr, &db.log_mgr_);
    ASSERT_NE(idle_txn, nullptr);

    auto checkpoint_result = std::async(std::launch::async, [&] { return checkpoint_mgr.RunCleanCheckpoint(); });
    // Give the checkpoint thread time to reach the drain wait.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const auto probe_begin = std::chrono::steady_clock::now();
    auto probe = std::async(std::launch::async, [&] { return txn_mgr.begin(nullptr, &db.log_mgr_); });
    ASSERT_EQ(probe.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "a new transaction was still blocked 5s after an idle-in-transaction client blocked the drain";
    Transaction* probe_txn = probe.get();
    const auto probe_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - probe_begin).count();
    ASSERT_NE(probe_txn, nullptr);
    EXPECT_LT(probe_ms, 5000) << "new transaction took " << probe_ms << " ms";

    ASSERT_EQ(checkpoint_result.wait_for(std::chrono::seconds(15)), std::future_status::ready)
        << "RunCleanCheckpoint never returned";
    EXPECT_FALSE(checkpoint_result.get()) << "checkpoint should abandon the round when the drain times out";
    const auto stats = txn_mgr.checkpoint_observability();
    EXPECT_EQ(stats.attempt, 1u);
    EXPECT_EQ(stats.drain_timeout, 1u);
    EXPECT_GT(stats.drain_ns, 0u);
    EXPECT_GT(stats.block_ns, 0u);

    txn_mgr.abort(probe_txn, &db.log_mgr_);
    txn_mgr.abort(idle_txn, &db.log_mgr_);
}

// The abandoned round must leave no residual blocking: once the stuck
// transaction is gone, a later checkpoint has to succeed again.
TEST(CheckpointDrainTest, CheckpointSucceedsAfterBlockedRoundIsAbandoned) {
    ScopedDrainTestDir test_dir("checkpoint_drain_retry_root");
    DrainTestDb db("checkpoint_drain_retry_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    Transaction* idle_txn = txn_mgr.begin(nullptr, &db.log_mgr_);
    ASSERT_NE(idle_txn, nullptr);
    EXPECT_FALSE(checkpoint_mgr.RunCleanCheckpoint());
    txn_mgr.abort(idle_txn, &db.log_mgr_);

    // RunCleanCheckpoint is the explicit (user-requested) entry point and must
    // not be gated by the automatic retry backoff.
    EXPECT_TRUE(checkpoint_mgr.RunCleanCheckpoint());
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
    const auto stats = txn_mgr.checkpoint_observability();
    EXPECT_EQ(stats.attempt, 2u);
    EXPECT_EQ(stats.drain_timeout, 1u);
    EXPECT_EQ(stats.success, 1u);
    EXPECT_GT(stats.final_wal_ns, 0u);
    EXPECT_GT(stats.final_data_ns, 0u);
}
