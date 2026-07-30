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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

TEST(CheckpointOptionsTest, ProductionDefaultTargetIsFourGiB) {
    const CheckpointOptions options;
    EXPECT_EQ(options.auto_checkpoint_bytes, 4LL * 1024 * 1024 * 1024);
}

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
    explicit DrainTestDb(const std::string& db_name, bool with_index = false)
        : bpm_(64, &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_), sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_),
          log_mgr_(&disk_) {
        sm_mgr_.create_db(db_name);
        sm_mgr_.open_db(db_name);
        bpm_.set_log_manager(&log_mgr_);
        sm_mgr_.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
        if (with_index) {
            sm_mgr_.create_index("t", {"id"}, nullptr);
        }
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

PageId MakeDirtyTablePage(DrainTestDb* db) {
    PageId page_id{db->sm_mgr_.fhs_.at("t")->GetFd(), INVALID_PAGE_ID};
    Page* page = db->bpm_.new_page(&page_id);
    EXPECT_NE(page, nullptr);
    if (page != nullptr) {
        page->set_page_lsn(0);
        EXPECT_TRUE(db->bpm_.unpin_page(page_id, true));
    }
    return page_id;
}

bool IsDirty(BufferPoolManager* bpm, PageId page_id) {
    Page* page = bpm->fetch_page(page_id);
    EXPECT_NE(page, nullptr);
    if (page == nullptr) {
        return false;
    }
    const bool dirty = page->is_dirty();
    EXPECT_TRUE(bpm->unpin_page(page_id, false));
    return dirty;
}

int64_t AppendBegin(LogManager* log_manager, txn_id_t txn_id) {
    BeginLogRecord record(txn_id);
    log_manager->add_log_to_buffer(&record);
    return log_manager->current_log_offset();
}

} // namespace

// Force checkpoint admission to linearize before a concurrent BEGIN. The new
// transaction must observe the block, stay out of the active set, and enter
// only after the bounded drain timeout tears down BlockGuard.
TEST(CheckpointDrainTest, CheckpointBlockWinsAdmissionRaceAndTimeoutUnblocksBegin) {
    ScopedDrainTestDir test_dir("checkpoint_drain_root");
    DrainTestDb db("checkpoint_drain_db");
    LockManager lock_mgr;

    std::promise<void> drain_entered_signal;
    auto drain_entered = drain_entered_signal.get_future();
    std::promise<void> release_drain_signal;
    auto release_drain = release_drain_signal.get_future().share();
    std::promise<void> begin_waiting_signal;
    auto begin_waiting = begin_waiting_signal.get_future();
    TransactionManager::CheckpointAdmissionTestOptions test_options;
    test_options.hook = [&](std::string_view event) {
        if (event == "drain_waiting") {
            drain_entered_signal.set_value();
            release_drain.wait();
        } else if (event == "begin_waiting") {
            begin_waiting_signal.set_value();
        }
    };
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_, ConcurrencyMode::TWO_PHASE_LOCKING, std::move(test_options));
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    // A client that ran BEGIN and then went silent.
    Transaction* idle_txn = txn_mgr.begin(nullptr, &db.log_mgr_);
    ASSERT_NE(idle_txn, nullptr);

    auto checkpoint_result = std::async(std::launch::async, [&] { return checkpoint_mgr.RunCleanCheckpoint(); });
    const auto drain_status = drain_entered.wait_for(std::chrono::seconds(5));
    if (drain_status != std::future_status::ready) {
        release_drain_signal.set_value();
        txn_mgr.abort(idle_txn, &db.log_mgr_);
    }
    ASSERT_EQ(drain_status, std::future_status::ready) << "checkpoint never reached its blocked drain";

    const auto probe_begin = std::chrono::steady_clock::now();
    std::promise<void> probe_started_signal;
    auto probe_started = probe_started_signal.get_future();
    auto probe = std::async(std::launch::async, [&] {
        probe_started_signal.set_value();
        return txn_mgr.begin(nullptr, &db.log_mgr_);
    });
    const auto probe_started_status = probe_started.wait_for(std::chrono::seconds(5));
    release_drain_signal.set_value();
    ASSERT_EQ(probe_started_status, std::future_status::ready);
    ASSERT_EQ(begin_waiting.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "BEGIN did not observe the checkpoint block";

    const auto active_while_blocked = txn_mgr.get_active_txn_lsn_snapshot();
    ASSERT_EQ(active_while_blocked.size(), 1u);
    EXPECT_EQ(active_while_blocked.count(idle_txn->get_transaction_id()), 1u);
    EXPECT_EQ(probe.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout)
        << "blocked BEGIN returned before checkpoint released admission";

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

// Force BEGIN to insert itself into the active set while holding the same
// checkpoint latch that BlockGuard needs. Once BlockGuard wins the latch, its
// drain must wait until that transaction retires.
TEST(CheckpointDrainTest, BeginWinsAdmissionRaceAndCheckpointDrainsUntilRetire) {
    ScopedDrainTestDir test_dir("checkpoint_begin_wins_root");
    DrainTestDb db("checkpoint_begin_wins_db");
    LockManager lock_mgr;

    std::promise<void> begin_admitted_signal;
    auto begin_admitted = begin_admitted_signal.get_future();
    std::promise<void> release_begin_signal;
    auto release_begin = release_begin_signal.get_future().share();
    std::promise<void> drain_entered_signal;
    auto drain_entered = drain_entered_signal.get_future();
    TransactionManager::CheckpointAdmissionTestOptions test_options;
    test_options.hook = [&](std::string_view event) {
        if (event == "begin_admitted") {
            begin_admitted_signal.set_value();
            release_begin.wait();
        } else if (event == "drain_waiting") {
            drain_entered_signal.set_value();
        }
    };
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_, ConcurrencyMode::TWO_PHASE_LOCKING, std::move(test_options));
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    auto begin_result = std::async(std::launch::async, [&] { return txn_mgr.begin(nullptr, &db.log_mgr_); });
    const auto begin_status = begin_admitted.wait_for(std::chrono::seconds(5));
    if (begin_status != std::future_status::ready) {
        release_begin_signal.set_value();
    }
    ASSERT_EQ(begin_status, std::future_status::ready) << "BEGIN never reached the active-set linearization point";

    std::promise<void> checkpoint_started_signal;
    auto checkpoint_started = checkpoint_started_signal.get_future();
    auto checkpoint_result = std::async(std::launch::async, [&] {
        checkpoint_started_signal.set_value();
        return checkpoint_mgr.RunCleanCheckpoint();
    });
    ASSERT_EQ(checkpoint_started.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    release_begin_signal.set_value();

    ASSERT_EQ(begin_result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    Transaction* active_txn = begin_result.get();
    ASSERT_NE(active_txn, nullptr);
    ASSERT_EQ(drain_entered.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "checkpoint never entered drain after BEGIN won admission";

    const auto active_during_drain = txn_mgr.get_active_txn_lsn_snapshot();
    ASSERT_EQ(active_during_drain.size(), 1u);
    EXPECT_EQ(active_during_drain.count(active_txn->get_transaction_id()), 1u);
    EXPECT_EQ(checkpoint_result.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout)
        << "checkpoint did not wait for the admitted transaction";

    txn_mgr.abort(active_txn, &db.log_mgr_);
    ASSERT_EQ(checkpoint_result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(checkpoint_result.get());
    const auto stats = txn_mgr.checkpoint_observability();
    EXPECT_EQ(stats.attempt, 1u);
    EXPECT_EQ(stats.drain_timeout, 0u);
    EXPECT_EQ(stats.success, 1u);
}

// If a checkpoint stage throws after BlockGuard has closed admission, stack
// unwinding must reopen admission and reset the global running guard.
TEST(CheckpointDrainTest, ExceptionDuringDrainUnblocksAdmission) {
    ScopedDrainTestDir test_dir("checkpoint_exception_unblock_root");
    DrainTestDb db("checkpoint_exception_unblock_db");
    LockManager lock_mgr;

    std::atomic<bool> throw_once{true};
    TransactionManager::CheckpointAdmissionTestOptions test_options;
    test_options.hook = [&](std::string_view event) {
        if (event == "drain_waiting" && throw_once.exchange(false)) {
            throw std::runtime_error("injected checkpoint drain failure");
        }
    };
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_, ConcurrencyMode::TWO_PHASE_LOCKING, std::move(test_options));
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    EXPECT_THROW(checkpoint_mgr.RunCleanCheckpoint(), std::runtime_error);

    auto begin_result = std::async(std::launch::async, [&] { return txn_mgr.begin(nullptr, &db.log_mgr_); });
    const auto begin_status = begin_result.wait_for(std::chrono::seconds(5));
    if (begin_status != std::future_status::ready) {
        // Keep a failed regression from hanging test teardown forever.
        txn_mgr.unblock_new_transactions_after_checkpoint();
    }
    ASSERT_EQ(begin_status, std::future_status::ready) << "exception left checkpoint admission blocked";
    Transaction* txn = begin_result.get();
    ASSERT_NE(txn, nullptr);
    txn_mgr.abort(txn, &db.log_mgr_);

    EXPECT_TRUE(checkpoint_mgr.RunCleanCheckpoint()) << "exception left the global checkpoint running guard set";
    const auto stats = txn_mgr.checkpoint_observability();
    EXPECT_EQ(stats.attempt, 2u);
    EXPECT_EQ(stats.success, 1u);
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

TEST(CheckpointScheduleTest, TickRunsOnlyWhenTargetIsReached) {
    ScopedDrainTestDir test_dir("checkpoint_target_only_root");
    DrainTestDb db("checkpoint_target_only_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    const PageId dirty_page = MakeDirtyTablePage(&db);
    ASSERT_NE(dirty_page.page_no, INVALID_PAGE_ID);
    const int64_t below_target = AppendBegin(&db.log_mgr_, 200);
    ASSERT_GT(below_target, 0);

    CheckpointOptions options;
    options.auto_checkpoint_bytes = below_target + 1;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_TRUE(IsDirty(&db.bpm_, dirty_page));
    EXPECT_EQ(txn_mgr.checkpoint_observability().attempt, 0u);
    EXPECT_EQ(db.log_mgr_.current_log_offset(), below_target);

    ASSERT_GT(AppendBegin(&db.log_mgr_, 201), options.auto_checkpoint_bytes);
    EXPECT_TRUE(checkpoint_mgr.Tick());
    EXPECT_FALSE(IsDirty(&db.bpm_, dirty_page));
    EXPECT_EQ(db.log_mgr_.current_log_offset(), 0);
    const auto stats = txn_mgr.checkpoint_observability();
    EXPECT_EQ(stats.attempt, 1u);
    EXPECT_EQ(stats.success, 1u);
}
