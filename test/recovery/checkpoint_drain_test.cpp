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
#include "common/checkpoint_phase_metrics.h"
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
#include <cstdlib>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

TEST(CheckpointOptionsTest, ProductionDefaultTargetIsFourGiB) {
    const CheckpointOptions options;
    EXPECT_EQ(options.auto_checkpoint_bytes, 4LL * 1024 * 1024 * 1024);
    EXPECT_EQ(options.tick_bytes, 4ULL * 1024 * 1024);
    EXPECT_EQ(options.tick_time_us, 5000u);
    EXPECT_EQ(options.io_quantum_pages, 64u);
}

TEST(CheckpointOptionsTest, EnvironmentIsStrictAndClamped) {
    const auto set = [](const char* key, const char* value) { ASSERT_EQ(setenv(key, value, 1), 0); };
    set("RMDB_AUTO_CHECKPOINT_BYTES", "1");
    set("RMDB_CHECKPOINT_TICK_BYTES", "9999999999999");
    set("RMDB_CHECKPOINT_TICK_TIME_US", "1");
    set("RMDB_CHECKPOINT_IO_QUANTUM_PAGES", "999");
    const CheckpointOptions clamped = CheckpointOptions::FromEnvironment();
    EXPECT_EQ(clamped.auto_checkpoint_bytes, 1LL * 1024 * 1024);
    EXPECT_EQ(clamped.tick_bytes, 1024ULL * 1024 * 1024);
    EXPECT_EQ(clamped.tick_time_us, 100u);
    EXPECT_EQ(clamped.io_quantum_pages, 64u);
    set("RMDB_CHECKPOINT_TICK_BYTES", "64k");
    EXPECT_THROW((void)CheckpointOptions::FromEnvironment(), std::invalid_argument);
    unsetenv("RMDB_AUTO_CHECKPOINT_BYTES");
    unsetenv("RMDB_CHECKPOINT_TICK_BYTES");
    unsetenv("RMDB_CHECKPOINT_TICK_TIME_US");
    unsetenv("RMDB_CHECKPOINT_IO_QUANTUM_PAGES");
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
    explicit DrainTestDb(const std::string& db_name, bool with_index = false, size_t pool_size = 64)
        : bpm_(pool_size, &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_),
          sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_), log_mgr_(&disk_) {
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

size_t MakeDirtyTablePages(DrainTestDb* db, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        const PageId page_id = MakeDirtyTablePage(db);
        EXPECT_NE(page_id.page_no, INVALID_PAGE_ID);
        if (page_id.page_no == INVALID_PAGE_ID) {
            return i;
        }
    }
    return count;
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
    CheckpointPhaseMetrics metrics(true);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_, &metrics);

    EXPECT_THROW(checkpoint_mgr.RunCleanCheckpoint(), std::runtime_error);
    auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.clean_attempts, 1U);
    EXPECT_EQ(snapshot.clean_successes, 0U);
    EXPECT_EQ(snapshot.clean_failures, 1U);

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
    snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.clean_attempts, 2U);
    EXPECT_EQ(snapshot.clean_successes, 1U);
    EXPECT_EQ(snapshot.clean_failures, 1U);
    EXPECT_EQ(snapshot.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::CleanDataSync)].count, 1U);
    EXPECT_EQ(snapshot.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::CleanMetaFlush)].count, 1U);
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
}

TEST(CheckpointOptionsTest, SetOptionsClampsPacingButPreservesAutoThreshold) {
    ScopedDrainTestDir test_dir("checkpoint_options_clamp_root");
    DrainTestDb db("checkpoint_options_clamp_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = 1;
    options.tick_bytes = 0;
    options.tick_time_us = std::numeric_limits<uint64_t>::max();
    options.io_quantum_pages = std::numeric_limits<size_t>::max();
    checkpoint_mgr.SetOptions(options);
    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    ASSERT_GT(AppendBegin(&db.log_mgr_, 201), 0);
    // The small threshold remains effective while overflow-prone pacing values
    // are normalized before Tick converts them into a signed duration.
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_TRUE(checkpoint_mgr.Tick());
}

TEST(CheckpointScheduleTest, TickPublishesNonzeroRestartCutWithoutTruncatingWal) {
    ScopedDrainTestDir test_dir("checkpoint_target_only_root");
    DrainTestDb db("checkpoint_target_only_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointPhaseMetrics metrics(true);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_, &metrics);

    const PageId dirty_page = MakeDirtyTablePage(&db);
    ASSERT_NE(dirty_page.page_no, INVALID_PAGE_ID);
    const int64_t cut_offset = AppendBegin(&db.log_mgr_, 200);
    ASSERT_GT(cut_offset, 0);

    CheckpointOptions options;
    options.auto_checkpoint_bytes = cut_offset;
    checkpoint_mgr.SetOptions(options);

    // The first Tick only establishes a durable cut and reopens admission.
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_TRUE(IsDirty(&db.bpm_, dirty_page));
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
    EXPECT_GT(db.log_mgr_.current_log_offset(), cut_offset);

    // The next Tick drains the fixed cohort, stabilizes detached headers/meta,
    // and publishes the cut. Automatic fuzzy checkpointing retains WAL.
    EXPECT_TRUE(checkpoint_mgr.Tick());
    EXPECT_FALSE(IsDirty(&db.bpm_, dirty_page));
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, cut_offset);
    EXPECT_GT(db.disk_.get_file_size(LOG_FILE_NAME), 0);
    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.fuzzy_attempts, 1U);
    EXPECT_EQ(snapshot.fuzzy_successes, 1U);
    EXPECT_EQ(snapshot.fuzzy_failures, 0U);
    EXPECT_EQ(snapshot.fuzzy_cancels, 0U);
    EXPECT_EQ(snapshot.page_write_calls, 1U);
    EXPECT_EQ(snapshot.pages_remaining_max, 0U);
    EXPECT_EQ(snapshot.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::FuzzyFinalPublish)].count, 1U);
    EXPECT_EQ(snapshot.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::FuzzyLifetime)].count, 1U);
}

TEST(CheckpointScheduleTest, FuzzyPageWriteFailureIsFailureNotCancel) {
    ScopedDrainTestDir test_dir("checkpoint_metrics_failure_root");
    DrainTestDb db("checkpoint_metrics_failure_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointPhaseMetrics metrics(true);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_, &metrics);
    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    const int64_t cut_offset = AppendBegin(&db.log_mgr_, 202);
    ASSERT_GT(cut_offset, 0);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = cut_offset;
    checkpoint_mgr.SetOptions(options);

    BufferPoolManager::set_flush_batch_before_write_test_hook(
        [](PageId, Page*) { throw std::runtime_error("injected fuzzy page-write failure"); });
    struct HookReset {
        ~HookReset() { BufferPoolManager::set_flush_batch_before_write_test_hook({}); }
    } hook_reset;

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.fuzzy_attempts, 1U);
    EXPECT_EQ(snapshot.fuzzy_successes, 0U);
    EXPECT_EQ(snapshot.fuzzy_failures, 1U);
    EXPECT_EQ(snapshot.fuzzy_cancels, 0U);
    EXPECT_EQ(snapshot.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::FuzzyLifetime)].count, 1U);
}

TEST(CheckpointScheduleTest, DestroyingActiveFuzzyCheckpointRecordsCancelOnly) {
    ScopedDrainTestDir test_dir("checkpoint_metrics_cancel_root");
    DrainTestDb db("checkpoint_metrics_cancel_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointPhaseMetrics metrics(true);
    const int64_t cut_offset = AppendBegin(&db.log_mgr_, 201);
    ASSERT_GT(cut_offset, 0);
    {
        CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_, &metrics);
        CheckpointOptions options;
        options.auto_checkpoint_bytes = cut_offset;
        checkpoint_mgr.SetOptions(options);
        EXPECT_FALSE(checkpoint_mgr.Tick());
    }
    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.fuzzy_attempts, 1U);
    EXPECT_EQ(snapshot.fuzzy_successes, 0U);
    EXPECT_EQ(snapshot.fuzzy_failures, 0U);
    EXPECT_EQ(snapshot.fuzzy_cancels, 1U);
    EXPECT_EQ(snapshot.timing[static_cast<size_t>(CheckpointPhaseMetrics::Timing::FuzzyLifetime)].count, 1U);
}

TEST(CheckpointScheduleTest, FixedCohortExcludesPagesDirtiedAfterCut) {
    ScopedDrainTestDir test_dir("checkpoint_post_cut_dirty_root");
    DrainTestDb db("checkpoint_post_cut_dirty_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    const PageId pre_cut_page = MakeDirtyTablePage(&db);
    ASSERT_NE(pre_cut_page.page_no, INVALID_PAGE_ID);
    const int64_t cut_offset = AppendBegin(&db.log_mgr_, 250);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = cut_offset;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    const PageId post_cut_page = MakeDirtyTablePage(&db);
    ASSERT_NE(post_cut_page.page_no, INVALID_PAGE_ID);

    EXPECT_TRUE(checkpoint_mgr.Tick());
    EXPECT_FALSE(IsDirty(&db.bpm_, pre_cut_page));
    EXPECT_TRUE(IsDirty(&db.bpm_, post_cut_page));
}

TEST(CheckpointScheduleTest, FixedCohortIncludesTableAndIndexPages) {
    ScopedDrainTestDir test_dir("checkpoint_table_index_cohort_root");
    DrainTestDb db("checkpoint_table_index_cohort_db", true);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    const PageId table_page = MakeDirtyTablePage(&db);
    ASSERT_NE(table_page.page_no, INVALID_PAGE_ID);
    const int index_fd = db.sm_mgr_.ihs_.begin()->second->GetFd();
    const PageId index_root{index_fd, IX_INIT_ROOT_PAGE};
    Page* index_page = db.bpm_.fetch_page(index_root);
    ASSERT_NE(index_page, nullptr);
    BufferPoolManager::mark_dirty(index_page);
    ASSERT_TRUE(db.bpm_.unpin_page(index_root, false));

    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 275);
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_TRUE(checkpoint_mgr.Tick());
    EXPECT_FALSE(IsDirty(&db.bpm_, table_page));
    EXPECT_FALSE(IsDirty(&db.bpm_, index_root));
    EXPECT_GT(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
}

TEST(CheckpointScheduleTest, TickHonorsByteBudgetAcrossBoundedQuanta) {
    ScopedDrainTestDir test_dir("checkpoint_fixed_cohort_batch_root");
    DrainTestDb db("checkpoint_fixed_cohort_batch_db", false, 1200);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    constexpr size_t kDirtyPages = 65;
    ASSERT_EQ(MakeDirtyTablePages(&db, kDirtyPages), kDirtyPages);
    const int64_t log_offset = AppendBegin(&db.log_mgr_, 300);
    ASSERT_GT(log_offset, 0);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = log_offset;
    options.tick_bytes = 64ULL * PAGE_SIZE;
    options.tick_time_us = 1000000;
    options.io_quantum_pages = 64;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, kDirtyPages - 64);
    EXPECT_TRUE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 0u);
}

TEST(CheckpointScheduleTest, TickDeadlineYieldsAfterOneQuantum) {
    ScopedDrainTestDir test_dir("checkpoint_tick_deadline_root");
    DrainTestDb db("checkpoint_tick_deadline_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointPhaseMetrics metrics(true);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_, &metrics);
    ASSERT_EQ(MakeDirtyTablePages(&db, 2), 2u);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 325);
    options.tick_bytes = 2ULL * PAGE_SIZE;
    options.tick_time_us = 100;
    options.io_quantum_pages = 1;
    checkpoint_mgr.SetOptions(options);
    ASSERT_FALSE(checkpoint_mgr.Tick());

    BufferPoolManager::set_flush_batch_before_write_test_hook([](PageId, Page*) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    struct HookReset {
        ~HookReset() {
            BufferPoolManager::set_flush_batch_before_write_test_hook({});
        }
    } hook_reset;
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 1u);
    EXPECT_EQ(metrics.snapshot().budget_yields_time, 1u);
    EXPECT_TRUE(checkpoint_mgr.Tick());
}

TEST(CheckpointScheduleTest, TickLeavesPagesDirtyBelowRelativeTarget) {
    ScopedDrainTestDir test_dir("checkpoint_relative_target_root");
    DrainTestDb db("checkpoint_relative_target_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    const int64_t log_offset = AppendBegin(&db.log_mgr_, 350);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = log_offset * 3;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 1u);
}

TEST(CheckpointScheduleTest, TickDoesNotStartAtZeroForOneByteTarget) {
    ScopedDrainTestDir test_dir("checkpoint_one_byte_target_root");
    DrainTestDb db("checkpoint_one_byte_target_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = 1;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 1u);
}

TEST(CheckpointScheduleTest, ExplicitCleanWaitsForInFlightFuzzyCohort) {
    ScopedDrainTestDir test_dir("checkpoint_fuzzy_serial_root");
    DrainTestDb db("checkpoint_fuzzy_serial_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    const int64_t log_offset = AppendBegin(&db.log_mgr_, 400);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = log_offset;
    checkpoint_mgr.SetOptions(options);
    ASSERT_FALSE(checkpoint_mgr.Tick());

    std::promise<void> cohort_flush_entered_signal;
    auto cohort_flush_entered = cohort_flush_entered_signal.get_future();
    std::promise<void> release_cohort_flush_signal;
    const auto release_cohort_flush = release_cohort_flush_signal.get_future().share();
    std::atomic<bool> first_flush{true};
    BufferPoolManager::set_flush_batch_before_write_test_hook([&](PageId, Page*) {
        if (first_flush.exchange(false)) {
            cohort_flush_entered_signal.set_value();
            release_cohort_flush.wait();
        }
    });
    struct HookReset {
        ~HookReset() {
            BufferPoolManager::set_flush_batch_before_write_test_hook({});
        }
    } hook_reset;

    auto cohort_tick = std::async(std::launch::async, [&] { return checkpoint_mgr.Tick(); });
    const auto cohort_status = cohort_flush_entered.wait_for(std::chrono::seconds(1));
    if (cohort_status != std::future_status::ready) {
        release_cohort_flush_signal.set_value();
    }
    ASSERT_EQ(cohort_status, std::future_status::ready);
    CheckpointManager explicit_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    auto explicit_clean = std::async(std::launch::async, [&] { return explicit_mgr.RunCleanCheckpoint(); });
    EXPECT_EQ(explicit_clean.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    release_cohort_flush_signal.set_value();
    EXPECT_TRUE(cohort_tick.get());
    EXPECT_TRUE(explicit_clean.get());
    EXPECT_EQ(db.log_mgr_.current_log_offset(), 0);
}

TEST(CheckpointScheduleTest, AutomaticDrainExceptionIsContainedAndUnblocksAdmission) {
    ScopedDrainTestDir test_dir("checkpoint_automatic_exception_root");
    DrainTestDb db("checkpoint_automatic_exception_db");
    LockManager lock_mgr;

    std::atomic<bool> throw_once{true};
    TransactionManager::CheckpointAdmissionTestOptions test_options;
    test_options.hook = [&](std::string_view event) {
        if (event == "drain_waiting" && throw_once.exchange(false)) {
            throw std::runtime_error("injected automatic checkpoint drain failure");
        }
    };
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_, ConcurrencyMode::TWO_PHASE_LOCKING, std::move(test_options));
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 450);
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    auto begin_result = std::async(std::launch::async, [&] { return txn_mgr.begin(nullptr, &db.log_mgr_); });
    ASSERT_EQ(begin_result.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "automatic checkpoint exception left transaction admission blocked";
    Transaction* txn = begin_result.get();
    ASSERT_NE(txn, nullptr);
    txn_mgr.abort(txn, &db.log_mgr_);
}
