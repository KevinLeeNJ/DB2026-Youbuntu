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

class ScopedEnvironmentVariable {
public:
    ScopedEnvironmentVariable(const char* name, const char* value) : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        setenv(name, value, 1);
    }
    ~ScopedEnvironmentVariable() {
        if (had_previous_)
            setenv(name_.c_str(), previous_.c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

private:
    std::string name_;
    std::string previous_;
    bool had_previous_{false};
};

TEST(CheckpointOptionsTest, ProductionDefaultTargetIsFourGiBWithPrecleanOff) {
    const CheckpointOptions options;
    EXPECT_EQ(options.auto_checkpoint_bytes, 4LL * 1024 * 1024 * 1024);
    EXPECT_EQ(options.tick_bytes, 1ULL * 1024 * 1024);
    EXPECT_EQ(options.tick_time_us, 5000u);
    EXPECT_EQ(options.io_quantum_pages, 64u);
    EXPECT_FALSE(options.background_preclean_enabled);
    EXPECT_EQ(options.background_preclean_low_percent, 10u);
    EXPECT_EQ(options.background_preclean_high_percent, 15u);
    EXPECT_EQ(options.background_preclean_batch_pages, 160u);
    EXPECT_EQ(options.background_preclean_max_pages, 160u);
}

TEST(CheckpointOptionsTest, BackgroundPrecleanDefaultsOffAndEnvironmentCanOptIn) {
    ScopedEnvironmentVariable opt_in("RMDB_BACKGROUND_PRECLEAN", "1");
    EXPECT_TRUE(CheckpointOptions::FromEnvironment().background_preclean_enabled);
}

TEST(CheckpointOptionsTest, EnvironmentIsStrictAndClamped) {
    const auto set = [](const char* key, const char* value) { ASSERT_EQ(setenv(key, value, 1), 0); };
    set("RMDB_AUTO_CHECKPOINT_BYTES", "1");
    set("RMDB_CHECKPOINT_TICK_BYTES", "9999999999999");
    set("RMDB_CHECKPOINT_TICK_TIME_US", "1");
    set("RMDB_CHECKPOINT_IO_QUANTUM_PAGES", "999");
    set("RMDB_BACKGROUND_PRECLEAN_LOW_PERCENT", "22");
    set("RMDB_BACKGROUND_PRECLEAN_HIGH_PERCENT", "44");
    set("RMDB_BACKGROUND_PRECLEAN_BATCH_PAGES", "96");
    set("RMDB_BACKGROUND_PRECLEAN_MAX_PAGES", "192");
    const CheckpointOptions clamped = CheckpointOptions::FromEnvironment();
    EXPECT_EQ(clamped.auto_checkpoint_bytes, 1LL * 1024 * 1024);
    EXPECT_EQ(clamped.tick_bytes, 1024ULL * 1024 * 1024);
    EXPECT_EQ(clamped.tick_time_us, 100u);
    EXPECT_EQ(clamped.io_quantum_pages, 64u);
    EXPECT_EQ(clamped.background_preclean_low_percent, 22u);
    EXPECT_EQ(clamped.background_preclean_high_percent, 44u);
    EXPECT_EQ(clamped.background_preclean_batch_pages, 96u);
    EXPECT_EQ(clamped.background_preclean_max_pages, 192u);
    set("RMDB_CHECKPOINT_TICK_BYTES", "64k");
    EXPECT_THROW((void)CheckpointOptions::FromEnvironment(), std::invalid_argument);
    unsetenv("RMDB_AUTO_CHECKPOINT_BYTES");
    unsetenv("RMDB_CHECKPOINT_TICK_BYTES");
    unsetenv("RMDB_CHECKPOINT_TICK_TIME_US");
    unsetenv("RMDB_CHECKPOINT_IO_QUANTUM_PAGES");
    unsetenv("RMDB_BACKGROUND_PRECLEAN_LOW_PERCENT");
    unsetenv("RMDB_BACKGROUND_PRECLEAN_HIGH_PERCENT");
    unsetenv("RMDB_BACKGROUND_PRECLEAN_BATCH_PAGES");
    unsetenv("RMDB_BACKGROUND_PRECLEAN_MAX_PAGES");
}

TEST(CheckpointOptionsTest, PrecleanBudgetCapsDebtAndPausesForSlowWal) {
    EXPECT_EQ(CheckpointManager::compute_background_preclean_budget_for_test(200000, 200000, 20, 32, 512, 0), 512u);
    EXPECT_EQ(CheckpointManager::compute_background_preclean_budget_for_test(60000, 200000, 20, 512, 512,
                                                                             80ULL * 1000 * 1000),
              0u);
    EXPECT_EQ(CheckpointManager::compute_background_preclean_budget_for_test(150000, 200000, 20, 32, 512,
                                                                             80ULL * 1000 * 1000),
              0u);
}

TEST(CheckpointOptionsTest, PrecleanMetricsSeparatePauseThrottleAndRamp) {
    BackgroundPrecleanMetrics metrics(true);
    metrics.congestion_pause();
    metrics.congestion_throttle();
    metrics.congestion_ramp();
    metrics.foreground_dirty_pwrite(4096, 7, true);
    metrics.background_dirty_pwrite(8192, 11, false);
    metrics.checkpoint_dirty_pwrite(4096, 13, true);
    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.congestion_pauses, 1u);
    EXPECT_EQ(snapshot.congestion_throttles, 1u);
    EXPECT_EQ(snapshot.congestion_ramps, 1u);
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.timing.count, 1u);
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.bytes, 4096u);
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.timing.elapsed_ns, 7u);
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.runs_with_wal_dependency, 1u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.timing.count, 1u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.bytes, 8192u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.timing.elapsed_ns, 11u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.runs_with_wal_dependency, 0u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.timing.count, 1u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.bytes, 4096u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.timing.elapsed_ns, 13u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.runs_with_wal_dependency, 1u);
}

TEST(CheckpointOptionsTest, DisabledPrecleanMetricsDoNotRecordDirtyPwrites) {
    BackgroundPrecleanMetrics metrics(false);
    metrics.foreground_dirty_pwrite(4096, 7, true);
    metrics.background_dirty_pwrite(8192, 11, true);
    metrics.checkpoint_dirty_pwrite(4096, 13, true);

    const auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.timing.count, 0u);
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.bytes, 0u);
    EXPECT_EQ(snapshot.foreground_dirty_pwrite.runs_with_wal_dependency, 0u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.timing.count, 0u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.bytes, 0u);
    EXPECT_EQ(snapshot.background_dirty_pwrite.runs_with_wal_dependency, 0u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.timing.count, 0u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.bytes, 0u);
    EXPECT_EQ(snapshot.checkpoint_dirty_pwrite.runs_with_wal_dependency, 0u);
}

TEST(CheckpointOptionsTest, ControllerCountersSaturateWithoutUnsignedWrap) {
    EXPECT_EQ(CheckpointManager::saturate_healthy_observations_for_test(0, UINT64_MAX), 5u);
    EXPECT_EQ(CheckpointManager::saturate_healthy_observations_for_test(4, UINT64_MAX), 5u);
    EXPECT_EQ(CheckpointManager::saturate_ramp_level_for_test(254), 255u);
    EXPECT_EQ(CheckpointManager::saturate_ramp_level_for_test(255), 255u);
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

bool AdvanceFuzzyUntilFinished(CheckpointManager* checkpoint_mgr) {
    for (int i = 0; i < 32; ++i) {
        if (checkpoint_mgr->Tick())
            return true;
    }
    return false;
}

int64_t AppendBegin(LogManager* log_manager, txn_id_t txn_id) {
    BeginLogRecord record(txn_id);
    log_manager->add_log_to_buffer(&record);
    return log_manager->current_log_offset();
}

TEST(CheckpointScheduleTest, FdatasyncObservationWindowCountsConcurrentPublishers) {
    DiskManager disk;
    LogManager log_manager(&disk);
    constexpr size_t kThreads = 8;
    constexpr size_t kSamplesPerThread = 100;
    std::vector<std::thread> publishers;
    publishers.reserve(kThreads);
    for (size_t thread = 0; thread < kThreads; ++thread) {
        publishers.emplace_back([&, thread] {
            for (size_t sample = 0; sample < kSamplesPerThread; ++sample) {
                log_manager.publish_fdatasync_observation_for_test(thread * kSamplesPerThread + sample + 1);
            }
        });
    }
    for (auto& publisher : publishers)
        publisher.join();

    const auto window = log_manager.consume_fdatasync_observations();
    EXPECT_EQ(window.count, kThreads * kSamplesPerThread);
    EXPECT_EQ(window.sequence, kThreads * kSamplesPerThread);
    EXPECT_EQ(window.max_elapsed_ns, kThreads * kSamplesPerThread);
    EXPECT_EQ(log_manager.consume_fdatasync_observations().count, 0u);
}

TEST(CheckpointScheduleTest, PrecleanControllerUsesNewWalSamplesAndRecoversAfterFiveHealthySamples) {
    ScopedEnvironmentVariable metrics_env("RMDB_BACKGROUND_PRECLEAN_METRICS", "1");
    ScopedDrainTestDir test_dir("checkpoint_preclean_controller_root");
    DrainTestDb db("checkpoint_preclean_controller_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.background_preclean_enabled = true;
    options.background_preclean_batch_pages = 512;
    options.background_preclean_max_pages = 512;
    checkpoint_mgr.SetOptions(options);
    ASSERT_EQ(MakeDirtyTablePages(&db, 40), 40u);

    db.log_mgr_.set_fdatasync_observation_for_test(1, 80ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    auto slow = checkpoint_mgr.background_preclean_controller_snapshot_for_test();
    EXPECT_TRUE(slow.active);
    EXPECT_EQ(slow.wal_sequence, 1u);
    EXPECT_EQ(slow.last_budget, 0u);
    EXPECT_EQ(db.bpm_.background_preclean_metrics().snapshot().congestion_pauses, 1u);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    auto duplicate = checkpoint_mgr.background_preclean_controller_snapshot_for_test();
    EXPECT_EQ(duplicate.wal_sequence, 1u);
    EXPECT_EQ(duplicate.wal_ewma_ns, slow.wal_ewma_ns);
    EXPECT_EQ(duplicate.healthy_samples, 0u);

    for (uint64_t sequence = 2; sequence <= 6; ++sequence) {
        db.log_mgr_.set_fdatasync_observation_for_test(sequence, 4ULL * 1000 * 1000);
        EXPECT_FALSE(checkpoint_mgr.Tick());
    }
    const auto recovered = checkpoint_mgr.background_preclean_controller_snapshot_for_test();
    EXPECT_EQ(recovered.healthy_samples, 5u);
    EXPECT_EQ(recovered.wal_ewma_ns, 4ULL * 1000 * 1000);
    EXPECT_EQ(recovered.last_budget, 512u);
    EXPECT_EQ(recovered.ramp_level, 1u);
    EXPECT_EQ(db.bpm_.background_preclean_metrics().snapshot().congestion_ramps, 1u);
}

TEST(CheckpointScheduleTest, PrecleanControllerKeepsSlowSampleWhenFastSampleSharesTick) {
    ScopedDrainTestDir test_dir("checkpoint_preclean_observation_window_root");
    DrainTestDb db("checkpoint_preclean_observation_window_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.background_preclean_enabled = true;
    options.background_preclean_batch_pages = 512;
    options.background_preclean_max_pages = 512;
    checkpoint_mgr.SetOptions(options);
    ASSERT_EQ(MakeDirtyTablePages(&db, 40), 40u);

    // Both physical syncs become visible before this scheduler tick.  The
    // later fast sample must not erase the preceding 80ms congestion signal.
    db.log_mgr_.set_fdatasync_observation_for_test(1, 80ULL * 1000 * 1000);
    db.log_mgr_.set_fdatasync_observation_for_test(2, 4ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    const auto snapshot = checkpoint_mgr.background_preclean_controller_snapshot_for_test();
    EXPECT_EQ(snapshot.wal_sequence, 2u);
    EXPECT_EQ(snapshot.healthy_samples, 0u);
    EXPECT_EQ(snapshot.last_budget, 0u);
}

TEST(CheckpointScheduleTest, PrecleanRampsOnlyAfterFiveHealthyObservations) {
    ScopedDrainTestDir test_dir("checkpoint_preclean_ramp_root");
    DrainTestDb db("checkpoint_preclean_ramp_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.background_preclean_enabled = true;
    options.background_preclean_batch_pages = 32;
    options.background_preclean_max_pages = 160;
    checkpoint_mgr.SetOptions(options);
    ASSERT_EQ(MakeDirtyTablePages(&db, 100), 100u);

    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(checkpoint_mgr.background_preclean_controller_snapshot_for_test().last_budget, 0u);
    for (uint64_t sequence = 2; sequence <= 6; ++sequence) {
        db.log_mgr_.set_fdatasync_observation_for_test(sequence, 4ULL * 1000 * 1000);
        EXPECT_FALSE(checkpoint_mgr.Tick());
    }
    EXPECT_EQ(checkpoint_mgr.background_preclean_controller_snapshot_for_test().last_budget, 32u);
    db.log_mgr_.set_fdatasync_observation_for_test(7, 4ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(checkpoint_mgr.background_preclean_controller_snapshot_for_test().last_budget, 64u);
}

TEST(CheckpointScheduleTest, CleanCheckpointWaitsForInFlightPrecleanFlush) {
    ScopedDrainTestDir test_dir("checkpoint_preclean_clean_coordination_root");
    DrainTestDb db("checkpoint_preclean_clean_coordination_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager preclean_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointManager clean_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions preclean_options;
    preclean_options.background_preclean_enabled = true;
    preclean_mgr.SetOptions(preclean_options);
    const PageId durable_page = MakeDirtyTablePage(&db);
    ASSERT_NE(durable_page.page_no, INVALID_PAGE_ID);
    Page* page = db.bpm_.fetch_page(durable_page);
    ASSERT_NE(page, nullptr);
    std::strcpy(page->get_data() + Page::OFFSET_PAGE_HDR, "preclean-before-clean");
    ASSERT_TRUE(db.bpm_.unpin_page(durable_page, true));
    ASSERT_EQ(MakeDirtyTablePages(&db, 39), 39u);
    ASSERT_GT(AppendBegin(&db.log_mgr_, 700), 0);
    db.log_mgr_.flush_log_to_disk_with_sync();
    ASSERT_GT(db.disk_.get_file_size(LOG_FILE_NAME), 0);

    std::mutex mutex;
    std::condition_variable cv;
    bool preclean_claimed = false;
    bool release_preclean = false;
    std::thread::id preclean_thread;
    PageId preclean_target;
    bool preclean_target_set = false;
    bool preclean_hook_timed_out = false;
    std::atomic<int> stage{0};
    std::atomic<bool> preclean_write_recorded{false};
    std::atomic<bool> clean_touched_preclean_stage{false};
    std::atomic<int> preclean_write_stage{0};
    std::atomic<int> clean_sync_stage{0};
    std::atomic<int> reset_stage{0};
    BufferPoolManager::set_flush_batch_before_write_test_hook([&](PageId page_id, Page*) {
        std::unique_lock lock{mutex};
        if (std::this_thread::get_id() != preclean_thread)
            return;
        if (!preclean_target_set) {
            preclean_target = page_id;
            preclean_target_set = true;
        }
        if (!(page_id == preclean_target))
            return;
        preclean_claimed = true;
        cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds(2), [&] { return release_preclean; })) {
            preclean_hook_timed_out = true;
        }
    });
    BufferPoolManager::set_flush_page_after_write_test_hook([&](PageId page_id, Page*) {
        {
            std::lock_guard lock{mutex};
            if (!preclean_target_set || !(page_id == preclean_target))
                return;
            if (std::this_thread::get_id() != preclean_thread) {
                clean_touched_preclean_stage = true;
                return;
            }
        }
        bool expected = false;
        if (preclean_write_recorded.compare_exchange_strong(expected, true)) {
            preclean_write_stage = stage.fetch_add(1) + 1;
        }
    });
    CheckpointManager::set_phase_test_hook([&](std::string_view phase) {
        if (phase == "clean_data_sync_end")
            clean_sync_stage = stage.fetch_add(1) + 1;
        if (phase == "reset_wal_begin")
            reset_stage = stage.fetch_add(1) + 1;
    });
    struct HookReset {
        ~HookReset() {
            BufferPoolManager::set_flush_batch_before_write_test_hook({});
            BufferPoolManager::set_flush_page_after_write_test_hook({});
            CheckpointManager::set_phase_test_hook({});
        }
    } hook_reset;

    auto preclean = std::async(std::launch::async, [&] {
        {
            std::lock_guard lock{mutex};
            preclean_thread = std::this_thread::get_id();
        }
        return preclean_mgr.Tick();
    });
    {
        std::unique_lock lock{mutex};
        if (!cv.wait_for(lock, std::chrono::seconds(1), [&] { return preclean_claimed; })) {
            release_preclean = true;
            lock.unlock();
            cv.notify_all();
            const auto rescued = preclean.wait_for(std::chrono::seconds(1));
            EXPECT_EQ(rescued, std::future_status::ready);
            FAIL() << "preclean never claimed a dirty frame";
        }
    }
    std::promise<void> clean_started_promise;
    auto clean_started = clean_started_promise.get_future();
    auto clean = std::async(std::launch::async, [&] {
        clean_started_promise.set_value();
        return clean_mgr.RunCleanCheckpoint();
    });
    const auto clean_started_status = clean_started.wait_for(std::chrono::seconds(1));
    if (clean_started_status != std::future_status::ready) {
        {
            std::lock_guard lock{mutex};
            release_preclean = true;
        }
        cv.notify_all();
        const auto preclean_rescued = preclean.wait_for(std::chrono::seconds(1));
        const auto clean_rescued = clean.wait_for(std::chrono::seconds(3));
        EXPECT_EQ(preclean_rescued, std::future_status::ready);
        EXPECT_EQ(clean_rescued, std::future_status::ready);
        FAIL() << "clean worker did not publish its started handshake";
    }
    const auto clean_while_preclean_blocked = clean.wait_for(std::chrono::milliseconds(30));
    const int64_t wal_size_while_blocked = db.disk_.get_file_size(LOG_FILE_NAME);
    {
        std::lock_guard lock{mutex};
        release_preclean = true;
    }
    cv.notify_all();
    const auto preclean_finished = preclean.wait_for(std::chrono::seconds(1));
    const auto clean_finished = clean.wait_for(std::chrono::seconds(3));
    ASSERT_EQ(preclean_finished, std::future_status::ready);
    ASSERT_EQ(clean_finished, std::future_status::ready);
    EXPECT_FALSE(preclean.get());
    EXPECT_TRUE(clean.get());
    EXPECT_EQ(clean_while_preclean_blocked, std::future_status::timeout);
    EXPECT_GT(wal_size_while_blocked, 0);
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
    EXPECT_EQ(preclean_write_stage.load(), 1);
    EXPECT_EQ(clean_sync_stage.load(), 2);
    EXPECT_EQ(reset_stage.load(), 3);
    EXPECT_FALSE(clean_touched_preclean_stage.load());
    EXPECT_FALSE(preclean_hook_timed_out);
    std::array<char, PAGE_SIZE> disk_image{};
    db.disk_.read_page(durable_page.fd, durable_page.page_no, disk_image.data(), PAGE_SIZE);
    EXPECT_STREQ(disk_image.data() + Page::OFFSET_PAGE_HDR, "preclean-before-clean");
}

TEST(CheckpointScheduleTest, PrecleanControllerKeepsHysteresisUntilLowWatermark) {
    ScopedDrainTestDir test_dir("checkpoint_preclean_hysteresis_root");
    DrainTestDb db("checkpoint_preclean_hysteresis_db", false, 64);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.background_preclean_enabled = true;
    checkpoint_mgr.SetOptions(options);
    ASSERT_EQ(MakeDirtyTablePages(&db, 20), 20u);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_TRUE(checkpoint_mgr.background_preclean_controller_snapshot_for_test().active);
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 0u);
    EXPECT_TRUE(checkpoint_mgr.background_preclean_controller_snapshot_for_test().active);

    checkpoint_mgr.force_background_preclean_sample_for_test();
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.background_preclean_controller_snapshot_for_test().active);
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
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
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

    // Later ticks pace final fdatasync one descriptor at a time before
    // atomically publishing metadata and the cut.
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
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

TEST(CheckpointScheduleTest, StartReopensAdmissionBeforeCutDurableSync) {
    ScopedDrainTestDir test_dir("checkpoint_start_reopens_admission_root");
    DrainTestDb db("checkpoint_start_reopens_admission_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 260);
    checkpoint_mgr.SetOptions(options);

    ASSERT_FALSE(checkpoint_mgr.Tick());
    auto begin_result = std::async(std::launch::async, [&] { return txn_mgr.begin(nullptr, &db.log_mgr_); });
    ASSERT_EQ(begin_result.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
    Transaction* txn = begin_result.get();
    ASSERT_NE(txn, nullptr);
    txn_mgr.abort(txn, &db.log_mgr_);
}

TEST(CheckpointScheduleTest, CutSyncFailureFlushesNoCohortPageOrManifest) {
    ScopedDrainTestDir test_dir("checkpoint_cut_sync_failure_root");
    DrainTestDb db("checkpoint_cut_sync_failure_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointPhaseMetrics metrics(true);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_, &metrics);

    const PageId dirty_page = MakeDirtyTablePage(&db);
    ASSERT_NE(dirty_page.page_no, INVALID_PAGE_ID);
    db.disk_.SetLogFd(db.disk_.open_file(LOG_FILE_NAME));
    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 261);
    checkpoint_mgr.SetOptions(options);

    ASSERT_FALSE(checkpoint_mgr.Tick());
    const int log_fd = db.disk_.GetLogFd();
    ASSERT_GE(log_fd, 0);
    db.disk_.SetLogFd(-2);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    db.disk_.SetLogFd(log_fd);

    EXPECT_TRUE(IsDirty(&db.bpm_, dirty_page));
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
    EXPECT_EQ(metrics.snapshot().fuzzy_failures, 1U);
}

TEST(CheckpointScheduleTest, FinalPublishSyncsOneDescriptorPerTick) {
    ScopedDrainTestDir test_dir("checkpoint_one_fd_per_tick_root");
    DrainTestDb db("checkpoint_one_fd_per_tick_db", true);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);

    ASSERT_NE(MakeDirtyTablePage(&db).page_no, INVALID_PAGE_ID);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 262);
    checkpoint_mgr.SetOptions(options);

    ASSERT_EQ(db.sm_mgr_.fhs_.size() + db.sm_mgr_.ihs_.size(), 2U);
    EXPECT_FALSE(checkpoint_mgr.Tick()); // start
    EXPECT_FALSE(checkpoint_mgr.Tick()); // durable cut only
    EXPECT_FALSE(checkpoint_mgr.Tick()); // cohort and header write
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
    EXPECT_FALSE(checkpoint_mgr.Tick()); // first fdatasync
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
    EXPECT_FALSE(checkpoint_mgr.Tick()); // second fdatasync
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
    EXPECT_TRUE(checkpoint_mgr.Tick()); // db.meta then manifest
    EXPECT_GT(db.log_mgr_.read_restart_manifest().checkpoint_offset, 0);
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
        ~HookReset() {
            BufferPoolManager::set_flush_batch_before_write_test_hook({});
        }
    } hook_reset;

    EXPECT_FALSE(checkpoint_mgr.Tick());
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

    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
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
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
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
    (void)db.log_mgr_.consume_fdatasync_observations();
    for (uint64_t sequence = 1; sequence <= 5; ++sequence)
        db.log_mgr_.set_fdatasync_observation_for_test(sequence, 4ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, kDirtyPages - 64);
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 0u);
}

TEST(CheckpointScheduleTest, FuzzyCohortCongestionStillMakesMandatoryProgress) {
    ScopedDrainTestDir test_dir("checkpoint_fuzzy_congestion_pause_root");
    DrainTestDb db("checkpoint_fuzzy_congestion_pause_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    constexpr size_t kDirtyPages = 80;
    ASSERT_EQ(MakeDirtyTablePages(&db, kDirtyPages), kDirtyPages);
    int64_t target = 0;
    for (txn_id_t id = 0; id < 128; ++id)
        target = AppendBegin(&db.log_mgr_, 1000 + id);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = target;
    options.background_preclean_enabled = false;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick()); // cut
    EXPECT_FALSE(checkpoint_mgr.Tick()); // durable cut
    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_LT(db.sm_mgr_.table_dirty_page_stats().dirty_pages, kDirtyPages);
    EXPECT_FALSE(checkpoint_mgr.background_preclean_controller_snapshot_for_test().active);
}

TEST(CheckpointScheduleTest, RepeatedSlowWalObservationsCannotStarveCohortDebt) {
    ScopedDrainTestDir test_dir("checkpoint_fuzzy_repeated_slow_root");
    DrainTestDb db("checkpoint_fuzzy_repeated_slow_db", false, 8);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    ASSERT_EQ(MakeDirtyTablePages(&db, 3), 3u);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = AppendBegin(&db.log_mgr_, 1123);
    options.tick_bytes = PAGE_SIZE;
    options.tick_time_us = 1000000;
    options.io_quantum_pages = 1;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick()); // cut
    EXPECT_FALSE(checkpoint_mgr.Tick()); // durable cut
    for (uint64_t sequence = 1; sequence <= 5; ++sequence) {
        db.log_mgr_.set_fdatasync_observation_for_test(sequence, 9ULL * 1000 * 1000);
        EXPECT_FALSE(checkpoint_mgr.Tick());
        EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 3u - (sequence + 1) / 2);
    }
}

TEST(CheckpointScheduleTest, CohortServiceDebtSeparatesAheadFromBehind) {
    EXPECT_FALSE(CheckpointManager::cohort_service_debt_for_test(8, 0, 0, 8, true));
    EXPECT_TRUE(CheckpointManager::cohort_service_debt_for_test(8, 8, 0, 8, true));
    EXPECT_FALSE(CheckpointManager::cohort_service_debt_for_test(8, 4, 0, 8, true));
    EXPECT_TRUE(CheckpointManager::cohort_service_debt_for_test(8, 4, 4, 8, true));
    EXPECT_TRUE(CheckpointManager::cohort_service_debt_for_test(8, 4, 0, 8, false));
}

TEST(CheckpointScheduleTest, FuzzyCohortDoesNotWaitForHealthySamples) {
    ScopedDrainTestDir test_dir("checkpoint_fuzzy_healthy_ramp_root");
    DrainTestDb db("checkpoint_fuzzy_healthy_ramp_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    constexpr size_t kDirtyPages = 80;
    ASSERT_EQ(MakeDirtyTablePages(&db, kDirtyPages), kDirtyPages);
    int64_t target = 0;
    for (txn_id_t id = 0; id < 128; ++id)
        target = AppendBegin(&db.log_mgr_, 2000 + id);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = target;
    options.background_preclean_enabled = false;
    options.tick_time_us = 1000000;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 0u);
}

TEST(CheckpointScheduleTest, FuzzyCohortNoPostCutWalStillMakesInitialProgress) {
    ScopedDrainTestDir test_dir("checkpoint_fuzzy_emergency_progress_root");
    DrainTestDb db("checkpoint_fuzzy_emergency_progress_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    constexpr size_t kDirtyPages = 80;
    ASSERT_EQ(MakeDirtyTablePages(&db, kDirtyPages), kDirtyPages);
    int64_t target = 0;
    for (txn_id_t id = 0; id < 128; ++id)
        target = AppendBegin(&db.log_mgr_, 3000 + id);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = target;
    options.background_preclean_enabled = false;
    options.tick_time_us = 1000000;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 0u);
}

TEST(CheckpointScheduleTest, CutTailCountsWalAppendedBeforeCutSync) {
    ScopedDrainTestDir test_dir("checkpoint_cut_tail_growth_root");
    DrainTestDb db("checkpoint_cut_tail_growth_db", false, 8);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    ASSERT_EQ(MakeDirtyTablePages(&db, 3), 3u);
    const int64_t target = AppendBegin(&db.log_mgr_, 9000);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = target;
    options.tick_bytes = PAGE_SIZE;
    options.tick_time_us = 1000000;
    options.io_quantum_pages = 1;
    checkpoint_mgr.SetOptions(options);

    bool appended_after_cut = false;
    CheckpointManager::set_phase_test_hook([&](std::string_view phase) {
        if (phase == "fuzzy_cut_appended" && !appended_after_cut) {
            appended_after_cut = true;
            (void)AppendBegin(&db.log_mgr_, 9001);
        }
    });
    struct HookReset {
        ~HookReset() {
            CheckpointManager::set_phase_test_hook({});
        }
    } hook_reset;

    EXPECT_FALSE(checkpoint_mgr.Tick()); // start and append after the cut
    EXPECT_TRUE(appended_after_cut);
    EXPECT_FALSE(checkpoint_mgr.Tick()); // durable cut
    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 2u);
    db.log_mgr_.set_fdatasync_observation_for_test(2, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    // The append is measured from tail_offset captured under the WAL latch,
    // so its one-page growth debt is serviced rather than deferred.
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 1u);
}

TEST(CheckpointScheduleTest, FuzzyCohortWithoutNewWalObservationKeepsMakingProgress) {
    ScopedDrainTestDir test_dir("checkpoint_fuzzy_idle_progress_root");
    DrainTestDb db("checkpoint_fuzzy_idle_progress_db", false, 8);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    constexpr size_t kDirtyPages = 3;
    ASSERT_EQ(MakeDirtyTablePages(&db, kDirtyPages), kDirtyPages);
    int64_t target = 0;
    for (txn_id_t id = 0; id < 8; ++id)
        target = AppendBegin(&db.log_mgr_, 5000 + id);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = target;
    options.background_preclean_enabled = false;
    options.tick_bytes = PAGE_SIZE;
    options.tick_time_us = 1000000;
    options.io_quantum_pages = 1;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, kDirtyPages - 1);
    // No new observation means no currently proven contention, so the active
    // cohort must not inherit the old 50-tick congestion pause.
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, kDirtyPages - 2);
}

TEST(CheckpointScheduleTest, EmptyFuzzyCohortPublishesDespiteWalCongestion) {
    ScopedDrainTestDir test_dir("checkpoint_empty_fuzzy_congestion_root");
    DrainTestDb db("checkpoint_empty_fuzzy_congestion_db");
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    const int64_t target = AppendBegin(&db.log_mgr_, 6000);
    CheckpointOptions options;
    options.auto_checkpoint_bytes = target;
    options.background_preclean_enabled = false;
    checkpoint_mgr.SetOptions(options);

    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
    EXPECT_EQ(db.log_mgr_.read_restart_manifest().checkpoint_offset, target);
}

TEST(CheckpointScheduleTest, HealthyObservationCountSaturatesAtFiveWithoutNarrowing) {
    ScopedDrainTestDir test_dir("checkpoint_preclean_count_saturation_root");
    DrainTestDb db("checkpoint_preclean_count_saturation_db", false, 128);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, &db.log_mgr_);
    CheckpointOptions options;
    options.background_preclean_enabled = true;
    options.background_preclean_batch_pages = 32;
    options.background_preclean_max_pages = 160;
    checkpoint_mgr.SetOptions(options);
    ASSERT_EQ(MakeDirtyTablePages(&db, 100), 100u);

    db.log_mgr_.set_fdatasync_observation_for_test(1, 9ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    for (size_t sample = 0; sample < 256; ++sample)
        db.log_mgr_.publish_fdatasync_observation_for_test(4ULL * 1000 * 1000);
    EXPECT_FALSE(checkpoint_mgr.Tick());
    const auto snapshot = checkpoint_mgr.background_preclean_controller_snapshot_for_test();
    EXPECT_EQ(snapshot.healthy_samples, 5u);
    EXPECT_EQ(snapshot.ramp_level, 1u);
    EXPECT_EQ(snapshot.last_budget, 32u);
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

    BufferPoolManager::set_flush_batch_before_write_test_hook(
        [](PageId, Page*) { std::this_thread::sleep_for(std::chrono::milliseconds(1)); });
    struct HookReset {
        ~HookReset() {
            BufferPoolManager::set_flush_batch_before_write_test_hook({});
        }
    } hook_reset;
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_FALSE(checkpoint_mgr.Tick());
    EXPECT_EQ(db.sm_mgr_.table_dirty_page_stats().dirty_pages, 1u);
    EXPECT_EQ(metrics.snapshot().budget_yields_time, 1u);
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
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
    ASSERT_FALSE(checkpoint_mgr.Tick());
    (void)db.log_mgr_.consume_fdatasync_observations();
    for (uint64_t sequence = 1; sequence <= 5; ++sequence)
        db.log_mgr_.set_fdatasync_observation_for_test(sequence, 4ULL * 1000 * 1000);

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
    // Final descriptor sync and publication are deliberately paced across
    // later ticks, so this cohort-writing tick is not the completion tick.
    EXPECT_FALSE(cohort_tick.get());
    EXPECT_TRUE(AdvanceFuzzyUntilFinished(&checkpoint_mgr));
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
