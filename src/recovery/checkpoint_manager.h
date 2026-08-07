/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <functional>
#include <string_view>

class LogManager;
class SmManager;
class TransactionManager;
class CheckpointPhaseMetrics;

struct CheckpointOptions {
    // These defaults bound each 100ms scheduler invocation instead of letting a
    // large dirty cut turn into one foreground-visible I/O burst.
    int64_t auto_checkpoint_bytes = 4LL * 1024 * 1024 * 1024;
    size_t tick_bytes = 1ULL * 1024 * 1024;
    uint64_t tick_time_us = 5000;
    size_t io_quantum_pages = 64;
    bool background_preclean_enabled = false;
    uint8_t background_preclean_low_percent = 10;
    uint8_t background_preclean_high_percent = 15;
    size_t background_preclean_batch_pages = 160;
    size_t background_preclean_max_pages = 160;

    // Environment values are strict unsigned decimal; valid out-of-range
    // values are clamped to conservative scheduler limits.
    static CheckpointOptions FromEnvironment();
};

class CheckpointManager {
public:
    CheckpointManager(TransactionManager* txn_mgr, SmManager* sm_mgr, LogManager* log_mgr,
                      CheckpointPhaseMetrics* metrics = nullptr);
    ~CheckpointManager();

    bool RunCleanCheckpoint();
    bool Tick();
    bool RunIfNeeded();
    void SetOptions(CheckpointOptions options);
    struct BackgroundPrecleanControllerSnapshot {
        bool active{false};
        size_t dirty_pages{0};
        size_t capacity{0};
        size_t last_budget{0};
        uint64_t wal_sequence{0};
        uint64_t wal_ewma_ns{0};
        uint8_t healthy_samples{0};
        uint8_t ramp_level{0};
    };
    BackgroundPrecleanControllerSnapshot background_preclean_controller_snapshot_for_test() const noexcept {
        return {background_preclean_active_,          background_preclean_dirty_pages_,
                background_preclean_capacity_,        background_preclean_last_budget_,
                background_preclean_wal_sequence_,    background_preclean_wal_ewma_ns_,
                background_preclean_healthy_samples_, background_preclean_ramp_level_};
    }
    void force_background_preclean_sample_for_test() noexcept {
        background_preclean_next_sample_ = {};
    }
    static size_t compute_background_preclean_budget_for_test(size_t dirty_pages, size_t capacity, uint8_t low_percent,
                                                              size_t base_pages, size_t max_pages,
                                                              uint64_t wal_ewma_ns) noexcept;
    static uint8_t saturate_healthy_observations_for_test(uint8_t current, uint64_t count) noexcept;
    static uint8_t saturate_ramp_level_for_test(uint8_t current) noexcept;
    static void set_phase_test_hook(std::function<void(std::string_view)> hook);

private:
    struct FuzzyCheckpointState;

    bool StartFuzzyCheckpoint();
    bool AdvanceFuzzyCheckpoint();
    void CancelFuzzyCheckpoint(bool record_cancel = true) noexcept;
    void DeferAutomaticRetry() noexcept;
    int64_t RetainedWalBytes(int64_t current_offset) noexcept;
    void MaybeRunBackgroundPreclean();
    void ObserveBackgroundIoWal();
    bool BackgroundIoPaused() const noexcept;
    size_t BackgroundPrecleanBudget() const noexcept;
    size_t FuzzyCohortBudget(size_t configured_budget_pages) noexcept;

    TransactionManager* txn_mgr_;
    SmManager* sm_mgr_;
    LogManager* log_mgr_;
    CheckpointPhaseMetrics* metrics_;
    CheckpointOptions options_{};
    std::unique_ptr<FuzzyCheckpointState> fuzzy_checkpoint_;
    // Backoff state for automatic checkpoints after a failed or over-budget
    // round. Retrying every scheduler tick would make the whole process flap
    // between blocked and unblocked while the underlying condition persists.
    std::chrono::steady_clock::time_point drain_retry_time_{};
    int64_t drain_retry_log_offset_{0};
    bool background_preclean_active_{false};
    std::chrono::steady_clock::time_point background_preclean_next_sample_{};
    size_t background_preclean_dirty_pages_{0};
    size_t background_preclean_capacity_{0};
    uint64_t background_preclean_wal_sequence_{0};
    uint64_t background_preclean_wal_ewma_ns_{0};
    uint8_t background_preclean_healthy_samples_{0};
    uint8_t background_preclean_ramp_level_{0};
    uint64_t background_preclean_wal_window_max_ns_{0};
    bool background_preclean_congestion_latched_{false};
    uint64_t background_io_ticks_without_wal_{0};
    size_t background_preclean_last_budget_{0};
};
