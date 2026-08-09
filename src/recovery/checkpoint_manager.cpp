/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "checkpoint_manager.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/fault_injection.h"
#include "common/checkpoint_phase_metrics.h"
#include "minilog.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace {

// The coordination lock serializes explicit clean checkpoints against the
// complete lifetime of an automatic fuzzy checkpoint. This flag only elects
// one automatic owner because shared-lock holders do not exclude each other.
std::atomic<bool> g_fuzzy_checkpoint_running{false};
std::atomic<uint64_t> g_checkpoint_event_id{0};
// STATIC_CHECKPOINT is created through a separate manager instance. Keeping
// the process's published boundary here lets the persistent background manager
// observe its manifest-zero/WAL-reset transition without polling db.restart on
// every scheduler tick.
std::atomic<int64_t> g_published_restart_offset{0};
// STATIC_CHECKPOINT and the background scheduler use distinct
// CheckpointManager instances. Coordinate their preflush/final-flush phases at
// process scope so a clean checkpoint cannot skip an in-flight FLUSHING page.
std::shared_mutex g_checkpoint_coordination;
std::mutex g_checkpoint_phase_test_hook_latch;
std::function<void(std::string_view)> g_checkpoint_phase_test_hook;
std::atomic<bool> g_checkpoint_phase_test_hook_enabled{false};

// Longest the checkpoint may keep new transactions blocked while waiting for
// active ones to finish. Saturated connections drain well inside this window,
// while a stuck transaction can never stall the startup liveness probe.
constexpr std::chrono::milliseconds kDrainTimeout{2000};
// Admission budget before entering the non-interruptible final data sync.
constexpr std::chrono::seconds kCheckpointDeadline{10};
// After an abandoned round, an automatic checkpoint waits for both this much
// time and this much extra WAL before blocking writers again.
constexpr std::chrono::seconds kDrainRetryBackoff{30};
constexpr int64_t kDrainRetryBackoffBytes = 32LL * 1024 * 1024;
constexpr size_t kMaxCheckpointQuantumPages = 64;
constexpr uint64_t kMinAutoCheckpointBytes = 1ULL * 1024 * 1024;
constexpr uint64_t kMaxAutoCheckpointBytes = 1ULL * 1024 * 1024 * 1024 * 1024;
constexpr uint64_t kMinTickBytes = PAGE_SIZE;
constexpr uint64_t kMaxTickBytes = 1024ULL * 1024 * 1024;
constexpr uint64_t kMinTickTimeUs = 100;
constexpr uint64_t kMaxTickTimeUs = 1000000;
constexpr size_t kMinPrecleanBatchPages = 32;
constexpr size_t kMaxPrecleanBatchPages = 512;
constexpr size_t kPrecleanHorizonTicks = 300;
constexpr uint64_t kHealthyWalFdatasyncNs = 8ULL * 1000 * 1000;
constexpr uint8_t kHealthyWalObservationsToResume = 5;

size_t PercentPages(size_t capacity, uint8_t percent, bool round_up) noexcept {
    const size_t quotient = capacity / 100;
    const size_t remainder = capacity % 100;
    const size_t residual = remainder * percent;
    return quotient * percent + (residual + (round_up ? 99 : 0)) / 100;
}

bool ReadCheckpointBoolEnv(const char* name, bool default_value) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) return default_value;
    if (std::strcmp(raw, "1") == 0) return true;
    if (std::strcmp(raw, "0") == 0) return false;
    throw std::invalid_argument(std::string("invalid ") + name + ": expected 0 or 1");
}

uint64_t NextCheckpointEventId() noexcept {
    return g_checkpoint_event_id.fetch_add(1, std::memory_order_relaxed) + 1;
}

uint64_t ElapsedMs(std::chrono::steady_clock::time_point started) noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count());
}

uint64_t ReadCheckpointEnv(const char* name, uint64_t default_value, uint64_t minimum, uint64_t maximum) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) return default_value;
    const char* end = raw;
    while (*end != '\0') ++end;
    uint64_t parsed = 0;
    const auto result = std::from_chars(raw, end, parsed);
    if (raw == end || result.ec != std::errc{} || result.ptr != end) {
        throw std::invalid_argument(std::string("invalid ") + name + ": expected unsigned decimal");
    }
    return std::clamp(parsed, minimum, maximum);
}

void RunCheckpointPhaseTestHook(std::string_view phase) {
    if (!g_checkpoint_phase_test_hook_enabled.load(std::memory_order_acquire)) return;
    std::function<void(std::string_view)> hook;
    {
        std::scoped_lock lock{g_checkpoint_phase_test_hook_latch};
        hook = g_checkpoint_phase_test_hook;
    }
    if (hook) hook(phase);
}

int64_t RetryLogOffset(int64_t current_offset) {
    if (current_offset > INT64_MAX - kDrainRetryBackoffBytes) {
        return INT64_MAX;
    }
    return current_offset + kDrainRetryBackoffBytes;
}

class CleanAttemptMetric {
public:
    explicit CleanAttemptMetric(CheckpointPhaseMetrics* metrics) noexcept : metrics_(metrics) {
        if (metrics_ != nullptr) metrics_->clean_attempt();
    }
    ~CleanAttemptMetric() noexcept {
        if (metrics_ != nullptr && !complete_) metrics_->clean_failure();
    }
    void Succeed() noexcept {
        if (metrics_ != nullptr) metrics_->clean_success();
        complete_ = true;
    }

private:
    CheckpointPhaseMetrics* metrics_;
    bool complete_{false};
};

class FuzzyAttemptMetric {
public:
    explicit FuzzyAttemptMetric(CheckpointPhaseMetrics* metrics) noexcept : metrics_(metrics) {
        if (metrics_ != nullptr) metrics_->fuzzy_attempt();
        if (metrics_ != nullptr && metrics_->enabled()) started_ = std::chrono::steady_clock::now();
    }
    ~FuzzyAttemptMetric() noexcept {
        if (transferred_) return;
        if (metrics_ != nullptr) metrics_->fuzzy_failure();
        RecordLifetime(metrics_, started_);
    }
    std::chrono::steady_clock::time_point started() const noexcept { return started_; }
    void Transfer() noexcept { transferred_ = true; }
    static void RecordLifetime(CheckpointPhaseMetrics* metrics,
                               std::chrono::steady_clock::time_point started) noexcept {
        if (metrics == nullptr || !metrics->enabled() || started == std::chrono::steady_clock::time_point{}) return;
        metrics->record(CheckpointPhaseMetrics::Timing::FuzzyLifetime,
                        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - started).count()));
    }

private:
    CheckpointPhaseMetrics* metrics_;
    std::chrono::steady_clock::time_point started_{};
    bool transferred_{false};
};

} // namespace

CheckpointOptions CheckpointOptions::FromEnvironment() {
    CheckpointOptions options;
    options.auto_checkpoint_bytes = static_cast<int64_t>(ReadCheckpointEnv(
        "RMDB_AUTO_CHECKPOINT_BYTES", options.auto_checkpoint_bytes, kMinAutoCheckpointBytes, kMaxAutoCheckpointBytes));
    options.tick_bytes = static_cast<size_t>(
        ReadCheckpointEnv("RMDB_CHECKPOINT_TICK_BYTES", options.tick_bytes, kMinTickBytes, kMaxTickBytes));
    options.tick_time_us =
        ReadCheckpointEnv("RMDB_CHECKPOINT_TICK_TIME_US", options.tick_time_us, kMinTickTimeUs, kMaxTickTimeUs);
    options.io_quantum_pages = static_cast<size_t>(ReadCheckpointEnv("RMDB_CHECKPOINT_IO_QUANTUM_PAGES",
                                                                       options.io_quantum_pages, 1,
                                                                       kMaxCheckpointQuantumPages));
    options.background_preclean_enabled = ReadCheckpointBoolEnv("RMDB_BACKGROUND_PRECLEAN", options.background_preclean_enabled);
    options.background_preclean_low_percent = static_cast<uint8_t>(ReadCheckpointEnv("RMDB_BACKGROUND_PRECLEAN_LOW_PERCENT", options.background_preclean_low_percent, 1, 98));
    options.background_preclean_high_percent = static_cast<uint8_t>(ReadCheckpointEnv("RMDB_BACKGROUND_PRECLEAN_HIGH_PERCENT", options.background_preclean_high_percent, 2, 99));
    if (options.background_preclean_low_percent >= options.background_preclean_high_percent) throw std::invalid_argument("RMDB_BACKGROUND_PRECLEAN_LOW_PERCENT must be below high percent");
    options.background_preclean_batch_pages = static_cast<size_t>(ReadCheckpointEnv("RMDB_BACKGROUND_PRECLEAN_BATCH_PAGES", options.background_preclean_batch_pages, kMinPrecleanBatchPages, kMaxPrecleanBatchPages));
    options.background_preclean_max_pages = static_cast<size_t>(
        ReadCheckpointEnv("RMDB_BACKGROUND_PRECLEAN_MAX_PAGES", options.background_preclean_max_pages,
                          kMinPrecleanBatchPages, kMaxPrecleanBatchPages));
    return options;
}

struct CheckpointManager::FuzzyCheckpointState {
    FuzzyCheckpointState(std::shared_lock<std::shared_mutex> coordination_guard,
                         SmManager::CatalogSharedGuard catalog_guard, FixedCheckpointHeaderSnapshot headers,
                         BufferPoolManager::CheckpointCohort cohort, CheckpointWalCut wal_cut, RestartManifest manifest,
                         std::chrono::steady_clock::time_point metrics_started)
        : coordination_guard(std::move(coordination_guard)), catalog_guard(std::move(catalog_guard)),
          headers(std::move(headers)), cohort(cohort), wal_cut(std::move(wal_cut)), manifest(manifest),
          metrics_started(metrics_started) {}

    // Destruction order is the reverse of declaration order: all captured
    // catalog-owned state is destroyed before the process coordination guard
    // is released to a waiting explicit clean checkpoint.
    std::shared_lock<std::shared_mutex> coordination_guard;
    SmManager::CatalogSharedGuard catalog_guard;
    FixedCheckpointHeaderSnapshot headers;
    BufferPoolManager::CheckpointCohort cohort;
    CheckpointWalCut wal_cut;
    RestartManifest manifest;
    uint64_t event_id{};
    std::chrono::steady_clock::time_point event_started{};
    std::chrono::steady_clock::time_point cohort_flush_started{};
    size_t cohort_pages_written{};
    bool cohort_flush_logged{false};
    bool cohort_flush_complete{false};
    bool cohort_debt_yielded{false};
    bool wal_cut_synced{false};
    bool headers_written{false};
    size_t next_file_sync{};
    std::chrono::steady_clock::time_point file_sync_started{};
    uint64_t file_sync_total_ms{};
    uint64_t file_sync_max_ms{};
    const char* phase{"start"};
    std::chrono::steady_clock::time_point metrics_started{};
};

CheckpointManager::CheckpointManager(TransactionManager* txn_mgr, SmManager* sm_mgr, LogManager* log_mgr,
                                     CheckpointPhaseMetrics* metrics)
    : txn_mgr_(txn_mgr), sm_mgr_(sm_mgr), log_mgr_(log_mgr), metrics_(metrics) {
    if (log_mgr_ != nullptr) {
        g_published_restart_offset.store(log_mgr_->read_restart_manifest().checkpoint_offset,
                                         std::memory_order_release);
    }
}

CheckpointManager::~CheckpointManager() {
    CancelFuzzyCheckpoint();
}

void CheckpointManager::set_phase_test_hook(std::function<void(std::string_view)> hook) {
    std::scoped_lock lock{g_checkpoint_phase_test_hook_latch};
    g_checkpoint_phase_test_hook_enabled.store(false, std::memory_order_release);
    g_checkpoint_phase_test_hook = std::move(hook);
    g_checkpoint_phase_test_hook_enabled.store(static_cast<bool>(g_checkpoint_phase_test_hook),
                                               std::memory_order_release);
}

void CheckpointManager::SetOptions(CheckpointOptions options) {
    // Programmatic callers share the same pacing safety envelope as the
    // environment path. Keep auto_checkpoint_bytes untouched: focused tests
    // intentionally use byte-sized thresholds to exercise relative WAL cuts.
    options.tick_bytes = static_cast<size_t>(std::clamp<uint64_t>(static_cast<uint64_t>(options.tick_bytes),
                                                                  kMinTickBytes, kMaxTickBytes));
    options.tick_time_us = std::clamp(options.tick_time_us, kMinTickTimeUs, kMaxTickTimeUs);
    options.io_quantum_pages = static_cast<size_t>(std::clamp<uint64_t>(
        static_cast<uint64_t>(options.io_quantum_pages), 1, kMaxCheckpointQuantumPages));
    options.background_preclean_low_percent = std::clamp<uint8_t>(options.background_preclean_low_percent, 1, 98);
    options.background_preclean_high_percent = std::clamp<uint8_t>(options.background_preclean_high_percent, 2, 99);
    if (options.background_preclean_low_percent >= options.background_preclean_high_percent) {
        options.background_preclean_low_percent = 10;
        options.background_preclean_high_percent = 15;
    }
    options.background_preclean_batch_pages = static_cast<size_t>(std::clamp<uint64_t>(static_cast<uint64_t>(options.background_preclean_batch_pages), kMinPrecleanBatchPages, kMaxPrecleanBatchPages));
    options.background_preclean_max_pages =
        static_cast<size_t>(std::clamp<uint64_t>(static_cast<uint64_t>(options.background_preclean_max_pages),
                                                 options.background_preclean_batch_pages, kMaxPrecleanBatchPages));
    options_ = options;
}

void CheckpointManager::ObserveBackgroundIoWal() {
    background_io_observed_this_tick_ = false;
    const LogManager::FdatasyncObservationWindow wal = log_mgr_->consume_fdatasync_observations();
    if (wal.count == 0) {
        return;
    }
    background_io_observed_this_tick_ = true;
    background_preclean_wal_sequence_ = wal.sequence;
    background_preclean_wal_window_max_ns_ = wal.max_elapsed_ns;
    background_preclean_wal_ewma_ns_ = background_preclean_wal_ewma_ns_ == 0 ? wal.max_elapsed_ns : background_preclean_wal_ewma_ns_ - background_preclean_wal_ewma_ns_ / 4 + wal.max_elapsed_ns / 4;
    if (wal.max_elapsed_ns > kHealthyWalFdatasyncNs) {
        background_preclean_congestion_latched_ = true;
        background_preclean_healthy_samples_ = 0;
        background_preclean_ramp_level_ = 0;
        return;
    }
    background_preclean_healthy_samples_ =
        saturate_healthy_observations_for_test(background_preclean_healthy_samples_, wal.count);
    if (background_preclean_healthy_samples_ == kHealthyWalObservationsToResume) {
        background_preclean_wal_ewma_ns_ = wal.last_elapsed_ns;
        background_preclean_congestion_latched_ = false;
        background_preclean_ramp_level_ = saturate_ramp_level_for_test(background_preclean_ramp_level_);
    }
}

uint8_t CheckpointManager::saturate_healthy_observations_for_test(uint8_t current, uint64_t count) noexcept {
    const uint8_t bounded = std::min<uint8_t>(current, kHealthyWalObservationsToResume);
    const uint8_t remaining = kHealthyWalObservationsToResume - bounded;
    if (count >= remaining) return kHealthyWalObservationsToResume;
    return static_cast<uint8_t>(bounded + static_cast<uint8_t>(count));
}

uint8_t CheckpointManager::saturate_ramp_level_for_test(uint8_t current) noexcept {
    if (current == std::numeric_limits<uint8_t>::max()) return current;
    return static_cast<uint8_t>(current + 1);
}

bool CheckpointManager::BackgroundIoPaused() const noexcept {
    return background_preclean_congestion_latched_ || background_preclean_wal_window_max_ns_ > kHealthyWalFdatasyncNs || background_preclean_wal_ewma_ns_ > kHealthyWalFdatasyncNs;
}

size_t CheckpointManager::BackgroundPrecleanBudget() const noexcept {
    if (BackgroundIoPaused()) return 0;
    size_t pages = compute_background_preclean_budget_for_test(background_preclean_dirty_pages_, background_preclean_capacity_, options_.background_preclean_low_percent, options_.background_preclean_batch_pages, options_.background_preclean_max_pages, 0);
    if (background_preclean_ramp_level_ != 0) pages = std::min(options_.background_preclean_max_pages, std::max(pages, options_.background_preclean_batch_pages * background_preclean_ramp_level_));
    return pages;
}

bool CheckpointManager::cohort_service_debt_for_test(size_t total_pages, size_t remaining_pages,
                                                      int64_t growth_since_cut, int64_t target_wal_bytes,
                                                      bool congested_observation) noexcept {
    if (remaining_pages == 0) return false;
    if (!congested_observation || remaining_pages >= total_pages) return true;
    const size_t completed_pages = total_pages - remaining_pages;
    const uint64_t bounded_growth = static_cast<uint64_t>(std::max<int64_t>(0, growth_since_cut));
    const uint64_t target = static_cast<uint64_t>(std::max<int64_t>(1, target_wal_bytes));
    const uint64_t bounded_target_growth = std::min(bounded_growth, target);
    // ceil(total_pages * bounded_growth / target), formed without a product
    // overflow. This is service debt, not a calibrated recovery-time model.
    const uint64_t remainder = bounded_target_growth % target;
    const unsigned __int128 numerator = static_cast<unsigned __int128>(total_pages) * remainder;
    const uint64_t growth_due = bounded_target_growth == target
                                    ? static_cast<uint64_t>(total_pages)
                                    : static_cast<uint64_t>((numerator + target - 1) / target);
    return completed_pages <= growth_due;
}

void CheckpointManager::MaybeRunBackgroundPreclean() {
    if (!options_.background_preclean_enabled || fuzzy_checkpoint_ != nullptr) return;
    // Keep the whole discovery/claim/write/cleanup interval inside the same
    // coordination domain as a fuzzy checkpoint.  In particular, an explicit
    // clean checkpoint cannot publish its manifest or reset WAL while a
    // preclean frame is still FLUSHING.  Lock order is coordination -> catalog
    // -> BPM, matching every other checkpoint path.
    std::shared_lock<std::shared_mutex> checkpoint_guard{g_checkpoint_coordination};
    if (fuzzy_checkpoint_ != nullptr) return;
    const auto now = std::chrono::steady_clock::now();
    if (background_preclean_next_sample_ == std::chrono::steady_clock::time_point{} || now >= background_preclean_next_sample_) {
        const TableDirtyPageStats dirty = sm_mgr_->table_and_index_dirty_page_stats();
        if (dirty.frame_capacity == 0) return;
        background_preclean_dirty_pages_ = dirty.dirty_pages;
        background_preclean_capacity_ = dirty.frame_capacity;
        const size_t high = PercentPages(dirty.frame_capacity, options_.background_preclean_high_percent, true);
        const size_t low = PercentPages(dirty.frame_capacity, options_.background_preclean_low_percent, false);
        const bool was_active = background_preclean_active_;
        if (!background_preclean_active_) background_preclean_active_ = dirty.dirty_pages >= high;
        else if (dirty.dirty_pages <= low) background_preclean_active_ = false;
        background_preclean_next_sample_ = now + std::chrono::seconds(1);
        if (was_active != background_preclean_active_) LOG_WARN("background-preclean state=%s dirty_pages=%zu capacity=%zu low=%zu high=%zu", background_preclean_active_ ? "active" : "idle", dirty.dirty_pages, dirty.frame_capacity, low, high);
    }
    if (!background_preclean_active_) return;

    ObserveBackgroundIoWal();
    const size_t pages = BackgroundPrecleanBudget();
    background_preclean_last_budget_ = pages;
    BufferPoolManager* const bpm = sm_mgr_->get_bpm();
    if (bpm != nullptr && pages == 0) bpm->background_preclean_metrics().congestion_pause();
    if (bpm != nullptr && pages != 0 && background_preclean_ramp_level_ != 0) bpm->background_preclean_metrics().congestion_ramp();
    if (pages == 0) return;
    const bool metrics_enabled = bpm != nullptr && bpm->background_preclean_metrics().enabled();
    const auto started = metrics_enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    const size_t flushed = sm_mgr_->flush_dirty_table_and_index_pages(pages);
    if (metrics_enabled)
        bpm->background_preclean_metrics().background_flush(
            flushed, static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                               std::chrono::steady_clock::now() - started)
                                               .count()));
}

size_t CheckpointManager::compute_background_preclean_budget_for_test(size_t dirty_pages, size_t capacity,
                                                                      uint8_t low_percent, size_t base_pages,
                                                                      size_t max_pages, uint64_t wal_ewma_ns) noexcept {
    const size_t low = PercentPages(capacity, low_percent, false);
    const size_t debt = dirty_pages > low ? (dirty_pages - low + kPrecleanHorizonTicks - 1) / kPrecleanHorizonTicks : 0;
    if (wal_ewma_ns > kHealthyWalFdatasyncNs) return 0;
    return std::min(max_pages, std::max(base_pages, debt));
}

bool CheckpointManager::RunCleanCheckpoint() {
    const uint64_t event_id = NextCheckpointEventId();
    const auto event_started = std::chrono::steady_clock::now();
    if (txn_mgr_ == nullptr || sm_mgr_ == nullptr || log_mgr_ == nullptr) {
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=failure reason=precondition elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }

    const int64_t start_offset = log_mgr_->current_log_offset();
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=start log_offset=%lld",
             static_cast<unsigned long long>(event_id), static_cast<long long>(start_offset));

    CleanAttemptMetric attempt(metrics_);
    // Lock order for every checkpoint path is coordination -> catalog ->
    // transaction admission. execute_tree_impl deliberately does not acquire
    // the catalog guard ahead of STATIC_CHECKPOINT.
    // Take the exclusive side before doing any work. If the background owner
    // is pacing a fixed cohort, an explicit STATIC_CHECKPOINT waits for it and
    // then retains the historical clean/manifest-zero/WAL-truncate behavior.
    std::unique_lock<std::shared_mutex> checkpoint_guard{g_checkpoint_coordination};
    auto catalog_guard = sm_mgr_->acquire_catalog_shared();
    const auto deadline = std::chrono::steady_clock::now() + kCheckpointDeadline;
    const auto defer_automatic_retry = [&] {
        drain_retry_time_ = std::chrono::steady_clock::now() + kDrainRetryBackoff;
        drain_retry_log_offset_ = RetryLogOffset(log_mgr_->current_log_offset());
    };
    const auto deadline_expired = [&] { return std::chrono::steady_clock::now() >= deadline; };

    struct BlockGuard {
        TransactionManager* txn_mgr;

        explicit BlockGuard(TransactionManager* mgr) : txn_mgr(mgr) {
            txn_mgr->block_new_transactions_for_checkpoint();
        }

        ~BlockGuard() {
            txn_mgr->unblock_new_transactions_after_checkpoint();
        }
    } block_guard(txn_mgr_);
    bool drained = false;
    const auto drain_started = std::chrono::steady_clock::now();
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=drain-begin", static_cast<unsigned long long>(event_id));
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::DrainAdmissionWait);
        drained = txn_mgr_->wait_active_transactions_drained_for_checkpoint(kDrainTimeout);
    }
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=drain-finished success=%d elapsed_ms=%llu",
             static_cast<unsigned long long>(event_id), drained,
             static_cast<unsigned long long>(ElapsedMs(drain_started)));
    if (!drained) {
        // Abandon this round. ~BlockGuard releases the block immediately, so a
        // stuck transaction can never freeze every other transaction. The WAL
        // keeps growing, which only makes the next recovery slower.
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(false);
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=failure reason=drain elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }
    if (deadline_expired()) {
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(true);
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=failure reason=deadline-before-wal elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }
    const auto wal_started = std::chrono::steady_clock::now();
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=wal-sync-begin", static_cast<unsigned long long>(event_id));
    log_mgr_->flush_log_to_disk_with_sync();
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=wal-sync-finished elapsed_ms=%llu",
             static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(wal_started)));
    if (deadline_expired()) {
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(true);
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=failure reason=deadline-after-wal elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }
    FaultInjector::Point("before_checkpoint_data_sync");
    bool final_data_stable = false;
    const auto flush_all_started = std::chrono::steady_clock::now();
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=flush-all-begin", static_cast<unsigned long long>(event_id));
    try {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::CleanDataSync);
        final_data_stable = sm_mgr_->flush_all_table_and_index_pages(FlushDependencyPolicy::AlreadyDurable());
    } catch (...) {
        // A failed table/index fdatasync must leave the complete WAL and old
        // restart manifest intact so this checkpoint can be retried.
    }
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=flush-all-end success=%d elapsed_ms=%llu",
             static_cast<unsigned long long>(event_id), final_data_stable,
             static_cast<unsigned long long>(ElapsedMs(flush_all_started)));
    RunCheckpointPhaseTestHook("clean_data_sync_end");
    if (!final_data_stable) {
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(false);
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=failure reason=flush-all elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }
    FaultInjector::Point("after_checkpoint_data_sync");
    {
        const auto meta_started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=flush-meta-begin",
                 static_cast<unsigned long long>(event_id));
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::CleanMetaFlush);
        sm_mgr_->flush_meta();
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=flush-meta-end elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(meta_started)));
    }
    // Once the non-interruptible final data sync has succeeded, finish the
    // manifest/truncate sequence even if that sync crossed the admission
    // deadline. Abandoning here would retain the full WAL after already paying
    // the complete writer pause, causing the same pause to repeat and making
    // crash recovery unnecessarily scan the old WAL.
    // Publish the restart manifest before truncating WAL. If anything after
    // this point fails, the complete WAL is still available for recovery.
    //
    // 计数器快照必须在这里取，而这里恰好是唯一能取到可靠快照的地方：所有脏页刚刚
    // 落盘、且没有活跃事务，所以**每一个已持久化的 commit_ts_ 都小于此刻的
    // next_timestamp_**。截断 WAL 会连带丢掉那些页的 COMMIT 记录，因此这份快照就是
    // 恢复期唯一能覆盖“早于本次 checkpoint”的那部分行的信息来源。
    // 读的顺序（先取快照，再截断）由本函数的语句顺序保证。
    RestartManifest manifest;
    manifest.checkpoint_offset = 0;
    manifest.next_timestamp = txn_mgr_->peek_next_timestamp();
    manifest.next_txn_id = txn_mgr_->peek_next_txn_id();
    {
        const auto manifest_started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=manifest-begin", static_cast<unsigned long long>(event_id));
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::ManifestPublish);
        log_mgr_->write_restart_manifest(manifest);
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=manifest-end elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id),
                 static_cast<unsigned long long>(ElapsedMs(manifest_started)));
    }
    FaultInjector::Point("before_wal_truncate");
    {
        const auto reset_started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=reset-wal-begin",
                 static_cast<unsigned long long>(event_id));
        RunCheckpointPhaseTestHook("reset_wal_begin");
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::WalResetReclaim);
        log_mgr_->reset_log(log_mgr_->get_global_lsn());
        LOG_WARN("checkpoint-event kind=clean id=%llu phase=reset-wal-end elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(reset_started)));
    }
    g_published_restart_offset.store(0, std::memory_order_release);
    drain_retry_time_ = {};
    drain_retry_log_offset_ = 0;
    attempt.Succeed();
    LOG_WARN("checkpoint-event kind=clean id=%llu phase=finish elapsed_ms=%llu",
             static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
    return true;
}

bool CheckpointManager::RunIfNeeded() {
    return Tick();
}

void CheckpointManager::DeferAutomaticRetry() noexcept {
    if (metrics_ != nullptr) metrics_->retry_deferral(false);
    drain_retry_time_ = std::chrono::steady_clock::now() + kDrainRetryBackoff;
    try {
        drain_retry_log_offset_ = RetryLogOffset(log_mgr_ == nullptr ? 0 : log_mgr_->current_log_offset());
    } catch (...) {
        drain_retry_log_offset_ = INT64_MAX;
    }
}

int64_t CheckpointManager::RetainedWalBytes(int64_t current_offset) noexcept {
    int64_t published_offset = g_published_restart_offset.load(std::memory_order_acquire);
    if (current_offset < published_offset) {
        published_offset = 0;
        g_published_restart_offset.store(0, std::memory_order_release);
    }
    return current_offset - published_offset;
}

void CheckpointManager::CancelFuzzyCheckpoint(bool record_cancel) noexcept {
    if (fuzzy_checkpoint_ == nullptr) {
        return;
    }

    if (metrics_ != nullptr && record_cancel) metrics_->fuzzy_cancel();
    auto checkpoint = std::move(fuzzy_checkpoint_);
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=cancel prior_phase=%s elapsed_ms=%llu",
             static_cast<unsigned long long>(checkpoint->event_id), checkpoint->phase,
             static_cast<unsigned long long>(ElapsedMs(checkpoint->event_started)));
    FuzzyAttemptMetric::RecordLifetime(metrics_, checkpoint->metrics_started);
    try {
        if (sm_mgr_ != nullptr && sm_mgr_->get_bpm() != nullptr && checkpoint->cohort.epoch != 0) {
            sm_mgr_->get_bpm()->cancel_checkpoint_cohort(checkpoint->cohort.epoch);
        }
    } catch (...) {
        // Destruction and background error handling must still release catalog,
        // checkpoint coordination, and transaction admission ownership.
    }
    checkpoint.reset();
    g_fuzzy_checkpoint_running.store(false, std::memory_order_release);
}

bool CheckpointManager::StartFuzzyCheckpoint() {
    std::shared_lock<std::shared_mutex> checkpoint_guard{g_checkpoint_coordination};

    // Tick performs a cheap optimistic threshold check before contending on
    // coordination. Recheck after acquiring it because a waiting explicit
    // checkpoint may have reset the WAL in between.
    const int64_t current_offset = log_mgr_->current_log_offset();
    const int64_t target = std::max<int64_t>(1, options_.auto_checkpoint_bytes);
    if (RetainedWalBytes(current_offset) < target || std::chrono::steady_clock::now() < drain_retry_time_ ||
        current_offset < drain_retry_log_offset_) {
        return false;
    }

    bool expected = false;
    if (!g_fuzzy_checkpoint_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false;
    }
    FuzzyAttemptMetric attempt(metrics_);
    const uint64_t event_id = NextCheckpointEventId();
    const auto event_started = std::chrono::steady_clock::now();
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=start log_offset=%lld retained_bytes=%lld target_bytes=%lld",
             static_cast<unsigned long long>(event_id), static_cast<long long>(current_offset),
             static_cast<long long>(RetainedWalBytes(current_offset)), static_cast<long long>(target));
    struct ReservationGuard {
        bool armed{true};

        ~ReservationGuard() {
            if (armed) {
                g_fuzzy_checkpoint_running.store(false, std::memory_order_release);
            }
        }
    } reservation;

    auto catalog_guard = sm_mgr_->acquire_catalog_shared();
    struct BlockGuard {
        TransactionManager* txn_mgr;

        explicit BlockGuard(TransactionManager* mgr) : txn_mgr(mgr) {
            txn_mgr->block_new_transactions_for_checkpoint();
        }

        ~BlockGuard() {
            txn_mgr->unblock_new_transactions_after_checkpoint();
        }
    } block_guard(txn_mgr_);

    bool drained = false;
    const auto drain_started = std::chrono::steady_clock::now();
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=drain-begin", static_cast<unsigned long long>(event_id));
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::DrainAdmissionWait);
        drained = txn_mgr_->wait_active_transactions_drained_for_checkpoint(kDrainTimeout);
    }
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=drain-finished success=%d elapsed_ms=%llu",
             static_cast<unsigned long long>(event_id), drained,
             static_cast<unsigned long long>(ElapsedMs(drain_started)));
    if (!drained) {
        DeferAutomaticRetry();
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=backoff reason=drain elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }

    auto cut_timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::CutCapture);
    FixedCheckpointHeaderSnapshot headers = sm_mgr_->capture_fixed_checkpoint_headers(catalog_guard);
    BufferPoolManager* bpm = sm_mgr_->get_bpm();
    if (bpm == nullptr) {
        throw InternalError("fuzzy checkpoint requires a buffer pool manager");
    }

    // begin_checkpoint_cohort is the cut linearization point for dirty page
    // membership. Its implementation must include any allowed-fd writes that
    // were already FLUSHING/EVICTING when transaction admission drained.
    const auto cohort = bpm->begin_checkpoint_cohort(headers.data_and_index_fds, kDrainTimeout);
    if (!cohort.success) {
        DeferAutomaticRetry();
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=backoff reason=cohort-begin elapsed_ms=%llu",
                 static_cast<unsigned long long>(event_id), static_cast<unsigned long long>(ElapsedMs(event_started)));
        return false;
    }
    try {
        RestartManifest manifest;
        manifest.next_timestamp = txn_mgr_->peek_next_timestamp();
        manifest.next_txn_id = txn_mgr_->peek_next_txn_id();
        CheckpointWalCut wal_cut = log_mgr_->create_checkpoint_wal_cut(headers.index_file_names);
        RunCheckpointPhaseTestHook("fuzzy_cut_appended");
        cut_timer.Finish();
        manifest.checkpoint_offset = wal_cut.checkpoint_offset;
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=wal-cut-append-end checkpoint_offset=%lld last_lsn=%d",
                 static_cast<unsigned long long>(event_id), static_cast<long long>(wal_cut.checkpoint_offset),
                 static_cast<int>(wal_cut.last_lsn));

        if (metrics_ != nullptr) metrics_->pages_marked(cohort.pages_marked);
        fuzzy_checkpoint_ =
            std::make_unique<FuzzyCheckpointState>(std::move(checkpoint_guard), std::move(catalog_guard),
                                                   std::move(headers), cohort, std::move(wal_cut), manifest,
                                                   attempt.started());
        fuzzy_checkpoint_->event_id = event_id;
        fuzzy_checkpoint_->event_started = event_started;
        fuzzy_checkpoint_->phase = "wal-cut-sync-pending";
        attempt.Transfer();
    } catch (...) {
        bpm->cancel_checkpoint_cohort(cohort.epoch);
        throw;
    }

    // BlockGuard reopens admission as this function returns. The cut is only
    // appended here; its durable prefix sync intentionally happens in a later
    // tick, and no cohort page is flushed before that succeeds.
    reservation.armed = false;
    return true;
}

bool CheckpointManager::AdvanceFuzzyCheckpoint() {
    BufferPoolManager* bpm = sm_mgr_->get_bpm();
    if (bpm == nullptr || fuzzy_checkpoint_ == nullptr) {
        throw InternalError("invalid fuzzy checkpoint state");
    }

    if (!fuzzy_checkpoint_->wal_cut_synced) {
        fuzzy_checkpoint_->phase = "wal-cut-sync";
        const auto started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=wal-cut-sync-begin last_lsn=%d",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
                 static_cast<int>(fuzzy_checkpoint_->wal_cut.last_lsn));
        log_mgr_->sync_checkpoint_wal_cut(fuzzy_checkpoint_->wal_cut);
        fuzzy_checkpoint_->wal_cut_synced = true;
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=wal-cut-sync-end elapsed_ms=%llu",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
                 static_cast<unsigned long long>(ElapsedMs(started)));
        return false;
    }

    if (!fuzzy_checkpoint_->cohort_flush_complete) {
        FaultInjector::Point("before_fuzzy_checkpoint_data_sync");
        fuzzy_checkpoint_->phase = "cohort-flush";
        if (!fuzzy_checkpoint_->cohort_flush_logged) {
            fuzzy_checkpoint_->cohort_flush_started = std::chrono::steady_clock::now();
            fuzzy_checkpoint_->cohort_flush_logged = true;
            LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=cohort-flush-begin epoch=%llu",
                     static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
                     static_cast<unsigned long long>(fuzzy_checkpoint_->cohort.epoch));
        }
        ObserveBackgroundIoWal();
        // A zero-I/O probe lets an empty or already-discharged cohort complete.
        const auto completion_probe =
            bpm->flush_checkpoint_cohort(fuzzy_checkpoint_->cohort.epoch, 0, 1);
        if (!completion_probe.success) throw UnixError();
        if (completion_probe.pages_remaining == 0) {
            fuzzy_checkpoint_->cohort_flush_complete = true;
        }
        if (!fuzzy_checkpoint_->cohort_flush_complete) {
        const size_t quantum_pages = options_.io_quantum_pages;
        const size_t byte_budget_pages = std::max<size_t>(size_t{1}, options_.tick_bytes / PAGE_SIZE);
        const int64_t current_offset = log_mgr_->current_log_offset();
        const int64_t target_wal_bytes = std::max<int64_t>(1, options_.auto_checkpoint_bytes);
        const int64_t wal_growth_since_cut =
            std::max<int64_t>(0, current_offset - fuzzy_checkpoint_->wal_cut.tail_offset);
        const bool congested_observation = background_io_observed_this_tick_ && BackgroundIoPaused();
        bool service_debt = cohort_service_debt_for_test(
            fuzzy_checkpoint_->cohort.pages_marked, completion_probe.pages_remaining, wal_growth_since_cut,
            target_wal_bytes, congested_observation);
        // Congestion may pause optional preclean, but not a cohort with
        // outstanding service debt. Its quantum remains the per-tick latency
        // bound rather than a controller tuning knob.
        if (!service_debt) {
            // An ahead congested cohort may defer once, but that defer itself
            // becomes debt. Without a calibrated arrival/service model this
            // alternating bound prevents a stale zero-growth window from
            // extending the active checkpoint indefinitely.
            if (!fuzzy_checkpoint_->cohort_debt_yielded) {
                fuzzy_checkpoint_->cohort_debt_yielded = true;
                return false;
            }
            service_debt = true;
        }
        const size_t io_budget_pages = byte_budget_pages;
        // SetOptions clamps the unsigned input before this signed duration
        // conversion, so neither conversion nor now()+duration can overflow.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(options_.tick_time_us);
        size_t pages_written = 0;
        size_t pages_remaining = 0;
        bool first_quantum = true;
        for (;;) {
            const size_t remaining_budget = io_budget_pages > pages_written ? io_budget_pages - pages_written : 0;
            if (!first_quantum && (remaining_budget == 0 || std::chrono::steady_clock::now() >= deadline)) {
                if (metrics_ != nullptr) {
                    if (remaining_budget == 0) {
                        metrics_->budget_yield_io();
                    } else {
                        metrics_->budget_yield_time();
                    }
                }
                return false;
            }
            const size_t quantum = std::min(quantum_pages, std::max<size_t>(size_t{1}, remaining_budget));
            const size_t visit_budget = std::max<size_t>(64, quantum * 4);
            BufferPoolManager::CheckpointCohortFlushResult result;
            {
                auto page_timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::PageWrite);
                result = bpm->flush_checkpoint_cohort(fuzzy_checkpoint_->cohort.epoch, quantum, visit_budget);
            }
            if (metrics_ != nullptr) metrics_->page_write(result.pages_written, result.pages_remaining);
            if (!result.success) throw UnixError();
            pages_written += result.pages_written;
            fuzzy_checkpoint_->cohort_pages_written += result.pages_written;
            if (result.pages_written != 0) fuzzy_checkpoint_->cohort_debt_yielded = false;
            pages_remaining = result.pages_remaining;
            if (pages_remaining == 0) break;
            // No successful write and no remaining obligation change would spin on
            // a transient FLUSHING frame; yield to the next scheduler tick.
            if (result.pages_written == 0) {
                if (metrics_ != nullptr) metrics_->zero_progress_yield();
                return false;
            }
            first_quantum = false;
        }
        fuzzy_checkpoint_->cohort_flush_complete = true;
        }

        // The detached images and descriptor set were captured at the same cut.
        // Pace final publication just like page writeback: headers once, one data
        // fd per tick, then db.meta and finally the restart manifest.
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=cohort-flush-end pages=%zu remaining=0 elapsed_ms=%llu "
                 "lifetime_ms=%llu",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id), fuzzy_checkpoint_->cohort_pages_written,
                 static_cast<unsigned long long>(ElapsedMs(fuzzy_checkpoint_->cohort_flush_started)),
                 static_cast<unsigned long long>(ElapsedMs(fuzzy_checkpoint_->event_started)));
    }
    if (!fuzzy_checkpoint_->headers_written) {
        fuzzy_checkpoint_->phase = "header-write";
        const auto started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=header-write-begin",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id));
        sm_mgr_->write_fixed_checkpoint_headers_only(fuzzy_checkpoint_->headers, fuzzy_checkpoint_->catalog_guard);
        fuzzy_checkpoint_->headers_written = true;
        fuzzy_checkpoint_->file_sync_started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=header-write-end elapsed_ms=%llu",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
                 static_cast<unsigned long long>(ElapsedMs(started)));
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=file-sync-begin files=%zu",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
                 fuzzy_checkpoint_->headers.data_and_index_fds.size());
        return false;
    }
    if (fuzzy_checkpoint_->next_file_sync < fuzzy_checkpoint_->headers.data_and_index_fds.size()) {
        fuzzy_checkpoint_->phase = "file-sync";
        const int fd = fuzzy_checkpoint_->headers.data_and_index_fds[fuzzy_checkpoint_->next_file_sync];
        const auto started = std::chrono::steady_clock::now();
        sm_mgr_->sync_fixed_checkpoint_file(fd, fuzzy_checkpoint_->catalog_guard);
        const uint64_t elapsed = ElapsedMs(started);
        fuzzy_checkpoint_->file_sync_total_ms += elapsed;
        fuzzy_checkpoint_->file_sync_max_ms = std::max(fuzzy_checkpoint_->file_sync_max_ms, elapsed);
        ++fuzzy_checkpoint_->next_file_sync;
        if (elapsed > 20) LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=file-sync-slow fd=%d elapsed_ms=%llu",
                                   static_cast<unsigned long long>(fuzzy_checkpoint_->event_id), fd,
                                   static_cast<unsigned long long>(elapsed));
        return false;
    }
    FaultInjector::Point("after_fuzzy_checkpoint_data_sync");
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=file-sync-end files=%zu total_ms=%llu max_ms=%llu "
             "elapsed_ms=%llu",
             static_cast<unsigned long long>(fuzzy_checkpoint_->event_id), fuzzy_checkpoint_->next_file_sync,
             static_cast<unsigned long long>(fuzzy_checkpoint_->file_sync_total_ms),
             static_cast<unsigned long long>(fuzzy_checkpoint_->file_sync_max_ms),
             static_cast<unsigned long long>(ElapsedMs(fuzzy_checkpoint_->file_sync_started)));
    fuzzy_checkpoint_->phase = "db-meta";
    const auto meta_started = std::chrono::steady_clock::now();
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=db-meta-begin",
             static_cast<unsigned long long>(fuzzy_checkpoint_->event_id));
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::FuzzyFinalPublish);
        sm_mgr_->publish_fixed_checkpoint_meta(fuzzy_checkpoint_->headers, fuzzy_checkpoint_->catalog_guard);
    }
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=db-meta-end elapsed_ms=%llu",
             static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
             static_cast<unsigned long long>(ElapsedMs(meta_started)));
    FaultInjector::Point("before_fuzzy_checkpoint_manifest_publish");
    {
        fuzzy_checkpoint_->phase = "manifest-publish";
        const auto manifest_started = std::chrono::steady_clock::now();
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=manifest-begin",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id));
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::ManifestPublish);
        log_mgr_->write_restart_manifest(fuzzy_checkpoint_->manifest);
        LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=manifest-end elapsed_ms=%llu",
                 static_cast<unsigned long long>(fuzzy_checkpoint_->event_id),
                 static_cast<unsigned long long>(ElapsedMs(manifest_started)));
    }

    g_published_restart_offset.store(fuzzy_checkpoint_->wal_cut.checkpoint_offset, std::memory_order_release);
    fuzzy_checkpoint_->cohort.epoch = 0;
    auto completed = std::move(fuzzy_checkpoint_);
    FuzzyAttemptMetric::RecordLifetime(metrics_, completed->metrics_started);
    LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=finish checkpoint_offset=%lld elapsed_ms=%llu",
             static_cast<unsigned long long>(completed->event_id),
             static_cast<long long>(completed->wal_cut.checkpoint_offset),
             static_cast<unsigned long long>(ElapsedMs(completed->event_started)));
    completed.reset();
    g_fuzzy_checkpoint_running.store(false, std::memory_order_release);
    drain_retry_time_ = {};
    drain_retry_log_offset_ = 0;
    if (metrics_ != nullptr) metrics_->fuzzy_success();
    return true;
}

bool CheckpointManager::Tick() {
    if (log_mgr_ == nullptr || sm_mgr_ == nullptr || txn_mgr_ == nullptr) {
        return false;
    }

    try {
        if (fuzzy_checkpoint_ != nullptr) {
            return AdvanceFuzzyCheckpoint();
        }
        MaybeRunBackgroundPreclean();

        const int64_t current_offset = log_mgr_->current_log_offset();
        const int64_t target = std::max<int64_t>(1, options_.auto_checkpoint_bytes);
        if (RetainedWalBytes(current_offset) < target) {
            return false;
        }

        const bool retry_suppressed =
            std::chrono::steady_clock::now() < drain_retry_time_ || current_offset < drain_retry_log_offset_;
        if (retry_suppressed) {
            return false;
        }

        // Starting performs only the short transaction-free cut. Page and
        // header writeback is deliberately deferred to subsequent Ticks.
        StartFuzzyCheckpoint();
        return false;
    } catch (...) {
        if (fuzzy_checkpoint_ != nullptr) {
            LOG_WARN("checkpoint-event kind=fuzzy id=%llu phase=failure prior_phase=%s elapsed_ms=%llu",
                     static_cast<unsigned long long>(fuzzy_checkpoint_->event_id), fuzzy_checkpoint_->phase,
                     static_cast<unsigned long long>(ElapsedMs(fuzzy_checkpoint_->event_started)));
        }
        if (metrics_ != nullptr && fuzzy_checkpoint_ != nullptr) metrics_->fuzzy_failure();
        CancelFuzzyCheckpoint(false);
        DeferAutomaticRetry();
        return false;
    }
}
