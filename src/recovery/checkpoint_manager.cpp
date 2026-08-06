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
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace {

// The coordination lock serializes explicit clean checkpoints against the
// complete lifetime of an automatic fuzzy checkpoint. This flag only elects
// one automatic owner because shared-lock holders do not exclude each other.
std::atomic<bool> g_fuzzy_checkpoint_running{false};
// STATIC_CHECKPOINT is created through a separate manager instance. Keeping
// the process's published boundary here lets the persistent background manager
// observe its manifest-zero/WAL-reset transition without polling db.restart on
// every scheduler tick.
std::atomic<int64_t> g_published_restart_offset{0};
// STATIC_CHECKPOINT and the background scheduler use distinct
// CheckpointManager instances. Coordinate their preflush/final-flush phases at
// process scope so a clean checkpoint cannot skip an in-flight FLUSHING page.
std::shared_mutex g_checkpoint_coordination;

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

void CheckpointManager::SetOptions(CheckpointOptions options) {
    // Programmatic callers share the same pacing safety envelope as the
    // environment path. Keep auto_checkpoint_bytes untouched: focused tests
    // intentionally use byte-sized thresholds to exercise relative WAL cuts.
    options.tick_bytes = static_cast<size_t>(std::clamp<uint64_t>(static_cast<uint64_t>(options.tick_bytes),
                                                                  kMinTickBytes, kMaxTickBytes));
    options.tick_time_us = std::clamp(options.tick_time_us, kMinTickTimeUs, kMaxTickTimeUs);
    options.io_quantum_pages = static_cast<size_t>(std::clamp<uint64_t>(
        static_cast<uint64_t>(options.io_quantum_pages), 1, kMaxCheckpointQuantumPages));
    options_ = options;
}

bool CheckpointManager::RunCleanCheckpoint() {
    if (txn_mgr_ == nullptr || sm_mgr_ == nullptr || log_mgr_ == nullptr) {
        return false;
    }

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
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::DrainAdmissionWait);
        drained = txn_mgr_->wait_active_transactions_drained_for_checkpoint(kDrainTimeout);
    }
    if (!drained) {
        // Abandon this round. ~BlockGuard releases the block immediately, so a
        // stuck transaction can never freeze every other transaction. The WAL
        // keeps growing, which only makes the next recovery slower.
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(false);
        return false;
    }
    if (deadline_expired()) {
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(true);
        return false;
    }
    log_mgr_->flush_log_to_disk_with_sync();
    if (deadline_expired()) {
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(true);
        return false;
    }
    FaultInjector::Point("before_checkpoint_data_sync");
    bool final_data_stable = false;
    try {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::CleanDataSync);
        final_data_stable = sm_mgr_->flush_all_table_and_index_pages(FlushDependencyPolicy::AlreadyDurable());
    } catch (...) {
        // A failed table/index fdatasync must leave the complete WAL and old
        // restart manifest intact so this checkpoint can be retried.
    }
    if (!final_data_stable) {
        defer_automatic_retry();
        if (metrics_ != nullptr) metrics_->retry_deferral(false);
        return false;
    }
    FaultInjector::Point("after_checkpoint_data_sync");
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::CleanMetaFlush);
        sm_mgr_->flush_meta();
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
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::ManifestPublish);
        log_mgr_->write_restart_manifest(manifest);
    }
    FaultInjector::Point("before_wal_truncate");
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::WalResetReclaim);
        log_mgr_->reset_log(log_mgr_->get_global_lsn());
    }
    g_published_restart_offset.store(0, std::memory_order_release);
    drain_retry_time_ = {};
    drain_retry_log_offset_ = 0;
    attempt.Succeed();
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
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::DrainAdmissionWait);
        drained = txn_mgr_->wait_active_transactions_drained_for_checkpoint(kDrainTimeout);
    }
    if (!drained) {
        DeferAutomaticRetry();
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
        return false;
    }
    try {
        RestartManifest manifest;
        manifest.next_timestamp = txn_mgr_->peek_next_timestamp();
        manifest.next_txn_id = txn_mgr_->peek_next_txn_id();
        CheckpointWalCut wal_cut = log_mgr_->create_checkpoint_wal_cut(headers.index_file_names);
        cut_timer.Finish();
        manifest.checkpoint_offset = wal_cut.checkpoint_offset;

        if (metrics_ != nullptr) metrics_->pages_marked(cohort.pages_marked);
        fuzzy_checkpoint_ =
            std::make_unique<FuzzyCheckpointState>(std::move(checkpoint_guard), std::move(catalog_guard),
                                                   std::move(headers), cohort, std::move(wal_cut), manifest,
                                                   attempt.started());
        attempt.Transfer();
    } catch (...) {
        bpm->cancel_checkpoint_cohort(cohort.epoch);
        throw;
    }

    // BlockGuard reopens admission as this function returns. The durable WAL
    // cut and all state needed by later Ticks are already owned by the state.
    reservation.armed = false;
    return true;
}

bool CheckpointManager::AdvanceFuzzyCheckpoint() {
    BufferPoolManager* bpm = sm_mgr_->get_bpm();
    if (bpm == nullptr || fuzzy_checkpoint_ == nullptr) {
        throw InternalError("invalid fuzzy checkpoint state");
    }

    FaultInjector::Point("before_fuzzy_checkpoint_data_sync");
    const size_t quantum_pages = options_.io_quantum_pages;
    const size_t byte_budget_pages = std::max<size_t>(size_t{1}, options_.tick_bytes / PAGE_SIZE);
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

    // The detached images and descriptor set were captured at the same cut as
    // the cohort. This call writes headers, fdatasyncs every captured data/index
    // fd, and atomically replaces db.meta while the original catalog guard is
    // still held.
    FaultInjector::Point("after_fuzzy_checkpoint_data_sync");
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::FuzzyFinalPublish);
        sm_mgr_->write_fixed_checkpoint_headers(fuzzy_checkpoint_->headers, fuzzy_checkpoint_->catalog_guard);
    }
    FaultInjector::Point("before_fuzzy_checkpoint_manifest_publish");
    {
        auto timer = CheckpointPhaseMetrics::Scope(metrics_, CheckpointPhaseMetrics::Timing::ManifestPublish);
        log_mgr_->write_restart_manifest(fuzzy_checkpoint_->manifest);
    }

    g_published_restart_offset.store(fuzzy_checkpoint_->wal_cut.checkpoint_offset, std::memory_order_release);
    fuzzy_checkpoint_->cohort.epoch = 0;
    auto completed = std::move(fuzzy_checkpoint_);
    FuzzyAttemptMetric::RecordLifetime(metrics_, completed->metrics_started);
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
        if (metrics_ != nullptr && fuzzy_checkpoint_ != nullptr) metrics_->fuzzy_failure();
        CancelFuzzyCheckpoint(false);
        DeferAutomaticRetry();
        return false;
    }
}
