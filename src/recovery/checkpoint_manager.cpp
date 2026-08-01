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
#include <mutex>

#include "common/fault_injection.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace {

std::atomic<bool> g_checkpoint_running{false};

// Longest the checkpoint may keep new transactions blocked while waiting for
// active ones to finish. Saturated connections drain well inside this window,
// while a stuck transaction can never stall the startup liveness probe.
constexpr std::chrono::milliseconds kDrainTimeout{2000};
// Wall-clock budget for one checkpoint round.
constexpr std::chrono::seconds kCheckpointDeadline{10};
// After an abandoned round, an automatic checkpoint waits for both this much
// time and this much extra WAL before blocking writers again.
constexpr std::chrono::seconds kDrainRetryBackoff{30};
constexpr int64_t kDrainRetryBackoffBytes = 32LL * 1024 * 1024;
constexpr size_t kMinPreflushBatchPages = 64;
constexpr size_t kMaxPreflushBatchPages = 1024;

uint64_t ElapsedNs(std::chrono::steady_clock::time_point begin) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count());
}

} // namespace

CheckpointManager::CheckpointManager(TransactionManager* txn_mgr, SmManager* sm_mgr, LogManager* log_mgr)
    : txn_mgr_(txn_mgr), sm_mgr_(sm_mgr), log_mgr_(log_mgr) {}

void CheckpointManager::SetOptions(CheckpointOptions options) {
    options_ = options;
}

bool CheckpointManager::RunCleanCheckpoint() {
    bool expected = false;
    if (!g_checkpoint_running.compare_exchange_strong(expected, true)) {
        return false;
    }
    running_.store(true);
    std::unique_lock<std::shared_mutex> preflush_guard{preflush_latch_};
    if (txn_mgr_ != nullptr) {
        txn_mgr_->observe_checkpoint_attempt();
    }

    struct RunningGuard {
        std::atomic<bool>* local_running;

        ~RunningGuard() {
            local_running->store(false);
            g_checkpoint_running.store(false);
        }
    } running_guard{&running_};

    if (txn_mgr_ == nullptr || sm_mgr_ == nullptr || log_mgr_ == nullptr) {
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + kCheckpointDeadline;

    struct BlockGuard {
        TransactionManager* txn_mgr;
        std::chrono::steady_clock::time_point begin;

        explicit BlockGuard(TransactionManager* mgr) : txn_mgr(mgr) {
            txn_mgr->block_new_transactions_for_checkpoint();
            begin = std::chrono::steady_clock::now();
        }

        ~BlockGuard() {
            txn_mgr->observe_checkpoint_block_ns(ElapsedNs(begin));
            txn_mgr->unblock_new_transactions_after_checkpoint();
        }
    } block_guard(txn_mgr_);
    const auto drain_begin = std::chrono::steady_clock::now();
    if (!txn_mgr_->wait_active_transactions_drained_for_checkpoint(kDrainTimeout)) {
        txn_mgr_->observe_checkpoint_drain_ns(ElapsedNs(drain_begin));
        txn_mgr_->observe_checkpoint_drain_timeout();
        // Abandon this round. ~BlockGuard releases the block immediately, so a
        // stuck transaction can never freeze every other transaction. The WAL
        // keeps growing, which only makes the next recovery slower.
        drain_retry_time_ = std::chrono::steady_clock::now() + kDrainRetryBackoff;
        drain_retry_log_offset_ = log_mgr_->current_log_offset() + kDrainRetryBackoffBytes;
        return false;
    }
    txn_mgr_->observe_checkpoint_drain_ns(ElapsedNs(drain_begin));
    if (std::chrono::steady_clock::now() >= deadline) {
        txn_mgr_->observe_checkpoint_deadline();
        return false;
    }
    const auto final_wal_begin = std::chrono::steady_clock::now();
    log_mgr_->flush_log_to_disk_with_sync();
    txn_mgr_->observe_checkpoint_final_wal_ns(ElapsedNs(final_wal_begin));
    FaultInjector::Point("before_checkpoint_data_sync");
    const auto final_data_begin = std::chrono::steady_clock::now();
    bool final_data_stable = false;
    try {
        final_data_stable = sm_mgr_->flush_all_table_and_index_pages(FlushDependencyPolicy::AlreadyDurable());
    } catch (...) {
        // A failed table/index fdatasync must leave the complete WAL and old
        // restart manifest intact so this checkpoint can be retried.
    }
    if (!final_data_stable) {
        txn_mgr_->observe_checkpoint_final_data_ns(ElapsedNs(final_data_begin));
        txn_mgr_->observe_checkpoint_final_data_fail();
        return false;
    }
    txn_mgr_->observe_checkpoint_final_data_ns(ElapsedNs(final_data_begin));
    FaultInjector::Point("after_checkpoint_data_sync");
    const auto meta_begin = std::chrono::steady_clock::now();
    sm_mgr_->flush_meta();
    txn_mgr_->observe_checkpoint_meta_ns(ElapsedNs(meta_begin));
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
    const auto manifest_begin = std::chrono::steady_clock::now();
    log_mgr_->write_restart_manifest(manifest);
    txn_mgr_->observe_checkpoint_manifest_ns(ElapsedNs(manifest_begin));
    FaultInjector::Point("before_wal_truncate");
    const auto truncate_begin = std::chrono::steady_clock::now();
    log_mgr_->reset_log(log_mgr_->get_global_lsn());
    txn_mgr_->observe_checkpoint_truncate_ns(ElapsedNs(truncate_begin));
    txn_mgr_->observe_checkpoint_success();
    return true;
}

bool CheckpointManager::RunIfNeeded() {
    return Tick();
}

bool CheckpointManager::Tick() {
    if (log_mgr_ == nullptr || sm_mgr_ == nullptr) {
        return false;
    }

    const int64_t target = std::max<int64_t>(1, options_.auto_checkpoint_bytes);
    const int64_t soft_watermark = target / 2 + target % 2;
    if (log_mgr_->current_log_offset() < soft_watermark) {
        return false;
    }

    bool run_checkpoint = false;
    {
        std::shared_lock<std::shared_mutex> preflush_guard{preflush_latch_};
        // A clean checkpoint sets running_ before waiting for this lock.  The
        // second check closes the race with a Tick that was already headed for
        // the shared side when an explicit checkpoint arrived.
        if (running_.load()) {
            return false;
        }
        const int64_t log_offset = log_mgr_->current_log_offset();
        if (log_offset < soft_watermark) {
            return false;
        }

        const double progress = std::clamp(static_cast<double>(log_offset - soft_watermark) /
                                               static_cast<double>(std::max<int64_t>(1, target - soft_watermark)),
                                           0.0, 1.0);
        const size_t batch_pages =
            kMinPreflushBatchPages +
            static_cast<size_t>(progress * progress *
                                static_cast<double>(kMaxPreflushBatchPages - kMinPreflushBatchPages));
        sm_mgr_->flush_dirty_table_pages(batch_pages);

        if (log_offset < target ||
            (std::chrono::steady_clock::now() < drain_retry_time_ && log_offset < drain_retry_log_offset_)) {
            return false;
        }
        const TableDirtyPageStats dirty_stats = sm_mgr_->table_dirty_page_stats();
        const int64_t hard_watermark = target > INT64_MAX - target / 10 ? INT64_MAX : target + target / 10;
        const size_t residue_limit = dirty_stats.frame_capacity / 50;
        run_checkpoint = log_offset >= hard_watermark || dirty_stats.dirty_pages <= residue_limit;
    }
    return run_checkpoint && RunCleanCheckpoint();
}
