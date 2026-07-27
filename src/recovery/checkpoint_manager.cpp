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

#include <atomic>

#include "common/fault_injection.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace {

std::atomic<bool> g_checkpoint_running{false};
std::atomic<bool> g_preflush_running{false};

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
// Bounded extra preflush rounds before the blocking window opens. A round that
// finds fewer than this many dirty pages is not worth another pass.
constexpr int kPreblockFlushRounds = 4;
constexpr size_t kPreblockFlushProgressPages = 256;

uint64_t ElapsedNs(std::chrono::steady_clock::time_point begin) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - begin).count());
}

struct PreflushGuard {
    std::atomic<bool>* running;

    ~PreflushGuard() {
        running->store(false, std::memory_order_release);
    }
};

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
    if (txn_mgr_ != nullptr) {
        txn_mgr_->observe_checkpoint_attempt();
    }
    running_.store(true);

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

    // Writers remain active during this pass. Let each page batch establish
    // its own WAL-before-data boundary instead of treating the initial flush
    // as sufficient for pages dirtied concurrently with this checkpoint.
    const auto initial_begin = std::chrono::steady_clock::now();
    log_mgr_->flush_log_to_disk_with_sync();
    if (!sm_mgr_->flush_dirty_data_pages(false)) {
        txn_mgr_->observe_checkpoint_initial_ns(ElapsedNs(initial_begin));
        return false;
    }
    txn_mgr_->observe_checkpoint_initial_ns(ElapsedNs(initial_begin));
    // Keep draining the dirty set while writers are still running, so the
    // blocking window below only has to write the residue.
    const auto preblock_begin = std::chrono::steady_clock::now();
    for (int round = 0; round < kPreblockFlushRounds; ++round) {
        if (sm_mgr_->flush_dirty_pages(options_.preflush_batch_pages) < kPreblockFlushProgressPages) {
            break;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }
    }
    txn_mgr_->observe_checkpoint_preblock_ns(ElapsedNs(preblock_begin));
    FaultInjector::Point("after_background_page_write_before_final_wal_flush");

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
    if (!sm_mgr_->flush_all_table_and_index_pages(true)) {
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
    if (log_mgr_ == nullptr || sm_mgr_ == nullptr) {
        return false;
    }

    const int64_t log_offset = log_mgr_->current_log_offset();
    const int64_t preflush_start = options_.auto_checkpoint_bytes > options_.preflush_trigger_bytes
                                       ? options_.auto_checkpoint_bytes - options_.preflush_trigger_bytes
                                       : 0;
    if (log_offset >= preflush_start && log_offset < options_.auto_checkpoint_bytes) {
        bool expected = false;
        if (g_preflush_running.compare_exchange_strong(expected, true)) {
            PreflushGuard preflush_guard{&g_preflush_running};
            if (txn_mgr_ != nullptr) {
                txn_mgr_->observe_checkpoint_preflush();
            }
            sm_mgr_->flush_dirty_pages(options_.preflush_batch_pages);
        }
    }

    if (log_offset < options_.auto_checkpoint_bytes) {
        return false;
    }
    if (std::chrono::steady_clock::now() < drain_retry_time_ && log_offset < drain_retry_log_offset_) {
        return false;
    }
    return RunCleanCheckpoint();
}
