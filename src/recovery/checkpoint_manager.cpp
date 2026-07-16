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
#include <chrono>
#include <cstdlib>

#include "common/fault_injection.h"
#include "minilog.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace {

std::atomic<bool> g_checkpoint_running{false};
std::atomic<bool> g_preflush_running{false};

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
    const auto checkpoint_start = std::chrono::steady_clock::now();
    bool expected = false;
    if (!g_checkpoint_running.compare_exchange_strong(expected, true)) {
        return false;
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

    // Establish a WAL durability point before the background page pass. The
    // per-page flush path can then usually observe that its page LSN is
    // already durable, while the final WAL flush below still covers writers
    // that race with this pass.
    log_mgr_->flush_log_to_disk_with_sync();
    const auto background_flush_start = std::chrono::steady_clock::now();
    if (!sm_mgr_->flush_dirty_data_pages(true)) {
        return false;
    }
    const auto background_flush_end = std::chrono::steady_clock::now();

    struct BlockGuard {
        TransactionManager* txn_mgr;

        explicit BlockGuard(TransactionManager* mgr) : txn_mgr(mgr) {
            txn_mgr->block_new_transactions_for_checkpoint();
        }

        ~BlockGuard() {
            txn_mgr->unblock_new_transactions_after_checkpoint();
        }
    } block_guard(txn_mgr_);
    const auto drain_start = std::chrono::steady_clock::now();
    txn_mgr_->wait_active_transactions_drained_for_checkpoint();
    const auto drain_end = std::chrono::steady_clock::now();
    log_mgr_->flush_log_to_disk_with_sync();
    const auto wal_end = std::chrono::steady_clock::now();
    FaultInjector::Point("before_checkpoint_data_sync");
    if (!sm_mgr_->flush_all_table_and_index_pages(true)) {
        return false;
    }
    const auto data_end = std::chrono::steady_clock::now();
    FaultInjector::Point("after_checkpoint_data_sync");
    sm_mgr_->flush_meta();
    const auto meta_end = std::chrono::steady_clock::now();
    // Publish the restart manifest before truncating WAL. If anything after
    // this point fails, the complete WAL is still available for recovery.
    log_mgr_->write_restart_offset(0);
    FaultInjector::Point("before_wal_truncate");
    log_mgr_->reset_log(log_mgr_->get_global_lsn());
    if (std::getenv("RMDB_CHECKPOINT_METRICS") != nullptr) {
        const auto milliseconds = [](auto begin, auto end) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
        };
        LOG_WARN("checkpoint metrics: drain_ms=%lld wal_ms=%lld data_ms=%lld meta_ms=%lld total_ms=%lld",
                 static_cast<long long>(milliseconds(drain_start, drain_end)),
                 static_cast<long long>(milliseconds(drain_end, wal_end)),
                 static_cast<long long>(milliseconds(wal_end, data_end)),
                 static_cast<long long>(milliseconds(data_end, meta_end)),
                 static_cast<long long>(milliseconds(checkpoint_start, std::chrono::steady_clock::now())));
        LOG_WARN("checkpoint background data flush: elapsed_ms=%lld",
                 static_cast<long long>(milliseconds(background_flush_start, background_flush_end)));
    }
    return true;
}

bool CheckpointManager::RunIfNeeded() {
    if (log_mgr_ == nullptr || sm_mgr_ == nullptr) {
        return false;
    }

    const int64_t log_offset = log_mgr_->current_log_offset();
    const int64_t preflush_start =
        options_.auto_checkpoint_bytes > options_.preflush_trigger_bytes
            ? options_.auto_checkpoint_bytes - options_.preflush_trigger_bytes
            : 0;
    if (log_offset >= preflush_start && log_offset < options_.auto_checkpoint_bytes) {
        bool expected = false;
        if (g_preflush_running.compare_exchange_strong(expected, true)) {
            PreflushGuard preflush_guard{&g_preflush_running};
            const auto preflush_start_time = std::chrono::steady_clock::now();
            sm_mgr_->flush_dirty_pages(options_.preflush_batch_pages);
            if (std::getenv("RMDB_CHECKPOINT_METRICS") != nullptr) {
                const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::steady_clock::now() - preflush_start_time)
                                         .count();
                LOG_WARN("checkpoint preflush metrics: pages=%zu elapsed_ms=%lld", options_.preflush_batch_pages,
                         static_cast<long long>(elapsed));
            }
        }
    }

    if (log_offset < options_.auto_checkpoint_bytes) {
        return false;
    }
    return RunCleanCheckpoint();
}
