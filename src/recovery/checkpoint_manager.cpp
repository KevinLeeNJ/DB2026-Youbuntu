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

    // Writers remain active during this pass. Let each page batch establish
    // its own WAL-before-data boundary instead of treating the initial flush
    // as sufficient for pages dirtied concurrently with this checkpoint.
    log_mgr_->flush_log_to_disk_with_sync();
    if (!sm_mgr_->flush_dirty_data_pages(false)) {
        return false;
    }
    FaultInjector::Point("after_background_page_write_before_final_wal_flush");

    struct BlockGuard {
        TransactionManager* txn_mgr;

        explicit BlockGuard(TransactionManager* mgr) : txn_mgr(mgr) {
            txn_mgr->block_new_transactions_for_checkpoint();
        }

        ~BlockGuard() {
            txn_mgr->unblock_new_transactions_after_checkpoint();
        }
    } block_guard(txn_mgr_);
    txn_mgr_->wait_active_transactions_drained_for_checkpoint();
    log_mgr_->flush_log_to_disk_with_sync();
    FaultInjector::Point("before_checkpoint_data_sync");
    if (!sm_mgr_->flush_all_table_and_index_pages(true)) {
        return false;
    }
    FaultInjector::Point("after_checkpoint_data_sync");
    sm_mgr_->flush_meta();
    // Publish the restart manifest before truncating WAL. If anything after
    // this point fails, the complete WAL is still available for recovery.
    log_mgr_->write_restart_offset(0);
    FaultInjector::Point("before_wal_truncate");
    log_mgr_->reset_log(log_mgr_->get_global_lsn());
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
            sm_mgr_->flush_dirty_pages(options_.preflush_batch_pages);
        }
    }

    if (log_offset < options_.auto_checkpoint_bytes) {
        return false;
    }
    return RunCleanCheckpoint();
}
