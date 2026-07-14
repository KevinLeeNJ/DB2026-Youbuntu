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

#include "common/fault_injection.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"

namespace {

std::atomic<bool> g_checkpoint_running{false};

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

    const auto block_begin = std::chrono::steady_clock::now();
    struct BlockGuard {
        TransactionManager* txn_mgr;

        explicit BlockGuard(TransactionManager* mgr) : txn_mgr(mgr) {
            txn_mgr->block_new_transactions_for_checkpoint();
        }

        ~BlockGuard() {
            txn_mgr->unblock_new_transactions_after_checkpoint();
        }
    } block_guard(txn_mgr_);
    last_block_new_txn_ns_.store(
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - block_begin)
                                  .count()),
        std::memory_order_release);

    const auto drain_begin = std::chrono::steady_clock::now();
    txn_mgr_->wait_active_transactions_drained_for_checkpoint();
    last_drain_ns_.store(
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - drain_begin)
                                  .count()),
        std::memory_order_release);
    const auto log_sync_begin = std::chrono::steady_clock::now();
    log_mgr_->flush_log_to_disk_with_sync();
    last_log_sync_ns_.store(
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - log_sync_begin)
                                  .count()),
        std::memory_order_release);
    FaultInjector::Point("before_checkpoint_data_sync");
    const auto data_flush_begin = std::chrono::steady_clock::now();
    sm_mgr_->flush_all_table_and_index_pages();
    last_data_flush_ns_.store(static_cast<uint64_t>(
                                  std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() -
                                                                                         data_flush_begin)
                                      .count()),
                              std::memory_order_release);
    FaultInjector::Point("after_checkpoint_data_sync");
    const auto meta_flush_begin = std::chrono::steady_clock::now();
    sm_mgr_->flush_meta();
    last_meta_flush_ns_.store(static_cast<uint64_t>(
                                  std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() -
                                                                                         meta_flush_begin)
                                      .count()),
                              std::memory_order_release);
    // Publish the restart manifest before truncating WAL. If anything after
    // this point fails, the complete WAL is still available for recovery.
    log_mgr_->write_restart_offset(0);
    FaultInjector::Point("before_wal_truncate");
    const auto truncate_begin = std::chrono::steady_clock::now();
    log_mgr_->reset_log(log_mgr_->get_global_lsn());
    last_truncate_ns_.store(static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - truncate_begin)
                                    .count()),
                            std::memory_order_release);
    return true;
}

bool CheckpointManager::RunIfNeeded() {
    if (log_mgr_ == nullptr || log_mgr_->current_log_offset() < options_.auto_checkpoint_bytes) {
        return false;
    }
    return RunCleanCheckpoint();
}
