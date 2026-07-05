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

#include "recovery/log_manager.h"
#include "system/schema_manager.h"
#include "transaction/transaction_manager.h"

namespace rmdb::recovery {
namespace {

std::atomic<bool> g_checkpoint_running{false};

} // namespace

CheckpointManager::CheckpointManager(TransactionManager* txn_mgr, SchemaManager* schema_mgr, LogManager* log_mgr)
    : txn_mgr_(txn_mgr), schema_mgr_(schema_mgr), log_mgr_(log_mgr) {}

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

    if (txn_mgr_ == nullptr || schema_mgr_ == nullptr || log_mgr_ == nullptr) {
        return false;
    }

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
    schema_mgr_->flush_all_table_and_index_pages();
    schema_mgr_->flush_meta();
    log_mgr_->reset_log(log_mgr_->get_global_lsn());
    log_mgr_->write_restart_offset(0);
    return true;
}

bool CheckpointManager::RunIfNeeded() {
    if (log_mgr_ == nullptr || log_mgr_->current_log_offset() < options_.auto_checkpoint_bytes) {
        return false;
    }
    return RunCleanCheckpoint();
}

} // namespace rmdb::recovery
