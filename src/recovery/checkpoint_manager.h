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

#include <atomic>
#include <cstdint>

class LogManager;
class SmManager;
class TransactionManager;

struct CheckpointOptions {
    int64_t auto_checkpoint_bytes = 256LL * 1024 * 1024;
};

class CheckpointManager {
public:
    CheckpointManager(TransactionManager* txn_mgr, SmManager* sm_mgr, LogManager* log_mgr);

    bool RunCleanCheckpoint();
    bool RunIfNeeded();
    void SetOptions(CheckpointOptions options);

    uint64_t last_block_new_txn_ns() const {
        return last_block_new_txn_ns_.load(std::memory_order_acquire);
    }

    uint64_t last_drain_ns() const {
        return last_drain_ns_.load(std::memory_order_acquire);
    }

    uint64_t last_data_flush_ns() const {
        return last_data_flush_ns_.load(std::memory_order_acquire);
    }

    uint64_t last_meta_flush_ns() const {
        return last_meta_flush_ns_.load(std::memory_order_acquire);
    }

    uint64_t last_log_sync_ns() const {
        return last_log_sync_ns_.load(std::memory_order_acquire);
    }

    uint64_t last_truncate_ns() const {
        return last_truncate_ns_.load(std::memory_order_acquire);
    }

private:
    TransactionManager* txn_mgr_;
    SmManager* sm_mgr_;
    LogManager* log_mgr_;
    CheckpointOptions options_{};
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> last_block_new_txn_ns_{0};
    std::atomic<uint64_t> last_drain_ns_{0};
    std::atomic<uint64_t> last_data_flush_ns_{0};
    std::atomic<uint64_t> last_meta_flush_ns_{0};
    std::atomic<uint64_t> last_log_sync_ns_{0};
    std::atomic<uint64_t> last_truncate_ns_{0};
};
