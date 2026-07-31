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
#include <chrono>
#include <cstddef>
#include <cstdint>

class LogManager;
class SmManager;
class TransactionManager;

struct CheckpointOptions {
    // A clean checkpoint is deliberately infrequent: the final quiescent pass
    // is much cheaper than repeatedly writing hot pages during normal traffic.
    int64_t auto_checkpoint_bytes = 4LL * 1024 * 1024 * 1024;
    // Spread writeback over the second half of a WAL generation. The final
    // clean checkpoint still owns the durability barrier and truncation, but
    // it should only have to write the pages dirtied since the last tick.
    int64_t preflush_trigger_bytes = 2LL * 1024 * 1024 * 1024;
    int64_t checkpoint_defer_bytes = 2LL * 1024 * 1024 * 1024;
    size_t preflush_batch_pages = 4096;
};

class CheckpointManager {
public:
    CheckpointManager(TransactionManager* txn_mgr, SmManager* sm_mgr, LogManager* log_mgr);

    bool RunCleanCheckpoint();
    bool RunIfNeeded();
    void SetOptions(CheckpointOptions options);

private:
    TransactionManager* txn_mgr_;
    SmManager* sm_mgr_;
    LogManager* log_mgr_;
    CheckpointOptions options_{};
    std::atomic<bool> running_{false};
    // Monotonic identifier for the current WAL lifetime. It lets the buffer
    // pool bound one background write attempt per page without clearing a
    // pool-sized table after every checkpoint.
    uint64_t preflush_generation_{1};
    int64_t last_log_offset_{0};
    // Backoff state for automatic checkpoints after a drain failure. Retrying
    // every scheduler tick would make the whole process flap between blocked
    // and unblocked while a stuck transaction stays open.
    std::chrono::steady_clock::time_point drain_retry_time_{};
    int64_t drain_retry_log_offset_{0};
};
