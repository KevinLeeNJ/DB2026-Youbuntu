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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

class LogManager;
class SmManager;
class TransactionManager;

struct CheckpointOptions {
    int64_t auto_checkpoint_bytes = 4LL * 1024 * 1024 * 1024;
};

class CheckpointManager {
public:
    CheckpointManager(TransactionManager* txn_mgr, SmManager* sm_mgr, LogManager* log_mgr);
    ~CheckpointManager();

    bool RunCleanCheckpoint();
    bool Tick();
    bool RunIfNeeded();
    void SetOptions(CheckpointOptions options);

private:
    struct FuzzyCheckpointState;

    bool StartFuzzyCheckpoint();
    bool AdvanceFuzzyCheckpoint();
    void CancelFuzzyCheckpoint() noexcept;
    void DeferAutomaticRetry() noexcept;
    int64_t RetainedWalBytes(int64_t current_offset) noexcept;

    TransactionManager* txn_mgr_;
    SmManager* sm_mgr_;
    LogManager* log_mgr_;
    CheckpointOptions options_{};
    std::unique_ptr<FuzzyCheckpointState> fuzzy_checkpoint_;
    // Backoff state for automatic checkpoints after a failed or over-budget
    // round. Retrying every scheduler tick would make the whole process flap
    // between blocked and unblocked while the underlying condition persists.
    std::chrono::steady_clock::time_point drain_retry_time_{};
    int64_t drain_retry_log_offset_{0};
};
