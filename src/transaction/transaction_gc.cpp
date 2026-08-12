/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "system/sm_manager.h"

#include <mutex>
#include <unordered_set>
#include <vector>

namespace {

// 垃圾回收节流：避免每次 commit 都全表扫描 txn_map
constexpr uint64_t GC_COMMIT_INTERVAL = 256;  // 每 N 次提交尝试一次 GC
constexpr size_t GC_TXN_MAP_THRESHOLD = 1024; // 或 txn_map 超过该阈值立即 GC
constexpr size_t GC_BATCH_SIZE = 128;
constexpr size_t GC_SCAN_LIMIT = 512;

} // namespace

bool TransactionManager::TransactionHasUndoNeededByVersionChain(Transaction* txn) const {
    return txn != nullptr && txn->GetUndoLogNum() > 0;
}

bool TransactionManager::CanRetireTransactionUnlocked(Transaction* txn) const {
    if (txn == nullptr) {
        return false;
    }
    if (txn->has_commit_publication_pin()) {
        return false;
    }
    TransactionState state = txn->get_state();
    if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
        return false;
    }
    txn_id_t txn_id = txn->get_transaction_id();
    if (active_txn_ids_.count(txn_id) > 0) {
        return false;
    }
    if (TransactionHasSsiStateUnlocked(txn_id)) {
        return false;
    }
    if (TransactionHasUndoNeededByVersionChain(txn)) {
        return false;
    }
    auto* non_const_this = const_cast<TransactionManager*>(this);
    return !non_const_this->HasActiveSerializableOverlapUnlocked(txn);
}

void TransactionManager::RetireTransactionIfSafe(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    bool no_active_transactions = false;
    {
        // Keep the transaction active-set transition and the txn_map lifetime
        // decision atomic with respect to GC's active-set snapshot.
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        if (txn->has_commit_publication_pin()) {
            return;
        }
        active_txn_ids_.erase(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
        no_active_transactions = active_txn_count_ == 0;

        std::unique_lock<std::mutex> lock(latch_);
        if (CanRetireTransactionUnlocked(txn)) {
            auto it = txn_map_.find(txn->get_transaction_id());
            if (it != txn_map_.end() && it->second.get() == txn) {
                txn_map_.erase(it);
            }
        }
    }
    checkpoint_cv_.notify_all();
    if (no_active_transactions) {
        gc_cv_.notify_one();
    }
}

timestamp_t TransactionManager::GetWatermark() {
    return running_txns_.GetWatermark();
}

void TransactionManager::MaybeRunGarbageCollection() {
    bool notify_gc = false;
    {
        std::lock_guard<std::mutex> lock(latch_);
        ++commits_since_gc_;
        if (!gc_requested_ && (commits_since_gc_ >= GC_COMMIT_INTERVAL || txn_map_.size() >= GC_TXN_MAP_THRESHOLD)) {
            gc_requested_ = true;
            commits_since_gc_ = 0;
            notify_gc = true;
        }
    }
    if (notify_gc) {
        gc_cv_.notify_one();
    }
}

void TransactionManager::GarbageCollectionLoop() {
    std::unique_lock<std::mutex> lock(latch_);
    while (!gc_stop_.load(std::memory_order_acquire)) {
        gc_cv_.wait(lock, [this] { return gc_stop_.load(std::memory_order_acquire) || gc_requested_; });
        if (gc_stop_.load(std::memory_order_acquire)) {
            break;
        }
        // GarbageCollectionBatch uses the oldest active read timestamp as
        // its safety boundary. It must not wait for a globally quiescent
        // transaction set, otherwise a sustained workload can starve GC.
        gc_requested_ = false;
        gc_running_ = true;
        lock.unlock();

        bool more = false;
        try {
            more = GarbageCollectionBatch();
        } catch (...) {
            // GC is opportunistic. A transient metadata/storage error must
            // not terminate the server or strand the worker in gc_running_.
        }

        lock.lock();
        gc_running_ = false;
        if (more && !gc_stop_.load(std::memory_order_acquire)) {
            gc_requested_ = true;
        }
    }
}

void TransactionManager::GarbageCollection() {
    // Explicit callers (checkpoint/tests) ask for convergence. Keep the
    // background worker bounded by calling GarbageCollectionBatch() directly
    // there, while this synchronous API drains successive bounded batches.
    while (GarbageCollectionBatch()) {
    }
}

bool TransactionManager::GarbageCollectionBatch() {
    // 安全条件：水位线是所有活跃事务读时间戳的最小值。只有 commit_ts（已提交）
    // 或 start_ts（已中止）严格小于水位线的事务，其 undo log 才不会被任何活跃
    // 事务的版本链遍历访问到，因而可安全从 txn_map 回收。
    // 与 GetUndoLog 互斥：二者都持 latch_。
    timestamp_t watermark = running_txns_.GetWatermark();
    // active_txn_ids_ is maintained under checkpoint_latch_. Take a conservative
    // snapshot first, then release that lock before scanning txn_map_ so GC does
    // not block transaction begin/commit for the duration of the scan.
    std::unordered_set<txn_id_t> active_txn_snapshot;
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_txn_snapshot = active_txn_ids_;
    }

    std::vector<txn_id_t> to_erase;
    {
        std::unique_lock<std::mutex> lock(latch_);
        size_t scanned = 0;
        for (auto it = txn_map_.begin(); it != txn_map_.end() && scanned < GC_SCAN_LIMIT; ++it, ++scanned) {
            Transaction* txn = it->second.get();
            if (txn == nullptr) {
                to_erase.push_back(it->first);
                continue;
            }
            TransactionState state = txn->get_state();
            if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
                continue;
            }
            if (active_txn_snapshot.find(it->first) != active_txn_snapshot.end()) {
                continue;
            }
            if (TransactionHasRetainedSsiStateUnlocked(it->first)) {
                continue;
            }
            if (state == TransactionState::COMMITTED) {
                if (txn->get_commit_ts() == INVALID_TS || txn->get_commit_ts() >= watermark) {
                    continue;
                }
            } else {
                // 已中止事务：commit_ts 无效，保守用 start_ts 判断。
                // 任何 read_ts >= start_ts 的活跃事务都已无法看到该事务的版本。
                if (txn->get_start_ts() >= watermark) {
                    continue;
                }
            }
            // 仅在无 SSI 活跃重叠时回收，避免误删仍被 SSI 依赖追踪的事务
            auto* non_const_this = const_cast<TransactionManager*>(this);
            if (non_const_this->HasActiveSerializableOverlapUnlocked(txn)) {
                continue;
            }
            to_erase.push_back(it->first);
            if (to_erase.size() >= GC_BATCH_SIZE) {
                break;
            }
        }
        for (txn_id_t txn_id : to_erase) {
            txn_map_.erase(txn_id);
        }
    }
    // 回收 SmManager 侧随写操作单调增长的历史索引键/删除候选（需访问 tuple meta，
    // 不在 latch_ 下进行，避免与缓冲池操作死锁）
    // History/index candidates have the same watermark safety rule as txn_map
    // entries and must be pruned even when this batch found no transaction
    // object to erase.
    sm_manager_->version_history().prune(watermark);
    return to_erase.size() >= GC_BATCH_SIZE;
}
