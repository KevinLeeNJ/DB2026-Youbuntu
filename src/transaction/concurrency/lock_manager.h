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

#pragma once

#include <array>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <atomic>
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

// Record-lock acquisition needs to tell an executor whether a failed wait was
// a deadlock victim selection or an ordinary transaction cancellation. Keep an
// implicit bool conversion so existing low-level callers retain their compact
// success checks while mutation paths can preserve the abort classification.
class LockAcquireResult {
public:
    enum class Value { Granted, Cancelled, DeadlockVictim, WriteConflict };

    constexpr LockAcquireResult(Value value) : value_(value) {}
    constexpr LockAcquireResult(bool granted) : value_(granted ? Value::Granted : Value::Cancelled) {}

    constexpr operator bool() const {
        return value_ == Value::Granted;
    }

    constexpr Value value() const {
        return value_;
    }

private:
    Value value_;
};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX };

    /* 事务的加锁申请 */
    class LockRequest {
    public:
        enum class State { Waiting, GrantedUnpublished, Completed, Cancelled, DeadlockVictim };

        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode) {}

        txn_id_t txn_id_;             // 申请加锁的事务ID
        LockMode lock_mode_;          // 事务申请加锁的类型
        State state_{State::Waiting}; // protected by LockRequestQueue::latch_
        std::condition_variable cv_;
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        txn_id_t owner_txn_id_{INVALID_TXN_ID};
        std::deque<std::shared_ptr<LockRequest>> waiters_;
        std::mutex latch_;
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;
        size_t active_users_ = 0; // 在分片锁外持有并访问该队列的线程数
    };

    class LockTableShard {
    public:
        std::mutex latch_;
        std::unordered_map<LockDataId, std::shared_ptr<LockRequestQueue>> lock_table_;
    };

public:
    LockManager() = default;

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    LockAcquireResult lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    // Reserve a logical unique-index key for the lifetime of txn. This is
    // separate from the B+ tree structural latch because history/current-index
    // validation must be protected across the whole write protocol.
    bool lock_exclusive_on_unique_key(Transaction* txn, int index_fd, const std::vector<char>& key);
    // DELETE publishes a transaction-lifetime exact-row intent before writing
    // its tombstone. INSERT only probes for another transaction's intent. This
    // is directional: insert-before-delete and concurrent inserts remain valid.
    bool register_logical_row_delete_intent(Transaction* txn, uint64_t table_runtime_id,
                                            const std::vector<char>& record_bytes);
    bool logical_row_delete_intent_conflicts(Transaction* txn, uint64_t table_runtime_id,
                                             const std::vector<char>& record_bytes);
    bool unregister_logical_row_delete_intent(Transaction* txn, const std::string& intent_id);
    bool unlock_unique_key(Transaction* txn, const std::string& lock_id);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

    // Cancel pending lock requests owned by txn. Granted locks are released by the transaction manager.
    void cancel_transaction(Transaction* txn);
    // Wait until every record/unique-key waiter has stopped accessing txn.
    // Transaction retirement must not free a cancelled waiter underneath its
    // lock acquisition path.
    void wait_for_transaction_lock_requests(txn_id_t txn_id);

    // Test-only state probes. These inspect the real wait queues and are not
    // maintained as production counters.
    bool has_record_waiters_for_test();
    bool has_unique_waiters_for_test();

    // Test-only hooks for the owner-handoff cancellation window. Production
    // construction leaves these empty.
    void set_record_handoff_test_hook(std::function<void()> hook);
    void set_record_handoff_published_test_hook(std::function<void()> hook);
    void set_record_handoff_checked_test_hook(std::function<void()> hook);
    void set_record_handoff_pre_notify_test_hook(std::function<void()> hook);
    void set_unique_handoff_published_test_hook(std::function<void()> hook);
    void set_cycle_cancel_before_record_queue_test_hook(std::function<void()> hook);
    void set_cycle_cancel_before_flag_test_hook(std::function<void()> hook);
    void cancel_waiting_transaction_for_test(txn_id_t txn_id);

private:
    static constexpr size_t LOCK_TABLE_SHARD_COUNT = 64;

    struct PendingLock {
        LockDataId lock_data_id;
        std::shared_ptr<LockRequestQueue> queue;
        std::shared_ptr<LockRequest> request;
    };

    struct WaitingTxn {
        Transaction* txn;
        size_t registrations{0};
    };

    LockTableShard& get_shard(const LockDataId& lock_data_id);
    std::shared_ptr<LockRequestQueue> get_or_create_queue(const LockDataId& lock_data_id);
    std::shared_ptr<LockRequestQueue> get_queue(const LockDataId& lock_data_id);
    void release_queue_user(const LockDataId& lock_data_id, const std::shared_ptr<LockRequestQueue>& request_queue);
    void try_remove_empty_queue(const LockDataId& lock_data_id, const std::shared_ptr<LockRequestQueue>& request_queue);
    bool register_pending_lock(Transaction* txn, const LockDataId& lock_data_id,
                               const std::shared_ptr<LockRequestQueue>& request_queue,
                               const std::shared_ptr<LockRequest>& request);
    void unregister_pending_lock(txn_id_t txn_id, const LockDataId& lock_data_id,
                                 const std::shared_ptr<LockRequest>& request);
    using WaitForGraph = std::unordered_map<txn_id_t, std::vector<txn_id_t>>;
    WaitForGraph build_wait_for_graph_snapshot();
    txn_id_t find_youngest_cycle_victim(txn_id_t requester);
    void note_wait_topology_change();
    bool register_waiting_txn(Transaction* txn);
    void unregister_waiting_txn(txn_id_t txn_id);
    void unregister_waiting_txn_locked(txn_id_t txn_id);
    bool cancel_waiting_transaction(txn_id_t txn_id);
    void run_record_handoff_test_hook();
    void run_record_handoff_published_test_hook();
    void run_record_handoff_checked_test_hook();
    void run_record_handoff_pre_notify_test_hook();
    void run_unique_handoff_published_test_hook();
    void run_cycle_cancel_before_record_queue_test_hook();
    void run_cycle_cancel_before_flag_test_hook();

    std::array<LockTableShard, LOCK_TABLE_SHARD_COUNT> lock_table_shards_;
    std::mutex pending_latch_;
    std::condition_variable pending_cv_;
    std::unordered_map<txn_id_t, std::vector<PendingLock>> pending_locks_;
    std::unordered_map<txn_id_t, WaitingTxn> waiting_txns_;
    std::atomic<uint64_t> wait_topology_epoch_{0};
    std::mutex record_handoff_test_hook_latch_;
    std::function<void()> record_handoff_test_hook_;
    std::function<void()> record_handoff_published_test_hook_;
    std::function<void()> record_handoff_checked_test_hook_;
    std::atomic<bool> record_handoff_test_hook_enabled_{false};
    std::atomic<bool> record_handoff_published_test_hook_enabled_{false};
    std::atomic<bool> record_handoff_checked_test_hook_enabled_{false};
    std::function<void()> record_handoff_pre_notify_test_hook_;
    std::atomic<bool> record_handoff_pre_notify_test_hook_enabled_{false};
    std::function<void()> unique_handoff_published_test_hook_;
    std::atomic<bool> unique_handoff_published_test_hook_enabled_{false};
    std::function<void()> cycle_cancel_before_record_queue_test_hook_;
    std::function<void()> cycle_cancel_before_flag_test_hook_;
    std::atomic<bool> cycle_cancel_before_record_queue_test_hook_enabled_{false};
    std::atomic<bool> cycle_cancel_before_flag_test_hook_enabled_{false};
    static constexpr size_t UNIQUE_KEY_SHARD_COUNT = 64;
    struct UniqueKeyQueue {
        txn_id_t owner{INVALID_TXN_ID};
        std::deque<txn_id_t> waiters;
        std::condition_variable cv;
    };
    struct UniqueKeyShard {
        std::mutex latch;
        std::unordered_map<std::string, std::shared_ptr<UniqueKeyQueue>> queues;
    };
    bool lock_exclusive_on_unique_key_id(Transaction* txn, const std::string& lock_id);
    static std::string make_unique_key_lock_id(int index_fd, const std::vector<char>& key);
    std::array<UniqueKeyShard, UNIQUE_KEY_SHARD_COUNT> unique_key_shards_;

    static constexpr size_t LOGICAL_ROW_INTENT_SHARD_COUNT = 64;
    struct LogicalRowShard {
        std::mutex latch;
        std::unordered_map<std::string, std::unordered_set<txn_id_t>> delete_intents;
    };
    static std::string make_logical_row_delete_intent_id(uint64_t table_runtime_id,
                                                         const std::vector<char>& record_bytes);
    std::array<LogicalRowShard, LOGICAL_ROW_INTENT_SHARD_COUNT> logical_row_shards_;
};
