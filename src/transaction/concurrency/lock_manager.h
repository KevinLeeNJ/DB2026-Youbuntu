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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <atomic>
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

struct LockObservabilitySnapshot {
    uint64_t immediate_conflict{0};
    uint64_t wait_enqueued{0};
    uint64_t wait_granted{0};
    uint64_t wait_cancelled{0};
    uint64_t wait_ns{0};
    uint64_t queue_depth_max{0};
    uint64_t cycle_checks{0};
    uint64_t cycle_victims{0};
};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX };

    /* 事务的加锁申请 */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;    // 申请加锁的事务ID
        LockMode lock_mode_; // 事务申请加锁的类型
        bool granted_;       // 该事务是否已经被赋予锁
        bool cancelled_{false};
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

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

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

    uint64_t wait_cycle_abort_count() const {
        return wait_cycle_abort_count_.load(std::memory_order_acquire);
    }

    LockObservabilitySnapshot record_lock_observability() const;
    LockObservabilitySnapshot unique_key_lock_observability() const;

private:
    static constexpr size_t LOCK_TABLE_SHARD_COUNT = 64;

    struct PendingLock {
        LockDataId lock_data_id;
        std::shared_ptr<LockRequestQueue> queue;
        std::shared_ptr<LockRequest> request;
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
    void register_waiting_txn(Transaction* txn);
    void unregister_waiting_txn(txn_id_t txn_id);
    void cancel_waiting_transaction(txn_id_t txn_id);
    static void observe_queue_depth(std::atomic<uint64_t>& maximum, size_t depth);

    std::array<LockTableShard, LOCK_TABLE_SHARD_COUNT> lock_table_shards_;
    std::mutex pending_latch_;
    std::unordered_map<txn_id_t, std::vector<PendingLock>> pending_locks_;
    std::unordered_map<txn_id_t, Transaction*> waiting_txns_;
    std::atomic<uint64_t> wait_topology_epoch_{0};
    std::atomic<uint64_t> wait_cycle_abort_count_{0};
    std::atomic<uint64_t> record_immediate_conflict_{0};
    std::atomic<uint64_t> record_wait_enqueued_{0};
    std::atomic<uint64_t> record_wait_granted_{0};
    std::atomic<uint64_t> record_wait_cancelled_{0};
    std::atomic<uint64_t> record_wait_ns_{0};
    std::atomic<uint64_t> record_queue_depth_max_{0};
    std::atomic<uint64_t> record_cycle_checks_{0};
    std::atomic<uint64_t> record_cycle_victims_{0};
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
    std::atomic<uint64_t> unique_immediate_conflict_{0};
    std::atomic<uint64_t> unique_wait_enqueued_{0};
    std::atomic<uint64_t> unique_wait_granted_{0};
    std::atomic<uint64_t> unique_wait_cancelled_{0};
    std::atomic<uint64_t> unique_wait_ns_{0};
    std::atomic<uint64_t> unique_queue_depth_max_{0};
    std::atomic<uint64_t> unique_cycle_checks_{0};
    std::atomic<uint64_t> unique_cycle_victims_{0};

    static constexpr size_t LOGICAL_ROW_INTENT_SHARD_COUNT = 64;
    struct LogicalRowShard {
        std::mutex latch;
        std::unordered_map<std::string, std::unordered_set<txn_id_t>> delete_intents;
    };
    static std::string make_logical_row_delete_intent_id(uint64_t table_runtime_id,
                                                         const std::vector<char>& record_bytes);
    std::array<LogicalRowShard, LOGICAL_ROW_INTENT_SHARD_COUNT> logical_row_shards_;
};
