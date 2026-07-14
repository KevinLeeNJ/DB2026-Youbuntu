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
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

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
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    // Reserve a logical unique-index key for the lifetime of txn. This is
    // separate from the B+ tree structural latch because history/current-index
    // validation must be protected across the whole write protocol.
    bool lock_exclusive_on_unique_key(Transaction* txn, int index_fd, const std::vector<char>& key);
    bool unlock_unique_key(Transaction* txn, const std::string& lock_id);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

    // Cancel pending lock requests owned by txn. Granted locks are released by the transaction manager.
    void cancel_transaction(Transaction* txn);

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

    std::array<LockTableShard, LOCK_TABLE_SHARD_COUNT> lock_table_shards_;
    std::mutex pending_latch_;
    std::unordered_map<txn_id_t, std::vector<PendingLock>> pending_locks_;
    std::mutex unique_key_latch_;
    std::unordered_map<std::string, txn_id_t> unique_key_owners_;
};
