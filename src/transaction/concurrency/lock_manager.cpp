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

#include "lock_manager.h"

#include <algorithm>
#include <cstring>
#include <functional>

LockManager::LockTableShard& LockManager::get_shard(const LockDataId& lock_data_id) {
    return lock_table_shards_[std::hash<LockDataId>{}(lock_data_id) % LOCK_TABLE_SHARD_COUNT];
}

std::shared_ptr<LockManager::LockRequestQueue> LockManager::get_or_create_queue(const LockDataId& lock_data_id) {
    auto& shard = get_shard(lock_data_id);
    std::scoped_lock<std::mutex> lock(shard.latch_);
    auto& request_queue = shard.lock_table_[lock_data_id];
    if (request_queue == nullptr) {
        request_queue = std::make_shared<LockRequestQueue>();
    }
    request_queue->active_users_++;
    return request_queue;
}

std::shared_ptr<LockManager::LockRequestQueue> LockManager::get_queue(const LockDataId& lock_data_id) {
    auto& shard = get_shard(lock_data_id);
    std::scoped_lock<std::mutex> lock(shard.latch_);
    auto queue_it = shard.lock_table_.find(lock_data_id);
    if (queue_it == shard.lock_table_.end()) {
        return nullptr;
    }
    queue_it->second->active_users_++;
    return queue_it->second;
}

void LockManager::release_queue_user(const LockDataId& lock_data_id,
                                     const std::shared_ptr<LockRequestQueue>& request_queue) {
    auto& shard = get_shard(lock_data_id);
    std::scoped_lock<std::mutex> lock(shard.latch_);
    request_queue->active_users_--;
}

void LockManager::try_remove_empty_queue(const LockDataId& lock_data_id,
                                         const std::shared_ptr<LockRequestQueue>& request_queue) {
    auto& shard = get_shard(lock_data_id);
    std::scoped_lock<std::mutex> shard_lock(shard.latch_);
    std::scoped_lock<std::mutex> queue_lock(request_queue->latch_);
    auto queue_it = shard.lock_table_.find(lock_data_id);
    if (queue_it != shard.lock_table_.end() && queue_it->second == request_queue &&
        request_queue->owner_txn_id_ == INVALID_TXN_ID && request_queue->waiters_.empty() &&
        request_queue->active_users_ == 0) {
        shard.lock_table_.erase(queue_it);
    }
}

bool LockManager::register_pending_lock(Transaction* txn, const LockDataId& lock_data_id,
                                        const std::shared_ptr<LockRequestQueue>& request_queue,
                                        const std::shared_ptr<LockRequest>& request) {
    std::lock_guard<std::mutex> lock(pending_latch_);
    if (txn->is_lock_cancellation_requested()) {
        return false;
    }
    pending_locks_[txn->get_transaction_id()].push_back(PendingLock{lock_data_id, request_queue, request});
    waiting_txns_[txn->get_transaction_id()] = txn;
    return true;
}

void LockManager::unregister_pending_lock(txn_id_t txn_id, const LockDataId& lock_data_id,
                                          const std::shared_ptr<LockRequest>& request) {
    std::lock_guard<std::mutex> lock(pending_latch_);
    auto txn_it = pending_locks_.find(txn_id);
    if (txn_it == pending_locks_.end()) {
        waiting_txns_.erase(txn_id);
        return;
    }
    auto& pending = txn_it->second;
    pending.erase(std::remove_if(pending.begin(), pending.end(),
                                 [&](const PendingLock& item) {
                                     return item.lock_data_id == lock_data_id && item.request == request;
                                 }),
                  pending.end());
    if (pending.empty()) {
        pending_locks_.erase(txn_it);
        waiting_txns_.erase(txn_id);
    }
}

void LockManager::register_waiting_txn(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(pending_latch_);
    waiting_txns_[txn->get_transaction_id()] = txn;
}

void LockManager::unregister_waiting_txn(txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(pending_latch_);
    waiting_txns_.erase(txn_id);
}

void LockManager::note_wait_topology_change() {
    wait_topology_epoch_.fetch_add(1, std::memory_order_release);
}

LockManager::WaitForGraph LockManager::build_wait_for_graph_snapshot() {
    for (;;) {
        const uint64_t start_epoch = wait_topology_epoch_.load(std::memory_order_acquire);
        WaitForGraph graph;

        std::vector<std::shared_ptr<LockRequestQueue>> record_queues;
        for (auto& shard : lock_table_shards_) {
            std::lock_guard<std::mutex> shard_lock(shard.latch_);
            record_queues.reserve(record_queues.size() + shard.lock_table_.size());
            for (const auto& [_, queue] : shard.lock_table_) {
                if (queue != nullptr) {
                    record_queues.push_back(queue);
                }
            }
        }
        for (const auto& queue : record_queues) {
            std::lock_guard<std::mutex> queue_lock(queue->latch_);
            txn_id_t predecessor = queue->owner_txn_id_;
            for (const auto& waiter : queue->waiters_) {
                if (waiter == nullptr || waiter->cancelled_) {
                    continue;
                }
                if (predecessor != INVALID_TXN_ID) {
                    graph[waiter->txn_id_].push_back(predecessor);
                }
                predecessor = waiter->txn_id_;
            }
        }

        for (auto& shard : unique_key_shards_) {
            std::lock_guard<std::mutex> shard_lock(shard.latch);
            for (const auto& [_, queue] : shard.queues) {
                if (queue == nullptr) {
                    continue;
                }
                txn_id_t predecessor = queue->owner;
                for (txn_id_t waiter : queue->waiters) {
                    if (predecessor != INVALID_TXN_ID) {
                        graph[waiter].push_back(predecessor);
                    }
                    predecessor = waiter;
                }
            }
        }

        if (start_epoch == wait_topology_epoch_.load(std::memory_order_acquire)) {
            return graph;
        }
    }
}

txn_id_t LockManager::find_youngest_cycle_victim(txn_id_t requester) {
    const WaitForGraph graph = build_wait_for_graph_snapshot();
    std::unordered_map<txn_id_t, int> color;
    std::unordered_map<txn_id_t, size_t> stack_position;
    std::vector<txn_id_t> stack;
    txn_id_t victim = INVALID_TXN_ID;

    std::function<bool(txn_id_t)> visit = [&](txn_id_t txn_id) {
        color[txn_id] = 1;
        stack_position[txn_id] = stack.size();
        stack.push_back(txn_id);
        auto graph_it = graph.find(txn_id);
        if (graph_it != graph.end()) {
            for (txn_id_t dependency : graph_it->second) {
                if (color[dependency] == 0) {
                    if (visit(dependency)) {
                        return true;
                    }
                } else if (color[dependency] == 1) {
                    victim = dependency;
                    for (size_t index = stack_position[dependency]; index < stack.size(); ++index) {
                        victim = std::max(victim, stack[index]);
                    }
                    return true;
                }
            }
        }
        stack.pop_back();
        stack_position.erase(txn_id);
        color[txn_id] = 2;
        return false;
    };
    visit(requester);
    return victim;
}

void LockManager::cancel_waiting_transaction(txn_id_t txn_id) {
    Transaction* txn = nullptr;
    std::vector<PendingLock> pending;
    {
        std::lock_guard<std::mutex> lock(pending_latch_);
        auto txn_it = waiting_txns_.find(txn_id);
        if (txn_it != waiting_txns_.end()) {
            txn = txn_it->second;
        }
        auto pending_it = pending_locks_.find(txn_id);
        if (pending_it != pending_locks_.end()) {
            pending = pending_it->second;
        }
    }
    for (const auto& item : pending) {
        std::shared_ptr<LockRequest> next_request;
        bool cancelled = false;
        {
            std::unique_lock<std::mutex> lock(item.queue->latch_);
            auto request_it = std::find(item.queue->waiters_.begin(), item.queue->waiters_.end(), item.request);
            if (request_it != item.queue->waiters_.end()) {
                (*request_it)->cancelled_ = true;
                item.queue->waiters_.erase(request_it);
                note_wait_topology_change();
                cancelled = true;
                if (txn != nullptr) {
                    txn->mark_lock_cancellation_requested();
                }
                if (item.queue->owner_txn_id_ == INVALID_TXN_ID && !item.queue->waiters_.empty()) {
                    next_request = item.queue->waiters_.front();
                    item.queue->waiters_.pop_front();
                    next_request->granted_ = true;
                    item.queue->owner_txn_id_ = next_request->txn_id_;
                    item.queue->group_lock_mode_ = GroupLockMode::X;
                    note_wait_topology_change();
                }
            }
        }
        if (cancelled) {
            item.request->cv_.notify_one();
        }
        if (next_request != nullptr) {
            next_request->cv_.notify_one();
        }
    }

    // Unique-key waiters are represented by transaction IDs rather than
    // LockRequest objects, so remove the selected victim from every shard and
    // wake its waiter through the transaction cancellation flag.
    for (auto& shard : unique_key_shards_) {
        std::lock_guard<std::mutex> lock(shard.latch);
        for (auto it = shard.queues.begin(); it != shard.queues.end();) {
            auto& queue = it->second;
            const auto old_size = queue->waiters.size();
            queue->waiters.erase(std::remove(queue->waiters.begin(), queue->waiters.end(), txn_id),
                                 queue->waiters.end());
            if (queue->waiters.size() != old_size) {
                note_wait_topology_change();
                if (txn != nullptr) {
                    txn->mark_lock_cancellation_requested();
                }
                queue->cv.notify_all();
            }
            if (queue->owner == INVALID_TXN_ID && queue->waiters.empty()) {
                it = shard.queues.erase(it);
            } else {
                ++it;
            }
        }
    }
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    (void)txn;
    (void)rid;
    (void)tab_fd;

    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    if (txn == nullptr) {
        return false;
    }

    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto request_queue = get_or_create_queue(lock_data_id);
    std::unique_lock<std::mutex> lock(request_queue->latch_);

    if (request_queue->owner_txn_id_ == INVALID_TXN_ID) {
        auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), LockMode::EXLUCSIVE);
        request->granted_ = true;
        request_queue->owner_txn_id_ = txn->get_transaction_id();
        request_queue->group_lock_mode_ = GroupLockMode::X;
        note_wait_topology_change();
        txn->get_lock_set()->insert(lock_data_id);
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return true;
    }

    if (request_queue->owner_txn_id_ == txn->get_transaction_id()) {
        txn->get_lock_set()->insert(lock_data_id);
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return true;
    }

    // Conflicting requests wait unless they would close a real wait-for
    // cycle. This avoids aborting ordinary RC hot-row conflicts merely because
    // the requester happens to have a larger transaction id.
    // Preserve the established immediate SI/SER conflict for a transaction
    // that has not acquired any other record lock. Once a transaction already
    // owns a record lock, however, waiting is necessary to resolve genuine
    // cross-record cycles (the cycle detector will pick one victim).
    const bool can_wait_for_cycle =
        txn->get_isolation_level() == IsolationLevel::READ_COMMITTED || !txn->get_lock_set()->empty();
    if (!can_wait_for_cycle) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return false;
    }
    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    request_queue->waiters_.push_back(request);
    note_wait_topology_change();
    if (!register_pending_lock(txn, lock_data_id, request_queue, request)) {
        request->cancelled_ = true;
        request_queue->waiters_.pop_back();
        note_wait_topology_change();
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        try_remove_empty_queue(lock_data_id, request_queue);
        return false;
    }
    lock.unlock();
    const txn_id_t cycle_victim = find_youngest_cycle_victim(txn->get_transaction_id());
    if (cycle_victim != INVALID_TXN_ID) {
        wait_cycle_abort_count_.fetch_add(1, std::memory_order_acq_rel);
        cancel_waiting_transaction(cycle_victim);
    }
    lock.lock();
    request->cv_.wait(lock, [&request] { return request->granted_ || request->cancelled_; });
    const bool cancelled = request->cancelled_;
    lock.unlock();
    unregister_pending_lock(txn->get_transaction_id(), lock_data_id, request);
    release_queue_user(lock_data_id, request_queue);
    if (cancelled) {
        try_remove_empty_queue(lock_data_id, request_queue);
        return false;
    }
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

bool LockManager::lock_exclusive_on_unique_key(Transaction* txn, int index_fd, const std::vector<char>& key) {
    if (txn == nullptr) {
        return false;
    }

    std::string lock_id(sizeof(index_fd), '\0');
    std::memcpy(lock_id.data(), &index_fd, sizeof(index_fd));
    lock_id.append(key.data(), key.size());

    auto& shard = unique_key_shards_[std::hash<std::string>{}(lock_id) % UNIQUE_KEY_SHARD_COUNT];
    std::unique_lock<std::mutex> lock(shard.latch);
    auto& queue = shard.queues[lock_id];
    if (queue == nullptr) {
        queue = std::make_shared<UniqueKeyQueue>();
    }
    const txn_id_t txn_id = txn->get_transaction_id();
    if (queue->owner == txn_id) {
        txn->get_unique_key_lock_set()->insert(lock_id);
        return true;
    }
    if (queue->owner == INVALID_TXN_ID && queue->waiters.empty()) {
        queue->owner = txn_id;
        note_wait_topology_change();
        txn->get_unique_key_lock_set()->insert(lock_id);
        return true;
    }

    // Preserve SI/serializable's established immediate unique-key conflict
    // behavior. RC uses the shared wait-for cycle policy.
    if (txn->get_isolation_level() != IsolationLevel::READ_COMMITTED) {
        return false;
    }
    queue->waiters.push_back(txn_id);
    note_wait_topology_change();
    register_waiting_txn(txn);
    lock.unlock();
    const txn_id_t cycle_victim = find_youngest_cycle_victim(txn_id);
    if (cycle_victim != INVALID_TXN_ID) {
        wait_cycle_abort_count_.fetch_add(1, std::memory_order_acq_rel);
        cancel_waiting_transaction(cycle_victim);
    }
    lock.lock();
    queue->cv.wait(lock, [&] {
        return txn->is_lock_cancellation_requested() ||
               (queue->owner == INVALID_TXN_ID && !queue->waiters.empty() && queue->waiters.front() == txn_id);
    });
    if (txn->is_lock_cancellation_requested()) {
        auto it = std::find(queue->waiters.begin(), queue->waiters.end(), txn_id);
        if (it != queue->waiters.end()) {
            queue->waiters.erase(it);
            note_wait_topology_change();
        }
        unregister_waiting_txn(txn_id);
        queue->cv.notify_all();
        return false;
    }
    queue->waiters.pop_front();
    queue->owner = txn_id;
    note_wait_topology_change();
    unregister_waiting_txn(txn_id);
    txn->get_unique_key_lock_set()->insert(lock_id);
    return true;
}

bool LockManager::unlock_unique_key(Transaction* txn, const std::string& lock_id) {
    if (txn == nullptr) {
        return false;
    }
    auto& shard = unique_key_shards_[std::hash<std::string>{}(lock_id) % UNIQUE_KEY_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.latch);
    auto it = shard.queues.find(lock_id);
    if (it == shard.queues.end() || it->second->owner != txn->get_transaction_id()) {
        return false;
    }
    auto queue = it->second;
    queue->owner = INVALID_TXN_ID;
    note_wait_topology_change();
    txn->get_unique_key_lock_set()->erase(lock_id);
    queue->cv.notify_all();
    if (queue->waiters.empty()) {
        shard.queues.erase(it);
    }
    return true;
}

void LockManager::cancel_transaction(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    const txn_id_t txn_id = txn->get_transaction_id();
    txn->mark_lock_cancellation_requested();
    for (auto& shard : unique_key_shards_) {
        std::lock_guard<std::mutex> lock(shard.latch);
        for (auto& [_, queue] : shard.queues) {
            queue->cv.notify_all();
        }
    }
    std::vector<PendingLock> pending;
    {
        std::lock_guard<std::mutex> lock(pending_latch_);
        auto it = pending_locks_.find(txn_id);
        if (it != pending_locks_.end()) {
            pending = std::move(it->second);
            pending_locks_.erase(it);
        }
    }

    for (const auto& item : pending) {
        std::shared_ptr<LockRequest> next_request;
        bool cancelled = false;
        {
            std::unique_lock<std::mutex> lock(item.queue->latch_);
            auto request_it = std::find(item.queue->waiters_.begin(), item.queue->waiters_.end(), item.request);
            if (request_it != item.queue->waiters_.end()) {
                (*request_it)->cancelled_ = true;
                item.queue->waiters_.erase(request_it);
                note_wait_topology_change();
                cancelled = true;
                if (item.queue->owner_txn_id_ == INVALID_TXN_ID && !item.queue->waiters_.empty()) {
                    next_request = item.queue->waiters_.front();
                    item.queue->waiters_.pop_front();
                    next_request->granted_ = true;
                    item.queue->owner_txn_id_ = next_request->txn_id_;
                    item.queue->group_lock_mode_ = GroupLockMode::X;
                    note_wait_topology_change();
                }
            }
        }
        if (cancelled) {
            item.request->cv_.notify_one();
        }
        if (next_request != nullptr) {
            next_request->cv_.notify_one();
        }
        try_remove_empty_queue(item.lock_data_id, item.queue);
    }
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    (void)txn;
    (void)tab_fd;

    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    (void)txn;
    (void)tab_fd;

    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    (void)txn;
    (void)tab_fd;

    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    (void)txn;
    (void)tab_fd;

    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    if (txn == nullptr) {
        return false;
    }

    auto request_queue = get_queue(lock_data_id);
    if (request_queue == nullptr) {
        return false;
    }

    std::unique_lock<std::mutex> lock(request_queue->latch_);
    if (request_queue->owner_txn_id_ != txn->get_transaction_id()) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return false;
    }

    request_queue->owner_txn_id_ = INVALID_TXN_ID;
    note_wait_topology_change();
    txn->get_lock_set()->erase(lock_data_id);
    std::shared_ptr<LockRequest> next_request;
    if (request_queue->waiters_.empty()) {
        request_queue->group_lock_mode_ = GroupLockMode::NON_LOCK;
    } else {
        next_request = request_queue->waiters_.front();
        request_queue->waiters_.pop_front();
        next_request->granted_ = true;
        request_queue->owner_txn_id_ = next_request->txn_id_;
        request_queue->group_lock_mode_ = GroupLockMode::X;
        note_wait_topology_change();
    }
    lock.unlock();
    if (next_request != nullptr) {
        next_request->cv_.notify_one();
    }
    release_queue_user(lock_data_id, request_queue);
    try_remove_empty_queue(lock_data_id, request_queue);
    return true;
}
