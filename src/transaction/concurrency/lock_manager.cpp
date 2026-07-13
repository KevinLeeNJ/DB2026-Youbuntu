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
        request_queue->request_queue_.empty() && request_queue->active_users_ == 0) {
        shard.lock_table_.erase(queue_it);
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

    auto owner_it = std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                                 [](const auto& request) { return request->granted_; });
    if (owner_it == request_queue->request_queue_.end()) {
        auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), LockMode::EXLUCSIVE);
        request->granted_ = true;
        request_queue->request_queue_.push_back(request);
        request_queue->group_lock_mode_ = GroupLockMode::X;
        txn->get_lock_set()->insert(lock_data_id);
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return true;
    }

    if ((*owner_it)->txn_id_ == txn->get_transaction_id()) {
        txn->get_lock_set()->insert(lock_data_id);
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return true;
    }

    // A transaction that already owns locks uses wait-die: an older
    // requester may wait for a younger owner, while a younger requester
    // aborts. For RC, a transaction with no locks cannot participate in a
    // wait cycle, so its first request is allowed to wait regardless of age.
    // Preserve the established SI/RR/SER first-writer conflict behavior;
    // this plan changes the RC path only.
    bool first_lock = txn->get_lock_set()->empty();
    bool rc_first_lock = first_lock && txn->get_isolation_level() == IsolationLevel::READ_COMMITTED;
    if (first_lock && !rc_first_lock) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return false;
    }
    if (!first_lock && txn->get_transaction_id() > (*owner_it)->txn_id_) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return false;
    }
    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    request_queue->request_queue_.push_back(request);
    request->cv_.wait(lock, [&request] { return request->granted_ || request->cancelled_; });
    if (request->cancelled_) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        try_remove_empty_queue(lock_data_id, request_queue);
        return false;
    }
    txn->get_lock_set()->insert(lock_data_id);
    lock.unlock();
    release_queue_user(lock_data_id, request_queue);
    return true;
}

void LockManager::cancel_transaction(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    const txn_id_t txn_id = txn->get_transaction_id();
    for (auto& shard : lock_table_shards_) {
        std::vector<std::pair<LockDataId, std::shared_ptr<LockRequestQueue>>> queues;
        {
            std::lock_guard<std::mutex> shard_lock(shard.latch_);
            queues.reserve(shard.lock_table_.size());
            for (const auto& [lock_data_id, request_queue] : shard.lock_table_) {
                request_queue->active_users_++;
                queues.emplace_back(lock_data_id, request_queue);
            }
        }

        for (auto& [lock_data_id, request_queue] : queues) {
            std::vector<std::shared_ptr<LockRequest>> cancelled_requests;
            std::shared_ptr<LockRequest> next_request;
            {
                std::unique_lock<std::mutex> lock(request_queue->latch_);
                for (auto it = request_queue->request_queue_.begin(); it != request_queue->request_queue_.end();) {
                    const auto& request = *it;
                    if (request->txn_id_ == txn_id && !request->granted_) {
                        request->cancelled_ = true;
                        cancelled_requests.push_back(request);
                        it = request_queue->request_queue_.erase(it);
                    } else {
                        ++it;
                    }
                }

                auto owner_it = std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                                             [](const auto& request) { return request->granted_; });
                if (owner_it == request_queue->request_queue_.end() && !request_queue->request_queue_.empty()) {
                    next_request = request_queue->request_queue_.front();
                    next_request->granted_ = true;
                    request_queue->group_lock_mode_ = GroupLockMode::X;
                }
            }

            for (const auto& request : cancelled_requests) {
                request->cv_.notify_one();
            }
            if (next_request != nullptr) {
                next_request->cv_.notify_one();
            }
            release_queue_user(lock_data_id, request_queue);
            try_remove_empty_queue(lock_data_id, request_queue);
        }
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
    auto request_it = std::find_if(
        request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
        [&](const auto& request) { return request->granted_ && request->txn_id_ == txn->get_transaction_id(); });
    if (request_it == request_queue->request_queue_.end()) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        return false;
    }

    request_queue->request_queue_.erase(request_it);
    txn->get_lock_set()->erase(lock_data_id);
    std::shared_ptr<LockRequest> next_request;
    if (request_queue->request_queue_.empty()) {
        request_queue->group_lock_mode_ = GroupLockMode::NON_LOCK;
    } else {
        next_request = request_queue->request_queue_.front();
        next_request->granted_ = true;
        request_queue->group_lock_mode_ = GroupLockMode::X;
    }
    lock.unlock();
    if (next_request != nullptr) {
        next_request->cv_.notify_one();
    }
    release_queue_user(lock_data_id, request_queue);
    try_remove_empty_queue(lock_data_id, request_queue);
    return true;
}
