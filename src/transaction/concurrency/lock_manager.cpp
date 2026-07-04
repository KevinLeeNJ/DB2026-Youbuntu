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
    std::unique_lock<std::mutex> lock(latch_);
    auto& queue_ptr = lock_table_[lock_data_id];
    if (queue_ptr == nullptr) {
        queue_ptr = std::make_shared<LockRequestQueue>();
    }
    auto request_queue = queue_ptr;

    while (true) {
        auto owner_it = std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                                     [](const LockRequest& request) { return request.granted_; });
        if (owner_it == request_queue->request_queue_.end()) {
            LockRequest request(txn->get_transaction_id(), LockMode::EXLUCSIVE);
            request.granted_ = true;
            request_queue->request_queue_.push_back(request);
            request_queue->group_lock_mode_ = GroupLockMode::X;
            txn->get_lock_set()->insert(lock_data_id);
            return true;
        }

        if (owner_it->txn_id_ == txn->get_transaction_id()) {
            txn->get_lock_set()->insert(lock_data_id);
            return true;
        }

        if (txn->get_transaction_id() > owner_it->txn_id_) {
            return false;
        }

        if (txn->get_lock_set()->empty()) {
            return false;
        }

        request_queue->waiting_count_++;
        request_queue->cv_.wait(lock);
        request_queue->waiting_count_--;
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

    std::scoped_lock<std::mutex> lock(latch_);
    auto queue_it = lock_table_.find(lock_data_id);
    if (queue_it == lock_table_.end()) {
        return false;
    }

    auto request_queue_ptr = queue_it->second;
    auto& request_queue = request_queue_ptr->request_queue_;
    auto request_it = std::find_if(request_queue.begin(), request_queue.end(), [&](const LockRequest& request) {
        return request.txn_id_ == txn->get_transaction_id();
    });
    if (request_it == request_queue.end()) {
        return false;
    }

    request_queue.erase(request_it);
    txn->get_lock_set()->erase(lock_data_id);
    request_queue_ptr->cv_.notify_all();
    if (request_queue.empty() && request_queue_ptr->waiting_count_ == 0) {
        lock_table_.erase(queue_it);
    } else {
        request_queue_ptr->group_lock_mode_ = request_queue.empty() ? GroupLockMode::NON_LOCK : GroupLockMode::X;
    }
    return true;
}
