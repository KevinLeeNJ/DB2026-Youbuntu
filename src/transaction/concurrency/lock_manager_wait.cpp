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
#include <functional>
#include <unordered_map>
#include <vector>

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
                if (waiter == nullptr || waiter->state_ != LockRequest::State::Waiting) {
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

bool LockManager::cancel_waiting_transaction(txn_id_t txn_id) {
    Transaction* txn = nullptr;
    std::vector<PendingLock> pending;
    {
        std::lock_guard<std::mutex> lock(pending_latch_);
        auto txn_it = waiting_txns_.find(txn_id);
        if (txn_it != waiting_txns_.end()) {
            txn = txn_it->second.txn;
            // Keep the raw pointer valid after releasing pending_latch_. The
            // registration pin cannot disappear until this extra pin exists.
            txn->pin_lock_operation();
        }
        auto pending_it = pending_locks_.find(txn_id);
        if (pending_it != pending_locks_.end()) {
            pending = pending_it->second;
        }
    }
    bool victim_cancelled = false;
    std::vector<std::shared_ptr<LockRequest>> record_notifications;
    std::vector<std::shared_ptr<LockRequest>> record_next_notifications;
    std::vector<std::shared_ptr<UniqueKeyQueue>> unique_notifications;
    run_cycle_cancel_before_record_queue_test_hook();
    for (const auto& item : pending) {
        std::shared_ptr<LockRequest> next_request;
        bool cancelled = false;
        {
            std::unique_lock<std::mutex> lock(item.queue->latch_);
            if (item.request->state_ == LockRequest::State::Waiting) {
                auto request_it = std::find(item.queue->waiters_.begin(), item.queue->waiters_.end(), item.request);
                if (request_it != item.queue->waiters_.end()) {
                    item.request->state_ = LockRequest::State::DeadlockVictim;
                    item.queue->waiters_.erase(request_it);
                    note_wait_topology_change();
                    cancelled = true;
                    if (item.queue->owner_txn_id_ == INVALID_TXN_ID && !item.queue->waiters_.empty()) {
                        next_request = item.queue->waiters_.front();
                        item.queue->waiters_.pop_front();
                        next_request->state_ = LockRequest::State::GrantedUnpublished;
                        item.queue->owner_txn_id_ = next_request->txn_id_;
                        item.queue->group_lock_mode_ = GroupLockMode::X;
                        note_wait_topology_change();
                    }
                }
            } else if (item.request->state_ == LockRequest::State::GrantedUnpublished) {
                // This request owns the queue but has not yet published that
                // ownership into txn->lock_set_. The waiter will relinquish
                // it under the queue latch when it observes this terminal
                // state.
                item.request->state_ = LockRequest::State::DeadlockVictim;
                cancelled = true;
            }
        }
        victim_cancelled = victim_cancelled || cancelled;
        if (cancelled) {
            record_notifications.push_back(item.request);
        }
        if (next_request != nullptr) {
            record_next_notifications.push_back(next_request);
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
                victim_cancelled = true;
                unique_notifications.push_back(queue);
            }
            if (queue->owner == INVALID_TXN_ID && queue->waiters.empty()) {
                it = shard.queues.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Every queue/state change is now visible. Publish the terminal txn state
    // before any waiter notification so unique wait predicates cannot sleep
    // between removal and the flag becoming observable.
    if (victim_cancelled && txn != nullptr) {
        run_cycle_cancel_before_flag_test_hook();
        // The cycle snapshot may be stale. Only a request that was still
        // Waiting or GrantedUnpublished above is a valid victim.
        txn->mark_lock_deadlock_victim();
        txn->mark_lock_cancellation_requested();
    }
    for (const auto& request : record_notifications)
        request->cv_.notify_one();
    for (const auto& request : record_next_notifications)
        request->cv_.notify_one();
    for (const auto& queue : unique_notifications)
        queue->cv.notify_all();
    if (txn != nullptr) {
        txn->unpin_lock_operation();
    }
    return victim_cancelled;
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
            pending = it->second;
        }
    }

    for (const auto& item : pending) {
        std::shared_ptr<LockRequest> next_request;
        bool cancelled = false;
        {
            std::unique_lock<std::mutex> lock(item.queue->latch_);
            if (item.request->state_ == LockRequest::State::Waiting) {
                auto request_it = std::find(item.queue->waiters_.begin(), item.queue->waiters_.end(), item.request);
                if (request_it == item.queue->waiters_.end()) {
                    continue;
                }
                item.request->state_ = LockRequest::State::Cancelled;
                item.queue->waiters_.erase(request_it);
                note_wait_topology_change();
                cancelled = true;
                if (item.queue->owner_txn_id_ == INVALID_TXN_ID && !item.queue->waiters_.empty()) {
                    next_request = item.queue->waiters_.front();
                    item.queue->waiters_.pop_front();
                    next_request->state_ = LockRequest::State::GrantedUnpublished;
                    item.queue->owner_txn_id_ = next_request->txn_id_;
                    item.queue->group_lock_mode_ = GroupLockMode::X;
                    note_wait_topology_change();
                }
            } else if (item.request->state_ == LockRequest::State::GrantedUnpublished) {
                item.request->state_ = LockRequest::State::Cancelled;
                cancelled = true;
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

void LockManager::wait_for_transaction_lock_requests(txn_id_t txn_id) {
    std::unique_lock<std::mutex> lock(pending_latch_);
    pending_cv_.wait(lock, [&] {
        return pending_locks_.find(txn_id) == pending_locks_.end() && waiting_txns_.find(txn_id) == waiting_txns_.end();
    });
}
