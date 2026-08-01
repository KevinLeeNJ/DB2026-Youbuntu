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
#include <chrono>
#include <cstring>
#include <functional>

namespace {
class TransactionOperationPin {
public:
    explicit TransactionOperationPin(Transaction* txn) : txn_(txn) {
        if (txn_ != nullptr) {
            txn_->pin_lock_operation();
        }
    }
    ~TransactionOperationPin() {
        if (txn_ != nullptr) {
            txn_->unpin_lock_operation();
        }
    }
    TransactionOperationPin(const TransactionOperationPin&) = delete;
    TransactionOperationPin& operator=(const TransactionOperationPin&) = delete;

private:
    Transaction* txn_;
};
} // namespace

LockManager::LockTableShard& LockManager::get_shard(const LockDataId& lock_data_id) {
    return lock_table_shards_[std::hash<LockDataId>{}(lock_data_id) % LOCK_TABLE_SHARD_COUNT];
}

void LockManager::observe_queue_depth(std::atomic<uint64_t>& maximum, size_t depth) {
    uint64_t observed = maximum.load(std::memory_order_relaxed);
    const uint64_t wanted = static_cast<uint64_t>(depth);
    while (observed < wanted &&
           !maximum.compare_exchange_weak(observed, wanted, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

LockObservabilitySnapshot LockManager::record_lock_observability() const {
    return {record_immediate_conflict_.load(std::memory_order_relaxed),
            record_wait_enqueued_.load(std::memory_order_relaxed),
            record_wait_granted_.load(std::memory_order_relaxed),
            record_wait_cancelled_.load(std::memory_order_relaxed),
            record_wait_ns_.load(std::memory_order_relaxed),
            record_queue_depth_max_.load(std::memory_order_relaxed),
            record_cycle_checks_.load(std::memory_order_relaxed),
            record_cycle_victims_.load(std::memory_order_relaxed)};
}

LockObservabilitySnapshot LockManager::unique_key_lock_observability() const {
    return {unique_immediate_conflict_.load(std::memory_order_relaxed),
            unique_wait_enqueued_.load(std::memory_order_relaxed),
            unique_wait_granted_.load(std::memory_order_relaxed),
            unique_wait_cancelled_.load(std::memory_order_relaxed),
            unique_wait_ns_.load(std::memory_order_relaxed),
            unique_queue_depth_max_.load(std::memory_order_relaxed),
            unique_cycle_checks_.load(std::memory_order_relaxed),
            unique_cycle_victims_.load(std::memory_order_relaxed)};
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
    txn->pin_lock_operation();
    pending_locks_[txn->get_transaction_id()].push_back(PendingLock{lock_data_id, request_queue, request});
    auto [it, inserted] = waiting_txns_.try_emplace(txn->get_transaction_id(), WaitingTxn{txn, 0});
    assert(inserted || it->second.txn == txn);
    ++it->second.registrations;
    return true;
}

void LockManager::unregister_pending_lock(txn_id_t txn_id, const LockDataId& lock_data_id,
                                          const std::shared_ptr<LockRequest>& request) {
    std::lock_guard<std::mutex> lock(pending_latch_);
    auto txn_it = pending_locks_.find(txn_id);
    if (txn_it == pending_locks_.end()) {
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
    }
    unregister_waiting_txn_locked(txn_id);
}

bool LockManager::register_waiting_txn(Transaction* txn) {
    if (txn == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(pending_latch_);
    if (txn->is_lock_cancellation_requested()) {
        return false;
    }
    txn->pin_lock_operation();
    auto [it, inserted] = waiting_txns_.try_emplace(txn->get_transaction_id(), WaitingTxn{txn, 0});
    assert(inserted || it->second.txn == txn);
    ++it->second.registrations;
    return true;
}

void LockManager::unregister_waiting_txn(txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(pending_latch_);
    unregister_waiting_txn_locked(txn_id);
}

void LockManager::unregister_waiting_txn_locked(txn_id_t txn_id) {
    auto it = waiting_txns_.find(txn_id);
    if (it == waiting_txns_.end()) {
        return;
    }
    assert(it->second.registrations > 0);
    Transaction* txn = it->second.txn;
    if (--it->second.registrations == 0) {
        waiting_txns_.erase(it);
        pending_cv_.notify_all();
    }
    // This must remain under pending_latch_: a cycle canceller that copied the
    // raw pointer pins it under the same latch before this source pin can go.
    txn->unpin_lock_operation();
}

void LockManager::note_wait_topology_change() {
    wait_topology_epoch_.fetch_add(1, std::memory_order_release);
}

void LockManager::set_record_handoff_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    record_handoff_test_hook_ = std::move(hook);
    record_handoff_test_hook_enabled_.store(static_cast<bool>(record_handoff_test_hook_), std::memory_order_release);
}

void LockManager::set_record_handoff_published_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    record_handoff_published_test_hook_ = std::move(hook);
    record_handoff_published_test_hook_enabled_.store(static_cast<bool>(record_handoff_published_test_hook_),
                                                      std::memory_order_release);
}

void LockManager::set_record_handoff_checked_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    record_handoff_checked_test_hook_ = std::move(hook);
    record_handoff_checked_test_hook_enabled_.store(static_cast<bool>(record_handoff_checked_test_hook_),
                                                    std::memory_order_release);
}

void LockManager::set_record_handoff_pre_notify_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    record_handoff_pre_notify_test_hook_ = std::move(hook);
    record_handoff_pre_notify_test_hook_enabled_.store(static_cast<bool>(record_handoff_pre_notify_test_hook_),
                                                        std::memory_order_release);
}

void LockManager::set_unique_handoff_published_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    unique_handoff_published_test_hook_ = std::move(hook);
    unique_handoff_published_test_hook_enabled_.store(static_cast<bool>(unique_handoff_published_test_hook_),
                                                       std::memory_order_release);
}

void LockManager::set_cycle_cancel_before_record_queue_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    cycle_cancel_before_record_queue_test_hook_ = std::move(hook);
    cycle_cancel_before_record_queue_test_hook_enabled_.store(static_cast<bool>(cycle_cancel_before_record_queue_test_hook_),
                                                               std::memory_order_release);
}

void LockManager::set_cycle_cancel_before_flag_test_hook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
    cycle_cancel_before_flag_test_hook_ = std::move(hook);
    cycle_cancel_before_flag_test_hook_enabled_.store(static_cast<bool>(cycle_cancel_before_flag_test_hook_),
                                                       std::memory_order_release);
}

void LockManager::run_record_handoff_test_hook() {
    if (!record_handoff_test_hook_enabled_.load(std::memory_order_acquire)) {
        return;
    }
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
        hook = std::move(record_handoff_test_hook_);
        record_handoff_test_hook_enabled_.store(false, std::memory_order_release);
    }
    if (hook) {
        hook();
    }
}

void LockManager::run_record_handoff_published_test_hook() {
    if (!record_handoff_published_test_hook_enabled_.load(std::memory_order_acquire)) {
        return;
    }
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
        hook = std::move(record_handoff_published_test_hook_);
        record_handoff_published_test_hook_enabled_.store(false, std::memory_order_release);
    }
    if (hook) {
        hook();
    }
}

void LockManager::run_record_handoff_checked_test_hook() {
    if (!record_handoff_checked_test_hook_enabled_.load(std::memory_order_acquire)) {
        return;
    }
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
        hook = std::move(record_handoff_checked_test_hook_);
        record_handoff_checked_test_hook_enabled_.store(false, std::memory_order_release);
    }
    if (hook) {
        hook();
    }
}

void LockManager::run_record_handoff_pre_notify_test_hook() {
    if (!record_handoff_pre_notify_test_hook_enabled_.load(std::memory_order_acquire)) return;
    std::function<void()> hook;
    { std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_); hook = std::move(record_handoff_pre_notify_test_hook_); record_handoff_pre_notify_test_hook_enabled_.store(false, std::memory_order_release); }
    if (hook) hook();
}

void LockManager::run_unique_handoff_published_test_hook() {
    if (!unique_handoff_published_test_hook_enabled_.load(std::memory_order_acquire)) {
        return;
    }
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_);
        hook = std::move(unique_handoff_published_test_hook_);
        unique_handoff_published_test_hook_enabled_.store(false, std::memory_order_release);
    }
    if (hook) {
        hook();
    }
}

void LockManager::run_cycle_cancel_before_record_queue_test_hook() {
    if (!cycle_cancel_before_record_queue_test_hook_enabled_.load(std::memory_order_acquire)) return;
    std::function<void()> hook;
    { std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_); hook = std::move(cycle_cancel_before_record_queue_test_hook_); cycle_cancel_before_record_queue_test_hook_enabled_.store(false, std::memory_order_release); }
    if (hook) hook();
}

void LockManager::run_cycle_cancel_before_flag_test_hook() {
    if (!cycle_cancel_before_flag_test_hook_enabled_.load(std::memory_order_acquire)) return;
    std::function<void()> hook;
    { std::lock_guard<std::mutex> lock(record_handoff_test_hook_latch_); hook = std::move(cycle_cancel_before_flag_test_hook_); cycle_cancel_before_flag_test_hook_enabled_.store(false, std::memory_order_release); }
    if (hook) hook();
}

void LockManager::cancel_waiting_transaction_for_test(txn_id_t txn_id) {
    cancel_waiting_transaction(txn_id);
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
    for (const auto& request : record_notifications) request->cv_.notify_one();
    for (const auto& request : record_next_notifications) request->cv_.notify_one();
    for (const auto& queue : unique_notifications) queue->cv.notify_all();
    if (txn != nullptr) {
        txn->unpin_lock_operation();
    }
    return victim_cancelled;
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
LockAcquireResult LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    if (txn == nullptr) {
        return false;
    }
    TransactionOperationPin acquisition_pin(txn);
    if (txn->is_lock_cancellation_requested()) {
        return false;
    }

    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto request_queue = get_or_create_queue(lock_data_id);
    std::unique_lock<std::mutex> lock(request_queue->latch_);

    if (txn->is_lock_cancellation_requested()) {
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        try_remove_empty_queue(lock_data_id, request_queue);
        return false;
    }

    if (request_queue->owner_txn_id_ == INVALID_TXN_ID) {
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

    const bool owns_other_lock = !txn->get_lock_set()->empty() || !txn->get_unique_key_lock_set()->empty();

    // SERIALIZABLE keeps its established first-write behavior: an active
    // conflicting owner aborts a lock-free writer immediately. SI and READ
    // COMMITTED use the existing FIFO handoff path. Once a transaction has
    // already acquired a record or unique-key lock, preserve the existing
    // cycle path.
    const IsolationLevel isolation = txn->get_isolation_level();
    const bool immediate_snapshot_conflict = !owns_other_lock && isolation == IsolationLevel::SERIALIZABLE;
    if (immediate_snapshot_conflict) {
        record_immediate_conflict_.fetch_add(1, std::memory_order_relaxed);
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        try_remove_empty_queue(lock_data_id, request_queue);
        return LockAcquireResult::Value::WriteConflict;
    }

    auto request = std::make_shared<LockRequest>(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    request_queue->waiters_.push_back(request);
    const auto wait_begin = std::chrono::steady_clock::now();
    note_wait_topology_change();
    if (!register_pending_lock(txn, lock_data_id, request_queue, request)) {
        request->state_ = LockRequest::State::Cancelled;
        request_queue->waiters_.pop_back();
        record_wait_cancelled_.fetch_add(1, std::memory_order_relaxed);
        note_wait_topology_change();
        lock.unlock();
        release_queue_user(lock_data_id, request_queue);
        try_remove_empty_queue(lock_data_id, request_queue);
        return false;
    }
    record_wait_enqueued_.fetch_add(1, std::memory_order_relaxed);
    observe_queue_depth(record_queue_depth_max_, request_queue->waiters_.size());
    lock.unlock();
    // A transaction with no granted record or unique-key lock cannot be the
    // predecessor of another wait-for edge, so this first wait cannot close a
    // cycle. Avoid scanning all lock shards on the common hot-row path.
    if (owns_other_lock) {
        record_cycle_checks_.fetch_add(1, std::memory_order_relaxed);
        const txn_id_t cycle_victim = find_youngest_cycle_victim(txn->get_transaction_id());
        if (cycle_victim != INVALID_TXN_ID && cancel_waiting_transaction(cycle_victim)) {
            record_cycle_victims_.fetch_add(1, std::memory_order_relaxed);
            wait_cycle_abort_count_.fetch_add(1, std::memory_order_acq_rel);
        }
    }
    lock.lock();
    request->cv_.wait(lock, [&request] { return request->state_ != LockRequest::State::Waiting; });
    const bool owns_queue = request_queue->owner_txn_id_ == txn->get_transaction_id();
    const bool publishable = request->state_ == LockRequest::State::GrantedUnpublished && owns_queue;
    if (publishable) {
        // Keep the pending request alive until this ownership is published.
        // A concurrent abort either wins the final check below or observes the
        // lock in txn->lock_set_ and releases it through the normal path.
        txn->get_lock_set()->insert(lock_data_id);
        run_record_handoff_published_test_hook();
    }
    const bool deadlock_victim = request->state_ == LockRequest::State::DeadlockVictim;
    const bool cancelled = deadlock_victim || request->state_ == LockRequest::State::Cancelled ||
                           txn->is_lock_cancellation_requested();
    std::shared_ptr<LockRequest> next_request;
    if (cancelled && owns_queue) {
        // unlock() grants ownership before the waiter can publish it in its
        // lock set. If cancellation wins this interval, relinquish the owner
        // while still holding the queue latch and continue FIFO handoff.
        request_queue->owner_txn_id_ = INVALID_TXN_ID;
        note_wait_topology_change();
        txn->get_lock_set()->erase(lock_data_id);
        if (!request_queue->waiters_.empty()) {
            next_request = request_queue->waiters_.front();
            request_queue->waiters_.pop_front();
            next_request->state_ = LockRequest::State::GrantedUnpublished;
            request_queue->owner_txn_id_ = next_request->txn_id_;
            request_queue->group_lock_mode_ = GroupLockMode::X;
            note_wait_topology_change();
            run_record_handoff_test_hook();
        } else {
            request_queue->group_lock_mode_ = GroupLockMode::NON_LOCK;
        }
    }
    if (publishable && !cancelled) {
        // This is the record waiter's linearization point. A cycle detector
        // may cancel Waiting or GrantedUnpublished requests only; after this
        // transition a stale graph observation is a no-op.
        request->state_ = LockRequest::State::Completed;
        run_record_handoff_checked_test_hook();
    }
    record_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                        std::chrono::steady_clock::now() - wait_begin)
                                                        .count()),
                              std::memory_order_relaxed);
    lock.unlock();
    if (next_request != nullptr) {
        next_request->cv_.notify_one();
    }
    unregister_pending_lock(txn->get_transaction_id(), lock_data_id, request);
    release_queue_user(lock_data_id, request_queue);
    if (cancelled) {
        record_wait_cancelled_.fetch_add(1, std::memory_order_relaxed);
        try_remove_empty_queue(lock_data_id, request_queue);
        return deadlock_victim ? LockAcquireResult::Value::DeadlockVictim : LockAcquireResult::Value::Cancelled;
    }
    record_wait_granted_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

std::string LockManager::make_unique_key_lock_id(int index_fd, const std::vector<char>& key) {
    // The leading domain byte makes this representation disjoint from a
    // logical-row delete intent even when arbitrary binary keys are used.
    std::string lock_id(1, 'U');
    lock_id.append(sizeof(index_fd), '\0');
    std::memcpy(lock_id.data() + 1, &index_fd, sizeof(index_fd));
    lock_id.append(key.data(), key.size());
    return lock_id;
}

std::string LockManager::make_logical_row_delete_intent_id(uint64_t table_runtime_id,
                                                            const std::vector<char>& record_bytes) {
    std::string intent_id(1, 'R');
    intent_id.append(sizeof(table_runtime_id), '\0');
    std::memcpy(intent_id.data() + 1, &table_runtime_id, sizeof(table_runtime_id));
    intent_id.append(record_bytes.data(), record_bytes.size());
    return intent_id;
}

bool LockManager::lock_exclusive_on_unique_key(Transaction* txn, int index_fd, const std::vector<char>& key) {
    return lock_exclusive_on_unique_key_id(txn, make_unique_key_lock_id(index_fd, key));
}

bool LockManager::register_logical_row_delete_intent(Transaction* txn, uint64_t table_runtime_id,
                                                      const std::vector<char>& record_bytes) {
    if (txn == nullptr || txn->is_lock_cancellation_requested()) {
        return false;
    }
    const std::string intent_id = make_logical_row_delete_intent_id(table_runtime_id, record_bytes);
    auto& shard =
        logical_row_shards_[std::hash<std::string>{}(intent_id) % LOGICAL_ROW_INTENT_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.latch);
    if (txn->is_lock_cancellation_requested()) {
        return false;
    }
    shard.delete_intents[intent_id].insert(txn->get_transaction_id());
    txn->get_logical_row_delete_intent_set()->insert(intent_id);
    return true;
}

bool LockManager::logical_row_delete_intent_conflicts(Transaction* txn, uint64_t table_runtime_id,
                                                       const std::vector<char>& record_bytes) {
    if (txn == nullptr || txn->is_lock_cancellation_requested()) {
        return true;
    }
    const std::string intent_id = make_logical_row_delete_intent_id(table_runtime_id, record_bytes);
    auto& shard =
        logical_row_shards_[std::hash<std::string>{}(intent_id) % LOGICAL_ROW_INTENT_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.latch);
    if (txn->is_lock_cancellation_requested()) {
        return true;
    }
    auto it = shard.delete_intents.find(intent_id);
    if (it == shard.delete_intents.end()) {
        return false;
    }
    const txn_id_t txn_id = txn->get_transaction_id();
    return std::any_of(it->second.begin(), it->second.end(), [&](txn_id_t owner) { return owner != txn_id; });
}

bool LockManager::unregister_logical_row_delete_intent(Transaction* txn, const std::string& intent_id) {
    if (txn == nullptr) {
        return false;
    }
    auto& shard =
        logical_row_shards_[std::hash<std::string>{}(intent_id) % LOGICAL_ROW_INTENT_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.latch);
    auto it = shard.delete_intents.find(intent_id);
    if (it == shard.delete_intents.end() || it->second.erase(txn->get_transaction_id()) == 0) {
        return false;
    }
    txn->get_logical_row_delete_intent_set()->erase(intent_id);
    if (it->second.empty()) {
        shard.delete_intents.erase(it);
    }
    return true;
}

bool LockManager::lock_exclusive_on_unique_key_id(Transaction* txn, const std::string& lock_id) {
    if (txn == nullptr) {
        return false;
    }
    TransactionOperationPin acquisition_pin(txn);
    if (txn->is_lock_cancellation_requested()) {
        return false;
    }

    auto& shard = unique_key_shards_[std::hash<std::string>{}(lock_id) % UNIQUE_KEY_SHARD_COUNT];
    std::unique_lock<std::mutex> lock(shard.latch);
    auto& queue = shard.queues[lock_id];
    if (queue == nullptr) {
        queue = std::make_shared<UniqueKeyQueue>();
    }
    if (txn->is_lock_cancellation_requested()) {
        return false;
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
        unique_immediate_conflict_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    queue->waiters.push_back(txn_id);
    const auto wait_begin = std::chrono::steady_clock::now();
    note_wait_topology_change();
    if (!register_waiting_txn(txn)) {
        queue->waiters.erase(std::find(queue->waiters.begin(), queue->waiters.end(), txn_id));
        note_wait_topology_change();
        queue->cv.notify_all();
        return false;
    }
    unique_wait_enqueued_.fetch_add(1, std::memory_order_relaxed);
    observe_queue_depth(unique_queue_depth_max_, queue->waiters.size());
    lock.unlock();
    // The first unique-key wait of a lock-free transaction cannot form a
    // cycle. Defer graph construction until the transaction already owns a
    // lock and can therefore have an outgoing wait-for edge.
    if (!txn->get_lock_set()->empty() || !txn->get_unique_key_lock_set()->empty()) {
        unique_cycle_checks_.fetch_add(1, std::memory_order_relaxed);
        const txn_id_t cycle_victim = find_youngest_cycle_victim(txn_id);
        if (cycle_victim != INVALID_TXN_ID && cancel_waiting_transaction(cycle_victim)) {
            unique_cycle_victims_.fetch_add(1, std::memory_order_relaxed);
            wait_cycle_abort_count_.fetch_add(1, std::memory_order_acq_rel);
        }
    }
    lock.lock();
    queue->cv.wait(lock, [&] {
        return txn->is_lock_cancellation_requested() ||
               (queue->owner == INVALID_TXN_ID && !queue->waiters.empty() && queue->waiters.front() == txn_id);
    });
    if (txn->is_lock_cancellation_requested()) {
        unique_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                            std::chrono::steady_clock::now() - wait_begin)
                                                            .count()),
                                  std::memory_order_relaxed);
        unique_wait_cancelled_.fetch_add(1, std::memory_order_relaxed);
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
    unique_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                        std::chrono::steady_clock::now() - wait_begin)
                                                        .count()),
                              std::memory_order_relaxed);
    unique_wait_granted_.fetch_add(1, std::memory_order_relaxed);
    queue->owner = txn_id;
    note_wait_topology_change();
    // Publish ownership before dropping the waiter registration. An abort that
    // races this handoff either observes the registration or releases this
    // lock through TransactionManager::ReleaseLocks; it can never miss both.
    txn->get_unique_key_lock_set()->insert(lock_id);
    const bool cancelled_after_publish = txn->is_lock_cancellation_requested();
    run_unique_handoff_published_test_hook();
    if (cancelled_after_publish) {
        queue->owner = INVALID_TXN_ID;
        txn->get_unique_key_lock_set()->erase(lock_id);
        note_wait_topology_change();
        queue->cv.notify_all();
    }
    unregister_waiting_txn(txn_id);
    if (cancelled_after_publish) {
        unique_wait_cancelled_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
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
        next_request->state_ = LockRequest::State::GrantedUnpublished;
        request_queue->owner_txn_id_ = next_request->txn_id_;
        request_queue->group_lock_mode_ = GroupLockMode::X;
        note_wait_topology_change();
        run_record_handoff_test_hook();
    }
    lock.unlock();
    if (next_request != nullptr) {
        run_record_handoff_pre_notify_test_hook();
        next_request->cv_.notify_one();
    }
    release_queue_user(lock_data_id, request_queue);
    try_remove_empty_queue(lock_data_id, request_queue);
    return true;
}
