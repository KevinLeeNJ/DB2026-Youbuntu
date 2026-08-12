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

bool LockManager::has_unique_waiters_for_test() {
    for (auto& shard : unique_key_shards_) {
        std::lock_guard<std::mutex> shard_lock(shard.latch);
        for (const auto& [_, queue] : shard.queues) {
            if (!queue->waiters.empty()) {
                return true;
            }
        }
    }
    return false;
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
    auto& shard = logical_row_shards_[std::hash<std::string>{}(intent_id) % LOGICAL_ROW_INTENT_SHARD_COUNT];
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
    auto& shard = logical_row_shards_[std::hash<std::string>{}(intent_id) % LOGICAL_ROW_INTENT_SHARD_COUNT];
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
    auto& shard = logical_row_shards_[std::hash<std::string>{}(intent_id) % LOGICAL_ROW_INTENT_SHARD_COUNT];
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
        return false;
    }
    queue->waiters.push_back(txn_id);
    note_wait_topology_change();
    if (!register_waiting_txn(txn)) {
        queue->waiters.erase(std::find(queue->waiters.begin(), queue->waiters.end(), txn_id));
        note_wait_topology_change();
        queue->cv.notify_all();
        return false;
    }
    lock.unlock();
    // The first unique-key wait of a lock-free transaction cannot form a
    // cycle. Defer graph construction until the transaction already owns a
    // lock and can therefore have an outgoing wait-for edge.
    if (!txn->get_lock_set()->empty() || !txn->get_unique_key_lock_set()->empty()) {
        const txn_id_t cycle_victim = find_youngest_cycle_victim(txn_id);
        if (cycle_victim != INVALID_TXN_ID && cancel_waiting_transaction(cycle_victim)) {
        }
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
