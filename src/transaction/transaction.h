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

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/common.h"
#include "transaction/read_view.h"
#include "transaction/txn_defs.h"
#include "record/rm_defs.h"

// UndoLink is now defined in record/rm_defs.h (physical undo pointer)
// UndoLog is superseded by UndoLogRecord in undo/undo_defs.h

struct UndoLog {
    /* 此日志是否为删除标记 */
    bool is_deleted_;
    RmRecord* tuple_test_;
    /* 此撤销日志的时间戳 */
    timestamp_t ts_{INVALID_TS};
    /* 撤销日志的前一个版本 */
    UndoLink prev_version_{};

    // MVCC old version fields
    TupleMeta old_meta_;               // TupleMeta before the modification
    std::vector<char> old_tuple_data_; // old record data (for version chain traversal)
};

class Transaction {
private:
    // The prepared SI point-update path may revisit a row already locked by
    // this transaction.  Retain an owned copy only for that case: executor
    // records may borrow a buffer-pool page and therefore cannot outlive one
    // operation.  The small fixed limits keep a pathological transaction from
    // turning this into an unbounded per-transaction cache.
    static constexpr size_t SI_LOCKED_ROW_WORKSPACE_MAX_ROWS = 64;
    static constexpr size_t SI_LOCKED_ROW_WORKSPACE_MAX_ALIASES = 128;
    static constexpr size_t SI_LOCKED_ROW_WORKSPACE_MAX_BYTES = 256 * 1024;

    struct SiLockedRowWorkspaceEntry {
        int table_fd_;
        Rid rid_;
        std::unique_ptr<RmRecord> record_;
    };

    struct SiLockedRowWorkspaceAlias {
        int table_fd_;
        int index_fd_;
        std::vector<char> key_;
        Rid rid_;
    };

    struct UpdateWriteKey {
        std::string tab_name_;
        Rid rid_;
    };

    struct UpdateUndoEntry {
        UpdateWriteKey key_;
        UndoLink undo_link_;
    };

    struct ModifiedSlotKey {
        std::string tab_name_;
        Rid rid_;

        bool operator==(const ModifiedSlotKey& other) const {
            return tab_name_ == other.tab_name_ && rid_ == other.rid_;
        }
    };

    struct ModifiedSlotKeyHash {
        size_t operator()(const ModifiedSlotKey& key) const {
            size_t hash = std::hash<std::string>{}(key.tab_name_);
            hash ^= std::hash<int>{}(key.rid_.page_no) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
            hash ^= std::hash<int>{}(key.rid_.slot_no) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
            return hash;
        }
    };

    std::optional<UpdateWriteKey> pending_update_key_;
    std::optional<UndoLink> pending_update_undo_link_;
    std::vector<UpdateUndoEntry> update_undo_entries_;

public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = DEFAULT_ISOLATION_LEVEL)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
        begin_lsn_ = INVALID_LSN;
        prev_lsn_ = INVALID_LSN;
        thread_id_ = std::this_thread::get_id();
    }

    ~Transaction() = default;

    inline txn_id_t get_transaction_id() const {
        return txn_id_;
    }

    inline std::thread::id get_thread_id() {
        return thread_id_;
    }

    inline void set_txn_mode(bool txn_mode) {
        txn_mode_ = txn_mode;
    }
    inline bool get_txn_mode() {
        return txn_mode_;
    }

    inline void set_start_ts(timestamp_t start_ts) {
        start_ts_ = start_ts;
    }
    inline timestamp_t get_start_ts() {
        return start_ts_;
    }

    inline IsolationLevel get_isolation_level() {
        return isolation_level_;
    }
    inline void set_isolation_level(IsolationLevel level) {
        isolation_level_ = level;
    }

    inline TransactionState get_state() {
        return state_.load(std::memory_order_acquire);
    }
    inline void set_state(TransactionState state) {
        state_.store(state, std::memory_order_release);
    }
    inline bool compare_exchange_state(TransactionState& expected, TransactionState desired) {
        return state_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    inline void pin_commit_publication() {
        commit_publication_pins_.fetch_add(1, std::memory_order_acq_rel);
    }

    inline void unpin_commit_publication() {
        const auto previous = commit_publication_pins_.fetch_sub(1, std::memory_order_acq_rel);
        assert(previous > 0);
        (void)previous;
    }
    inline bool has_commit_publication_pin() const {
        return commit_publication_pins_.load(std::memory_order_acquire) != 0;
    }

    inline void mark_lock_cancellation_requested() {
        lock_cancellation_requested_.store(true, std::memory_order_release);
    }

    inline bool is_lock_cancellation_requested() const {
        return lock_cancellation_requested_.load(std::memory_order_acquire);
    }

    inline void mark_lock_deadlock_victim() {
        lock_deadlock_victim_.store(true, std::memory_order_release);
    }

    inline bool is_lock_deadlock_victim() const {
        return lock_deadlock_victim_.load(std::memory_order_acquire);
    }

    // A lock waiter or row-mutation path pins the transaction while it may
    // dereference it.  TransactionManager::abort waits for these users before
    // retirement, so cancellation cannot free a transaction underneath an
    // in-flight lock handoff or the mutation that follows it.
    inline void pin_lock_operation() {
        std::lock_guard<std::mutex> lock(lock_operation_latch_);
        ++lock_operation_pins_;
    }

    inline void unpin_lock_operation() {
        std::lock_guard<std::mutex> lock(lock_operation_latch_);
        assert(lock_operation_pins_ > 0);
        if (--lock_operation_pins_ == 0) {
            lock_operation_cv_.notify_all();
        }
    }

    inline void wait_for_lock_operations() {
        std::unique_lock<std::mutex> lock(lock_operation_latch_);
        lock_operation_cv_.wait(lock, [&] { return lock_operation_pins_ == 0; });
    }

    struct ActiveOwnerObservation {
        uint64_t first_observation_ns{0};
        uint64_t observer_count{0};
    };

    // LockManager transfers queue-local observations while releasing this
    // transaction's lock. No queue retains a Transaction pointer for metrics.
    void merge_active_owner_observation(uint64_t first_observation_ns, uint64_t observer_count) noexcept {
        if (observer_count == 0) {
            return;
        }
        active_owner_observer_count_.fetch_add(observer_count, std::memory_order_relaxed);
        uint64_t observed = active_owner_first_observation_ns_.load(std::memory_order_relaxed);
        while ((observed == 0 || first_observation_ns < observed) &&
               !active_owner_first_observation_ns_.compare_exchange_weak(
                   observed, first_observation_ns, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }

    ActiveOwnerObservation take_active_owner_observation() noexcept {
        const uint64_t observer_count = active_owner_observer_count_.exchange(0, std::memory_order_relaxed);
        const uint64_t first_observation_ns =
            active_owner_first_observation_ns_.exchange(0, std::memory_order_relaxed);
        return {first_observation_ns, observer_count};
    }

    inline lsn_t get_prev_lsn() {
        return prev_lsn_;
    }
    inline void set_prev_lsn(lsn_t prev_lsn) {
        prev_lsn_ = prev_lsn;
    }
    inline lsn_t get_begin_lsn() {
        return begin_lsn_;
    }
    inline void set_begin_lsn(lsn_t begin_lsn) {
        begin_lsn_ = begin_lsn;
    }

    inline std::deque<std::unique_ptr<WriteRecord>>& get_write_set() {
        return write_set_;
    }
    inline void append_write_record(std::unique_ptr<WriteRecord> write_record) {
        pending_update_key_.reset();
        pending_update_undo_link_.reset();

        if (write_record != nullptr && write_record->GetWriteType() == WType::UPDATE_TUPLE) {
            UpdateWriteKey key{write_record->GetTableName(), write_record->GetRid()};
            for (const auto& entry : update_undo_entries_) {
                if (entry.key_.tab_name_ == key.tab_name_ && entry.key_.rid_ == key.rid_) {
                    // Keep the first WriteRecord and its original undo link. The
                    // executor still applies each later update to the current tuple.
                    pending_update_undo_link_ = entry.undo_link_;
                    return;
                }
            }
            pending_update_key_ = std::move(key);
        } else if (write_record != nullptr) {
            // An INSERT/DELETE on the same RID starts a different write lifecycle.
            const std::string& tab_name = write_record->GetTableName();
            const Rid& rid = write_record->GetRid();
            update_undo_entries_.erase(std::remove_if(update_undo_entries_.begin(), update_undo_entries_.end(),
                                                      [&](const auto& entry) {
                                                          return entry.key_.tab_name_ == tab_name &&
                                                                 entry.key_.rid_ == rid;
                                                      }),
                                       update_undo_entries_.end());
        }
        write_set_.push_back(std::move(write_record));
    }

    inline std::deque<Page*>* get_index_deleted_page_set() {
        return &index_deleted_page_set_;
    }
    inline void append_index_deleted_page(Page* page) {
        index_deleted_page_set_.push_back(page);
    }

    inline std::deque<Page*>* get_index_latch_page_set() {
        return &index_latch_page_set_;
    }
    inline void append_index_latch_page_set(Page* page) {
        index_latch_page_set_.push_back(page);
    }

    inline std::unordered_set<LockDataId>* get_lock_set() {
        return &lock_set_;
    }

    inline std::unordered_set<std::string>* get_unique_key_lock_set() {
        return &unique_key_lock_set_;
    }

    inline std::unordered_set<std::string>* get_logical_row_delete_intent_set() {
        return &logical_row_delete_intent_set_;
    }

    inline timestamp_t get_read_ts() const {
        return read_ts_;
    }
    inline void set_read_ts(timestamp_t ts) {
        read_ts_ = ts;
    }
    inline std::shared_ptr<const ActiveWriterReadView> get_read_view() const { return read_view_; }
    inline void set_read_view(std::shared_ptr<const ActiveWriterReadView> read_view) { read_view_ = std::move(read_view); }
    inline size_t get_watermark_slot() const {
        return watermark_slot_;
    }
    inline void set_watermark_slot(size_t slot) {
        watermark_slot_ = slot;
    }
    inline timestamp_t get_commit_ts() const {
        return commit_ts_;
    }
    inline void set_commit_ts(timestamp_t ts) {
        commit_ts_ = ts;
    }

    // Modified slots tracking (for MVCC commit: mark slots as committed)
    inline auto& get_modified_slots() {
        return modified_slots_;
    }
    inline void append_modified_slot(const std::string& tab_name, const Rid& rid) {
        if (!modified_slot_set_.emplace(ModifiedSlotKey{tab_name, rid}).second) {
            return;
        }
        modified_slots_.emplace_back(tab_name, rid);
    }

    inline void clear_modified_slots() {
        modified_slots_.clear();
        modified_slot_set_.clear();
    }

    std::optional<std::pair<Rid, std::unique_ptr<RmRecord>>>
    LookupSiLockedRowWorkspace(int table_fd, int index_fd, const char* key, size_t key_size) const {
        if (key == nullptr || key_size == 0) {
            return std::nullopt;
        }
        for (const auto& alias : si_locked_row_workspace_aliases_) {
            if (alias.table_fd_ != table_fd || alias.index_fd_ != index_fd || alias.key_.size() != key_size ||
                !std::equal(alias.key_.begin(), alias.key_.end(), key)) {
                continue;
            }
            for (const auto& entry : si_locked_row_workspace_rows_) {
                if (entry.table_fd_ == table_fd && entry.rid_ == alias.rid_ && entry.record_ != nullptr) {
                    return std::make_pair(alias.rid_, std::make_unique<RmRecord>(*entry.record_));
                }
            }
            return std::nullopt;
        }
        return std::nullopt;
    }

    void InstallSiLockedRowWorkspaceRow(int table_fd, const Rid& rid, const RmRecord& record) {
        if (record.size < 0 || record.data == nullptr ||
            static_cast<size_t>(record.size) > SI_LOCKED_ROW_WORKSPACE_MAX_BYTES) {
            return;
        }
        for (auto& entry : si_locked_row_workspace_rows_) {
            if (entry.table_fd_ != table_fd || entry.rid_ != rid) {
                continue;
            }
            const size_t old_size = static_cast<size_t>(entry.record_->size);
            const size_t new_size = static_cast<size_t>(record.size);
            if (si_locked_row_workspace_bytes_ - old_size + new_size > SI_LOCKED_ROW_WORKSPACE_MAX_BYTES) {
                return;
            }
            entry.record_ = std::make_unique<RmRecord>(record);
            si_locked_row_workspace_bytes_ = si_locked_row_workspace_bytes_ - old_size + new_size;
            return;
        }
        if (si_locked_row_workspace_rows_.size() >= SI_LOCKED_ROW_WORKSPACE_MAX_ROWS ||
            si_locked_row_workspace_bytes_ + static_cast<size_t>(record.size) > SI_LOCKED_ROW_WORKSPACE_MAX_BYTES) {
            return;
        }
        si_locked_row_workspace_rows_.push_back(
            SiLockedRowWorkspaceEntry{table_fd, rid, std::make_unique<RmRecord>(record)});
        si_locked_row_workspace_bytes_ += static_cast<size_t>(record.size);
    }

    void RememberSiLockedRowWorkspaceAlias(int table_fd, int index_fd, const char* key, size_t key_size,
                                           const Rid& rid) {
        if (key == nullptr || key_size == 0 || key_size > SI_LOCKED_ROW_WORKSPACE_MAX_BYTES) {
            return;
        }
        for (auto& alias : si_locked_row_workspace_aliases_) {
            if (alias.table_fd_ == table_fd && alias.index_fd_ == index_fd && alias.key_.size() == key_size &&
                std::equal(alias.key_.begin(), alias.key_.end(), key)) {
                alias.rid_ = rid;
                return;
            }
        }
        if (si_locked_row_workspace_aliases_.size() >= SI_LOCKED_ROW_WORKSPACE_MAX_ALIASES ||
            si_locked_row_workspace_bytes_ + key_size > SI_LOCKED_ROW_WORKSPACE_MAX_BYTES) {
            return;
        }
        si_locked_row_workspace_aliases_.push_back(
            SiLockedRowWorkspaceAlias{table_fd, index_fd, std::vector<char>(key, key + key_size), rid});
        si_locked_row_workspace_bytes_ += key_size;
    }

    void InvalidateSiLockedRowWorkspaceAliases(int table_fd, const Rid& rid, bool remove_row = false) {
        for (auto it = si_locked_row_workspace_aliases_.begin(); it != si_locked_row_workspace_aliases_.end();) {
            if (it->table_fd_ == table_fd && it->rid_ == rid) {
                si_locked_row_workspace_bytes_ -= it->key_.size();
                it = si_locked_row_workspace_aliases_.erase(it);
            } else {
                ++it;
            }
        }
        if (!remove_row) {
            return;
        }
        for (auto it = si_locked_row_workspace_rows_.begin(); it != si_locked_row_workspace_rows_.end(); ++it) {
            if (it->table_fd_ == table_fd && it->rid_ == rid) {
                si_locked_row_workspace_bytes_ -= static_cast<size_t>(it->record_->size);
                si_locked_row_workspace_rows_.erase(it);
                break;
            }
        }
    }

    void ClearSiLockedRowWorkspace() {
        si_locked_row_workspace_rows_.clear();
        si_locked_row_workspace_aliases_.clear();
        si_locked_row_workspace_bytes_ = 0;
    }

    /** 修改现有的撤销日志 */
    inline auto ModifyUndoLog(int log_idx, UndoLog new_log) {
        std::scoped_lock<std::mutex> lck(latch_);
        undo_logs_[log_idx] = std::move(new_log);
    }

    /** @return an UndoLink pointing to the appended undo log.
     *  Uses the undo log index as the slot offset for in-memory undo storage. */
    inline auto AppendUndoLog(UndoLog log) -> UndoLink {
        if (pending_update_undo_link_.has_value()) {
            UndoLink undo_link = *pending_update_undo_link_;
            pending_update_undo_link_.reset();
            return undo_link;
        }

        std::scoped_lock<std::mutex> lck(latch_);
        int idx = static_cast<int>(undo_logs_.size());
        undo_logs_.emplace_back(std::move(log));
        UndoLink undo_link{0, idx, txn_id_}; // in-memory: page=0, slot=idx, txn=self
        if (pending_update_key_.has_value()) {
            update_undo_entries_.push_back(UpdateUndoEntry{std::move(*pending_update_key_), undo_link});
            pending_update_key_.reset();
        }
        return undo_link;
    }
    inline auto GetUndoLog(size_t log_id) -> UndoLog {
        std::scoped_lock<std::mutex> lck(latch_);
        if (log_id >= undo_logs_.size()) {
            // A stale or corrupted UndoLink must not index past the buffer.
            // Unchecked, this read whatever followed the vector and built an
            // UndoLog out of it, which crashed the server instead of failing
            // the one statement that followed the bad link.
            throw InternalError("GetUndoLog: undo log index out of range");
        }
        return undo_logs_[log_id];
    }

    /** @return 撤销日志的数量 */
    inline auto GetUndoLogNum() -> size_t {
        std::scoped_lock<std::mutex> lck(latch_);
        return undo_logs_.size();
    }

    // MVCC: slots modified by this txn (tab_name, rid) — for commit-time TupleMeta update
    std::vector<std::pair<std::string, Rid>> modified_slots_;
    std::unordered_set<ModifiedSlotKey, ModifiedSlotKeyHash> modified_slot_set_;

    // SSI dependency tracking (SER only)
    struct PredicateRead {
        std::string tab_name_;
        std::vector<Condition> conds_;
    };

    std::unordered_set<txn_id_t> in_rw_;        // transactions that ->rw this txn
    std::unordered_set<txn_id_t> out_rw_;       // transactions this txn ->rw
    std::unordered_set<std::string> read_rids_; // RIDs read by this txn (keyed as "page_no:slot_no")
    std::vector<PredicateRead> predicate_reads_;

    /** 提交时间戳 (public for TransactionManager access) */
    std::atomic<timestamp_t> commit_ts_{INVALID_TS};

private:
    bool txn_mode_{false};                // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
    std::atomic<TransactionState> state_; // 事务状态；GC/SSI may inspect it from another thread
    std::atomic<uint32_t> commit_publication_pins_{0};
    std::atomic<bool> lock_cancellation_requested_{false};
    std::atomic<bool> lock_deadlock_victim_{false};
    std::atomic<uint64_t> active_owner_first_observation_ns_{0};
    std::atomic<uint64_t> active_owner_observer_count_{0};
    std::mutex lock_operation_latch_;
    std::condition_variable lock_operation_cv_;
    uint32_t lock_operation_pins_{0};
    IsolationLevel isolation_level_; // 事务的隔离级别
    std::thread::id thread_id_;      // 当前事务对应的线程id
    lsn_t begin_lsn_;                // BEGIN日志对应的lsn，用于识别尚未产生其他WAL的事务
    lsn_t prev_lsn_;                 // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
    txn_id_t txn_id_;                // 事务的ID，唯一标识符
    timestamp_t start_ts_;           // 事务的开始时间戳

    std::deque<std::unique_ptr<WriteRecord>> write_set_;  // 事务包含的所有写操作
    std::unordered_set<LockDataId> lock_set_;             // 事务申请的所有锁
    std::unordered_set<std::string> unique_key_lock_set_; // 事务持有的逻辑唯一键 reservation
    // 无索引表精确行值的 delete intent，独立于 unique-key reservation。
    std::unordered_set<std::string> logical_row_delete_intent_set_;
    std::deque<Page*> index_latch_page_set_;   // 维护事务执行过程中加锁的索引页面
    std::deque<Page*> index_deleted_page_set_; // 维护事务执行过程中删除的索引页面

    std::vector<SiLockedRowWorkspaceEntry> si_locked_row_workspace_rows_;
    std::vector<SiLockedRowWorkspaceAlias> si_locked_row_workspace_aliases_;
    size_t si_locked_row_workspace_bytes_{0};

    std::atomic<timestamp_t> read_ts_{0};
    std::shared_ptr<const ActiveWriterReadView> read_view_;
    size_t watermark_slot_{std::numeric_limits<size_t>::max()};
    /**
     * @brief 存储撤销日志。
     * 其他撤销日志/表堆将存储 (txn_id, index) 对，因此只能向此vector中追加内容或就地更新内容，而不能删除任何内容。
     */
    std::vector<UndoLog> undo_logs_;
    /** 用于访问事务级撤销日志的锁。 */
    std::mutex latch_;
};
