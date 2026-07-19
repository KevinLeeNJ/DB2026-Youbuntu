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
#include <array>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <deque>
#include <memory>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <variant>
#include <vector>

#include "common/common.h"
#include "transaction/txn_defs.h"
#include "record/rm_defs.h"

// UndoLink is now defined in record/rm_defs.h (physical undo pointer)
// UndoLog is superseded by UndoLogRecord in undo/undo_defs.h

struct UndoLog {
    /* 此日志是否为删除标记 */
    bool is_deleted_;
    /* 此撤销日志修改的字段 */
    std::vector<bool> modified_fields_;
    /* 修改后的字段 */
    std::vector<Value> tuple_;
    RmRecord* tuple_test_;
    /* 此撤销日志的时间戳 */
    timestamp_t ts_{INVALID_TS};
    /* 撤销日志的前一个版本 */
    UndoLink prev_version_{};

    // MVCC old version fields
    TupleMeta old_meta_;               // TupleMeta before the modification
    std::vector<char> old_tuple_data_; // old record data (for version chain traversal)
};

class UniqueKeyId {
public:
    static constexpr size_t INLINE_KEY_CAPACITY = 20;
    using InlineKey = std::array<char, INLINE_KEY_CAPACITY>;

    UniqueKeyId(int index_fd, const std::vector<char>& key)
        : index_fd_(index_fd), key_size_(key.size()), storage_(InlineKey{}) {
        if (key_size_ <= INLINE_KEY_CAPACITY) {
            if (key_size_ > 0) {
                std::memcpy(std::get<InlineKey>(storage_).data(), key.data(), key_size_);
            }
        } else {
            storage_ = std::make_shared<const std::string>(key.data(), key_size_);
        }
    }

    int index_fd() const noexcept {
        return index_fd_;
    }

    size_t key_size() const noexcept {
        return key_size_;
    }

    const char* key_data() const noexcept {
        if (uses_inline_storage()) {
            return std::get<InlineKey>(storage_).data();
        }
        return std::get<std::shared_ptr<const std::string>>(storage_)->data();
    }

    bool uses_inline_storage() const noexcept {
        return std::holds_alternative<InlineKey>(storage_);
    }

    bool operator==(const UniqueKeyId& other) const noexcept {
        return index_fd_ == other.index_fd_ && key_size_ == other.key_size_ &&
               (key_size_ == 0 || std::memcmp(key_data(), other.key_data(), key_size_) == 0);
    }

private:
    int index_fd_;
    size_t key_size_;
    std::variant<InlineKey, std::shared_ptr<const std::string>> storage_;
};

struct UniqueKeyIdHash {
    size_t operator()(const UniqueKeyId& key) const noexcept {
        uint64_t hash = 14695981039346656037ULL;
        const uint32_t index_fd = static_cast<uint32_t>(key.index_fd());
        for (size_t offset = 0; offset < sizeof(index_fd); ++offset) {
            hash ^= static_cast<uint8_t>(index_fd >> (offset * 8U));
            hash *= 1099511628211ULL;
        }
        for (size_t offset = 0; offset < key.key_size(); ++offset) {
            hash ^= static_cast<uint8_t>(key.key_data()[offset]);
            hash *= 1099511628211ULL;
        }
        hash ^= key.key_size();
        hash *= 1099511628211ULL;
        return static_cast<size_t>(hash);
    }
};

class Transaction {
private:
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
        prev_lsn_ = INVALID_LSN;
        thread_id_ = std::this_thread::get_id();
    }

    ~Transaction() = default;

    inline txn_id_t get_transaction_id() {
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

    inline void mark_lock_cancellation_requested() {
        lock_cancellation_requested_.store(true, std::memory_order_release);
    }

    inline bool is_lock_cancellation_requested() const {
        return lock_cancellation_requested_.load(std::memory_order_acquire);
    }

    inline lsn_t get_prev_lsn() {
        return prev_lsn_;
    }
    inline void set_prev_lsn(lsn_t prev_lsn) {
        prev_lsn_ = prev_lsn;
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

    inline std::unordered_set<UniqueKeyId, UniqueKeyIdHash>* get_unique_key_lock_set() {
        return &unique_key_lock_set_;
    }

    inline timestamp_t get_read_ts() const {
        return read_ts_;
    }
    inline void set_read_ts(timestamp_t ts) {
        read_ts_ = ts;
    }
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
    std::atomic<bool> lock_cancellation_requested_{false};
    IsolationLevel isolation_level_; // 事务的隔离级别
    std::thread::id thread_id_;      // 当前事务对应的线程id
    lsn_t prev_lsn_;                 // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
    txn_id_t txn_id_;                // 事务的ID，唯一标识符
    timestamp_t start_ts_;           // 事务的开始时间戳

    std::deque<std::unique_ptr<WriteRecord>> write_set_;                   // 事务包含的所有写操作
    std::unordered_set<LockDataId> lock_set_;                              // 事务申请的所有锁
    std::unordered_set<UniqueKeyId, UniqueKeyIdHash> unique_key_lock_set_; // 事务持有的逻辑唯一键 reservation
    std::deque<Page*> index_latch_page_set_;   // 维护事务执行过程中加锁的索引页面
    std::deque<Page*> index_deleted_page_set_; // 维护事务执行过程中删除的索引页面

    std::atomic<timestamp_t> read_ts_{0};
    size_t watermark_slot_{std::numeric_limits<size_t>::max()};
    /**
     * @brief 存储撤销日志。
     * 其他撤销日志/表堆将存储 (txn_id, index) 对，因此只能向此vector中追加内容或就地更新内容，而不能删除任何内容。
     */
    std::vector<UndoLog> undo_logs_;
    /** 用于访问事务级撤销日志的锁。 */
    std::mutex latch_;
};
