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

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <vector>

std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> TransactionManager::txn_map = {};

namespace {

constexpr uint64_t SSI_FULL_PRUNE_COMMIT_INTERVAL = 4096;
constexpr size_t SSI_RECENT_WRITE_PRUNE_THRESHOLD = 8192;
constexpr size_t SSI_EDGE_PRUNE_THRESHOLD = 8192;

// 垃圾回收节流：避免每次 commit 都全表扫描 txn_map
constexpr uint64_t GC_COMMIT_INTERVAL = 256;  // 每 N 次提交尝试一次 GC
constexpr size_t GC_TXN_MAP_THRESHOLD = 1024; // 或 txn_map 超过该阈值立即 GC

void ClearWriteSet(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    txn->get_write_set().clear();
}

void ReleaseLocks(Transaction* txn, LockManager* lock_manager) {
    if (txn == nullptr || lock_manager == nullptr) {
        return;
    }
    auto lock_set = txn->get_lock_set();
    std::vector<LockDataId> locks(lock_set->begin(), lock_set->end());
    for (const auto& lock_id : locks) {
        lock_manager->unlock(txn, lock_id);
    }
    lock_set->clear();
}

std::vector<char> MakeIndexKey(const IndexMeta& index, const char* rec_data) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
    return key;
}

void DeleteIndexEntries(SmManager* sm_manager, const TabMeta& tab, const std::string& tab_name, const RmRecord& rec,
                        const Rid& rid, Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->delete_entry(key.data(), rid, txn);
    }
}

void InsertIndexEntries(SmManager* sm_manager, const TabMeta& tab, const std::string& tab_name, const RmRecord& rec,
                        const Rid& rid, Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->insert_entry(key.data(), rid, txn, true);
    }
}

bool CompareCondition(const Condition& cond, const RmRecord& rec, const std::vector<ColMeta>& cols) {
    auto get_col_meta = [&](const TabCol& target) -> const ColMeta& {
        auto it = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (it == cols.end()) {
            throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
        }
        return *it;
    };

    const ColMeta& lhs_col_meta = get_col_meta(cond.lhs_col);
    char* lhs_data = rec.data + lhs_col_meta.offset;
    ColType lhs_type = lhs_col_meta.type;
    ColType rhs_type;
    char* rhs_data = nullptr;
    const ColMeta* rhs_col_meta = nullptr;
    if (cond.is_rhs_val) {
        rhs_type = cond.rhs_val.type;
    } else {
        rhs_col_meta = &get_col_meta(cond.rhs_col);
        rhs_data = rec.data + rhs_col_meta->offset;
        rhs_type = rhs_col_meta->type;
    }

    bool can_cast = lhs_type == rhs_type || (lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
                    (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT) ||
                    ((lhs_type == TYPE_STRING || lhs_type == TYPE_DATETIME) &&
                     (rhs_type == TYPE_STRING || rhs_type == TYPE_DATETIME));
    if (!can_cast) {
        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
    }

    switch (lhs_type) {
    case TYPE_INT:
    case TYPE_FLOAT: {
        double lhs_val = lhs_type == TYPE_INT ? static_cast<double>(*reinterpret_cast<int*>(lhs_data))
                                              : *reinterpret_cast<double*>(lhs_data);
        double rhs_val;
        if (cond.is_rhs_val) {
            rhs_val = rhs_type == TYPE_INT ? static_cast<double>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
        } else {
            rhs_val = rhs_type == TYPE_INT ? static_cast<double>(*reinterpret_cast<int*>(rhs_data))
                                           : *reinterpret_cast<double*>(rhs_data);
        }
        switch (cond.op) {
        case OP_EQ:
            return lhs_val == rhs_val;
        case OP_NE:
            return lhs_val != rhs_val;
        case OP_LT:
            return lhs_val < rhs_val;
        case OP_GT:
            return lhs_val > rhs_val;
        case OP_LE:
            return lhs_val <= rhs_val;
        case OP_GE:
            return lhs_val >= rhs_val;
        }
        break;
    }
    case TYPE_STRING:
    case TYPE_DATETIME: {
        std::string lhs_val(lhs_data, strnlen(lhs_data, lhs_col_meta.len));
        std::string rhs_val =
            cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col_meta->len));
        switch (cond.op) {
        case OP_EQ:
            return lhs_val == rhs_val;
        case OP_NE:
            return lhs_val != rhs_val;
        case OP_LT:
            return lhs_val < rhs_val;
        case OP_GT:
            return lhs_val > rhs_val;
        case OP_LE:
            return lhs_val <= rhs_val;
        case OP_GE:
            return lhs_val >= rhs_val;
        }
    }
    }
    return false;
}

bool MatchesConditions(const std::vector<Condition>& conds, const RmRecord& rec, const std::vector<ColMeta>& cols) {
    for (const auto& cond : conds) {
        if (!CompareCondition(cond, rec, cols)) {
            return false;
        }
    }
    return true;
}

bool SsiTxnCanConflictWithWriter(Transaction* reader_txn, Transaction* writer_txn) {
    if (reader_txn == nullptr || writer_txn == nullptr) {
        return false;
    }
    if (reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    if (reader_txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (reader_txn->get_state() == TransactionState::GROWING) {
        return true;
    }
    if (reader_txn->get_state() != TransactionState::COMMITTED) {
        return false;
    }
    timestamp_t reader_commit_ts = reader_txn->get_commit_ts();
    return reader_commit_ts != INVALID_TS && reader_commit_ts > writer_txn->get_start_ts();
}

void WriteBeginLog(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || log_manager == nullptr) {
        return;
    }
    BeginLogRecord record(txn->get_transaction_id());
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(lsn);
}

void WriteCommitLog(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || log_manager == nullptr) {
        return;
    }
    CommitLogRecord record(txn->get_transaction_id());
    record.prev_lsn_ = txn->get_prev_lsn();
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(lsn);
    log_manager->flush_log_to_disk();
}

void WriteAbortLog(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || log_manager == nullptr) {
        return;
    }
    AbortLogRecord record(txn->get_transaction_id());
    record.prev_lsn_ = txn->get_prev_lsn();
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(lsn);
    log_manager->flush_log_to_disk();
}

std::optional<UndoLog> GetCurrentUndoLog(TransactionManager* txn_mgr, RmFileHandle* fh, const Rid& rid) {
    TupleMeta meta = fh->get_tuple_meta(rid);
    if (!meta.version_chain_head_.IsValid()) {
        return std::nullopt;
    }
    try {
        return txn_mgr->GetUndoLog(meta.version_chain_head_);
    } catch (const InternalError&) {
        return std::nullopt;
    }
}

TupleMeta FallbackCommittedMeta() {
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    return meta;
}

void UndoWriteRecord(TransactionManager* txn_mgr, SmManager* sm_manager, WriteRecord* write_record, Transaction* txn) {
    const std::string tab_name = write_record->GetTableName();
    auto& tab = sm_manager->db_.get_table(tab_name);
    auto* fh = sm_manager->fhs_.at(tab_name).get();
    Rid rid = write_record->GetRid();

    switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE: {
        if (fh->is_record(rid)) {
            auto rec = fh->get_record(rid, nullptr);
            DeleteIndexEntries(sm_manager, tab, tab_name, *rec, rid, txn);
            fh->delete_record(rid, nullptr);
        }
        break;
    }
    case WType::DELETE_TUPLE: {
        RmRecord old_rec = write_record->GetRecord();
        auto undo = GetCurrentUndoLog(txn_mgr, fh, rid);
        if (fh->is_record(rid)) {
            fh->update_record(rid, old_rec.data, nullptr);
        } else {
            fh->insert_record(rid, old_rec.data);
        }
        fh->set_tuple_meta(rid, undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta());
        InsertIndexEntries(sm_manager, tab, tab_name, old_rec, rid, txn);
        break;
    }
    case WType::UPDATE_TUPLE: {
        RmRecord old_rec = write_record->GetRecord();
        std::unique_ptr<RmRecord> current_rec;
        if (fh->is_record(rid)) {
            current_rec = fh->get_record(rid, nullptr);
            for (const auto& index : tab.indexes) {
                auto current_key = MakeIndexKey(index, current_rec->data);
                auto old_key = MakeIndexKey(index, old_rec.data);
                if (current_key == old_key) {
                    continue;
                }
                auto ih =
                    sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                ih->delete_entry(current_key.data(), rid, txn);
            }
        }
        auto undo = GetCurrentUndoLog(txn_mgr, fh, rid);
        fh->update_record(rid, old_rec.data, nullptr);
        fh->set_tuple_meta(rid, undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta());
        if (current_rec != nullptr) {
            for (const auto& index : tab.indexes) {
                auto current_key = MakeIndexKey(index, current_rec->data);
                auto old_key = MakeIndexKey(index, old_rec.data);
                if (current_key == old_key) {
                    continue;
                }
                auto ih =
                    sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                ih->insert_entry(old_key.data(), rid, txn, true);
            }
        }
        break;
    }
    }
}

} // namespace

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager, IsolationLevel isolation_level) {
    std::unique_ptr<Transaction> created;
    if (txn == nullptr) {
        txn_id_t txn_id = next_txn_id_.fetch_add(1);
        created = std::make_unique<Transaction>(txn_id, isolation_level);
        txn = created.get();
    }

    {
        std::unique_lock<std::mutex> checkpoint_lock(checkpoint_latch_);
        checkpoint_cv_.wait(checkpoint_lock, [&] { return !checkpoint_blocking_new_txns_; });
        active_txn_ids_.insert(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
    }

    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_.fetch_add(1));
    txn->set_read_ts(last_commit_ts_.load());
    // 用读时间戳维护水位线：RC 下每条语句的 read_ts 可能大于 start_ts，
    // 水位线必须反映当前真实 read_ts 才能安全驱动垃圾回收。
    running_txns_.AddTxn(txn->get_read_ts());
    WriteBeginLog(txn, log_manager);

    std::unique_lock<std::mutex> lock(latch_);
    if (created) {
        txn_map[txn->get_transaction_id()] = std::move(created);
    }
    if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE) {
        active_serializable_txns_.insert(txn->get_transaction_id());
    }
    return txn;
}

void TransactionManager::BeginStatement(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED) {
        // 刷新 read_ts 时同步更新水位线，确保水位线始终追踪当前真实读时间戳
        timestamp_t old_read_ts = txn->get_read_ts();
        timestamp_t new_read_ts = last_commit_ts_.load();
        if (new_read_ts != old_read_ts) {
            running_txns_.RemoveTxn(old_read_ts);
            txn->set_read_ts(new_read_ts);
            running_txns_.AddTxn(new_read_ts);
        }
    }
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
        {
            std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
            active_txn_ids_.erase(txn->get_transaction_id());
            active_txn_count_ = static_cast<int>(active_txn_ids_.size());
        }
        checkpoint_cv_.notify_all();
        RetireTransactionIfSafe(txn);
        return;
    }

    WriteCommitLog(txn, log_manager);

    // Allocate commit_ts (monotonic)
    timestamp_t commit_ts = next_timestamp_.fetch_add(1);
    txn->commit_ts_ = commit_ts;
    last_commit_ts_ = commit_ts;

    // 先更新提交时间戳，再用读时间戳从水位线移除，保证水位线计算正确
    running_txns_.UpdateCommitTs(commit_ts);
    running_txns_.RemoveTxn(txn->get_read_ts());

    // Mark all modified slots as committed
    sm_manager_->mark_slots_committed(*txn, commit_ts);

    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    txn->set_state(TransactionState::COMMITTED);
    CleanupTxnSsiState(txn->get_transaction_id());
    bool run_full_prune = false;
    {
        std::unique_lock<std::mutex> lock(latch_);
        ++commits_since_full_ssi_prune_;
        run_full_prune = ShouldRunFullSsiPruneUnlocked();
    }
    if (run_full_prune) {
        PruneSsiState();
    }
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_txn_ids_.erase(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
    }
    checkpoint_cv_.notify_all();
    RetireTransactionIfSafe(txn);
    MaybeRunGarbageCollection();
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::ABORTED || txn->get_state() == TransactionState::COMMITTED) {
        {
            std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
            active_txn_ids_.erase(txn->get_transaction_id());
            active_txn_count_ = static_cast<int>(active_txn_ids_.size());
        }
        checkpoint_cv_.notify_all();
        RetireTransactionIfSafe(txn);
        return;
    }

    auto& write_set = txn->get_write_set();
    for (auto it = write_set.rbegin(); it != write_set.rend(); ++it) {
        UndoWriteRecord(this, sm_manager_, it->get(), txn);
    }
    WriteAbortLog(txn, log_manager);
    running_txns_.RemoveTxn(txn->get_read_ts());
    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    txn->set_state(TransactionState::ABORTED);
    CleanupTxnSsiState(txn->get_transaction_id());
    bool run_full_prune = false;
    {
        std::unique_lock<std::mutex> lock(latch_);
        run_full_prune = ShouldRunFullSsiPruneUnlocked();
    }
    if (run_full_prune) {
        PruneSsiState();
    }
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_txn_ids_.erase(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
    }
    checkpoint_cv_.notify_all();
    RetireTransactionIfSafe(txn);
    MaybeRunGarbageCollection();
}

void TransactionManager::block_new_transactions_for_checkpoint() {
    std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
    checkpoint_blocking_new_txns_ = true;
}

void TransactionManager::unblock_new_transactions_after_checkpoint() {
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        checkpoint_blocking_new_txns_ = false;
    }
    checkpoint_cv_.notify_all();
}

std::unordered_map<txn_id_t, lsn_t> TransactionManager::wait_active_transactions_drained_for_checkpoint() {
    std::unique_lock<std::mutex> checkpoint_lock(checkpoint_latch_);
    checkpoint_cv_.wait(checkpoint_lock, [&] { return active_txn_count_ == 0; });
    return {};
}

std::unordered_map<txn_id_t, lsn_t> TransactionManager::get_active_txn_lsn_snapshot() {
    std::vector<txn_id_t> active_ids;
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_ids.assign(active_txn_ids_.begin(), active_txn_ids_.end());
    }

    std::unordered_map<txn_id_t, lsn_t> snapshot;
    std::unique_lock<std::mutex> lock(latch_);
    for (txn_id_t txn_id : active_ids) {
        auto it = txn_map.find(txn_id);
        if (it != txn_map.end() && it->second != nullptr) {
            snapshot.emplace(txn_id, it->second->get_prev_lsn());
        }
    }
    return snapshot;
}

UndoLog TransactionManager::GetUndoLog(UndoLink link) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map.find(link.undo_txn_id_);
    if (it == txn_map.end()) {
        throw InternalError("GetUndoLog: transaction not found");
    }
    return it->second->GetUndoLog(link.undo_slot_offset_);
}

std::optional<std::pair<TupleMeta, std::vector<char>>>
TransactionManager::FindVisibleVersion(const TupleMeta& start_meta, timestamp_t read_ts, txn_id_t self_txn_id) {
    const TupleMeta* cur = &start_meta;
    // Limit traversal depth to prevent infinite loops
    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH; ++depth) {
        // Own uncommitted write is always visible
        if (!cur->is_committed_ && cur->writer_txn_id_ == self_txn_id) {
            if (cur->is_deleted_)
                return std::nullopt;
            return std::make_pair(*cur, std::vector<char>{});
        }
        // Other's uncommitted write — dirty, follow chain
        if (!cur->is_committed_) {
            if (cur->version_chain_head_.IsValid()) {
                UndoLog log = GetUndoLog(cur->version_chain_head_);
                cur = &log.old_meta_;
                continue;
            }
            return std::nullopt;
        }
        // Committed and not deleted — check read_ts
        if (!cur->is_deleted_ && cur->commit_ts_ <= read_ts) {
            return std::make_pair(*cur, std::vector<char>{});
        }
        // Not visible — follow chain
        if (cur->version_chain_head_.IsValid()) {
            UndoLog log = GetUndoLog(cur->version_chain_head_);
            cur = &log.old_meta_;
            continue;
        }
        return std::nullopt;
    }
    return std::nullopt;
}

// ---- SSI Helper ----

bool TransactionManager::TupleMatches(const std::string& tab_name, const std::vector<Condition>& conds,
                                      const RmRecord& rec) {
    if (conds.empty()) {
        return true;
    }
    const auto& tab = sm_manager_->db_.get_table(tab_name);
    auto get_col_meta = [&](const TabCol& target) -> const ColMeta& {
        auto iter = std::find_if(tab.cols.begin(), tab.cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (iter == tab.cols.end()) {
            throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
        }
        return *iter;
    };
    for (const auto& cond : conds) {
        const auto& lhs_col = get_col_meta(cond.lhs_col);
        const char* lhs_data = rec.data + lhs_col.offset;
        ColType rhs_type;
        const char* rhs_data = nullptr;
        ColMeta rhs_col{};
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
        } else {
            rhs_col = get_col_meta(cond.rhs_col);
            rhs_type = rhs_col.type;
            rhs_data = rec.data + rhs_col.offset;
        }
        if (!((lhs_col.type == rhs_type) || (lhs_col.type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
              (lhs_col.type == TYPE_FLOAT && rhs_type == TYPE_INT) ||
              ((lhs_col.type == TYPE_STRING || lhs_col.type == TYPE_DATETIME) &&
               (rhs_type == TYPE_STRING || rhs_type == TYPE_DATETIME)))) {
            throw IncompatibleTypeError(coltype2str(lhs_col.type), coltype2str(rhs_type));
        }
        int cmp = 0;
        if (lhs_col.type == TYPE_STRING || lhs_col.type == TYPE_DATETIME) {
            std::string lhs(lhs_data, strnlen(lhs_data, lhs_col.len));
            std::string rhs =
                cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col.len));
            cmp = lhs.compare(rhs);
        } else {
            double lhs = lhs_col.type == TYPE_INT ? static_cast<double>(*reinterpret_cast<const int*>(lhs_data))
                                                  : *reinterpret_cast<const double*>(lhs_data);
            double rhs;
            if (cond.is_rhs_val) {
                rhs = rhs_type == TYPE_INT ? static_cast<double>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
            } else {
                rhs = rhs_type == TYPE_INT ? static_cast<double>(*reinterpret_cast<const int*>(rhs_data))
                                           : *reinterpret_cast<const double*>(rhs_data);
            }
            cmp = lhs == rhs ? 0 : (lhs < rhs ? -1 : 1);
        }
        bool ok = false;
        switch (cond.op) {
        case OP_EQ:
            ok = cmp == 0;
            break;
        case OP_NE:
            ok = cmp != 0;
            break;
        case OP_LT:
            ok = cmp < 0;
            break;
        case OP_GT:
            ok = cmp > 0;
            break;
        case OP_LE:
            ok = cmp <= 0;
            break;
        case OP_GE:
            ok = cmp >= 0;
            break;
        }
        if (!ok) {
            return false;
        }
    }
    return true;
}

TransactionManager::SsiRecordKey TransactionManager::MakeSsiRecordKey(const std::string& tab_name,
                                                                      const Rid& rid) const {
    return SsiRecordKey{tab_name, rid.page_no, rid.slot_no};
}

bool TransactionManager::HasActiveSsiReadersForWriteUnlocked(const std::string& tab_name,
                                                             const SsiRecordKey& key) const {
    auto record_it = active_record_readers_.find(key);
    if (record_it != active_record_readers_.end() && !record_it->second.empty()) {
        return true;
    }
    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    return predicate_it != active_predicate_readers_by_table_.end() && !predicate_it->second.empty();
}

bool TransactionManager::HasOtherActivePredicateReadersUnlocked(const std::string& tab_name, txn_id_t writer) const {
    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    if (predicate_it == active_predicate_readers_by_table_.end()) {
        return false;
    }
    for (txn_id_t reader : predicate_it->second) {
        if (reader != writer) {
            return true;
        }
    }
    return false;
}

bool TransactionManager::HasOtherActiveSerializableTxnUnlocked(txn_id_t writer) const {
    for (txn_id_t txn_id : active_serializable_txns_) {
        if (txn_id == writer) {
            continue;
        }
        auto txn_it = txn_map.find(txn_id);
        if (txn_it == txn_map.end() || txn_it->second == nullptr) {
            continue;
        }
        auto* txn = txn_it->second.get();
        if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
            txn->get_state() != TransactionState::COMMITTED && txn->get_state() != TransactionState::ABORTED) {
            return true;
        }
    }
    return false;
}

size_t TransactionManager::RecentWriteCountUnlocked() const {
    size_t total = ssi_writes_.size();
    for (const auto& [_, writes] : recent_writes_by_table_) {
        total += writes.size();
    }
    return total;
}

size_t TransactionManager::RwEdgeCountUnlocked() const {
    size_t total = 0;
    for (const auto& [_, outs] : rw_edges_) {
        total += outs.size();
    }
    return total;
}

bool TransactionManager::HasActiveSerializableOverlapUnlocked(Transaction* txn) {
    if (txn == nullptr) {
        return false;
    }
    txn_id_t txn_id = txn->get_transaction_id();
    for (txn_id_t other_id : active_serializable_txns_) {
        if (other_id == txn_id) {
            continue;
        }
        auto other_it = txn_map.find(other_id);
        auto* other = other_it == txn_map.end() ? nullptr : other_it->second.get();
        if (other == nullptr) {
            continue;
        }
        if (other->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
            continue;
        }
        if (other->get_state() == TransactionState::COMMITTED || other->get_state() == TransactionState::ABORTED) {
            continue;
        }
        if (TransactionsOverlap(txn, other)) {
            return true;
        }
    }
    return false;
}

void TransactionManager::CleanupTxnReadIndexesUnlocked(txn_id_t txn_id) {
    auto key_it = txn_record_read_keys_.find(txn_id);
    if (key_it != txn_record_read_keys_.end()) {
        for (const auto& key : key_it->second) {
            auto readers_it = active_record_readers_.find(key);
            if (readers_it == active_record_readers_.end()) {
                continue;
            }
            readers_it->second.erase(txn_id);
            if (readers_it->second.empty()) {
                active_record_readers_.erase(readers_it);
            }
        }
        txn_record_read_keys_.erase(key_it);
    }

    auto table_it = txn_predicate_read_tables_.find(txn_id);
    if (table_it != txn_predicate_read_tables_.end()) {
        for (const auto& table : table_it->second) {
            auto readers_it = active_predicate_readers_by_table_.find(table);
            if (readers_it == active_predicate_readers_by_table_.end()) {
                continue;
            }
            readers_it->second.erase(txn_id);
            if (readers_it->second.empty()) {
                active_predicate_readers_by_table_.erase(readers_it);
            }
        }
        txn_predicate_read_tables_.erase(table_it);
    }

    ssi_record_reads_.erase(std::remove_if(ssi_record_reads_.begin(), ssi_record_reads_.end(),
                                           [&](const auto& read) { return read.txn_id_ == txn_id; }),
                            ssi_record_reads_.end());

    auto txn_it = txn_map.find(txn_id);
    if (txn_it != txn_map.end() && txn_it->second != nullptr) {
        txn_it->second->read_rids_.clear();
        txn_it->second->predicate_reads_.clear();
    }
}

void TransactionManager::CleanupTxnRwEdgesUnlocked(txn_id_t txn_id) {
    auto txn_it = txn_map.find(txn_id);
    if (txn_it != txn_map.end() && txn_it->second != nullptr) {
        auto* txn = txn_it->second.get();
        for (auto in_id : txn->in_rw_) {
            auto in_it = txn_map.find(in_id);
            if (in_it != txn_map.end() && in_it->second != nullptr) {
                in_it->second->out_rw_.erase(txn_id);
            }
        }
        for (auto out_id : txn->out_rw_) {
            auto out_it = txn_map.find(out_id);
            if (out_it != txn_map.end() && out_it->second != nullptr) {
                out_it->second->in_rw_.erase(txn_id);
            }
        }
        txn->in_rw_.clear();
        txn->out_rw_.clear();
    }

    rw_edges_.erase(txn_id);
    for (auto it = rw_edges_.begin(); it != rw_edges_.end();) {
        it->second.erase(txn_id);
        if (it->second.empty()) {
            it = rw_edges_.erase(it);
        } else {
            ++it;
        }
    }
}

void TransactionManager::CleanupTxnRecentWritesUnlocked(txn_id_t txn_id) {
    ssi_writes_.erase(std::remove_if(ssi_writes_.begin(), ssi_writes_.end(),
                                     [&](const auto& write) { return write.txn_id_ == txn_id; }),
                      ssi_writes_.end());

    for (auto it = recent_writes_by_table_.begin(); it != recent_writes_by_table_.end();) {
        auto& writes = it->second;
        writes.erase(
            std::remove_if(writes.begin(), writes.end(), [&](const auto& write) { return write.txn_id_ == txn_id; }),
            writes.end());
        if (writes.empty()) {
            it = recent_writes_by_table_.erase(it);
        } else {
            ++it;
        }
    }
}

void TransactionManager::CleanupTxnSsiState(txn_id_t txn_id) {
    std::unique_lock<std::mutex> lock(latch_);
    auto txn_it = txn_map.find(txn_id);
    if (txn_it == txn_map.end() || txn_it->second == nullptr) {
        active_serializable_txns_.erase(txn_id);
        CleanupTxnReadIndexesUnlocked(txn_id);
        CleanupTxnRwEdgesUnlocked(txn_id);
        CleanupTxnRecentWritesUnlocked(txn_id);
        return;
    }

    auto* txn = txn_it->second.get();
    const bool aborted = txn->get_state() == TransactionState::ABORTED;
    const bool committed = txn->get_state() == TransactionState::COMMITTED;
    if (!aborted && !committed) {
        return;
    }

    if (committed && TransactionHasRetainedSsiStateUnlocked(txn_id) && HasActiveSerializableOverlapUnlocked(txn)) {
        return;
    }

    active_serializable_txns_.erase(txn_id);
    CleanupTxnReadIndexesUnlocked(txn_id);
    CleanupTxnRwEdgesUnlocked(txn_id);
    CleanupTxnRecentWritesUnlocked(txn_id);
}

bool TransactionManager::ShouldRunFullSsiPruneUnlocked() const {
    const bool has_ssi_state = !recent_writes_by_table_.empty() || !ssi_writes_.empty() ||
                               !active_record_readers_.empty() || !active_predicate_readers_by_table_.empty() ||
                               !rw_edges_.empty() || !active_serializable_txns_.empty();
    if (!has_ssi_state) {
        return false;
    }
    if (commits_since_full_ssi_prune_ >= SSI_FULL_PRUNE_COMMIT_INTERVAL && has_ssi_state) {
        return true;
    }
    if (RecentWriteCountUnlocked() >= SSI_RECENT_WRITE_PRUNE_THRESHOLD) {
        return true;
    }
    if (RwEdgeCountUnlocked() >= SSI_EDGE_PRUNE_THRESHOLD) {
        return true;
    }
    return false;
}

bool TransactionManager::TransactionHasSsiStateUnlocked(txn_id_t txn_id) const {
    if (active_serializable_txns_.count(txn_id) > 0) {
        return true;
    }
    return TransactionHasRetainedSsiStateUnlocked(txn_id);
}

bool TransactionManager::TransactionHasRetainedSsiStateUnlocked(txn_id_t txn_id) const {
    if (txn_record_read_keys_.count(txn_id) > 0 || txn_predicate_read_tables_.count(txn_id) > 0) {
        return true;
    }
    for (const auto& [_, readers] : active_record_readers_) {
        if (readers.count(txn_id) > 0) {
            return true;
        }
    }
    for (const auto& [_, readers] : active_predicate_readers_by_table_) {
        if (readers.count(txn_id) > 0) {
            return true;
        }
    }
    for (const auto& write : ssi_writes_) {
        if (write.txn_id_ == txn_id) {
            return true;
        }
    }
    for (const auto& [_, writes] : recent_writes_by_table_) {
        for (const auto& write : writes) {
            if (write.txn_id_ == txn_id) {
                return true;
            }
        }
    }
    auto edge_it = rw_edges_.find(txn_id);
    if (edge_it != rw_edges_.end() && !edge_it->second.empty()) {
        return true;
    }
    for (const auto& [_, outs] : rw_edges_) {
        if (outs.count(txn_id) > 0) {
            return true;
        }
    }
    auto txn_it = txn_map.find(txn_id);
    if (txn_it != txn_map.end() && txn_it->second != nullptr) {
        return !txn_it->second->in_rw_.empty() || !txn_it->second->out_rw_.empty() ||
               !txn_it->second->read_rids_.empty() || !txn_it->second->predicate_reads_.empty();
    }
    return false;
}

bool TransactionManager::TransactionHasUndoNeededByVersionChain(Transaction* txn) const {
    return txn != nullptr && txn->GetUndoLogNum() > 0;
}

bool TransactionManager::CanRetireTransactionUnlocked(Transaction* txn) const {
    if (txn == nullptr) {
        return false;
    }
    TransactionState state = txn->get_state();
    if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
        return false;
    }
    txn_id_t txn_id = txn->get_transaction_id();
    if (active_txn_ids_.count(txn_id) > 0) {
        return false;
    }
    if (TransactionHasSsiStateUnlocked(txn_id)) {
        return false;
    }
    if (TransactionHasUndoNeededByVersionChain(txn)) {
        return false;
    }
    auto* non_const_this = const_cast<TransactionManager*>(this);
    return !non_const_this->HasActiveSerializableOverlapUnlocked(txn);
}

void TransactionManager::RetireTransactionIfSafe(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
    std::unique_lock<std::mutex> lock(latch_);
    if (!CanRetireTransactionUnlocked(txn)) {
        return;
    }
    auto it = txn_map.find(txn->get_transaction_id());
    if (it != txn_map.end() && it->second.get() == txn) {
        txn_map.erase(it);
    }
}

timestamp_t TransactionManager::GetWatermark() {
    return running_txns_.GetWatermark();
}

void TransactionManager::MaybeRunGarbageCollection() {
    bool should_run = false;
    {
        std::unique_lock<std::mutex> lock(latch_);
        ++commits_since_gc_;
        if (commits_since_gc_ >= GC_COMMIT_INTERVAL || txn_map.size() >= GC_TXN_MAP_THRESHOLD) {
            should_run = true;
            commits_since_gc_ = 0;
        }
    }
    if (should_run) {
        GarbageCollection();
    }
}

void TransactionManager::GarbageCollection() {
    // 安全条件：水位线是所有活跃事务读时间戳的最小值。只有 commit_ts（已提交）
    // 或 start_ts（已中止）严格小于水位线的事务，其 undo log 才不会被任何活跃
    // 事务的版本链遍历访问到，因而可安全从 txn_map 回收。
    // 与 GetUndoLog 互斥：二者都持 latch_。
    timestamp_t watermark = running_txns_.GetWatermark();
    std::vector<txn_id_t> to_erase;
    {
        std::unique_lock<std::mutex> lock(latch_);
        for (auto it = txn_map.begin(); it != txn_map.end(); ++it) {
            Transaction* txn = it->second.get();
            if (txn == nullptr) {
                to_erase.push_back(it->first);
                continue;
            }
            TransactionState state = txn->get_state();
            if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
                continue;
            }
            if (active_txn_ids_.count(it->first) > 0) {
                continue;
            }
            if (TransactionHasRetainedSsiStateUnlocked(it->first)) {
                continue;
            }
            if (state == TransactionState::COMMITTED) {
                if (txn->get_commit_ts() == INVALID_TS || txn->get_commit_ts() >= watermark) {
                    continue;
                }
            } else {
                // 已中止事务：commit_ts 无效，保守用 start_ts 判断。
                // 任何 read_ts >= start_ts 的活跃事务都已无法看到该事务的版本。
                if (txn->get_start_ts() >= watermark) {
                    continue;
                }
            }
            // 仅在无 SSI 活跃重叠时回收，避免误删仍被 SSI 依赖追踪的事务
            auto* non_const_this = const_cast<TransactionManager*>(this);
            if (non_const_this->HasActiveSerializableOverlapUnlocked(txn)) {
                continue;
            }
            to_erase.push_back(it->first);
        }
        for (txn_id_t txn_id : to_erase) {
            txn_map.erase(txn_id);
        }
    }
    // 回收 SmManager 侧随写操作单调增长的历史索引键/删除候选（需访问 tuple meta，
    // 不在 latch_ 下进行，避免与缓冲池操作死锁）
    if (!to_erase.empty()) {
        sm_manager_->prune_version_history(watermark);
    }
}

bool TransactionManager::TransactionsOverlap(Transaction* lhs, Transaction* rhs) {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    timestamp_t lhs_end = lhs->get_commit_ts();
    timestamp_t rhs_end = rhs->get_commit_ts();
    if (lhs->get_state() != TransactionState::COMMITTED) {
        lhs_end = std::numeric_limits<timestamp_t>::max();
    }
    if (rhs->get_state() != TransactionState::COMMITTED) {
        rhs_end = std::numeric_limits<timestamp_t>::max();
    }
    return lhs->get_start_ts() < rhs_end && rhs->get_start_ts() < lhs_end;
}

bool TransactionManager::CommittedBefore(txn_id_t lhs, txn_id_t rhs) {
    std::unique_lock<std::mutex> lock(latch_);
    return CommittedBeforeUnlocked(lhs, rhs);
}

bool TransactionManager::CommittedBeforeUnlocked(txn_id_t lhs, txn_id_t rhs) {
    auto lhs_it = txn_map.find(lhs);
    auto rhs_it = txn_map.find(rhs);
    if (lhs_it == txn_map.end() || rhs_it == txn_map.end()) {
        return false;
    }
    auto* lhs_txn = lhs_it->second.get();
    auto* rhs_txn = rhs_it->second.get();
    if (lhs_txn->get_state() != TransactionState::COMMITTED) {
        return false;
    }
    if (rhs_txn->get_state() != TransactionState::COMMITTED) {
        return true;
    }
    return lhs_txn->get_commit_ts() < rhs_txn->get_commit_ts();
}

bool TransactionManager::HasDangerousStructure(txn_id_t current_txn) {
    // Danger detection uses the centralized rw_edges_ map. It needs to be
    // consistent with txn_map state (e.g., not skip aborted txns). Hold both
    // ssi_latch_ and latch_ if needed, but since all edge mutations also hold
    // latch_, we only hold latch_ here.
    std::unique_lock<std::mutex> lock(latch_);
    return HasDangerousStructureUnlocked(current_txn);
}

bool TransactionManager::HasDangerousStructureUnlocked(txn_id_t current_txn) {
    for (const auto& [tin, pivots] : rw_edges_) {
        auto tin_it = txn_map.find(tin);
        if (tin_it == txn_map.end() || tin_it->second->get_state() == TransactionState::ABORTED) {
            continue;
        }
        for (txn_id_t pivot : pivots) {
            auto pivot_it = txn_map.find(pivot);
            if (pivot_it == txn_map.end() || pivot_it->second->get_state() == TransactionState::ABORTED) {
                continue;
            }
            auto pivot_out_it = rw_edges_.find(pivot);
            if (pivot_out_it == rw_edges_.end()) {
                continue;
            }
            for (txn_id_t tout : pivot_out_it->second) {
                auto tout_it = txn_map.find(tout);
                if (tout_it == txn_map.end() || tout_it->second->get_state() == TransactionState::ABORTED) {
                    continue;
                }
                // Only consider structures that involve current_txn
                if (tin != current_txn && pivot != current_txn && tout != current_txn) {
                    continue;
                }
                // 两个连续的 rw 反依赖，且执行区间存在重叠
                if (!TransactionsOverlap(tin_it->second.get(), pivot_it->second.get()) ||
                    !TransactionsOverlap(pivot_it->second.get(), tout_it->second.get())) {
                    continue;
                }
                // Tin = Tout 或 Tout 的提交顺序早于 Tin
                if (tin == tout || CommittedBeforeUnlocked(tout, tin)) {
                    return true;
                }
            }
        }
    }
    return false;
}

// ---- SSI Dependency Tracking ----

size_t TransactionManager::DebugSsiWriteCount() {
    std::unique_lock<std::mutex> lock(latch_);
    return RecentWriteCountUnlocked();
}

size_t TransactionManager::DebugActiveRecordReaderKeyCount() {
    std::unique_lock<std::mutex> lock(latch_);
    return active_record_readers_.size();
}

size_t TransactionManager::DebugActivePredicateTableCount() {
    std::unique_lock<std::mutex> lock(latch_);
    return active_predicate_readers_by_table_.size();
}

size_t TransactionManager::DebugTxnMapSize() {
    std::unique_lock<std::mutex> lock(latch_);
    return txn_map.size();
}

bool TransactionManager::AddRwEdge(txn_id_t from, txn_id_t to) {
    std::unique_lock<std::mutex> lock(latch_);
    return AddRwEdgeInternal(from, to, from);
}

bool TransactionManager::AddRwEdgeInternal(txn_id_t reader, txn_id_t writer, txn_id_t current_txn) {
    if (reader == writer) {
        return false;
    }
    auto reader_it = txn_map.find(reader);
    auto writer_it = txn_map.find(writer);
    if (reader_it == txn_map.end() || writer_it == txn_map.end()) {
        return false;
    }
    auto* reader_txn = reader_it->second.get();
    auto* writer_txn = writer_it->second.get();
    if (reader_txn->get_state() == TransactionState::ABORTED || writer_txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (!TransactionsOverlap(reader_txn, writer_txn)) {
        return false;
    }
    // Already exists in centralized rw_edges_?
    auto& outs = rw_edges_[reader];
    if (outs.find(writer) != outs.end()) {
        return false;
    }
    outs.insert(writer);
    // Also update per-txn state for CleanupSsiState
    reader_txn->out_rw_.insert(writer);
    writer_txn->in_rw_.insert(reader);
    // Check for SSI danger structure
    return HasDangerousStructureUnlocked(current_txn);
}

void TransactionManager::RecordRead(txn_id_t reader, const std::string& tab_name, const Rid& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map.find(reader);
    if (it == txn_map.end() || it->second == nullptr)
        return;
    if (it->second->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return;
    // Per-txn record read tracking (for write-side CheckWriteAgainstReaders)
    std::string rid_key = tab_name + ":" + std::to_string(rid.page_no) + ":" + std::to_string(rid.slot_no);
    it->second->read_rids_.insert(rid_key);
    // Also store in centralized read list for PruneSsiState
    ssi_record_reads_.push_back({reader, tab_name, rid});
    SsiRecordKey key = MakeSsiRecordKey(tab_name, rid);
    active_record_readers_[key].insert(reader);
    txn_record_read_keys_[reader].push_back(key);
    active_serializable_txns_.insert(reader);
}

bool TransactionManager::RecordPredicateRead(Transaction* txn, const std::string& tab_name,
                                             const std::vector<Condition>& conds) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    bool dangerous = false;
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn->predicate_reads_.push_back(Transaction::PredicateRead{tab_name, conds});
        txn_id_t reader_id = txn->get_transaction_id();
        active_predicate_readers_by_table_[tab_name].insert(reader_id);
        txn_predicate_read_tables_[reader_id].push_back(tab_name);
        active_serializable_txns_.insert(reader_id);

        auto writes_it = recent_writes_by_table_.find(tab_name);
        if (writes_it == recent_writes_by_table_.end()) {
            return false;
        }
        for (const auto& write : writes_it->second) {
            if (write.txn_id_ == reader_id || write.tab_name_ != tab_name) {
                continue;
            }
            auto writer_it = txn_map.find(write.txn_id_);
            if (writer_it == txn_map.end()) {
                continue;
            }
            auto* writer_txn = writer_it->second.get();
            TransactionState writer_state = writer_txn->get_state();
            timestamp_t writer_commit_ts = writer_txn->get_commit_ts();
            if (writer_state == TransactionState::ABORTED) {
                continue;
            }
            bool invisible = writer_state != TransactionState::COMMITTED || writer_commit_ts > txn->get_start_ts();
            if (!invisible) {
                continue;
            }
            bool matches = (write.old_rec_.has_value() && TupleMatches(tab_name, conds, *write.old_rec_)) ||
                           (write.new_rec_.has_value() && TupleMatches(tab_name, conds, *write.new_rec_));
            if (matches && AddRwEdgeInternal(reader_id, write.txn_id_, reader_id)) {
                dangerous = true;
                break;
            }
        }
    }
    if (dangerous) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
    }
    return false;
}

bool TransactionManager::CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name) {
    std::unique_lock<std::mutex> lock(latch_);
    auto writer_it = txn_map.find(writer);
    auto* writer_txn = (writer_it != txn_map.end()) ? writer_it->second.get() : nullptr;
    if (writer_txn == nullptr || writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    SsiRecordKey key = MakeSsiRecordKey(tab_name, rid);
    if (!HasActiveSsiReadersForWriteUnlocked(tab_name, key)) {
        return false;
    }

    bool danger = false;
    auto record_it = active_record_readers_.find(key);
    if (record_it != active_record_readers_.end()) {
        std::vector<txn_id_t> readers(record_it->second.begin(), record_it->second.end());
        for (txn_id_t tid : readers) {
            if (tid == writer) {
                continue;
            }
            auto reader_it = txn_map.find(tid);
            auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
            if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
                continue;
            }
            if (AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }

    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    if (predicate_it == active_predicate_readers_by_table_.end()) {
        return danger;
    }
    std::vector<txn_id_t> predicate_readers(predicate_it->second.begin(), predicate_it->second.end());
    for (txn_id_t tid : predicate_readers) {
        if (tid == writer) {
            continue;
        }
        auto reader_it = txn_map.find(tid);
        auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
        if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
            continue;
        }
        if (AddRwEdgeInternal(tid, writer, writer)) {
            danger = true;
        }
    }
    return danger;
}

bool TransactionManager::CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name,
                                                  const std::optional<RmRecord>& old_rec,
                                                  const std::optional<RmRecord>& new_rec,
                                                  const std::vector<ColMeta>& cols) {
    (void)cols;
    std::unique_lock<std::mutex> lock(latch_);
    auto writer_it = txn_map.find(writer);
    auto* writer_txn = (writer_it != txn_map.end()) ? writer_it->second.get() : nullptr;
    if (writer_txn == nullptr)
        return false;
    if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        // For non-SER writers: don't store writes, don't check
        return false;
    }
    SsiRecordKey key = MakeSsiRecordKey(tab_name, rid);
    const bool has_other_active_serializable = HasOtherActiveSerializableTxnUnlocked(writer);
    if (!HasActiveSsiReadersForWriteUnlocked(tab_name, key)) {
        if (has_other_active_serializable) {
            recent_writes_by_table_[tab_name].push_back({writer, tab_name, rid, old_rec, new_rec});
        }
        return false;
    }

    bool danger = false;
    auto record_it = active_record_readers_.find(key);
    if (record_it != active_record_readers_.end()) {
        std::vector<txn_id_t> readers(record_it->second.begin(), record_it->second.end());
        for (txn_id_t tid : readers) {
            if (tid == writer) {
                continue;
            }
            auto reader_it = txn_map.find(tid);
            auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
            if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
                continue;
            }
            if (AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }

    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    if (predicate_it != active_predicate_readers_by_table_.end()) {
        std::vector<txn_id_t> predicate_readers(predicate_it->second.begin(), predicate_it->second.end());
        for (txn_id_t tid : predicate_readers) {
            if (tid == writer) {
                continue;
            }
            auto reader_it = txn_map.find(tid);
            auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
            if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
                continue;
            }

            bool read_matches = false;
            for (const auto& predicate : reader_txn->predicate_reads_) {
                if (predicate.tab_name_ != tab_name) {
                    continue;
                }
                if ((old_rec.has_value() && TupleMatches(tab_name, predicate.conds_, *old_rec)) ||
                    (new_rec.has_value() && TupleMatches(tab_name, predicate.conds_, *new_rec))) {
                    read_matches = true;
                    break;
                }
            }
            if (read_matches && AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }

    if (!danger && (HasOtherActivePredicateReadersUnlocked(tab_name, writer) || has_other_active_serializable)) {
        recent_writes_by_table_[tab_name].push_back({writer, tab_name, rid, old_rec, new_rec});
    }

    return danger;
}

bool TransactionManager::CheckInvisibleWrites(txn_id_t reader, Rid rid, const std::string& tab_name) {
    (void)tab_name;
    std::unique_lock<std::mutex> lock(latch_);
    bool danger = false;
    auto reader_it = txn_map.find(reader);
    auto* reader_txn = (reader_it != txn_map.end()) ? reader_it->second.get() : nullptr;
    if (reader_txn == nullptr || reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;

    for (auto& [tid, txn] : txn_map) {
        if (tid == reader)
            continue;
        if (txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
            continue;
        if (txn->get_state() == TransactionState::ABORTED)
            continue;

        bool invisible_to_reader =
            txn->get_state() == TransactionState::GROWING ||
            (txn->get_state() == TransactionState::COMMITTED && txn->get_commit_ts() > reader_txn->get_start_ts());
        if (!invisible_to_reader)
            continue;

        // Check if this transaction has modified the given RID
        bool modified_this_rid = false;
        for (const auto& [tab, slot_rid] : txn->get_modified_slots()) {
            if (slot_rid.page_no == rid.page_no && slot_rid.slot_no == rid.slot_no) {
                modified_this_rid = true;
                break;
            }
        }

        if (modified_this_rid) {
            if (AddRwEdgeInternal(reader, tid, reader)) {
                danger = true;
            }
        }
    }
    return danger;
}

bool TransactionManager::CheckInvisibleWriteEdge(txn_id_t reader, txn_id_t page_writer) {
    if (page_writer == reader)
        return false;
    if (page_writer == INVALID_TXN_ID)
        return false;

    std::unique_lock<std::mutex> lock(latch_);
    auto reader_it = txn_map.find(reader);
    if (reader_it == txn_map.end())
        return false;
    auto* reader_txn = reader_it->second.get();
    if (reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;

    auto writer_it = txn_map.find(page_writer);
    if (writer_it == txn_map.end())
        return false;
    auto* writer_txn = writer_it->second.get();
    if (writer_txn == nullptr)
        return false;
    if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;
    if (writer_txn->get_state() == TransactionState::ABORTED)
        return false;

    return AddRwEdgeInternal(reader, page_writer, reader);
}

bool TransactionManager::CheckPredicateInvisibleWrites(txn_id_t reader, const std::string& tab_name,
                                                       const std::vector<Condition>& conds, RmFileHandle* fh,
                                                       const std::vector<ColMeta>& cols) {
    if (fh == nullptr) {
        return false;
    }

    std::unique_lock<std::mutex> lock(latch_);
    auto reader_it = txn_map.find(reader);
    if (reader_it == txn_map.end())
        return false;
    auto* reader_txn = reader_it->second.get();
    if (reader_txn == nullptr || reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }

    bool danger = false;
    for (auto& [tid, writer_txn] : txn_map) {
        if (tid == reader || writer_txn == nullptr)
            continue;
        if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
            continue;
        if (writer_txn->get_state() == TransactionState::ABORTED)
            continue;

        bool invisible_to_reader = writer_txn->get_state() == TransactionState::GROWING ||
                                   (writer_txn->get_state() == TransactionState::COMMITTED &&
                                    writer_txn->get_commit_ts() > reader_txn->get_start_ts());
        if (!invisible_to_reader)
            continue;

        for (const auto& [slot_tab, rid] : writer_txn->get_modified_slots()) {
            if (slot_tab != tab_name || !fh->is_record(rid)) {
                continue;
            }
            TupleMeta meta = fh->get_tuple_meta(rid);
            if (meta.writer_txn_id_ != tid || meta.is_deleted_) {
                continue;
            }
            auto rec = fh->get_record(rid, nullptr);
            if (rec == nullptr || !MatchesConditions(conds, *rec, cols)) {
                continue;
            }

            if (AddRwEdgeInternal(reader, tid, reader)) {
                danger = true;
            }
        }
    }
    return danger;
}

void TransactionManager::CleanupSsiState(txn_id_t txn_id) {
    CleanupTxnSsiState(txn_id);
}

void TransactionManager::PruneSsiState() {
    std::unique_lock<std::mutex> lock(latch_);

    if (recent_writes_by_table_.empty() && ssi_writes_.empty() && active_record_readers_.empty() &&
        active_predicate_readers_by_table_.empty() && rw_edges_.empty() && active_serializable_txns_.empty()) {
        commits_since_full_ssi_prune_ = 0;
        return;
    }

    std::unordered_set<txn_id_t> candidates = active_serializable_txns_;
    for (const auto& write : ssi_writes_) {
        candidates.insert(write.txn_id_);
    }
    for (const auto& read : ssi_record_reads_) {
        candidates.insert(read.txn_id_);
    }
    for (const auto& [_, writes] : recent_writes_by_table_) {
        for (const auto& write : writes) {
            candidates.insert(write.txn_id_);
        }
    }
    for (const auto& [txn_id, _] : txn_record_read_keys_) {
        candidates.insert(txn_id);
    }
    for (const auto& [txn_id, _] : txn_predicate_read_tables_) {
        candidates.insert(txn_id);
    }
    for (const auto& [txn_id, _] : rw_edges_) {
        candidates.insert(txn_id);
    }
    for (const auto& [_, outs] : rw_edges_) {
        for (txn_id_t txn_id : outs) {
            candidates.insert(txn_id);
        }
    }

    for (txn_id_t txn_id : candidates) {
        auto txn_it = txn_map.find(txn_id);
        Transaction* txn = txn_it == txn_map.end() ? nullptr : txn_it->second.get();
        if (txn == nullptr || txn->get_state() == TransactionState::ABORTED ||
            (txn->get_state() == TransactionState::COMMITTED && !HasActiveSerializableOverlapUnlocked(txn))) {
            active_serializable_txns_.erase(txn_id);
            CleanupTxnReadIndexesUnlocked(txn_id);
            CleanupTxnRwEdgesUnlocked(txn_id);
            CleanupTxnRecentWritesUnlocked(txn_id);
        }
    }

    for (auto it = txn_map.begin(); it != txn_map.end();) {
        Transaction* txn = it->second.get();
        if (CanRetireTransactionUnlocked(txn)) {
            it = txn_map.erase(it);
        } else {
            ++it;
        }
    }

    commits_since_full_ssi_prune_ = 0;
}
