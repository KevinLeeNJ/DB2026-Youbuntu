/* Copyright (c) 2023 Renmin University of China
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

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

namespace {

void ClearWriteSet(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    auto write_set = txn->get_write_set();
    for (auto* write_record : *write_set) {
        delete write_record;
    }
    write_set->clear();
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
                        Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->delete_entry(key.data(), txn);
    }
}

void InsertIndexEntries(SmManager* sm_manager, const TabMeta& tab, const std::string& tab_name, const RmRecord& rec,
                        const Rid& rid, Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->insert_entry(key.data(), rid, txn);
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
                    (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT);
    if (!can_cast) {
        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
    }

    switch (lhs_type) {
    case TYPE_INT:
    case TYPE_FLOAT: {
        float lhs_val = lhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<int*>(lhs_data))
                                             : *reinterpret_cast<float*>(lhs_data);
        float rhs_val;
        if (cond.is_rhs_val) {
            rhs_val = rhs_type == TYPE_INT ? static_cast<float>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
        } else {
            rhs_val = rhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<int*>(rhs_data))
                                           : *reinterpret_cast<float*>(rhs_data);
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
    }
    case TYPE_STRING: {
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

std::optional<UndoLog> GetCurrentUndoLog(RmFileHandle* fh, const Rid& rid) {
    TupleMeta meta = fh->get_tuple_meta(rid);
    if (!meta.version_chain_head_.IsValid()) {
        return std::nullopt;
    }
    auto it = TransactionManager::txn_map.find(meta.version_chain_head_.undo_txn_id_);
    if (it == TransactionManager::txn_map.end()) {
        return std::nullopt;
    }
    return it->second->GetUndoLog(meta.version_chain_head_.undo_slot_offset_);
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

void UndoWriteRecord(SmManager* sm_manager, WriteRecord* write_record, Transaction* txn) {
    const std::string tab_name = write_record->GetTableName();
    auto& tab = sm_manager->db_.get_table(tab_name);
    auto* fh = sm_manager->fhs_.at(tab_name).get();
    Rid rid = write_record->GetRid();

    switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE: {
        if (fh->is_record(rid)) {
            auto rec = fh->get_record(rid, nullptr);
            DeleteIndexEntries(sm_manager, tab, tab_name, *rec, txn);
            fh->delete_record(rid, nullptr);
        }
        break;
    }
    case WType::DELETE_TUPLE: {
        RmRecord old_rec = write_record->GetRecord();
        auto undo = GetCurrentUndoLog(fh, rid);
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
        if (fh->is_record(rid)) {
            auto current_rec = fh->get_record(rid, nullptr);
            DeleteIndexEntries(sm_manager, tab, tab_name, *current_rec, txn);
        }
        RmRecord old_rec = write_record->GetRecord();
        auto undo = GetCurrentUndoLog(fh, rid);
        fh->update_record(rid, old_rec.data, nullptr);
        fh->set_tuple_meta(rid, undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta());
        InsertIndexEntries(sm_manager, tab, tab_name, old_rec, rid, txn);
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
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    (void)log_manager;
    if (txn == nullptr) {
        txn_id_t txn_id = next_txn_id_.fetch_add(1);
        txn = new Transaction(txn_id);
    }

    txn->set_state(TransactionState::GROWING);
    // start_ts = current timestamp (used for MVCC snapshot)
    txn->set_start_ts(next_timestamp_.fetch_add(1));
    // running_txns_ tracks active read timestamps for GC watermark
    running_txns_.AddTxn(txn->get_start_ts());

    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || txn->get_state() == TransactionState::COMMITTED) {
        return;
    }
    if (txn->get_state() == TransactionState::ABORTED) {
        return;
    }

    // Allocate commit_ts (monotonic)
    timestamp_t commit_ts = next_timestamp_.fetch_add(1);
    txn->commit_ts_ = commit_ts;
    last_commit_ts_ = commit_ts;

    // Remove from running txns (for watermark)
    running_txns_.RemoveTxn(txn->get_start_ts());

    // Mark all modified slots as committed
    sm_manager_->mark_slots_committed(*txn, commit_ts);

    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    txn->set_state(TransactionState::COMMITTED);
    PruneSsiState();
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || txn->get_state() == TransactionState::ABORTED) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTED) {
        return;
    }

    auto write_set = txn->get_write_set();
    for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
        UndoWriteRecord(sm_manager_, *it, txn);
    }
    running_txns_.RemoveTxn(txn->get_start_ts());
    CleanupSsiState(txn->get_transaction_id());
    txn->read_rids_.clear();
    txn->predicate_reads_.clear();
    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    txn->set_state(TransactionState::ABORTED);
    PruneSsiState();
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
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
              (lhs_col.type == TYPE_FLOAT && rhs_type == TYPE_INT))) {
            throw IncompatibleTypeError(coltype2str(lhs_col.type), coltype2str(rhs_type));
        }
        int cmp = 0;
        if (lhs_col.type == TYPE_STRING) {
            std::string lhs(lhs_data, strnlen(lhs_data, lhs_col.len));
            std::string rhs =
                cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col.len));
            cmp = lhs.compare(rhs);
        } else {
            float lhs = lhs_col.type == TYPE_INT ? static_cast<float>(*reinterpret_cast<const int*>(lhs_data))
                                                 : *reinterpret_cast<const float*>(lhs_data);
            float rhs;
            if (cond.is_rhs_val) {
                rhs = rhs_type == TYPE_INT ? static_cast<float>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
            } else {
                rhs = rhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<const int*>(rhs_data))
                                           : *reinterpret_cast<const float*>(rhs_data);
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
    auto* lhs_txn = lhs_it->second;
    auto* rhs_txn = rhs_it->second;
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
                if (!TransactionsOverlap(tin_it->second, pivot_it->second) ||
                    !TransactionsOverlap(pivot_it->second, tout_it->second)) {
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
    auto* reader_txn = reader_it->second;
    auto* writer_txn = writer_it->second;
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
        // Check centralized ssi_writes_ for invisible writes matching the predicate
        txn_id_t reader_id = txn->get_transaction_id();
        for (const auto& write : ssi_writes_) {
            if (write.txn_id_ == reader_id || write.tab_name_ != tab_name) {
                continue;
            }
            auto writer_it = txn_map.find(write.txn_id_);
            if (writer_it == txn_map.end()) {
                continue;
            }
            auto* writer_txn = writer_it->second;
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
    std::string rid_key = tab_name + ":" + std::to_string(rid.page_no) + ":" + std::to_string(rid.slot_no);
    bool danger = false;
    auto writer_it = txn_map.find(writer);
    auto* writer_txn = (writer_it != txn_map.end()) ? writer_it->second : nullptr;
    for (auto& [tid, txn] : txn_map) {
        if (tid == writer)
            continue;
        if (!SsiTxnCanConflictWithWriter(txn, writer_txn))
            continue;
        if (txn->read_rids_.count(rid_key)) {
            if (AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }
    return danger;
}

bool TransactionManager::CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name,
                                                  const std::optional<RmRecord>& old_rec,
                                                  const std::optional<RmRecord>& new_rec,
                                                  const std::vector<ColMeta>& cols) {
    std::unique_lock<std::mutex> lock(latch_);
    std::string rid_key = tab_name + ":" + std::to_string(rid.page_no) + ":" + std::to_string(rid.slot_no);
    bool danger = false;
    auto writer_it = txn_map.find(writer);
    auto* writer_txn = (writer_it != txn_map.end()) ? writer_it->second : nullptr;
    if (writer_txn == nullptr)
        return false;
    if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        // For non-SER writers: don't store writes, don't check
        return false;
    }

    // Check both old and new records against ALL reader transactions' read sets
    // and predicate reads. This is done atomically under latch_.
    for (auto& [tid, txn] : txn_map) {
        if (tid == writer)
            continue;
        if (!SsiTxnCanConflictWithWriter(txn, writer_txn))
            continue;

        bool read_matches = txn->read_rids_.count(rid_key) > 0;
        if (!read_matches && old_rec.has_value()) {
            for (const auto& predicate : txn->predicate_reads_) {
                if (predicate.tab_name_ == tab_name && TupleMatches(tab_name, predicate.conds_, *old_rec)) {
                    read_matches = true;
                    break;
                }
            }
        }
        if (!read_matches && new_rec.has_value()) {
            for (const auto& predicate : txn->predicate_reads_) {
                if (predicate.tab_name_ == tab_name && TupleMatches(tab_name, predicate.conds_, *new_rec)) {
                    read_matches = true;
                    break;
                }
            }
        }
        if (!read_matches)
            continue;

        if (AddRwEdgeInternal(tid, writer, writer)) {
            danger = true;
        }
    }

    // Only store the write if no danger was found (atomic write storage)
    if (!danger) {
        ssi_writes_.push_back({writer, tab_name, rid, old_rec, new_rec});
    }

    return danger;
}

bool TransactionManager::CheckInvisibleWrites(txn_id_t reader, Rid rid, const std::string& tab_name) {
    (void)tab_name;
    std::unique_lock<std::mutex> lock(latch_);
    bool danger = false;
    auto reader_it = txn_map.find(reader);
    auto* reader_txn = (reader_it != txn_map.end()) ? reader_it->second : nullptr;
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
    auto* reader_txn = reader_it->second;
    if (reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;

    auto writer_it = txn_map.find(page_writer);
    if (writer_it == txn_map.end())
        return false;
    auto* writer_txn = writer_it->second;
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
    auto* reader_txn = reader_it->second;
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
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map.find(txn_id);
    if (it == txn_map.end())
        return;
    auto* txn = it->second;
    // Clean per-txn state
    for (auto in_id : txn->in_rw_) {
        auto in_it = txn_map.find(in_id);
        if (in_it != txn_map.end())
            in_it->second->out_rw_.erase(txn_id);
    }
    for (auto out_id : txn->out_rw_) {
        auto out_it = txn_map.find(out_id);
        if (out_it != txn_map.end())
            out_it->second->in_rw_.erase(txn_id);
    }
    txn->in_rw_.clear();
    txn->out_rw_.clear();
    // Clean centralized SSI state
    ssi_writes_.erase(std::remove_if(ssi_writes_.begin(), ssi_writes_.end(),
                                     [&](const auto& write) { return write.txn_id_ == txn_id; }),
                      ssi_writes_.end());
    ssi_record_reads_.erase(std::remove_if(ssi_record_reads_.begin(), ssi_record_reads_.end(),
                                           [&](const auto& read) { return read.txn_id_ == txn_id; }),
                            ssi_record_reads_.end());
    rw_edges_.erase(txn_id);
    for (auto& [_, outs] : rw_edges_) {
        outs.erase(txn_id);
    }
}

void TransactionManager::PruneSsiState() {
    std::unique_lock<std::mutex> lock(latch_);

    auto can_remove = [&](txn_id_t txn_id) {
        auto txn_it = txn_map.find(txn_id);
        if (txn_it == txn_map.end() || txn_it->second == nullptr) {
            return true;
        }
        auto* txn = txn_it->second;
        if (txn->get_state() == TransactionState::ABORTED) {
            return true;
        }
        if (txn->get_state() != TransactionState::COMMITTED) {
            return false;
        }
        for (const auto& [_, other] : txn_map) {
            if (other == nullptr || other->get_transaction_id() == txn_id ||
                other->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
                continue;
            }
            if (other->get_state() == TransactionState::COMMITTED || other->get_state() == TransactionState::ABORTED) {
                continue;
            }
            if (TransactionsOverlap(txn, other)) {
                return false;
            }
        }
        return true;
    };

    std::unordered_set<txn_id_t> removable;
    for (const auto& write : ssi_writes_) {
        if (can_remove(write.txn_id_)) {
            removable.insert(write.txn_id_);
        }
    }
    for (const auto& read : ssi_record_reads_) {
        if (can_remove(read.txn_id_)) {
            removable.insert(read.txn_id_);
        }
    }
    for (const auto& [txn_id, _] : rw_edges_) {
        if (can_remove(txn_id)) {
            removable.insert(txn_id);
        }
    }
    for (const auto& [_, outs] : rw_edges_) {
        for (txn_id_t txn_id : outs) {
            if (can_remove(txn_id)) {
                removable.insert(txn_id);
            }
        }
    }
    if (removable.empty()) {
        return;
    }

    ssi_writes_.erase(std::remove_if(ssi_writes_.begin(), ssi_writes_.end(),
                                     [&](const auto& write) { return removable.count(write.txn_id_); }),
                      ssi_writes_.end());
    ssi_record_reads_.erase(std::remove_if(ssi_record_reads_.begin(), ssi_record_reads_.end(),
                                           [&](const auto& read) { return removable.count(read.txn_id_); }),
                            ssi_record_reads_.end());
    for (txn_id_t txn_id : removable) {
        rw_edges_.erase(txn_id);
    }
    for (auto& [_, outs] : rw_edges_) {
        for (txn_id_t txn_id : removable) {
            outs.erase(txn_id);
        }
    }
}
