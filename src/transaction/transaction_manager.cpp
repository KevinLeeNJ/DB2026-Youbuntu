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
#include <functional>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};
std::shared_mutex TransactionManager::txn_map_mutex_ = {};

namespace {

struct WriteRidKey {
    int fd;
    int page_no;
    int slot_no;

    bool operator==(const WriteRidKey& other) const {
        return fd == other.fd && page_no == other.page_no && slot_no == other.slot_no;
    }
};

struct WriteRidKeyHash {
    size_t operator()(const WriteRidKey& key) const {
        size_t seed = std::hash<int>{}(key.fd);
        seed ^= std::hash<int>{}(key.page_no) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        seed ^= std::hash<int>{}(key.slot_no) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }
};

WriteRidKey MakeWriteRidKey(RmFileHandle* fh, const Rid& rid) {
    return WriteRidKey{fh->GetFd(), rid.page_no, rid.slot_no};
}

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

bool SameIndexKeys(const TabMeta& tab, const RmRecord& lhs, const RmRecord& rhs) {
    for (const auto& index : tab.indexes) {
        if (MakeIndexKey(index, lhs.data) != MakeIndexKey(index, rhs.data)) {
            return false;
        }
    }
    return true;
}

void UndoWriteRecord(SmManager* sm_manager, WriteRecord* write_record, Transaction* txn) {
    const std::string tab_name = write_record->GetTableName();
    auto& tab = sm_manager->db_.get_table(tab_name);
    auto* fh = sm_manager->fhs_.at(tab_name).get();
    Rid rid = write_record->GetRid();

    switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE: {
        if (fh->is_record(rid)) {
            auto rec = fh->get_latest_record(rid);
            DeleteIndexEntries(sm_manager, tab, tab_name, *rec, txn);
            fh->rollback_insert(rid);
        }
        break;
    }
    case WType::DELETE_TUPLE: {
        RmRecord old_rec = write_record->GetRecord();
        auto link = fh->get_undo_link(rid);
        auto undo_log = txn->GetUndoLog(link.prev_log_idx_);
        fh->restore_record(rid, undo_log);
        break;
    }
    case WType::UPDATE_TUPLE: {
        auto link = fh->get_undo_link(rid);
        auto undo_log = txn->GetUndoLog(link.prev_log_idx_);
        fh->restore_record(rid, undo_log);
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
    txn->set_start_ts(next_timestamp_.fetch_add(1));

    std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
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

    timestamp_t commit_ts = next_timestamp_.fetch_add(1);
    txn->set_commit_ts(commit_ts);
    auto write_set = txn->get_write_set();
    std::unordered_map<WriteRidKey, WriteRecord*, WriteRidKeyHash> first_write_by_rid;
    for (auto* write_record : *write_set) {
        auto* fh = sm_manager_->fhs_.at(write_record->GetTableName()).get();
        Rid rid = write_record->GetRid();
        auto rid_key = MakeWriteRidKey(fh, rid);
        if (first_write_by_rid.find(rid_key) == first_write_by_rid.end()) {
            first_write_by_rid[rid_key] = write_record;
        }
    }
    for (auto& [_, write_record] : first_write_by_rid) {
        const std::string tab_name = write_record->GetTableName();
        auto& tab = sm_manager_->db_.get_table(tab_name);
        auto* fh = sm_manager_->fhs_.at(tab_name).get();
        Rid rid = write_record->GetRid();
        auto latest_rec = fh->get_latest_record(rid);
        auto latest_meta = fh->get_tuple_meta(rid);
        RmRecord old_index_rec = write_record->GetRecord();
        if (write_record->GetWriteType() == WType::INSERT_TUPLE) {
            if (latest_meta.is_deleted_) {
                DeleteIndexEntries(sm_manager_, tab, tab_name, old_index_rec, txn);
            } else if (latest_rec != nullptr && !SameIndexKeys(tab, old_index_rec, *latest_rec)) {
                DeleteIndexEntries(sm_manager_, tab, tab_name, old_index_rec, txn);
                InsertIndexEntries(sm_manager_, tab, tab_name, *latest_rec, rid, txn);
            }
        } else {
            DeleteIndexEntries(sm_manager_, tab, tab_name, old_index_rec, txn);
            if (!latest_meta.is_deleted_ && latest_rec != nullptr) {
                InsertIndexEntries(sm_manager_, tab, tab_name, *latest_rec, rid, txn);
            }
        }
    }
    std::unordered_set<WriteRidKey, WriteRidKeyHash> finalized;
    for (auto* write_record : *write_set) {
        const std::string tab_name = write_record->GetTableName();
        auto* fh = sm_manager_->fhs_.at(tab_name).get();
        Rid rid = write_record->GetRid();
        auto rid_key = MakeWriteRidKey(fh, rid);
        if (finalized.insert(rid_key).second) {
            fh->finalize_record(rid, txn->get_transaction_id(), commit_ts);
        }
    }
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
    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    CleanupSsiState(txn->get_transaction_id());
    txn->set_state(TransactionState::ABORTED);
    PruneSsiState();
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
}

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
    std::shared_lock<std::shared_mutex> map_lock(txn_map_mutex_);
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
    std::shared_lock<std::shared_mutex> map_lock(txn_map_mutex_);
    return HasDangerousStructureUnlocked(current_txn);
}

bool TransactionManager::HasDangerousStructureUnlocked(txn_id_t current_txn) {
    for (const auto& [tin, outs] : rw_edges_) {
        auto tin_it = txn_map.find(tin);
        if (tin_it == txn_map.end() || tin_it->second->get_state() == TransactionState::ABORTED) {
            continue;
        }
        for (txn_id_t pivot : outs) {
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
                if (tin != current_txn && pivot != current_txn && tout != current_txn) {
                    continue;
                }
                if (!TransactionsOverlap(tin_it->second, pivot_it->second) ||
                    !TransactionsOverlap(pivot_it->second, tout_it->second)) {
                    continue;
                }
                if (tin == tout || CommittedBeforeUnlocked(tout, tin)) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool TransactionManager::AddRwEdge(txn_id_t reader, txn_id_t writer, txn_id_t current_txn) {
    if (reader == writer) {
        return false;
    }
    std::shared_lock<std::shared_mutex> map_lock(txn_map_mutex_);
    auto reader_it = txn_map.find(reader);
    auto writer_it = txn_map.find(writer);
    if (reader_it == txn_map.end() || writer_it == txn_map.end()) {
        return false;
    }
    if (reader_it->second->get_state() == TransactionState::ABORTED ||
        writer_it->second->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (!TransactionsOverlap(reader_it->second, writer_it->second)) {
        return false;
    }
    auto [_, inserted] = rw_edges_[reader].insert(writer);
    if (!inserted) {
        return false;
    }
    return HasDangerousStructureUnlocked(current_txn);
}

void TransactionManager::SsiRecordPredicateRead(Transaction* txn, const std::string& tab_name,
                                                const std::vector<Condition>& conds) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return;
    }
    bool dangerous = false;
    {
        std::scoped_lock<std::mutex> lock(ssi_latch_);
        ssi_predicate_reads_.push_back({txn->get_transaction_id(), tab_name, conds});
        for (const auto& write : ssi_writes_) {
            if (write.txn_id_ == txn->get_transaction_id() || write.tab_name_ != tab_name) {
                continue;
            }
            TransactionState writer_state;
            timestamp_t writer_commit_ts;
            {
                std::shared_lock<std::shared_mutex> map_lock(txn_map_mutex_);
                auto writer_it = txn_map.find(write.txn_id_);
                if (writer_it == txn_map.end()) {
                    continue;
                }
                writer_state = writer_it->second->get_state();
                writer_commit_ts = writer_it->second->get_commit_ts();
            }
            if (writer_state == TransactionState::ABORTED) {
                continue;
            }
            bool invisible = writer_state != TransactionState::COMMITTED || writer_commit_ts > txn->get_start_ts();
            if (!invisible) {
                continue;
            }
            bool matches = (write.old_rec_.has_value() && TupleMatches(tab_name, conds, *write.old_rec_)) ||
                           (write.new_rec_.has_value() && TupleMatches(tab_name, conds, *write.new_rec_));
            if (matches && AddRwEdge(txn->get_transaction_id(), write.txn_id_, txn->get_transaction_id())) {
                dangerous = true;
                break;
            }
        }
    }
    if (dangerous) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_CONFLICT);
    }
}

void TransactionManager::SsiRecordRead(Transaction* txn, const std::string& tab_name, const Rid& rid) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return;
    }
    std::scoped_lock<std::mutex> lock(ssi_latch_);
    ssi_record_reads_.push_back({txn->get_transaction_id(), tab_name, rid});
}

void TransactionManager::SsiCheckWrite(Transaction* txn, const std::string& tab_name, std::optional<Rid> rid,
                                       const std::optional<RmRecord>& old_rec, const std::optional<RmRecord>& new_rec) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return;
    }
    bool dangerous = false;
    {
        std::scoped_lock<std::mutex> lock(ssi_latch_);
        txn_id_t writer = txn->get_transaction_id();
        for (const auto& read : ssi_record_reads_) {
            if (read.txn_id_ == writer || read.tab_name_ != tab_name || !rid.has_value() || read.rid_ != *rid) {
                continue;
            }
            if (AddRwEdge(read.txn_id_, writer, writer)) {
                dangerous = true;
                break;
            }
        }
        if (!dangerous) {
            for (const auto& read : ssi_predicate_reads_) {
                if (read.txn_id_ == writer || read.tab_name_ != tab_name) {
                    continue;
                }
                bool matches = (old_rec.has_value() && TupleMatches(tab_name, read.conds_, *old_rec)) ||
                               (new_rec.has_value() && TupleMatches(tab_name, read.conds_, *new_rec));
                if (matches && AddRwEdge(read.txn_id_, writer, writer)) {
                    dangerous = true;
                    break;
                }
            }
        }
        if (!dangerous) {
            ssi_writes_.push_back({writer, tab_name, old_rec, new_rec});
        }
    }
    if (dangerous) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_CONFLICT);
    }
}

void TransactionManager::CleanupSsiState(txn_id_t txn_id) {
    std::scoped_lock<std::mutex> lock(ssi_latch_);
    ssi_predicate_reads_.erase(std::remove_if(ssi_predicate_reads_.begin(), ssi_predicate_reads_.end(),
                                              [&](const auto& read) { return read.txn_id_ == txn_id; }),
                               ssi_predicate_reads_.end());
    ssi_record_reads_.erase(std::remove_if(ssi_record_reads_.begin(), ssi_record_reads_.end(),
                                           [&](const auto& read) { return read.txn_id_ == txn_id; }),
                            ssi_record_reads_.end());
    ssi_writes_.erase(std::remove_if(ssi_writes_.begin(), ssi_writes_.end(),
                                     [&](const auto& write) { return write.txn_id_ == txn_id; }),
                      ssi_writes_.end());
    rw_edges_.erase(txn_id);
    for (auto& [_, outs] : rw_edges_) {
        outs.erase(txn_id);
    }
}

void TransactionManager::PruneSsiState() {
    std::scoped_lock<std::mutex> lock(ssi_latch_);
    std::shared_lock<std::shared_mutex> map_lock(txn_map_mutex_);

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
    for (const auto& read : ssi_predicate_reads_) {
        if (can_remove(read.txn_id_)) {
            removable.insert(read.txn_id_);
        }
    }
    for (const auto& read : ssi_record_reads_) {
        if (can_remove(read.txn_id_)) {
            removable.insert(read.txn_id_);
        }
    }
    for (const auto& write : ssi_writes_) {
        if (can_remove(write.txn_id_)) {
            removable.insert(write.txn_id_);
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

    ssi_predicate_reads_.erase(
        std::remove_if(ssi_predicate_reads_.begin(), ssi_predicate_reads_.end(),
                       [&](const auto& read) { return removable.find(read.txn_id_) != removable.end(); }),
        ssi_predicate_reads_.end());
    ssi_record_reads_.erase(
        std::remove_if(ssi_record_reads_.begin(), ssi_record_reads_.end(),
                       [&](const auto& read) { return removable.find(read.txn_id_) != removable.end(); }),
        ssi_record_reads_.end());
    ssi_writes_.erase(
        std::remove_if(ssi_writes_.begin(), ssi_writes_.end(),
                       [&](const auto& write) { return removable.find(write.txn_id_) != removable.end(); }),
        ssi_writes_.end());
    for (txn_id_t txn_id : removable) {
        rw_edges_.erase(txn_id);
    }
    for (auto& [_, outs] : rw_edges_) {
        for (txn_id_t txn_id : removable) {
            outs.erase(txn_id);
        }
    }
}
