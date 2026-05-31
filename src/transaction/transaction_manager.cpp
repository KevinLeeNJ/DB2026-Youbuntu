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
        fh->insert_record(rid, old_rec.data);
        InsertIndexEntries(sm_manager, tab, tab_name, old_rec, rid, txn);
        break;
    }
    case WType::UPDATE_TUPLE: {
        if (fh->is_record(rid)) {
            auto current_rec = fh->get_record(rid, nullptr);
            DeleteIndexEntries(sm_manager, tab, tab_name, *current_rec, txn);
        }
        RmRecord old_rec = write_record->GetRecord();
        fh->update_record(rid, old_rec.data, nullptr);
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
    txn->set_start_ts(next_timestamp_.fetch_add(1));

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

    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    txn->set_state(TransactionState::COMMITTED);
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
    txn->set_state(TransactionState::ABORTED);
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
}
