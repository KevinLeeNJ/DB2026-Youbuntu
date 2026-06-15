/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include <algorithm>

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

    static std::vector<char> make_index_key(const IndexMeta& index, const char* rec_data) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
            std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

public:
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        for (Rid& rid : rids_) {
            std::unique_ptr<RmRecord> rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                continue;
            }
            bool match = true;
            for (auto cond : conds_) // 判断是否匹配
            {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                // MVCC Write-Write conflict detection
                if (context_ != nullptr && context_->txn_ != nullptr) {
                    auto txn = context_->txn_;
                    if (context_->lock_mgr_ != nullptr &&
                        !context_->lock_mgr_->lock_exclusive_on_record(txn, rid, fh_->GetFd())) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                    }
                    TupleMeta meta = fh_->get_tuple_meta(rid);
                    if (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id()) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                    }
                    if (meta.is_committed_ && meta.commit_ts_ > txn->get_start_ts() &&
                        meta.writer_txn_id_ != txn->get_transaction_id()) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                    }
                }

                auto new_rec = std::make_unique<RmRecord>(*rec); // 对原记录进行拷贝
                update_record(new_rec.get());                    // 对记录更新

                // SSI: Consolidated atomic check for both old and new records.
                // Per spec: UPDATE must check both old (pre-update) and new (post-update) records.
                // Both are checked atomically under a single lock; the write is stored
                // as one ssi_writes_ entry only if no danger is found.
                if (context_ != nullptr && context_->txn_ != nullptr &&
                    context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
                    context_->txn_mgr_ != nullptr) {
                    auto* txn_mgr = context_->txn_mgr_;
                    txn_id_t writer_id = context_->txn_->get_transaction_id();
                    bool danger =
                        txn_mgr->CheckWriteAgainstReaders(writer_id, rid, tab_name_, std::optional<RmRecord>(*rec),
                                                          std::optional<RmRecord>(*new_rec), tab_.cols);
                    if (danger) {
                        throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
                    }
                }

                struct IndexUpdate {
                    const IndexMeta* index;
                    std::vector<char> old_key;
                    std::vector<char> new_key;
                };
                std::vector<IndexUpdate> index_updates;
                auto txn = context_ == nullptr ? nullptr : context_->txn_;

                for (const auto& index : tab_.indexes) {
                    auto old_key = make_index_key(index, rec->data);
                    auto new_key = make_index_key(index, new_rec->data);
                    if (old_key == new_key) {
                        continue;
                    }
                    sm_manager_->remember_historical_index_key(
                        tab_name_, sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols), old_key, rid);
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                  .get();
                    std::vector<Rid> result;
                    if (ih->get_value(new_key.data(), &result, txn) &&
                        std::any_of(result.begin(), result.end(), [&](const Rid& found) { return found != rid; })) {
                        throw IndexEntryExistsError();
                    }
                    const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
                    auto candidate_rids = sm_manager_->get_historical_index_key_rids(tab_name_, index_name, new_key);
                    for (const auto& candidate_rid : candidate_rids) {
                        if (candidate_rid != rid &&
                            HistoricalIndexKeyConflictsWithTxn(fh_, candidate_rid, index, new_key, context_)) {
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                        }
                    }
                    index_updates.push_back(IndexUpdate{&index, std::move(old_key), std::move(new_key)});
                }
                if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                    UpdateLogRecord log_record(context_->txn_->get_transaction_id(), *rec, *new_rec, rid, tab_name_);
                    log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                    lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                    context_->txn_->set_prev_lsn(lsn);
                }
                if (context_ != nullptr && context_->txn_ != nullptr) {
                    context_->txn_->append_write_record(
                        std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, tab_name_, rid, *rec));

                    // Save old version as undo log for MVCC version chain
                    UndoLog undo;
                    undo.is_deleted_ = false;
                    undo.old_meta_ = fh_->get_tuple_meta(rid);
                    undo.old_tuple_data_.assign(rec->data, rec->data + rec->size);
                    undo.prev_version_ = undo.old_meta_.version_chain_head_;
                    UndoLink undo_link = context_->txn_->AppendUndoLog(undo);

                    // Track modified slot for MVCC commit
                    context_->txn_->append_modified_slot(tab_name_, rid);
                    // Update TupleMeta: mark as uncommitted, owned by this txn, chain to old version
                    TupleMeta meta;
                    meta.writer_txn_id_ = context_->txn_->get_transaction_id();
                    meta.is_committed_ = false;
                    meta.is_deleted_ = false;
                    meta.version_chain_head_ = undo_link;
                    fh_->set_tuple_meta(rid, meta);
                }

                std::vector<size_t> deleted_indexes;
                std::vector<size_t> inserted_indexes;
                try {
                    for (size_t i = 0; i < index_updates.size(); ++i) {
                        const auto& update = index_updates[i];
                        auto ih = sm_manager_->ihs_
                                      .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, update.index->cols))
                                      .get();
                        ih->delete_entry(update.old_key.data(), txn); // 删除旧索引
                        deleted_indexes.push_back(i);
                        ih->insert_entry(update.new_key.data(), rid, txn); // 插入新索引
                        inserted_indexes.push_back(i);
                    }
                } catch (...) // 失败时回滚已经修改过的索引
                {
                    for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
                        const auto& update = index_updates[*it];
                        auto ih = sm_manager_->ihs_
                                      .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, update.index->cols))
                                      .get();
                        ih->delete_entry(update.new_key.data(), txn); // 删除新索引
                    }
                    for (auto it = deleted_indexes.rbegin(); it != deleted_indexes.rend(); ++it) {
                        const auto& update = index_updates[*it];
                        auto ih = sm_manager_->ihs_
                                      .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, update.index->cols))
                                      .get();
                        ih->insert_entry(update.old_key.data(), rid, txn); // 恢复旧索引
                    }
                    throw; // 仍然抛出错误
                }
                fh_->update_record(rid, new_rec->data, context_);
            }
        }
        return nullptr;
    }

    Rid& rid() override {
        return _abstract_rid;
    }
    std::string getType() override {
        return "UpdateExecutor"; // 返回执行器的名称
    }
    /**
     * @brief 更新记录
     * @param rec 需要更新的记录
     */
    void update_record(RmRecord* rec) {
        for (const auto& set_clause : set_clauses_) {
            auto col_meta = get_col_offset(set_clause.lhs);
            char* data = rec->data + col_meta.offset;
            if (can_cast(col_meta.type, set_clause.rhs.type) == false) {
                throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(set_clause.rhs.type));
            }
            switch (col_meta.type) {
            case TYPE_INT: {
                if (set_clause.rhs.type == TYPE_INT) {
                    *(int*)data = set_clause.rhs.int_val;
                } else {
                    *(int*)data = (int)set_clause.rhs.float_val;
                }
                break;
            }
            case TYPE_FLOAT: {
                if (set_clause.rhs.type == TYPE_FLOAT) {
                    *(float*)data = set_clause.rhs.float_val;
                } else {
                    *(float*)data = (float)set_clause.rhs.int_val;
                }
                break;
            }
            case TYPE_STRING: {
                int len = get_col_offset(set_clause.lhs).len;
                std::memset(data, 0, len);
                std::memcpy(data, set_clause.rhs.str_val.c_str(), std::min(len, (int)set_clause.rhs.str_val.size()));
                break;
            }
            }
        }
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
