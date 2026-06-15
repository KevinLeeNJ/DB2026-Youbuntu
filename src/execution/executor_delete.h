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

class DeleteExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle* fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager* sm_manager_;

public:
    DeleteExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rids_.empty()) {
            return nullptr; // 没有更多记录可以删除
        }
        // 删除记录
        for (Rid rid : rids_) {
            auto rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                continue;
            }
            char* rec_data = rec->data;
            bool match = true;
            for (const auto& cond : conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (!match) {
                continue; // 如果记录不匹配条件，则跳过删除
            }
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

                // SSI: Check if this delete conflicts with other SER transactions' reads
                if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context_->txn_mgr_ != nullptr) {
                    auto* txn_mgr = context_->txn_mgr_;
                    if (txn_mgr->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, tab_name_,
                                                          std::optional<RmRecord>(*rec), std::nullopt, tab_.cols)) {
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
                    }
                }
            }
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                DeleteLogRecord log_record(context_->txn_->get_transaction_id(), *rec, rid, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
            }
            auto undo_record = context_ != nullptr && context_->txn_ != nullptr
                                   ? std::make_unique<WriteRecord>(WType::DELETE_TUPLE, tab_name_, rid, *rec)
                                   : nullptr;
            struct DeletedIndex {
                const IndexMeta* index;
                std::vector<char> key;
            };
            std::vector<DeletedIndex> deleted_indexes;
            try {
                for (auto& index : tab_.indexes) {
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                  .get();
                    std::vector<char> key(index.col_tot_len);
                    int offset = 0;
                    for (int i = 0; i < index.col_num; ++i) {
                        std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }
                    sm_manager_->remember_historical_index_key(
                        tab_name_, sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols), key, rid);
                    ih->delete_entry(key.data(), context_ == nullptr ? nullptr : context_->txn_);
                    deleted_indexes.push_back(DeletedIndex{&index, std::move(key)});
                }
            } catch (...) {
                for (auto it = deleted_indexes.rbegin(); it != deleted_indexes.rend(); ++it) {
                    auto ih =
                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, it->index->cols))
                            .get();
                    ih->insert_entry(it->key.data(), rid, context_ == nullptr ? nullptr : context_->txn_);
                }
                // undo_record is automatically cleaned up by unique_ptr
                throw;
            }
            if (undo_record != nullptr) {
                UndoLog undo;
                undo.is_deleted_ = true;
                undo.old_meta_ = fh_->get_tuple_meta(rid);
                undo.old_tuple_data_.assign(rec->data, rec->data + rec->size);
                undo.prev_version_ = undo.old_meta_.version_chain_head_;
                UndoLink undo_link = context_->txn_->AppendUndoLog(undo);

                context_->txn_->append_write_record(std::move(undo_record));
                context_->txn_->append_modified_slot(tab_name_, rid);

                TupleMeta tombstone;
                tombstone.writer_txn_id_ = context_->txn_->get_transaction_id();
                tombstone.is_committed_ = false;
                tombstone.is_deleted_ = true;
                tombstone.version_chain_head_ = undo_link;
                fh_->set_tuple_meta(rid, tombstone);
            } else {
                fh_->delete_record(rid, context_);
            }
        }
        return nullptr;
    }
    std::string getType() override {
        return "DeleteExecutor"; // 返回执行器的名称
    }
    Rid& rid() override {
        return _abstract_rid;
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == tab_name_ && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
