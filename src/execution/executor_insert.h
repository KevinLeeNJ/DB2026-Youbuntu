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

#include "execution_common.h"
#include "execution_defs.h"
#include "executor_abstract.h"
#include "row_mutation.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor {
private:
    const TabMeta* tab_;        // generation-scoped table metadata
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle* fh_;          // 表的数据文件句柄
    std::string tab_name_storage_;
    const std::string* tab_name_;
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager* sm_manager_;
    std::vector<RowMutationIndex> owned_indexes_;
    const std::vector<RowMutationIndex>* indexes_;
    Context* context_{nullptr};
    bool executed_{false};

    static std::vector<char> make_index_key(const IndexMeta& index, const char* rec_data) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
            memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

    void check_mvcc_unique_key_conflict(const RowMutationIndex& index, const std::vector<char>& key) {
        if (context_ == nullptr || context_->txn_ == nullptr || context_->txn_mgr_ == nullptr) {
            return;
        }

        auto candidate_rids = sm_manager_->version_history().get_historical_index_key_rids(*tab_name_, index.name, key);

        for (const auto& existing_rid : candidate_rids) {
            if (HistoricalIndexKeyConflictsWithTxn(fh_, existing_rid, *index.meta, key, context_)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
            }
        }
    }

public:
    InsertExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Value> values, Context* context) {
        sm_manager_ = sm_manager;
        tab_ = &sm_manager_->db_.get_table(tab_name);
        values_ = std::move(values);
        tab_name_storage_ = tab_name;
        tab_name_ = &tab_name_storage_;
        if (values_.size() != tab_->cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        owned_indexes_.reserve(tab_->indexes.size());
        for (const auto& index : tab_->indexes) {
            std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
            owned_indexes_.push_back(
                RowMutationIndex{&index, sm_manager_->ihs_.at(index_name).get(), std::move(index_name)});
        }
        indexes_ = &owned_indexes_;
    };

    void Execute() {
        if (executed_) {
            return;
        }
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        // 尾部的 null bitmap 不在列循环的覆盖范围内，必须先清零
        std::memset(rec.data, 0, static_cast<size_t>(rec.size));
        for (size_t i = 0; i < values_.size(); i++) {
            const auto& col = tab_->cols[i];
            auto& val = values_[i];
            // NULL 与列类型无关：只置位，数据字节保持全零
            if (val.is_null) {
                set_null(rec.data, col);
                continue;
            }
            if (col.type != val.type) {
                const bool compatible = (col.type == TYPE_INT && val.type == TYPE_FLOAT) ||
                                        (col.type == TYPE_FLOAT && val.type == TYPE_INT) ||
                                        ((col.type == TYPE_STRING || col.type == TYPE_DATETIME) &&
                                         (val.type == TYPE_STRING || val.type == TYPE_DATETIME));
                if (!compatible) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                // Convert value type for storage (e.g., INT literal into FLOAT column)
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                    val.set_float(static_cast<float>(val.int_val));
                } else if (col.type == TYPE_INT && val.type == TYPE_FLOAT) {
                    val.set_int(static_cast<int>(val.float_val));
                } else if ((col.type == TYPE_STRING || col.type == TYPE_DATETIME) &&
                           (val.type == TYPE_STRING || val.type == TYPE_DATETIME)) {
                    val.type = col.type;
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        if (indexes_->empty()) {
            CheckLogicalRowDeleteIntentForInsert(context_, sm_manager_, *tab_name_, rec);
        }

        if (context_ != nullptr && context_->txn_ != nullptr &&
            context_->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED &&
            DeletedTupleCandidatesConflictWithInsert(fh_, sm_manager_, *tab_name_, rec, context_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
        }

        std::vector<std::vector<char>> index_keys;
        index_keys.reserve(indexes_->size());
        for (const auto& index : *indexes_) {
            auto key = make_index_key(*index.meta, rec.data);
            ReserveUniqueKey(context_, index.handle->GetFd(), key);
            check_mvcc_unique_key_conflict(index, key);
            index_keys.push_back(std::move(key));
        }

        auto prepared_insert = fh_->prepare_insert_record();
        bool insert_finished = false;
        lsn_t log_lsn = INVALID_LSN;
        try {
            rid_ = prepared_insert.rid;
            TupleMeta pending_meta;
            if (context_ != nullptr && context_->txn_ != nullptr) {
                pending_meta.writer_txn_id_ = context_->txn_->get_transaction_id();
                pending_meta.is_committed_ = false;
                pending_meta.is_deleted_ = false;
                pending_meta.version_chain_head_ = UndoLink{};
            }
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, *tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
                log_lsn = lsn;
            }
            fh_->finish_insert_record(prepared_insert, rec.data,
                                      context_ != nullptr && context_->txn_ != nullptr ? &pending_meta : nullptr,
                                      log_lsn);
            insert_finished = true;
        } catch (...) {
            if (!insert_finished) {
                fh_->abort_prepared_insert(prepared_insert);
            }
            throw;
        }
        std::vector<size_t> inserted_indexes;
        std::optional<IndexWriteWalContext> wal_context;
        if (!indexes_->empty()) {
            wal_context.emplace(IndexWriteWalContext::LoggedRuntime(log_lsn));
        }
        const auto rollback_index_inserts = [&] {
            for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
                const auto& index = (*indexes_)[*it];
                index.handle->delete_entry(index_keys[*it].data(), rid_, *wal_context);
            }
            fh_->delete_record(rid_, context_);
        };
        try {
            for (size_t i = 0; i < indexes_->size(); ++i) {
                (*indexes_)[i].handle->insert_entry(index_keys[i].data(), rid_, *wal_context);
                inserted_indexes.push_back(i);
            }
        } catch (const IndexEntryExistsError&) {
            // The B+tree has no transaction context, so translate here: losing a
            // race for a unique key inside an explicit transaction is a retryable
            // conflict. A duplicate produced by an autocommit statement, CREATE
            // INDEX or LOAD is deterministic and stays a permanent SQL error.
            rollback_index_inserts();
            if (context_ != nullptr && context_->txn_ != nullptr && context_->txn_->get_txn_mode()) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::UNIQUE_KEY_CONFLICT);
            }
            throw;
        } catch (...) {
            rollback_index_inserts();
            throw;
        }
        if (context_ != nullptr && context_->txn_ != nullptr) {
            context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, *tab_name_, rid_));
            context_->txn_->append_modified_slot(*tab_name_, rid_);

            if (context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
                context_->txn_mgr_ != nullptr) {
                txn_id_t writer_id = context_->txn_->get_transaction_id();
                if (context_->txn_mgr_->CheckWriteAgainstReaders(writer_id, rid_, *tab_name_, std::nullopt,
                                                                 std::optional<RmRecord>(rec), tab_->cols)) {
                    throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
                }
            }
        }
        executed_ = true;
    }
    Rid& rid() {
        return rid_;
    }
};
