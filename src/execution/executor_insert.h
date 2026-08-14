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
#include <mutex>

#include "execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle* fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager* sm_manager_;

    static std::vector<char> make_index_key(const IndexMeta& index, const char* rec_data) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i) {
            memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

    void check_mvcc_unique_key_conflict(const IndexMeta& index, const std::vector<char>& key) {
        if (context_ == nullptr || context_->txn_ == nullptr || context_->txn_mgr_ == nullptr) {
            return;
        }

        const std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
        auto candidate_rids = sm_manager_->get_historical_index_key_rids(tab_name_, index_name, key);

        for (const auto& existing_rid : candidate_rids) {
            if (HistoricalIndexKeyConflictsWithTxn(fh_, existing_rid, index, key, context_)) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
            }
        }
    }

public:
    InsertExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Value> values, Context* context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto& col = tab_.cols[i];
            auto& val = values_[i];
            if (val.is_null) {
                throw RMDBError("INSERT cannot store NULL values");
            }
            if (col.type != val.type) {
                if (!can_cast(col.type, val.type)) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                // Convert value type for storage (e.g., INT literal into FLOAT column)
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                    val.set_float(static_cast<double>(val.int_val));
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

        if (context_ != nullptr && context_->txn_ != nullptr &&
            context_->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED &&
            DeletedTupleCandidatesConflictWithInsert(fh_, sm_manager_, tab_name_, rec, context_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
        }

        std::vector<std::vector<char>> index_keys;
        index_keys.reserve(tab_.indexes.size());
        for (const auto& index : tab_.indexes) {
            auto key = make_index_key(index, rec.data);
            check_mvcc_unique_key_conflict(index, key);
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            std::vector<Rid> result;
            if (ih->get_value(key.data(), &result, context_ == nullptr ? nullptr : context_->txn_)) {
                throw IndexEntryExistsError();
            }
            index_keys.push_back(std::move(key));
        }

        std::unique_lock<std::mutex> physical_lock(fh_->get_physical_latch());
        auto prepared_insert = fh_->prepare_insert_record();
        bool insert_finished = false;
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
                InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
                prepared_insert.page_handle.page->set_page_lsn(lsn);
            }
            fh_->finish_insert_record(prepared_insert, rec.data,
                                      context_ != nullptr && context_->txn_ != nullptr ? &pending_meta : nullptr);
            insert_finished = true;
        } catch (...) {
            if (!insert_finished) {
                fh_->abort_prepared_insert(prepared_insert);
            }
            throw;
        }
        physical_lock.unlock();

        std::vector<size_t> inserted_indexes;
        try {
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->insert_entry(index_keys[i].data(), rid_, context_ == nullptr ? nullptr : context_->txn_);
                inserted_indexes.push_back(i);
            }
        } catch (...) {
            for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
                auto& index = tab_.indexes[*it];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->delete_entry(index_keys[*it].data(), context_ == nullptr ? nullptr : context_->txn_);
            }
            fh_->delete_record(rid_, context_);
            throw;
        }
        if (context_ != nullptr && context_->txn_ != nullptr) {
            context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_name_, rid_));
            context_->txn_->append_modified_slot(tab_name_, rid_);

            if (context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
                context_->txn_mgr_ != nullptr) {
                txn_id_t writer_id = context_->txn_->get_transaction_id();
                if (context_->txn_mgr_->CheckWriteAgainstReaders(writer_id, rid_, tab_name_, std::nullopt,
                                                                 std::optional<RmRecord>(rec), tab_.cols)) {
                    throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
                }
            }
        }
        return nullptr;
    }
    Rid& rid() override {
        return rid_;
    }
};

class InsertSelectExecutor : public AbstractExecutor {
private:
    SmManager* sm_manager_;
    std::string tab_name_;
    std::vector<std::string> target_col_names_;
    std::unique_ptr<AbstractExecutor> source_;
    Context* context_;
    Rid rid_;

    static Value read_value(const RmRecord& record, const ColMeta& col, bool is_null) {
        Value value;
        if (is_null) {
            value.set_null();
            return value;
        }
        const char* data = record.data + col.offset;
        switch (col.type) {
        case TYPE_INT:
            value.set_int(*reinterpret_cast<const int*>(data));
            break;
        case TYPE_FLOAT:
            value.set_float(*reinterpret_cast<const double*>(data));
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            value.set_str(execution_scalar::trim_string(data, col.len));
            value.type = col.type;
            break;
        }
        return value;
    }

public:
    InsertSelectExecutor(SmManager* sm_manager, std::string tab_name, std::vector<std::string> target_col_names,
                         std::unique_ptr<AbstractExecutor> source, Context* context)
        : sm_manager_(sm_manager), tab_name_(std::move(tab_name)), target_col_names_(std::move(target_col_names)),
          source_(std::move(source)), context_(context) {
        source_->beginTuple();
    }

    std::unique_ptr<RmRecord> Next() override {
        const auto& table = sm_manager_->db_.get_table(tab_name_);
        if (target_col_names_.empty() && source_->cols().size() != table.cols.size()) {
            throw InvalidValueCountError();
        }
        if (!target_col_names_.empty() &&
            (target_col_names_.size() != table.cols.size() || target_col_names_.size() != source_->cols().size())) {
            throw InvalidValueCountError();
        }
        if (!target_col_names_.empty()) {
            for (size_t i = 0; i < target_col_names_.size(); ++i) {
                for (size_t j = 0; j < i; ++j) {
                    if (target_col_names_[i] == target_col_names_[j]) {
                        throw RMDBError("Duplicate INSERT target column: " + target_col_names_[i]);
                    }
                }
                auto target = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& col) {
                    return col.name == target_col_names_[i];
                });
                if (target == table.cols.end()) {
                    throw ColumnNotFoundError(target_col_names_[i]);
                }
            }
        }

        for (; !source_->is_end(); source_->nextTuple()) {
            auto source_record = source_->Next();
            if (source_record == nullptr) {
                continue;
            }
            std::vector<Value> values(table.cols.size());
            for (size_t source_idx = 0; source_idx < source_->cols().size(); ++source_idx) {
                std::string target_name = target_col_names_.empty() ? table.cols[source_idx].name
                                                                     : target_col_names_[source_idx];
                auto target = std::find_if(table.cols.begin(), table.cols.end(), [&](const ColMeta& col) {
                    return col.name == target_name;
                });
                if (target == table.cols.end()) {
                    throw ColumnNotFoundError(target_name);
                }
                size_t target_idx = static_cast<size_t>(target - table.cols.begin());
                bool is_null = source_idx < source_->nulls().size() && source_->nulls()[source_idx];
                values[target_idx] = read_value(*source_record, source_->cols()[source_idx], is_null);
            }
            InsertExecutor insert(sm_manager_, tab_name_, std::move(values), context_);
            insert.Next();
            rid_ = insert.rid();
        }
        return nullptr;
    }

    Rid& rid() override {
        return rid_;
    }
};
