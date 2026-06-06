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
#include <mutex>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_scan.h"
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

    static bool index_key_equals(const IndexMeta& index, const char* rec_data, const std::vector<char>& key) {
        return make_index_key(index, rec_data) == key;
    }

    bool top_version_is_invisible_to(Transaction* txn, const TupleMeta& meta) const {
        if (txn == nullptr || meta.writer_txn_id_ == txn->get_transaction_id()) {
            return false;
        }
        return !meta.is_committed_ || meta.commit_ts_ > txn->get_start_ts();
    }

    static bool tuples_equal(const std::vector<ColMeta>& cols, const RmRecord& a, const RmRecord& b) {
        for (const auto& col : cols) {
            if (memcmp(a.data + col.offset, b.data + col.offset, col.len) != 0) {
                return false;
            }
        }
        return true;
    }

    /// @brief Full-table scan to check if any invisible (to us) record has the
    /// same tuple values as the one we are about to insert. Required for tables
    /// both with and without indexes — two active transactions cannot insert
    /// the same tuple.
    void check_mvcc_duplicate_insert(const RmRecord& rec) {
        if (context_ == nullptr || context_->txn_ == nullptr || context_->txn_mgr_ == nullptr) {
            return;
        }

        auto* txn = context_->txn_;
        const txn_id_t self_id = txn->get_transaction_id();
        RmScan scan(fh_);
        while (!scan.is_end()) {
            Rid existing_rid = scan.rid();
            TupleMeta meta = fh_->get_tuple_meta(existing_rid);

            if (meta.writer_txn_id_ == self_id) {
                scan.next();
                continue;
            }

            if (!top_version_is_invisible_to(txn, meta)) {
                scan.next();
                continue;
            }

            // Check top version's physical record data
            auto current_rec = fh_->get_record(existing_rid, context_);
            if (current_rec != nullptr && tuples_equal(tab_.cols, *current_rec, rec)) {
                throw TransactionAbortException(self_id, AbortReason::WW_CONFLICT);
            }

            // Traverse undo chain: old versions may carry the same tuple data
            UndoLink link = meta.version_chain_head_;
            while (link.IsValid()) {
                UndoLog undo = context_->txn_mgr_->GetUndoLog(link);
                if (!undo.old_tuple_data_.empty()) {
                    RmRecord old_rec(static_cast<int>(undo.old_tuple_data_.size()));
                    memcpy(old_rec.data, undo.old_tuple_data_.data(), undo.old_tuple_data_.size());
                    if (tuples_equal(tab_.cols, old_rec, rec)) {
                        throw TransactionAbortException(self_id, AbortReason::WW_CONFLICT);
                    }
                }
                link = undo.prev_version_;
            }

            scan.next();
        }
    }

    void check_mvcc_unique_key_conflict(const IndexMeta& index, const std::vector<char>& key) {
        if (context_ == nullptr || context_->txn_ == nullptr || context_->txn_mgr_ == nullptr) {
            return;
        }

        auto* txn = context_->txn_;
        const txn_id_t self_id = txn->get_transaction_id();
        RmScan scan(fh_);
        while (!scan.is_end()) {
            Rid existing_rid = scan.rid();
            TupleMeta meta = fh_->get_tuple_meta(existing_rid);
            auto current_rec = fh_->get_record(existing_rid, context_);

            if (current_rec != nullptr && index_key_equals(index, current_rec->data, key) &&
                meta.writer_txn_id_ != self_id && top_version_is_invisible_to(txn, meta)) {
                throw TransactionAbortException(self_id, AbortReason::WW_CONFLICT);
            }

            bool has_invisible_top_version = top_version_is_invisible_to(txn, meta);
            UndoLink link = meta.version_chain_head_;
            while (has_invisible_top_version && link.IsValid()) {
                UndoLog undo = context_->txn_mgr_->GetUndoLog(link);
                if (!undo.old_tuple_data_.empty() && index_key_equals(index, undo.old_tuple_data_.data(), key)) {
                    throw TransactionAbortException(self_id, AbortReason::WW_CONFLICT);
                }
                link = undo.prev_version_;
            }

            scan.next();
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
            if (col.type != val.type) {
                if (!can_cast(col.type, val.type)) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                // Convert value type for storage (e.g., INT literal into FLOAT column)
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                    val.set_float(static_cast<float>(val.int_val));
                } else if (col.type == TYPE_INT && val.type == TYPE_FLOAT) {
                    val.set_int(static_cast<int>(val.float_val));
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // Full-tuple duplicate check: two active transactions cannot insert the
        // same tuple. Must run before the index-loop so tables without indexes
        // are also covered.
        check_mvcc_duplicate_insert(rec);

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
            if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr) {
                InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
                prepared_insert.page_handle.page->set_page_lsn(lsn);
            }
            fh_->finish_insert_record(prepared_insert, rec.data);
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
            context_->txn_->append_write_record(new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_));
            // Initialize TupleMeta for MVCC
            TupleMeta meta;
            meta.writer_txn_id_ = context_->txn_->get_transaction_id();
            meta.is_committed_ = false;
            meta.is_deleted_ = false;
            meta.version_chain_head_ = UndoLink{};
            fh_->set_tuple_meta(rid_, meta);
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
