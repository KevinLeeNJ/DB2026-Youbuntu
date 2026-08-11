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

#include "execution_defs.h"
#include "execution_common.h"
#include "executor_abstract.h"
#include "errors.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同
    std::vector<ConditionAddress> condition_addresses_;

    Rid rid_;
    std::unique_ptr<RecScan> scan_; // table_iterator

    SmManager* sm_manager_;
    bool predicate_recorded_{false};
    RmRecordViewWithMeta buffered_tuple_;

    void record_predicate_read() {
        if (predicate_recorded_ || context_ == nullptr || !context_->enable_ssi_read_tracking_ ||
            context_->txn_ == nullptr || context_->txn_->get_isolation_level() != IsolationLevel::SERIALIZABLE ||
            context_->txn_mgr_ == nullptr) {
            return;
        }
        predicate_recorded_ = true;
        if (context_->txn_mgr_->RecordPredicateRead(context_->txn_, tab_name_, fed_conds_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::SSI_DANGER);
        }
        if (context_->txn_mgr_->CheckPredicateInvisibleWrites(context_->txn_->get_transaction_id(), tab_name_,
                                                              fed_conds_, fh_, cols_)) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    void record_tuple_read(const Rid& rid, bool force = false) {
        if (context_ == nullptr || (!force && !context_->enable_ssi_read_tracking_) || context_->txn_ == nullptr ||
            context_->txn_->get_isolation_level() != IsolationLevel::SERIALIZABLE || context_->txn_mgr_ == nullptr) {
            return;
        }
        auto* txn_mgr = context_->txn_mgr_;
        txn_id_t reader_id = context_->txn_->get_transaction_id();
        txn_mgr->RecordRead(reader_id, tab_name_, rid);

        TupleMeta meta = fh_->get_tuple_meta(rid);
        if (meta.writer_txn_id_ == reader_id || meta.writer_txn_id_ == INVALID_TXN_ID) {
            return;
        }
        bool invisible = !meta.is_committed_ || meta.commit_ts_ > context_->txn_->get_read_ts();
        if (invisible && txn_mgr->CheckInvisibleWriteEdge(reader_id, meta.writer_txn_id_)) {
            throw TransactionAbortException(reader_id, AbortReason::SSI_DANGER);
        }
    }

public:
    SeqScanExecutor(SmManager* sm_manager, std::string tab_name, const TabMeta& table, RmFileHandle* table_handle,
                    std::vector<Condition> conds, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        fh_ = table_handle;
        cols_ = table.cols;
        // 元组长度取数据文件的 record_size，包含尾部 null bitmap，使 cols_ 里
        // 缓存的 null_byte 始终落在元组内（join 平移偏移量时同样依赖这一点）。
        len_ = static_cast<size_t>(fh_->get_file_hdr().record_size);

        context_ = context;

        fed_conds_ = conds_;
        condition_addresses_ = cache_condition_addresses(fed_conds_);
    }
    SeqScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds, Context* context)
        : SeqScanExecutor(sm_manager, tab_name, sm_manager->db_.get_table(tab_name),
                          sm_manager->fhs_.at(tab_name).get(), std::move(conds), context) {}
    std::unique_ptr<RmRecord> visible_record(const Rid& rid) {
        return GetVisibleRecord(fh_, rid, context_);
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件和MVCC可见性的元组停止,并赋值给rid_
     */
    void beginTuple() override {
        record_predicate_read();
        buffered_tuple_ = {};
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto tuple = GetVisibleTuple(fh_, rid_, context_);
            if (tuple.view.data == nullptr) {
                scan_->next();
                continue;
            }
            const TupleView view{tuple.view.data, tuple.view.size};
            const bool match = conditions_match(fed_conds_, condition_addresses_, view);
            if (match) {
                record_tuple_read(rid_);
                buffered_tuple_ = std::move(tuple);
                break;
            }
            scan_->next();
        }
    }
    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件和MVCC可见性的元组停止,并赋值给rid_
     */
    void nextTuple() override {
        buffered_tuple_ = {};
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto tuple = GetVisibleTuple(fh_, rid_, context_);
            if (tuple.view.data == nullptr) {
                scan_->next();
                continue;
            }
            const TupleView view{tuple.view.data, tuple.view.size};
            const bool match = conditions_match(fed_conds_, condition_addresses_, view);
            if (match) {
                record_tuple_read(rid_);
                buffered_tuple_ = std::move(tuple);
                break;
            }
            scan_->next();
        }
    }
    TupleView current() const override {
        if (is_end() || buffered_tuple_.view.data == nullptr) {
            return {};
        }
        return TupleView{buffered_tuple_.view.data, buffered_tuple_.view.size};
    }

    Rid& rid() override {
        return rid_;
    }

    bool is_end() const override {
        return scan_->is_end();
    }
    std::string getType() override {
        return "SeqScanExecutor"; // 返回执行器的名称
    }
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : cols_) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.col_name);
    }
    size_t tupleLen() const override {
        return len_;
    }
    std::string scan_table_name() const override {
        return tab_name_;
    }
    std::string_view scan_table_name_view() const override {
        return tab_name_;
    }
    std::vector<Condition> scan_conditions() const override {
        return fed_conds_;
    }
    const std::vector<Condition>& scan_conditions_ref() const override {
        return fed_conds_;
    }
    void record_current_read_for_ssi() override {
        if (!is_end()) {
            record_tuple_read(rid_, true);
        }
    }
};
