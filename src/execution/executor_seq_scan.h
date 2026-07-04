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
#include "execution_manager.h"
#include "executor_abstract.h"
#include "errors.h"
#include "index/ix.h"
#include "system/sm.h"
#include "system/schema_manager.h"

class SeqScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_; // table_iterator

    SchemaManager* schema_manager_;
    bool predicate_recorded_{false};

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
        bool invisible = !meta.is_committed_ || meta.commit_ts_ > context_->txn_->get_start_ts();
        if (invisible && txn_mgr->CheckInvisibleWriteEdge(reader_id, meta.writer_txn_id_)) {
            throw TransactionAbortException(reader_id, AbortReason::SSI_DANGER);
        }
    }

public:
    SeqScanExecutor(SchemaManager* schema_manager, std::string tab_name, std::vector<Condition> conds,
                    Context* context) {
        schema_manager_ = schema_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta& tab = schema_manager_->catalog().get_table(tab_name_);
        fh_ = schema_manager_->get_table_handle(tab_name_);
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }
    std::unique_ptr<RmRecord> visible_record(const Rid& rid) {
        return GetVisibleRecord(fh_, rid, context_);
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件和MVCC可见性的元组停止,并赋值给rid_
     */
    void beginTuple() override {
        record_predicate_read();
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = visible_record(rid_);
            if (rec == nullptr) {
                scan_->next();
                continue;
            }
            bool match = true;
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                record_tuple_read(rid_);
                break;
            }
            scan_->next();
        }
    }
    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件和MVCC可见性的元组停止,并赋值给rid_
     */
    void nextTuple() override {
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = visible_record(rid_);
            if (rec == nullptr) {
                scan_->next();
                continue;
            }
            bool match = true;
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                record_tuple_read(rid_);
                break;
            }
            scan_->next();
        }
    }
    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end())
            return nullptr;
        return visible_record(rid_);
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
    std::vector<Condition> scan_conditions() const override {
        return fed_conds_;
    }
    void record_current_read_for_ssi() override {
        if (!is_end()) {
            record_tuple_read(rid_, true);
        }
    }
};
