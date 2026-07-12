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

#include <limits>
#include <map>
#include <optional>
#include <set>
#include <utility>

#include "execution_defs.h"
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
protected:
    std::string tab_name_;              // 表名称
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // 扫描条件
    RmFileHandle* fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // 需要读取的字段
    size_t len_;                        // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;  // 扫描条件，和conds_字段相同
    std::vector<Condition> base_conds_; // original conditions from construction, for INLJ key injection

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    bool predicate_recorded_{false};
    bool use_heap_scan_for_mvcc_{false};
    std::unique_ptr<RmRecord> buffered_record_;

    SmManager* sm_manager_;

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

    struct BoundValue {
        std::vector<char> data;
        bool inclusive = true;
    };

    struct ColumnConstraint {
        std::optional<std::vector<char>> eq;
        std::optional<BoundValue> lower;
        std::optional<BoundValue> upper;
    };

    static void write_min(char* dest, const ColMeta& col) {
        switch (col.type) {
        case TYPE_INT: {
            int value = std::numeric_limits<int>::min();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double value = std::numeric_limits<double>::lowest();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memset(dest, 0, col.len);
            break;
        }
    }

    static void write_max(char* dest, const ColMeta& col) {
        switch (col.type) {
        case TYPE_INT: {
            int value = std::numeric_limits<int>::max();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double value = std::numeric_limits<double>::max();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memset(dest, 0xFF, col.len);
            break;
        }
    }

    static std::vector<char> value_to_key_part(const Value& value, const ColMeta& col) {
        std::vector<char> data(col.len, 0);
        switch (col.type) {
        case TYPE_INT: {
            int converted = value.type == TYPE_FLOAT ? static_cast<int>(value.float_val) : value.int_val;
            memcpy(data.data(), &converted, col.len);
            break;
        }
        case TYPE_FLOAT: {
            double converted = value.type == TYPE_INT ? static_cast<double>(value.int_val) : value.float_val;
            memcpy(data.data(), &converted, col.len);
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME:
            memcpy(data.data(), value.str_val.c_str(), std::min(static_cast<int>(value.str_val.size()), col.len));
            break;
        }
        return data;
    }

    static int compare_key_part(const std::vector<char>& lhs, const std::vector<char>& rhs, const ColMeta& col) {
        return ix_compare(lhs.data(), rhs.data(), col.type, col.len);
    }

    std::map<std::string, ColumnConstraint> build_constraints() const {
        std::map<std::string, ColMeta> col_meta;
        for (const auto& col : index_meta_.cols) {
            col_meta[col.name] = col;
        }

        std::map<std::string, ColumnConstraint> constraints;
        for (const auto& cond : conds_) {
            if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name_) {
                continue;
            }
            auto meta_it = col_meta.find(cond.lhs_col.col_name);
            if (meta_it == col_meta.end() || cond.op == OP_NE) {
                continue;
            }
            const auto& col = meta_it->second;
            auto value = value_to_key_part(cond.rhs_val, col);
            auto& constraint = constraints[col.name];
            switch (cond.op) {
            case OP_EQ:
                constraint.eq = value;
                break;
            case OP_GT:
            case OP_GE: {
                bool inclusive = cond.op == OP_GE;
                if (!constraint.lower || compare_key_part(value, constraint.lower->data, col) > 0 ||
                    (compare_key_part(value, constraint.lower->data, col) == 0 && !inclusive)) {
                    constraint.lower = BoundValue{value, inclusive};
                }
                break;
            }
            case OP_LT:
            case OP_LE: {
                bool inclusive = cond.op == OP_LE;
                if (!constraint.upper || compare_key_part(value, constraint.upper->data, col) < 0 ||
                    (compare_key_part(value, constraint.upper->data, col) == 0 && !inclusive)) {
                    constraint.upper = BoundValue{value, inclusive};
                }
                break;
            }
            case OP_NE:
                break;
            }
        }
        return constraints;
    }

    bool needs_historical_heap_scan() const {
        if (context_ == nullptr || context_->txn_ == nullptr) {
            return false;
        }
        IsolationLevel level = context_->txn_->get_isolation_level();
        return level == IsolationLevel::SNAPSHOT_ISOLATION || level == IsolationLevel::REPEATABLE_READ ||
               (level == IsolationLevel::SERIALIZABLE && context_->txn_->get_txn_mode());
    }

public:
    IndexScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context* context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        for (auto& cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_comp_op(cond.op);
            }
        }
        fed_conds_ = conds_;
        base_conds_ = conds_; // save original conditions before any key injection
    }

    void beginTuple() override {
        record_predicate_read();

        use_heap_scan_for_mvcc_ = needs_historical_heap_scan();
        if (use_heap_scan_for_mvcc_) {
            scan_ = std::make_unique<RmScan>(fh_);
            advance_to_match();
            return;
        }

        auto ih =
            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
        auto index_latch_guard = ih->lock_shared();
        auto constraints = build_constraints();

        std::vector<char> lower_key(index_meta_.col_tot_len);
        std::vector<char> upper_key(index_meta_.col_tot_len);
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_min(lower_key.data() + offset, col);
            write_max(upper_key.data() + offset, col);
            offset += col.len;
        }

        bool lower_exclusive = false;
        bool upper_inclusive = true;
        bool saw_range = false;
        offset = 0;
        for (const auto& col : index_meta_.cols) {
            auto constraint_it = constraints.find(col.name);
            if (constraint_it == constraints.end() || saw_range) {
                break;
            }

            const auto& constraint = constraint_it->second;
            if (constraint.eq.has_value()) {
                memcpy(lower_key.data() + offset, constraint.eq->data(), col.len);
                memcpy(upper_key.data() + offset, constraint.eq->data(), col.len);
                offset += col.len;
                continue;
            }

            if (constraint.lower.has_value()) {
                memcpy(lower_key.data() + offset, constraint.lower->data.data(), col.len);
                lower_exclusive = !constraint.lower->inclusive;
            }
            if (constraint.upper.has_value()) {
                memcpy(upper_key.data() + offset, constraint.upper->data.data(), col.len);
                upper_inclusive = constraint.upper->inclusive;
            }

            int suffix_offset = offset + col.len;
            if (constraint.lower.has_value() && lower_exclusive) {
                for (size_t i = (&col - index_meta_.cols.data()) + 1; i < index_meta_.cols.size(); ++i) {
                    write_max(lower_key.data() + suffix_offset, index_meta_.cols[i]);
                    suffix_offset += index_meta_.cols[i].len;
                }
            }
            suffix_offset = offset + col.len;
            if (constraint.upper.has_value() && !upper_inclusive) {
                for (size_t i = (&col - index_meta_.cols.data()) + 1; i < index_meta_.cols.size(); ++i) {
                    write_min(upper_key.data() + suffix_offset, index_meta_.cols[i]);
                    suffix_offset += index_meta_.cols[i].len;
                }
            }
            saw_range = true;
            break;
        }

        Iid lower, upper;
        if (!lower_exclusive && upper_inclusive && lower_key == upper_key) {
            auto [lo, hi] = ih->equal_range(lower_key.data());
            lower = lo;
            upper = hi;
        } else {
            lower = lower_exclusive ? ih->upper_bound(lower_key.data()) : ih->lower_bound(lower_key.data());
            upper = upper_inclusive ? ih->upper_bound(upper_key.data()) : ih->lower_bound(upper_key.data());
        }
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm(), std::move(index_latch_guard));
        advance_to_match();
    }

    void nextTuple() override {
        scan_->next();
        advance_to_match();
    }

    void advance_to_match() {
        buffered_record_.reset();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = GetVisibleRecord(fh_, rid_, context_);
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
                buffered_record_ = std::move(rec);
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || buffered_record_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*buffered_record_);
    }

    Rid& rid() override {
        return rid_;
    }

    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    std::string getType() override {
        return "IndexScanExecutor";
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return *get_col(cols_, target);
    }

    size_t tupleLen() const override {
        return len_;
    }

    void set_key_conditions(std::vector<Condition> key_conds) override {
        // Combine base filter conditions with injected key conditions
        conds_ = base_conds_;
        for (auto& kc : key_conds) {
            // Ensure lhs points to this table
            if (kc.lhs_col.tab_name != tab_name_ && !kc.is_rhs_val && kc.rhs_col.tab_name == tab_name_) {
                std::swap(kc.lhs_col, kc.rhs_col);
                kc.op = swap_comp_op(kc.op);
            }
            conds_.push_back(std::move(kc));
        }
        fed_conds_ = conds_;
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

    // An ascending index range scan yields rows ordered by the index columns.
    // A min(col) aggregate on `col` can be answered from the first visible
    // matching row when `col` is an index column and every index column before
    // it is constrained to a single equality value (so the remaining order is
    // monotonic in `col`).
    bool provides_min_order(const TabCol& col) const override {
        if (use_heap_scan_for_mvcc_) {
            return false;
        }
        if (!col.tab_name.empty() && col.tab_name != tab_name_) {
            return false;
        }
        // Locate col within the index column sequence.
        size_t col_pos = index_meta_.cols.size();
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            if (index_meta_.cols[i].name == col.col_name) {
                col_pos = i;
                break;
            }
        }
        if (col_pos == index_meta_.cols.size()) {
            return false; // col not part of this index
        }
        // Every index column preceding col must have an equality predicate on
        // this table, otherwise the scan is not monotonic in col alone.
        for (size_t i = 0; i < col_pos; ++i) {
            const std::string& before_name = index_meta_.cols[i].name;
            bool has_eq = false;
            for (const auto& cond : fed_conds_) {
                if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.col_name == before_name &&
                    (cond.lhs_col.tab_name.empty() || cond.lhs_col.tab_name == tab_name_)) {
                    has_eq = true;
                    break;
                }
            }
            if (!has_eq) {
                return false;
            }
        }
        return true;
    }
};
