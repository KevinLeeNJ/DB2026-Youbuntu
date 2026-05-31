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

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <set>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;             // 表名称
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> conds_;     // 扫描条件
    RmFileHandle* fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // 需要读取的字段
    size_t len_;                       // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_; // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager* sm_manager_;
    bool predicate_recorded_{false};
    bool materialized_mode_{false};

    struct MatchedTuple {
        Rid rid;
        RmRecord record;
    };
    std::vector<MatchedTuple> materialized_;
    size_t materialized_pos_{0};

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
            float value = std::numeric_limits<float>::lowest();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_STRING:
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
            float value = std::numeric_limits<float>::max();
            memcpy(dest, &value, col.len);
            break;
        }
        case TYPE_STRING:
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
            float converted = value.type == TYPE_INT ? static_cast<float>(value.int_val) : value.float_val;
            memcpy(data.data(), &converted, col.len);
            break;
        }
        case TYPE_STRING:
            memcpy(data.data(), value.str_val.c_str(), std::min(static_cast<int>(value.str_val.size()), col.len));
            break;
        }
        return data;
    }

    static int compare_key_part(const std::vector<char>& lhs, const std::vector<char>& rhs, const ColMeta& col) {
        return ix_compare(lhs.data(), rhs.data(), col.type, col.len);
    }

    int compare_index_record(const RmRecord& lhs, const RmRecord& rhs) const {
        for (const auto& col : index_meta_.cols) {
            int cmp = ix_compare(lhs.data + col.offset, rhs.data + col.offset, col.type, col.len);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    bool need_snapshot_safe_scan() const {
        if (context_ == nullptr || context_->txn_ == nullptr) {
            return false;
        }
        auto level = context_->txn_->get_isolation_level();
        return level == IsolationLevel::SNAPSHOT_ISOLATION || level == IsolationLevel::SERIALIZABLE;
    }

    void begin_materialized_scan() {
        materialized_mode_ = true;
        scan_.reset();
        materialized_.clear();
        materialized_pos_ = 0;

        RmScan scan(fh_);
        while (!scan.is_end()) {
            auto current_rid = scan.rid();
            auto rec = fh_->get_record(current_rid, context_);
            scan.next();
            if (rec == nullptr) {
                continue;
            }
            bool match = true;
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (!match) {
                continue;
            }
            materialized_.push_back(MatchedTuple{current_rid, *rec});
        }

        std::sort(materialized_.begin(), materialized_.end(), [this](const MatchedTuple& lhs, const MatchedTuple& rhs) {
            int cmp = compare_index_record(lhs.record, rhs.record);
            if (cmp != 0) {
                return cmp < 0;
            }
            if (lhs.rid.page_no != rhs.rid.page_no) {
                return lhs.rid.page_no < rhs.rid.page_no;
            }
            return lhs.rid.slot_no < rhs.rid.slot_no;
        });

        if (!materialized_.empty()) {
            rid_ = materialized_[0].rid;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->SsiRecordRead(context_->txn_, tab_name_, rid_);
            }
        }
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
    }

    void beginTuple() override {
        if (!predicate_recorded_ && context_ != nullptr && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->SsiRecordPredicateRead(context_->txn_, tab_name_, fed_conds_);
            predicate_recorded_ = true;
        }
        if (need_snapshot_safe_scan()) {
            begin_materialized_scan();
            return;
        }
        materialized_mode_ = false;

        auto ih =
            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
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

        Iid lower = lower_exclusive ? ih->upper_bound(lower_key.data()) : ih->lower_bound(lower_key.data());
        Iid upper = upper_inclusive ? ih->upper_bound(upper_key.data()) : ih->lower_bound(upper_key.data());
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        advance_to_match();
    }

    void nextTuple() override {
        if (materialized_mode_) {
            ++materialized_pos_;
            if (!is_end()) {
                rid_ = materialized_[materialized_pos_].rid;
                if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                    context_->txn_mgr_->SsiRecordRead(context_->txn_, tab_name_, rid_);
                }
            }
            return;
        }
        scan_->next();
        advance_to_match();
    }

    void advance_to_match() {
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
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
                if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                    context_->txn_mgr_->SsiRecordRead(context_->txn_, tab_name_, rid_);
                }
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        if (materialized_mode_) {
            return std::make_unique<RmRecord>(materialized_[materialized_pos_].record);
        }
        return fh_->get_record(rid_, context_);
    }

    Rid& rid() override {
        return rid_;
    }

    bool is_end() const override {
        if (materialized_mode_) {
            return materialized_pos_ >= materialized_.size();
        }
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
};
