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
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "index/ix_scan.h"
#include "execution_common.h"
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
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto& cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    bool is_end() const override {
        return !scan_ || scan_->is_end();
    }

    void beginTuple() override {
        // Build key buffers for lower and upper bound
        char* lower_key = new char[index_meta_.col_tot_len]();
        char* upper_key = new char[index_meta_.col_tot_len]();
        bool has_lower = false, has_upper = false;
        bool gt_lower = false, lt_upper = false;

        for (auto& cond : fed_conds_) {
            if (!cond.is_rhs_val)
                continue;
            for (size_t j = 0; j < index_col_names_.size(); j++) {
                if (cond.lhs_col.col_name == index_col_names_[j]) {
                    auto& idx_col = index_meta_.cols[j];
                    cond.rhs_val.init_raw(idx_col.len);
                    switch (cond.op) {
                    case OP_EQ:
                        memcpy(lower_key + idx_col.offset, cond.rhs_val.raw->data, idx_col.len);
                        memcpy(upper_key + idx_col.offset, cond.rhs_val.raw->data, idx_col.len);
                        has_lower = true;
                        has_upper = true;
                        break;
                    case OP_GE:
                        memcpy(lower_key + idx_col.offset, cond.rhs_val.raw->data, idx_col.len);
                        has_lower = true;
                        break;
                    case OP_GT:
                        memcpy(lower_key + idx_col.offset, cond.rhs_val.raw->data, idx_col.len);
                        has_lower = true;
                        gt_lower = true;
                        break;
                    case OP_LE:
                        memcpy(upper_key + idx_col.offset, cond.rhs_val.raw->data, idx_col.len);
                        has_upper = true;
                        break;
                    case OP_LT:
                        memcpy(upper_key + idx_col.offset, cond.rhs_val.raw->data, idx_col.len);
                        has_upper = true;
                        lt_upper = true;
                        break;
                    default:
                        break;
                    }
                }
            }
        }

        IxIndexHandle* ih =
            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();

        Iid lower_iid;
        if (has_lower) {
            lower_iid = gt_lower ? ih->upper_bound(lower_key) : ih->lower_bound(lower_key);
        } else {
            lower_iid = ih->leaf_begin();
        }

        Iid upper_iid;
        if (has_upper) {
            upper_iid = lt_upper ? ih->lower_bound(upper_key) : ih->upper_bound(upper_key);
        } else {
            upper_iid = ih->leaf_end();
        }

        delete[] lower_key;
        delete[] upper_key;

        scan_ = std::make_unique<IxScan>(ih, lower_iid, upper_iid, sm_manager_->get_bpm());
        advance_to_valid();
    }

    void nextTuple() override {
        scan_->next();
        advance_to_valid();
    }

    std::unique_ptr<RmRecord> Next() override {
        rid_ = scan_->rid();
        return fh_->get_record(rid_, context_);
    }

    Rid& rid() override {
        return rid_;
    }

private:
    void advance_to_valid() {
        while (!scan_->is_end()) {
            Rid current_rid = scan_->rid();
            if (fh_->is_record(current_rid)) {
                auto rec = fh_->get_record(current_rid, context_);
                if (eval_conds(fed_conds_, cols_, *rec)) {
                    return; // Found a valid, matching record
                }
            }
            scan_->next();
        }
    }
};