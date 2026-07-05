/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "executor_abstract.h"
#include "transaction/transaction_manager.h"

namespace rmdb::exec {
class FilterExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::unique_ptr<RmRecord> buffered_record_;
    bool isend_ = true;
    bool predicate_recorded_ = false;

    bool should_track_ssi_reads() const {
        return context_ != nullptr && context_->enable_ssi_read_tracking && context_->txn != nullptr &&
               context_->txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context_->txn_mgr != nullptr &&
               !scan_table_name().empty();
    }

    void record_predicate_read() {
        if (predicate_recorded_ || !should_track_ssi_reads()) {
            return;
        }
        predicate_recorded_ = true;
        if (context_->txn_mgr->RecordPredicateRead(context_->txn, scan_table_name(), scan_conditions())) {
            throw TransactionAbortException(context_->txn->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    bool matches(const RmRecord& rec) {
        for (const auto& cond : conds_) {
            if (!compare(cond, rec)) {
                return false;
            }
        }
        return true;
    }

    void advance_to_match() {
        buffered_record_ = nullptr;
        while (!prev_->is_end()) {
            auto rec = prev_->Next();
            if (rec != nullptr && matches(*rec)) {
                buffered_record_ = std::move(rec);
                _abstract_rid = prev_->rid();
                prev_->record_current_read_for_ssi();
                isend_ = false;
                return;
            }
            prev_->nextTuple();
        }
        isend_ = true;
    }

public:
    FilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds) {
        prev_ = std::move(prev);
        context_ = prev_->context_;
        conds_ = std::move(conds);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
    }

    void beginTuple() override {
        record_predicate_read();
        bool old_tracking = context_ != nullptr ? context_->enable_ssi_read_tracking : false;
        bool suppress_child_tracking = should_track_ssi_reads();
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking = false;
        }
        prev_->beginTuple();
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking = old_tracking;
        }
        advance_to_match();
    }

    void nextTuple() override {
        bool old_tracking = context_ != nullptr ? context_->enable_ssi_read_tracking : false;
        bool suppress_child_tracking = should_track_ssi_reads();
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking = false;
        }
        if (!prev_->is_end()) {
            prev_->nextTuple();
        }
        if (context_ != nullptr && suppress_child_tracking) {
            context_->enable_ssi_read_tracking = old_tracking;
        }
        advance_to_match();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || buffered_record_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*buffered_record_);
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return isend_;
    }

    std::string getType() override {
        return "FilterExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = get_col(cols_, target);
        return *pos;
    }

    void set_counting_enabled(bool enabled) override {
        prev_->set_counting_enabled(enabled);
    }

    void set_key_conditions(std::vector<Condition> key_conds) override {
        prev_->set_key_conditions(std::move(key_conds));
    }

    std::string scan_table_name() const override {
        return prev_->scan_table_name();
    }

    std::vector<Condition> scan_conditions() const override {
        auto conds = prev_->scan_conditions();
        conds.insert(conds.end(), conds_.begin(), conds_.end());
        return conds;
    }
};

} // namespace rmdb::exec

namespace rmdb {
using exec::FilterExecutor;
} // namespace rmdb
