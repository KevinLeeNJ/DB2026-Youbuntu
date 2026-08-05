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

class FilterExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    std::vector<ConditionAddress> condition_addresses_;
    size_t len_;
    std::unique_ptr<RmRecord> fallback_record_;
    TupleView current_view_;
    mutable std::vector<Condition> scan_conditions_cache_;
    bool isend_ = true;
    bool predicate_recorded_ = false;

    bool should_track_ssi_reads() const {
        return context_ != nullptr && context_->enable_ssi_read_tracking_ && context_->txn_ != nullptr &&
               context_->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE && context_->txn_mgr_ != nullptr &&
               !scan_table_name_view().empty();
    }

    void record_predicate_read() {
        if (predicate_recorded_ || !should_track_ssi_reads()) {
            return;
        }
        predicate_recorded_ = true;
        if (context_->txn_mgr_->RecordPredicateRead(context_->txn_, std::string(scan_table_name_view()),
                                                    scan_conditions_ref())) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    bool matches(const TupleView& tuple) {
        return conditions_match(conds_, condition_addresses_, tuple);
    }

    template <typename Callback> void with_child_tracking_suppressed(Callback&& callback) {
        if (!should_track_ssi_reads()) {
            callback();
            return;
        }
        ScopedValueOverride<bool> tracking_override(context_->enable_ssi_read_tracking_, false);
        callback();
    }

    void advance_to_match() {
        fallback_record_.reset();
        current_view_ = {};
        while (!prev_->is_end()) {
            TupleView tuple = prev_->current();
            if (!tuple) {
                fallback_record_ = prev_->Next();
                if (fallback_record_ != nullptr) {
                    tuple = TupleView{fallback_record_->data, static_cast<uint32_t>(fallback_record_->size)};
                }
            }
            if (tuple && matches(tuple)) {
                current_view_ = tuple;
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
        condition_addresses_ = cache_condition_addresses(conds_);
        len_ = prev_->tupleLen();
    }

    void replace_prepared_conditions(std::vector<Condition>& staged_conditions) noexcept {
        assert(staged_conditions.size() == conds_.size());
        conds_.swap(staged_conditions);
        scan_conditions_cache_.clear();
    }

    void clear_prepared_parameters(const std::vector<size_t>& condition_indexes) noexcept {
        clear_prepared_parameter_values(conds_, condition_indexes);
        scan_conditions_cache_.clear();
    }

    const std::vector<Condition>& prepared_conditions_ref() const noexcept {
        return conds_;
    }

    void begin_operation(Context* context) noexcept override {
        context_ = context;
        predicate_recorded_ = false;
        prev_->begin_operation(context);
    }

    void end_operation() noexcept override {
        fallback_record_.reset();
        current_view_ = {};
        scan_conditions_cache_.clear();
        isend_ = true;
        predicate_recorded_ = false;
        prev_->end_operation();
        context_ = nullptr;
    }

    void beginTuple() override {
        record_predicate_read();
        with_child_tracking_suppressed([this] { prev_->beginTuple(); });
        advance_to_match();
    }

    void nextTuple() override {
        with_child_tracking_suppressed([this] {
            if (!prev_->is_end()) {
                prev_->nextTuple();
            }
        });
        advance_to_match();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end() || !current_view_) {
            return nullptr;
        }
        auto copy = std::make_unique<RmRecord>(static_cast<int>(current_view_.size));
        memcpy(copy->data, current_view_.data, current_view_.size);
        return copy;
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return isend_;
    }

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        return current_view_;
    }

    std::string getType() override {
        return "FilterExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return prev_->cols();
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = get_col(prev_->cols(), target);
        return *pos;
    }

    void set_counting_enabled(bool enabled) override {
        prev_->set_counting_enabled(enabled);
    }

    void set_key_conditions(std::vector<Condition> key_conds) override {
        prev_->set_key_conditions(std::move(key_conds));
    }

    void set_lookup_key(const TabCol& target, const char* key, size_t len) override {
        prev_->set_lookup_key(target, key, len);
    }

    std::string scan_table_name() const override {
        return prev_->scan_table_name();
    }
    std::string_view scan_table_name_view() const override {
        return prev_->scan_table_name_view();
    }

    std::vector<Condition> scan_conditions() const override {
        auto conds = prev_->scan_conditions();
        conds.insert(conds.end(), conds_.begin(), conds_.end());
        return conds;
    }
    const std::vector<Condition>& scan_conditions_ref() const override {
        scan_conditions_cache_ = prev_->scan_conditions_ref();
        scan_conditions_cache_.insert(scan_conditions_cache_.end(), conds_.begin(), conds_.end());
        return scan_conditions_cache_;
    }
};
