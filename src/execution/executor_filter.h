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

#include "executor_abstract.h"

class FilterExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::unique_ptr<RmRecord> buffered_record_;
    bool isend_ = true;

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
        conds_ = std::move(conds);
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
    }

    void beginTuple() override {
        prev_->beginTuple();
        advance_to_match();
    }

    void nextTuple() override {
        if (!prev_->is_end()) {
            prev_->nextTuple();
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
};
