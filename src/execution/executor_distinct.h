/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "executor_abstract.h"

class DistinctExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<RmRecord> tuples_;
    std::vector<std::vector<bool>> tuple_nulls_;
    std::unordered_set<std::string> seen_;
    size_t cursor_ = 0;
    bool materialized_ = false;

    std::string make_key(const RmRecord& record, const std::vector<bool>& nulls) const {
        std::string key(record.data, static_cast<size_t>(record.size));
        for (bool is_null : nulls) {
            key.push_back(is_null ? '\1' : '\0');
        }
        return key;
    }

    void materialize() {
        tuples_.clear();
        tuple_nulls_.clear();
        seen_.clear();
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            if (record == nullptr) {
                continue;
            }
            const auto& nulls = prev_->nulls();
            if (seen_.insert(make_key(*record, nulls)).second) {
                tuples_.push_back(*record);
                tuple_nulls_.push_back(nulls);
            }
        }
        materialized_ = true;
    }

public:
    explicit DistinctExecutor(std::unique_ptr<AbstractExecutor> prev) : prev_(std::move(prev)) {
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        context_ = prev_->context_;
    }

    void beginTuple() override {
        if (!materialized_) {
            materialize();
        }
        cursor_ = 0;
    }

    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(tuples_[cursor_]);
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return cursor_ >= tuples_.size();
    }

    std::string getType() override {
        return "DistinctExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : tuple_nulls_[cursor_];
    }

    size_t tupleLen() const override {
        return len_;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = get_col(cols_, target);
        return *pos;
    }
};
