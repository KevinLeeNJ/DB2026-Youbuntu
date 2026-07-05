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

#include "execution_defs.h"
#include "executor_abstract.h"

namespace rmdb::exec {
class LimitExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    size_t limit_ = 0;
    size_t returned_ = 0;

public:
    LimitExecutor(std::unique_ptr<AbstractExecutor> prev, size_t limit) : prev_(std::move(prev)), limit_(limit) {}

    void beginTuple() override {
        returned_ = 0;
        prev_->beginTuple();
    }

    void nextTuple() override {
        if (is_end()) {
            return;
        }
        ++returned_;
        prev_->nextTuple();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return prev_->Next();
    }

    bool is_end() const override {
        return returned_ >= limit_ || prev_->is_end();
    }

    Rid& rid() override {
        if (prev_ != nullptr) {
            return prev_->rid();
        }
        return _abstract_rid;
    }

    std::string getType() override {
        return "LimitExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return prev_->cols();
    }

    size_t tupleLen() const override {
        return prev_->tupleLen();
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return prev_->get_col_offset(target);
    }
};

} // namespace rmdb::exec

namespace rmdb {
using exec::LimitExecutor;
} // namespace rmdb
