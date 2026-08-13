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

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "executor_abstract.h"

class UnionExecutor : public AbstractExecutor {
private:
    std::vector<std::unique_ptr<AbstractExecutor>> branches_;
    std::vector<ColMeta> cols_;
    std::vector<bool> union_all_;
    std::vector<QuerySetOperator> operators_;
    size_t len_ = 0;
    std::vector<RmRecord> tuples_;
    std::vector<std::vector<bool>> tuple_nulls_;
    size_t cursor_ = 0;
    bool materialized_ = false;

    static std::string read_string_cell(const char* data, int len) {
        return std::string(data, strnlen(data, static_cast<size_t>(len)));
    }

    static void copy_cell(char* dst, const ColMeta& dst_col, const char* src, const ColMeta& src_col) {
        if (dst_col.type == TYPE_FLOAT) {
            double value = src_col.type == TYPE_INT ? static_cast<double>(*reinterpret_cast<const int*>(src))
                                                    : *reinterpret_cast<const double*>(src);
            if (value == 0.0) {
                value = 0.0;
            }
            std::memcpy(dst, &value, sizeof(double));
            return;
        }

        if ((dst_col.type == TYPE_STRING || dst_col.type == TYPE_DATETIME) &&
            (src_col.type == TYPE_STRING || src_col.type == TYPE_DATETIME)) {
            std::memset(dst, 0, static_cast<size_t>(dst_col.len));
            std::string value = read_string_cell(src, src_col.len);
            std::memcpy(dst, value.data(), std::min(value.size(), static_cast<size_t>(dst_col.len)));
            return;
        }

        std::memcpy(dst, src, static_cast<size_t>(dst_col.len));
    }

    RmRecord convert_record(const RmRecord& src_rec, const std::vector<ColMeta>& src_cols) const {
        RmRecord dst_rec(static_cast<int>(len_));
        for (size_t i = 0; i < cols_.size(); ++i) {
            const auto& dst_col = cols_[i];
            const auto& src_col = src_cols[i];
            copy_cell(dst_rec.data + dst_col.offset, dst_col, src_rec.data + src_col.offset, src_col);
        }
        return dst_rec;
    }

    static std::string make_key(const RmRecord& record, const std::vector<bool>& nulls) {
        std::string key(record.data, static_cast<size_t>(record.size));
        for (bool is_null : nulls) {
            key.push_back(is_null ? '\1' : '\0');
        }
        return key;
    }

    void materialize() {
        tuples_.clear();
        tuple_nulls_.clear();
        std::vector<std::vector<RmRecord>> branch_tuples(branches_.size());
        std::vector<std::vector<std::vector<bool>>> branch_nulls(branches_.size());
        for (size_t branch_idx = 0; branch_idx < branches_.size(); ++branch_idx) {
            auto& branch = branches_[branch_idx];
            const auto& branch_cols = branch->cols();
            for (branch->beginTuple(); !branch->is_end(); branch->nextTuple()) {
                auto rec = branch->Next();
                if (rec == nullptr) {
                    continue;
                }
                branch_tuples[branch_idx].push_back(convert_record(*rec, branch_cols));
                branch_nulls[branch_idx].push_back(branch->nulls());
            }
        }
        if (branches_.empty()) {
            materialized_ = true;
            return;
        }

        auto deduplicate = [&](std::vector<RmRecord> tuples, std::vector<std::vector<bool>> nulls) {
            std::unordered_set<std::string> seen;
            std::vector<RmRecord> unique_tuples;
            std::vector<std::vector<bool>> unique_nulls;
            for (size_t i = 0; i < tuples.size(); ++i) {
                if (seen.insert(make_key(tuples[i], nulls[i])).second) {
                    unique_tuples.push_back(std::move(tuples[i]));
                    unique_nulls.push_back(std::move(nulls[i]));
                }
            }
            return std::make_pair(std::move(unique_tuples), std::move(unique_nulls));
        };

        tuples_ = std::move(branch_tuples[0]);
        tuple_nulls_ = std::move(branch_nulls[0]);
        for (size_t branch_idx = 1; branch_idx < branches_.size(); ++branch_idx) {
            QuerySetOperator op = QuerySetOperator::UNION;
            if (branch_idx - 1 < operators_.size()) {
                op = operators_[branch_idx - 1];
            }
            if (op == QuerySetOperator::UNION) {
                bool keep_duplicates = branch_idx - 1 < union_all_.size() && union_all_[branch_idx - 1];
                for (auto& tuple : branch_tuples[branch_idx]) {
                    tuples_.push_back(std::move(tuple));
                }
                for (auto& nulls : branch_nulls[branch_idx]) {
                    tuple_nulls_.push_back(std::move(nulls));
                }
                if (!keep_duplicates) {
                    auto unique = deduplicate(std::move(tuples_), std::move(tuple_nulls_));
                    tuples_ = std::move(unique.first);
                    tuple_nulls_ = std::move(unique.second);
                }
                continue;
            }

            auto left = deduplicate(std::move(tuples_), std::move(tuple_nulls_));
            auto right = deduplicate(std::move(branch_tuples[branch_idx]), std::move(branch_nulls[branch_idx]));
            std::unordered_set<std::string> right_keys;
            for (size_t i = 0; i < right.first.size(); ++i) {
                right_keys.insert(make_key(right.first[i], right.second[i]));
            }
            for (size_t i = 0; i < left.first.size(); ++i) {
                bool present = right_keys.find(make_key(left.first[i], left.second[i])) != right_keys.end();
                if ((op == QuerySetOperator::INTERSECT && present) ||
                    (op == QuerySetOperator::EXCEPT && !present)) {
                    tuples_.push_back(std::move(left.first[i]));
                    tuple_nulls_.push_back(std::move(left.second[i]));
                }
            }
        }
        materialized_ = true;
    }

public:
    UnionExecutor(std::vector<std::unique_ptr<AbstractExecutor>> branches, std::vector<ColMeta> cols,
                  std::vector<bool> union_all = {}, std::vector<QuerySetOperator> operators = {})
        : branches_(std::move(branches)), cols_(std::move(cols)), union_all_(std::move(union_all)),
          operators_(std::move(operators)) {
        if (!cols_.empty()) {
            len_ = static_cast<size_t>(cols_.back().offset + cols_.back().len);
        }
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

    bool is_end() const override {
        return cursor_ >= tuples_.size();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(tuples_[cursor_]);
    }

    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : tuple_nulls_[cursor_];
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    std::string getType() override {
        return "UnionExecutor";
    }

    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
            if (!target.tab_name.empty()) {
                return col.tab_name == target.tab_name && col.name == target.col_name;
            }
            return col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *pos;
    }
};
