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
    size_t len_ = 0;      // 输出元组总长度（数据区 + null bitmap）
    size_t data_len_ = 0; // 数据区长度，即 null bitmap 的起始偏移
    std::vector<RmRecord> tuples_;
    std::unordered_set<std::string> seen_;
    size_t cursor_ = 0;
    bool materialized_ = false;

    static std::string read_string_cell(const char* data, int len) {
        return std::string(data, strnlen(data, static_cast<size_t>(len)));
    }

    static void copy_cell(char* dst, const ColMeta& dst_col, const char* src, const ColMeta& src_col) {
        if (dst_col.type == TYPE_FLOAT) {
            float value = src_col.type == TYPE_INT ? static_cast<float>(read_unaligned<int>(src)) : read_float(src);
            if (value == 0.0) {
                value = 0.0;
            }
            write_float(dst, value);
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
        return convert_view(TupleView{src_rec.data, static_cast<uint32_t>(src_rec.size)}, src_cols);
    }

    // UNION 重新打包元组，因此按输出列顺序重建尾部 null bitmap。NULL 单元的数据
    // 字节置零，使去重用的 seen_ 键对同一个 NULL 稳定。
    RmRecord convert_view(TupleView src_view, const std::vector<ColMeta>& src_cols) const {
        RmRecord dst_rec(static_cast<int>(len_));
        std::memset(dst_rec.data + data_len_, 0, static_cast<size_t>(len_) - data_len_);
        for (size_t i = 0; i < cols_.size(); ++i) {
            const auto& dst_col = cols_[i];
            const auto& src_col = src_cols[i];
            if (is_null(src_view.data, src_col)) {
                std::memset(dst_rec.data + dst_col.offset, 0, static_cast<size_t>(dst_col.len));
                set_null(dst_rec.data, dst_col);
                continue;
            }
            copy_cell(dst_rec.data + dst_col.offset, dst_col, src_view.data + src_col.offset, src_col);
        }
        return dst_rec;
    }

    void materialize() {
        tuples_.clear();
        seen_.clear();
        for (auto& branch : branches_) {
            const auto& branch_cols = branch->cols();
            for (branch->beginTuple(); !branch->is_end(); branch->nextTuple()) {
                TupleView view = branch->current();
                if (!view) {
                    throw InternalError("cursor returned an empty tuple");
                }
                if (!view) {
                    continue;
                }
                RmRecord converted = convert_view(view, branch_cols);
                std::string key(converted.data, static_cast<size_t>(converted.size));
                if (seen_.insert(key).second) {
                    tuples_.push_back(converted);
                }
            }
        }
        materialized_ = true;
    }

public:
    UnionExecutor(std::vector<std::unique_ptr<AbstractExecutor>> branches, std::vector<ColMeta> cols)
        : branches_(std::move(branches)), cols_(std::move(cols)) {
        if (!cols_.empty()) {
            data_len_ = static_cast<size_t>(cols_.back().offset + cols_.back().len);
        }
        bind_null_positions(cols_, static_cast<int>(data_len_));
        len_ = data_len_ + static_cast<size_t>(null_bitmap_bytes(cols_.size()));
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

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        return TupleView{tuples_[cursor_].data, static_cast<uint32_t>(tuples_[cursor_].size)};
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
