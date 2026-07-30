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
#include <type_traits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
    std::vector<ColMeta> cols_;              // 需要投影的字段
    size_t len_;                             // 输出元组总长度（数据区 + null bitmap）
    size_t data_len_ = 0;                    // 输出元组数据区长度，即 null bitmap 的起始偏移
    std::vector<size_t> sel_idxs_;
    std::unique_ptr<RmRecord> current_output_;
    std::unique_ptr<RmRecord> fallback_input_;
    TupleView current_view_;

    // 投影会重新打包元组，所以输出的 null bitmap 也要重排：输出列 i 的 NULL 位
    // 在 data_len_ + i/8 处，与输入侧的位置无关。
    bool materialize_view(TupleView input) {
        if (!input) {
            return false;
        }
        if (current_output_ == nullptr || current_output_->size != static_cast<int>(len_)) {
            current_output_ = std::make_unique<RmRecord>(static_cast<int>(len_));
        }
        char* out = current_output_->data;
        std::memset(out + data_len_, 0, static_cast<size_t>(len_) - data_len_);
        const auto& prev_cols = prev_->cols();
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            const auto& col = cols_[i];
            const auto& src_col = prev_cols[sel_idxs_[i]];
            std::memcpy(out + col.offset, input.data + src_col.offset, col.len);
            if (is_null(input.data, src_col)) {
                set_null(out, col);
            }
        }
        current_view_ = TupleView{out, static_cast<uint32_t>(current_output_->size)};
        return true;
    }

    bool materialize_current() {
        return materialize_view(prev_->current());
    }

    void append_projection_col(size_t prev_idx, const std::string& output_name = "") {
        auto col = prev_->cols()[prev_idx];
        if (!output_name.empty()) {
            col.name = output_name;
            col.tab_name.clear();
        }
        col.offset = len_;
        len_ += col.len;
        cols_.push_back(col);
        sel_idxs_.push_back(prev_idx);
    }

    // 所有输出列就位后才能确定 bitmap 的起始偏移，因此布局在构造末尾一次算定。
    void finalize_layout() {
        data_len_ = len_;
        bind_null_positions(cols_, static_cast<int>(data_len_));
        len_ = data_len_ + static_cast<size_t>(null_bitmap_bytes(cols_.size()));
    }

    static std::vector<ColMeta>::const_iterator find_col_by_name(const std::vector<ColMeta>& cols,
                                                                 const std::string& name) {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) {
            return col.name == name || (!col.tab_name.empty() && (col.tab_name + "." + col.name) == name);
        });
        if (pos == cols.end()) {
            throw ColumnNotFoundError(name);
        }
        return pos;
    }

    template <typename SelectItemT> void build_from_select_items(const std::vector<SelectItemT>& select_items) {
        const auto& prev_cols = prev_->cols();
        for (const auto& item : select_items) {
            const auto& expr = item.expr;
            std::string output_name = item.display_name;
            size_t prev_idx = prev_cols.size();

            auto try_resolve_by_name = [&](const std::string& name) -> bool {
                if (name.empty()) {
                    return false;
                }
                try {
                    auto pos = find_col_by_name(prev_cols, name);
                    prev_idx = static_cast<size_t>(pos - prev_cols.begin());
                    return true;
                } catch (const ColumnNotFoundError&) {
                    return false;
                }
            };

            if (static_cast<int>(expr.type) == 0) {
                try {
                    auto pos = get_col(prev_cols, expr.col);
                    prev_idx = static_cast<size_t>(pos - prev_cols.begin());
                } catch (const ColumnNotFoundError&) {
                    try_resolve_by_name(expr.col.col_name);
                }
            } else {
                try_resolve_by_name(item.display_name) || try_resolve_by_name(expr.display_name) ||
                    try_resolve_by_name(expr.agg.display_name);
            }

            if (prev_idx == prev_cols.size()) {
                throw ColumnNotFoundError(output_name.empty() ? expr.display_name : output_name);
            }
            if (output_name.empty()) {
                output_name = prev_cols[prev_idx].name;
            }
            append_projection_col(prev_idx, output_name);
        }
    }

public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols) {
        prev_ = std::move(prev);
        len_ = 0;

        auto& prev_cols = prev_->cols();
        for (auto& sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            append_projection_col(static_cast<size_t>(pos - prev_cols.begin()));
        }
        finalize_layout();
    }

    template <typename SelectItemT, typename = std::enable_if_t<!std::is_same_v<SelectItemT, TabCol>>>
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItemT>& select_items) {
        prev_ = std::move(prev);
        len_ = 0;
        build_from_select_items(select_items);
        finalize_layout();
    }

    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<size_t>& projection_ordinals,
                       const std::vector<std::string>& output_names) {
        prev_ = std::move(prev);
        len_ = 0;
        if (projection_ordinals.size() != output_names.size()) {
            throw InternalError("bound projection metadata size mismatch");
        }
        for (size_t i = 0; i < projection_ordinals.size(); ++i) {
            if (projection_ordinals[i] >= prev_->cols().size()) {
                throw InternalError("bound projection ordinal is out of range");
            }
            append_projection_col(projection_ordinals[i], output_names[i]);
        }
        finalize_layout();
    }

    void beginTuple() override {
        prev_->beginTuple();          // 调用儿子节点的beginTuple方法，准备开始遍历记录
        _abstract_rid = prev_->rid(); // 初始化抽象记录号
        current_view_ = {};
        materialize_current();
    }

    void nextTuple() override {
        prev_->nextTuple();           // 调用儿子节点的nextTuple方法，获取下一条记录
        _abstract_rid = prev_->rid(); // 更新抽象记录号
        current_view_ = {};
        materialize_current();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (prev_->is_end()) {
            return nullptr; // 如果儿子节点已经结束，则返回nullptr
        }
        if (!current_view_) {
            fallback_input_ = prev_->Next();
            if (!fallback_input_) {
                return nullptr;
            }
            if (!materialize_view(TupleView{fallback_input_->data, static_cast<uint32_t>(fallback_input_->size)})) {
                return nullptr;
            }
        }
        auto copy = std::make_unique<RmRecord>(static_cast<int>(current_view_.size));
        std::memcpy(copy->data, current_view_.data, current_view_.size);
        return copy;
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return prev_->is_end(); // 判断儿子节点是否结束
    }

    TupleView current() const override {
        if (is_end()) {
            return {};
        }
        return current_view_;
    }
    std::string getType() override {
        return "ProjectionExecutor"; // 返回执行器的名称
    }
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }
    size_t tupleLen() const override {
        return len_;
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
        return prev_->scan_conditions();
    }

    const std::vector<Condition>& scan_conditions_ref() const override {
        return prev_->scan_conditions_ref();
    }
};
