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
#ifdef RMDB_ENABLE_JIT
#include "jit/jit_tuple_kernels.h"
#endif
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
    std::vector<ColMeta> cols_;              // 需要投影的字段
    size_t len_;                             // 字段总长度
    std::vector<size_t> sel_idxs_;
    std::unique_ptr<RmRecord> current_output_;
    std::unique_ptr<RmRecord> fallback_input_;
    TupleView current_view_;
#ifdef RMDB_ENABLE_JIT
    std::unique_ptr<jit::ProjectionKernel> jit_projection_;
#endif

    bool materialize_view(TupleView input) {
        phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::PROJECTION_COPY,
                                                   phase_metrics::sample_rate(phase_metrics::Phase::PROJECTION_COPY));
        if (!input) {
            return false;
        }
        if (current_output_ == nullptr || current_output_->size != static_cast<int>(len_)) {
            current_output_ = std::make_unique<RmRecord>(static_cast<int>(len_));
        }
#ifdef RMDB_ENABLE_JIT
        if (jit_projection_ != nullptr && jit_projection_->valid()) {
            jit_projection_->project(input.data, current_output_->data);
        } else {
#endif
            for (size_t i = 0; i < sel_idxs_.size(); ++i) {
                const auto& col = cols_[i];
                const auto& src_col = prev_->cols()[sel_idxs_[i]];
                std::memcpy(current_output_->data + col.offset, input.data + src_col.offset, col.len);
            }
#ifdef RMDB_ENABLE_JIT
        }
#endif
        current_view_ = TupleView{current_output_->data, static_cast<uint32_t>(current_output_->size)};
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
#ifdef RMDB_ENABLE_JIT
        if (jit::tuple_jit_enabled()) {
            jit_projection_ = std::make_unique<jit::ProjectionKernel>(cols_, sel_idxs_, prev_cols);
        }
#endif
    }

    template <typename SelectItemT, typename = std::enable_if_t<!std::is_same_v<SelectItemT, TabCol>>>
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItemT>& select_items) {
        prev_ = std::move(prev);
        len_ = 0;
        build_from_select_items(select_items);
#ifdef RMDB_ENABLE_JIT
        if (jit::tuple_jit_enabled()) {
            jit_projection_ = std::make_unique<jit::ProjectionKernel>(cols_, sel_idxs_, prev_->cols());
        }
#endif
    }

    void beginTuple() override {
        prev_->beginTuple();          // 调用儿子节点的beginTuple方法，准备开始遍历记录
        _abstract_rid = prev_->rid(); // 初始化抽象记录号
        current_view_ = {};
        materialize_current();
    }

    TupleView ProjectForPipeline(TupleView input) {
        current_view_ = {};
        return materialize_view(input) ? current_view_ : TupleView{};
    }

    void ResetPreparedRequest(Context* context) {
        context_ = context;
        current_view_ = {};
        fallback_input_.reset();
    }

    void ResetForPreparedPool() noexcept {
        context_ = nullptr;
        current_view_ = {};
        fallback_input_.reset();
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

    void bind_lookup_key(const TabCol& target, LookupKeyView key) override {
        prev_->bind_lookup_key(target, key);
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
