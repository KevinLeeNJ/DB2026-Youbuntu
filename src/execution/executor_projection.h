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
    size_t len_;                             // 字段总长度
    std::vector<size_t> sel_idxs_;

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
    }

    template <typename SelectItemT, typename = std::enable_if_t<!std::is_same_v<SelectItemT, TabCol>>>
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItemT>& select_items) {
        prev_ = std::move(prev);
        len_ = 0;
        build_from_select_items(select_items);
    }

    void beginTuple() override {
        prev_->beginTuple();          // 调用儿子节点的beginTuple方法，准备开始遍历记录
        _abstract_rid = prev_->rid(); // 初始化抽象记录号
    }

    void nextTuple() override {
        prev_->nextTuple();           // 调用儿子节点的nextTuple方法，获取下一条记录
        _abstract_rid = prev_->rid(); // 更新抽象记录号
    }

    std::unique_ptr<RmRecord> Next() override {
        if (prev_->is_end()) {
            return nullptr; // 如果儿子节点已经结束，则返回nullptr
        }
        auto rec = prev_->Next();
        if (!rec) {
            return nullptr; // 如果儿子节点没有记录，则返回nullptr
        }

        // 创建一个新的记录，用于存储投影后的结果
        auto new_rec = std::make_unique<RmRecord>(len_);
        // 将投影的字段从儿子节点的记录中复制到新的记录中
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            auto& col = cols_[i];
            auto& src_col = prev_->cols()[sel_idxs_[i]];
            std::memcpy(new_rec->data + col.offset, rec->data + src_col.offset, col.len);
        }
        return new_rec;
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return prev_->is_end(); // 判断儿子节点是否结束
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
};
