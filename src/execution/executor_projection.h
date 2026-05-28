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

public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto& prev_cols = prev_->cols();
        for (auto& sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
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
};