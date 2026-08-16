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

/**
 * @brief 对子执行器结果执行 OFFSET/LIMIT 截断。
 *
 * beginTuple() 先跳过 offset 条记录，returned_ 只统计已经向上层暴露并被
 * nextTuple() 跳过的记录，因此 LIMIT 判断不会把 OFFSET 计入返回数量。
 */
class LimitExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    int limit_ = -1;
    size_t offset_ = 0;
    size_t skipped_ = 0;
    size_t returned_ = 0;

public:
    /**
     * @brief 创建 LIMIT 执行器。
     * @param prev 子执行器。
     * @param limit 最多返回的记录数；负数表示不限制。
     * @param offset 开始返回前需要跳过的记录数。
     */
    LimitExecutor(std::unique_ptr<AbstractExecutor> prev, int limit, int offset = 0)
        : prev_(std::move(prev)), limit_(limit), offset_(static_cast<size_t>(offset)) {}

    /**
     * @brief 初始化子执行器并跳过 OFFSET 指定的记录。
     */
    void beginTuple() override {
        skipped_ = 0;
        returned_ = 0;
        prev_->beginTuple();
        while (skipped_ < offset_ && !prev_->is_end()) {
            prev_->nextTuple();
            ++skipped_;
        }
    }

    /**
     * @brief 消费当前记录并推进到下一条记录。
     */
    void nextTuple() override {
        if (is_end()) {
            return;
        }
        ++returned_;
        prev_->nextTuple();
    }

    /**
     * @brief 返回当前未被截断的子记录。
     * @return 子执行器当前记录；达到 LIMIT 或子执行器结束时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return prev_->Next();
    }

    /**
     * @brief 判断 LIMIT 或子执行器是否结束。
     * @return 达到返回上限或子执行器耗尽时返回 true。
     */
    bool is_end() const override {
        return (limit_ >= 0 && returned_ >= static_cast<size_t>(limit_)) || prev_->is_end();
    }

    /**
     * @brief 返回当前子记录的 RID。
     * @return 子执行器 RID 引用；没有子执行器时返回本节点 RID。
     */
    Rid& rid() override {
        if (prev_ != nullptr) {
            return prev_->rid();
        }
        return _abstract_rid;
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "LimitExecutor"。
     */
    std::string getType() override {
        return "LimitExecutor";
    }

    /**
     * @brief 返回 LIMIT 输出列元数据。
     * @return 子执行器列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return prev_->cols();
    }

    /**
     * @brief 返回 LIMIT 输出元组长度。
     * @return 子执行器元组长度。
     */
    size_t tupleLen() const override {
        return prev_->tupleLen();
    }

    /**
     * @brief 返回当前子记录的 NULL 标记。
     * @return 子执行器 NULL 标记引用。
     */
    const std::vector<bool>& nulls() const override {
        return prev_->nulls();
    }

    /**
     * @brief 查找 LIMIT 输出列的元数据及偏移。
     * @param target 目标列。
     * @return 子执行器返回的列元数据。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        return prev_->get_col_offset(target);
    }
};
