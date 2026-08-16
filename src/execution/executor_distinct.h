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

/**
 * @brief 对子执行器结果执行 DISTINCT 去重。
 *
 * 去重键由记录字节和每列 NULL 标记共同组成，因此物理值相同但 NULL 状态
 * 不同的记录不会被错误合并。
 */
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

    /**
     * @brief 构造包含记录字节和 NULL 状态的去重键。
     * @param record 输入记录。
     * @param nulls 输入记录各列的 NULL 标记。
     * @return 可用于哈希集合的二进制键。
     */
    std::string make_key(const RmRecord& record, const std::vector<bool>& nulls) const {
        std::string key(record.data, static_cast<size_t>(record.size));
        for (bool is_null : nulls) {
            key.push_back(is_null ? '\1' : '\0');
        }
        return key;
    }

    /**
     * @brief 消费子执行器并保留每个不同键的第一条记录。
     */
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
    /**
     * @brief 创建 DISTINCT 执行器。
     * @param prev 子执行器。
     */
    explicit DistinctExecutor(std::unique_ptr<AbstractExecutor> prev) : prev_(std::move(prev)) {
        cols_ = prev_->cols();
        len_ = prev_->tupleLen();
        context_ = prev_->context_;
    }

    /**
     * @brief 首次调用时完成去重物化并重置输出游标。
     */
    void beginTuple() override {
        if (!materialized_) {
            materialize();
        }
        cursor_ = 0;
    }

    /**
     * @brief 推进去重结果游标。
     */
    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    /**
     * @brief 返回当前不同记录的副本。
     * @return 当前记录副本；执行结束时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(tuples_[cursor_]);
    }

    /**
     * @brief 返回 DISTINCT 节点的抽象 RID。
     * @return 抽象记录号引用。
     */
    Rid& rid() override {
        return _abstract_rid;
    }

    /**
     * @brief 判断去重结果是否耗尽。
     * @return 游标超出物化结果时返回 true。
     */
    bool is_end() const override {
        return cursor_ >= tuples_.size();
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "DistinctExecutor"。
     */
    std::string getType() override {
        return "DistinctExecutor";
    }

    /**
     * @brief 返回去重输出列元数据。
     * @return 子执行器列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    /**
     * @brief 返回当前去重记录的 NULL 标记。
     * @return 当前记录标记；执行结束时返回空数组引用。
     */
    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : tuple_nulls_[cursor_];
    }

    /**
     * @brief 返回去重输出元组长度。
     * @return 子执行器元组长度。
     */
    size_t tupleLen() const override {
        return len_;
    }

    /**
     * @brief 查找去重输出列的元数据及偏移。
     * @param target 目标列。
     * @return 匹配列元数据。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = get_col(cols_, target);
        return *pos;
    }
};
