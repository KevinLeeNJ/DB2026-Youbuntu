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

/**
 * @brief 执行 UNION、UNION ALL、INTERSECT 和 EXCEPT 集合操作。
 *
 * 算子先把各分支转换到统一的输出列布局，再按集合操作逐步合并；集合键
 * 同时包含记录字节和 NULL 标记，以保持 SQL 集合语义中的值/NULL 区分。
 */
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

    /**
     * @brief 读取定长字符串字段并去除尾部零填充。
     * @param data 字段首地址。
     * @param len 字段物理长度。
     * @return 字符串内容。
     */
    static std::string read_string_cell(const char* data, int len) {
        return std::string(data, strnlen(data, static_cast<size_t>(len)));
    }

    /**
     * @brief 将分支字段转换并复制到统一输出字段。
     * @param dst 目标字段首地址。
     * @param dst_col 目标列元数据。
     * @param src 源字段首地址。
     * @param src_col 源列元数据。
     *
     * 浮点目标支持 int 到 double 的提升；字符串/日期按目标列宽清零后复制，
     * 其他兼容类型直接复制目标字段宽度。
     */
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

    /**
     * @brief 将一个分支记录转换到 UNION 输出布局。
     * @param src_rec 分支记录。
     * @param src_cols 分支列元数据。
     * @return 按本节点列布局构造的新记录。
     */
    RmRecord convert_record(const RmRecord& src_rec, const std::vector<ColMeta>& src_cols,
                            const std::vector<bool>& nulls) const {
        RmRecord dst_rec(static_cast<int>(len_));
        for (size_t i = 0; i < cols_.size(); ++i) {
            const auto& dst_col = cols_[i];
            const auto& src_col = src_cols[i];
            if (i < nulls.size() && nulls[i]) {
                std::memset(dst_rec.data + dst_col.offset, 0, static_cast<size_t>(dst_col.len));
                continue;
            }
            copy_cell(dst_rec.data + dst_col.offset, dst_col, src_rec.data + src_col.offset, src_col);
        }
        return dst_rec;
    }

    /**
     * @brief 构造集合操作使用的记录键。
     * @param record 已转换到统一布局的记录。
     * @param nulls 记录的 NULL 标记。
     * @return 包含记录字节和 NULL 状态的二进制键。
     */
    static std::string make_key(const RmRecord& record, const std::vector<bool>& nulls) {
        std::string key(record.data, static_cast<size_t>(record.size));
        for (bool is_null : nulls) {
            key.push_back(is_null ? '\1' : '\0');
        }
        return key;
    }

    /**
     * @brief 物化所有分支并执行集合合并。
     *
     * 先分别读取和类型转换各分支；UNION 根据 UNION ALL 决定是否去重，
     * INTERSECT/EXCEPT 则先分别去重，再利用右侧键集合筛选左侧结果。
     */
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
                const auto& nulls = branch->nulls();
                branch_tuples[branch_idx].push_back(convert_record(*rec, branch_cols, nulls));
                branch_nulls[branch_idx].push_back(nulls);
            }
        }
        if (branches_.empty()) {
            materialized_ = true;
            return;
        }

        // 对每一步集合运算使用的输入先去重，保证 INTERSECT/EXCEPT 的集合语义。
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

        using Relation = std::pair<std::vector<RmRecord>, std::vector<std::vector<bool>>>;
        auto apply_operation = [&](Relation left, Relation right, QuerySetOperator op, bool keep_duplicates) {
            if (op == QuerySetOperator::UNION) {
                for (auto& tuple : right.first) {
                    left.first.push_back(std::move(tuple));
                }
                for (auto& nulls : right.second) {
                    left.second.push_back(std::move(nulls));
                }
                return keep_duplicates ? std::move(left) : deduplicate(std::move(left.first), std::move(left.second));
            }

            left = deduplicate(std::move(left.first), std::move(left.second));
            right = deduplicate(std::move(right.first), std::move(right.second));
            std::unordered_set<std::string> right_keys;
            for (size_t i = 0; i < right.first.size(); ++i) {
                right_keys.insert(make_key(right.first[i], right.second[i]));
            }
            Relation result;
            for (size_t i = 0; i < left.first.size(); ++i) {
                bool present = right_keys.find(make_key(left.first[i], left.second[i])) != right_keys.end();
                if ((op == QuerySetOperator::INTERSECT && present) || (op == QuerySetOperator::EXCEPT && !present)) {
                    result.first.push_back(std::move(left.first[i]));
                    result.second.push_back(std::move(left.second[i]));
                }
            }
            return result;
        };

        auto take_branch = [&](size_t index) {
            return Relation{std::move(branch_tuples[index]), std::move(branch_nulls[index])};
        };

        Relation result;
        bool have_result = false;
        size_t branch_idx = 0;
        while (branch_idx < branches_.size()) {
            const size_t term_start = branch_idx;
            Relation term = take_branch(branch_idx);
            while (branch_idx < operators_.size() && operators_[branch_idx] == QuerySetOperator::INTERSECT) {
                term =
                    apply_operation(std::move(term), take_branch(branch_idx + 1), QuerySetOperator::INTERSECT, false);
                ++branch_idx;
            }

            if (!have_result) {
                result = std::move(term);
                have_result = true;
            } else {
                const size_t op_index = term_start - 1;
                const QuerySetOperator op =
                    op_index < operators_.size() ? operators_[op_index] : QuerySetOperator::UNION;
                const bool keep_duplicates =
                    op == QuerySetOperator::UNION && op_index < union_all_.size() && union_all_[op_index];
                result = apply_operation(std::move(result), std::move(term), op, keep_duplicates);
            }
            ++branch_idx;
        }
        tuples_ = std::move(result.first);
        tuple_nulls_ = std::move(result.second);
        materialized_ = true;
    }

public:
    /**
     * @brief 创建集合操作执行器。
     * @param branches 各集合分支执行器，顺序与集合操作对应。
     * @param cols 统一输出列元数据。
     * @param union_all 每个 UNION 是否保留重复项。
     * @param operators 分支之间的集合操作类型。
     */
    UnionExecutor(std::vector<std::unique_ptr<AbstractExecutor>> branches, std::vector<ColMeta> cols,
                  std::vector<bool> union_all = {}, std::vector<QuerySetOperator> operators = {})
        : branches_(std::move(branches)), cols_(std::move(cols)), union_all_(std::move(union_all)),
          operators_(std::move(operators)) {
        if (!cols_.empty()) {
            len_ = static_cast<size_t>(cols_.back().offset + cols_.back().len);
        }
    }

    /**
     * @brief 首次调用时物化集合结果并重置输出游标。
     */
    void beginTuple() override {
        if (!materialized_) {
            materialize();
        }
        cursor_ = 0;
    }

    /**
     * @brief 推进集合结果游标。
     */
    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    /**
     * @brief 判断集合结果是否耗尽。
     * @return 游标超出物化结果时返回 true。
     */
    bool is_end() const override {
        return cursor_ >= tuples_.size();
    }

    /**
     * @brief 返回当前集合结果记录副本。
     * @return 当前记录副本；结果结束时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(tuples_[cursor_]);
    }

    /**
     * @brief 返回当前集合结果的 NULL 标记。
     * @return 当前记录标记；执行结束时返回空数组引用。
     */
    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : tuple_nulls_[cursor_];
    }

    /**
     * @brief 返回集合节点的抽象 RID。
     * @return 抽象记录号引用。
     */
    Rid& rid() override {
        return _abstract_rid;
    }

    /**
     * @brief 返回集合输出列元数据。
     * @return 输出列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    /**
     * @brief 返回集合输出元组长度。
     * @return 输出字段长度总和。
     */
    size_t tupleLen() const override {
        return len_;
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "UnionExecutor"。
     */
    std::string getType() override {
        return "UnionExecutor";
    }

    /**
     * @brief 查找集合输出列的元数据及偏移。
     * @param target 目标列。
     * @return 匹配列元数据。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
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
