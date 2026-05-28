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

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t len_;                              // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;               // join后获得的记录的字段

    std::vector<Condition> fed_conds_; // join条件
    bool isend;
    std::unordered_map<std::string, std::vector<ColMeta>::iterator>
        cols_map;                                // 存储链接条件中涉及的列的偏移量和字段长度
    std::unique_ptr<RmRecord> current_left_rec_; // 当前缓存的左表记录
    std::unique_ptr<RmRecord> _buffered_record;

    static bool compare_numeric(const int& op, const double& lhs, const double& rhs) {
        switch (op) {
        case OP_EQ:
            return lhs == rhs;
        case OP_NE:
            return lhs != rhs;
        case OP_LT:
            return lhs < rhs;
        case OP_GT:
            return lhs > rhs;
        case OP_LE:
            return lhs <= rhs;
        case OP_GE:
            return lhs >= rhs;
        default:
            throw InternalError("Unsupported comparison operator for numeric types");
        }
    }

    static bool compare_string(const int& op, const std::string& lhs, const std::string& rhs) {
        switch (op) {
        case OP_EQ:
            return lhs == rhs;
        case OP_NE:
            return lhs != rhs;
        case OP_LT:
            return lhs < rhs;
        case OP_GT:
            return lhs > rhs;
        case OP_LE:
            return lhs <= rhs;
        case OP_GE:
            return lhs >= rhs;
        default:
            throw InternalError("Unsupported comparison operator for string types");
        }
    }

    bool is_condition(const std::unique_ptr<RmRecord>& left_rec, const std::unique_ptr<RmRecord>& right_rec) const {
        for (const auto& cond : fed_conds_) {
            auto lhs_col_iter = cols_map.find(cond.lhs_col.tab_name + '.' + cond.lhs_col.col_name);
            if (lhs_col_iter == cols_map.end())
                throw ColumnNotFoundError(cond.lhs_col.tab_name + '.' + cond.lhs_col.col_name);

            const ColMeta& lhs_col_meta = *(lhs_col_iter->second);
            char* lhs_data = left_rec->data + lhs_col_meta.offset;

            bool condition_result = false;

            if (cond.is_rhs_val) {
                // INT/FLOAT类型转换比较
                if ((lhs_col_meta.type == TYPE_INT || lhs_col_meta.type == TYPE_FLOAT) &&
                    (cond.rhs_val.type == TYPE_INT || cond.rhs_val.type == TYPE_FLOAT)) {
                    double lhs_val = (lhs_col_meta.type == TYPE_INT) ? static_cast<double>(*(int*)lhs_data)
                                                                     : static_cast<double>(*(float*)lhs_data);
                    double rhs_val = (cond.rhs_val.type == TYPE_INT) ? static_cast<double>(cond.rhs_val.int_val)
                                                                     : static_cast<double>(cond.rhs_val.float_val);
                    condition_result = compare_numeric(cond.op, lhs_val, rhs_val);
                } else if (lhs_col_meta.type == TYPE_STRING && cond.rhs_val.type == TYPE_STRING) {
                    std::string lhs_str(lhs_data, lhs_col_meta.len);
                    condition_result = compare_string(cond.op, lhs_str, cond.rhs_val.str_val);
                } else {
                    std::string lhs_type_str = coltype2str(lhs_col_meta.type);
                    std::string rhs_type_str = coltype2str(cond.rhs_val.type);
                    throw IncompatibleTypeError(lhs_type_str, rhs_type_str);
                }
            } else {
                auto rhs_col_iter = cols_map.find(cond.rhs_col.tab_name + '.' + cond.rhs_col.col_name);
                if (rhs_col_iter == cols_map.end())
                    throw ColumnNotFoundError(cond.rhs_col.tab_name + '.' + cond.rhs_col.col_name);

                const ColMeta& rhs_col_meta = *(rhs_col_iter->second);
                char* rhs_data = right_rec->data + (rhs_col_meta.offset - left_->tupleLen());

                // INT/FLOAT类型转换比较
                if ((lhs_col_meta.type == TYPE_INT || lhs_col_meta.type == TYPE_FLOAT) &&
                    (rhs_col_meta.type == TYPE_INT || rhs_col_meta.type == TYPE_FLOAT)) {
                    // 转成double 防止转换溢出
                    double lhs_val = (lhs_col_meta.type == TYPE_INT) ? static_cast<double>(*(int*)lhs_data)
                                                                     : static_cast<double>(*(float*)lhs_data);
                    double rhs_val = (rhs_col_meta.type == TYPE_INT) ? static_cast<double>(*(int*)rhs_data)
                                                                     : static_cast<double>(*(float*)rhs_data);
                    condition_result = compare_numeric(cond.op, lhs_val, rhs_val);
                } else if (lhs_col_meta.type == TYPE_STRING && rhs_col_meta.type == TYPE_STRING) {
                    std::string lhs_str(lhs_data, lhs_col_meta.len);
                    std::string rhs_str(rhs_data, rhs_col_meta.len);
                    condition_result = compare_string(cond.op, lhs_str, rhs_str);
                } else {
                    std::string lhs_type_str = coltype2str(lhs_col_meta.type);
                    std::string rhs_type_str = coltype2str(rhs_col_meta.type);
                    throw IncompatibleTypeError(lhs_type_str, rhs_type_str);
                }
            }

            if (!condition_result)
                return false;
        }
        return true;
    }

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto col = right_cols.begin(); col != right_cols.end(); ++col) {
            col->offset += left_->tupleLen(); // 更新右儿子节点的列偏移量
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        for (auto it = cols_.begin(); it != cols_.end(); ++it) {
            cols_map[it->tab_name + "." + it->name] = it;
        }

        isend = false;
        fed_conds_ = std::move(conds);
        current_left_rec_ = nullptr; // 初始化 current_left_rec_
        _buffered_record = nullptr;  // 初始化 _buffered_record
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        isend = false;
        current_left_rec_ = nullptr; // 获取左表的第一条记录
        _buffered_record = nullptr;
        nextTuple();
    }

    /**
     * @brief 寻找下一个匹配的记录对，并将结果存储在 _buffered_record 中。
     */
    void nextTuple() override {
        if (isend) {
            _buffered_record = nullptr;
            return;
        }

        while (true) {
            // 如果没有缓存的左表记录，或者右表已经为当前的左表记录扫描完毕
            if (current_left_rec_ == nullptr) {
                // 从左表获取下一条记录
                // left_->nextTuple();
                current_left_rec_ = left_->Next();
                if (left_->is_end() || current_left_rec_ == nullptr) {
                    isend = true;
                    _buffered_record = nullptr;
                    return;
                }
                left_->nextTuple();
                right_->beginTuple();
            }

            // 用当前的左表记录，继续扫描右表
            while (true) {
                auto right_rec = right_->Next();
                if (right_->is_end() || right_rec == nullptr) {
                    // 右表已为当前左表记录扫描完毕
                    current_left_rec_ = nullptr; // 清空左表记录，以便外层循环获取下一条
                    break;                       // 退出内层循环，回到外层循环
                }

                if (is_condition(current_left_rec_, right_rec)) {
                    // 找到一个匹配项
                    _buffered_record = std::make_unique<RmRecord>(len_);
                    memcpy(_buffered_record->data, current_left_rec_->data, left_->tupleLen());
                    memcpy(_buffered_record->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
                    right_->nextTuple(); // 继续扫描右表的下一条记录

                    return;
                }
                right_->nextTuple(); // 继续扫描右表的下一条记录
            }
        }
    }

    /**
     * @brief 返回由 nextTuple() 准备好的记录，并触发下一次寻找。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        // 取出当前缓存的记录
        auto result = std::move(_buffered_record);

        return result;
    }
    Rid& rid() override {
        if (left_ && !left_->is_end()) {
            return left_->rid();
        }
        return _abstract_rid;
    }
    bool is_end() const override {
        return isend;
    }
    std::string getType() override {
        return "NestedLoopJoinExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }
    size_t tupleLen() const override {
        return len_;
    }
};