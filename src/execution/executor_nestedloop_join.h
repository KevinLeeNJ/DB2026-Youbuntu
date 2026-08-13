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

#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "parser/ast.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t left_tuple_len_ = 0;
    size_t right_tuple_len_ = 0;
    size_t len_;                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_; // join后获得的记录的字段

    enum class OperandSource { Left, Right, Literal };

    struct CompiledOperand {
        OperandSource source = OperandSource::Left;
        int offset = 0;
        int len = 0;
        ColType type = TYPE_INT;
        Value literal;
    };

    struct CompiledCondition {
        CompOp op = OP_EQ;
        CompiledOperand lhs;
        CompiledOperand rhs;
    };

    std::vector<Condition> fed_conds_; // join条件
    std::vector<CompiledCondition> compiled_conds_;
    bool isend;
    std::unordered_map<std::string, std::vector<ColMeta>::iterator>
        cols_map;                                // 存储链接条件中涉及的列的偏移量和字段长度
    std::unique_ptr<RmRecord> current_left_rec_; // 当前缓存的左表记录
    std::unique_ptr<RmRecord> _buffered_record;  // 复用的输出缓冲
    bool buffered_record_available_ = false;
    JoinType join_type_ = INNER_JOIN;
    bool has_extended_condition_ = false;

    struct OuterTuple {
        RmRecord record;
        std::vector<bool> nulls;
    };
    std::vector<OuterTuple> outer_tuples_;
    size_t outer_cursor_ = 0;
    std::vector<bool> current_left_nulls_;
    std::vector<bool> current_output_nulls_;

    // INLJ support
    bool inlj_mode_ = false;
    TabCol inlj_left_col_;             // left-side column providing lookup key
    TabCol inlj_right_col_;            // right-table indexed column
    int left_key_offset_ = 0;          // pre-compiled offset of left key in left tuple
    int left_key_len_ = 0;             // pre-compiled length of left key
    ColType left_key_type_ = TYPE_INT; // pre-compiled type of left key

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

    static bool compare_string(const int& op, std::string_view lhs, std::string_view rhs) {
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

    static std::string make_col_key(const TabCol& col) {
        return col.tab_name + "." + col.col_name;
    }

    const ColMeta& lookup_col_meta(const TabCol& target) const {
        auto key = make_col_key(target);
        auto col_iter = cols_map.find(key);
        if (col_iter == cols_map.end()) {
            throw ColumnNotFoundError(key);
        }
        return *(col_iter->second);
    }

    CompiledOperand compile_column_operand(const TabCol& target) const {
        const auto& col_meta = lookup_col_meta(target);

        CompiledOperand operand;
        operand.type = col_meta.type;
        operand.len = col_meta.len;
        if (col_meta.offset < static_cast<int>(left_tuple_len_)) {
            operand.source = OperandSource::Left;
            operand.offset = col_meta.offset;
        } else {
            operand.source = OperandSource::Right;
            operand.offset = col_meta.offset - static_cast<int>(left_tuple_len_);
        }
        return operand;
    }

    void compile_conditions() {
        compiled_conds_.clear();
        compiled_conds_.reserve(fed_conds_.size());
        for (const auto& cond : fed_conds_) {
            has_extended_condition_ = has_extended_condition_ || cond.op == OP_LIKE || cond.op == OP_IN ||
                                     cond.op == OP_BETWEEN;
            CompiledCondition compiled;
            compiled.op = cond.op;
            compiled.lhs = compile_column_operand(cond.lhs_col);
            if (cond.is_rhs_val) {
                compiled.rhs.source = OperandSource::Literal;
                compiled.rhs.type = cond.rhs_val.type;
                compiled.rhs.literal = cond.rhs_val;
            } else {
                compiled.rhs = compile_column_operand(cond.rhs_col);
            }
            compiled_conds_.push_back(std::move(compiled));
        }
    }

    static const char* get_operand_data(const CompiledOperand& operand, const RmRecord& left_rec,
                                        const RmRecord& right_rec) {
        switch (operand.source) {
        case OperandSource::Left:
            return left_rec.data + operand.offset;
        case OperandSource::Right:
            return right_rec.data + operand.offset;
        case OperandSource::Literal:
            return nullptr;
        }
        return nullptr;
    }

    static double read_numeric_operand(const CompiledOperand& operand, const RmRecord& left_rec,
                                       const RmRecord& right_rec) {
        if (operand.source == OperandSource::Literal) {
            return operand.type == TYPE_INT ? static_cast<double>(operand.literal.int_val)
                                            : static_cast<double>(operand.literal.float_val);
        }

        const char* data = get_operand_data(operand, left_rec, right_rec);
        return operand.type == TYPE_INT ? static_cast<double>(*reinterpret_cast<const int*>(data))
                                        : *reinterpret_cast<const double*>(data);
    }

    static std::string_view read_string_operand(const CompiledOperand& operand, const RmRecord& left_rec,
                                                const RmRecord& right_rec) {
        if (operand.source == OperandSource::Literal) {
            return operand.literal.str_val;
        }

        const char* data = get_operand_data(operand, left_rec, right_rec);
        return std::string_view(data, operand.len);
    }

    bool is_condition(const RmRecord& left_rec, const RmRecord& right_rec,
                      const std::vector<bool>& left_nulls = {}, const std::vector<bool>& right_nulls = {}) {
        if (has_extended_condition_ || !left_nulls.empty() || !right_nulls.empty()) {
            RmRecord joined(static_cast<int>(len_));
            std::memcpy(joined.data, left_rec.data, left_tuple_len_);
            std::memcpy(joined.data + left_tuple_len_, right_rec.data, right_tuple_len_);
            std::vector<bool> joined_nulls(cols_.size(), false);
            for (size_t i = 0; i < left_nulls.size() && i < left_->cols().size(); ++i) {
                joined_nulls[i] = left_nulls[i];
            }
            for (size_t i = 0; i < right_nulls.size() && i < right_->cols().size(); ++i) {
                joined_nulls[left_->cols().size() + i] = right_nulls[i];
            }
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, joined, joined_nulls)) {
                    return false;
                }
            }
            return true;
        }
        for (const auto& cond : compiled_conds_) {
            const auto lhs_type = cond.lhs.type;
            const auto rhs_type = cond.rhs.type;
            if (!can_cast(lhs_type, rhs_type)) {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }

            bool condition_result = false;
            if ((lhs_type == TYPE_INT || lhs_type == TYPE_FLOAT) && (rhs_type == TYPE_INT || rhs_type == TYPE_FLOAT)) {
                condition_result = compare_numeric(cond.op, read_numeric_operand(cond.lhs, left_rec, right_rec),
                                                   read_numeric_operand(cond.rhs, left_rec, right_rec));
            } else if ((lhs_type == TYPE_STRING || lhs_type == TYPE_DATETIME) &&
                       (rhs_type == TYPE_STRING || rhs_type == TYPE_DATETIME)) {
                condition_result = compare_string(cond.op, read_string_operand(cond.lhs, left_rec, right_rec),
                                                  read_string_operand(cond.rhs, left_rec, right_rec));
            } else {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }

            if (!condition_result) {
                return false;
            }
        }
        return true;
    }

    void reset_buffered_record() {
        _buffered_record = nullptr;
        buffered_record_available_ = false;
    }

    static std::vector<bool> copy_nulls(const AbstractExecutor& executor) {
        return executor.nulls();
    }

    std::vector<std::pair<RmRecord, std::vector<bool>>> materialize_child(AbstractExecutor& child) {
        std::vector<std::pair<RmRecord, std::vector<bool>>> rows;
        for (child.beginTuple(); !child.is_end(); child.nextTuple()) {
            auto record = child.Next();
            if (record != nullptr) {
                rows.emplace_back(*record, copy_nulls(child));
            }
        }
        return rows;
    }

    bool is_outer_join() const {
        return join_type_ == LEFT_JOIN || join_type_ == RIGHT_JOIN || join_type_ == FULL_JOIN;
    }

    void append_outer_tuple(const std::pair<RmRecord, std::vector<bool>>* left,
                            const std::pair<RmRecord, std::vector<bool>>* right, bool left_null, bool right_null) {
        RmRecord output(static_cast<int>(len_));
        std::memset(output.data, 0, len_);
        std::vector<bool> nulls(cols_.size(), false);
        if (left != nullptr && !left_null) {
            std::memcpy(output.data, left->first.data, left_tuple_len_);
            for (size_t i = 0; i < left->second.size() && i < left_->cols().size(); ++i) {
                nulls[i] = left->second[i];
            }
        } else {
            for (size_t i = 0; i < left_->cols().size(); ++i) {
                nulls[i] = true;
            }
        }
        if (right != nullptr && !right_null) {
            std::memcpy(output.data + left_tuple_len_, right->first.data, right_tuple_len_);
            for (size_t i = 0; i < right->second.size() && i < right_->cols().size(); ++i) {
                nulls[left_->cols().size() + i] = right->second[i];
            }
        } else {
            for (size_t i = 0; i < right_->cols().size(); ++i) {
                nulls[left_->cols().size() + i] = true;
            }
        }
        outer_tuples_.push_back(OuterTuple{std::move(output), std::move(nulls)});
    }

    void materialize_outer_join() {
        auto left_rows = materialize_child(*left_);
        auto right_rows = materialize_child(*right_);
        std::vector<bool> matched_left(left_rows.size(), false);
        std::vector<bool> matched_right(right_rows.size(), false);
        outer_tuples_.clear();

        auto append_left_or_right_matches = [&](size_t left_index, size_t right_index) {
            if (is_condition(left_rows[left_index].first, right_rows[right_index].first,
                             left_rows[left_index].second, right_rows[right_index].second)) {
                matched_left[left_index] = true;
                matched_right[right_index] = true;
                append_outer_tuple(&left_rows[left_index], &right_rows[right_index], false, false);
                return true;
            }
            return false;
        };

        if (join_type_ == RIGHT_JOIN) {
            for (size_t right_index = 0; right_index < right_rows.size(); ++right_index) {
                bool matched = false;
                for (size_t left_index = 0; left_index < left_rows.size(); ++left_index) {
                    bool current_match = append_left_or_right_matches(left_index, right_index);
                    matched = current_match || matched;
                }
                if (!matched) {
                    append_outer_tuple(nullptr, &right_rows[right_index], true, false);
                }
            }
            return;
        }

        for (size_t left_index = 0; left_index < left_rows.size(); ++left_index) {
            bool matched = false;
            for (size_t right_index = 0; right_index < right_rows.size(); ++right_index) {
                bool current_match = append_left_or_right_matches(left_index, right_index);
                matched = current_match || matched;
            }
            if (!matched && (join_type_ == LEFT_JOIN || join_type_ == FULL_JOIN)) {
                append_outer_tuple(&left_rows[left_index], nullptr, false, true);
            }
        }
        if (join_type_ == FULL_JOIN) {
            for (size_t right_index = 0; right_index < right_rows.size(); ++right_index) {
                if (!matched_right[right_index]) {
                    append_outer_tuple(nullptr, &right_rows[right_index], true, false);
                }
            }
        }
    }

    void ensure_output_buffer() {
        if (_buffered_record == nullptr || _buffered_record->size != static_cast<int>(len_)) {
            _buffered_record = std::make_unique<RmRecord>(len_);
        }
    }

    Value extract_key_from_left(const RmRecord& left_rec) const {
        Value val;
        const char* data = left_rec.data + left_key_offset_;
        switch (left_key_type_) {
        case TYPE_INT:
            val.set_int(*reinterpret_cast<const int*>(data));
            break;
        case TYPE_FLOAT:
            val.set_float(*reinterpret_cast<const double*>(data));
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            val.set_str(std::string(data, strnlen(data, left_key_len_)));
            val.type = left_key_type_;
            break;
        }
        return val;
    }

    std::vector<Condition> build_key_conditions(const RmRecord& left_rec) const {
        Condition key_cond;
        key_cond.lhs_col = inlj_right_col_;
        key_cond.op = OP_EQ;
        key_cond.is_rhs_val = true;
        key_cond.rhs_val = extract_key_from_left(left_rec);
        key_cond.rhs_val.init_raw(left_key_len_);
        return {key_cond};
    }

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds, TabCol inlj_left_col = {}, TabCol inlj_right_col = {},
                           const std::string& inlj_index_col_name = "", JoinType join_type = INNER_JOIN) {
        (void)inlj_index_col_name;
        left_ = std::move(left);
        right_ = std::move(right);
        join_type_ = join_type;
        left_tuple_len_ = left_->tupleLen();
        right_tuple_len_ = right_->tupleLen();
        len_ = left_tuple_len_ + right_tuple_len_;
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto col = right_cols.begin(); col != right_cols.end(); ++col) {
            col->offset += left_tuple_len_; // 更新右儿子节点的列偏移量
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        cols_map.reserve(cols_.size());
        for (auto it = cols_.begin(); it != cols_.end(); ++it) {
            cols_map[it->tab_name + "." + it->name] = it;
        }

        isend = false;
        fed_conds_ = std::move(conds);
        compile_conditions();

        // INLJ initialization
        if (!inlj_right_col.tab_name.empty()) {
            inlj_mode_ = true;
            inlj_left_col_ = inlj_left_col;
            inlj_right_col_ = inlj_right_col;
            // Pre-compile left key offset/type/len from cols_map
            auto key = make_col_key(inlj_left_col_);
            auto col_iter = cols_map.find(key);
            if (col_iter != cols_map.end()) {
                const auto& meta = *(col_iter->second);
                left_key_offset_ = meta.offset;
                left_key_len_ = meta.len;
                left_key_type_ = meta.type;
            }
        }

        current_left_rec_ = nullptr; // 初始化 current_left_rec_
        reset_buffered_record();
    }

    void beginTuple() override {
        if (is_outer_join()) {
            materialize_outer_join();
            outer_cursor_ = 0;
            isend = outer_tuples_.empty();
            return;
        }
        left_->beginTuple();
        right_->set_counting_enabled(false);
        right_->beginTuple();
        right_->set_counting_enabled(true);
        isend = false;
        current_left_rec_ = nullptr; // 获取左表的第一条记录
        current_left_nulls_.clear();
        current_output_nulls_.clear();
        reset_buffered_record();
        nextTuple();
    }

    /**
     * @brief 寻找下一个匹配的记录对，并将结果存储在 _buffered_record 中。
     */
    void nextTuple() override {
        if (is_outer_join()) {
            if (!isend) {
                ++outer_cursor_;
                isend = outer_cursor_ >= outer_tuples_.size();
            }
            return;
        }
        if (isend) {
            reset_buffered_record();
            return;
        }
        buffered_record_available_ = false;

        while (true) {
            // 如果没有缓存的左表记录，或者右表已经为当前的左表记录扫描完毕
            if (current_left_rec_ == nullptr) {
                // 从左表获取下一条记录
                // left_->nextTuple();
                current_left_rec_ = left_->Next();
                if (left_->is_end() || current_left_rec_ == nullptr) {
                    isend = true;
                    current_output_nulls_.clear();
                    reset_buffered_record();
                    return;
                }
                current_left_nulls_ = left_->nulls();
                left_->nextTuple();
                // INLJ: inject join key before resetting inner scan
                if (inlj_mode_) {
                    right_->set_key_conditions(build_key_conditions(*current_left_rec_));
                }
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

                auto right_nulls = right_->nulls();
                if (is_condition(*current_left_rec_, *right_rec, current_left_nulls_, right_nulls)) {
                    // 找到一个匹配项
                    ensure_output_buffer();
                    memcpy(_buffered_record->data, current_left_rec_->data, left_tuple_len_);
                    memcpy(_buffered_record->data + left_tuple_len_, right_rec->data, right_tuple_len_);
                    current_output_nulls_.assign(cols_.size(), false);
                    for (size_t i = 0; i < current_left_nulls_.size() && i < left_->cols().size(); ++i) {
                        current_output_nulls_[i] = current_left_nulls_[i];
                    }
                    for (size_t i = 0; i < right_nulls.size() && i < right_->cols().size(); ++i) {
                        current_output_nulls_[left_->cols().size() + i] = right_nulls[i];
                    }
                    buffered_record_available_ = true;
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
        if (is_outer_join()) {
            if (isend) {
                return nullptr;
            }
            return std::make_unique<RmRecord>(outer_tuples_[outer_cursor_].record);
        }
        if (is_end() || !buffered_record_available_ || _buffered_record == nullptr) {
            return nullptr;
        }
        buffered_record_available_ = false;
        return std::make_unique<RmRecord>(*_buffered_record);
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

    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
            return (target.tab_name.empty() || col.tab_name == target.tab_name) && col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *pos;
    }

    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        if (is_outer_join()) {
            return !isend ? outer_tuples_[outer_cursor_].nulls : no_nulls;
        }
        return current_output_nulls_;
    }
};
