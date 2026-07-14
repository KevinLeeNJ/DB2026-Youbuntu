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

#include <string_view>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
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
        return operand.type == TYPE_INT ? static_cast<double>(read_unaligned<int>(data))
                                        : read_unaligned<double>(data);
    }

    static std::string_view read_string_operand(const CompiledOperand& operand, const RmRecord& left_rec,
                                                const RmRecord& right_rec) {
        if (operand.source == OperandSource::Literal) {
            return operand.literal.str_val;
        }

        const char* data = get_operand_data(operand, left_rec, right_rec);
        return std::string_view(data, operand.len);
    }

    bool is_condition(const RmRecord& left_rec, const RmRecord& right_rec) const {
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
            val.set_int(read_unaligned<int>(data));
            break;
        case TYPE_FLOAT:
            val.set_float(read_unaligned<double>(data));
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
                           const std::string& inlj_index_col_name = "") {
        (void)inlj_index_col_name;
        left_ = std::move(left);
        right_ = std::move(right);
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
        left_->beginTuple();
        right_->set_counting_enabled(false);
        right_->beginTuple();
        right_->set_counting_enabled(true);
        isend = false;
        current_left_rec_ = nullptr; // 获取左表的第一条记录
        reset_buffered_record();
        nextTuple();
    }

    /**
     * @brief 寻找下一个匹配的记录对，并将结果存储在 _buffered_record 中。
     */
    void nextTuple() override {
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
                    reset_buffered_record();
                    return;
                }
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

                if (is_condition(*current_left_rec_, *right_rec)) {
                    // 找到一个匹配项
                    ensure_output_buffer();
                    memcpy(_buffered_record->data, current_left_rec_->data, left_tuple_len_);
                    memcpy(_buffered_record->data + left_tuple_len_, right_rec->data, right_tuple_len_);
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
};
