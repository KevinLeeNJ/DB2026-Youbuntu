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
#include "executor_expr.h"
#include "index/ix.h"
#include "parser/ast.h"
#include "system/sm.h"

/**
 * @brief 执行嵌套循环连接，并支持普通、索引嵌套循环及外连接。
 *
 * 内连接逐条缓存左记录并扫描右子执行器；INLJ 会在每次打开右子执行器前
 * 注入当前左键条件。LEFT/RIGHT/FULL JOIN 则先物化两侧并补出未匹配行，
 * 补出的字段通过 NULL 标记而不是物理字节表示空值。
 */
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
    std::vector<std::shared_ptr<QueryExpr>> exprs_;
    QueryExprEvaluator::SubqueryRunner subquery_runner_;
    const QueryExprOuterContext* outer_context_ = nullptr;
    std::vector<CompiledCondition> compiled_conds_;
    bool isend;
    std::unordered_map<std::string, std::vector<ColMeta>::iterator>
        cols_map;                                // 存储链接条件中涉及的列的偏移量和字段长度
    std::unique_ptr<RmRecord> current_left_rec_; // 当前缓存的左表记录
    std::unique_ptr<RmRecord> _buffered_record;  // 复用的输出缓冲
    bool buffered_record_available_ = false;
    JoinType join_type_ = INNER_JOIN;
    bool has_extended_condition_ = false;

    /**
     * @brief 保存外连接输出记录及其 NULL 标记。
     */
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

    /**
     * @brief 比较两个数值连接操作数。
     * @param op 比较操作符编号。
     * @param lhs 左操作数。
     * @param rhs 右操作数。
     * @return 比较成立时返回 true。
     * @throws InternalError 操作符不受支持时抛出。
     */
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

    /**
     * @brief 比较两个字符串或日期时间连接操作数。
     * @param op 比较操作符编号。
     * @param lhs 左操作数。
     * @param rhs 右操作数。
     * @return 比较成立时返回 true。
     * @throws InternalError 操作符不受支持时抛出。
     */
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

    /**
     * @brief 将表列转换为列元数据映射使用的限定名。
     * @param col 表列。
     * @return "table.column" 形式的键。
     */
    static std::string make_col_key(const TabCol& col) {
        return col.tab_name + "." + col.col_name;
    }

    /**
     * @brief 从连接输出列映射中查找列元数据。
     * @param target 目标列。
     * @return 匹配列元数据引用。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    const ColMeta& lookup_col_meta(const TabCol& target) const {
        auto key = make_col_key(target);
        auto col_iter = cols_map.find(key);
        if (col_iter == cols_map.end()) {
            throw ColumnNotFoundError(key);
        }
        return *(col_iter->second);
    }

    /**
     * @brief 将列引用编译为左右记录中的物理偏移和类型。
     * @param target 待编译的列引用。
     * @return 编译后的列操作数。
     * @throws ColumnNotFoundError 找不到列元数据时抛出。
     */
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

    /**
     * @brief 预编译简单连接条件，并识别需要通用表达式求值的条件。
     *
     * 纯列比较可以直接从左右记录按偏移读取；LIKE、IN、BETWEEN 或带表达式的
     * 条件会设置 has_extended_condition_，在运行时统一构造连接记录求值。
     */
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

    /**
     * @brief 获取编译操作数在左右记录中的数据地址。
     * @param operand 编译后的操作数。
     * @param left_rec 左记录。
     * @param right_rec 右记录。
     * @return 物理字段地址；字面量操作数返回 nullptr。
     */
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

    /**
     * @brief 读取并提升数值操作数为 double。
     * @param operand 编译后的操作数。
     * @param left_rec 左记录。
     * @param right_rec 右记录。
     * @return 数值操作数。
     */
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

    /**
     * @brief 读取字符串或日期时间操作数。
     * @param operand 编译后的操作数。
     * @param left_rec 左记录。
     * @param right_rec 右记录。
     * @return 定长字段视图或字面量字符串视图。
     */
    static std::string_view read_string_operand(const CompiledOperand& operand, const RmRecord& left_rec,
                                                const RmRecord& right_rec) {
        if (operand.source == OperandSource::Literal) {
            return operand.literal.str_val;
        }

        const char* data = get_operand_data(operand, left_rec, right_rec);
        return std::string_view(data, operand.len);
    }

    /**
     * @brief 判断左右记录是否满足全部连接条件。
     * @param left_rec 左记录。
     * @param right_rec 右记录。
     * @param left_nulls 左记录 NULL 标记。
     * @param right_nulls 右记录 NULL 标记。
     * @return 连接条件成立时返回 true。
     *
     * 若存在扩展条件、表达式或 NULL 标记，函数先拼接一条连接记录并使用
     * AbstractExecutor::compare/QueryExprEvaluator；纯数值/字符串条件则走预编译
     * 的直接读取路径，避免每次比较都构造临时记录。
     */
    bool is_condition(const RmRecord& left_rec, const RmRecord& right_rec,
                      const std::vector<bool>& left_nulls = {}, const std::vector<bool>& right_nulls = {}) {
        if (has_extended_condition_ || !exprs_.empty() || !left_nulls.empty() || !right_nulls.empty()) {
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
            if (!exprs_.empty()) {
                QueryExprEvaluator evaluator(cols_, joined_nulls, &subquery_runner_, outer_context_);
                for (const auto& expr : exprs_) {
                    if (expr != nullptr && !evaluator.matches(*expr, joined)) {
                        return false;
                    }
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

    /**
     * @brief 清空当前内连接输出缓存。
     */
    void reset_buffered_record() {
        _buffered_record = nullptr;
        buffered_record_available_ = false;
    }

    /**
     * @brief 复制子执行器当前记录的 NULL 标记。
     * @param executor 子执行器。
     * @return NULL 标记副本。
     */
    static std::vector<bool> copy_nulls(const AbstractExecutor& executor) {
        return executor.nulls();
    }

    /**
     * @brief 完整消费一个子执行器，保存其记录和 NULL 标记。
     * @param child 待物化的子执行器。
     * @return 记录与 NULL 标记组成的行数组。
     */
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

    /**
     * @brief 判断当前是否为某种外连接。
     * @return 连接类型为 LEFT/RIGHT/FULL JOIN 时返回 true。
     */
    bool is_outer_join() const {
        return join_type_ == LEFT_JOIN || join_type_ == RIGHT_JOIN || join_type_ == FULL_JOIN;
    }

    /**
     * @brief 构造并保存一条外连接输出行。
     * @param left 左侧行；补左侧 NULL 行时可为 nullptr。
     * @param right 右侧行；补右侧 NULL 行时可为 nullptr。
     * @param left_null 是否将左侧整行视为 NULL。
     * @param right_null 是否将右侧整行视为 NULL。
     */
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

    /**
     * @brief 物化 LEFT/RIGHT/FULL JOIN 的全部结果。
     *
     * 先分别读取左右输入，再通过匹配标记判断未匹配行。RIGHT JOIN 以右侧为
     * 外表，LEFT/FULL JOIN 以左侧为外表；FULL JOIN 最后补出未匹配的右行。
     */
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

    /**
     * @brief 确保内连接输出缓存具有正确的记录长度。
     */
    void ensure_output_buffer() {
        if (_buffered_record == nullptr || _buffered_record->size != static_cast<int>(len_)) {
            _buffered_record = std::make_unique<RmRecord>(len_);
        }
    }

    /**
     * @brief 从当前左记录提取 INLJ 右表查找键。
     * @param left_rec 左侧记录。
     * @return 转换为 Value 的查找键。
     */
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

    /**
     * @brief 构造注入右侧索引扫描器的等值键条件。
     * @param left_rec 当前左侧记录。
     * @return 右索引列等于左键值的单条件列表。
     */
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
    /**
     * @brief 创建嵌套循环连接执行器。
     * @param left 左子执行器。
     * @param right 右子执行器。
     * @param conds 传统连接条件。
     * @param inlj_left_col INLJ 模式下左侧查找列。
     * @param inlj_right_col INLJ 模式下右侧索引列；为空表示普通 NLJ。
     * @param inlj_index_col_name 保留的索引名称参数。
     * @param join_type 连接类型。
     * @param exprs 扩展连接表达式。
     * @param subquery_runner 子查询执行回调。
     * @param outer_context 相关子查询使用的外层行上下文。
     */
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds, TabCol inlj_left_col = {}, TabCol inlj_right_col = {},
                           const std::string& inlj_index_col_name = "", JoinType join_type = INNER_JOIN,
                           std::vector<std::shared_ptr<QueryExpr>> exprs = {},
                           QueryExprEvaluator::SubqueryRunner subquery_runner = {},
                           const QueryExprOuterContext* outer_context = nullptr) {
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
        exprs_ = std::move(exprs);
        subquery_runner_ = std::move(subquery_runner);
        outer_context_ = outer_context;
        compile_conditions();
        has_extended_condition_ = has_extended_condition_ || !exprs_.empty();

        // 构造连接输出列布局，并把右列偏移整体平移到左元组之后。
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

    /**
     * @brief 初始化连接扫描并定位到第一条结果。
     *
     * 外连接先一次性物化结果；内连接初始化左右游标，再调用 nextTuple() 进入
     * 首个左/右记录配对。右子节点的计数在初始化期间暂时关闭，避免连接初始化
     * 被统计为一次普通扫描。
     */
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
     * @brief 寻找下一条匹配的连接结果并放入输出缓存。
     *
     * 外连接只推进物化结果游标；内连接采用“固定一条左记录、扫描整个右侧”
     * 的双层循环，并在每次命中后保存连接记录和 NULL 标记。
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
     * @brief 返回由 nextTuple() 准备好的连接记录副本。
     * @return 当前连接结果；没有缓存结果或执行结束时返回 nullptr。
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
    /**
     * @brief 返回当前连接结果关联的抽象 RID。
     * @return 左子执行器 RID；左侧结束时返回本节点 RID。
     */
    Rid& rid() override {
        if (left_ && !left_->is_end()) {
            return left_->rid();
        }
        return _abstract_rid;
    }
    /**
     * @brief 判断连接结果是否耗尽。
     * @return 连接扫描结束时返回 true。
     */
    bool is_end() const override {
        return isend;
    }
    /**
     * @brief 返回执行器类型名称。
     * @return "NestedLoopJoinExecutor"。
     */
    std::string getType() override {
        return "NestedLoopJoinExecutor";
    }

    /**
     * @brief 返回连接输出列元数据。
     * @return 左列后接右列的输出列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }
    /**
     * @brief 返回连接输出元组长度。
     * @return 左右元组长度之和。
     */
    size_t tupleLen() const override {
        return len_;
    }

    /**
     * @brief 查找连接输出列的元数据及偏移。
     * @param target 目标列。
     * @return 匹配列元数据。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
    ColMeta get_col_offset(const TabCol& target) override {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta& col) {
            return (target.tab_name.empty() || col.tab_name == target.tab_name) && col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *pos;
    }

    /**
     * @brief 返回当前连接结果的 NULL 标记。
     * @return 外连接物化行或内连接当前缓存行的 NULL 标记；结束时为空数组。
     */
    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        if (is_outer_join()) {
            return !isend ? outer_tuples_[outer_cursor_].nulls : no_nulls;
        }
        return current_output_nulls_;
    }
};
