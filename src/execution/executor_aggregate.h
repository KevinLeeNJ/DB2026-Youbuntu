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
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "errors.h"
#include "execution_defs.h"
#include "execution_scalar.h"
#include "executor_abstract.h"

/**
 * @brief 执行 GROUP BY、聚合函数以及 HAVING 过滤。
 *
 * 算子会先消费子执行器并物化分组状态，再按游标逐组输出结果。无 GROUP BY
 * 时使用单个全局分组，并针对 COUNT(*) 和有序 MIN 提供可选的快速路径。
 */
class AggregateExecutor : public AbstractExecutor {
private:
    enum class LocalAggType { COUNT = 0, MAX = 1, MIN = 2, SUM = 3, AVG = 4 };
    enum class OperandKind { GROUP_COL, AGG_RESULT, VALUE };

    using CellValue = execution_scalar::CellValue;
    using CellValueHash = execution_scalar::CellValueHash;

    /**
     * @brief 保存分组列的值及其 NULL 状态。
     */
    struct GroupKey {
        std::vector<CellValue> values;
        std::vector<bool> nulls;

        /**
         * @brief 判断两个分组键是否完全相同。
         * @param other 待比较的分组键。
         * @return 值和 NULL 标记都相同时返回 true。
         */
        bool operator==(const GroupKey& other) const {
            return values == other.values && nulls == other.nulls;
        }
    };

    /**
     * @brief 为分组键计算哈希值。
     */
    struct GroupKeyHash {
        /**
         * @brief 组合所有分组列值和 NULL 标记的哈希。
         * @param key 待哈希的分组键。
         * @return 分组键哈希值。
         */
        size_t operator()(const GroupKey& key) const {
            size_t seed = 0;
            CellValueHash hash_cell;
            for (const auto& value : key.values) {
                execution_scalar::hash_combine(seed, hash_cell(value));
            }
            for (bool is_null : key.nulls) {
                execution_scalar::hash_combine(seed, std::hash<bool>()(is_null));
            }
            return seed;
        }
    };

    struct AggregateSpec {
        LocalAggType type = LocalAggType::COUNT;
        bool is_star = false;
        bool is_distinct = false;
        TabCol col;
        std::string output_name;
        ColType input_type = TYPE_INT;
        int input_len = static_cast<int>(sizeof(int));
        ColMeta input_col;
    };

    struct AggregateState {
        int64_t count = 0;
        double sum = 0.0;
        bool has_value = false;
        CellValue value;
        std::unordered_set<CellValue, CellValueHash> distinct_values;
    };

    struct GroupState {
        std::vector<CellValue> group_values;
        std::vector<bool> group_nulls;
        std::vector<AggregateState> aggregate_states;
    };

    struct ResolvedHavingOperand {
        CellValue value;
        bool is_null = false;
    };

    struct HavingOperand {
        OperandKind kind = OperandKind::VALUE;
        size_t index = 0;
        CellValue literal;
    };

    struct HavingSpec {
        HavingOperand lhs;
        CompOp op = OP_EQ;
        HavingOperand rhs;
        HavingOperand rhs_upper;
        std::vector<HavingOperand> rhs_values;
        bool has_rhs_upper = false;
        bool negated = false;
    };

    // 以下 traits 用于兼容不同分析阶段表达式结构的字段命名。
    template <typename T, typename = void> struct has_member_val : std::false_type {};

    template <typename T>
    struct has_member_val<T, std::void_t<decltype(std::declval<const T&>().val)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_value : std::false_type {};

    template <typename T>
    struct has_member_value<T, std::void_t<decltype(std::declval<const T&>().value)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_agg : std::false_type {};

    template <typename T>
    struct has_member_agg<T, std::void_t<decltype(std::declval<const T&>().agg)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_rhs_vals : std::false_type {};

    template <typename T>
    struct has_member_rhs_vals<T, std::void_t<decltype(std::declval<const T&>().rhs_vals)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_rhs_upper : std::false_type {};

    template <typename T>
    struct has_member_rhs_upper<T, std::void_t<decltype(std::declval<const T&>().rhs_upper)>> : std::true_type {};

    template <typename T, typename = void> struct has_member_has_rhs_upper : std::false_type {};

    template <typename T>
    struct has_member_has_rhs_upper<T, std::void_t<decltype(std::declval<const T&>().has_rhs_upper)>>
        : std::true_type {};

    template <typename T, typename = void> struct has_member_negated : std::false_type {};

    template <typename T>
    struct has_member_negated<T, std::void_t<decltype(std::declval<const T&>().negated)>> : std::true_type {};

    std::unique_ptr<AbstractExecutor> prev_;
    Context* context_ = nullptr;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    std::vector<ColMeta> group_cols_;
    std::vector<AggregateSpec> aggregates_;
    std::vector<HavingSpec> having_conds_;
    std::vector<GroupState> groups_;
    std::vector<bool> nulls_;
    size_t cursor_ = 0;
    bool materialized_ = false;
    bool has_group_by_ = false;

    /**
     * @brief 去除定长字符串字段尾部的填充字符。
     * @param data 字符串字段首地址。
     * @param len 字段物理长度。
     * @return 去除尾部填充后的字符串。
     */
    static std::string trim_string(const char* data, int len) {
        return execution_scalar::trim_string(data, len);
    }

    /**
     * @brief 将分析阶段的聚合类型编号转换为执行器内部枚举。
     * @param type_code 分析阶段保存的聚合类型编号。
     * @return 内部聚合类型。
     * @throws InternalError 类型编号未知时抛出。
     */
    static LocalAggType normalize_agg_type(int type_code) {
        switch (type_code) {
        case 0:
            return LocalAggType::COUNT;
        case 1:
            return LocalAggType::MAX;
        case 2:
            return LocalAggType::MIN;
        case 3:
            return LocalAggType::SUM;
        case 4:
            return LocalAggType::AVG;
        default:
            throw InternalError("Unexpected aggregate type");
        }
    }

    /**
     * @brief 按列元数据从记录中读取一个执行器内部值。
     * @param rec 输入记录。
     * @param col 要读取的列元数据。
     * @return 转换后的 CellValue；NULL 状态由调用方的 nulls 数组单独传递。
     */
    CellValue read_cell(const RmRecord& rec, const ColMeta& col) const {
        CellValue value;
        value.type = col.type;
        const char* data = rec.data + col.offset;
        switch (col.type) {
        case TYPE_INT:
            value.int_val = *reinterpret_cast<const int*>(data);
            break;
        case TYPE_FLOAT:
            value.float_val = *reinterpret_cast<const double*>(data);
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            value.str_val = trim_string(data, col.len);
            break;
        }
        return value;
    }

    /**
     * @brief 比较两个内部值。
     * @param lhs 左值。
     * @param rhs 右值。
     * @return lhs 小于、等于或大于 rhs 时分别返回负数、0 或正数。
     */
    static int compare_cells(const CellValue& lhs, const CellValue& rhs) {
        return execution_scalar::compare_cells(lhs, rhs);
    }

    /**
     * @brief 将三路比较结果按比较操作符转换为布尔值。
     * @param op 比较操作符。
     * @param cmp compare_cells 的结果。
     * @return 比较条件成立时返回 true。
     * @throws InternalError 该函数不支持的操作符传入时抛出。
     */
    static bool compare_with_op(CompOp op, int cmp) {
        switch (op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        case OP_LIKE:
        case OP_IN:
        case OP_BETWEEN:
            break;
        }
        throw InternalError("Unexpected comparison operator");
    }

    /**
     * @brief 构造指定类型的零值。
     * @param type 目标列类型。
     * @return 对应类型的零值。
     */
    static CellValue zero_value(ColType type) {
        return execution_scalar::zero_value(type);
    }

    /**
     * @brief 从不同表达式结构中提取字面量字段。
     * @tparam ExprT 表达式类型。
     * @param expr 可能包含 val 或 value 字段的表达式。
     * @return 字面量引用。
     */
    template <typename ExprT> static const Value& get_expr_literal_value(const ExprT& expr) {
        if constexpr (has_member_val<ExprT>::value) {
            return expr.val;
        } else if constexpr (has_member_value<ExprT>::value) {
            return expr.value;
        } else {
            static_assert(has_member_val<ExprT>::value || has_member_value<ExprT>::value,
                          "Expression type must expose val or value");
        }
    }

    /**
     * @brief 将一个内部值写入聚合结果记录的指定列槽位。
     * @param dest 目标记录数据首地址。
     * @param col 目标列元数据。
     * @param value 待写入的内部值。
     *
     * NULL 标记不在这里编码，而是由输出的 nulls_ 数组维护；字符串字段会按
     * 固定列宽清零后复制，避免残留旧数据。
     */
    void write_cell(char* dest, const ColMeta& col, const CellValue& value) const {
        switch (col.type) {
        case TYPE_INT:
            *reinterpret_cast<int*>(dest + col.offset) = value.int_val;
            break;
        case TYPE_FLOAT:
            *reinterpret_cast<double*>(dest + col.offset) = value.float_val;
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            std::memset(dest + col.offset, 0, col.len);
            std::memcpy(dest + col.offset, value.str_val.data(), std::min<int>(col.len, value.str_val.size()));
            break;
        }
    }

    /**
     * @brief 在子执行器输出中解析聚合输入列。
     * @param target 聚合函数引用的表列。
     * @return 输入列元数据。
     * @throws ColumnNotFoundError 找不到输入列时抛出。
     */
    ColMeta find_input_col(const TabCol& target) const {
        return prev_->get_col_offset(target);
    }

    /**
     * @brief 将一列追加到聚合输出布局并计算其物理偏移。
     * @param col 源列或新建列元数据。
     * @param output_name 输出列名，为空时保留原名。
     */
    void append_output_col(ColMeta col, const std::string& output_name) {
        col.offset = static_cast<int>(len_);
        if (!output_name.empty()) {
            col.name = output_name;
            col.tab_name.clear();
        }
        len_ += static_cast<size_t>(col.len);
        cols_.push_back(col);
    }

    /**
     * @brief 从不同 GROUP BY 选择项表示中提取 TabCol。
     * @tparam GroupByT 分组项类型。
     * @param item 分组项。
     * @return 分组列引用。
     */
    template <typename GroupByT> static const TabCol& extract_group_col(const GroupByT& item) {
        if constexpr (std::is_same_v<GroupByT, TabCol>) {
            return item;
        } else {
            return item.col;
        }
    }

    /**
     * @brief 将具体聚合表达式转换为内部 AggregateSpec。
     * @tparam AggExprT 聚合表达式类型。
     * @param agg_expr 聚合表达式。
     * @return 执行器内部聚合描述。
     * @throws ColumnNotFoundError 非 COUNT(*) 聚合的输入列不存在时抛出。
     */
    template <typename AggExprT> AggregateSpec make_aggregate_spec_direct(const AggExprT& agg_expr) const {
        AggregateSpec spec;
        spec.type = normalize_agg_type(static_cast<int>(agg_expr.type));
        spec.is_star = agg_expr.is_star;
        spec.is_distinct = agg_expr.is_distinct;
        spec.col = agg_expr.col;
        spec.output_name = agg_expr.display_name;
        if (spec.type == LocalAggType::COUNT && spec.is_star) {
            spec.input_type = TYPE_INT;
            spec.input_len = static_cast<int>(sizeof(int));
        } else {
            spec.input_col = find_input_col(spec.col);
            spec.input_type = spec.input_col.type;
            spec.input_len = spec.input_col.len;
        }
        return spec;
    }

    /**
     * @brief 兼容直接聚合表达式和带 agg 成员的选择项包装。
     * @tparam AggItemT 聚合项类型。
     * @param agg_item 聚合项。
     * @return 内部聚合描述。
     */
    template <typename AggItemT> AggregateSpec make_aggregate_spec(const AggItemT& agg_item) const {
        if constexpr (has_member_agg<AggItemT>::value) {
            return make_aggregate_spec_direct(agg_item.agg);
        } else {
            return make_aggregate_spec_direct(agg_item);
        }
    }

    /**
     * @brief 将 HAVING 表达式解析为分组列、聚合结果或常量操作数。
     * @tparam ExprT HAVING 表达式类型。
     * @param expr 待解析表达式。
     * @return 内部 HAVING 操作数。
     * @throws ColumnNotFoundError 无法匹配分组列或聚合结果时抛出。
     */
    template <typename ExprT> HavingOperand make_operand_from_expr(const ExprT& expr) const {
        HavingOperand operand;
        const int expr_type = static_cast<int>(expr.type);
        if (expr_type == 2) {
            const Value& literal = get_expr_literal_value(expr);
            operand.kind = OperandKind::VALUE;
            operand.literal = zero_value(literal.type);
            operand.literal.type = literal.type;
            if (literal.type == TYPE_INT) {
                operand.literal.int_val = literal.int_val;
            } else if (literal.type == TYPE_FLOAT) {
                operand.literal.float_val = literal.float_val;
            } else {
                operand.literal.str_val = literal.str_val;
            }
            return operand;
        }

        if (expr_type == 0) {
            for (size_t i = 0; i < group_cols_.size(); ++i) {
                if (group_cols_[i].tab_name == expr.col.tab_name && group_cols_[i].name == expr.col.col_name) {
                    operand.kind = OperandKind::GROUP_COL;
                    operand.index = i;
                    return operand;
                }
                if (group_cols_[i].name == expr.col.col_name) {
                    operand.kind = OperandKind::GROUP_COL;
                    operand.index = i;
                    return operand;
                }
            }
            throw ColumnNotFoundError(expr.col.col_name);
        }

        for (size_t i = 0; i < aggregates_.size(); ++i) {
            const auto& agg = aggregates_[i];
            if (!expr.display_name.empty() && agg.output_name == expr.display_name) {
                operand.kind = OperandKind::AGG_RESULT;
                operand.index = i;
                return operand;
            }
            if (agg.is_star == expr.agg.is_star && agg.col.tab_name == expr.agg.col.tab_name &&
                agg.col.col_name == expr.agg.col.col_name &&
                static_cast<int>(agg.type) == static_cast<int>(normalize_agg_type(static_cast<int>(expr.agg.type)))) {
                operand.kind = OperandKind::AGG_RESULT;
                operand.index = i;
                return operand;
            }
        }
        throw ColumnNotFoundError(expr.display_name);
    }

    /**
     * @brief 将分析阶段 HAVING 条件转换为可执行描述。
     * @tparam HavingCondT HAVING 条件类型。
     * @param cond HAVING 条件。
     * @return 内部 HAVING 条件描述。
     *
     * 函数同时处理普通右值、IN 列表、BETWEEN 上界以及 NOT/negated 信息，
     * 使执行阶段不必再依赖具体的分析结构类型。
     */
    template <typename HavingCondT> HavingSpec make_having_spec(const HavingCondT& cond) const {
        HavingSpec spec;
        spec.lhs = make_operand_from_expr(cond.lhs);
        spec.op = cond.op;
        if constexpr (has_member_negated<HavingCondT>::value) {
            spec.negated = cond.negated;
        }
        if constexpr (has_member_rhs_vals<HavingCondT>::value) {
            for (const auto& rhs_value : cond.rhs_vals) {
                HavingOperand operand;
                operand.kind = OperandKind::VALUE;
                operand.literal.type = rhs_value.type;
                if (rhs_value.type == TYPE_INT) {
                    operand.literal.int_val = rhs_value.int_val;
                } else if (rhs_value.type == TYPE_FLOAT) {
                    operand.literal.float_val = rhs_value.float_val;
                } else {
                    operand.literal.str_val = rhs_value.str_val;
                }
                spec.rhs_values.push_back(std::move(operand));
            }
        }
        if constexpr (has_member_has_rhs_upper<HavingCondT>::value && has_member_rhs_upper<HavingCondT>::value) {
            if (cond.has_rhs_upper) {
                spec.has_rhs_upper = true;
                spec.rhs_upper.kind = OperandKind::VALUE;
                spec.rhs_upper.literal.type = cond.rhs_upper.type;
                if (cond.rhs_upper.type == TYPE_INT) {
                    spec.rhs_upper.literal.int_val = cond.rhs_upper.int_val;
                } else if (cond.rhs_upper.type == TYPE_FLOAT) {
                    spec.rhs_upper.literal.float_val = cond.rhs_upper.float_val;
                } else {
                    spec.rhs_upper.literal.str_val = cond.rhs_upper.str_val;
                }
            }
        }
        if (!spec.rhs_values.empty()) {
            return spec;
        }
        if (cond.is_rhs_value) {
            spec.rhs.kind = OperandKind::VALUE;
            spec.rhs.literal.type = cond.rhs_val.type;
            if (cond.rhs_val.type == TYPE_INT) {
                spec.rhs.literal.int_val = cond.rhs_val.int_val;
            } else if (cond.rhs_val.type == TYPE_FLOAT) {
                spec.rhs.literal.float_val = cond.rhs_val.float_val;
            } else {
                spec.rhs.literal.str_val = cond.rhs_val.str_val;
            }
        } else {
            spec.rhs = make_operand_from_expr(cond.rhs_expr);
        }
        return spec;
    }

    /**
     * @brief 为一个聚合描述创建初始状态。
     * @param spec 聚合描述。
     * @return count/sum 清零且值初始化的聚合状态。
     */
    AggregateState init_aggregate_state(const AggregateSpec& spec) const {
        AggregateState state;
        state.value = zero_value(spec.input_type);
        return state;
    }

    /**
     * @brief 使用一条输入记录更新聚合状态。
     * @param state 待更新的聚合状态。
     * @param spec 聚合描述。
     * @param rec 当前输入记录。
     * @param nulls 子执行器输出列的 NULL 标记。
     *
     * 更新顺序是：识别输入 NULL、执行 DISTINCT 去重、再按 COUNT/SUM/AVG/MIN/MAX
     * 的语义累加。这样 NULL 不会污染数值聚合，也不会进入 DISTINCT 集合。
     */
    void update_aggregate_state(AggregateState& state, const AggregateSpec& spec, const RmRecord& rec,
                                const std::vector<bool>& nulls) const {
        CellValue current_value;
        bool current_is_null = false;
        if (!spec.is_star) {
            current_value = read_cell(rec, spec.input_col);
            auto pos = std::find_if(prev_->cols().begin(), prev_->cols().end(), [&](const ColMeta& col) {
                return col.tab_name == spec.input_col.tab_name && col.name == spec.input_col.name;
            });
            if (pos != prev_->cols().end()) {
                size_t index = static_cast<size_t>(pos - prev_->cols().begin());
                current_is_null = index < nulls.size() && nulls[index];
            }
        }
        if (current_is_null) {
            return;
        }
        if (spec.is_distinct) {
            if (!state.distinct_values.insert(current_value).second) {
                return;
            }
        }

        switch (spec.type) {
        case LocalAggType::COUNT:
            ++state.count;
            break;
        case LocalAggType::SUM:
            state.sum += (current_value.type == TYPE_INT) ? static_cast<double>(current_value.int_val)
                                                          : static_cast<double>(current_value.float_val);
            break;
        case LocalAggType::AVG:
            state.sum += (current_value.type == TYPE_INT) ? static_cast<double>(current_value.int_val)
                                                          : static_cast<double>(current_value.float_val);
            ++state.count;
            break;
        case LocalAggType::MAX:
            if (!state.has_value || compare_cells(current_value, state.value) > 0) {
                state.value = current_value;
                state.has_value = true;
            }
            break;
        case LocalAggType::MIN:
            if (!state.has_value || compare_cells(current_value, state.value) < 0) {
                state.value = current_value;
                state.has_value = true;
            }
            break;
        }
    }

    /**
     * @brief 将一组聚合状态批量转换为输出值。
     * @param group_state 分组状态。
     * @return 按聚合列顺序排列的最终值。
     */
    std::vector<CellValue> finalize_aggregate_values(const GroupState& group_state) const {
        std::vector<CellValue> aggregate_values;
        aggregate_values.reserve(aggregates_.size());
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            aggregate_values.push_back(finalize_aggregate(aggregates_[i], group_state.aggregate_states[i]));
        }
        return aggregate_values;
    }

    /**
     * @brief 计算单个聚合状态的最终结果。
     * @param spec 聚合描述。
     * @param state 已累积的聚合状态。
     * @return 聚合结果值；空 MIN/MAX 使用输入类型零值。
     * @throws InternalError 聚合类型未知时抛出。
     */
    CellValue finalize_aggregate(const AggregateSpec& spec, const AggregateState& state) const {
        switch (spec.type) {
        case LocalAggType::COUNT: {
            CellValue value;
            value.type = TYPE_INT;
            value.int_val = static_cast<int>(state.count);
            return value;
        }
        case LocalAggType::SUM: {
            CellValue value;
            value.type = spec.input_type;
            if (spec.input_type == TYPE_INT) {
                value.int_val = static_cast<int>(state.sum);
            } else {
                value.float_val = state.sum;
            }
            return value;
        }
        case LocalAggType::AVG: {
            CellValue value;
            value.type = TYPE_FLOAT;
            value.float_val = state.count == 0 ? 0.0 : state.sum / static_cast<double>(state.count);
            return value;
        }
        case LocalAggType::MAX:
        case LocalAggType::MIN:
            if (state.has_value) {
                return state.value;
            }
            return zero_value(spec.input_type);
        }
        throw InternalError("Unexpected aggregate type");
    }

    /**
     * @brief 将 HAVING 操作数解析为当前分组中的实际值。
     * @param operand 内部操作数描述。
     * @param group_values 当前分组列值。
     * @param group_nulls 当前分组列 NULL 标记。
     * @param aggregate_values 当前分组聚合结果。
     * @return 带 NULL 状态的实际操作数。
     */
    ResolvedHavingOperand resolve_having_operand(const HavingOperand& operand,
                                                 const std::vector<CellValue>& group_values,
                                                 const std::vector<bool>& group_nulls,
                                                 const std::vector<CellValue>& aggregate_values) const {
        switch (operand.kind) {
        case OperandKind::GROUP_COL:
            return {group_values.at(operand.index), operand.index < group_nulls.size() && group_nulls[operand.index]};
        case OperandKind::AGG_RESULT:
            return {aggregate_values.at(operand.index), false};
        case OperandKind::VALUE:
            return {operand.literal, false};
        }
        throw InternalError("Unexpected HAVING operand kind");
    }

    /**
     * @brief 判断一组已计算结果是否通过全部 HAVING 条件。
     * @param group_values 当前分组列值。
     * @param group_nulls 当前分组列 NULL 标记。
     * @param aggregate_values 当前分组聚合结果。
     * @return 全部 HAVING 条件成立时返回 true。
     *
     * IN、BETWEEN 和 LIKE 在此处分别展开处理，其余比较操作使用三路比较结果；
     * 左值或必要的右值为 NULL 时按过滤语义判为不通过。
     */
    bool passes_having(const std::vector<CellValue>& group_values,
                       const std::vector<bool>& group_nulls,
                       const std::vector<CellValue>& aggregate_values) const {
        for (const auto& cond : having_conds_) {
            auto lhs = resolve_having_operand(cond.lhs, group_values, group_nulls, aggregate_values);
            if (lhs.is_null) {
                return false;
            }
            if (cond.op == OP_IN) {
                bool matched = false;
                for (const auto& rhs_operand : cond.rhs_values) {
                    auto rhs = resolve_having_operand(rhs_operand, group_values, group_nulls, aggregate_values);
                    if (!rhs.is_null && compare_cells(lhs.value, rhs.value) == 0) {
                        matched = true;
                        break;
                    }
                }
                if (cond.negated ? matched : !matched) {
                    return false;
                }
                continue;
            }
            if (cond.op == OP_BETWEEN) {
                if (!cond.has_rhs_upper) {
                    throw InternalError("HAVING BETWEEN predicate is missing its upper bound");
                }
                auto lower = resolve_having_operand(cond.rhs, group_values, group_nulls, aggregate_values);
                auto upper = resolve_having_operand(cond.rhs_upper, group_values, group_nulls, aggregate_values);
                bool matched = !lower.is_null && !upper.is_null && compare_cells(lhs.value, lower.value) >= 0 &&
                               compare_cells(lhs.value, upper.value) <= 0;
                if (cond.negated ? matched : !matched) {
                    return false;
                }
                continue;
            }
            auto rhs = resolve_having_operand(cond.rhs, group_values, group_nulls, aggregate_values);
            if (rhs.is_null) {
                return false;
            }
            bool matched;
            if (cond.op == OP_LIKE) {
                if ((lhs.value.type != TYPE_STRING && lhs.value.type != TYPE_DATETIME) ||
                    (rhs.value.type != TYPE_STRING && rhs.value.type != TYPE_DATETIME)) {
                    throw IncompatibleTypeError(coltype2str(lhs.value.type), coltype2str(rhs.value.type));
                }
                matched = execution_scalar::like_match(lhs.value.str_val, rhs.value.str_val);
            } else {
                matched = compare_with_op(cond.op, compare_cells(lhs.value, rhs.value));
            }
            if (cond.negated ? matched : !matched) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief 使用分组状态判断 HAVING 是否通过。
     * @param group_state 待判断的分组状态。
     * @return HAVING 条件通过时返回 true。
     */
    bool passes_having(const GroupState& group_state) const {
        std::vector<CellValue> aggregate_values = finalize_aggregate_values(group_state);
        return passes_having(group_state.group_values, group_state.group_nulls, aggregate_values);
    }

    /**
     * @brief 判断是否可以仅通过遍历游标计数 COUNT(*)。
     * @return 满足无分组、无 HAVING、单个 COUNT(*) 且底层为表扫描时返回 true。
     */
    bool can_count_star_by_cursor_only() const {
        return !has_group_by_ && having_conds_.empty() && aggregates_.size() == 1 &&
               aggregates_[0].type == LocalAggType::COUNT && aggregates_[0].is_star &&
               !prev_->scan_table_name().empty();
    }

    /**
     * @brief 判断是否可以利用子执行器的升序直接求 MIN。
     * @return 单个无分组无 HAVING 的 MIN 且子执行器提供升序时返回 true。
     */
    bool can_use_min_index_shortcut() const {
        if (has_group_by_ || !having_conds_.empty() || aggregates_.size() != 1) {
            return false;
        }
        const auto& agg = aggregates_[0];
        if (agg.type != LocalAggType::MIN || agg.is_star) {
            return false;
        }
        return prev_->provides_min_order(agg.col);
    }

    /**
     * @brief 将一组分组值和聚合值写入新的输出记录。
     * @param group_state 待物化的分组状态。
     * @return 聚合结果记录。
     */
    std::unique_ptr<RmRecord> materialize_group_result(const GroupState& group_state) const {
        std::vector<CellValue> aggregate_values = finalize_aggregate_values(group_state);
        RmRecord rec(static_cast<int>(len_));
        std::memset(rec.data, 0, len_);
        for (size_t i = 0; i < group_state.group_values.size(); ++i) {
            write_cell(rec.data, cols_[i], group_state.group_values[i]);
        }
        for (size_t i = 0; i < aggregate_values.size(); ++i) {
            write_cell(rec.data, cols_[group_state.group_values.size() + i], aggregate_values[i]);
        }
        return std::make_unique<RmRecord>(rec);
    }

    /**
     * @brief 创建包含指定分组键并初始化全部聚合状态的 GroupState。
     * @param group_values 分组列值。
     * @param group_nulls 分组列 NULL 标记。
     * @return 初始化后的分组状态。
     */
    GroupState make_group_state(const std::vector<CellValue>& group_values, const std::vector<bool>& group_nulls = {}) const {
        GroupState state;
        state.group_values = group_values;
        state.group_nulls = group_nulls;
        state.aggregate_states.reserve(aggregates_.size());
        for (const auto& aggregate : aggregates_) {
            state.aggregate_states.push_back(init_aggregate_state(aggregate));
        }
        return state;
    }

    /**
     * @brief 消费子执行器并物化所有可输出分组。
     *
     * 有 GROUP BY 时通过哈希表合并相同分组键，随后应用 HAVING；无 GROUP BY
     * 时使用单个全局状态，并依次尝试 COUNT(*) 游标计数、索引有序 MIN 和完整扫描。
     */
    void materialize_groups() {
        groups_.clear();
        if (has_group_by_) {
            // 分组路径：读取输入、构造包含 NULL 标记的键，并累积每个聚合状态。
            std::unordered_map<GroupKey, size_t, GroupKeyHash> group_indexes;
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                auto rec = prev_->Next();
                if (rec == nullptr) {
                    continue;
                }

                GroupKey key;
                key.values.reserve(group_cols_.size());
                key.nulls.reserve(group_cols_.size());
                const auto& child_nulls = prev_->nulls();
                for (const auto& col : group_cols_) {
                    key.values.push_back(read_cell(*rec, col));
                    auto col_pos = std::find_if(prev_->cols().begin(), prev_->cols().end(), [&](const ColMeta& input) {
                        return input.name == col.name && input.offset == col.offset;
                    });
                    size_t col_index = static_cast<size_t>(col_pos - prev_->cols().begin());
                    key.nulls.push_back(col_index < child_nulls.size() && child_nulls[col_index]);
                }

                auto [it, inserted] = group_indexes.emplace(key, groups_.size());
                if (inserted) {
                    groups_.push_back(make_group_state(key.values, key.nulls));
                }
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    update_aggregate_state(groups_[it->second].aggregate_states[i], aggregates_[i], *rec,
                                            child_nulls);
                }
            }

            // 原地压缩掉未通过 HAVING 的分组，避免改变后续输出游标语义。
            size_t kept = 0;
            for (size_t i = 0; i < groups_.size(); ++i) {
                if (!passes_having(groups_[i])) {
                    continue;
                }
                if (kept != i) {
                    groups_[kept] = std::move(groups_[i]);
                }
                ++kept;
            }
            groups_.resize(kept);
            return;
        }

        GroupState global_state = make_group_state({});
        if (can_count_star_by_cursor_only()) {
            // COUNT(*) 不依赖具体字段值，只需统计子游标能够产生的记录数。
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                ++global_state.aggregate_states[0].count;
            }
            groups_.push_back(std::move(global_state));
            return;
        }

        if (can_use_min_index_shortcut()) {
            // 子节点按聚合列升序输出时，第一条可见记录即为 MIN，无需扫描剩余范围。
            for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
                auto rec = prev_->Next();
                if (rec == nullptr) {
                    continue;
                }
                update_aggregate_state(global_state.aggregate_states[0], aggregates_[0], *rec, prev_->nulls());
                groups_.push_back(std::move(global_state));
                return;
            }
            // 没有可见行时仍保留一个空聚合结果。
            if (passes_having(global_state)) {
                groups_.push_back(std::move(global_state));
            }
            return;
        }

        // 通用路径：完整消费所有输入记录，再统一判断全局分组是否通过 HAVING。
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec == nullptr) {
                continue;
            }
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                update_aggregate_state(global_state.aggregate_states[i], aggregates_[i], *rec, prev_->nulls());
            }
        }

        if (passes_having(global_state)) {
            groups_.push_back(std::move(global_state));
        }
    }

    /**
     * @brief 解析并建立 GROUP BY 输出列布局。
     * @param group_by_cols 分组列列表。
     * @throws ColumnNotFoundError 分组列不存在时抛出。
     */
    void init_group_cols(const std::vector<TabCol>& group_by_cols) {
        has_group_by_ = !group_by_cols.empty();
        for (const auto& group_by_col : group_by_cols) {
            ColMeta input_col = find_input_col(group_by_col);
            group_cols_.push_back(input_col);
            append_output_col(input_col, "");
        }
    }

    /**
     * @brief 从包装类型 GROUP BY 项中提取列后建立布局。
     * @tparam GroupByT 分组项类型。
     * @param group_by_cols 分组项列表。
     */
    template <typename GroupByT> void init_group_cols(const std::vector<GroupByT>& group_by_cols) {
        std::vector<TabCol> cols;
        cols.reserve(group_by_cols.size());
        for (const auto& group_by_col : group_by_cols) {
            cols.push_back(extract_group_col(group_by_col));
        }
        init_group_cols(cols);
    }

    /**
     * @brief 解析聚合表达式并建立聚合输出列布局。
     * @tparam AggExprT 聚合表达式或选择项类型。
     * @param aggregate_exprs 聚合表达式列表。
     */
    template <typename AggExprT> void init_aggregate_cols(const std::vector<AggExprT>& aggregate_exprs) {
        for (const auto& aggregate_expr : aggregate_exprs) {
            AggregateSpec spec = make_aggregate_spec(aggregate_expr);
            aggregates_.push_back(spec);

            ColMeta output_col;
            output_col.tab_name.clear();
            output_col.name = spec.output_name;
            switch (spec.type) {
            case LocalAggType::COUNT:
                output_col.type = TYPE_INT;
                output_col.len = static_cast<int>(sizeof(int));
                break;
            case LocalAggType::SUM:
                output_col.type = spec.input_type;
                output_col.len = spec.input_len;
                break;
            case LocalAggType::AVG:
                output_col.type = TYPE_FLOAT;
                output_col.len = static_cast<int>(sizeof(double));
                break;
            case LocalAggType::MAX:
            case LocalAggType::MIN:
                output_col.type = spec.input_type;
                output_col.len = spec.input_len;
                break;
            }
            append_output_col(output_col, output_col.name);
        }
    }

    /**
     * @brief 预解析 HAVING 条件。
     * @tparam HavingCondT HAVING 条件类型。
     * @param having_conds HAVING 条件列表。
     */
    template <typename HavingCondT> void init_having_conds(const std::vector<HavingCondT>& having_conds) {
        having_conds_.reserve(having_conds.size());
        for (const auto& having_cond : having_conds) {
            having_conds_.push_back(make_having_spec(having_cond));
        }
    }

public:
    /**
     * @brief 创建聚合执行器并解析分组、聚合和 HAVING 定义。
     * @tparam GroupByT 分组项类型。
     * @tparam AggExprT 聚合项类型。
     * @tparam HavingCondT HAVING 条件类型。
     * @param prev 子执行器。
     * @param group_by_cols GROUP BY 项列表。
     * @param aggregate_exprs 聚合表达式列表。
     * @param having_conds HAVING 条件列表。
     * @param context 当前执行上下文。
     */
    template <typename GroupByT, typename AggExprT, typename HavingCondT>
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<GroupByT>& group_by_cols,
                      const std::vector<AggExprT>& aggregate_exprs, const std::vector<HavingCondT>& having_conds,
                      Context* context = nullptr)
        : prev_(std::move(prev)), context_(context) {
        init_group_cols(group_by_cols);
        init_aggregate_cols(aggregate_exprs);
        init_having_conds(having_conds);
    }

    /**
     * @brief 首次调用时物化分组，并将输出游标置于第一组。
     */
    void beginTuple() override {
        if (!materialized_) {
            materialize_groups();
            materialized_ = true;
        }
        cursor_ = 0;
    }

    /**
     * @brief 将输出游标推进到下一组。
     */
    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    /**
     * @brief 返回当前分组的聚合结果记录。
     * @return 当前分组记录；游标越界时返回 nullptr。
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        nulls_.assign(cols_.size(), false);
        for (size_t i = 0; i < groups_[cursor_].group_nulls.size() && i < nulls_.size(); ++i) {
            nulls_[i] = groups_[cursor_].group_nulls[i];
        }
        return materialize_group_result(groups_[cursor_]);
    }

    /**
     * @brief 判断所有物化分组是否已经输出完毕。
     * @return 游标位于分组数组末尾时返回 true。
     */
    bool is_end() const override {
        return cursor_ >= groups_.size();
    }

    /**
     * @brief 返回聚合结果的抽象 RID。
     * @return 聚合节点维护的 RID 引用；聚合结果通常不对应底层物理记录。
     */
    Rid& rid() override {
        return _abstract_rid;
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "AggregateExecutor"。
     */
    std::string getType() override {
        return "AggregateExecutor";
    }

    /**
     * @brief 返回聚合输出列元数据。
     * @return 输出列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    /**
     * @brief 返回聚合输出元组长度。
     * @return 输出字段长度总和。
     */
    size_t tupleLen() const override {
        return len_;
    }

    /**
     * @brief 查找聚合输出列的元数据及偏移。
     * @param target 目标表列。
     * @return 匹配的列元数据。
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

    /**
     * @brief 返回当前聚合结果的 NULL 标记。
     * @return 当前分组的 NULL 标记；执行结束时返回空数组引用。
     */
    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : nulls_;
    }
};
