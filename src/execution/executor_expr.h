/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "execution_scalar.h"
#include "record/rm_defs.h"

/**
 * @brief 保存表达式求值后的标量值及其 NULL 状态。
 *
 * 即使 is_null 为 true，cell.type 仍保留表达式推断出的类型，便于上层在
 * NULL 传播和输出列布局时继续进行类型判断。
 */
struct EvaluatedValue {
    execution_scalar::CellValue cell;
    bool is_null = false;
};

/**
 * @brief 为相关子查询提供外层记录、列元数据和 NULL 位图。
 *
 * 指针均为借用关系，生命周期由调用投影/过滤执行器的上层上下文保证。
 */
struct QueryExprOuterContext {
    const RmRecord* record = nullptr;
    const std::vector<ColMeta>* cols = nullptr;
    const std::vector<bool>* nulls = nullptr;
};

/**
 * @brief 在一条记录上递归计算 QueryExpr，并实现 SQL 的 NULL/三值逻辑语义。
 *
 * 普通列、常量、算术、逻辑、CASE、谓词和子查询由本类处理；聚合表达式
 * 必须先由 AggregateExecutor 物化为列，不能直接在这里求值。
 */
class QueryExprEvaluator {
public:
    using SubqueryRunner = std::function<std::vector<EvaluatedValue>(
        const Plan&, const RmRecord&, const std::vector<ColMeta>&, const std::vector<bool>&)>;

private:
    enum class TruthValue { FALSE_VALUE, TRUE_VALUE, UNKNOWN };

    const std::vector<ColMeta>& cols_;
    const std::vector<bool>& nulls_;
    const SubqueryRunner* subquery_runner_ = nullptr;
    const QueryExprOuterContext* outer_context_ = nullptr;

    /**
     * @brief 构造指定类型的 NULL 求值结果。
     * @param type NULL 值应保留的逻辑类型，默认使用 INT。
     * @return is_null 为 true 且 cell.type 为 type 的结果。
     */
    static EvaluatedValue null_value(ColType type = TYPE_INT) {
        EvaluatedValue result;
        result.cell.type = type;
        result.is_null = true;
        return result;
    }

    /**
     * @brief 将解析阶段的 Value 转换为执行阶段的求值结果。
     * @param value 字面量值，可能自身带有 NULL 标志。
     * @return 拷贝后的类型化标量及 NULL 状态。
     */
    static EvaluatedValue literal_value(const Value& value) {
        EvaluatedValue result;
        result.cell.type = value.type;
        result.is_null = value.is_null;
        if (result.is_null) {
            return result;
        }
        if (value.type == TYPE_INT) {
            result.cell.int_val = value.int_val;
        } else if (value.type == TYPE_FLOAT) {
            result.cell.float_val = value.float_val;
        } else {
            result.cell.str_val = value.str_val;
        }
        return result;
    }

    /**
     * @brief 按 WHERE/HAVING 的过滤规则判断求值结果是否为真。
     * @param value 待转换的求值结果。
     * @return 非 NULL 且为非零 INT 时返回 true；FLOAT、字符串和 UNKNOWN 不直接视为真。
     */
    static bool is_true(const EvaluatedValue& value) {
        return !value.is_null && value.cell.type == TYPE_INT && value.cell.int_val != 0;
    }

    /**
     * @brief 将求值结果映射为 SQL 三值逻辑中的 TRUE/FALSE/UNKNOWN。
     * @param value 待转换的求值结果。
     * @return 对应的三值逻辑状态。
     */
    static TruthValue to_truth(const EvaluatedValue& value) {
        if (value.is_null) {
            return TruthValue::UNKNOWN;
        }
        return is_true(value) ? TruthValue::TRUE_VALUE : TruthValue::FALSE_VALUE;
    }

    /**
     * @brief 将三值逻辑结果编码为 INT 或 NULL 的求值结果。
     * @param value 三值逻辑状态。
     * @return TRUE/FALSE 分别编码为 1/0，UNKNOWN 编码为 INT 类型 NULL。
     */
    static EvaluatedValue truth_value(TruthValue value) {
        if (value == TruthValue::UNKNOWN) {
            return null_value(TYPE_INT);
        }
        EvaluatedValue result;
        result.cell.type = TYPE_INT;
        result.cell.int_val = value == TruthValue::TRUE_VALUE ? 1 : 0;
        return result;
    }

    /**
     * @brief 计算三值逻辑 NOT。
     * @param value 操作数的三值逻辑状态。
     * @return TRUE 与 FALSE 互换，UNKNOWN 保持 UNKNOWN。
     */
    static TruthValue logical_not(TruthValue value) {
        if (value == TruthValue::UNKNOWN) {
            return TruthValue::UNKNOWN;
        }
        return value == TruthValue::TRUE_VALUE ? TruthValue::FALSE_VALUE : TruthValue::TRUE_VALUE;
    }

    /**
     * @brief 计算三值逻辑 AND。
     * @param lhs 左操作数状态。
     * @param rhs 右操作数状态。
     * @return 按 SQL 三值逻辑真值表计算出的结果。
     */
    static TruthValue logical_and(TruthValue lhs, TruthValue rhs) {
        if (lhs == TruthValue::FALSE_VALUE || rhs == TruthValue::FALSE_VALUE) {
            return TruthValue::FALSE_VALUE;
        }
        if (lhs == TruthValue::UNKNOWN || rhs == TruthValue::UNKNOWN) {
            return TruthValue::UNKNOWN;
        }
        return TruthValue::TRUE_VALUE;
    }

    /**
     * @brief 计算三值逻辑 OR。
     * @param lhs 左操作数状态。
     * @param rhs 右操作数状态。
     * @return 按 SQL 三值逻辑真值表计算出的结果。
     */
    static TruthValue logical_or(TruthValue lhs, TruthValue rhs) {
        if (lhs == TruthValue::TRUE_VALUE || rhs == TruthValue::TRUE_VALUE) {
            return TruthValue::TRUE_VALUE;
        }
        if (lhs == TruthValue::UNKNOWN || rhs == TruthValue::UNKNOWN) {
            return TruthValue::UNKNOWN;
        }
        return TruthValue::FALSE_VALUE;
    }

    /**
     * @brief 在列元数据集合中解析一个表列引用。
     * @param cols 待搜索的列元数据。
     * @param target 目标表列；表名为空时按列名搜索。
     * @return 匹配列的地址；没有匹配列时返回 nullptr。
     * @throws AmbiguousColumnError 未限定表名且匹配多个列时抛出。
     */
    static const ColMeta* find_column_in(const std::vector<ColMeta>& cols, const TabCol& target) {
        const ColMeta* result = nullptr;
        for (const auto& col : cols) {
            bool matches = target.tab_name.empty() ? col.name == target.col_name
                                                   : col.tab_name == target.tab_name && col.name == target.col_name;
            if (!matches) {
                continue;
            }
            if (result != nullptr) {
                throw AmbiguousColumnError(target.col_name);
            }
            result = &col;
        }
        if (result == nullptr) {
            return nullptr;
        }
        return result;
    }

    /**
     * @brief 从当前记录或外层相关记录中读取列表达式。
     * @param expr 列类型的查询表达式。
     * @param rec 当前执行层的记录。
     * @return 列值及其 NULL 状态。
     * @throws AmbiguousColumnError 列引用在同一层解析出多个列时抛出。
     * @throws ColumnNotFoundError 当前层和外层上下文都找不到列时抛出。
     */
    EvaluatedValue evaluate_column(const QueryExpr& expr, const RmRecord& rec) const {
        const ColMeta* local_col = find_column_in(cols_, expr.col);
        const ColMeta* col = local_col;
        const RmRecord* record = &rec;
        const std::vector<bool>* nulls = &nulls_;
        // 优先解析子查询自己的列；只有本层不存在时，才回退到外层相关记录。
        if (col == nullptr && outer_context_ != nullptr && outer_context_->record != nullptr &&
            outer_context_->cols != nullptr && outer_context_->nulls != nullptr) {
            col = find_column_in(*outer_context_->cols, expr.col);
            record = outer_context_->record;
            nulls = outer_context_->nulls;
        }
        if (col == nullptr) {
            throw ColumnNotFoundError(expr.col.col_name);
        }
        size_t index = static_cast<size_t>(col - (record == &rec ? cols_.data() : outer_context_->cols->data()));
        if (index < nulls->size() && (*nulls)[index]) {
            return null_value(col->type);
        }
        EvaluatedValue result;
        result.cell.type = col->type;
        const char* data = record->data + col->offset;
        switch (col->type) {
        case TYPE_INT:
            result.cell.int_val = *reinterpret_cast<const int*>(data);
            break;
        case TYPE_FLOAT:
            result.cell.float_val = *reinterpret_cast<const double*>(data);
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            result.cell.str_val = execution_scalar::trim_string(data, col->len);
            break;
        }
        return result;
    }

    /**
     * @brief 计算二元数值算术表达式。
     * @param expr 包含左右子表达式和算术操作符的表达式。
     * @param rec 当前记录，用于递归求值列操作数。
     * @return 算术结果；任一操作数为 NULL 时返回保留数值类型的 NULL。
     * @throws IncompatibleTypeError 任一操作数不是数值类型时抛出。
     * @throws InternalError 除数为零时抛出。
     */
    EvaluatedValue evaluate_arithmetic(const QueryExpr& expr, const RmRecord& rec) const {
        auto lhs = evaluate(*expr.lhs, rec);
        auto rhs = evaluate(*expr.rhs, rec);
        // SQL 算术遵循 NULL 传播；先确定结果类型，再避免读取不存在的数值字段。
        if (lhs.is_null || rhs.is_null) {
            return null_value(lhs.cell.type == TYPE_FLOAT || rhs.cell.type == TYPE_FLOAT ? TYPE_FLOAT : TYPE_INT);
        }
        if (!execution_scalar::is_numeric_type(lhs.cell.type) ||
            !execution_scalar::is_numeric_type(rhs.cell.type)) {
            throw IncompatibleTypeError(coltype2str(lhs.cell.type), coltype2str(rhs.cell.type));
        }
        bool floating = lhs.cell.type == TYPE_FLOAT || rhs.cell.type == TYPE_FLOAT;
        double left = execution_scalar::promote_numeric_value(lhs.cell);
        double right = execution_scalar::promote_numeric_value(rhs.cell);
        if (expr.arithmetic_op == QueryArithmeticOp::DIV && right == 0.0) {
            throw InternalError("division by zero");
        }
        double value = 0.0;
        // 先以 double 统一计算，只有两个操作数都是 INT 且不是除法时才回写 INT。
        switch (expr.arithmetic_op) {
        case QueryArithmeticOp::ADD:
            value = left + right;
            break;
        case QueryArithmeticOp::SUB:
            value = left - right;
            break;
        case QueryArithmeticOp::MUL:
            value = left * right;
            break;
        case QueryArithmeticOp::DIV:
            value = left / right;
            floating = true;
            break;
        }
        EvaluatedValue result;
        result.cell.type = floating ? TYPE_FLOAT : TYPE_INT;
        if (floating) {
            result.cell.float_val = value;
        } else {
            result.cell.int_val = static_cast<int>(value);
        }
        return result;
    }

    /**
     * @brief 计算 NOT、AND 或 OR 逻辑表达式。
     * @param expr 逻辑操作符和操作数列表。
     * @param rec 当前记录。
     * @return 编码为 INT/NULL 的三值逻辑结果。
     * @throws InternalError NOT 操作数数量错误或逻辑表达式没有操作数时抛出。
     */
    EvaluatedValue evaluate_logical(const QueryExpr& expr, const RmRecord& rec) const {
        if (expr.logical_op == QueryLogicalOp::NOT) {
            if (expr.operands.size() != 1) {
                throw InternalError("NOT expression must have one operand");
            }
            return truth_value(logical_not(to_truth(evaluate(*expr.operands[0], rec))));
        }
        if (expr.operands.empty()) {
            throw InternalError("Logical expression must have an operand");
        }
        // 逐个合并三值逻辑状态，不能用普通 bool 短路替代 UNKNOWN 的传播规则。
        TruthValue result = to_truth(evaluate(*expr.operands.front(), rec));
        for (size_t i = 1; i < expr.operands.size(); ++i) {
            TruthValue next = to_truth(evaluate(*expr.operands[i], rec));
            result = expr.logical_op == QueryLogicalOp::AND ? logical_and(result, next) : logical_or(result, next);
        }
        return truth_value(result);
    }

    /**
     * @brief 按顺序计算 CASE WHEN 分支并返回第一个真分支的结果。
     * @param expr 包含 WHEN 条件、THEN 结果和可选 ELSE 的 CASE 表达式。
     * @param rec 当前记录。
     * @return 首个满足条件的 THEN 值、ELSE 值，或带有推断类型的 NULL。
     */
    EvaluatedValue evaluate_case(const QueryExpr& expr, const RmRecord& rec) const {
        ColType result_type = TYPE_INT;
        if (!expr.case_when.empty() && expr.case_when.front().second != nullptr) {
            result_type = evaluate(*expr.case_when.front().second, rec).cell.type;
        }
        // WHEN 的 NULL/UNKNOWN 不算真，只有 is_true() 返回 true 才选择对应 THEN。
        for (const auto& clause : expr.case_when) {
            if (is_true(evaluate(*clause.first, rec))) {
                return evaluate(*clause.second, rec);
            }
        }
        if (expr.else_expr != nullptr) {
            return evaluate(*expr.else_expr, rec);
        }
        return null_value(result_type);
    }

    /**
     * @brief 执行标量子查询并取其第一项结果。
     * @param expr 带有已规划 subquery_plan 的子查询表达式。
     * @param rec 当前外层记录，用于相关子查询参数传递。
     * @return 子查询第一项；子查询无行时返回 INT 类型 NULL。
     * @throws InternalError 未配置子查询执行器或表达式尚未规划时抛出。
     */
    EvaluatedValue scalar_subquery(const QueryExpr& expr, const RmRecord& rec) const {
        if (subquery_runner_ == nullptr) {
            throw InternalError("Subquery evaluator is not configured");
        }
        if (expr.subquery_plan == nullptr) {
            throw InternalError("Subquery expression has not been planned");
        }
        auto values = (*subquery_runner_)(*expr.subquery_plan, rec, cols_, nulls_);
        if (values.empty()) {
            return null_value();
        }
        return values.front();
    }

    /**
     * @brief 计算比较、LIKE、IN、BETWEEN、NULL、EXISTS 和量化谓词。
     * @param expr 谓词表达式及其右值/子查询信息。
     * @param rec 当前记录。
     * @return 编码为 TRUE/FALSE/UNKNOWN 的求值结果。
     * @throws IncompatibleTypeError 比较类型不兼容或 LIKE 用于非字符串时抛出。
     * @throws InternalError 子查询、IN/BETWEEN 边界或量化谓词结构不完整时抛出。
     *
     * 普通比较先处理 NULL，再执行类型检查和三路比较；IN、BETWEEN、ANY、ALL
     * 各自维护自己的三值累积状态，最后统一经过 truth_value() 编码。
     */
    EvaluatedValue evaluate_predicate(const QueryExpr& expr, const RmRecord& rec) const {
        // EXISTS 只关心子查询是否返回行，不读取子查询返回值本身。
        if (expr.predicate_op == OP_EXISTS) {
            if (subquery_runner_ == nullptr || expr.rhs == nullptr || expr.rhs->subquery_plan == nullptr) {
                throw InternalError("EXISTS predicate is missing its subquery");
            }
            bool exists = !(*subquery_runner_)(*expr.rhs->subquery_plan, rec, cols_, nulls_).empty();
            return truth_value(exists ? TruthValue::TRUE_VALUE : TruthValue::FALSE_VALUE);
        }

        auto lhs = expr.lhs == nullptr ? null_value() : evaluate(*expr.lhs, rec);
        if (expr.predicate_op == OP_IS_NULL || expr.predicate_op == OP_IS_NOT_NULL) {
            bool result = expr.predicate_op == OP_IS_NULL ? lhs.is_null : !lhs.is_null;
            return truth_value(result ? TruthValue::TRUE_VALUE : TruthValue::FALSE_VALUE);
        }

        auto compare_one = [&](const EvaluatedValue& rhs, CompOp op) -> TruthValue {
            if (lhs.is_null || rhs.is_null) {
                return TruthValue::UNKNOWN;
            }
            if (op == OP_LIKE) {
                if ((lhs.cell.type != TYPE_STRING && lhs.cell.type != TYPE_DATETIME) ||
                    (rhs.cell.type != TYPE_STRING && rhs.cell.type != TYPE_DATETIME)) {
                    throw IncompatibleTypeError(coltype2str(lhs.cell.type), coltype2str(rhs.cell.type));
                }
                bool matched = execution_scalar::like_match(lhs.cell.str_val, rhs.cell.str_val);
                return (expr.negated ? !matched : matched) ? TruthValue::TRUE_VALUE : TruthValue::FALSE_VALUE;
            }
            if (!((execution_scalar::is_numeric_type(lhs.cell.type) &&
                   execution_scalar::is_numeric_type(rhs.cell.type)) ||
                  ((lhs.cell.type == TYPE_STRING || lhs.cell.type == TYPE_DATETIME) &&
                   (rhs.cell.type == TYPE_STRING || rhs.cell.type == TYPE_DATETIME)))) {
                throw IncompatibleTypeError(coltype2str(lhs.cell.type), coltype2str(rhs.cell.type));
            }
            bool matched = false;
            int cmp = execution_scalar::compare_cells(lhs.cell, rhs.cell);
            switch (op) {
            case OP_EQ:
                matched = cmp == 0;
                break;
            case OP_NE:
                matched = cmp != 0;
                break;
            case OP_LT:
                matched = cmp < 0;
                break;
            case OP_GT:
                matched = cmp > 0;
                break;
            case OP_LE:
                matched = cmp <= 0;
                break;
            case OP_GE:
                matched = cmp >= 0;
                break;
            default:
                throw InternalError("Unexpected comparison operator");
            }
            return matched ? TruthValue::TRUE_VALUE : TruthValue::FALSE_VALUE;
        };

        // IN 需要在字面量和子查询结果之间合并匹配状态；BETWEEN 则组合上下界比较。
        if (expr.predicate_op == OP_IN || expr.predicate_op == OP_BETWEEN) {
            TruthValue result = TruthValue::FALSE_VALUE;
            if (expr.predicate_op == OP_IN) {
                std::vector<EvaluatedValue> subquery_values;
                if (expr.rhs != nullptr) {
                    if (expr.rhs->type != QueryExprType::SUBQUERY || expr.rhs->subquery_plan == nullptr ||
                        subquery_runner_ == nullptr) {
                        throw InternalError("IN predicate has an invalid subquery");
                    }
                    subquery_values =
                        (*subquery_runner_)(*expr.rhs->subquery_plan, rec, cols_, nulls_);
                }
                auto compare_values = [&](const EvaluatedValue& value) {
                    TruthValue current = compare_one(value, OP_EQ);
                    if (current == TruthValue::TRUE_VALUE) {
                        result = TruthValue::TRUE_VALUE;
                    } else if (current == TruthValue::UNKNOWN) {
                        result = TruthValue::UNKNOWN;
                    }
                };
                for (const auto& value : subquery_values) {
                    compare_values(value);
                    if (result == TruthValue::TRUE_VALUE) {
                        break;
                    }
                }
                for (const auto& value : expr.rhs_values) {
                    if (result == TruthValue::TRUE_VALUE) {
                        break;
                    }
                    TruthValue current = compare_one(evaluate(*value, rec), OP_EQ);
                    if (current == TruthValue::TRUE_VALUE) {
                        result = TruthValue::TRUE_VALUE;
                        break;
                    }
                    if (current == TruthValue::UNKNOWN) {
                        result = TruthValue::UNKNOWN;
                    }
                }
            } else {
                if (expr.rhs == nullptr || expr.rhs_upper == nullptr) {
                    throw InternalError("BETWEEN predicate is missing a bound");
                }
                result = logical_and(compare_one(evaluate(*expr.rhs, rec), OP_GE),
                                     compare_one(evaluate(*expr.rhs_upper, rec), OP_LE));
            }
            if (expr.negated) {
                result = logical_not(result);
            }
            return truth_value(result);
        }

        // ANY 命中一个 TRUE 即结束，ALL 遇到一个 FALSE 即结束；UNKNOWN 只能延迟保留。
        if (expr.quantifier != QueryQuantifier::NONE) {
            if (expr.rhs == nullptr || expr.rhs->subquery_plan == nullptr || subquery_runner_ == nullptr) {
                throw InternalError("Quantified predicate is missing its subquery");
            }
            auto values = (*subquery_runner_)(*expr.rhs->subquery_plan, rec, cols_, nulls_);
            if (expr.quantifier == QueryQuantifier::ANY) {
                TruthValue result = TruthValue::FALSE_VALUE;
                for (const auto& value : values) {
                    TruthValue current = compare_one(value, expr.predicate_op);
                    if (current == TruthValue::TRUE_VALUE) {
                        result = TruthValue::TRUE_VALUE;
                        break;
                    }
                    if (current == TruthValue::UNKNOWN) {
                        result = TruthValue::UNKNOWN;
                    }
                }
                return truth_value(result);
            }
            TruthValue result = TruthValue::TRUE_VALUE;
            for (const auto& value : values) {
                TruthValue current = compare_one(value, expr.predicate_op);
                if (current == TruthValue::FALSE_VALUE) {
                    result = TruthValue::FALSE_VALUE;
                    break;
                }
                if (current == TruthValue::UNKNOWN) {
                    result = TruthValue::UNKNOWN;
                }
            }
            return truth_value(result);
        }

        EvaluatedValue rhs = expr.rhs == nullptr ? null_value() :
                                                   (expr.rhs->type == QueryExprType::SUBQUERY
                                                        ? scalar_subquery(*expr.rhs, rec)
                                                        : evaluate(*expr.rhs, rec));
        return truth_value(compare_one(rhs, expr.predicate_op));
    }

public:
    /**
     * @brief 创建使用指定列元数据和 NULL 位图的表达式求值器。
     * @param cols 当前执行层记录的列元数据。
     * @param nulls 当前执行层记录的 NULL 位图。
     * @param subquery_runner 可选的子查询执行回调，未配置时不能计算子查询。
     * @param outer_context 可选的外层记录上下文，用于相关子查询列解析。
     */
    QueryExprEvaluator(const std::vector<ColMeta>& cols, const std::vector<bool>& nulls,
                       const SubqueryRunner* subquery_runner = nullptr,
                       const QueryExprOuterContext* outer_context = nullptr)
        : cols_(cols), nulls_(nulls), subquery_runner_(subquery_runner), outer_context_(outer_context) {}

    /**
     * @brief 按表达式类型分派并计算一条记录上的查询表达式。
     * @param expr 待计算的查询表达式树。
     * @param rec 当前记录。
     * @return 表达式结果及 NULL 状态。
     * @throws InternalError 聚合表达式不应由本类计算，或表达式类型未知时抛出。
     *
     * 该函数只负责顶层分派，具体语义由各类 evaluate_* 辅助函数实现；递归子表达式
     * 继续共享同一列元数据、NULL 位图、子查询回调和外层上下文。
     */
    EvaluatedValue evaluate(const QueryExpr& expr, const RmRecord& rec) const {
        switch (expr.type) {
        case QueryExprType::COLUMN:
            return evaluate_column(expr, rec);
        case QueryExprType::AGGREGATE:
            throw InternalError("Aggregate expressions must be evaluated by AggregateExecutor");
        case QueryExprType::VALUE:
            return literal_value(expr.value);
        case QueryExprType::ARITHMETIC:
            return evaluate_arithmetic(expr, rec);
        case QueryExprType::LOGICAL:
            return evaluate_logical(expr, rec);
        case QueryExprType::CASE_EXPR:
            return evaluate_case(expr, rec);
        case QueryExprType::PREDICATE:
            return evaluate_predicate(expr, rec);
        case QueryExprType::SUBQUERY:
            return scalar_subquery(expr, rec);
        }
        throw InternalError("Unexpected query expression type");
    }

    /**
     * @brief 将表达式结果转换为过滤器使用的真假判断。
     * @param expr 待计算的谓词或逻辑表达式。
     * @param rec 当前记录。
     * @return 结果为非 NULL 且逻辑上为真时返回 true；FALSE/UNKNOWN 返回 false。
     */
    bool matches(const QueryExpr& expr, const RmRecord& rec) const {
        return is_true(evaluate(expr, rec));
    }
};
