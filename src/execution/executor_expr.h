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

struct EvaluatedValue {
    execution_scalar::CellValue cell;
    bool is_null = false;
};

struct QueryExprOuterContext {
    const RmRecord* record = nullptr;
    const std::vector<ColMeta>* cols = nullptr;
    const std::vector<bool>* nulls = nullptr;
};

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

    static EvaluatedValue null_value(ColType type = TYPE_INT) {
        EvaluatedValue result;
        result.cell.type = type;
        result.is_null = true;
        return result;
    }

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

    static bool is_true(const EvaluatedValue& value) {
        return !value.is_null && value.cell.type == TYPE_INT && value.cell.int_val != 0;
    }

    static TruthValue to_truth(const EvaluatedValue& value) {
        if (value.is_null) {
            return TruthValue::UNKNOWN;
        }
        return is_true(value) ? TruthValue::TRUE_VALUE : TruthValue::FALSE_VALUE;
    }

    static EvaluatedValue truth_value(TruthValue value) {
        if (value == TruthValue::UNKNOWN) {
            return null_value(TYPE_INT);
        }
        EvaluatedValue result;
        result.cell.type = TYPE_INT;
        result.cell.int_val = value == TruthValue::TRUE_VALUE ? 1 : 0;
        return result;
    }

    static TruthValue logical_not(TruthValue value) {
        if (value == TruthValue::UNKNOWN) {
            return TruthValue::UNKNOWN;
        }
        return value == TruthValue::TRUE_VALUE ? TruthValue::FALSE_VALUE : TruthValue::TRUE_VALUE;
    }

    static TruthValue logical_and(TruthValue lhs, TruthValue rhs) {
        if (lhs == TruthValue::FALSE_VALUE || rhs == TruthValue::FALSE_VALUE) {
            return TruthValue::FALSE_VALUE;
        }
        if (lhs == TruthValue::UNKNOWN || rhs == TruthValue::UNKNOWN) {
            return TruthValue::UNKNOWN;
        }
        return TruthValue::TRUE_VALUE;
    }

    static TruthValue logical_or(TruthValue lhs, TruthValue rhs) {
        if (lhs == TruthValue::TRUE_VALUE || rhs == TruthValue::TRUE_VALUE) {
            return TruthValue::TRUE_VALUE;
        }
        if (lhs == TruthValue::UNKNOWN || rhs == TruthValue::UNKNOWN) {
            return TruthValue::UNKNOWN;
        }
        return TruthValue::FALSE_VALUE;
    }

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

    EvaluatedValue evaluate_column(const QueryExpr& expr, const RmRecord& rec) const {
        const ColMeta* local_col = find_column_in(cols_, expr.col);
        const ColMeta* col = local_col;
        const RmRecord* record = &rec;
        const std::vector<bool>* nulls = &nulls_;
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

    EvaluatedValue evaluate_arithmetic(const QueryExpr& expr, const RmRecord& rec) const {
        auto lhs = evaluate(*expr.lhs, rec);
        auto rhs = evaluate(*expr.rhs, rec);
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
        TruthValue result = to_truth(evaluate(*expr.operands.front(), rec));
        for (size_t i = 1; i < expr.operands.size(); ++i) {
            TruthValue next = to_truth(evaluate(*expr.operands[i], rec));
            result = expr.logical_op == QueryLogicalOp::AND ? logical_and(result, next) : logical_or(result, next);
        }
        return truth_value(result);
    }

    EvaluatedValue evaluate_case(const QueryExpr& expr, const RmRecord& rec) const {
        ColType result_type = TYPE_INT;
        if (!expr.case_when.empty() && expr.case_when.front().second != nullptr) {
            result_type = evaluate(*expr.case_when.front().second, rec).cell.type;
        }
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

    EvaluatedValue evaluate_predicate(const QueryExpr& expr, const RmRecord& rec) const {
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
    QueryExprEvaluator(const std::vector<ColMeta>& cols, const std::vector<bool>& nulls,
                       const SubqueryRunner* subquery_runner = nullptr,
                       const QueryExprOuterContext* outer_context = nullptr)
        : cols_(cols), nulls_(nulls), subquery_runner_(subquery_runner), outer_context_(outer_context) {}

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

    bool matches(const QueryExpr& expr, const RmRecord& rec) const {
        return is_true(evaluate(expr, rec));
    }
};
