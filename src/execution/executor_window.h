/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "execution_scalar.h"
#include "executor_abstract.h"
#include "executor_expr.h"

/**
 * @brief 物化子执行器结果并计算窗口函数。
 *
 * 窗口函数不会改变行数，因此本执行器保留子执行器的所有列，并在记录尾部
 * 追加内部窗口结果列。最终投影通过 QueryExprEvaluator 读取这些内部列，
 * 从而可以继续组合算术、CASE 等普通表达式。
 */
class WindowExecutor : public AbstractExecutor {
private:
    struct MaterializedRow {
        RmRecord record;
        std::vector<bool> nulls;
        Rid rid{0, 0};
    };

    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<QueryExpr> window_exprs_;
    QueryExprEvaluator::SubqueryRunner subquery_runner_;
    const QueryExprOuterContext* outer_context_ = nullptr;
    std::vector<ColMeta> cols_;
    size_t len_ = 0;
    size_t source_len_ = 0;
    std::vector<MaterializedRow> rows_;
    size_t cursor_ = 0;
    bool materialized_ = false;

    static EvaluatedValue null_value(ColType type) {
        EvaluatedValue result;
        result.cell.type = type;
        result.is_null = true;
        return result;
    }

    static const ColMeta* find_column(const std::vector<ColMeta>& cols, const TabCol& target) {
        const ColMeta* result = nullptr;
        for (const auto& col : cols) {
            if ((!target.tab_name.empty() && col.tab_name != target.tab_name) || col.name != target.col_name) {
                continue;
            }
            if (result != nullptr) {
                throw AmbiguousColumnError(target.col_name);
            }
            result = &col;
        }
        return result;
    }

    static ColType expression_type(const QueryExpr& expr, const std::vector<ColMeta>& cols) {
        switch (expr.type) {
        case QueryExprType::COLUMN: {
            const auto* col = find_column(cols, expr.col);
            return col == nullptr ? TYPE_INT : col->type;
        }
        case QueryExprType::AGGREGATE: {
            const auto* col = find_column(cols, {.tab_name = "", .col_name = expr.agg.display_name});
            if (col != nullptr) {
                return col->type;
            }
            return expr.agg.type == AggType::AVG ? TYPE_FLOAT : TYPE_INT;
        }
        case QueryExprType::VALUE:
            return expr.value.type;
        case QueryExprType::ARITHMETIC:
            if (expr.lhs != nullptr && expr.rhs != nullptr &&
                (expression_type(*expr.lhs, cols) == TYPE_FLOAT || expression_type(*expr.rhs, cols) == TYPE_FLOAT)) {
                return TYPE_FLOAT;
            }
            return TYPE_INT;
        case QueryExprType::LOGICAL:
        case QueryExprType::PREDICATE:
            return TYPE_INT;
        case QueryExprType::CASE_EXPR:
            for (const auto& clause : expr.case_when) {
                ColType type = expression_type(*clause.second, cols);
                if (type == TYPE_FLOAT || type == TYPE_STRING || type == TYPE_DATETIME) {
                    return type;
                }
            }
            return expr.else_expr == nullptr ? TYPE_INT : expression_type(*expr.else_expr, cols);
        case QueryExprType::SUBQUERY:
            return TYPE_INT;
        case QueryExprType::WINDOW:
            return TYPE_INT;
        }
        return TYPE_INT;
    }

    static int expression_length(const QueryExpr& expr, const std::vector<ColMeta>& cols) {
        switch (expr.type) {
        case QueryExprType::COLUMN: {
            const auto* col = find_column(cols, expr.col);
            return col == nullptr ? static_cast<int>(sizeof(int)) : col->len;
        }
        case QueryExprType::VALUE:
            if (expr.value.type == TYPE_STRING || expr.value.type == TYPE_DATETIME) {
                return std::max<int>(1, static_cast<int>(expr.value.str_val.size()));
            }
            return expr.value.type == TYPE_FLOAT ? sizeof(double) : sizeof(int);
        case QueryExprType::ARITHMETIC:
            return expression_type(expr, cols) == TYPE_FLOAT ? sizeof(double) : sizeof(int);
        case QueryExprType::CASE_EXPR: {
            int length = sizeof(int);
            for (const auto& clause : expr.case_when) {
                length = std::max(length, expression_length(*clause.second, cols));
            }
            if (expr.else_expr != nullptr) {
                length = std::max(length, expression_length(*expr.else_expr, cols));
            }
            return length;
        }
        case QueryExprType::WINDOW:
            return sizeof(int);
        case QueryExprType::AGGREGATE:
        case QueryExprType::LOGICAL:
        case QueryExprType::PREDICATE:
        case QueryExprType::SUBQUERY:
            return sizeof(int);
        }
        return sizeof(int);
    }

    static ColType window_type(const QueryExpr& expr, const std::vector<ColMeta>& cols) {
        switch (expr.window_func) {
        case WindowFuncType::ROW_NUMBER:
        case WindowFuncType::RANK:
        case WindowFuncType::DENSE_RANK:
            return TYPE_INT;
        case WindowFuncType::AVG:
            return TYPE_FLOAT;
        case WindowFuncType::LAG:
        case WindowFuncType::LEAD:
        case WindowFuncType::SUM:
            return expr.window_args.empty() ? TYPE_INT : expression_type(*expr.window_args.front(), cols);
        }
        return TYPE_INT;
    }

    static int window_length(const QueryExpr& expr, const std::vector<ColMeta>& cols) {
        const ColType type = window_type(expr, cols);
        if (type == TYPE_FLOAT) {
            return sizeof(double);
        }
        if (type == TYPE_STRING || type == TYPE_DATETIME) {
            return expr.window_args.empty() ? 1 : expression_length(*expr.window_args.front(), cols);
        }
        return sizeof(int);
    }

    static void write_value(char* destination, const ColMeta& col, const EvaluatedValue& value) {
        if (value.is_null) {
            std::memset(destination + col.offset, 0, col.len);
            return;
        }
        switch (col.type) {
        case TYPE_INT:
            *reinterpret_cast<int*>(destination + col.offset) =
                value.cell.type == TYPE_FLOAT ? checked_int_cast(value.cell.float_val) : value.cell.int_val;
            break;
        case TYPE_FLOAT:
            *reinterpret_cast<double*>(destination + col.offset) =
                value.cell.type == TYPE_FLOAT ? value.cell.float_val : value.cell.int_val;
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            std::memset(destination + col.offset, 0, col.len);
            std::memcpy(destination + col.offset, value.cell.str_val.data(),
                        std::min<int>(col.len, static_cast<int>(value.cell.str_val.size())));
            break;
        }
    }

    static int compare_values(const EvaluatedValue& lhs, const EvaluatedValue& rhs, bool is_desc = false,
                              int nulls_order = 0) {
        if (lhs.is_null || rhs.is_null) {
            if (lhs.is_null && rhs.is_null) {
                return 0;
            }
            const bool nulls_first = nulls_order == 1 || (nulls_order == 0 && !is_desc);
            return lhs.is_null == nulls_first ? -1 : 1;
        }
        const int result = execution_scalar::compare_cells(lhs.cell, rhs.cell);
        return is_desc ? -result : result;
    }

    static bool equal_values(const EvaluatedValue& lhs, const EvaluatedValue& rhs) {
        if (lhs.is_null || rhs.is_null) {
            return lhs.is_null && rhs.is_null;
        }
        return execution_scalar::compare_cells(lhs.cell, rhs.cell) == 0;
    }

    static EvaluatedValue numeric_result(ColType type, double value) {
        EvaluatedValue result;
        result.cell.type = type;
        if (type == TYPE_FLOAT) {
            result.cell.float_val = value;
        } else {
            result.cell.int_val = checked_int_cast(value);
        }
        return result;
    }

    void materialize_input() {
        source_len_ = prev_->tupleLen();
        rows_.clear();
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            if (record == nullptr) {
                continue;
            }
            std::vector<bool> nulls(prev_->cols().size(), false);
            const auto& child_nulls = prev_->nulls();
            for (size_t i = 0; i < child_nulls.size() && i < nulls.size(); ++i) {
                nulls[i] = child_nulls[i];
            }
            rows_.push_back({std::move(*record), std::move(nulls), prev_->rid()});
        }
    }

    void calculate_window(const QueryExpr& expr, size_t result_index,
                          std::vector<std::vector<EvaluatedValue>>& values) {
        const size_t row_count = rows_.size();
        std::vector<std::vector<EvaluatedValue>> partition_values(row_count);
        std::vector<std::vector<EvaluatedValue>> order_values(row_count);
        std::vector<EvaluatedValue> argument_values(row_count);
        std::vector<EvaluatedValue> default_values(row_count);

        for (size_t row_index = 0; row_index < row_count; ++row_index) {
            QueryExprEvaluator evaluator(prev_->cols(), rows_[row_index].nulls, &subquery_runner_, outer_context_);
            for (const auto& partition_expr : expr.window_partition_by) {
                partition_values[row_index].push_back(evaluator.evaluate(*partition_expr, rows_[row_index].record));
            }
            for (const auto& order_expr : expr.window_order_by) {
                order_values[row_index].push_back(evaluator.evaluate(*order_expr, rows_[row_index].record));
            }
            if (!expr.window_args.empty()) {
                argument_values[row_index] = evaluator.evaluate(*expr.window_args.front(), rows_[row_index].record);
            }
            if (expr.window_args.size() == 3) {
                default_values[row_index] = evaluator.evaluate(*expr.window_args[2], rows_[row_index].record);
            }
        }

        std::vector<size_t> ordered(row_count);
        std::iota(ordered.begin(), ordered.end(), 0);
        std::stable_sort(ordered.begin(), ordered.end(), [&](size_t lhs, size_t rhs) {
            for (size_t key = 0; key < partition_values[lhs].size(); ++key) {
                int cmp = compare_values(partition_values[lhs][key], partition_values[rhs][key]);
                if (cmp != 0) {
                    return cmp < 0;
                }
            }
            for (size_t key = 0; key < order_values[lhs].size(); ++key) {
                int cmp = compare_values(order_values[lhs][key], order_values[rhs][key], expr.window_order_desc[key],
                                         expr.window_nulls_order[key]);
                if (cmp != 0) {
                    return cmp < 0;
                }
            }
            return false;
        });

        auto same_partition = [&](size_t lhs, size_t rhs) {
            if (partition_values[lhs].size() != partition_values[rhs].size()) {
                return false;
            }
            for (size_t key = 0; key < partition_values[lhs].size(); ++key) {
                if (!equal_values(partition_values[lhs][key], partition_values[rhs][key])) {
                    return false;
                }
            }
            return true;
        };
        auto same_peer = [&](size_t lhs, size_t rhs) {
            if (order_values[lhs].size() != order_values[rhs].size()) {
                return false;
            }
            for (size_t key = 0; key < order_values[lhs].size(); ++key) {
                if (!equal_values(order_values[lhs][key], order_values[rhs][key])) {
                    return false;
                }
            }
            return true;
        };

        std::vector<std::vector<size_t>> partitions;
        for (size_t pos = 0; pos < ordered.size();) {
            size_t end = pos + 1;
            while (end < ordered.size() && same_partition(ordered[pos], ordered[end])) {
                ++end;
            }
            partitions.emplace_back(ordered.begin() + static_cast<std::ptrdiff_t>(pos),
                                    ordered.begin() + static_cast<std::ptrdiff_t>(end));
            pos = end;
        }

        auto assign_sum = [&](double sum, int64_t count, size_t row_index) {
            if (count == 0) {
                values[row_index][result_index] = null_value(window_type(expr, prev_->cols()));
            } else if (expr.window_func == WindowFuncType::AVG) {
                values[row_index][result_index] = numeric_result(TYPE_FLOAT, sum / static_cast<double>(count));
            } else {
                values[row_index][result_index] = numeric_result(window_type(expr, prev_->cols()), sum);
            }
        };

        for (const auto& partition : partitions) {
            switch (expr.window_func) {
            case WindowFuncType::ROW_NUMBER:
                for (size_t pos = 0; pos < partition.size(); ++pos) {
                    values[partition[pos]][result_index] = numeric_result(TYPE_INT, static_cast<double>(pos + 1));
                }
                break;
            case WindowFuncType::RANK:
            case WindowFuncType::DENSE_RANK: {
                int rank = 1;
                int dense_rank = 1;
                for (size_t pos = 0; pos < partition.size(); ++pos) {
                    if (pos > 0 && !expr.window_order_by.empty() && !same_peer(partition[pos - 1], partition[pos])) {
                        rank = static_cast<int>(pos + 1);
                        ++dense_rank;
                    }
                    values[partition[pos]][result_index] =
                        numeric_result(TYPE_INT, expr.window_func == WindowFuncType::RANK ? rank : dense_rank);
                }
                break;
            }
            case WindowFuncType::LAG:
            case WindowFuncType::LEAD: {
                int offset = 1;
                if (expr.window_args.size() >= 2) {
                    offset = expr.window_args[1]->value.int_val;
                }
                for (size_t pos = 0; pos < partition.size(); ++pos) {
                    const bool has_target = expr.window_func == WindowFuncType::LAG
                                                ? pos >= static_cast<size_t>(offset)
                                                : pos + static_cast<size_t>(offset) < partition.size();
                    if (has_target) {
                        const size_t target_pos = expr.window_func == WindowFuncType::LAG
                                                      ? pos - static_cast<size_t>(offset)
                                                      : pos + static_cast<size_t>(offset);
                        values[partition[pos]][result_index] = argument_values[partition[target_pos]];
                    } else if (expr.window_args.size() == 3) {
                        values[partition[pos]][result_index] = default_values[partition[pos]];
                    } else {
                        values[partition[pos]][result_index] = null_value(window_type(expr, prev_->cols()));
                    }
                }
                break;
            }
            case WindowFuncType::SUM:
            case WindowFuncType::AVG: {
                double sum = 0.0;
                int64_t count = 0;
                if (expr.window_order_by.empty()) {
                    for (size_t row_index : partition) {
                        if (argument_values[row_index].is_null) {
                            continue;
                        }
                        sum += execution_scalar::promote_numeric_value(argument_values[row_index].cell);
                        ++count;
                    }
                    for (size_t row_index : partition) {
                        assign_sum(sum, count, row_index);
                    }
                    break;
                }

                for (size_t pos = 0; pos < partition.size();) {
                    size_t peer_end = pos + 1;
                    while (peer_end < partition.size() && same_peer(partition[pos], partition[peer_end])) {
                        ++peer_end;
                    }
                    for (size_t peer_pos = pos; peer_pos < peer_end; ++peer_pos) {
                        const auto& value = argument_values[partition[peer_pos]];
                        if (!value.is_null) {
                            sum += execution_scalar::promote_numeric_value(value.cell);
                            ++count;
                        }
                    }
                    for (size_t peer_pos = pos; peer_pos < peer_end; ++peer_pos) {
                        assign_sum(sum, count, partition[peer_pos]);
                    }
                    pos = peer_end;
                }
                break;
            }
            }
        }
    }

    void materialize_all() {
        materialize_input();
        std::vector<std::vector<EvaluatedValue>> values(rows_.size(),
                                                        std::vector<EvaluatedValue>(window_exprs_.size()));
        for (size_t i = 0; i < window_exprs_.size(); ++i) {
            calculate_window(window_exprs_[i], i, values);
        }

        for (size_t row_index = 0; row_index < rows_.size(); ++row_index) {
            RmRecord output(static_cast<int>(len_));
            std::memcpy(output.data, rows_[row_index].record.data, source_len_);
            rows_[row_index].record = std::move(output);
            for (size_t window_index = 0; window_index < window_exprs_.size(); ++window_index) {
                const auto& col = cols_[prev_->cols().size() + window_index];
                write_value(rows_[row_index].record.data, col, values[row_index][window_index]);
                rows_[row_index].nulls.push_back(values[row_index][window_index].is_null);
            }
        }
        materialized_ = true;
    }

public:
    WindowExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<QueryExpr> window_exprs,
                   QueryExprEvaluator::SubqueryRunner subquery_runner = {},
                   const QueryExprOuterContext* outer_context = nullptr)
        : prev_(std::move(prev)), window_exprs_(std::move(window_exprs)), subquery_runner_(std::move(subquery_runner)),
          outer_context_(outer_context) {
        cols_ = prev_->cols();
        source_len_ = prev_->tupleLen();
        len_ = source_len_;
        for (size_t i = 0; i < window_exprs_.size(); ++i) {
            auto& expr = window_exprs_[i];
            if (expr.window_result_name.empty()) {
                expr.window_result_name = "__rmdb_window_" + std::to_string(i);
            }
            ColMeta col;
            col.tab_name.clear();
            col.name = expr.window_result_name;
            col.type = window_type(expr, cols_);
            col.len = window_length(expr, cols_);
            col.offset = static_cast<int>(len_);
            len_ += static_cast<size_t>(col.len);
            cols_.push_back(std::move(col));
        }
    }

    void beginTuple() override {
        if (!materialized_) {
            materialize_all();
        }
        cursor_ = 0;
    }

    void nextTuple() override {
        if (!is_end()) {
            ++cursor_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        _abstract_rid = rows_[cursor_].rid;
        return std::make_unique<RmRecord>(rows_[cursor_].record);
    }

    bool is_end() const override {
        return cursor_ >= rows_.size();
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    std::string getType() override {
        return "WindowExecutor";
    }

    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<bool>& nulls() const override {
        static const std::vector<bool> no_nulls;
        return is_end() ? no_nulls : rows_[cursor_].nulls;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        const auto* col = find_column(cols_, target);
        if (col == nullptr) {
            throw ColumnNotFoundError(target.col_name);
        }
        return *col;
    }
};
