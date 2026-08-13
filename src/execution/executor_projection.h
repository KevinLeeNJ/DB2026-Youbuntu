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
#include <type_traits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_expr.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
    std::vector<ColMeta> cols_;              // 需要投影的字段
    size_t len_;                             // 字段总长度
    std::vector<size_t> sel_idxs_;
    std::vector<bool> nulls_;
    struct ExpressionProjection {
        QueryExpr expr;
        bool copy_source = false;
        size_t source_index = 0;
    };
    std::vector<ExpressionProjection> expression_projections_;
    QueryExprEvaluator::SubqueryRunner subquery_runner_;
    const QueryExprOuterContext* outer_context_ = nullptr;

    static int expression_length(const QueryExpr& expr, const std::vector<ColMeta>& source_cols) {
        switch (expr.type) {
        case QueryExprType::COLUMN: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return (expr.col.tab_name.empty() || col.tab_name == expr.col.tab_name) && col.name == expr.col.col_name;
            });
            return pos == source_cols.end() ? static_cast<int>(sizeof(int)) : pos->len;
        }
        case QueryExprType::AGGREGATE: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return col.name == expr.agg.display_name || col.name == expr.display_name;
            });
            return pos == source_cols.end() ? static_cast<int>(sizeof(int)) : pos->len;
        }
        case QueryExprType::VALUE:
            if (expr.value.type == TYPE_STRING || expr.value.type == TYPE_DATETIME) {
                return std::max<int>(1, expr.value.is_null ? 1 : expr.value.str_val.size());
            }
            return expr.value.type == TYPE_FLOAT ? sizeof(double) : sizeof(int);
        case QueryExprType::ARITHMETIC:
            return (expr.lhs != nullptr && expr.rhs != nullptr &&
                    (expr.lhs->type == QueryExprType::VALUE && expr.lhs->value.type == TYPE_FLOAT ||
                     expr.rhs->type == QueryExprType::VALUE && expr.rhs->value.type == TYPE_FLOAT))
                       ? sizeof(double)
                       : sizeof(int);
        case QueryExprType::LOGICAL:
        case QueryExprType::PREDICATE:
            return sizeof(int);
        case QueryExprType::CASE_EXPR: {
            int len = sizeof(int);
            for (const auto& clause : expr.case_when) {
                len = std::max(len, expression_length(*clause.second, source_cols));
            }
            if (expr.else_expr != nullptr) {
                len = std::max(len, expression_length(*expr.else_expr, source_cols));
            }
            return len;
        }
        case QueryExprType::SUBQUERY:
            return sizeof(int);
        }
        return sizeof(int);
    }

    static ColType expression_type(const QueryExpr& expr, const std::vector<ColMeta>& source_cols) {
        switch (expr.type) {
        case QueryExprType::COLUMN: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return (expr.col.tab_name.empty() || col.tab_name == expr.col.tab_name) && col.name == expr.col.col_name;
            });
            return pos == source_cols.end() ? TYPE_INT : pos->type;
        }
        case QueryExprType::AGGREGATE: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return col.name == expr.agg.display_name || col.name == expr.display_name;
            });
            return pos == source_cols.end() ? TYPE_INT : pos->type;
        }
        case QueryExprType::VALUE:
            return expr.value.type;
        case QueryExprType::ARITHMETIC:
            if (expr.lhs != nullptr && expr.rhs != nullptr &&
                (expression_type(*expr.lhs, source_cols) == TYPE_FLOAT ||
                 expression_type(*expr.rhs, source_cols) == TYPE_FLOAT)) {
                return TYPE_FLOAT;
            }
            return TYPE_INT;
        case QueryExprType::LOGICAL:
        case QueryExprType::PREDICATE:
            return TYPE_INT;
        case QueryExprType::CASE_EXPR:
            for (const auto& clause : expr.case_when) {
                ColType type = expression_type(*clause.second, source_cols);
                if (type == TYPE_STRING || type == TYPE_DATETIME || type == TYPE_FLOAT) {
                    return type;
                }
            }
            return expr.else_expr == nullptr ? TYPE_INT : expression_type(*expr.else_expr, source_cols);
        case QueryExprType::SUBQUERY:
            return TYPE_INT;
        }
        return TYPE_INT;
    }

    static void write_value(char* destination, const ColMeta& col, const EvaluatedValue& value) {
        if (value.is_null) {
            std::memset(destination + col.offset, 0, col.len);
            return;
        }
        switch (col.type) {
        case TYPE_INT:
            *reinterpret_cast<int*>(destination + col.offset) = value.cell.type == TYPE_FLOAT
                                                                      ? static_cast<int>(value.cell.float_val)
                                                                      : value.cell.int_val;
            break;
        case TYPE_FLOAT:
            *reinterpret_cast<double*>(destination + col.offset) = value.cell.type == TYPE_FLOAT
                                                                        ? value.cell.float_val
                                                                        : value.cell.int_val;
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            std::memset(destination + col.offset, 0, col.len);
            std::memcpy(destination + col.offset, value.cell.str_val.data(),
                        std::min<int>(col.len, value.cell.str_val.size()));
            break;
        }
    }

    void append_projection_col(size_t prev_idx, const std::string& output_name = "") {
        auto col = prev_->cols()[prev_idx];
        if (!output_name.empty()) {
            col.name = output_name;
            col.tab_name.clear();
        }
        col.offset = len_;
        len_ += col.len;
        cols_.push_back(col);
        sel_idxs_.push_back(prev_idx);
    }

    static std::vector<ColMeta>::const_iterator find_col_by_name(const std::vector<ColMeta>& cols,
                                                                 const std::string& name) {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) {
            return col.name == name || (!col.tab_name.empty() && (col.tab_name + "." + col.name) == name);
        });
        if (pos == cols.end()) {
            throw ColumnNotFoundError(name);
        }
        return pos;
    }

    template <typename SelectItemT> void build_from_select_items(const std::vector<SelectItemT>& select_items) {
        const auto& prev_cols = prev_->cols();
        for (const auto& item : select_items) {
            const auto& expr = item.expr;
            std::string output_name = item.display_name;
            size_t prev_idx = prev_cols.size();

            auto try_resolve_by_name = [&](const std::string& name) -> bool {
                if (name.empty()) {
                    return false;
                }
                try {
                    auto pos = find_col_by_name(prev_cols, name);
                    prev_idx = static_cast<size_t>(pos - prev_cols.begin());
                    return true;
                } catch (const ColumnNotFoundError&) {
                    return false;
                }
            };

            if (static_cast<int>(expr.type) == 0) {
                try {
                    auto pos = get_col(prev_cols, expr.col);
                    prev_idx = static_cast<size_t>(pos - prev_cols.begin());
                } catch (const ColumnNotFoundError&) {
                    try_resolve_by_name(expr.col.col_name);
                }
            } else {
                try_resolve_by_name(item.display_name) || try_resolve_by_name(expr.display_name) ||
                    try_resolve_by_name(expr.agg.display_name);
            }

            if (prev_idx == prev_cols.size()) {
                throw ColumnNotFoundError(output_name.empty() ? expr.display_name : output_name);
            }
            if (output_name.empty()) {
                output_name = prev_cols[prev_idx].name;
            }
            append_projection_col(prev_idx, output_name);
        }
    }

    void build_from_expression_items(const std::vector<SelectItem>& select_items) {
        const auto& prev_cols = prev_->cols();
        for (const auto& item : select_items) {
            ExpressionProjection projection;
            projection.expr = item.expr;
            std::string output_name = item.output_name.empty()
                                          ? (item.alias.empty() ? item.expr.display_name : item.alias)
                                          : item.output_name;
            auto find_source = [&](const std::string& name) -> size_t {
                if (name.empty()) {
                    return prev_cols.size();
                }
                for (size_t i = 0; i < prev_cols.size(); ++i) {
                    if (prev_cols[i].name == name ||
                        (!prev_cols[i].tab_name.empty() && prev_cols[i].tab_name + "." + prev_cols[i].name == name)) {
                        return i;
                    }
                }
                return prev_cols.size();
            };
            size_t source_index = prev_cols.size();
            if (item.expr.type == QueryExprType::COLUMN) {
                for (size_t i = 0; i < prev_cols.size(); ++i) {
                    if (prev_cols[i].name == item.expr.col.col_name &&
                        (item.expr.col.tab_name.empty() || prev_cols[i].tab_name == item.expr.col.tab_name)) {
                        source_index = i;
                        break;
                    }
                }
            } else if (item.expr.type == QueryExprType::AGGREGATE) {
                source_index = find_source(item.expr.display_name);
                if (source_index == prev_cols.size()) {
                    source_index = find_source(item.expr.agg.display_name);
                }
            }
            projection.copy_source = source_index != prev_cols.size();
            projection.source_index = source_index;
            expression_projections_.push_back(std::move(projection));

            ColMeta output;
            if (source_index != prev_cols.size()) {
                output = prev_cols[source_index];
            } else {
                output.type = expression_type(item.expr, prev_cols);
                output.len = expression_length(item.expr, prev_cols);
                output.tab_name.clear();
            }
            output.name = output_name;
            output.tab_name.clear();
            output.offset = static_cast<int>(len_);
            len_ += output.len;
            cols_.push_back(output);
        }
    }

public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols) {
        prev_ = std::move(prev);
        len_ = 0;

        auto& prev_cols = prev_->cols();
        for (auto& sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            append_projection_col(static_cast<size_t>(pos - prev_cols.begin()));
        }
    }

    template <typename SelectItemT, typename = std::enable_if_t<!std::is_same_v<SelectItemT, TabCol>>>
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItemT>& select_items) {
        prev_ = std::move(prev);
        len_ = 0;
        build_from_select_items(select_items);
    }

    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItem>& select_items,
                       QueryExprEvaluator::SubqueryRunner subquery_runner = {},
                       const QueryExprOuterContext* outer_context = nullptr) {
        prev_ = std::move(prev);
        len_ = 0;
        subquery_runner_ = std::move(subquery_runner);
        outer_context_ = outer_context;
        build_from_expression_items(select_items);
    }

    void beginTuple() override {
        prev_->beginTuple();          // 调用儿子节点的beginTuple方法，准备开始遍历记录
        _abstract_rid = prev_->rid(); // 初始化抽象记录号
    }

    void nextTuple() override {
        prev_->nextTuple();           // 调用儿子节点的nextTuple方法，获取下一条记录
        _abstract_rid = prev_->rid(); // 更新抽象记录号
    }

    std::unique_ptr<RmRecord> Next() override {
        if (prev_->is_end()) {
            return nullptr; // 如果儿子节点已经结束，则返回nullptr
        }
        auto rec = prev_->Next();
        if (!rec) {
            return nullptr; // 如果儿子节点没有记录，则返回nullptr
        }

        auto new_rec = std::make_unique<RmRecord>(len_);
        if (!expression_projections_.empty()) {
            nulls_.assign(expression_projections_.size(), false);
            QueryExprEvaluator evaluator(prev_->cols(), prev_->nulls(), &subquery_runner_, outer_context_);
            for (size_t i = 0; i < expression_projections_.size(); ++i) {
                const auto& projection = expression_projections_[i];
                if (projection.copy_source) {
                    const auto& source = prev_->cols()[projection.source_index];
                    std::memcpy(new_rec->data + cols_[i].offset, rec->data + source.offset, cols_[i].len);
                    if (projection.source_index < prev_->nulls().size()) {
                        nulls_[i] = prev_->nulls()[projection.source_index];
                    }
                } else {
                    auto value = evaluator.evaluate(projection.expr, *rec);
                    nulls_[i] = value.is_null;
                    write_value(new_rec->data, cols_[i], value);
                }
            }
            return new_rec;
        }
        nulls_.assign(sel_idxs_.size(), false);
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            auto& col = cols_[i];
            auto& src_col = prev_->cols()[sel_idxs_[i]];
            std::memcpy(new_rec->data + col.offset, rec->data + src_col.offset, col.len);
            const auto& prev_nulls = prev_->nulls();
            if (sel_idxs_[i] < prev_nulls.size()) {
                nulls_[i] = prev_nulls[sel_idxs_[i]];
            }
        }
        return new_rec;
    }

    Rid& rid() override {
        return _abstract_rid;
    }

    bool is_end() const override {
        return prev_->is_end(); // 判断儿子节点是否结束
    }
    std::string getType() override {
        return "ProjectionExecutor"; // 返回执行器的名称
    }
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    const std::vector<bool>& nulls() const override {
        return nulls_;
    }
    size_t tupleLen() const override {
        return len_;
    }
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

    void set_counting_enabled(bool enabled) override {
        prev_->set_counting_enabled(enabled);
    }

    void set_key_conditions(std::vector<Condition> key_conds) override {
        prev_->set_key_conditions(std::move(key_conds));
    }

    std::string scan_table_name() const override {
        return prev_->scan_table_name();
    }

    std::vector<Condition> scan_conditions() const override {
        return prev_->scan_conditions();
    }
};
