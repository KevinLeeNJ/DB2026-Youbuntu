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

/**
 * @brief 从子执行器输出中选择列，或计算表达式后重新组织元组。
 *
 * 简单列投影直接复制源字段；表达式投影则先建立输出列布局，再在 Next()
 * 中逐项求值并写入新记录，同时维护与输出列一一对应的 NULL 标记。
 */
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

    /**
     * @brief 推导表达式结果所需的物理存储长度。
     * @param expr 待推导的查询表达式。
     * @param source_cols 子执行器输出列元数据。
     * @return 结果字段长度。
     *
     * CASE 表达式取各分支的最大长度，以保证任意运行时分支都能写入输出槽位；
     * 逻辑、谓词和子查询结果按整数结果处理。
     */
    static int expression_length(const QueryExpr& expr, const std::vector<ColMeta>& source_cols) {
        switch (expr.type) {
        case QueryExprType::COLUMN: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return (expr.col.tab_name.empty() || col.tab_name == expr.col.tab_name) &&
                       col.name == expr.col.col_name;
            });
            return pos == source_cols.end() ? static_cast<int>(sizeof(int)) : pos->len;
        }
        case QueryExprType::AGGREGATE: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return col.name == expr.agg.display_name || col.name == expr.display_name;
            });
            return pos == source_cols.end() ? static_cast<int>(sizeof(int)) : pos->len;
        }
        case QueryExprType::WINDOW: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(),
                                    [&](const ColMeta& col) { return col.name == expr.window_result_name; });
            return pos == source_cols.end() ? static_cast<int>(sizeof(int)) : pos->len;
        }
        case QueryExprType::VALUE:
            if (expr.value.type == TYPE_STRING || expr.value.type == TYPE_DATETIME) {
                return std::max<int>(1, expr.value.is_null ? 1 : expr.value.str_val.size());
            }
            return expr.value.type == TYPE_FLOAT ? sizeof(double) : sizeof(int);
        case QueryExprType::ARITHMETIC:
            return expression_type(expr, source_cols) == TYPE_FLOAT ? sizeof(double) : sizeof(int);
        case QueryExprType::SCALAR_FUNCTION:
            if (expr.scalar_func == ScalarFuncType::LENGTH) {
                return sizeof(int);
            }
            if (expr.scalar_func == ScalarFuncType::ABS || expr.scalar_func == ScalarFuncType::ROUND) {
                return expression_type(expr, source_cols) == TYPE_FLOAT ? sizeof(double) : sizeof(int);
            }
            if (expr.scalar_func == ScalarFuncType::NULLIF && !expr.operands.empty()) {
                return expression_length(*expr.operands.front(), source_cols);
            }
            {
                int len = 1;
                for (const auto& arg : expr.operands) {
                    len = std::max(len, expression_length(*arg, source_cols));
                }
                return len;
            }
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
            return expr.subquery != nullptr && expr.subquery->output_cols.size() == 1
                       ? expr.subquery->output_cols[0].len
                       : sizeof(int);
        }
        return sizeof(int);
    }

    /**
     * @brief 推导表达式结果的列类型。
     * @param expr 待推导的查询表达式。
     * @param source_cols 子执行器输出列元数据。
     * @return 结果列类型。
     */
    static ColType expression_type(const QueryExpr& expr, const std::vector<ColMeta>& source_cols) {
        switch (expr.type) {
        case QueryExprType::COLUMN: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return (expr.col.tab_name.empty() || col.tab_name == expr.col.tab_name) &&
                       col.name == expr.col.col_name;
            });
            return pos == source_cols.end() ? TYPE_INT : pos->type;
        }
        case QueryExprType::AGGREGATE: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(), [&](const ColMeta& col) {
                return col.name == expr.agg.display_name || col.name == expr.display_name;
            });
            return pos == source_cols.end() ? TYPE_INT : pos->type;
        }
        case QueryExprType::WINDOW: {
            auto pos = std::find_if(source_cols.begin(), source_cols.end(),
                                    [&](const ColMeta& col) { return col.name == expr.window_result_name; });
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
        case QueryExprType::SCALAR_FUNCTION:
            if (expr.scalar_func == ScalarFuncType::LENGTH) {
                return TYPE_INT;
            }
            if (expr.scalar_func == ScalarFuncType::LOWER || expr.scalar_func == ScalarFuncType::UPPER ||
                expr.scalar_func == ScalarFuncType::TRIM) {
                return TYPE_STRING;
            }
            if (expr.scalar_func == ScalarFuncType::ABS || expr.scalar_func == ScalarFuncType::ROUND ||
                expr.scalar_func == ScalarFuncType::NULLIF) {
                return expr.operands.empty() ? TYPE_INT : expression_type(*expr.operands.front(), source_cols);
            }
            {
                ColType type = TYPE_INT;
                for (const auto& arg : expr.operands) {
                    ColType arg_type = expression_type(*arg, source_cols);
                    if (arg_type == TYPE_STRING || arg_type == TYPE_DATETIME) {
                        return arg_type;
                    }
                    if (arg_type == TYPE_FLOAT) {
                        type = TYPE_FLOAT;
                    }
                }
                return type;
            }
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
            return expr.subquery != nullptr && expr.subquery->output_cols.size() == 1
                       ? expr.subquery->output_cols[0].type
                       : TYPE_INT;
        }
        return TYPE_INT;
    }

    /**
     * @brief 将表达式求值结果按输出列类型写入记录缓冲区。
     * @param destination 目标记录数据首地址。
     * @param col 目标列元数据，包含偏移、长度和类型。
     * @param value 已求值的值及其 NULL 状态。
     *
     * NULL 不使用额外的物理哨兵值，而是将字段区域清零并由 nulls_ 单独记录；
     * 数值类型之间在写入时完成必要的 int/float 转换，字符串则按列宽截断或补零。
     */
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
                        std::min<int>(col.len, value.cell.str_val.size()));
            break;
        }
    }

    /**
     * @brief 将一个子执行器源列追加到投影输出布局。
     * @param prev_idx 源列在子执行器列数组中的下标。
     * @param output_name 可选的输出列名；为空时保留源列名。
     */
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

    /**
     * @brief 按裸列名或 table.column 名称查找列元数据。
     * @param cols 待搜索的列元数据。
     * @param name 裸列名或限定列名。
     * @return 匹配列的迭代器。
     * @throws ColumnNotFoundError 找不到目标列时抛出。
     */
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

    /**
     * @brief 根据旧式选择项建立“源列复制”投影布局。
     * @tparam SelectItemT 具有 expr/display_name 字段的选择项类型。
     * @param select_items 查询中的选择项。
     *
     * 该路径优先按列对象解析；解析失败时再按显示名尝试匹配聚合输出列，
     * 以兼容分析阶段生成的不同选择项表示。
     */
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

            // 普通列优先使用完整 TabCol 解析，失败后退化为裸列名匹配。
            if (static_cast<int>(expr.type) == 0) {
                try {
                    auto pos = get_col(prev_cols, expr.col);
                    prev_idx = static_cast<size_t>(pos - prev_cols.begin());
                } catch (const ColumnNotFoundError&) {
                    try_resolve_by_name(expr.col.col_name);
                }
            } else {
                // 聚合或其他已经由前置算子产生的表达式，按显示名寻找源列。
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

    /**
     * @brief 根据 QueryExpr 选择项建立表达式投影布局。
     * @param select_items 查询中的表达式选择项。
     *
     * 对能直接对应子列的 COLUMN/AGGREGATE 使用复制路径，避免重复求值；
     * 其余表达式根据静态推导的类型和长度分配新的输出列槽位。
     */
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
            // 只有能唯一定位到现有源列的简单列/聚合才走零转换复制路径。
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
    /**
     * @brief 创建按 TabCol 列表进行简单字段复制的投影执行器。
     * @param prev 子执行器。
     * @param sel_cols 要输出的列列表及顺序。
     * @throws ColumnNotFoundError 选择列不在子执行器输出中时抛出。
     */
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols) {
        prev_ = std::move(prev);
        len_ = 0;

        auto& prev_cols = prev_->cols();
        for (auto& sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            append_projection_col(static_cast<size_t>(pos - prev_cols.begin()));
        }
    }

    /**
     * @brief 创建兼容旧式选择项的投影执行器。
     * @tparam SelectItemT 选择项类型，不能是 TabCol。
     * @param prev 子执行器。
     * @param select_items 选择项列表。
     * @throws ColumnNotFoundError 无法解析选择项对应源列时抛出。
     */
    template <typename SelectItemT, typename = std::enable_if_t<!std::is_same_v<SelectItemT, TabCol>>>
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItemT>& select_items) {
        prev_ = std::move(prev);
        len_ = 0;
        build_from_select_items(select_items);
    }

    /**
     * @brief 创建支持 QueryExpr 求值的投影执行器。
     * @param prev 子执行器。
     * @param select_items 表达式选择项列表。
     * @param subquery_runner 执行表达式内部子查询的回调。
     * @param outer_context 相关子查询访问的外层行上下文。
     */
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<SelectItem>& select_items,
                       QueryExprEvaluator::SubqueryRunner subquery_runner = {},
                       const QueryExprOuterContext* outer_context = nullptr) {
        prev_ = std::move(prev);
        len_ = 0;
        subquery_runner_ = std::move(subquery_runner);
        outer_context_ = outer_context;
        build_from_expression_items(select_items);
    }

    /**
     * @brief 初始化子执行器并同步当前记录 RID。
     */
    void beginTuple() override {
        prev_->beginTuple();          // 调用儿子节点的beginTuple方法，准备开始遍历记录
        _abstract_rid = prev_->rid(); // 初始化抽象记录号
    }

    /**
     * @brief 推进子执行器并同步当前记录 RID。
     */
    void nextTuple() override {
        prev_->nextTuple();           // 调用儿子节点的nextTuple方法，获取下一条记录
        _abstract_rid = prev_->rid(); // 更新抽象记录号
    }

    /**
     * @brief 生成当前记录的投影结果。
     * @return 新建的投影记录；子执行器结束或没有当前记录时返回 nullptr。
     *
     * 表达式投影逐列求值并写入；简单投影则按 sel_idxs_ 直接复制字段，
     * 两条路径都同步更新 nulls_，供上层算子继续执行三值逻辑。
     */
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
                    // 源列可直接复用时只复制字节，并继承源列的 NULL 标记。
                    const auto& source = prev_->cols()[projection.source_index];
                    std::memcpy(new_rec->data + cols_[i].offset, rec->data + source.offset, cols_[i].len);
                    if (projection.source_index < prev_->nulls().size()) {
                        nulls_[i] = prev_->nulls()[projection.source_index];
                    }
                } else {
                    // 复杂表达式需要在当前输入记录上求值，再按输出列类型落盘。
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

    /**
     * @brief 返回当前投影记录对应的 RID。
     * @return 当前抽象记录号的引用。
     */
    Rid& rid() override {
        return _abstract_rid;
    }

    /**
     * @brief 判断子执行器是否已经耗尽。
     * @return 子执行器结束时返回 true。
     */
    bool is_end() const override {
        return prev_->is_end(); // 判断儿子节点是否结束
    }
    /**
     * @brief 返回执行器类型名称。
     * @return "ProjectionExecutor"。
     */
    std::string getType() override {
        return "ProjectionExecutor"; // 返回执行器的名称
    }
    /**
     * @brief 返回投影后的列元数据。
     * @return 输出列元数据引用。
     */
    const std::vector<ColMeta>& cols() const override {
        return cols_;
    }

    /**
     * @brief 返回当前投影记录的 NULL 标记。
     * @return 输出列对应的 NULL 标记引用。
     */
    const std::vector<bool>& nulls() const override {
        return nulls_;
    }
    /**
     * @brief 返回投影输出元组长度。
     * @return 所有输出字段长度之和。
     */
    size_t tupleLen() const override {
        return len_;
    }
    /**
     * @brief 查找投影输出列的元数据及其偏移。
     * @param target 目标表列。
     * @return 匹配的输出列元数据。
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
     * @brief 将计数开关透传给子执行器。
     * @param enabled 是否启用计数。
     */
    void set_counting_enabled(bool enabled) override {
        prev_->set_counting_enabled(enabled);
    }

    /**
     * @brief 将索引键条件透传给子执行器。
     * @param key_conds 可用于索引定位的条件。
     */
    void set_key_conditions(std::vector<Condition> key_conds) override {
        prev_->set_key_conditions(std::move(key_conds));
    }

    /**
     * @brief 返回底层扫描表名。
     * @return 子执行器报告的表名。
     */
    std::string scan_table_name() const override {
        return prev_->scan_table_name();
    }

    /**
     * @brief 返回底层扫描条件。
     * @return 子执行器报告的条件列表。
     */
    std::vector<Condition> scan_conditions() const override {
        return prev_->scan_conditions();
    }
};
