/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You may use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "statement/statement_runner.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <utility>

#include "access/load_data_service.h"
#include "access/table_write_service.h"
#include "execution/executor_abstract.h"
#include "execution/executor_aggregate.h"
#include "execution/executor_delete.h"
#include "execution/executor_filter.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_index_skip_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_limit.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_union.h"
#include "execution/executor_update.h"
#include "execution/execution_sort.h"
#include "optimizer/planner.h"
#include "recovery/checkpoint_manager.h"
#include "record/rm.h"
#include "record_printer.h"
#include "system/schema_manager.h"
#include "transaction/transaction_manager.h"

namespace rmdb::statement {

namespace {

const char* help_info = "Supported SQL syntax:\n"
                        "  command ;\n"
                        "command:\n"
                        "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                        "  DROP TABLE table_name\n"
                        "  CREATE INDEX table_name (column_name)\n"
                        "  DROP INDEX table_name (column_name)\n"
                        "  INSERT INTO table_name VALUES (value [, value ...])\n"
                        "  DELETE FROM table_name [WHERE where_clause]\n"
                        "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                        "  SELECT selector FROM table_name [WHERE where_clause]\n"
                        "type:\n"
                        "  {INT | FLOAT | CHAR(n)}\n"
                        "where_clause:\n"
                        "  condition [AND condition ...]\n"
                        "condition:\n"
                        "  column op {column | value}\n"
                        "column:\n"
                        "  [table_name.]column_name\n"
                        "op:\n"
                        "  {= | <> | < | > | <= | >=}\n"
                        "selector:\n"
                        "  {* | column [, column ...]}\n";

// 旧 Portal 内部用于构造 ProjectionExecutor / AggregateExecutor 的中间结构。
// 执行器以模板接收，仅需对应字段名，故保留为文件局部类型。
struct ExecutorQueryExpr {
    QueryExprType type = QueryExprType::COLUMN;
    TabCol col;
    AggExpr agg;
    Value val;
    Value value;
    std::string display_name;
};

struct ExecutorSelectItem {
    ExecutorQueryExpr expr;
    std::string alias;
    std::string display_name;
    std::string output_name;
};

struct ExecutorHavingCondition {
    ExecutorQueryExpr lhs;
    CompOp op = OP_EQ;
    bool is_rhs_val = false;
    bool is_rhs_value = false;
    ExecutorQueryExpr rhs_expr;
    Value rhs_val;
};

ExecutorQueryExpr to_executor_query_expr(const QueryExpr& expr) {
    ExecutorQueryExpr executor_expr;
    executor_expr.type = expr.type;
    executor_expr.col = expr.col;
    executor_expr.agg = expr.agg;
    executor_expr.val = expr.value;
    executor_expr.value = expr.value;
    executor_expr.display_name = expr.display_name;
    return executor_expr;
}

std::vector<ExecutorSelectItem> to_executor_select_items(const std::vector<SelectItem>& select_items) {
    std::vector<ExecutorSelectItem> executor_items;
    executor_items.reserve(select_items.size());
    for (const auto& item : select_items) {
        ExecutorSelectItem executor_item;
        executor_item.expr = to_executor_query_expr(item.expr);
        executor_item.alias = item.alias;
        executor_item.display_name =
            !item.output_name.empty() ? item.output_name : (!item.alias.empty() ? item.alias : item.expr.display_name);
        executor_item.output_name = item.output_name;
        executor_items.push_back(std::move(executor_item));
    }
    return executor_items;
}

std::vector<ExecutorHavingCondition> to_executor_having_conds(const std::vector<HavingCondition>& having_conds) {
    std::vector<ExecutorHavingCondition> executor_conds;
    executor_conds.reserve(having_conds.size());
    for (const auto& cond : having_conds) {
        ExecutorHavingCondition executor_cond;
        executor_cond.lhs = to_executor_query_expr(cond.lhs);
        executor_cond.op = cond.op;
        executor_cond.is_rhs_val = cond.is_rhs_val;
        executor_cond.is_rhs_value = cond.is_rhs_val;
        executor_cond.rhs_expr = to_executor_query_expr(cond.rhs_expr);
        executor_cond.rhs_val = cond.rhs_val;
        executor_conds.push_back(std::move(executor_cond));
    }
    return executor_conds;
}

bool same_tab_col(const TabCol& lhs, const TabCol& rhs) {
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}

bool same_query_expr(const QueryExpr& lhs, const QueryExpr& rhs) {
    if (lhs.type != rhs.type) {
        return false;
    }
    switch (lhs.type) {
    case QueryExprType::COLUMN:
        return same_tab_col(lhs.col, rhs.col);
    case QueryExprType::VALUE:
        return false;
    case QueryExprType::AGGREGATE:
        return lhs.agg.type == rhs.agg.type && lhs.agg.is_star == rhs.agg.is_star &&
               (lhs.agg.is_star || same_tab_col(lhs.agg.col, rhs.agg.col));
    }
    return false;
}

std::string get_select_item_output_name(const SelectItem& item) {
    if (!item.output_name.empty()) {
        return item.output_name;
    }
    if (!item.alias.empty()) {
        return item.alias;
    }
    if (!item.expr.display_name.empty()) {
        return item.expr.display_name;
    }
    if (item.expr.type == QueryExprType::AGGREGATE) {
        return item.expr.agg.display_name;
    }
    return item.expr.col.col_name;
}

std::vector<OrderByItem> bind_sort_output_names(const SortPlan& plan) {
    auto order_by_items = plan.order_by_items_;
    auto* projection = dynamic_cast<ProjectionPlan*>(plan.subplan_.get());
    if (projection == nullptr) {
        return order_by_items;
    }

    for (auto& item : order_by_items) {
        if (!item.order_name.empty()) {
            continue;
        }
        auto pos =
            std::find_if(projection->select_items_.begin(), projection->select_items_.end(),
                         [&](const SelectItem& select_item) { return same_query_expr(select_item.expr, item.expr); });
        if (pos != projection->select_items_.end()) {
            item.order_name = get_select_item_output_name(*pos);
        }
    }
    return order_by_items;
}

std::vector<std::string> build_projection_output_names(const ProjectionPlan& plan) {
    if (!plan.output_names_.empty()) {
        return plan.output_names_;
    }

    std::vector<std::string> output_names;
    output_names.reserve(plan.select_items_.size());
    for (const auto& item : plan.select_items_) {
        if (!item.output_name.empty()) {
            output_names.push_back(item.output_name);
        } else if (!item.alias.empty()) {
            output_names.push_back(item.alias);
        } else if (!item.expr.display_name.empty()) {
            output_names.push_back(item.expr.display_name);
        } else if (item.expr.type == QueryExprType::AGGREGATE) {
            output_names.push_back(item.expr.agg.display_name);
        } else {
            output_names.push_back(item.expr.col.col_name);
        }
    }
    return output_names;
}

std::string display_table(const Plan& plan, const std::string& table_name) {
    auto pos = plan.table_name_to_display_.find(table_name);
    if (pos == plan.table_name_to_display_.end()) {
        return table_name;
    }
    return pos->second;
}

std::string display_col(const Plan& plan, const TabCol& col) {
    return display_table(plan, col.tab_name) + "." + col.col_name;
}

std::string comp_op_to_string(CompOp op) {
    switch (op) {
    case OP_EQ:
        return "=";
    case OP_NE:
        return "<>";
    case OP_LT:
        return "<";
    case OP_GT:
        return ">";
    case OP_LE:
        return "<=";
    case OP_GE:
        return ">=";
    }
    throw InternalError("Unexpected comparison operator");
}

std::string value_to_string(const Value& val) {
    switch (val.type) {
    case TYPE_INT:
        return std::to_string(val.int_val);
    case TYPE_FLOAT: {
        std::ostringstream out;
        out << std::fixed << std::setprecision(6) << val.float_val;
        auto str = out.str();
        while (str.size() > 2 && str.back() == '0' && str[str.size() - 2] != '.') {
            str.pop_back();
        }
        return str;
    }
    case TYPE_STRING:
    case TYPE_DATETIME:
        return "'" + val.str_val + "'";
    }
    throw InternalError("Unexpected value type");
}

std::string condition_to_string(const Plan& plan, const Condition& cond) {
    std::string result = display_col(plan, cond.lhs_col) + comp_op_to_string(cond.op);
    if (cond.is_rhs_val) {
        result += cond.rhs_display.empty() ? value_to_string(cond.rhs_val) : cond.rhs_display;
    } else {
        result += display_col(plan, cond.rhs_col);
    }
    return result;
}

std::string join_strings(std::vector<std::string> values) {
    std::sort(values.begin(), values.end());
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i != 0) {
            out << ", ";
        }
        out << values[i];
    }
    return out.str();
}

std::vector<std::string> condition_strings(const Plan& plan, const std::vector<Condition>& conds) {
    std::vector<std::string> values;
    values.reserve(conds.size());
    for (const auto& cond : conds) {
        values.push_back(condition_to_string(plan, cond));
    }
    std::sort(values.begin(), values.end());
    return values;
}

void collect_tables(Plan* plan, std::set<std::string>& tables) {
    switch (plan->tag) {
    case T_SeqScan:
    case T_IndexSkipScan:
    case T_IndexScan:
        tables.insert(static_cast<ScanPlan*>(plan)->tab_name_);
        break;
    case T_Filter:
        collect_tables(static_cast<FilterPlan*>(plan)->subplan_.get(), tables);
        break;
    case T_Projection:
        collect_tables(static_cast<ProjectionPlan*>(plan)->subplan_.get(), tables);
        break;
    case T_NestLoop:
    case T_SortMerge: {
        auto join = static_cast<JoinPlan*>(plan);
        collect_tables(join->left_.get(), tables);
        collect_tables(join->right_.get(), tables);
        break;
    }
    default:
        break;
    }
}

void reset_runtime_rows(Plan* plan) {
    if (plan == nullptr) {
        return;
    }
    plan->runtime_rows_ = 0;
    switch (plan->tag) {
    case T_Filter:
        reset_runtime_rows(static_cast<FilterPlan*>(plan)->subplan_.get());
        break;
    case T_Projection:
        reset_runtime_rows(static_cast<ProjectionPlan*>(plan)->subplan_.get());
        break;
    case T_NestLoop:
    case T_SortMerge: {
        auto join = static_cast<JoinPlan*>(plan);
        reset_runtime_rows(join->left_.get());
        reset_runtime_rows(join->right_.get());
        break;
    }
    default:
        break;
    }
}

std::vector<std::string> projection_columns(const ProjectionPlan& plan) {
    if (plan.is_select_star_) {
        return {"*"};
    }
    std::vector<std::string> cols;
    cols.reserve(plan.select_items_.size());
    for (const auto& item : plan.select_items_) {
        if (item.expr.type == QueryExprType::COLUMN) {
            cols.push_back(display_col(plan, item.expr.col));
        }
    }
    std::sort(cols.begin(), cols.end());
    return cols;
}

void append_to_sink(const std::string& text, OutputSink* sink) {
    if (sink == nullptr || sink->data_send == nullptr || sink->offset == nullptr) {
        return;
    }
    memcpy(sink->data_send + *sink->offset, text.c_str(), text.size());
    *sink->offset += static_cast<int>(text.size());
}

// 计数包装器：EXPLAIN ANALYZE 需要统计每层算子返回行数。
class CountingExecutor : public rmdb::exec::AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> inner_;
    Plan* plan_;
    bool counting_enabled_ = true;

public:
    CountingExecutor(std::unique_ptr<AbstractExecutor> inner, Plan* plan) {
        inner_ = std::move(inner);
        plan_ = plan;
        context_ = inner_->context_;
    }

    size_t tupleLen() const override {
        return inner_->tupleLen();
    }

    const std::vector<ColMeta>& cols() const override {
        return inner_->cols();
    }

    std::string getType() override {
        return inner_->getType();
    }

    void beginTuple() override {
        inner_->beginTuple();
    }

    void nextTuple() override {
        inner_->nextTuple();
    }

    bool is_end() const override {
        return inner_->is_end();
    }

    Rid& rid() override {
        return inner_->rid();
    }

    std::unique_ptr<RmRecord> Next() override {
        auto rec = inner_->Next();
        if (rec != nullptr && counting_enabled_) {
            ++plan_->runtime_rows_;
        }
        return rec;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return inner_->get_col_offset(target);
    }

    void set_counting_enabled(bool enabled) override {
        counting_enabled_ = enabled;
        inner_->set_counting_enabled(enabled);
    }

    void set_key_conditions(std::vector<Condition> key_conds) override {
        inner_->set_key_conditions(std::move(key_conds));
    }

    std::string scan_table_name() const override {
        return inner_->scan_table_name();
    }

    std::vector<Condition> scan_conditions() const override {
        return inner_->scan_conditions();
    }

    void record_current_read_for_ssi() override {
        inner_->record_current_read_for_ssi();
    }
};

std::unique_ptr<AbstractExecutor> maybe_count(std::unique_ptr<AbstractExecutor> executor, Plan* plan, bool count_rows) {
    if (!count_rows) {
        return executor;
    }
    return std::make_unique<CountingExecutor>(std::move(executor), plan);
}

} // namespace

std::vector<std::string> StatementRunner::get_plan_output_names(Plan* plan) {
    switch (plan->tag) {
    case T_Projection:
        return build_projection_output_names(*static_cast<ProjectionPlan*>(plan));
    case T_Sort:
        return get_plan_output_names(static_cast<SortPlan*>(plan)->subplan_.get());
    case T_Limit:
        return get_plan_output_names(static_cast<LimitPlan*>(plan)->subplan_.get());
    case T_Aggregate:
        return build_aggregate_output_names(*static_cast<AggregatePlan*>(plan));
    case T_Union: {
        auto union_plan = static_cast<UnionPlan*>(plan);
        if (!union_plan->output_names_.empty()) {
            return union_plan->output_names_;
        }
        std::vector<std::string> output_names;
        output_names.reserve(union_plan->cols_.size());
        for (const auto& col : union_plan->cols_) {
            output_names.push_back(col.name);
        }
        return output_names;
    }
    case T_SeqScan:
    case T_IndexSkipScan:
    case T_IndexScan: {
        std::vector<std::string> output_names;
        const auto& cols = static_cast<ScanPlan*>(plan)->cols_;
        output_names.reserve(cols.size());
        for (const auto& col : cols) {
            output_names.push_back(col.name);
        }
        return output_names;
    }
    case T_NestLoop:
    case T_SortMerge: {
        auto join_plan = static_cast<JoinPlan*>(plan);
        auto output_names = get_plan_output_names(join_plan->left_.get());
        auto right_output_names = get_plan_output_names(join_plan->right_.get());
        output_names.insert(output_names.end(), right_output_names.begin(), right_output_names.end());
        return output_names;
    }
    default:
        return {};
    }
}

std::vector<std::string> StatementRunner::build_aggregate_output_names(const rmdb::optimizer::AggregatePlan& plan) {
    std::vector<std::string> output_names;
    output_names.reserve(plan.group_by_cols_.size() + plan.agg_exprs_.size());
    for (const auto& group_col : plan.group_by_cols_) {
        output_names.push_back(group_col.col_name);
    }
    for (const auto& agg_expr : plan.agg_exprs_) {
        output_names.push_back(agg_expr.display_name);
    }
    return output_names;
}

void StatementRunner::render_explain_plan(Plan* plan, int depth, std::ostringstream& out) {
    out << std::string(static_cast<size_t>(depth), '\t');
    switch (plan->tag) {
    case T_SeqScan: {
        auto scan = static_cast<ScanPlan*>(plan);
        out << "Scan(table=" << scan->tab_name_ << ", type=SeqScan, rows=" << plan->runtime_rows_ << ")\n";
        break;
    }
    case T_IndexScan: {
        auto scan = static_cast<ScanPlan*>(plan);
        out << "Scan(table=" << scan->tab_name_ << ", type=IndexScan, using_index=(" << scan->index_col_names_[0]
            << "), rows=" << plan->runtime_rows_ << ")\n";
        break;
    }
    case T_IndexSkipScan: {
        auto scan = static_cast<ScanPlan*>(plan);
        out << "Scan(table=" << scan->tab_name_ << ", type=IndexSkipScan, using_index=(" << scan->index_col_names_[0]
            << "), rows=" << plan->runtime_rows_ << ")\n";
        break;
    }
    case T_Filter: {
        auto filter = static_cast<FilterPlan*>(plan);
        out << "Filter(condition=[" << join_strings(condition_strings(*plan, filter->conds_))
            << "], rows=" << plan->runtime_rows_ << ")\n";
        render_explain_plan(filter->subplan_.get(), depth + 1, out);
        break;
    }
    case T_Projection: {
        auto projection = static_cast<ProjectionPlan*>(plan);
        out << "Project(columns=[" << join_strings(projection_columns(*projection)) << "], rows=" << plan->runtime_rows_
            << ")\n";
        render_explain_plan(projection->subplan_.get(), depth + 1, out);
        break;
    }
    case T_NestLoop:
    case T_SortMerge: {
        auto join = static_cast<JoinPlan*>(plan);
        std::set<std::string> table_set;
        collect_tables(plan, table_set);
        std::vector<std::string> tables(table_set.begin(), table_set.end());
        out << "Join(tables=[" << join_strings(std::move(tables)) << "], condition=["
            << join_strings(condition_strings(*plan, join->conds_)) << "], rows=" << plan->runtime_rows_ << ")\n";
        render_explain_plan(join->left_.get(), depth + 1, out);
        render_explain_plan(join->right_.get(), depth + 1, out);
        break;
    }
    default:
        break;
    }
}

std::unique_ptr<AbstractExecutor> StatementRunner::build_executor_tree(Plan* plan, StatementContext* stmt_ctx,
                                                                       bool count_rows) {
    return convert_plan_executor(plan, stmt_ctx, count_rows);
}

std::unique_ptr<AbstractExecutor> StatementRunner::convert_plan_executor(Plan* plan, StatementContext* context,
                                                                         bool count_rows) {
    using rmdb::exec::AbstractExecutor;
    switch (plan->tag) {
    case T_Projection: {
        auto x = static_cast<ProjectionPlan*>(plan);
        std::unique_ptr<AbstractExecutor> subplan = convert_plan_executor(x->subplan_.get(), context, count_rows);
        std::unique_ptr<AbstractExecutor> executor;
        if (x->preserve_col_names_) {
            std::vector<TabCol> cols;
            cols.reserve(x->select_items_.size());
            for (const auto& item : x->select_items_) {
                cols.push_back(item.expr.col);
            }
            executor = std::make_unique<ProjectionExecutor>(std::move(subplan), cols);
        } else {
            auto select_items = to_executor_select_items(x->select_items_);
            executor = std::make_unique<ProjectionExecutor>(std::move(subplan), select_items);
        }
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_Filter: {
        auto x = static_cast<FilterPlan*>(plan);
        std::unique_ptr<AbstractExecutor> executor =
            std::make_unique<FilterExecutor>(convert_plan_executor(x->subplan_.get(), context, count_rows), x->conds_);
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_Aggregate: {
        auto x = static_cast<AggregatePlan*>(plan);
        auto having_conds = to_executor_having_conds(x->having_conds_);
        std::unique_ptr<AbstractExecutor> executor =
            std::make_unique<AggregateExecutor>(convert_plan_executor(x->subplan_.get(), context, count_rows),
                                                x->group_by_cols_, x->agg_exprs_, having_conds, context);
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_SeqScan:
    case T_IndexSkipScan:
    case T_IndexScan: {
        auto x = static_cast<ScanPlan*>(plan);
        std::unique_ptr<AbstractExecutor> executor;
        if (x->tag == T_SeqScan) {
            executor = std::make_unique<SeqScanExecutor>(schema_manager_, x->tab_name_, x->conds_, context);
        } else if (x->tag == T_IndexSkipScan) {
            executor = std::make_unique<IndexSkipScanExecutor>(schema_manager_, x->tab_name_, x->conds_,
                                                               x->index_col_names_, context);
        } else {
            executor = std::make_unique<IndexScanExecutor>(schema_manager_, x->tab_name_, x->conds_,
                                                           x->index_col_names_, context);
        }
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_NestLoop:
    case T_SortMerge: {
        auto x = static_cast<JoinPlan*>(plan);
        std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_.get(), context, count_rows);
        std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_.get(), context, count_rows);
        std::unique_ptr<AbstractExecutor> join =
            std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), x->conds_, x->inlj_left_col_,
                                                     x->inlj_right_col_, x->inlj_index_col_name_);
        return maybe_count(std::move(join), plan, count_rows);
    }
    case T_Sort: {
        auto x = static_cast<SortPlan*>(plan);
        std::unique_ptr<AbstractExecutor> executor = std::make_unique<SortExecutor>(
            convert_plan_executor(x->subplan_.get(), context, count_rows), bind_sort_output_names(*x), x->limit_);
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_Limit: {
        auto x = static_cast<LimitPlan*>(plan);
        std::unique_ptr<AbstractExecutor> executor = std::make_unique<LimitExecutor>(
            convert_plan_executor(x->subplan_.get(), context, count_rows), static_cast<size_t>(x->limit_));
        return maybe_count(std::move(executor), plan, count_rows);
    }
    case T_Union: {
        auto x = static_cast<UnionPlan*>(plan);
        std::vector<std::unique_ptr<AbstractExecutor>> branches;
        branches.reserve(x->branches_.size());
        for (const auto& branch_plan : x->branches_) {
            branches.push_back(convert_plan_executor(branch_plan.get(), context, count_rows));
        }
        return maybe_count(std::make_unique<UnionExecutor>(std::move(branches), x->cols_), plan, count_rows);
    }
    default:
        break;
    }
    return nullptr;
}

// SELECT 输出格式化：先写入语句本地缓冲，完成后再拷回客户端缓冲与 output.txt。
// 该不变量与旧 QlManager::select_from 完全一致，必须逐行保留。
static void run_select(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<std::string> output_names,
                       StatementContext* stmt_ctx, OutputSink* sink, SchemaManager* schema_manager) {
    const auto& result_cols = executorTreeRoot->cols();
    std::vector<std::string> captions = std::move(output_names);
    if (captions.size() != result_cols.size()) {
        captions.clear();
        captions.reserve(result_cols.size());
        for (const auto& col : result_cols) {
            captions.push_back(col.name);
        }
    }

    // Print records
    size_t num_rec = 0;
    std::vector<char> local_send(BUFFER_LENGTH, 0);
    int local_offset = 0;
    // 语句本地输出缓冲：SELECT 完成后才拷回客户端缓冲与 output.txt
    OutputSink print_sink{local_send.data(), &local_offset, false};

    // SSI 读跟踪需作用在与执行器树相同的 StatementContext 上
    struct SsiReadTrackingGuard {
        StatementContext* stmt_ctx_;
        bool old_value_;

        explicit SsiReadTrackingGuard(StatementContext* stmt_ctx) : stmt_ctx_(stmt_ctx), old_value_(false) {
            if (stmt_ctx_ != nullptr) {
                old_value_ = stmt_ctx_->enable_ssi_read_tracking;
                stmt_ctx_->enable_ssi_read_tracking = true;
            }
        }

        ~SsiReadTrackingGuard() {
            if (stmt_ctx_ != nullptr) {
                stmt_ctx_->enable_ssi_read_tracking = old_value_;
            }
        }
    } ssi_read_tracking_guard(stmt_ctx);

    // Print header into a statement-local buffer. It is copied to the client
    // and output.txt only after the SELECT completes without aborting.
    RecordPrinter rec_printer(captions.size());
    rec_printer.print_separator(&print_sink);
    rec_printer.print_record(captions, &print_sink);
    rec_printer.print_separator(&print_sink);

    std::ostringstream out_file_stream;
    out_file_stream << "|";
    for (const auto& cap : captions) {
        out_file_stream << " " << cap << " |";
    }
    out_file_stream << "\n";

    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        columns.reserve(result_cols.size());
        for (auto& col : result_cols) {
            std::string col_str;
            char* rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int*)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float*)rec_buf);
            } else if (col.type == TYPE_STRING || col.type == TYPE_DATETIME) {
                col_str = std::string((char*)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
        }
        // print record into client buffer
        rec_printer.print_record(columns, &print_sink);
        // print record into output.txt (compact borderless)
        out_file_stream << "|";
        for (const auto& col_str : columns) {
            out_file_stream << " " << col_str << " |";
        }
        out_file_stream << "\n";
        num_rec++;
    }
    // Print footer into client buffer
    rec_printer.print_separator(&print_sink);
    // Print record count into client buffer
    RecordPrinter::print_record_count(num_rec, &print_sink);

    if (local_offset > 0) {
        memcpy(sink->data_send + *sink->offset, local_send.data(), local_offset);
        *sink->offset += local_offset;

        if (schema_manager->output_file_enabled()) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << out_file_stream.str();
            outfile.close();
        }
    }
}

void StatementRunner::run(std::unique_ptr<Plan> plan, StatementContext* stmt_ctx, OutputSink* sink, txn_id_t* txn_id) {
    switch (plan->tag) {
    case T_Help:
    case T_ShowTable:
    case T_ShowIndex:
    case T_DescTable:
    case T_Transaction_begin:
    case T_Transaction_commit:
    case T_Transaction_abort:
    case T_Transaction_rollback: {
        auto* x = static_cast<OtherPlan*>(plan.get());
        switch (plan->tag) {
        case T_Help: {
            memcpy(sink->data_send + *sink->offset, help_info, strlen(help_info));
            *sink->offset = strlen(help_info);
            break;
        }
        case T_ShowTable: {
            schema_manager_->show_tables(sink);
            break;
        }
        case T_ShowIndex: {
            schema_manager_->show_index(x->tab_name_, sink);
            break;
        }
        case T_DescTable: {
            schema_manager_->desc_table(x->tab_name_, sink);
            break;
        }
        case T_Transaction_begin: {
            // 显示开启一个事务
            if (stmt_ctx->txn == nullptr) {
                stmt_ctx->txn = txn_mgr_->begin(nullptr, stmt_ctx->log_mgr, stmt_ctx->isolation_level);
                *txn_id = stmt_ctx->txn->get_transaction_id();
            }
            stmt_ctx->txn->set_txn_mode(true);
            // Propagate isolation level from StatementContext to Transaction
            stmt_ctx->txn->set_isolation_level(stmt_ctx->isolation_level);
            break;
        }
        case T_Transaction_commit: {
            stmt_ctx->txn = txn_mgr_->get_transaction(*txn_id);
            if (stmt_ctx->txn != nullptr) {
                txn_mgr_->commit(stmt_ctx->txn, stmt_ctx->log_mgr);
            }
            stmt_ctx->txn = nullptr;
            break;
        }
        case T_Transaction_rollback: {
            stmt_ctx->txn = txn_mgr_->get_transaction(*txn_id);
            if (stmt_ctx->txn != nullptr) {
                txn_mgr_->abort(stmt_ctx->txn, stmt_ctx->log_mgr);
            }
            stmt_ctx->txn = nullptr;
            break;
        }
        case T_Transaction_abort: {
            stmt_ctx->txn = txn_mgr_->get_transaction(*txn_id);
            if (stmt_ctx->txn != nullptr) {
                txn_mgr_->abort(stmt_ctx->txn, stmt_ctx->log_mgr);
            }
            stmt_ctx->txn = nullptr;
            break;
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }
        break;
    }
    case T_SetTransaction: {
        auto* x = static_cast<SetTransactionPlan*>(plan.get());
        switch (x->isolation_level_) {
        case rmdb::parser::ast::IsolationLevelType::SNAPSHOT_ISOLATION: {
            stmt_ctx->isolation_level = IsolationLevel::SNAPSHOT_ISOLATION;
            break;
        }
        case rmdb::parser::ast::IsolationLevelType::SERIALIZABLE: {
            stmt_ctx->isolation_level = IsolationLevel::SERIALIZABLE;
            break;
        }
        }
        break;
    }
    case T_SetKnob: {
        auto* x = static_cast<SetKnobPlan*>(plan.get());
        switch (x->set_knob_type_) {
        case rmdb::parser::ast::SetKnobType::EnableNestLoop: {
            planner_->set_enable_nestedloop_join(x->bool_value_);
            break;
        }
        case rmdb::parser::ast::SetKnobType::EnableSortMerge: {
            planner_->set_enable_sortmerge_join(x->bool_value_);
            break;
        }
        default: {
            throw RMDBError("Not implemented!\n");
            break;
        }
        }
        break;
    }
    case T_SetOutputFile: {
        auto* x = static_cast<SetOutputFilePlan*>(plan.get());
        // output_file is a database-global toggle shared across all connections;
        // store it on SmManager so it persists across connection lifetimes.
        schema_manager_->set_output_file(x->enable_);
        break;
    }
    case T_LoadData: {
        auto* x = static_cast<LoadDataPlan*>(plan.get());
        load_data_service_->load_csv(x->file_name_, x->tab_name_, stmt_ctx);
        break;
    }
    case T_StaticCheckpoint: {
        if (txn_id != nullptr) {
            Transaction* current_txn = txn_mgr_->get_transaction(*txn_id);
            if (current_txn != nullptr && current_txn->get_state() != TransactionState::COMMITTED &&
                current_txn->get_state() != TransactionState::ABORTED) {
                throw RMDBError("static checkpoint cannot run inside an active transaction");
            }
        }
        CheckpointManager checkpoint_mgr(txn_mgr_, schema_manager_, stmt_ctx == nullptr ? nullptr : stmt_ctx->log_mgr);
        checkpoint_mgr.RunCleanCheckpoint();
        break;
    }
    case T_CreateTable: {
        auto* x = static_cast<DDLPlan*>(plan.get());
        schema_manager_->create_table(x->tab_name_, x->cols_, stmt_ctx);
        break;
    }
    case T_DropTable: {
        auto* x = static_cast<DDLPlan*>(plan.get());
        schema_manager_->drop_table(x->tab_name_, stmt_ctx);
        break;
    }
    case T_CreateIndex: {
        auto* x = static_cast<DDLPlan*>(plan.get());
        schema_manager_->create_index(x->tab_name_, x->tab_col_names_, stmt_ctx);
        break;
    }
    case T_DropIndex: {
        auto* x = static_cast<DDLPlan*>(plan.get());
        schema_manager_->drop_index(x->tab_name_, x->tab_col_names_, stmt_ctx);
        break;
    }
    case T_select: {
        auto* x = static_cast<DMLPlan*>(plan.get());
        std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), stmt_ctx);
        std::vector<std::string> output_names = get_plan_output_names(x->subplan_.get());
        run_select(std::move(root), std::move(output_names), stmt_ctx, sink, schema_manager_);
        break;
    }
    case T_ExplainAnalyze: {
        auto* x = static_cast<DMLPlan*>(plan.get());
        reset_runtime_rows(x->subplan_.get());
        std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), stmt_ctx, true);
        for (root->beginTuple(); !root->is_end(); root->nextTuple()) {
            (void)root->Next();
        }
        std::ostringstream out;
        render_explain_plan(x->subplan_.get(), 0, out);
        append_to_sink(out.str(), sink);
        if (schema_manager_->output_file_enabled()) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << out.str();
            outfile.close();
        }
        break;
    }
    case T_Update: {
        auto* x = static_cast<DMLPlan*>(plan.get());
        std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_.get(), stmt_ctx);
        std::vector<Rid> rids;
        for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
            rids.push_back(scan->rid());
        }
        std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
            schema_manager_, write_service_, x->tab_name_, x->set_clauses_, x->conds_, rids, stmt_ctx);
        root->Next();
        break;
    }
    case T_Delete: {
        auto* x = static_cast<DMLPlan*>(plan.get());
        std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_.get(), stmt_ctx);
        std::vector<Rid> rids;
        for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
            rids.push_back(scan->rid());
        }
        std::unique_ptr<AbstractExecutor> root =
            std::make_unique<DeleteExecutor>(schema_manager_, write_service_, x->tab_name_, x->conds_, rids, stmt_ctx);
        root->Next();
        break;
    }
    case T_Insert: {
        auto* x = static_cast<DMLPlan*>(plan.get());
        std::unique_ptr<AbstractExecutor> root =
            std::make_unique<InsertExecutor>(schema_manager_, write_service_, x->tab_name_, x->values_, stmt_ctx);
        root->Next();
        break;
    }
    default:
        throw InternalError("Unexpected field type");
        break;
    }
}

} // namespace rmdb::statement
