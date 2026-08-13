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

#ifdef private
#pragma push_macro("private")
#undef private
#define RMDB_PORTAL_RESTORE_PRIVATE
#endif

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#ifdef RMDB_PORTAL_RESTORE_PRIVATE
#pragma pop_macro("private")
#undef RMDB_PORTAL_RESTORE_PRIVATE
#endif
#include "execution/executor_abstract.h"
#include "execution/executor_aggregate.h"
#include "execution/executor_delete.h"
#include "execution/executor_distinct.h"
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
#include "common/common.h"
#include "optimizer/plan.h"

typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_EXPLAIN_ANALYZE,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;

struct PortalStmt {
    portalTag tag;

    std::vector<std::string> output_names;
    std::unique_ptr<AbstractExecutor> root;
    std::unique_ptr<Plan> plan;

    PortalStmt(portalTag tag_, std::vector<std::string> output_names_, std::unique_ptr<AbstractExecutor> root_,
               std::unique_ptr<Plan> plan_)
        : tag(tag_), output_names(std::move(output_names_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

class Portal {
private:
    SmManager* sm_manager_;

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
        Value rhs_upper;
        std::vector<Value> rhs_vals;
        bool has_rhs_upper = false;
        bool negated = false;
    };

    class CountingExecutor : public AbstractExecutor {
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

        const std::vector<bool>& nulls() const override {
            return inner_->nulls();
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

    static ExecutorQueryExpr to_executor_query_expr(const QueryExpr& expr) {
        ExecutorQueryExpr executor_expr;
        executor_expr.type = expr.type;
        executor_expr.col = expr.col;
        executor_expr.agg = expr.agg;
        executor_expr.val = expr.value;
        executor_expr.value = expr.value;
        executor_expr.display_name = expr.display_name;
        return executor_expr;
    }

    static std::vector<ExecutorSelectItem> to_executor_select_items(const std::vector<SelectItem>& select_items) {
        std::vector<ExecutorSelectItem> executor_items;
        executor_items.reserve(select_items.size());
        for (const auto& item : select_items) {
            ExecutorSelectItem executor_item;
            executor_item.expr = to_executor_query_expr(item.expr);
            executor_item.alias = item.alias;
            executor_item.display_name = !item.output_name.empty()
                                             ? item.output_name
                                             : (!item.alias.empty() ? item.alias : item.expr.display_name);
            executor_item.output_name = item.output_name;
            executor_items.push_back(std::move(executor_item));
        }
        return executor_items;
    }

    static std::vector<ExecutorHavingCondition>
    to_executor_having_conds(const std::vector<HavingCondition>& having_conds) {
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
            executor_cond.rhs_upper = cond.rhs_upper;
            executor_cond.rhs_vals = cond.rhs_vals;
            executor_cond.has_rhs_upper = cond.has_rhs_upper;
            executor_cond.negated = cond.negated;
            executor_conds.push_back(std::move(executor_cond));
        }
        return executor_conds;
    }

    static bool same_tab_col(const TabCol& lhs, const TabCol& rhs) {
        return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
    }

    static bool same_query_expr(const QueryExpr& lhs, const QueryExpr& rhs) {
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

    static std::string get_select_item_output_name(const SelectItem& item) {
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

    static std::vector<OrderByItem> bind_sort_output_names(const SortPlan& plan) {
        auto order_by_items = plan.order_by_items_;
        auto* projection = dynamic_cast<ProjectionPlan*>(plan.subplan_.get());
        if (projection == nullptr) {
            return order_by_items;
        }

        for (auto& item : order_by_items) {
            if (!item.order_name.empty()) {
                continue;
            }
            auto pos = std::find_if(
                projection->select_items_.begin(), projection->select_items_.end(),
                [&](const SelectItem& select_item) { return same_query_expr(select_item.expr, item.expr); });
            if (pos != projection->select_items_.end()) {
                item.order_name = get_select_item_output_name(*pos);
            }
        }
        return order_by_items;
    }

    static std::vector<std::string> build_projection_output_names(const ProjectionPlan& plan) {
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

    static std::vector<std::string> build_aggregate_output_names(const AggregatePlan& plan) {
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

    std::vector<std::string> get_plan_output_names(Plan* plan) const {
        switch (plan->tag) {
        case T_Projection:
            return build_projection_output_names(*static_cast<ProjectionPlan*>(plan));
        case T_Distinct:
            return get_plan_output_names(static_cast<DistinctPlan*>(plan)->subplan_.get());
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

    static std::string display_table(const Plan& plan, const std::string& table_name) {
        auto pos = plan.table_name_to_display_.find(table_name);
        if (pos == plan.table_name_to_display_.end()) {
            return table_name;
        }
        return pos->second;
    }

    static std::string display_col(const Plan& plan, const TabCol& col) {
        return display_table(plan, col.tab_name) + "." + col.col_name;
    }

    static std::string comp_op_to_string(CompOp op) {
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
        case OP_LIKE:
            return " LIKE ";
        case OP_IN:
            return " IN ";
        case OP_BETWEEN:
            return " BETWEEN ";
        }
        throw InternalError("Unexpected comparison operator");
    }

    static std::string value_to_string(const Value& val) {
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

    static std::string condition_to_string(const Plan& plan, const Condition& cond) {
        std::string op = comp_op_to_string(cond.op);
        if (cond.negated && (cond.op == OP_LIKE || cond.op == OP_IN || cond.op == OP_BETWEEN)) {
            op.insert(0, " NOT");
        }
        std::string result = display_col(plan, cond.lhs_col) + op;
        if (cond.is_rhs_val) {
            if (cond.op == OP_IN) {
                result += "(";
                for (size_t i = 0; i < cond.rhs_vals.size(); ++i) {
                    if (i != 0) {
                        result += ", ";
                    }
                    result += value_to_string(cond.rhs_vals[i]);
                }
                result += ")";
            } else {
                result += cond.rhs_display.empty() ? value_to_string(cond.rhs_val) : cond.rhs_display;
                if (cond.op == OP_BETWEEN && cond.has_rhs_upper) {
                    result += " AND " + value_to_string(cond.rhs_upper);
                }
            }
        } else {
            result += display_col(plan, cond.rhs_col);
        }
        return result;
    }

    static std::string join_strings(std::vector<std::string> values) {
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

    static std::vector<std::string> condition_strings(const Plan& plan, const std::vector<Condition>& conds) {
        std::vector<std::string> values;
        values.reserve(conds.size());
        for (const auto& cond : conds) {
            values.push_back(condition_to_string(plan, cond));
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    static void collect_tables(Plan* plan, std::set<std::string>& tables) {
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
        case T_Distinct:
            collect_tables(static_cast<DistinctPlan*>(plan)->subplan_.get(), tables);
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

    static void reset_runtime_rows(Plan* plan) {
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
        case T_Distinct:
            reset_runtime_rows(static_cast<DistinctPlan*>(plan)->subplan_.get());
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

    static std::vector<std::string> projection_columns(const ProjectionPlan& plan) {
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

    static void render_explain_plan(Plan* plan, int depth, std::ostringstream& out) {
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
            out << "Scan(table=" << scan->tab_name_ << ", type=IndexSkipScan, using_index=("
                << scan->index_col_names_[0] << "), rows=" << plan->runtime_rows_ << ")\n";
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
            out << "Project(columns=[" << join_strings(projection_columns(*projection))
                << "], rows=" << plan->runtime_rows_ << ")\n";
            render_explain_plan(projection->subplan_.get(), depth + 1, out);
            break;
        }
        case T_Distinct: {
            auto distinct = static_cast<DistinctPlan*>(plan);
            out << "Distinct(rows=" << plan->runtime_rows_ << ")\n";
            render_explain_plan(distinct->subplan_.get(), depth + 1, out);
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

    static void append_to_context(const std::string& text, Context* context) {
        if (context == nullptr || context->data_send_ == nullptr || context->offset_ == nullptr) {
            return;
        }
        memcpy(context->data_send_ + *(context->offset_), text.c_str(), text.size());
        *(context->offset_) += static_cast<int>(text.size());
    }

    void write_explain_output(const std::string& text, Context* context) {
        append_to_context(text, context);
        if (sm_manager_->output_file_enabled_) {
            std::fstream outfile;
            outfile.open("output.txt", std::ios::out | std::ios::app);
            outfile << text;
            outfile.close();
        }
    }

    static std::unique_ptr<AbstractExecutor> maybe_count(std::unique_ptr<AbstractExecutor> executor, Plan* plan,
                                                         bool count_rows) {
        if (!count_rows) {
            return executor;
        }
        return std::make_unique<CountingExecutor>(std::move(executor), plan);
    }

public:
    Portal(SmManager* sm_manager) : sm_manager_(sm_manager) {}
    ~Portal() {}

    // 将查询执行计划转换成对应的算子树
    std::unique_ptr<PortalStmt> start(std::unique_ptr<Plan> plan, Context* context) {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        switch (plan->tag) {
        case T_Help:
        case T_ShowTable:
        case T_ShowIndex:
        case T_DescTable:
        case T_Transaction_begin:
        case T_Transaction_commit:
        case T_Transaction_abort:
        case T_Transaction_rollback:
        case T_StaticCheckpoint:
            return std::make_unique<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_SetKnob:
        case T_SetTransaction:
        case T_SetOutputFile:
        case T_LoadData:
            return std::make_unique<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_CreateTable:
        case T_DropTable:
        case T_CreateIndex:
        case T_DropIndex:
            return std::make_unique<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<std::string>(),
                                                std::unique_ptr<AbstractExecutor>(), std::move(plan));
        case T_select:
        case T_ExplainAnalyze:
        case T_Update:
        case T_Delete:
        case T_Insert: {
            auto* x = static_cast<DMLPlan*>(plan.get());
            switch (x->tag) {
            case T_select: {
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), context);
                std::vector<std::string> output_names = get_plan_output_names(x->subplan_.get());
                return std::make_unique<PortalStmt>(PORTAL_ONE_SELECT, std::move(output_names), std::move(root),
                                                    std::move(plan));
            }
            case T_ExplainAnalyze: {
                reset_runtime_rows(x->subplan_.get());
                std::unique_ptr<AbstractExecutor> root = convert_plan_executor(x->subplan_.get(), context, true);
                return std::make_unique<PortalStmt>(PORTAL_EXPLAIN_ANALYZE, std::vector<std::string>(), std::move(root),
                                                    std::move(plan));
            }

            case T_Update: {
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_.get(), context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }
                std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(
                    sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }
            case T_Delete: {
                std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_.get(), context);
                std::vector<Rid> rids;
                for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    rids.push_back(scan->rid());
                }

                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }

            case T_Insert: {
                std::unique_ptr<AbstractExecutor> root =
                    std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

                return std::make_unique<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<std::string>(),
                                                    std::move(root), std::move(plan));
            }

            default:
                throw InternalError("Unexpected field type");
                break;
            }
        }
        default:
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::unique_ptr<PortalStmt> portal, QlManager* ql, txn_id_t* txn_id, Context* context) {
        switch (portal->tag) {
        case PORTAL_ONE_SELECT: {
            ql->select_from(std::move(portal->root), std::move(portal->output_names), context);
            break;
        }

        case PORTAL_EXPLAIN_ANALYZE: {
            for (portal->root->beginTuple(); !portal->root->is_end(); portal->root->nextTuple()) {
                (void)portal->root->Next();
            }
            auto* dml = static_cast<DMLPlan*>(portal->plan.get());
            std::ostringstream out;
            render_explain_plan(dml->subplan_.get(), 0, out);
            write_explain_output(out.str(), context);
            break;
        }

        case PORTAL_DML_WITHOUT_SELECT: {
            ql->run_dml(std::move(portal->root));
            break;
        }
        case PORTAL_MULTI_QUERY: {
            ql->run_mutli_query(portal->plan.get(), context);
            break;
        }
        case PORTAL_CMD_UTILITY: {
            ql->run_cmd_utility(portal->plan.get(), txn_id, context);
            break;
        }
        default: {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(Plan* plan, Context* context, bool count_rows = false) {
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
        case T_Distinct: {
            auto x = static_cast<DistinctPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor =
                std::make_unique<DistinctExecutor>(convert_plan_executor(x->subplan_.get(), context, count_rows));
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Filter: {
            auto x = static_cast<FilterPlan*>(plan);
            std::unique_ptr<AbstractExecutor> executor = std::make_unique<FilterExecutor>(
                convert_plan_executor(x->subplan_.get(), context, count_rows), x->conds_);
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
                executor = std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            } else if (x->tag == T_IndexSkipScan) {
                executor = std::make_unique<IndexSkipScanExecutor>(sm_manager_, x->tab_name_, x->conds_,
                                                                   x->index_col_names_, context);
            } else {
                executor = std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_,
                                                               x->index_col_names_, context);
            }
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto x = static_cast<JoinPlan*>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_.get(), context, count_rows);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_.get(), context, count_rows);
            std::unique_ptr<AbstractExecutor> join = std::make_unique<NestedLoopJoinExecutor>(
                std::move(left), std::move(right), x->conds_, x->inlj_left_col_, x->inlj_right_col_,
                x->inlj_index_col_name_, x->type);
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
                convert_plan_executor(x->subplan_.get(), context, count_rows), x->limit_, x->offset_);
            return maybe_count(std::move(executor), plan, count_rows);
        }
        case T_Union: {
            auto x = static_cast<UnionPlan*>(plan);
            std::vector<std::unique_ptr<AbstractExecutor>> branches;
            branches.reserve(x->branches_.size());
            for (const auto& branch_plan : x->branches_) {
                branches.push_back(convert_plan_executor(branch_plan.get(), context, count_rows));
            }
            return maybe_count(std::make_unique<UnionExecutor>(std::move(branches), x->cols_, x->union_all_), plan,
                               count_rows);
        }
        default:
            break;
        }
        return nullptr;
    }
};
