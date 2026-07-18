/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of this software at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/common.h"
#include "system/sm.h"
#include "optimizer/plan.h"

#include <stdexcept>

namespace {

Value clone_value(const Value& source) {
    Value value;
    value.type = source.type;
    if (source.type == TYPE_INT) {
        value.int_val = source.int_val;
    } else if (source.type == TYPE_FLOAT) {
        value.float_val = source.float_val;
    } else {
        value.str_val = source.str_val;
    }
    if (source.raw != nullptr)
        value.raw = std::make_shared<RmRecord>(*source.raw);
    return value;
}

Condition clone_condition(const Condition& source) {
    Condition condition = source;
    condition.rhs_val = clone_value(source.rhs_val);
    return condition;
}

SetClause clone_set_clause(const SetClause& source) {
    SetClause clause = source;
    clause.rhs = clone_value(source.rhs);
    return clause;
}

QueryExpr clone_query_expr(const QueryExpr& source) {
    QueryExpr expression = source;
    expression.value = clone_value(source.value);
    return expression;
}

SelectItem clone_select_item(const SelectItem& source) {
    SelectItem item = source;
    item.expr = clone_query_expr(source.expr);
    return item;
}

OrderByItem clone_order_item(const OrderByItem& source) {
    OrderByItem item = source;
    item.expr = clone_query_expr(source.expr);
    return item;
}

HavingCondition clone_having(const HavingCondition& source) {
    HavingCondition condition = source;
    condition.lhs = clone_query_expr(source.lhs);
    condition.rhs_expr = clone_query_expr(source.rhs_expr);
    condition.rhs_val = clone_value(source.rhs_val);
    return condition;
}

void copy_base(const Plan& source, Plan* destination) {
    destination->runtime_rows_ = source.runtime_rows_;
    destination->order_satisfied_ = source.order_satisfied_;
    destination->table_name_to_display_ = source.table_name_to_display_;
}

std::unique_ptr<Plan> clone_plan_impl(const Plan& source, SmManager* sm_manager) {
    switch (source.tag) {
    case T_SeqScan:
    case T_IndexScan:
    case T_IndexSkipScan: {
        const auto& scan = static_cast<const ScanPlan&>(source);
        auto copy = std::make_unique<ScanPlan>(scan.tag, sm_manager, scan.tab_name_, std::vector<Condition>{},
                                               scan.index_col_names_);
        copy->cols_ = scan.cols_;
        copy->conds_.clear();
        for (const auto& condition : scan.conds_)
            copy->conds_.push_back(clone_condition(condition));
        copy->fed_conds_.clear();
        for (const auto& condition : scan.fed_conds_)
            copy->fed_conds_.push_back(clone_condition(condition));
        copy->len_ = scan.len_;
        copy->scan_backward_ = scan.scan_backward_;
        copy_base(source, copy.get());
        return copy;
    }
    case T_NestLoop:
    case T_SortMerge: {
        const auto& join = static_cast<const JoinPlan&>(source);
        std::vector<Condition> conditions;
        for (const auto& condition : join.conds_)
            conditions.push_back(clone_condition(condition));
        auto copy = std::make_unique<JoinPlan>(join.tag, clone_plan_impl(*join.left_, sm_manager),
                                               clone_plan_impl(*join.right_, sm_manager), std::move(conditions));
        copy->inlj_left_col_ = join.inlj_left_col_;
        copy->inlj_right_col_ = join.inlj_right_col_;
        copy->inlj_index_col_name_ = join.inlj_index_col_name_;
        copy->type = join.type;
        copy_base(source, copy.get());
        return copy;
    }
    case T_Filter: {
        const auto& filter = static_cast<const FilterPlan&>(source);
        std::vector<Condition> conditions;
        for (const auto& condition : filter.conds_)
            conditions.push_back(clone_condition(condition));
        auto copy = std::make_unique<FilterPlan>(T_Filter, clone_plan_impl(*filter.subplan_, sm_manager),
                                                 std::move(conditions));
        copy_base(source, copy.get());
        return copy;
    }
    case T_Projection: {
        const auto& projection = static_cast<const ProjectionPlan&>(source);
        std::vector<SelectItem> items;
        for (const auto& item : projection.select_items_)
            items.push_back(clone_select_item(item));
        auto copy = std::make_unique<ProjectionPlan>(T_Projection, clone_plan_impl(*projection.subplan_, sm_manager),
                                                     std::move(items), projection.output_names_,
                                                     projection.preserve_col_names_, projection.is_select_star_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_Aggregate: {
        const auto& aggregate = static_cast<const AggregatePlan&>(source);
        std::vector<HavingCondition> having;
        for (const auto& condition : aggregate.having_conds_)
            having.push_back(clone_having(condition));
        auto copy = std::make_unique<AggregatePlan>(T_Aggregate, clone_plan_impl(*aggregate.subplan_, sm_manager),
                                                    aggregate.group_by_cols_, aggregate.agg_exprs_, std::move(having));
        copy_base(source, copy.get());
        return copy;
    }
    case T_Sort: {
        const auto& sort = static_cast<const SortPlan&>(source);
        std::vector<OrderByItem> items;
        for (const auto& item : sort.order_by_items_)
            items.push_back(clone_order_item(item));
        auto copy = std::make_unique<SortPlan>(T_Sort, clone_plan_impl(*sort.subplan_, sm_manager), std::move(items),
                                               sort.limit_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_Limit: {
        const auto& limit = static_cast<const LimitPlan&>(source);
        auto copy = std::make_unique<LimitPlan>(T_Limit, clone_plan_impl(*limit.subplan_, sm_manager), limit.limit_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_Union: {
        const auto& union_plan = static_cast<const UnionPlan&>(source);
        std::vector<std::unique_ptr<Plan>> branches;
        for (const auto& branch : union_plan.branches_)
            branches.push_back(clone_plan_impl(*branch, sm_manager));
        auto copy =
            std::make_unique<UnionPlan>(T_Union, std::move(branches), union_plan.cols_, union_plan.output_names_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_Insert:
    case T_Update:
    case T_Delete:
    case T_select:
    case T_ExplainAnalyze: {
        const auto& dml = static_cast<const DMLPlan&>(source);
        std::vector<Value> values;
        for (const auto& value : dml.values_)
            values.push_back(clone_value(value));
        std::vector<Condition> conditions;
        for (const auto& condition : dml.conds_)
            conditions.push_back(clone_condition(condition));
        std::vector<SetClause> clauses;
        for (const auto& clause : dml.set_clauses_)
            clauses.push_back(clone_set_clause(clause));
        auto subplan = dml.subplan_ == nullptr ? nullptr : clone_plan_impl(*dml.subplan_, sm_manager);
        auto copy = std::make_unique<DMLPlan>(dml.tag, std::move(subplan), dml.tab_name_, std::move(values),
                                              std::move(conditions), std::move(clauses));
        copy_base(source, copy.get());
        return copy;
    }
    case T_CreateTable:
    case T_DropTable:
    case T_CreateIndex:
    case T_DropIndex: {
        const auto& ddl = static_cast<const DDLPlan&>(source);
        auto copy = std::make_unique<DDLPlan>(ddl.tag, ddl.tab_name_, ddl.tab_col_names_, ddl.cols_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_Help:
    case T_ShowTable:
    case T_ShowIndex:
    case T_DescTable:
    case T_Transaction_begin:
    case T_Transaction_commit:
    case T_Transaction_abort:
    case T_Transaction_rollback:
    case T_StaticCheckpoint: {
        const auto& other = static_cast<const OtherPlan&>(source);
        auto copy = std::make_unique<OtherPlan>(other.tag, other.tab_name_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_SetKnob: {
        const auto& knob = static_cast<const SetKnobPlan&>(source);
        auto copy = std::make_unique<SetKnobPlan>(knob.set_knob_type_, knob.bool_value_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_SetTransaction: {
        const auto& transaction = static_cast<const SetTransactionPlan&>(source);
        auto copy = std::make_unique<SetTransactionPlan>(transaction.isolation_level_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_SetOutputFile: {
        const auto& output = static_cast<const SetOutputFilePlan&>(source);
        auto copy = std::make_unique<SetOutputFilePlan>(output.enable_);
        copy_base(source, copy.get());
        return copy;
    }
    case T_LoadData: {
        const auto& load = static_cast<const LoadDataPlan&>(source);
        auto copy = std::make_unique<LoadDataPlan>(load.file_name_, load.tab_name_);
        copy_base(source, copy.get());
        return copy;
    }
    default:
        throw std::logic_error("unsupported plan type for physical blueprint clone");
    }
}

} // namespace

std::unique_ptr<Plan> clone_plan(const Plan& plan, SmManager* sm_manager) {
    if (sm_manager == nullptr) {
        throw std::invalid_argument("clone_plan requires a catalog manager");
    }
    return clone_plan_impl(plan, sm_manager);
}
