/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the license at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "prepared_plan_descriptor.h"

#include <map>
#include <utility>

namespace {

using ParameterTypes = std::map<std::size_t, ColType>;

bool add_parameter(ParameterTypes& parameters, std::size_t ordinal, ColType type) {
    if (ordinal == 0) {
        return true;
    }
    const auto [position, inserted] = parameters.emplace(ordinal, type);
    return inserted || position->second == type;
}

bool add_value_parameter(ParameterTypes& parameters, const Value& value) {
    return add_parameter(parameters, value.parameter_ordinal, value.type);
}

bool add_condition_parameters(ParameterTypes& parameters, const std::vector<Condition>& conditions) {
    for (const auto& condition : conditions) {
        if (condition.is_rhs_val && !add_value_parameter(parameters, condition.rhs_val)) {
            return false;
        }
    }
    return true;
}

bool add_value_parameters(ParameterTypes& parameters, const std::vector<Value>& values) {
    for (const auto& value : values) {
        if (!add_value_parameter(parameters, value)) {
            return false;
        }
    }
    return true;
}

bool add_set_clause_parameters(ParameterTypes& parameters, const std::vector<SetClause>& set_clauses) {
    for (const auto& set_clause : set_clauses) {
        if (!add_value_parameter(parameters, set_clause.rhs)) {
            return false;
        }
        for (const auto& term : set_clause.additional_terms) {
            if (!add_value_parameter(parameters, term.rhs)) {
                return false;
            }
        }
    }
    return true;
}

bool add_query_expression_parameter(ParameterTypes& parameters, const QueryExpr& expression) {
    return expression.type != QueryExprType::VALUE || add_value_parameter(parameters, expression.value);
}

bool inspect_plan(const Plan& plan, ParameterTypes& parameters, PreparedLimitOffsetLayout& limit_offset_layout,
                  bool& parameter_error) {
    switch (plan.tag) {
    case T_select: {
        const auto* dml = dynamic_cast<const DMLPlan*>(&plan);
        if (dml == nullptr || dml->subplan_ == nullptr) {
            return false;
        }
        if (!add_condition_parameters(parameters, dml->conds_)) {
            parameter_error = true;
            return false;
        }
        return inspect_plan(*dml->subplan_, parameters, limit_offset_layout, parameter_error);
    }
    case T_Projection: {
        const auto* projection = dynamic_cast<const ProjectionPlan*>(&plan);
        if (projection == nullptr || projection->subplan_ == nullptr) {
            return false;
        }
        for (const auto& item : projection->select_items_) {
            if (!add_query_expression_parameter(parameters, item.expr)) {
                parameter_error = true;
                return false;
            }
        }
        return inspect_plan(*projection->subplan_, parameters, limit_offset_layout, parameter_error);
    }
    case T_Filter: {
        const auto* filter = dynamic_cast<const FilterPlan*>(&plan);
        if (filter == nullptr || filter->subplan_ == nullptr) {
            return false;
        }
        if (!add_condition_parameters(parameters, filter->conds_)) {
            parameter_error = true;
            return false;
        }
        return inspect_plan(*filter->subplan_, parameters, limit_offset_layout, parameter_error);
    }
    case T_SeqScan:
    case T_IndexScan: {
        const auto* scan = dynamic_cast<const ScanPlan*>(&plan);
        if (scan == nullptr) {
            return false;
        }
        if (!add_condition_parameters(parameters, scan->conds_) ||
            !add_condition_parameters(parameters, scan->fed_conds_)) {
            parameter_error = true;
            return false;
        }
        return true;
    }
    case T_Limit: {
        const auto* limit = dynamic_cast<const LimitPlan*>(&plan);
        if (limit == nullptr || limit->subplan_ == nullptr) {
            return false;
        }
        if (limit->limit_parameter_ordinal_ != 0) {
            if (limit_offset_layout.limit_ordinal.has_value() ||
                !add_parameter(parameters, limit->limit_parameter_ordinal_, TYPE_INT)) {
                parameter_error = true;
                return false;
            }
            limit_offset_layout.limit_ordinal = limit->limit_parameter_ordinal_;
        }
        if (limit->offset_parameter_ordinal_ != 0) {
            if (limit_offset_layout.offset_ordinal.has_value() ||
                !add_parameter(parameters, limit->offset_parameter_ordinal_, TYPE_INT)) {
                parameter_error = true;
                return false;
            }
            limit_offset_layout.offset_ordinal = limit->offset_parameter_ordinal_;
        }
        return inspect_plan(*limit->subplan_, parameters, limit_offset_layout, parameter_error);
    }
    case T_Sort: {
        const auto* sort = dynamic_cast<const SortPlan*>(&plan);
        if (sort == nullptr || sort->subplan_ == nullptr) {
            return false;
        }
        for (const auto& item : sort->order_by_items_) {
            if (!add_query_expression_parameter(parameters, item.expr)) {
                parameter_error = true;
                return false;
            }
        }
        return inspect_plan(*sort->subplan_, parameters, limit_offset_layout, parameter_error);
    }
    default:
        return false;
    }
}

bool make_dense_parameter_layout(const ParameterTypes& parameters, std::vector<PreparedParameterSlot>& layout) {
    layout.clear();
    layout.reserve(parameters.size());
    std::size_t expected = 1;
    for (const auto& [ordinal, type] : parameters) {
        if (ordinal != expected) {
            layout.clear();
            return false;
        }
        layout.push_back(PreparedParameterSlot{ordinal, type});
        ++expected;
    }
    return true;
}

bool inspect_insert(const DMLPlan& dml, ParameterTypes& parameters, bool& parameter_error) {
    if (dml.tag != T_Insert || dml.tab_name_.empty() || dml.subplan_ != nullptr || dml.values_.empty() ||
        !dml.conds_.empty() || !dml.set_clauses_.empty()) {
        return false;
    }
    parameter_error = !add_value_parameters(parameters, dml.values_);
    return !parameter_error;
}

bool inspect_update(const DMLPlan& dml, ParameterTypes& parameters, bool& parameter_error) {
    if (dml.tag != T_Update || dml.tab_name_.empty() || dml.subplan_ == nullptr || dml.set_clauses_.empty() ||
        !dml.values_.empty() || (dml.subplan_->tag != T_SeqScan && dml.subplan_->tag != T_IndexScan)) {
        return false;
    }
    const auto* scan = dynamic_cast<const ScanPlan*>(dml.subplan_.get());
    if (scan == nullptr || !add_condition_parameters(parameters, dml.conds_) ||
        !add_condition_parameters(parameters, scan->conds_) ||
        !add_condition_parameters(parameters, scan->fed_conds_) ||
        !add_set_clause_parameters(parameters, dml.set_clauses_)) {
        parameter_error = true;
        return false;
    }
    return true;
}

} // namespace

std::unique_ptr<const PreparedPlanDescriptor>
PreparedPlanDescriptor::Build(std::unique_ptr<Plan> plan, PreparedStatementKind statement_kind,
                              std::vector<std::string> output_names, std::vector<ColMeta> result_schema,
                              std::string database_identity, std::uint64_t catalog_generation) {
    return std::unique_ptr<const PreparedPlanDescriptor>(new PreparedPlanDescriptor(
        std::unique_ptr<const Plan>(std::move(plan)), statement_kind, std::move(output_names), std::move(result_schema),
        std::move(database_identity), catalog_generation));
}

PreparedPlanDescriptor::PreparedPlanDescriptor(std::unique_ptr<const Plan> plan, PreparedStatementKind statement_kind,
                                               std::vector<std::string> output_names,
                                               std::vector<ColMeta> result_schema, std::string database_identity,
                                               std::uint64_t catalog_generation)
    : plan_(std::move(plan)), statement_kind_(statement_kind), output_names_(std::move(output_names)),
      result_schema_(std::move(result_schema)), database_identity_(std::move(database_identity)),
      catalog_generation_(catalog_generation) {
    if (plan_ == nullptr) {
        fallback_reason_ = PreparedPlanFallbackReason::NullPlan;
        return;
    }
    const auto* dml = dynamic_cast<const DMLPlan*>(plan_.get());
    if (dml == nullptr) {
        fallback_reason_ = PreparedPlanFallbackReason::UnsupportedStatement;
        return;
    }
    ParameterTypes parameters;
    bool parameter_error = false;
    bool supported = false;
    switch (statement_kind_) {
    case PreparedStatementKind::Select:
        if (plan_->tag != T_select) {
            fallback_reason_ = PreparedPlanFallbackReason::UnsupportedStatement;
            return;
        }
        supported = inspect_plan(*plan_, parameters, limit_offset_layout_, parameter_error);
        break;
    case PreparedStatementKind::Insert:
        if (plan_->tag != T_Insert) {
            fallback_reason_ = PreparedPlanFallbackReason::UnsupportedStatement;
            return;
        }
        supported = inspect_insert(*dml, parameters, parameter_error);
        break;
    case PreparedStatementKind::Update:
        if (plan_->tag != T_Update) {
            fallback_reason_ = PreparedPlanFallbackReason::UnsupportedStatement;
            return;
        }
        supported = inspect_update(*dml, parameters, parameter_error);
        break;
    case PreparedStatementKind::Unsupported:
        fallback_reason_ = PreparedPlanFallbackReason::UnsupportedStatement;
        return;
    }
    if (!supported) {
        fallback_reason_ = parameter_error ? PreparedPlanFallbackReason::InvalidParameterLayout
                                           : PreparedPlanFallbackReason::UnsupportedShape;
        limit_offset_layout_ = {};
    } else if (!make_dense_parameter_layout(parameters, parameter_layout_)) {
        fallback_reason_ = PreparedPlanFallbackReason::InvalidParameterLayout;
        limit_offset_layout_ = {};
    }
}
