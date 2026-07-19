/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/prepared_select_descriptor.h"

#include <algorithm>

#include "execution/executor_aggregate.h"
#include "execution/executor_filter.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_projection.h"

namespace {

uint32_t condition_max_length(const Condition& condition, const std::vector<ColMeta>& columns) {
    auto found = std::find_if(columns.begin(), columns.end(), [&](const ColMeta& column) {
        return column.tab_name == condition.lhs_col.tab_name && column.name == condition.lhs_col.col_name;
    });
    return found == columns.end() ? 0 : static_cast<uint32_t>(found->len);
}

bool register_conditions(const std::vector<Condition>& conditions, const std::vector<ColMeta>& columns,
                         PreparedParameterLayout* parameters) {
    for (const auto& condition : conditions) {
        if (condition.is_rhs_val &&
            !parameters->Register(condition.rhs_val, condition_max_length(condition, columns))) {
            return false;
        }
    }
    return true;
}

bool bind_conditions(const std::vector<Condition>& source, const PreparedParameterLayout& parameters,
                     const compiled::ParameterFrame& frame, std::vector<Condition>* destination) {
    *destination = source;
    for (auto& condition : *destination) {
        if (condition.is_rhs_val && !parameters.Apply(frame, &condition.rhs_val)) {
            return false;
        }
    }
    return true;
}

std::vector<std::string> projection_output_names(const ProjectionPlan& projection) {
    if (!projection.output_names_.empty()) {
        return projection.output_names_;
    }
    std::vector<std::string> names;
    names.reserve(projection.select_items_.size());
    for (const auto& item : projection.select_items_) {
        if (!item.output_name.empty()) {
            names.push_back(item.output_name);
        } else if (!item.alias.empty()) {
            names.push_back(item.alias);
        } else if (!item.expr.display_name.empty()) {
            names.push_back(item.expr.display_name);
        } else {
            names.push_back(item.expr.col.col_name);
        }
    }
    return names;
}

} // namespace

std::shared_ptr<const PreparedSelectDescriptor> PreparedSelectDescriptor::Build(const Plan& plan,
                                                                                SmManager* sm_manager) {
    if (sm_manager == nullptr || plan.tag != T_select) {
        return nullptr;
    }
    const auto& select = static_cast<const DMLPlan&>(plan);
    if (select.subplan_ == nullptr || select.subplan_->tag != T_Projection) {
        return nullptr;
    }
    const auto& projection = static_cast<const ProjectionPlan&>(*select.subplan_);
    const Plan* child = projection.subplan_.get();
    if (child == nullptr) {
        return nullptr;
    }

    const AggregatePlan* aggregate = nullptr;
    if (child->tag == T_Aggregate) {
        aggregate = static_cast<const AggregatePlan*>(child);
        child = aggregate->subplan_.get();
    }

    const FilterPlan* filter = nullptr;
    if (child->tag == T_Filter) {
        filter = static_cast<const FilterPlan*>(child);
        child = filter->subplan_.get();
    }
    if (child == nullptr || child->tag != T_IndexScan) {
        return nullptr;
    }
    const auto& scan = static_cast<const ScanPlan&>(*child);

    PreparedProjectionNode projection_node;
    projection_node.preserve_column_names = projection.preserve_col_names_;
    projection_node.columns.reserve(projection.select_items_.size());
    projection_node.items.reserve(projection.select_items_.size());
    for (const auto& item : projection.select_items_) {
        if (item.expr.type != QueryExprType::COLUMN &&
            (aggregate == nullptr || item.expr.type != QueryExprType::AGGREGATE)) {
            return nullptr;
        }
        if (projection.preserve_col_names_) {
            if (item.expr.type != QueryExprType::COLUMN) {
                return nullptr;
            }
            projection_node.columns.push_back(item.expr.col);
        } else {
            projection_node.items.push_back(PreparedProjectionItem{
                item.expr, !item.output_name.empty() ? item.output_name
                                                     : (!item.alias.empty() ? item.alias : item.expr.display_name)});
        }
    }

    auto descriptor = std::shared_ptr<PreparedSelectDescriptor>(new PreparedSelectDescriptor());
    descriptor->catalog_generation_ = sm_manager->get_catalog_generation();
    auto scan_descriptor =
        IndexScanDescriptor::Build(sm_manager, scan.tab_name_, scan.conds_, scan.index_col_names_,
                                   scan.scan_backward_ ? ScanDirection::Backward : ScanDirection::Forward);
    const auto scan_columns = scan_descriptor.columns();
    if (!register_conditions(scan_descriptor.conditions(), scan_columns, &descriptor->parameters_)) {
        return nullptr;
    }
    descriptor->nodes_.push_back(PreparedIndexScanNode{std::move(scan_descriptor)});
    if (filter != nullptr) {
        if (!register_conditions(filter->conds_, scan_columns, &descriptor->parameters_)) {
            return nullptr;
        }
        descriptor->nodes_.push_back(PreparedFilterNode{filter->conds_});
    }
    if (aggregate != nullptr) {
        // HAVING literals are part of the normalized statement parameter set,
        // but AggregateDescriptor intentionally contains finalized CellValues.
        // Until HAVING gains request-local value slots, reject that capability
        // instead of freezing the first execution's literal into shared state.
        for (const auto& condition : aggregate->having_conds_) {
            if (condition.is_rhs_val && condition.rhs_val.lexical_slot >= 0) {
                return nullptr;
            }
        }
        auto aggregate_descriptor = AggregateExecutor::BuildDescriptor(scan_columns, aggregate->group_by_cols_,
                                                                       aggregate->agg_exprs_, aggregate->having_conds_);
        descriptor->nodes_.push_back(PreparedAggregateNode{std::move(aggregate_descriptor)});
    }
    descriptor->nodes_.push_back(std::move(projection_node));
    descriptor->output_names_ = projection_output_names(projection);
    return descriptor;
}

bool PreparedSelectDescriptor::Matches(const SmManager* sm_manager) const noexcept {
    return sm_manager != nullptr && catalog_generation_ == sm_manager->get_catalog_generation();
}

std::unique_ptr<AbstractExecutor> PreparedSelectDescriptor::Instantiate(const parser::OwnedTokenStream& lexical,
                                                                        SmManager* sm_manager, Context* context) const {
    if (!Matches(sm_manager)) {
        return nullptr;
    }
    auto frame = parameters_.Bind(lexical);
    if (!frame.has_value()) {
        return nullptr;
    }

    std::unique_ptr<AbstractExecutor> executor;
    for (const auto& node : nodes_) {
        if (const auto* scan = std::get_if<PreparedIndexScanNode>(&node)) {
            std::vector<Condition> conditions;
            if (!bind_conditions(scan->descriptor.conditions(), parameters_, *frame, &conditions)) {
                return nullptr;
            }
            executor =
                std::make_unique<IndexScanExecutor>(sm_manager, scan->descriptor, std::move(conditions), context);
        } else if (const auto* filter = std::get_if<PreparedFilterNode>(&node)) {
            if (executor == nullptr) {
                return nullptr;
            }
            std::vector<Condition> conditions;
            if (!bind_conditions(filter->conditions, parameters_, *frame, &conditions)) {
                return nullptr;
            }
            executor = std::make_unique<FilterExecutor>(std::move(executor), std::move(conditions));
        } else if (const auto* aggregate = std::get_if<PreparedAggregateNode>(&node)) {
            if (executor == nullptr || aggregate->descriptor == nullptr) {
                return nullptr;
            }
            executor = std::make_unique<AggregateExecutor>(std::move(executor), aggregate->descriptor, context);
        } else if (const auto* projection = std::get_if<PreparedProjectionNode>(&node)) {
            if (executor == nullptr) {
                return nullptr;
            }
            if (projection->preserve_column_names) {
                executor = std::make_unique<ProjectionExecutor>(std::move(executor), projection->columns);
            } else {
                executor = std::make_unique<ProjectionExecutor>(std::move(executor), projection->items);
            }
        }
    }
    return executor;
}
