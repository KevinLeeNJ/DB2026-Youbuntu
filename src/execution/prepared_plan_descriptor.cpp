/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "prepared_plan_descriptor.h"

#include <algorithm>
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

bool add_having_parameters(ParameterTypes& parameters, const std::vector<HavingCondition>& conditions) {
    for (const auto& condition : conditions) {
        if (!add_query_expression_parameter(parameters, condition.lhs) ||
            (condition.is_rhs_val ? !add_value_parameter(parameters, condition.rhs_val)
                                  : !add_query_expression_parameter(parameters, condition.rhs_expr))) {
            return false;
        }
    }
    return true;
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
    case T_Aggregate: {
        const auto* aggregate = dynamic_cast<const AggregatePlan*>(&plan);
        if (aggregate == nullptr || aggregate->subplan_ == nullptr) {
            return false;
        }
        if (!add_having_parameters(parameters, aggregate->having_conds_)) {
            parameter_error = true;
            return false;
        }
        return inspect_plan(*aggregate->subplan_, parameters, limit_offset_layout, parameter_error);
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
    case T_NestLoop: {
        const auto* join = dynamic_cast<const JoinPlan*>(&plan);
        if (join == nullptr || join->left_ == nullptr || join->right_ == nullptr) {
            return false;
        }
        if (!add_condition_parameters(parameters, join->conds_)) {
            parameter_error = true;
            return false;
        }
        return inspect_plan(*join->left_, parameters, limit_offset_layout, parameter_error) &&
               inspect_plan(*join->right_, parameters, limit_offset_layout, parameter_error);
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
    if (!add_value_parameters(parameters, dml.values_)) {
        parameter_error = true;
        return false;
    }
    return true;
}

std::unique_ptr<const PreparedInsertExecutable> build_insert_executable(const DMLPlan& dml,
                                                                        std::uint64_t catalog_generation) {
    SmManager* sm_manager = dml.sm_manager_;
    if (sm_manager == nullptr || sm_manager->get_catalog_generation() != catalog_generation) {
        return nullptr;
    }

    const auto& table = sm_manager->db_.get_table(dml.tab_name_);
    if (dml.values_.size() != table.cols.size()) {
        return nullptr;
    }
    const auto table_handle = sm_manager->fhs_.find(dml.tab_name_);
    if (table_handle == sm_manager->fhs_.end() || table_handle->second == nullptr) {
        return nullptr;
    }

    auto executable = std::make_unique<PreparedInsertExecutable>();
    executable->sm_manager = sm_manager;
    executable->table_name = dml.tab_name_;
    executable->table = &table;
    executable->table_handle = table_handle->second.get();
    executable->values.reserve(dml.values_.size());
    for (std::size_t i = 0; i < dml.values_.size(); ++i) {
        executable->values.push_back(PreparedInsertValueBinding{dml.values_[i].parameter_ordinal, table.cols[i].type,
                                                                table.cols[i].len, dml.values_[i]});
    }

    executable->indexes.reserve(table.indexes.size());
    for (const auto& index : table.indexes) {
        std::string index_name = sm_manager->get_ix_manager()->get_index_name(dml.tab_name_, index.cols);
        const auto index_handle = sm_manager->ihs_.find(index_name);
        if (index_handle == sm_manager->ihs_.end() || index_handle->second == nullptr) {
            return nullptr;
        }
        executable->indexes.push_back(
            PreparedInsertIndexBinding{&index, index_handle->second.get(), std::move(index_name)});
    }
    return executable;
}

const ColMeta* find_bound_column(const std::vector<ColMeta>& columns, const TabCol& target) {
    auto position = std::find_if(columns.begin(), columns.end(), [&](const ColMeta& column) {
        return column.name == target.col_name && (target.tab_name.empty() || column.tab_name == target.tab_name);
    });
    return position == columns.end() ? nullptr : &*position;
}

const ColMeta* find_projection_column(const std::vector<ColMeta>& columns, const TabCol& target) {
    if (const ColMeta* exact = find_bound_column(columns, target); exact != nullptr) {
        return exact;
    }
    auto position = std::find_if(columns.begin(), columns.end(),
                                 [&](const ColMeta& column) { return column.name == target.col_name; });
    return position == columns.end() ? nullptr : &*position;
}

bool bind_condition_metadata(const std::vector<Condition>& conditions, const std::vector<ColMeta>& columns,
                             std::vector<PreparedConditionBinding>* output) {
    output->clear();
    output->reserve(conditions.size());
    for (const auto& condition : conditions) {
        const ColMeta* lhs = find_bound_column(columns, condition.lhs_col);
        if (lhs == nullptr || (!condition.is_rhs_val && find_bound_column(columns, condition.rhs_col) == nullptr)) {
            output->clear();
            return false;
        }
        output->push_back(PreparedConditionBinding{condition, condition.is_rhs_val ? lhs->len : 0});
    }
    return true;
}

std::unique_ptr<PreparedScanExecutable> build_scan_executable(const ScanPlan& scan, std::uint64_t catalog_generation) {
    SmManager* sm_manager = scan.sm_manager_;
    if (sm_manager == nullptr || sm_manager->get_catalog_generation() != catalog_generation ||
        (scan.tag != T_SeqScan && scan.tag != T_IndexScan)) {
        return nullptr;
    }
    const auto table_handle = sm_manager->fhs_.find(scan.tab_name_);
    if (table_handle == sm_manager->fhs_.end() || table_handle->second == nullptr) {
        return nullptr;
    }

    auto& table = sm_manager->db_.get_table(scan.tab_name_);
    auto executable = std::make_unique<PreparedScanExecutable>();
    executable->sm_manager = sm_manager;
    executable->table_name = scan.tab_name_;
    executable->table = &table;
    executable->table_handle = table_handle->second.get();
    executable->scan_backward = scan.scan_backward_;
    if (!bind_condition_metadata(scan.conds_, table.cols, &executable->conditions)) {
        return nullptr;
    }

    if (scan.tag == T_IndexScan) {
        const auto index = table.get_index_meta(scan.index_col_names_);
        if (index == table.indexes.end()) {
            return nullptr;
        }
        executable->uses_index = true;
        executable->index = &*index;
        executable->index_name = sm_manager->get_ix_manager()->get_index_name(scan.tab_name_, index->cols);
        const auto index_handle = sm_manager->ihs_.find(executable->index_name);
        if (index_handle == sm_manager->ihs_.end() || index_handle->second == nullptr) {
            return nullptr;
        }
        executable->index_handle = index_handle->second.get();
    }
    return executable;
}

std::string projection_output_name(const SelectItem& item, const ColMeta& input, bool preserve_col_names) {
    if (preserve_col_names) {
        return {};
    }
    if (!item.output_name.empty()) {
        return item.output_name;
    }
    if (!item.alias.empty()) {
        return item.alias;
    }
    if (!item.expr.display_name.empty()) {
        return item.expr.display_name;
    }
    return input.name;
}

std::unique_ptr<const PreparedSelectExecutable> build_select_executable(const DMLPlan& dml,
                                                                        std::uint64_t catalog_generation) {
    if (dml.tag != T_select || dml.subplan_ == nullptr || !dml.conds_.empty()) {
        return nullptr;
    }

    std::vector<const Plan*> outer_to_inner;
    const Plan* cursor = dml.subplan_.get();
    while (cursor != nullptr && cursor->tag != T_SeqScan && cursor->tag != T_IndexScan) {
        outer_to_inner.push_back(cursor);
        switch (cursor->tag) {
        case T_Filter:
            cursor = static_cast<const FilterPlan*>(cursor)->subplan_.get();
            break;
        case T_Projection:
            cursor = static_cast<const ProjectionPlan*>(cursor)->subplan_.get();
            break;
        case T_Limit:
            cursor = static_cast<const LimitPlan*>(cursor)->subplan_.get();
            break;
        default:
            return nullptr;
        }
    }
    const auto* scan = dynamic_cast<const ScanPlan*>(cursor);
    if (scan == nullptr) {
        return nullptr;
    }
    auto bound_scan = build_scan_executable(*scan, catalog_generation);
    if (bound_scan == nullptr || bound_scan->table == nullptr) {
        return nullptr;
    }

    auto executable = std::make_unique<PreparedSelectExecutable>();
    executable->scan = std::move(*bound_scan);
    std::vector<ColMeta> current_columns = executable->scan.table->cols;
    executable->layers.reserve(outer_to_inner.size());
    for (auto node = outer_to_inner.rbegin(); node != outer_to_inner.rend(); ++node) {
        PreparedSelectLayer layer;
        switch ((*node)->tag) {
        case T_Filter: {
            layer.kind = PreparedSelectLayerKind::Filter;
            const auto* filter = static_cast<const FilterPlan*>(*node);
            if (!bind_condition_metadata(filter->conds_, current_columns, &layer.conditions)) {
                return nullptr;
            }
            break;
        }
        case T_Projection: {
            layer.kind = PreparedSelectLayerKind::Projection;
            const auto* projection = static_cast<const ProjectionPlan*>(*node);
            std::vector<ColMeta> projected_columns;
            projected_columns.reserve(projection->select_items_.size());
            layer.projection_ordinals.reserve(projection->select_items_.size());
            layer.projection_names.reserve(projection->select_items_.size());
            int output_offset = 0;
            for (const auto& item : projection->select_items_) {
                if (item.expr.type != QueryExprType::COLUMN) {
                    return nullptr;
                }
                const ColMeta* input = find_projection_column(current_columns, item.expr.col);
                if (input == nullptr) {
                    return nullptr;
                }
                const std::size_t ordinal = static_cast<std::size_t>(input - current_columns.data());
                const std::string output_name = projection_output_name(item, *input, projection->preserve_col_names_);
                layer.projection_ordinals.push_back(ordinal);
                layer.projection_names.push_back(output_name);
                ColMeta output = *input;
                if (!output_name.empty()) {
                    output.name = output_name;
                    output.tab_name.clear();
                }
                output.offset = output_offset;
                output_offset += output.len;
                projected_columns.push_back(std::move(output));
            }
            bind_null_positions(projected_columns, output_offset);
            current_columns = std::move(projected_columns);
            break;
        }
        case T_Limit: {
            layer.kind = PreparedSelectLayerKind::Limit;
            const auto* limit = static_cast<const LimitPlan*>(*node);
            layer.limit = limit->limit_;
            layer.offset = limit->offset_;
            layer.limit_parameter_ordinal = limit->limit_parameter_ordinal_;
            layer.offset_parameter_ordinal = limit->offset_parameter_ordinal_;
            break;
        }
        default:
            return nullptr;
        }
        executable->layers.push_back(std::move(layer));
    }
    return executable;
}

bool inspect_update(const DMLPlan& dml, ParameterTypes& parameters, bool& parameter_error) {
    if (dml.tag != T_Update || dml.tab_name_.empty() || dml.subplan_ == nullptr || dml.set_clauses_.empty() ||
        !dml.values_.empty()) {
        return false;
    }
    if (dml.subplan_->tag != T_SeqScan && dml.subplan_->tag != T_IndexScan) {
        return false;
    }
    const auto* scan = dynamic_cast<const ScanPlan*>(dml.subplan_.get());
    if (scan == nullptr) {
        return false;
    }
    if (!add_condition_parameters(parameters, dml.conds_) || !add_condition_parameters(parameters, scan->conds_) ||
        !add_condition_parameters(parameters, scan->fed_conds_) ||
        !add_set_clause_parameters(parameters, dml.set_clauses_)) {
        parameter_error = true;
        return false;
    }
    return true;
}

std::optional<PreparedPointUpdateExecutable> build_point_update_executable(const DMLPlan& dml,
                                                                           const PreparedScanExecutable& scan) {
    if (!dml.point_access_.has_value() || scan.table == nullptr || scan.sm_manager == nullptr ||
        scan.table_name != dml.tab_name_ || dml.set_clauses_.empty()) {
        return std::nullopt;
    }

    const bool lock_only = dml.update_execution_mode_ == UpdateExecutionMode::LockOnlySelfAssignment;
    if (lock_only) {
        for (const auto& clause : dml.set_clauses_) {
            if (clause.lhs.tab_name != dml.tab_name_ || !clause.is_self_ref || clause.op != UpdateOp::ASSIGNMENT ||
                clause.rhs_col.tab_name != dml.tab_name_ || clause.rhs_col.col_name != clause.lhs.col_name ||
                !clause.additional_terms.empty()) {
                return std::nullopt;
            }
        }
    }

    const PointAccessPath& path = *dml.point_access_;
    const auto index_it =
        std::find_if(scan.table->indexes.begin(), scan.table->indexes.end(), [&](const IndexMeta& index) {
            if (index.cols.size() != path.index_cols.size()) {
                return false;
            }
            for (std::size_t i = 0; i < index.cols.size(); ++i) {
                if (index.cols[i].name != path.index_cols[i]) {
                    return false;
                }
            }
            return true;
        });
    if (index_it == scan.table->indexes.end() || path.condition_positions.size() != index_it->cols.size()) {
        return std::nullopt;
    }

    PreparedPointUpdateExecutable executable;
    executable.index = &*index_it;
    executable.key_length = index_it->col_tot_len;
    executable.index_name = scan.sm_manager->get_ix_manager()->get_index_name(scan.table_name, index_it->cols);
    executable.lock_only = lock_only;
    const auto index_handle = scan.sm_manager->ihs_.find(executable.index_name);
    if (index_handle == scan.sm_manager->ihs_.end() || index_handle->second == nullptr) {
        return std::nullopt;
    }
    executable.index_handle = index_handle->second.get();
    executable.key_parts.reserve(index_it->cols.size());
    int key_offset = 0;
    for (std::size_t i = 0; i < index_it->cols.size(); ++i) {
        const std::size_t condition_index = path.condition_positions[i];
        if (condition_index >= dml.conds_.size()) {
            return std::nullopt;
        }
        const Condition& condition = dml.conds_[condition_index];
        const ColMeta& column = index_it->cols[i];
        if (condition.lhs_col.tab_name != dml.tab_name_ || condition.lhs_col.col_name != column.name ||
            !is_indexable_value_condition(condition) || condition.op != OP_EQ) {
            return std::nullopt;
        }
        executable.key_parts.push_back(PreparedPointKeyPart{condition_index, condition.rhs_val.parameter_ordinal,
                                                            column.type, column.len, key_offset});
        key_offset += column.len;
    }
    if (key_offset != executable.key_length) {
        return std::nullopt;
    }
    return executable;
}

std::unique_ptr<const PreparedUpdateExecutable> build_update_executable(const DMLPlan& dml,
                                                                        std::uint64_t catalog_generation) {
    const auto* scan = dynamic_cast<const ScanPlan*>(dml.subplan_.get());
    if (scan == nullptr) {
        return nullptr;
    }
    auto bound_scan = build_scan_executable(*scan, catalog_generation);
    if (bound_scan == nullptr || bound_scan->table == nullptr) {
        return nullptr;
    }

    auto executable = std::make_unique<PreparedUpdateExecutable>();
    executable->scan = std::move(*bound_scan);
    const TabMeta& table = *executable->scan.table;
    if (!bind_condition_metadata(dml.conds_, table.cols, &executable->conditions)) {
        return nullptr;
    }
    executable->set_clauses.reserve(dml.set_clauses_.size());
    for (const auto& clause : dml.set_clauses_) {
        const ColMeta* target = find_bound_column(table.cols, clause.lhs);
        if (target == nullptr) {
            return nullptr;
        }
        executable->set_clauses.push_back(PreparedSetClauseBinding{clause, target->type, target->len});
    }

    try {
        executable->bound_conditions = BindMutationConditions(table, dml.conds_);
        executable->bound_set_clauses = BindMutationSetClauses(table, dml.set_clauses_);
    } catch (const RMDBError&) {
        return nullptr;
    }

    executable->affected_index_bitmap.assign(table.indexes.size(), false);
    executable->indexes.reserve(table.indexes.size());
    for (std::size_t index_ordinal = 0; index_ordinal < table.indexes.size(); ++index_ordinal) {
        const auto& index = table.indexes[index_ordinal];
        std::string index_name =
            executable->scan.sm_manager->get_ix_manager()->get_index_name(executable->scan.table_name, index.cols);
        const auto index_handle = executable->scan.sm_manager->ihs_.find(index_name);
        if (index_handle == executable->scan.sm_manager->ihs_.end() || index_handle->second == nullptr) {
            return nullptr;
        }
        executable->indexes.push_back(RowMutationIndex{&index, index_handle->second.get(), std::move(index_name)});
        for (const auto& clause : dml.set_clauses_) {
            if (std::any_of(index.cols.begin(), index.cols.end(),
                            [&](const ColMeta& column) { return column.name == clause.lhs.col_name; })) {
                executable->affected_index_bitmap[index_ordinal] = true;
                break;
            }
        }
    }
    executable->point_update = build_point_update_executable(dml, executable->scan);
    return executable;
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
        return;
    }
    if (!make_dense_parameter_layout(parameters, parameter_layout_)) {
        fallback_reason_ = PreparedPlanFallbackReason::InvalidParameterLayout;
        limit_offset_layout_ = {};
        return;
    }
    if (statement_kind_ == PreparedStatementKind::Select) {
        select_executable_ = build_select_executable(*dml, catalog_generation_);
    } else if (statement_kind_ == PreparedStatementKind::Insert) {
        insert_executable_ = build_insert_executable(*dml, catalog_generation_);
    } else if (statement_kind_ == PreparedStatementKind::Update) {
        update_executable_ = build_update_executable(*dml, catalog_generation_);
    }
}
