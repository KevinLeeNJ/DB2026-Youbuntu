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

#include "planner.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <sstream>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

namespace {

std::string condition_sort_key(const Condition& cond) {
    auto col_key = [](const TabCol& col) { return col.tab_name + "." + col.col_name; };
    std::string key = col_key(cond.lhs_col);
    switch (cond.op) {
    case OP_EQ:
        key += "=";
        break;
    case OP_NE:
        key += "<>";
        break;
    case OP_LT:
        key += "<";
        break;
    case OP_GT:
        key += ">";
        break;
    case OP_LE:
        key += "<=";
        break;
    case OP_GE:
        key += ">=";
        break;
    }
    if (cond.null_test != NullTest::NONE) {
        key += cond.null_test == NullTest::IS_NULL ? " IS NULL" : " IS NOT NULL";
    } else if (cond.is_rhs_val) {
        if (!cond.rhs_display.empty()) {
            key += cond.rhs_display;
        } else {
            switch (cond.rhs_val.type) {
            case TYPE_INT:
                key += std::to_string(cond.rhs_val.int_val);
                break;
            case TYPE_FLOAT:
                key += std::to_string(cond.rhs_val.float_val);
                break;
            case TYPE_STRING:
            case TYPE_DATETIME:
                key += cond.rhs_val.str_val;
                break;
            }
        }
    } else {
        key += col_key(cond.rhs_col);
    }
    return key;
}

void append_cache_key_part(std::string& key, const std::string& value) {
    key.append(std::to_string(value.size()));
    key.push_back(':');
    key.append(value);
    key.push_back('|');
}

void append_cache_key_part(std::string& key, int value) {
    append_cache_key_part(key, std::to_string(value));
}

void append_cache_key_part(std::string& key, bool value) {
    append_cache_key_part(key, std::string(value ? "1" : "0"));
}

void append_tab_col_shape(std::string& key, const TabCol& col) {
    append_cache_key_part(key, col.tab_name);
    append_cache_key_part(key, col.col_name);
}

void append_query_expr_shape(std::string& key, const QueryExpr& expr) {
    append_cache_key_part(key, static_cast<int>(expr.type));
    switch (expr.type) {
    case QueryExprType::COLUMN:
        append_tab_col_shape(key, expr.col);
        break;
    case QueryExprType::AGGREGATE:
        append_cache_key_part(key, static_cast<int>(expr.agg.type));
        append_cache_key_part(key, expr.agg.is_star);
        append_cache_key_part(key, expr.agg.is_distinct);
        if (!expr.agg.is_star) {
            append_tab_col_shape(key, expr.agg.col);
        }
        break;
    case QueryExprType::VALUE:
        append_cache_key_part(key, static_cast<int>(expr.value.type));
        break;
    }
}

std::string condition_shape_key(const Condition& cond) {
    std::string key;
    append_tab_col_shape(key, cond.lhs_col);
    append_cache_key_part(key, static_cast<int>(cond.op));
    append_cache_key_part(key, static_cast<int>(cond.null_test));
    append_cache_key_part(key, cond.is_rhs_val);
    if (cond.is_rhs_val) {
        append_cache_key_part(key, cond.rhs_val.is_null);
        append_cache_key_part(key, static_cast<int>(cond.rhs_val.type));
    } else {
        append_tab_col_shape(key, cond.rhs_col);
    }
    return key;
}

void append_having_shape(std::string& key, const HavingCondition& cond) {
    append_query_expr_shape(key, cond.lhs);
    append_cache_key_part(key, static_cast<int>(cond.op));
    append_cache_key_part(key, cond.is_rhs_val);
    if (cond.is_rhs_val) {
        append_cache_key_part(key, static_cast<int>(cond.rhs_val.type));
    } else {
        append_query_expr_shape(key, cond.rhs_expr);
    }
}

void reorder_index_scan_conditions(PlanTag scan_tag, const std::string& tab_name,
                                   const std::vector<std::string>& index_col_names,
                                   std::vector<Condition>& conditions) {
    if ((scan_tag != T_IndexScan && scan_tag != T_IndexSkipScan) || index_col_names.empty()) {
        return;
    }

    std::vector<size_t> selected;
    selected.reserve(conditions.size());
    if (scan_tag == T_IndexScan) {
        bool used_range = false;
        for (const auto& index_col_name : index_col_names) {
            std::vector<size_t> equality_conditions;
            std::vector<size_t> range_conditions;
            for (size_t condition_no = 0; condition_no < conditions.size(); ++condition_no) {
                const auto& condition = conditions[condition_no];
                if (!is_indexable_value_condition(condition) || condition.lhs_col.tab_name != tab_name ||
                    condition.lhs_col.col_name != index_col_name || condition.op == OP_NE) {
                    continue;
                }
                if (condition.op == OP_EQ) {
                    equality_conditions.push_back(condition_no);
                } else {
                    range_conditions.push_back(condition_no);
                }
            }
            if (!equality_conditions.empty() && !used_range) {
                selected.insert(selected.end(), equality_conditions.begin(), equality_conditions.end());
                continue;
            }
            if (!range_conditions.empty() && !used_range) {
                selected.insert(selected.end(), range_conditions.begin(), range_conditions.end());
                used_range = true;
            }
            break;
        }
    } else {
        bool saw_missing_prefix = false;
        bool collecting_suffix = false;
        for (const auto& index_col_name : index_col_names) {
            std::vector<size_t> equality_conditions;
            for (size_t condition_no = 0; condition_no < conditions.size(); ++condition_no) {
                const auto& condition = conditions[condition_no];
                if (is_indexable_value_condition(condition) && condition.op == OP_EQ &&
                    condition.lhs_col.tab_name == tab_name && condition.lhs_col.col_name == index_col_name) {
                    equality_conditions.push_back(condition_no);
                }
            }
            if (equality_conditions.empty()) {
                if (collecting_suffix) {
                    break;
                }
                saw_missing_prefix = true;
                continue;
            }
            if (!saw_missing_prefix) {
                continue;
            }
            collecting_suffix = true;
            selected.insert(selected.end(), equality_conditions.begin(), equality_conditions.end());
        }
    }

    std::vector<bool> used(conditions.size(), false);
    std::vector<Condition> reordered;
    reordered.reserve(conditions.size());
    for (size_t condition_no : selected) {
        if (!used[condition_no]) {
            reordered.push_back(std::move(conditions[condition_no]));
            used[condition_no] = true;
        }
    }
    for (size_t condition_no = 0; condition_no < conditions.size(); ++condition_no) {
        if (!used[condition_no]) {
            reordered.push_back(std::move(conditions[condition_no]));
        }
    }
    conditions = std::move(reordered);
}

SelectItem make_column_select_item(const TabCol& col) {
    SelectItem item;
    item.expr.type = QueryExprType::COLUMN;
    item.expr.col = col;
    item.expr.display_name = col.col_name;
    return item;
}

void attach_display_names(Plan* plan, const std::unordered_map<std::string, std::string>& table_name_to_display) {
    if (plan == nullptr) {
        return;
    }
    plan->table_name_to_display_ = table_name_to_display;
    switch (plan->tag) {
    case T_Filter:
        attach_display_names(static_cast<FilterPlan*>(plan)->subplan_.get(), table_name_to_display);
        break;
    case T_Projection:
        attach_display_names(static_cast<ProjectionPlan*>(plan)->subplan_.get(), table_name_to_display);
        break;
    case T_NestLoop: {
        auto* join = static_cast<JoinPlan*>(plan);
        attach_display_names(join->left_.get(), table_name_to_display);
        attach_display_names(join->right_.get(), table_name_to_display);
        break;
    }
    default:
        break;
    }
}
bool needs_aggregate_plan(const Query& query) {
    return query.has_aggregate || !query.group_by_cols.empty() || !query.having_conds.empty();
}
bool has_value_equality(const std::vector<Condition>& conds, const std::string& tab_name, const std::string& col_name) {
    return std::any_of(conds.begin(), conds.end(), [&](const Condition& cond) {
        return is_indexable_value_condition(cond) && cond.op == OP_EQ && cond.lhs_col.tab_name == tab_name &&
               cond.lhs_col.col_name == col_name;
    });
}

bool has_value_range(const std::vector<Condition>& conds, const std::string& tab_name, const std::string& col_name) {
    return std::any_of(conds.begin(), conds.end(), [&](const Condition& cond) {
        return is_indexable_value_condition(cond) && cond.op != OP_EQ && cond.op != OP_NE &&
               cond.lhs_col.tab_name == tab_name && cond.lhs_col.col_name == col_name;
    });
}

int index_access_score(const std::string& tab_name, const std::vector<std::string>& index_col_names,
                       const std::vector<Condition>& scan_conds) {
    if (index_col_names.empty()) {
        return 0;
    }

    int score = 0;
    for (const auto& col_name : index_col_names) {
        if (has_value_equality(scan_conds, tab_name, col_name)) {
            score += 10;
            continue;
        }
        if (has_value_range(scan_conds, tab_name, col_name)) {
            score += 5;
        }
        break;
    }
    return score;
}

int skip_scan_access_score(const std::string& tab_name, const std::vector<std::string>& index_col_names,
                           const std::vector<Condition>& scan_conds) {
    if (index_col_names.empty()) {
        return 0;
    }

    int score = 0;
    bool missing_prefix = false;
    for (const auto& col_name : index_col_names) {
        if (has_value_equality(scan_conds, tab_name, col_name)) {
            score += missing_prefix ? 8 : 10;
            continue;
        }
        missing_prefix = true;
        if (score > 0) {
            break;
        }
    }
    return score;
}

bool index_is_exact_value_lookup(const std::string& tab_name, const std::vector<std::string>& index_col_names,
                                 const std::vector<Condition>& scan_conds) {
    return !index_col_names.empty() &&
           std::all_of(index_col_names.begin(), index_col_names.end(),
                       [&](const std::string& col_name) { return has_value_equality(scan_conds, tab_name, col_name); });
}

bool index_can_bind_join_col(const IndexMeta& index, const TabCol& right_col,
                             const std::vector<Condition>& right_scan_conds) {
    for (const auto& col : index.cols) {
        if (col.name == right_col.col_name) {
            return true;
        }
        if (!has_value_equality(right_scan_conds, index.tab_name, col.name)) {
            return false;
        }
    }
    return false;
}

bool index_is_parameterized_exact_lookup(const IndexMeta& index, const TabCol& right_col,
                                         const std::vector<Condition>& right_scan_conds) {
    bool binds_join_col = false;
    for (const auto& col : index.cols) {
        if (col.name == right_col.col_name) {
            binds_join_col = true;
            continue;
        }
        if (!has_value_equality(right_scan_conds, index.tab_name, col.name)) {
            return false;
        }
    }
    return binds_join_col;
}

std::vector<std::string> index_col_names(const IndexMeta& index) {
    std::vector<std::string> names;
    names.reserve(index.cols.size());
    for (const auto& col : index.cols) {
        names.push_back(col.name);
    }
    return names;
}

std::vector<std::string> index_cols_for_inlj(const TabMeta& right_tab, const TabCol& right_col,
                                             const std::vector<Condition>& right_scan_conds) {
    std::vector<std::string> prefix_candidate;
    std::vector<std::string> exact_candidate;
    for (const auto& index : right_tab.indexes) {
        if (!index_can_bind_join_col(index, right_col, right_scan_conds)) {
            continue;
        }
        auto candidate = index_col_names(index);
        if (index_is_parameterized_exact_lookup(index, right_col, right_scan_conds)) {
            if (candidate.size() > exact_candidate.size()) {
                exact_candidate = std::move(candidate);
            }
            continue;
        }
        if (prefix_candidate.empty()) {
            prefix_candidate = std::move(candidate);
        }
    }
    if (!exact_candidate.empty()) {
        return exact_candidate;
    }
    return prefix_candidate;
}

// Rebuild the right plan tree, replacing the scan leaf with new_scan (IndexScan).
// The tree structure is: [Projection -> [Filter ->]] ScanPlan
std::unique_ptr<Plan> rebuild_right_plan_with_index(std::unique_ptr<Plan> plan, std::unique_ptr<Plan> new_scan) {
    if (plan->tag == T_SeqScan || plan->tag == T_IndexScan || plan->tag == T_IndexSkipScan) {
        return new_scan;
    }
    if (plan->tag == T_Filter) {
        auto* filter = static_cast<FilterPlan*>(plan.get());
        auto rebuilt_sub = rebuild_right_plan_with_index(std::move(filter->subplan_), std::move(new_scan));
        return std::make_unique<FilterPlan>(T_Filter, std::move(rebuilt_sub), filter->conds_);
    }
    if (plan->tag == T_Projection) {
        auto* proj = static_cast<ProjectionPlan*>(plan.get());
        auto rebuilt_sub = rebuild_right_plan_with_index(std::move(proj->subplan_), std::move(new_scan));
        return std::make_unique<ProjectionPlan>(T_Projection, std::move(rebuilt_sub), proj->select_items_,
                                                proj->output_names_, proj->preserve_col_names_, proj->is_select_star_);
    }
    return plan;
}

} // namespace

std::unique_ptr<Query> Planner::logical_optimization(std::unique_ptr<Query> query, Context* context) {
    (void)context;
    std::stable_sort(query->conds.begin(), query->conds.end(), [](const Condition& lhs, const Condition& rhs) {
        return condition_sort_key(lhs) < condition_sort_key(rhs);
    });

    return query;
}

std::string Planner::make_physical_plan_cache_key(const Query& query, std::uint64_t catalog_generation) const {
    std::string key;
    append_cache_key_part(key, std::string("physical-select-v1"));
    append_cache_key_part(key, std::to_string(catalog_generation));
    append_cache_key_part(key, query.is_union);
    append_cache_key_part(key, query.is_explain_analyze);
    append_cache_key_part(key, query.has_select_star);
    append_cache_key_part(key, query.has_aggregate);

    append_cache_key_part(key, std::to_string(query.tables.size()));
    for (const auto& table : query.tables) {
        append_cache_key_part(key, table);
    }

    std::vector<std::string> condition_shapes;
    condition_shapes.reserve(query.conds.size());
    for (const auto& cond : query.conds) {
        condition_shapes.push_back(condition_shape_key(cond));
    }
    std::sort(condition_shapes.begin(), condition_shapes.end());
    append_cache_key_part(key, std::to_string(condition_shapes.size()));
    for (const auto& condition_shape : condition_shapes) {
        append_cache_key_part(key, condition_shape);
    }

    append_cache_key_part(key, std::to_string(query.select_items.size()));
    for (const auto& item : query.select_items) {
        append_query_expr_shape(key, item.expr);
    }

    append_cache_key_part(key, std::to_string(query.group_by_cols.size()));
    for (const auto& col : query.group_by_cols) {
        append_tab_col_shape(key, col);
    }

    append_cache_key_part(key, std::to_string(query.having_conds.size()));
    for (const auto& cond : query.having_conds) {
        append_having_shape(key, cond);
    }

    append_cache_key_part(key, std::to_string(query.order_by_items.size()));
    for (const auto& item : query.order_by_items) {
        append_query_expr_shape(key, item.expr);
        append_cache_key_part(key, item.is_desc);
    }
    append_cache_key_part(key, query.has_limit);
    append_cache_key_part(key, query.limit);
    append_cache_key_part(key, query.offset);
    append_cache_key_part(key, std::to_string(query.limit_parameter_ordinal));
    append_cache_key_part(key, std::to_string(query.offset_parameter_ordinal));
    return key;
}

Planner::PhysicalPlanTemplate Planner::build_physical_plan_template(const Query& query) {
    std::map<std::string, std::vector<Condition>> table_filters;
    std::vector<Condition> join_conds;
    for (const auto& cond : query.conds) {
        if (cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name) {
            table_filters[cond.lhs_col.tab_name].push_back(cond);
        } else {
            join_conds.push_back(cond);
        }
    }

    auto plan_tables = query.tables;
    std::vector<std::vector<Condition>> table_scan_conds;
    std::vector<int> table_access_scores;
    std::vector<PhysicalPlanTemplate::ScanDecision> scan_decisions;
    table_scan_conds.reserve(plan_tables.size());
    table_access_scores.reserve(plan_tables.size());
    scan_decisions.reserve(plan_tables.size());
    for (const auto& table : plan_tables) {
        auto filter_pos = table_filters.find(table);
        std::vector<Condition> scan_conds;
        if (filter_pos != table_filters.end()) {
            scan_conds = filter_pos->second;
        }
        if (!scan_conds.empty()) {
            std::stable_sort(scan_conds.begin(), scan_conds.end(), [](const Condition& lhs, const Condition& rhs) {
                return condition_sort_key(lhs) < condition_sort_key(rhs);
            });
        }
        std::vector<std::string> index_col_names;
        PlanTag scan_tag = choose_scan_plan_tag(table, scan_conds, index_col_names);
        table_access_scores.push_back(scan_tag == T_IndexSkipScan
                                          ? skip_scan_access_score(table, index_col_names, scan_conds)
                                          : index_access_score(table, index_col_names, scan_conds));
        table_scan_conds.push_back(scan_conds);
        scan_decisions.push_back({scan_tag, std::move(index_col_names)});
    }

    std::vector<char> enables_parameterized_exact_lookup(plan_tables.size(), false);
    // This structural heuristic reasons about one join edge. Keep multi-table ordering on the
    // existing score model until it has a cardinality-aware join-order search.
    if (plan_tables.size() == 2) {
        for (size_t outer_pos = 0; outer_pos < plan_tables.size(); ++outer_pos) {
            const auto& outer_table_name = plan_tables[outer_pos];
            const auto& outer_scan = scan_decisions[outer_pos];
            if (outer_scan.tag != T_IndexScan ||
                !index_is_exact_value_lookup(outer_table_name, outer_scan.index_col_names,
                                             table_scan_conds[outer_pos])) {
                continue;
            }

            auto& outer_tab = sm_manager_->db_.get_table(outer_table_name);
            for (const auto& cond : join_conds) {
                if (cond.op != OP_EQ || cond.is_rhs_val) {
                    continue;
                }

                TabCol outer_col;
                TabCol inner_col;
                if (cond.lhs_col.tab_name == outer_table_name) {
                    outer_col = cond.lhs_col;
                    inner_col = cond.rhs_col;
                } else if (cond.rhs_col.tab_name == outer_table_name) {
                    outer_col = cond.rhs_col;
                    inner_col = cond.lhs_col;
                } else {
                    continue;
                }

                auto inner_pos = std::find(plan_tables.begin(), plan_tables.end(), inner_col.tab_name);
                if (inner_pos == plan_tables.end()) {
                    continue;
                }
                size_t inner_table_pos = static_cast<size_t>(inner_pos - plan_tables.begin());
                auto& inner_tab = sm_manager_->db_.get_table(inner_col.tab_name);
                auto outer_meta = outer_tab.get_col(outer_col.col_name);
                auto inner_meta = inner_tab.get_col(inner_col.col_name);
                if (outer_meta == outer_tab.cols.end() || inner_meta == inner_tab.cols.end()) {
                    continue;
                }
                if (outer_meta->type != inner_meta->type || outer_meta->len != inner_meta->len) {
                    continue;
                }

                bool has_exact_inner =
                    std::any_of(inner_tab.indexes.begin(), inner_tab.indexes.end(), [&](const auto& index) {
                        return index_is_parameterized_exact_lookup(index, inner_col, table_scan_conds[inner_table_pos]);
                    });
                if (has_exact_inner) {
                    enables_parameterized_exact_lookup[outer_pos] = true;
                    break;
                }
            }
        }
    }

    std::vector<size_t> table_order(plan_tables.size());
    std::iota(table_order.begin(), table_order.end(), 0);
    std::stable_sort(table_order.begin(), table_order.end(), [&](size_t lhs, size_t rhs) {
        if (enables_parameterized_exact_lookup[lhs] != enables_parameterized_exact_lookup[rhs]) {
            return enables_parameterized_exact_lookup[lhs] != 0;
        }
        return table_access_scores[lhs] > table_access_scores[rhs];
    });
    std::vector<std::string> ordered_plan_tables;
    std::vector<std::vector<Condition>> ordered_table_scan_conds;
    std::vector<PhysicalPlanTemplate::ScanDecision> ordered_scan_decisions;
    ordered_plan_tables.reserve(plan_tables.size());
    ordered_table_scan_conds.reserve(table_scan_conds.size());
    ordered_scan_decisions.reserve(scan_decisions.size());
    for (size_t old_pos : table_order) {
        ordered_plan_tables.push_back(std::move(plan_tables[old_pos]));
        ordered_table_scan_conds.push_back(std::move(table_scan_conds[old_pos]));
        ordered_scan_decisions.push_back(std::move(scan_decisions[old_pos]));
    }
    plan_tables = std::move(ordered_plan_tables);
    table_scan_conds = std::move(ordered_table_scan_conds);
    scan_decisions = std::move(ordered_scan_decisions);

    if (plan_tables.empty()) {
        throw InternalError("SELECT has no table plan");
    }
    std::set<std::string> joined_tables{plan_tables[0]};
    PhysicalPlanTemplate plan_template;
    plan_template.ordered_tables = std::move(plan_tables);
    plan_template.scan_decisions = std::move(scan_decisions);
    plan_template.join_decisions.reserve(plan_template.ordered_tables.size() - 1);
    for (size_t i = 1; i < plan_template.ordered_tables.size(); ++i) {
        const auto& next_table = plan_template.ordered_tables[i];
        std::vector<Condition> curr_join_conds;
        for (const auto& cond : join_conds) {
            bool lhs_joined = joined_tables.find(cond.lhs_col.tab_name) != joined_tables.end();
            bool rhs_joined = joined_tables.find(cond.rhs_col.tab_name) != joined_tables.end();
            if ((lhs_joined && cond.rhs_col.tab_name == next_table) ||
                (rhs_joined && cond.lhs_col.tab_name == next_table)) {
                curr_join_conds.push_back(cond);
            }
        }
        std::stable_sort(curr_join_conds.begin(), curr_join_conds.end(),
                         [](const Condition& lhs, const Condition& rhs) {
                             return condition_sort_key(lhs) < condition_sort_key(rhs);
                         });

        // INLJ detection: find if right table has index on a join column
        PhysicalPlanTemplate::JoinDecision join_decision;
        if (!curr_join_conds.empty()) {
            TabMeta& next_tab = sm_manager_->db_.get_table(next_table);
            for (const auto& cond : curr_join_conds) {
                if (cond.op != OP_EQ || cond.is_rhs_val) {
                    continue;
                }
                // Identify which side is right table, which is left side
                TabCol right_col, left_col;
                if (cond.rhs_col.tab_name == next_table) {
                    right_col = cond.rhs_col;
                    left_col = cond.lhs_col;
                } else if (cond.lhs_col.tab_name == next_table) {
                    right_col = cond.lhs_col;
                    left_col = cond.rhs_col;
                } else {
                    continue;
                }
                // Check if left_col belongs to already-joined tables
                if (joined_tables.find(left_col.tab_name) == joined_tables.end()) {
                    continue;
                }
                // Check if right_col can complete an indexed prefix on the right table.
                auto candidate_index_cols = index_cols_for_inlj(next_tab, right_col, table_scan_conds[i]);
                if (!candidate_index_cols.empty()) {
                    join_decision.use_inlj = true;
                    join_decision.inlj_right_col = right_col;
                    join_decision.inlj_left_col = left_col;
                    join_decision.inlj_index_col_names = std::move(candidate_index_cols);
                    break; // only use the first matching index
                }
            }
        }
        plan_template.join_decisions.push_back(std::move(join_decision));
        joined_tables.insert(next_table);
    }
    return plan_template;
}

std::unique_ptr<Plan> Planner::instantiate_physical_plan(const Query& query,
                                                         const PhysicalPlanTemplate& plan_template) {
    if (plan_template.ordered_tables.size() != query.tables.size() ||
        plan_template.scan_decisions.size() != query.tables.size() ||
        plan_template.join_decisions.size() + 1 != query.tables.size()) {
        throw InternalError("Invalid cached SELECT physical plan template");
    }

    std::map<std::string, std::vector<Condition>> table_filters;
    std::vector<Condition> join_conds;
    for (const auto& cond : query.conds) {
        if (cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name) {
            table_filters[cond.lhs_col.tab_name].push_back(cond);
        } else {
            join_conds.push_back(cond);
        }
    }

    std::map<std::string, std::set<TabCol>> needed_cols;
    if (query.tables.size() > 1 && !query.has_select_star && !needs_aggregate_plan(query)) {
        for (const auto& item : query.select_items) {
            if (item.expr.type == QueryExprType::COLUMN) {
                needed_cols[item.expr.col.tab_name].insert(item.expr.col);
            }
        }
        for (const auto& cond : query.conds) {
            if (!cond.is_rhs_val) {
                needed_cols[cond.lhs_col.tab_name].insert(cond.lhs_col);
                needed_cols[cond.rhs_col.tab_name].insert(cond.rhs_col);
            }
        }
        // ORDER BY may reference columns that are not projected; the sort runs above the join,
        // so those columns must survive the projection pushdown.
        for (const auto& item : query.order_by_items) {
            if (item.expr.type == QueryExprType::COLUMN) {
                needed_cols[item.expr.col.tab_name].insert(item.expr.col);
            }
        }
    }

    std::vector<std::unique_ptr<Plan>> table_plans;
    std::vector<std::vector<Condition>> table_scan_conds;
    table_plans.reserve(plan_template.ordered_tables.size());
    table_scan_conds.reserve(plan_template.ordered_tables.size());
    for (size_t i = 0; i < plan_template.ordered_tables.size(); ++i) {
        const auto& table = plan_template.ordered_tables[i];
        auto filter_pos = table_filters.find(table);
        std::vector<Condition> scan_conds;
        if (filter_pos != table_filters.end()) {
            scan_conds = filter_pos->second;
        }
        if (!scan_conds.empty()) {
            std::stable_sort(scan_conds.begin(), scan_conds.end(), [](const Condition& lhs, const Condition& rhs) {
                return condition_sort_key(lhs) < condition_sort_key(rhs);
            });
        }
        const auto& scan_decision = plan_template.scan_decisions[i];
        reorder_index_scan_conditions(scan_decision.tag, table, scan_decision.index_col_names, scan_conds);
        table_scan_conds.push_back(scan_conds);
        std::unique_ptr<Plan> table_plan = std::make_unique<ScanPlan>(scan_decision.tag, sm_manager_, table, scan_conds,
                                                                      scan_decision.index_col_names);

        auto needed_pos = needed_cols.find(table);
        if (needed_pos != needed_cols.end() && !needed_pos->second.empty()) {
            std::vector<SelectItem> pushdown_items;
            pushdown_items.reserve(needed_pos->second.size());
            for (const auto& col : needed_pos->second) {
                pushdown_items.push_back(make_column_select_item(col));
            }
            table_plan = std::make_unique<ProjectionPlan>(T_Projection, std::move(table_plan),
                                                          std::move(pushdown_items), std::vector<std::string>(), true);
        }
        table_plans.push_back(std::move(table_plan));
    }

    if (table_plans.size() == 1) {
        attach_display_names(table_plans[0].get(), query.table_name_to_display);
        return std::move(table_plans[0]);
    }

    std::unique_ptr<Plan> joined = std::move(table_plans[0]);
    std::set<std::string> joined_tables{plan_template.ordered_tables[0]};
    for (size_t i = 1; i < table_plans.size(); ++i) {
        const auto& next_table = plan_template.ordered_tables[i];
        std::vector<Condition> curr_join_conds;
        for (const auto& cond : join_conds) {
            bool lhs_joined = joined_tables.find(cond.lhs_col.tab_name) != joined_tables.end();
            bool rhs_joined = joined_tables.find(cond.rhs_col.tab_name) != joined_tables.end();
            if ((lhs_joined && cond.rhs_col.tab_name == next_table) ||
                (rhs_joined && cond.lhs_col.tab_name == next_table)) {
                curr_join_conds.push_back(cond);
            }
        }
        std::stable_sort(curr_join_conds.begin(), curr_join_conds.end(),
                         [](const Condition& lhs, const Condition& rhs) {
                             return condition_sort_key(lhs) < condition_sort_key(rhs);
                         });

        const auto& join_decision = plan_template.join_decisions[i - 1];
        std::unique_ptr<Plan> right_plan = std::move(table_plans[i]);
        if (join_decision.use_inlj) {
            std::unique_ptr<Plan> new_scan = std::make_unique<ScanPlan>(
                T_IndexScan, sm_manager_, next_table, table_scan_conds[i], join_decision.inlj_index_col_names);
            right_plan = rebuild_right_plan_with_index(std::move(right_plan), std::move(new_scan));
        }

        if (!query.is_explain_analyze && curr_join_conds.empty()) {
            joined = std::make_unique<JoinPlan>(T_NestLoop, std::move(right_plan), std::move(joined), curr_join_conds);
        } else {
            auto join_plan =
                std::make_unique<JoinPlan>(T_NestLoop, std::move(joined), std::move(right_plan), curr_join_conds);
            if (join_decision.use_inlj) {
                join_plan->inlj_left_col_ = join_decision.inlj_left_col;
                join_plan->inlj_right_col_ = join_decision.inlj_right_col;
                join_plan->inlj_index_col_name_ = join_decision.inlj_right_col.col_name;
            }
            joined = std::move(join_plan);
        }
        joined_tables.insert(next_table);
    }

    attach_display_names(joined.get(), query.table_name_to_display);
    return joined;
}

std::optional<std::shared_ptr<const Planner::PhysicalPlanTemplate>>
Planner::find_physical_plan_template(const std::string& key, std::uint64_t catalog_generation) {
    std::unique_lock<std::shared_mutex> lock(physical_plan_cache_latch_);
    if (physical_plan_cache_generation_ != catalog_generation) {
        return std::nullopt;
    }

    auto cache_pos = physical_plan_cache_.find(key);
    if (cache_pos == physical_plan_cache_.end()) {
        return std::nullopt;
    }

    physical_plan_cache_lru_.splice(physical_plan_cache_lru_.begin(), physical_plan_cache_lru_,
                                    cache_pos->second.lru_position);
    cache_pos->second.lru_position = physical_plan_cache_lru_.begin();
    physical_plan_cache_hits_.fetch_add(1, std::memory_order_relaxed);
    return cache_pos->second.plan_template;
}

void Planner::cache_physical_plan_template(std::string key, std::uint64_t catalog_generation,
                                           PhysicalPlanTemplate plan_template) {
    if (sm_manager_->get_catalog_generation() != catalog_generation) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(physical_plan_cache_latch_);
    if (physical_plan_cache_generation_ != catalog_generation) {
        physical_plan_cache_.clear();
        physical_plan_cache_lru_.clear();
        physical_plan_cache_generation_ = catalog_generation;
    }

    auto cache_pos = physical_plan_cache_.find(key);
    if (cache_pos != physical_plan_cache_.end()) {
        cache_pos->second.plan_template = std::make_shared<const PhysicalPlanTemplate>(std::move(plan_template));
        physical_plan_cache_lru_.splice(physical_plan_cache_lru_.begin(), physical_plan_cache_lru_,
                                        cache_pos->second.lru_position);
        cache_pos->second.lru_position = physical_plan_cache_lru_.begin();
        return;
    }

    physical_plan_cache_lru_.push_front(key);
    auto lru_position = physical_plan_cache_lru_.begin();
    try {
        physical_plan_cache_.emplace(
            std::move(key), PhysicalPlanCacheEntry{
                                std::make_shared<const PhysicalPlanTemplate>(std::move(plan_template)), lru_position});
    } catch (...) {
        physical_plan_cache_lru_.pop_front();
        throw;
    }

    while (physical_plan_cache_.size() > kPhysicalPlanCacheCapacity) {
        auto oldest = std::prev(physical_plan_cache_lru_.end());
        physical_plan_cache_.erase(*oldest);
        physical_plan_cache_lru_.pop_back();
    }
}

std::unique_ptr<Plan> Planner::physical_optimization(Query* query, Context* context) {
    (void)context;
    const std::uint64_t catalog_generation = sm_manager_->get_catalog_generation();
    const std::string cache_key = make_physical_plan_cache_key(*query, catalog_generation);
    auto cached_template = find_physical_plan_template(cache_key, catalog_generation);
    if (cached_template.has_value()) {
        return instantiate_physical_plan(*query, *cached_template.value());
    }

    physical_plan_cache_misses_.fetch_add(1, std::memory_order_relaxed);
    auto plan_template = build_physical_plan_template(*query);
    cache_physical_plan_template(cache_key, catalog_generation, plan_template);
    return instantiate_physical_plan(*query, plan_template);
}
