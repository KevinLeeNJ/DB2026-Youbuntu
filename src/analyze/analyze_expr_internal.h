/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "analyze.h"

#include <memory>
#include <string>
#include <vector>

namespace analyze_internal {

// --- AST value/node conversion ---
Value convert_ast_value_node(const ast::Value* sv_val);

// --- Column resolution ---
const ColMeta* resolve_column_meta(const std::vector<ColMeta>& all_cols, TabCol& target);

// --- Type checks ---
bool can_cast_types(ColType lhs_type, ColType rhs_type);
bool is_numeric_type(ColType type);
bool is_groupable_type(ColType type);

// --- Aggregate utilities ---
std::string agg_type_to_string(AggType type);
AggType convert_ast_agg_type(ast::AggFuncType type);
std::string build_agg_display_name(const AggExpr& agg);
void validate_agg_expr(AggExpr& agg, const std::vector<ColMeta>& all_cols);

// --- TabCol utilities ---
bool same_tab_col(const TabCol& lhs, const TabCol& rhs);
bool contains_group_col(const std::vector<TabCol>& group_by_cols, const TabCol& col);
QueryExpr make_column_expr(TabCol col, std::string display_name = "");

// --- QueryExpr operations ---
void normalize_query_expr(QueryExpr& expr, const std::vector<ColMeta>& all_cols);
ColType infer_expr_type(const QueryExpr& expr, const std::vector<ColMeta>& all_cols);
bool same_query_expr(const QueryExpr& lhs, const QueryExpr& rhs);

// --- Query-level expression utilities ---
bool contains_select_expr(const Query& query, const QueryExpr& expr);
const SelectItem* find_output_item_by_name(const Query& query, const std::string& name);
bool having_uses_plain_column(const HavingCondition& cond);

} // namespace analyze_internal
