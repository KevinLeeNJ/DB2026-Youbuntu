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

// 使用最左匹配原则选择索引：等值前缀后最多接一个范围列，其他条件留给执行器过滤。
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                             std::vector<std::string>& index_col_names) {
    index_col_names.clear();
    if (curr_conds.empty()) {
        return false;
    }
    TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        return false;
    }

    for (auto& cond : curr_conds) {
        if (cond.lhs_col.tab_name != tab_name && !cond.is_rhs_val && cond.rhs_col.tab_name == tab_name) {
            std::swap(cond.lhs_col, cond.rhs_col);
            cond.op = swap_comp_op(cond.op);
        }
    }

    int best_index = -1;
    int best_prefix_len = 0;
    int best_condition_count = 0;
    std::vector<int> best_condition_order;

    for (size_t index_no = 0; index_no < tab.indexes.size(); ++index_no) {
        const auto& index = tab.indexes[index_no];
        std::vector<int> condition_order;
        bool used_range = false;
        int prefix_len = 0;

        for (const auto& index_col : index.cols) {
            std::vector<int> eq_conds;
            std::vector<int> range_conds;
            for (size_t cond_no = 0; cond_no < curr_conds.size(); ++cond_no) {
                const auto& cond = curr_conds[cond_no];
                if (!is_indexable_value_condition(cond) || cond.lhs_col.tab_name != tab_name ||
                    cond.lhs_col.col_name != index_col.name || cond.op == OP_NE) {
                    continue;
                }
                if (cond.op == OP_EQ) {
                    eq_conds.push_back(static_cast<int>(cond_no));
                } else {
                    range_conds.push_back(static_cast<int>(cond_no));
                }
            }

            if (!eq_conds.empty() && !used_range) {
                condition_order.insert(condition_order.end(), eq_conds.begin(), eq_conds.end());
                ++prefix_len;
                continue;
            }
            if (!range_conds.empty() && !used_range) {
                condition_order.insert(condition_order.end(), range_conds.begin(), range_conds.end());
                ++prefix_len;
                used_range = true;
            }
            break;
        }

        if (prefix_len > best_prefix_len ||
            (prefix_len == best_prefix_len && static_cast<int>(condition_order.size()) > best_condition_count)) {
            best_index = static_cast<int>(index_no);
            best_prefix_len = prefix_len;
            best_condition_count = static_cast<int>(condition_order.size());
            best_condition_order = std::move(condition_order);
        }
    }

    if (best_index < 0 || best_prefix_len == 0) {
        return false;
    }

    const auto& best_meta = tab.indexes[best_index];
    for (const auto& col : best_meta.cols) {
        index_col_names.push_back(col.name);
    }

    std::vector<bool> used(curr_conds.size(), false);
    std::vector<Condition> reordered;
    reordered.reserve(curr_conds.size());
    for (int cond_no : best_condition_order) {
        if (!used[cond_no]) {
            reordered.push_back(curr_conds[cond_no]);
            used[cond_no] = true;
        }
    }
    for (size_t cond_no = 0; cond_no < curr_conds.size(); ++cond_no) {
        if (!used[cond_no]) {
            reordered.push_back(curr_conds[cond_no]);
        }
    }
    curr_conds = std::move(reordered);
    return true;
}

bool Planner::get_skip_scan_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                                       std::vector<std::string>& index_col_names) {
    index_col_names.clear();
    if (curr_conds.empty()) {
        return false;
    }
    TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        return false;
    }

    for (auto& cond : curr_conds) {
        if (cond.lhs_col.tab_name != tab_name && !cond.is_rhs_val && cond.rhs_col.tab_name == tab_name) {
            std::swap(cond.lhs_col, cond.rhs_col);
            cond.op = swap_comp_op(cond.op);
        }
    }

    int best_index = -1;
    int best_suffix_eq_count = 0;
    int best_missing_prefix = std::numeric_limits<int>::max();
    std::vector<int> best_condition_order;

    for (size_t index_no = 0; index_no < tab.indexes.size(); ++index_no) {
        const auto& index = tab.indexes[index_no];
        std::vector<int> condition_order;
        bool saw_missing_prefix = false;
        bool collecting_suffix = false;
        int missing_prefix = 0;
        int suffix_eq_count = 0;

        for (const auto& index_col : index.cols) {
            std::vector<int> eq_conds;
            for (size_t cond_no = 0; cond_no < curr_conds.size(); ++cond_no) {
                const auto& cond = curr_conds[cond_no];
                if (!is_indexable_value_condition(cond) || cond.op != OP_EQ || cond.lhs_col.tab_name != tab_name ||
                    cond.lhs_col.col_name != index_col.name) {
                    continue;
                }
                eq_conds.push_back(static_cast<int>(cond_no));
            }

            if (eq_conds.empty()) {
                if (collecting_suffix) {
                    break;
                }
                saw_missing_prefix = true;
                ++missing_prefix;
                continue;
            }

            if (!saw_missing_prefix) {
                continue;
            }

            collecting_suffix = true;
            ++suffix_eq_count;
            condition_order.insert(condition_order.end(), eq_conds.begin(), eq_conds.end());
        }

        if (suffix_eq_count == 0 || missing_prefix == 0) {
            continue;
        }
        if (suffix_eq_count > best_suffix_eq_count ||
            (suffix_eq_count == best_suffix_eq_count && missing_prefix < best_missing_prefix)) {
            best_index = static_cast<int>(index_no);
            best_suffix_eq_count = suffix_eq_count;
            best_missing_prefix = missing_prefix;
            best_condition_order = std::move(condition_order);
        }
    }

    if (best_index < 0) {
        return false;
    }

    const auto& best_meta = tab.indexes[best_index];
    for (const auto& col : best_meta.cols) {
        index_col_names.push_back(col.name);
    }

    std::vector<bool> used(curr_conds.size(), false);
    std::vector<Condition> reordered;
    reordered.reserve(curr_conds.size());
    for (int cond_no : best_condition_order) {
        if (!used[cond_no]) {
            reordered.push_back(curr_conds[cond_no]);
            used[cond_no] = true;
        }
    }
    for (size_t cond_no = 0; cond_no < curr_conds.size(); ++cond_no) {
        if (!used[cond_no]) {
            reordered.push_back(curr_conds[cond_no]);
        }
    }
    curr_conds = std::move(reordered);
    return true;
}

PlanTag Planner::choose_scan_plan_tag(std::string tab_name, std::vector<Condition>& curr_conds,
                                      std::vector<std::string>& index_col_names) {
    if (get_index_cols(tab_name, curr_conds, index_col_names)) {
        return T_IndexScan;
    }
    if (get_skip_scan_index_cols(tab_name, curr_conds, index_col_names)) {
        return T_IndexSkipScan;
    }
    index_col_names.clear();
    return T_SeqScan;
}
