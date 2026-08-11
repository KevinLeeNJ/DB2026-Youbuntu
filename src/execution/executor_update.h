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
#include "execution_defs.h"
#include "executor_abstract.h"
#include "execution_common.h"
#include "index/ix.h"
#include "row_mutation.h"
#include "system/sm.h"
#include "optimizer/plan.h"
#include <algorithm>

class UpdateExecutor {
private:
    TabMeta tab_storage_;
    const TabMeta* tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::unique_ptr<AbstractExecutor> fallback_scan_;
    std::optional<PointAccessPath> point_access_;
    bool targets_resolved_{false};
    bool executed_{false};
    bool lock_only_{false};
    std::string tab_name_storage_;
    const std::string* tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

    std::vector<bool> owned_affected_index_bitmap_;
    std::vector<RowMutationIndex> owned_indexes_;
    std::vector<BoundMutationCondition> owned_bound_conditions_;
    std::vector<BoundMutationSetClause> owned_bound_set_clauses_;
    const std::vector<bool>* affected_index_bitmap_;
    const std::vector<RowMutationIndex>* indexes_;
    const std::vector<BoundMutationCondition>* bound_conditions_;
    const std::vector<BoundMutationSetClause>* bound_set_clauses_;
    Context* context_{nullptr};
    Rid abstract_rid_{};

public:
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::unique_ptr<AbstractExecutor> fallback_scan,
                   std::optional<PointAccessPath> point_access, UpdateExecutionMode execution_mode, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_storage_ = tab_name;
        tab_name_ = &tab_name_storage_;
        set_clauses_ = std::move(set_clauses);
        tab_storage_ = sm_manager_->db_.get_table(tab_name);
        tab_ = &tab_storage_;
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        fallback_scan_ = std::move(fallback_scan);
        point_access_ = std::move(point_access);
        context_ = context;
        lock_only_ = execution_mode == UpdateExecutionMode::LockOnlySelfAssignment && context != nullptr &&
                     context->txn_ != nullptr && context->lock_mgr_ != nullptr && context->txn_mgr_ != nullptr &&
                     context->txn_->get_isolation_level() == IsolationLevel::SNAPSHOT_ISOLATION;
        owned_bound_conditions_ = BindMutationConditions(*tab_, conds_);
        owned_bound_set_clauses_ = BindMutationSetClauses(*tab_, set_clauses_);
        bound_conditions_ = &owned_bound_conditions_;
        bound_set_clauses_ = &owned_bound_set_clauses_;

        owned_affected_index_bitmap_.assign(tab_->indexes.size(), false);
        owned_indexes_.reserve(tab_->indexes.size());
        for (size_t index_idx = 0; index_idx < tab_->indexes.size(); ++index_idx) {
            const auto& index = tab_->indexes[index_idx];
            std::string index_name = sm_manager_->get_ix_manager()->get_index_name(*tab_name_, index.cols);
            owned_indexes_.push_back(
                RowMutationIndex{&index, sm_manager_->ihs_.at(index_name).get(), std::move(index_name)});

            for (const auto& set_clause : set_clauses_) {
                if (std::any_of(index.cols.begin(), index.cols.end(),
                                [&](const ColMeta& index_col) { return index_col.name == set_clause.lhs.col_name; })) {
                    owned_affected_index_bitmap_[index_idx] = true;
                    break;
                }
            }
        }
        affected_index_bitmap_ = &owned_affected_index_bitmap_;
        indexes_ = &owned_indexes_;
    }
    void Execute() {
        if (executed_) {
            return;
        }
        if (!targets_resolved_) {
            const auto point = point_access_.has_value() ? ResolvePointMutationTarget(*sm_manager_, *tab_name_, conds_,
                                                                                      *point_access_, context_)
                                                         : PointTargetLookup{};
            if (point.kind == PointTargetLookupKind::ExactRid) {
                rids_.push_back(*point.rid);
            } else if (point.kind == PointTargetLookupKind::UnsafeFallback && fallback_scan_ != nullptr) {
                for (fallback_scan_->beginTuple(); !fallback_scan_->is_end(); fallback_scan_->nextTuple()) {
                    rids_.push_back(fallback_scan_->rid());
                }
            }
            targets_resolved_ = true;
        }
        for (const Rid& rid : rids_) {
            std::unique_ptr<RmRecord> rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                if (context_ != nullptr && context_->txn_ != nullptr &&
                    context_->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED) {
                    throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
                }
                continue;
            }
            if (lock_only_) {
                RowMutationRuntimeInfo info{sm_manager_, tab_name_, tab_, fh_, &conds_, bound_conditions_, indexes_};
                RowMutationEngine::LockOnly(rid, *rec, info, context_);
            } else {
                UpdateRuntimeInfo info{sm_manager_,
                                       tab_name_,
                                       tab_,
                                       fh_,
                                       &conds_,
                                       bound_conditions_,
                                       indexes_,
                                       &set_clauses_,
                                       bound_set_clauses_,
                                       affected_index_bitmap_};
                RowMutationEngine::UpdateOne(rid, *rec, info, context_);
            }
        }
        executed_ = true;
    }

    Rid& rid() {
        return abstract_rid_;
    }
    std::string getType() {
        return "UpdateExecutor"; // 返回执行器的名称
    }
    ColMeta get_col_offset(const TabCol& target) {
        for (const auto& col : tab_->cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
