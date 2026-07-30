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
#include "execution_common.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "prepared_execution_binding.h"
#include "index/ix.h"
#include "row_mutation.h"
#include "system/sm.h"
#include <algorithm>

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_storage_;
    const TabMeta* tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::optional<Rid> point_rid_;
    bool point_lookup_{false};
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

public:
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_storage_ = tab_name;
        tab_name_ = &tab_name_storage_;
        set_clauses_ = std::move(set_clauses);
        tab_storage_ = sm_manager_->db_.get_table(tab_name);
        tab_ = &tab_storage_;
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
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
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, PointMutationTarget target, Context* context, bool point_path)
        : UpdateExecutor(sm_manager, tab_name, std::move(set_clauses), std::move(conds), std::vector<Rid>{}, context) {
        point_rid_ = target.rid;
        point_lookup_ = point_path;
    }
    UpdateExecutor(const PreparedUpdateExecutable& executable, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conditions, std::vector<Rid> rids, Context* context)
        : tab_(executable.scan.table), conds_(std::move(conditions)), fh_(executable.scan.table_handle),
          rids_(std::move(rids)), tab_name_(&executable.scan.table_name), set_clauses_(std::move(set_clauses)),
          sm_manager_(executable.scan.sm_manager), affected_index_bitmap_(&executable.affected_index_bitmap),
          indexes_(&executable.indexes), bound_conditions_(&executable.bound_conditions),
          bound_set_clauses_(&executable.bound_set_clauses) {
        if (tab_ == nullptr || fh_ == nullptr || sm_manager_ == nullptr) {
            throw InternalError("bound UPDATE metadata is incomplete");
        }
        context_ = context;
    }
    UpdateExecutor(const PreparedUpdateExecutable& executable, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conditions, PointMutationTarget target, Context* context)
        : UpdateExecutor(executable, std::move(set_clauses), std::move(conditions), std::vector<Rid>{}, context) {
        point_rid_ = target.rid;
        point_lookup_ = true;
        lock_only_ = executable.point_update.has_value() && executable.point_update->lock_only;
    }
    std::unique_ptr<RmRecord> Next() override {
        const size_t rid_count = point_lookup_ ? (point_rid_.has_value() ? 1 : 0) : rids_.size();
        for (size_t rid_index = 0; rid_index < rid_count; ++rid_index) {
            Rid& rid = point_lookup_ ? *point_rid_ : rids_[rid_index];
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
        return nullptr;
    }

    Rid& rid() override {
        return _abstract_rid;
    }
    std::string getType() override {
        return "UpdateExecutor"; // 返回执行器的名称
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_->cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
