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

class DeleteExecutor {
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle* fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::unique_ptr<AbstractExecutor> fallback_scan_;
    std::optional<PointAccessPath> point_access_;
    bool targets_resolved_{false};
    bool executed_{false};
    std::string tab_name_; // 表名称
    SmManager* sm_manager_;
    std::vector<RowMutationIndex> cached_indexes_;
    std::vector<BoundMutationCondition> bound_conditions_;
    Context* context_{nullptr};
    Rid abstract_rid_{};

public:
    DeleteExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Condition> conds,
                   std::unique_ptr<AbstractExecutor> fallback_scan, std::optional<PointAccessPath> point_access,
                   Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        fallback_scan_ = std::move(fallback_scan);
        point_access_ = std::move(point_access);
        context_ = context;
        bound_conditions_ = BindMutationConditions(tab_, conds_);
        cached_indexes_.reserve(tab_.indexes.size());
        for (const auto& index : tab_.indexes) {
            std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
            cached_indexes_.push_back(
                RowMutationIndex{&index, sm_manager_->ihs_.at(index_name).get(), std::move(index_name)});
        }
    }

    void Execute() {
        if (executed_) {
            return;
        }
        if (!targets_resolved_) {
            const auto point = point_access_.has_value() ? ResolvePointMutationTarget(*sm_manager_, tab_name_, conds_,
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
            auto rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                continue;
            }
            DeleteRuntimeInfo info{sm_manager_, &tab_name_, &tab_, fh_, &conds_, &bound_conditions_, &cached_indexes_};
            RowMutationEngine::DeleteOne(rid, *rec, info, context_);
        }
        executed_ = true;
    }
    std::string getType() {
        return "DeleteExecutor"; // 返回执行器的名称
    }
    Rid& rid() {
        return abstract_rid_;
    }
    ColMeta get_col_offset(const TabCol& target) {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == tab_name_ && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
