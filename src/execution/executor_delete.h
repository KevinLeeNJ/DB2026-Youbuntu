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
#include "index/ix.h"
#include "row_mutation.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle* fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::optional<Rid> point_rid_;
    bool point_lookup_{false};
    std::string tab_name_; // 表名称
    SmManager* sm_manager_;
    std::vector<RowMutationIndex> cached_indexes_;
    std::vector<BoundMutationCondition> bound_conditions_;

public:
    DeleteExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        bound_conditions_ = BindMutationConditions(tab_, conds_);
        cached_indexes_.reserve(tab_.indexes.size());
        for (const auto& index : tab_.indexes) {
            std::string index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
            cached_indexes_.push_back(
                RowMutationIndex{&index, sm_manager_->ihs_.at(index_name).get(), std::move(index_name)});
        }
    }

    DeleteExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Condition> conds,
                   PointMutationTarget target, Context* context, bool point_path)
        : DeleteExecutor(sm_manager, tab_name, std::move(conds), std::vector<Rid>{}, context) {
        point_rid_ = target.rid;
        point_lookup_ = point_path;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (!point_lookup_ && rids_.empty()) {
            return nullptr; // 没有更多记录可以删除
        }
        // 删除记录
        const size_t rid_count = point_lookup_ ? (point_rid_.has_value() ? 1 : 0) : rids_.size();
        for (size_t rid_index = 0; rid_index < rid_count; ++rid_index) {
            Rid rid = point_lookup_ ? *point_rid_ : rids_[rid_index];
            auto rec = GetVisibleRecord(fh_, rid, context_);
            if (rec == nullptr) {
                continue;
            }
            DeleteRuntimeInfo info{sm_manager_, &tab_name_, &tab_, fh_, &conds_, &bound_conditions_, &cached_indexes_};
            RowMutationEngine::DeleteOne(rid, *rec, info, context_);
        }
        return nullptr;
    }
    std::string getType() override {
        return "DeleteExecutor"; // 返回执行器的名称
    }
    Rid& rid() override {
        return _abstract_rid;
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == tab_name_ && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
