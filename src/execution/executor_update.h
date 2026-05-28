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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

public:
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        for (Rid& rid : rids_) {
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            bool match = true;
            for (auto cond : conds_) // 判断是否匹配
            {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                auto staged_rec = std::make_unique<RmRecord>(*rec); // 对原记录进行拷贝
                std::vector<IndexMeta> deleted_indexes;
                std::vector<IndexMeta> updated_indexes;
                update_record(rec.get()); // 对记录更新
                try {
                    for (const auto& index : tab_.indexes) {
                        auto ih =
                            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                .get();
                        ih->delete_entry(staged_rec->data, context_->txn_); // 删除旧索引
                        deleted_indexes.push_back(index);
                        ih->insert_entry(rec->data, rid, context_->txn_); // 插入新索引
                        updated_indexes.push_back(index);
                    }
                } catch (IndexEntryExistsError&) // 有重复的话则回滚
                {
                    for (const auto& index : deleted_indexes) {
                        auto ih =
                            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                .get();
                        ih->insert_entry(staged_rec->data, rid, context_->txn_); // 恢复旧索引
                    }
                    for (const auto& index : updated_indexes) {
                        auto ih =
                            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                                .get();
                        ih->delete_entry(rec->data, context_->txn_); // 删除新索引
                    }
                    throw; // 仍然抛出错误
                }
                fh_->update_record(rid, rec->data, context_);
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
    /**
     * @brief 更新记录
     * @param rec 需要更新的记录
     */
    void update_record(RmRecord* rec) {
        for (const auto& set_clause : set_clauses_) {
            auto col_meta = get_col_offset(set_clause.lhs);
            char* data = rec->data + col_meta.offset;
            if (can_cast(col_meta.type, set_clause.rhs.type) == false) {
                throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(set_clause.rhs.type));
            }
            switch (col_meta.type) {
            case TYPE_INT: {
                if (set_clause.rhs.type == TYPE_INT) {
                    *(int*)data = set_clause.rhs.int_val;
                } else {
                    *(int*)data = (int)set_clause.rhs.float_val;
                }
                break;
            }
            case TYPE_FLOAT: {
                if (set_clause.rhs.type == TYPE_FLOAT) {
                    *(float*)data = set_clause.rhs.float_val;
                } else {
                    *(float*)data = (float)set_clause.rhs.int_val;
                }
                break;
            }
            case TYPE_STRING: {
                int len = get_col_offset(set_clause.lhs).len;
                std::memset(data, 0, len);
                std::memcpy(data, set_clause.rhs.str_val.c_str(), std::min(len, (int)set_clause.rhs.str_val.size()));
                break;
            }
            }
        }
    }
    ColMeta get_col_offset(const TabCol& target) override {
        for (const auto& col : tab_.cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
    }
};