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
#include "system/sm.h"
#include "system/schema_manager.h"
#include "access/table_write_service.h"

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SchemaManager* schema_manager_;
    dbaccess::TableWriteService* write_service_;

public:
    UpdateExecutor(SchemaManager* schema_manager, dbaccess::TableWriteService* write_service,
                   const std::string& tab_name, std::vector<SetClause> set_clauses, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context* context)
        : write_service_(write_service) {
        schema_manager_ = schema_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = schema_manager_->catalog().get_table(tab_name);
        fh_ = schema_manager_->get_table_handle(tab_name);
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        for (Rid& rid : rids_) {
            // 写协议（可见性预检 / 加锁 / 重读 / WW / WAL / Undo / 索引 / TupleMeta / SSI）
            // 统一交由 TableWriteService::update 执行。
            write_service_->update(tab_name_, rid, set_clauses_, conds_, context_ == nullptr ? nullptr : context_->txn_,
                                   context_);
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
        for (const auto& col : tab_.cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
};
