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

class DeleteExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle* fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SchemaManager* schema_manager_;
    dbaccess::TableWriteService* write_service_;

public:
    DeleteExecutor(SchemaManager* schema_manager, dbaccess::TableWriteService* write_service,
                   const std::string& tab_name, std::vector<Condition> conds, std::vector<Rid> rids, Context* context)
        : write_service_(write_service) {
        schema_manager_ = schema_manager;
        tab_name_ = tab_name;
        tab_ = schema_manager_->catalog().get_table(tab_name);
        fh_ = schema_manager_->get_table_handle(tab_name);
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rids_.empty()) {
            return nullptr; // 没有更多记录可以删除
        }
        for (Rid rid : rids_) {
            // 写协议（可见性预检 / 加锁 / 重读 / WW / WAL / Undo / 索引 / TupleMeta / SSI）
            // 统一交由 TableWriteService::remove 执行。
            write_service_->remove(tab_name_, rid, conds_, context_ == nullptr ? nullptr : context_->txn_, context_);
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
