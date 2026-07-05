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
#include <mutex>

#include "access/table_write_service.h"
#include "access/mvcc_access.h"
#include "execution_defs.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm_meta.h"
#include "system/schema_manager.h"

namespace rmdb::exec {
class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    std::string tab_name_;      // 表名称
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SchemaManager* schema_manager_;
    rmdb::access::TableWriteService* write_service_;

public:
    InsertExecutor(SchemaManager* schema_manager, rmdb::access::TableWriteService* write_service,
                   const std::string& tab_name, std::vector<Value> values, StatementContext* context)
        : write_service_(write_service) {
        schema_manager_ = schema_manager;
        tab_ = schema_manager_->catalog().get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // 构造记录缓冲区，按列做类型转换。
        int record_size = tab_.cols.back().offset + tab_.cols.back().len;
        RmRecord rec(record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto& col = tab_.cols[i];
            auto& val = values_[i];
            if (col.type != val.type) {
                if (!can_cast(col.type, val.type)) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                // 转换存储类型（例如 INT 字面量写入 FLOAT 列）。
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
                    val.set_float(static_cast<float>(val.int_val));
                } else if (col.type == TYPE_INT && val.type == TYPE_FLOAT) {
                    val.set_int(static_cast<int>(val.float_val));
                } else if ((col.type == TYPE_STRING || col.type == TYPE_DATETIME) &&
                           (val.type == TYPE_STRING || val.type == TYPE_DATETIME)) {
                    val.type = col.type;
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        rid_ = write_service_->insert(tab_name_, rec, context_ == nullptr ? nullptr : context_->txn, context_);
        return nullptr;
    }
    Rid& rid() override {
        return rid_;
    }
};

} // namespace rmdb::exec

namespace rmdb {
using exec::InsertExecutor;
} // namespace rmdb
