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

#include "execution_common.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "row_mutation.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle* fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_; // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager* sm_manager_;

public:
    InsertExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<Value> values, Context* context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        // 尾部的 null bitmap 不在列循环的覆盖范围内，必须先清零
        std::memset(rec.data, 0, static_cast<size_t>(rec.size));
        for (size_t i = 0; i < values_.size(); i++) {
            auto& col = tab_.cols[i];
            auto& val = values_[i];
            // NULL 与列类型无关：只置位，数据字节保持全零
            if (val.is_null) {
                set_null(rec.data, col);
                continue;
            }
            if (col.type != val.type) {
                if (!can_cast(col.type, val.type)) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                // Convert value type for storage (e.g., INT literal into FLOAT column)
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

        rid_ = RowInsertEngine::InsertOne(sm_manager_, tab_name_, tab_, fh_, rec, context_, &rid_);
        return nullptr;
    }
    Rid& rid() override {
        return rid_;
    }
};
