/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/Mulan PSL v2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <string>

#include "record/rm_file_handle.h"
#include "system/schema_manager.h"
#include "transaction/transaction.h"

namespace dbaccess {

/// TupleMeta 写入桥接。事务 commit/abort 路径更新 TupleMeta 时使用，
/// 通过 SchemaManager::get_table_handle 访问句柄，不散装访问 fhs_。
class TupleMetaWriter {
public:
    explicit TupleMetaWriter(SchemaManager* schema_mgr) : schema_mgr_(schema_mgr) {}

    void set_tuple_meta(const std::string& tab_name, const Rid& rid, const TupleMeta& meta) {
        schema_mgr_->get_table_handle(tab_name)->set_tuple_meta(rid, meta);
    }

    TupleMeta get_tuple_meta(const std::string& tab_name, const Rid& rid) const {
        return schema_mgr_->get_table_handle(tab_name)->get_tuple_meta(rid);
    }

private:
    SchemaManager* schema_mgr_;
};

} // namespace dbaccess
