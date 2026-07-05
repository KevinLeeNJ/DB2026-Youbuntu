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

#include "access/table_write_service.h"
#include "common/context.h"
#include "system/schema_manager.h"

class Context;

namespace dbaccess {

/// LOAD DATA 服务。CSV 解析 + 批量插入，受写协议约束（无事务批量路径）。
/// 取代 SmManager::load_csv_data 的法外写路径。
class LoadDataService {
public:
    LoadDataService(SchemaManager* schema_mgr, TableWriteService* write_svc)
        : schema_mgr_(schema_mgr), write_svc_(write_svc) {}

    void load_csv(const std::string& file_path, const std::string& tab_name, Context* ctx);

private:
    SchemaManager* schema_mgr_;
    TableWriteService* write_svc_;
};

} // namespace dbaccess
