/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <string>

#include "system/sm_meta.h"

namespace rmdb::catalog {

/**
 * Read-only view of the currently opened database schema.
 *
 * Analyze and planning code can inspect table, column, and index metadata
 * through this object without gaining access to storage handles or managers.
 */
class Catalog {
public:
    explicit Catalog(const DbMeta* db) : db_(*db) {}

    bool is_table(const std::string& table_name) const {
        return db_.is_table(table_name);
    }

    const TabMeta& get_table(const std::string& table_name) const {
        return db_.get_table(table_name);
    }

    // Transitional compatibility view for analyzer/planner implementation.
    // The reference is const, so metadata consumers cannot mutate the schema.
    const DbMeta& db_;
};

} // namespace rmdb::catalog

using Catalog = rmdb::catalog::Catalog;
