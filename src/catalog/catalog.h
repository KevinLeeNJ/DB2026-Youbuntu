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

#include <cstdint>
#include <string>

#include "system/sm_meta.h"

namespace rmdb::system {
class SchemaManager;
}

namespace rmdb::catalog {
/// 只读 schema 视图。analyze/optimizer 通过它读取表/列/索引元数据，
/// 不接触存储句柄。唯一写入方是 SchemaManager。
class Catalog {
public:
    Catalog() = default;
    explicit Catalog(const DbMeta* db) : db_(db) {}

    bool is_table(const std::string& tab_name) const {
        return db_ != nullptr && db_->is_table(tab_name);
    }

    /// 获取表元数据。
    const TabMeta& get_table(const std::string& tab_name) const {
        return db_->get_table(tab_name);
    }

    /// schema 版本号，供未来 Cursor 失效检测使用。DDL 表级锁引入前不承诺强一致性。
    uint64_t schema_version() const {
        return version_;
    }

private:
    friend class rmdb::system::SchemaManager;

    void bump_schema_version() {
        ++version_;
    }

    const DbMeta* db_{nullptr};
    uint64_t version_{0};
};

} // namespace rmdb::catalog

namespace rmdb {
using catalog::Catalog;
} // namespace rmdb
