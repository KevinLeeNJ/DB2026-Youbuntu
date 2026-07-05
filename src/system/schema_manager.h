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

#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "record/rm_file_handle.h"
#include "server/output_sink.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "statement/statement_context.h"

namespace rmdb::index {
class IxManager;
class IxIndexHandle;
} // namespace rmdb::index

namespace rmdb {
using index::IxIndexHandle;
using index::IxManager;
} // namespace rmdb

namespace rmdb::record {
class RmManager;
}

namespace rmdb {
using record::RmManager;
}

namespace rmdb::pager {
class Pager;
}

namespace rmdb::system {

class SmManager;

/// DDL 编排 + 句柄所有权。提供 DDL 方法、窄句柄访问接口（不暴露容器）和只读 Catalog。
///
/// SchemaManager 是 system 模块的公开边界。SmManager 是 SchemaManager 内部
/// 持有的实现细节，其头文件不对 src/system/ 外部暴露。
class SchemaManager {
public:
    SchemaManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
                  IxManager* ix_manager, rmdb::pager::Pager* pager);

    // 析构在 .cpp 定义（unique_ptr<不完全类型> 需要完整类型）。
    ~SchemaManager();

    // ---- Catalog：只读 schema 视图 ----
    Catalog& catalog();

    // ---- DDL 委托 ----
    bool is_dir(const std::string& db_name);
    void create_db(const std::string& db_name);
    void drop_db(const std::string& db_name);
    void open_db(const std::string& db_name);
    void close_db();
    void flush_meta();
    void show_tables(OutputSink* sink);
    void show_index(const std::string& tab_name, OutputSink* sink);
    void desc_table(const std::string& tab_name, OutputSink* sink);
    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, StatementContext* context);
    void drop_table(const std::string& tab_name, StatementContext* context);
    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names,
                      StatementContext* context);
    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, StatementContext* context);
    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, StatementContext* context);

    // ---- 窄句柄接口（不暴露容器）----
    /// 获取表数据文件句柄。表不存在则抛异常。
    RmFileHandle* get_table_handle(const std::string& tab_name) const;
    /// 查找表数据文件句柄。表不存在返回 nullptr（recovery 用，不抛异常）。
    RmFileHandle* find_table_handle(const std::string& tab_name) const;
    /// 获取索引句柄（按表名 + 索引列）。
    IxIndexHandle* get_index_handle(const std::string& tab_name, const std::vector<ColMeta>& cols) const;
    IxIndexHandle* get_index_handle(const std::string& tab_name, const std::vector<std::string>& cols) const;
    /// 查找索引句柄。不存在返回 nullptr（不抛异常）。
    IxIndexHandle* find_index_handle(const std::string& tab_name, const std::vector<ColMeta>& cols) const;
    IxIndexHandle* find_index_handle(const std::string& tab_name, const std::vector<std::string>& cols) const;

    // ---- 底层 manager 访问器 ----
    BufferPoolManager* get_bpm();
    IxManager* get_ix_manager();

    // ---- output_file 开关（db-global，保留在 SmManager）----
    bool output_file_enabled() const;
    void set_output_file(bool enabled);

    // ---- DML 辅助 / MVCC / 恢复 ----
    void insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec);
    void delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec);
    void update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                    const RmRecord& new_rec);
    void flush_all_table_and_index_pages();
    void rebuild_all_indexes();
    void reset_all_tuple_meta_after_recovery();
    void mark_slots_committed(Transaction& txn, timestamp_t commit_ts);

private:
    std::unique_ptr<SmManager> sm_manager_;
    Catalog catalog_;
};

} // namespace rmdb::system

namespace rmdb {
using system::SchemaManager;
} // namespace rmdb
