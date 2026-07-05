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
#include <vector>

#include "catalog/catalog.h"
#include "common/context.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "system/sm_manager.h"

namespace rmdb::system {
struct ColDef;

/// DDL 编排 + 句柄所有权。Phase 2 委托 SmManager 实现，
/// Phase 6 末 SmManager 删除后直接实现逻辑。
/// 提供 DDL 方法、窄句柄访问接口（不暴露容器）和只读 Catalog。
class SchemaManager {
public:
    explicit SchemaManager(SmManager* sm_manager) : sm_manager_(sm_manager), catalog_(&sm_manager->db_) {}

    // ---- Catalog：只读 schema 视图 ----
    Catalog& catalog() {
        return catalog_;
    }

    // 元数据写访问（DDL setup 用，SchemaManager 是唯一写入方）。
    // Phase 6 SmManager 删除后随 Catalog 一并收敛。
    DbMeta& db() {
        return sm_manager_->db_;
    }

    // ---- DDL 委托 ----
    bool is_dir(const std::string& db_name) {
        return sm_manager_->is_dir(db_name);
    }
    void create_db(const std::string& db_name) {
        sm_manager_->create_db(db_name);
    }
    void drop_db(const std::string& db_name) {
        sm_manager_->drop_db(db_name);
    }
    void open_db(const std::string& db_name) {
        sm_manager_->open_db(db_name);
        catalog_.bump_schema_version();
    }
    void close_db() {
        sm_manager_->close_db();
    }
    void flush_meta() {
        sm_manager_->flush_meta();
    }
    void show_tables(Context* context) {
        sm_manager_->show_tables(context);
    }
    void show_index(const std::string& tab_name, Context* context) {
        sm_manager_->show_index(tab_name, context);
    }
    void desc_table(const std::string& tab_name, Context* context) {
        sm_manager_->desc_table(tab_name, context);
    }
    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
        sm_manager_->create_table(tab_name, col_defs, context);
        catalog_.bump_schema_version();
    }
    void drop_table(const std::string& tab_name, Context* context) {
        sm_manager_->drop_table(tab_name, context);
        catalog_.bump_schema_version();
    }
    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
        sm_manager_->create_index(tab_name, col_names, context);
        catalog_.bump_schema_version();
    }
    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
        sm_manager_->drop_index(tab_name, col_names, context);
        catalog_.bump_schema_version();
    }
    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context) {
        sm_manager_->drop_index(tab_name, col_names, context);
        catalog_.bump_schema_version();
    }

    // ---- 窄句柄接口（不暴露容器）----
    /// 获取表数据文件句柄。表不存在则抛异常。
    RmFileHandle* get_table_handle(const std::string& tab_name) const {
        return sm_manager_->fhs_.at(tab_name).get();
    }
    /// 查找表数据文件句柄。表不存在返回 nullptr（recovery 用，不抛异常）。
    RmFileHandle* find_table_handle(const std::string& tab_name) const {
        auto it = sm_manager_->fhs_.find(tab_name);
        return it == sm_manager_->fhs_.end() ? nullptr : it->second.get();
    }
    /// 获取索引句柄（按表名 + 索引列）。
    IxIndexHandle* get_index_handle(const std::string& tab_name, const std::vector<ColMeta>& cols) const {
        const std::string ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, cols);
        return sm_manager_->ihs_.at(ix_name).get();
    }
    IxIndexHandle* get_index_handle(const std::string& tab_name, const std::vector<std::string>& cols) const {
        const std::string ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, cols);
        return sm_manager_->ihs_.at(ix_name).get();
    }

    // ---- 底层 manager 访问器（过渡期保留，Phase 6 收敛）----
    BufferPoolManager* get_bpm() {
        return sm_manager_->get_bpm();
    }
    RmManager* get_rm_manager() {
        return sm_manager_->get_rm_manager();
    }
    IxManager* get_ix_manager() {
        return sm_manager_->get_ix_manager();
    }
    SmManager* sm_manager() {
        return sm_manager_;
    }

    // ---- output_file 开关（db-global，保留在 SmManager）----
    bool output_file_enabled() const {
        return sm_manager_->output_file_enabled_;
    }
    void set_output_file(bool enabled) {
        sm_manager_->output_file_enabled_ = enabled;
    }

    // ---- DML 辅助 / MVCC / 恢复 ----
    void insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec) {
        sm_manager_->insert_record_with_indexes(tab_name, rid, rec);
    }
    void delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec) {
        sm_manager_->delete_record_with_indexes(tab_name, rid, old_rec);
    }
    void update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                    const RmRecord& new_rec) {
        sm_manager_->update_record_with_indexes(tab_name, rid, old_rec, new_rec);
    }
    void flush_all_table_and_index_pages() {
        sm_manager_->flush_all_table_and_index_pages();
    }
    void rebuild_all_indexes() {
        sm_manager_->rebuild_all_indexes();
    }
    void reset_all_tuple_meta_after_recovery() {
        sm_manager_->reset_all_tuple_meta_after_recovery();
    }
    void mark_slots_committed(Transaction& txn, timestamp_t commit_ts) {
        sm_manager_->mark_slots_committed(txn, commit_ts);
    }
    void load_csv_data(const std::string& file_path, const std::string& tab_name, Context* context) {
        sm_manager_->load_csv_data(file_path, tab_name, context);
    }

private:
    SmManager* sm_manager_;
    Catalog catalog_;
};

} // namespace rmdb::system

namespace rmdb {
using system::SchemaManager;
} // namespace rmdb
