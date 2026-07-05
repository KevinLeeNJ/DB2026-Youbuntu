/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "schema_manager.h"

#include <memory>
#include <string>
#include <vector>

#include "index/ix.h"
#include "index/ix_manager.h"
#include "pager/pager.h"
#include "record/rm_manager.h"
#include "sm_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

namespace rmdb::system {

SchemaManager::SchemaManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
                             IxManager* ix_manager, rmdb::pager::Pager* pager)
    : sm_manager_(std::make_unique<SmManager>(disk_manager, buffer_pool_manager, rm_manager, ix_manager, pager)),
      catalog_(&sm_manager_->db_meta()) {}

// 析构必须在 .cpp 定义（unique_ptr<不完全类型> 在头文件中前向声明）。
SchemaManager::~SchemaManager() = default;

// ---- Catalog：只读 schema 视图 ----
Catalog& SchemaManager::catalog() {
    return catalog_;
}

// ---- DDL 委托 ----
bool SchemaManager::is_dir(const std::string& db_name) {
    return sm_manager_->is_dir(db_name);
}
void SchemaManager::create_db(const std::string& db_name) {
    sm_manager_->create_db(db_name);
}
void SchemaManager::drop_db(const std::string& db_name) {
    sm_manager_->drop_db(db_name);
}
void SchemaManager::open_db(const std::string& db_name) {
    sm_manager_->open_db(db_name);
    catalog_.bump_schema_version();
}
void SchemaManager::close_db() {
    sm_manager_->close_db();
}
void SchemaManager::flush_meta() {
    sm_manager_->flush_meta();
}
void SchemaManager::show_tables(OutputSink* sink) {
    sm_manager_->show_tables(sink);
}
void SchemaManager::show_index(const std::string& tab_name, OutputSink* sink) {
    sm_manager_->show_index(tab_name, sink);
}
void SchemaManager::desc_table(const std::string& tab_name, OutputSink* sink) {
    sm_manager_->desc_table(tab_name, sink);
}
void SchemaManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs,
                                 StatementContext* context) {
    sm_manager_->create_table(tab_name, col_defs, context);
    catalog_.bump_schema_version();
}
void SchemaManager::drop_table(const std::string& tab_name, StatementContext* context) {
    sm_manager_->drop_table(tab_name, context);
    catalog_.bump_schema_version();
}
void SchemaManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names,
                                 StatementContext* context) {
    sm_manager_->create_index(tab_name, col_names, context);
    catalog_.bump_schema_version();
}
void SchemaManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names,
                               StatementContext* context) {
    sm_manager_->drop_index(tab_name, col_names, context);
    catalog_.bump_schema_version();
}
void SchemaManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names,
                               StatementContext* context) {
    sm_manager_->drop_index(tab_name, col_names, context);
    catalog_.bump_schema_version();
}

// ---- 窄句柄接口（不暴露容器）----
RmFileHandle* SchemaManager::get_table_handle(const std::string& tab_name) const {
    return sm_manager_->get_table_handle(tab_name);
}
RmFileHandle* SchemaManager::find_table_handle(const std::string& tab_name) const {
    return sm_manager_->find_table_handle(tab_name);
}
IxIndexHandle* SchemaManager::get_index_handle(const std::string& tab_name, const std::vector<ColMeta>& cols) const {
    return sm_manager_->get_index_handle(tab_name, cols);
}
IxIndexHandle* SchemaManager::get_index_handle(const std::string& tab_name,
                                               const std::vector<std::string>& cols) const {
    return sm_manager_->get_index_handle(tab_name, cols);
}
IxIndexHandle* SchemaManager::find_index_handle(const std::string& tab_name, const std::vector<ColMeta>& cols) const {
    return sm_manager_->find_index_handle(tab_name, cols);
}
IxIndexHandle* SchemaManager::find_index_handle(const std::string& tab_name,
                                                const std::vector<std::string>& cols) const {
    return sm_manager_->find_index_handle(tab_name, cols);
}

// ---- 底层 manager 访问器 ----
BufferPoolManager* SchemaManager::get_bpm() {
    return sm_manager_->get_bpm();
}
IxManager* SchemaManager::get_ix_manager() {
    return sm_manager_->get_ix_manager();
}

// ---- output_file 开关（db-global，保留在 SmManager）----
bool SchemaManager::output_file_enabled() const {
    return sm_manager_->output_file_enabled();
}
void SchemaManager::set_output_file(bool enabled) {
    sm_manager_->set_output_file(enabled);
}

// ---- DML 辅助 / MVCC / 恢复 ----
void SchemaManager::insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec) {
    sm_manager_->insert_record_with_indexes(tab_name, rid, rec);
}
void SchemaManager::delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec) {
    sm_manager_->delete_record_with_indexes(tab_name, rid, old_rec);
}
void SchemaManager::update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                               const RmRecord& new_rec) {
    sm_manager_->update_record_with_indexes(tab_name, rid, old_rec, new_rec);
}
void SchemaManager::flush_all_table_and_index_pages() {
    sm_manager_->flush_all_table_and_index_pages();
}
void SchemaManager::rebuild_all_indexes() {
    sm_manager_->rebuild_all_indexes();
}
void SchemaManager::reset_all_tuple_meta_after_recovery() {
    sm_manager_->reset_all_tuple_meta_after_recovery();
}
void SchemaManager::mark_slots_committed(Transaction& txn, timestamp_t commit_ts) {
    sm_manager_->mark_slots_committed(txn, commit_ts);
}

} // namespace rmdb::system
