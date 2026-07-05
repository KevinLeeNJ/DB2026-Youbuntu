/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "access/cursor/table_access.h"

#include "access/mvcc_access.h"
#include "system/schema_manager.h"

namespace rmdb::access {

TableAccess::TableAccess(rmdb::system::SchemaManager* schema_manager) : schema_manager_(schema_manager) {}

std::unique_ptr<TableCursor> TableAccess::open_table_scan(const std::string& tab_name) {
    return std::make_unique<TableCursor>(schema_manager_->get_table_handle(tab_name), schema_manager_->get_bpm());
}

std::unique_ptr<IndexCursor> TableAccess::open_index_scan(const std::string& tab_name,
                                                          const std::vector<ColMeta>& index_cols) {
    return std::make_unique<IndexCursor>(schema_manager_->get_index_handle(tab_name, index_cols),
                                         schema_manager_->get_table_handle(tab_name), schema_manager_->get_bpm());
}

std::unique_ptr<IndexCursor> TableAccess::open_index_scan(const std::string& tab_name,
                                                          const std::vector<std::string>& index_cols) {
    return std::make_unique<IndexCursor>(schema_manager_->get_index_handle(tab_name, index_cols),
                                         schema_manager_->get_table_handle(tab_name), schema_manager_->get_bpm());
}

int TableAccess::record_size(const std::string& tab_name) {
    return schema_manager_->get_table_handle(tab_name)->get_file_hdr().record_size;
}

bool TableAccess::is_record(const std::string& tab_name, const Rid& rid) {
    return schema_manager_->get_table_handle(tab_name)->is_record(rid);
}

TupleMeta TableAccess::get_tuple_meta(const std::string& tab_name, const Rid& rid) {
    return schema_manager_->get_table_handle(tab_name)->get_tuple_meta(rid);
}

std::unique_ptr<RmRecord> TableAccess::get_record(const std::string& tab_name, const Rid& rid, rmdb::Context* context) {
    return schema_manager_->get_table_handle(tab_name)->get_record(rid, context);
}

std::unique_ptr<RmRecord> TableAccess::get_visible_record(const std::string& tab_name, const Rid& rid,
                                                          rmdb::Context* context) {
    return GetVisibleRecord(schema_manager_->get_table_handle(tab_name), rid, context);
}

bool TableAccess::check_predicate_invisible_writes(rmdb::Context* context, txn_id_t reader, const std::string& tab_name,
                                                   const std::vector<Condition>& conds,
                                                   const std::vector<ColMeta>& cols) {
    if (context == nullptr || context->txn_mgr_ == nullptr) {
        return false;
    }
    return context->txn_mgr_->CheckPredicateInvisibleWrites(reader, tab_name, conds,
                                                            schema_manager_->get_table_handle(tab_name), cols);
}

bool TableAccess::deleted_tuple_candidates_conflict_with_insert(const std::string& tab_name,
                                                                const RmRecord& inserted_rec, rmdb::Context* context) {
    return DeletedTupleCandidatesConflictWithInsert(schema_manager_->get_table_handle(tab_name), tab_name, inserted_rec,
                                                    context);
}

bool TableAccess::historical_index_key_conflicts_with_txn(const std::string& tab_name, const Rid& rid,
                                                          const IndexMeta& index, const std::vector<char>& key,
                                                          rmdb::Context* context) {
    return HistoricalIndexKeyConflictsWithTxn(schema_manager_->get_table_handle(tab_name), rid, index, key, context);
}

} // namespace rmdb::access
