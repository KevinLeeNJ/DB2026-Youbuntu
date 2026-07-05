/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "access/recovery_access.h"

#include <cstring>
#include <vector>

#include "record/rm_scan.h"

namespace dbaccess {

RecoveryAccess::RecoveryAccess(SchemaManager* schema_mgr) : schema_mgr_(schema_mgr) {}

bool RecoveryAccess::record_exists(const std::string& table_name, const Rid& rid) const {
    auto* fh = schema_mgr_->find_table_handle(table_name);
    if (fh == nullptr) {
        return false;
    }
    if (rid.page_no < 0 || rid.page_no >= fh->get_file_hdr().num_pages) {
        return false;
    }
    try {
        return fh->is_record(rid);
    } catch (const std::exception&) {
        return false;
    }
}

std::unique_ptr<RmRecord> RecoveryAccess::get_record_if_exists(const std::string& table_name, const Rid& rid) const {
    if (!record_exists(table_name, rid)) {
        return nullptr;
    }
    try {
        return schema_mgr_->get_table_handle(table_name)->get_record(rid, nullptr);
    } catch (const std::exception&) {
        return nullptr;
    }
}

bool RecoveryAccess::record_equals(const std::string& table_name, const Rid& rid, const RmRecord& expected) const {
    auto current = get_record_if_exists(table_name, rid);
    if (current == nullptr) {
        return false;
    }
    return current->size == expected.size && std::memcmp(current->data, expected.data, expected.size) == 0;
}

void RecoveryAccess::reset_tuple_meta(const std::string& table_name, const Rid& rid) {
    auto* it = schema_mgr_->find_table_handle(table_name);
    if (it == nullptr || !record_exists(table_name, rid)) {
        return;
    }
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    it->set_tuple_meta(rid, meta);
}

void RecoveryAccess::redo_insert(const InsertLogRecord& log) {
    schema_mgr_->get_table_handle(log.table_name_)->insert_record(log.rid_, log.insert_value_.data);
    // 标记 slot 由本 committed 事务重做写入，以便 undo_insert 据此区分所有权。
    auto* table_it = schema_mgr_->find_table_handle(log.table_name_);
    if (table_it != nullptr && table_it->is_record(log.rid_)) {
        TupleMeta meta;
        meta.commit_ts_ = 0;
        meta.writer_txn_id_ = log.log_tid_;
        meta.is_committed_ = true;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = UndoLink{};
        table_it->set_tuple_meta(log.rid_, meta);
    }
}

void RecoveryAccess::redo_delete(const DeleteLogRecord& log) {
    auto* table_it = schema_mgr_->find_table_handle(log.table_name_);
    if (table_it == nullptr) {
        return;
    }
    if (record_exists(log.table_name_, log.rid_)) {
        table_it->update_record(log.rid_, log.delete_value_.data, nullptr);
    } else {
        table_it->insert_record(log.rid_, log.delete_value_.data);
    }
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = log.log_tid_;
    meta.is_committed_ = true;
    meta.is_deleted_ = true;
    meta.version_chain_head_ = UndoLink{};
    table_it->set_tuple_meta(log.rid_, meta);
}

void RecoveryAccess::redo_update(const UpdateLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        schema_mgr_->get_table_handle(log.table_name_)->insert_record(log.rid_, log.new_value_.data);
    } else {
        schema_mgr_->get_table_handle(log.table_name_)->update_record(log.rid_, log.new_value_.data, nullptr);
    }
    auto* table_it = schema_mgr_->find_table_handle(log.table_name_);
    if (table_it != nullptr && table_it->is_record(log.rid_)) {
        TupleMeta meta;
        meta.commit_ts_ = 0;
        meta.writer_txn_id_ = log.log_tid_;
        meta.is_committed_ = true;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = UndoLink{};
        table_it->set_tuple_meta(log.rid_, meta);
    }
}

void RecoveryAccess::undo_insert(const InsertLogRecord& log) {
    // 幂等守卫：仅当 slot 仍归属本 loser 事务时才删除（内容比较无法区分 RID 复用）。
    auto* table_it = schema_mgr_->find_table_handle(log.table_name_);
    if (table_it == nullptr || !table_it->is_record(log.rid_)) {
        return;
    }
    TupleMeta meta = table_it->get_tuple_meta(log.rid_);
    if (meta.writer_txn_id_ != log.log_tid_) {
        return;
    }
    table_it->delete_record(log.rid_, nullptr);
}

void RecoveryAccess::undo_delete(const DeleteLogRecord& log) {
    // 幂等守卫：仅当 slot 为空或仍是本 loser 写下的 tombstone 时才恢复。
    auto* table_it = schema_mgr_->find_table_handle(log.table_name_);
    if (table_it == nullptr) {
        return;
    }
    if (record_exists(log.table_name_, log.rid_)) {
        TupleMeta meta = table_it->get_tuple_meta(log.rid_);
        if (meta.is_deleted_ && meta.writer_txn_id_ == log.log_tid_) {
            table_it->update_record(log.rid_, log.delete_value_.data, nullptr);
            TupleMeta restored_meta;
            restored_meta.commit_ts_ = 0;
            restored_meta.writer_txn_id_ = log.log_tid_;
            restored_meta.is_committed_ = false;
            restored_meta.is_deleted_ = false;
            restored_meta.version_chain_head_ = UndoLink{};
            table_it->set_tuple_meta(log.rid_, restored_meta);
        }
        return;
    }
    table_it->insert_record(log.rid_, log.delete_value_.data);
    TupleMeta restored_meta;
    restored_meta.commit_ts_ = 0;
    restored_meta.writer_txn_id_ = log.log_tid_;
    restored_meta.is_committed_ = false;
    restored_meta.is_deleted_ = false;
    restored_meta.version_chain_head_ = UndoLink{};
    table_it->set_tuple_meta(log.rid_, restored_meta);
}

void RecoveryAccess::undo_update(const UpdateLogRecord& log) {
    // 幂等守卫：仅当 rid 仍持有本 loser 写入的 new_value 时才回滚到 old_value。
    auto* table_it = schema_mgr_->find_table_handle(log.table_name_);
    if (table_it == nullptr || !table_it->is_record(log.rid_)) {
        return;
    }
    TupleMeta meta = table_it->get_tuple_meta(log.rid_);
    if (meta.writer_txn_id_ != log.log_tid_) {
        return;
    }
    if (!record_equals(log.table_name_, log.rid_, log.new_value_)) {
        return;
    }
    table_it->update_record(log.rid_, log.old_value_.data, nullptr);
    TupleMeta restored_meta;
    restored_meta.commit_ts_ = 0;
    restored_meta.writer_txn_id_ = log.log_tid_;
    restored_meta.is_committed_ = false;
    restored_meta.is_deleted_ = false;
    restored_meta.version_chain_head_ = UndoLink{};
    table_it->set_tuple_meta(log.rid_, restored_meta);
}

void RecoveryAccess::reset_all_tuple_meta() {
    schema_mgr_->reset_all_tuple_meta_after_recovery();
}

void RecoveryAccess::rebuild_indexes() {
    schema_mgr_->rebuild_all_indexes();
}

void RecoveryAccess::flush_all_table_and_index_pages() {
    schema_mgr_->flush_all_table_and_index_pages();
}

} // namespace dbaccess
