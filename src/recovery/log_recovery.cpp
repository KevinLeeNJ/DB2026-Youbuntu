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

#include "log_recovery.h"
#include "common/fault_injection.h"

#include <algorithm>
#include <fstream>
#include <vector>

namespace {

bool ResolveTableIdLog(LogRecord* record, SmManager* sm_manager) {
    oid_t table_id = 0;
    std::string* table_name = nullptr;
    switch (record->log_type_) {
    case LogType::INSERT_TABLE_ID: {
        auto* log = static_cast<InsertLogRecord*>(record);
        table_id = log->table_id_;
        table_name = &log->table_name_;
        break;
    }
    case LogType::DELETE_TABLE_ID: {
        auto* log = static_cast<DeleteLogRecord*>(record);
        table_id = log->table_id_;
        table_name = &log->table_name_;
        break;
    }
    case LogType::UPDATE_TABLE_ID: {
        auto* log = static_cast<UpdateLogRecord*>(record);
        table_id = log->table_id_;
        table_name = &log->table_name_;
        break;
    }
    default:
        return true;
    }
    auto resolved = sm_manager->get_table_name(table_id);
    if (resolved.has_value()) {
        *table_name = std::move(*resolved);
        return true;
    }
    if (sm_manager->is_known_table_id(table_id)) {
        table_name->clear();
        return true;
    }
    return false;
}

} // namespace

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    active_txn_last_lsn_.clear();
    committed_txns_.clear();
    log_records_.clear();
    log_order_.clear();
    max_lsn_ = INVALID_LSN;
    checkpoint_offset_ = 0;

    const int64_t file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return;
    }

    int64_t scan_offset = 0;
    {
        std::ifstream restart_file(LogManager::RESTART_FILE_NAME);
        int64_t restart_offset = 0;
        if (restart_file >> restart_offset) {
            if (restart_offset >= 0 && restart_offset + LOG_HEADER_SIZE <= file_size) {
                std::vector<char> header_buf(LOG_HEADER_SIZE);
                if (disk_manager_->read_log(header_buf.data(), LOG_HEADER_SIZE, restart_offset) == LOG_HEADER_SIZE) {
                    LogRecord header;
                    header.deserialize(header_buf.data());
                    if (header.log_type_ == LogType::CHECKPOINT && header.log_tot_len_ >= LOG_HEADER_SIZE &&
                        restart_offset + static_cast<int64_t>(header.log_tot_len_) <= file_size) {
                        std::vector<char> record_buf(header.log_tot_len_);
                        if (disk_manager_->read_log(record_buf.data(), static_cast<int>(record_buf.size()),
                                                    restart_offset) == static_cast<int>(record_buf.size())) {
                            auto checkpoint =
                                DeserializeLogRecord(record_buf.data(), static_cast<int>(record_buf.size()));
                            if (checkpoint != nullptr && checkpoint->log_type_ == LogType::CHECKPOINT) {
                                scan_offset = restart_offset;
                                checkpoint_offset_ = restart_offset;
                                const auto* checkpoint_log = static_cast<const CheckpointLogRecord*>(checkpoint.get());
                                active_txn_last_lsn_ = checkpoint_log->active_txns_;
                            }
                        }
                    }
                }
            }
        }
    }

    int64_t offset = scan_offset;
    while (offset + LOG_HEADER_SIZE <= file_size) {
        std::vector<char> header_buf(LOG_HEADER_SIZE);
        int header_bytes = disk_manager_->read_log(header_buf.data(), LOG_HEADER_SIZE, offset);
        if (header_bytes != LOG_HEADER_SIZE) {
            break;
        }

        LogRecord header;
        header.deserialize(header_buf.data());
        if (header.log_tot_len_ < LOG_HEADER_SIZE || offset + static_cast<int64_t>(header.log_tot_len_) > file_size) {
            break;
        }

        std::vector<char> record_buf(header.log_tot_len_);
        int record_bytes = disk_manager_->read_log(record_buf.data(), static_cast<int>(record_buf.size()), offset);
        if (record_bytes != static_cast<int>(record_buf.size())) {
            break;
        }

        auto record = DeserializeLogRecord(record_buf.data(), static_cast<int>(record_buf.size()));
        if (record == nullptr) {
            break;
        }
        if (!ResolveTableIdLog(record.get(), sm_manager_)) {
            throw InternalError("WAL references an unknown table id");
        }

        const lsn_t lsn = record->lsn_;
        const txn_id_t txn_id = record->log_tid_;
        switch (record->log_type_) {
        case LogType::BEGIN:
            active_txn_last_lsn_[txn_id] = lsn;
            break;
        case LogType::INSERT:
        case LogType::INSERT_TABLE_ID:
        case LogType::DELETE:
        case LogType::DELETE_TABLE_ID:
        case LogType::UPDATE:
        case LogType::UPDATE_TABLE_ID:
            active_txn_last_lsn_[txn_id] = lsn;
            break;
        case LogType::COMMIT:
            committed_txns_.insert(txn_id);
            active_txn_last_lsn_.erase(txn_id);
            break;
        case LogType::ABORT:
            // ABORT only records that rollback was requested. This system does not write CLRs,
            // so recovery must still idempotently undo the transaction's original DML records.
            active_txn_last_lsn_[txn_id] = lsn;
            break;
        case LogType::CHECKPOINT:
            break;
        }

        log_order_.push_back(lsn);
        log_records_[lsn] = std::move(record);
        if (lsn != INVALID_LSN && (max_lsn_ == INVALID_LSN || lsn > max_lsn_)) {
            max_lsn_ = lsn;
        }
        offset += static_cast<int64_t>(header.log_tot_len_);
    }

    // A crash may persist newly allocated record pages before the short file
    // header update reaches disk. Reconcile the in-memory allocation metadata
    // with the physical file size before redo/undo can fetch those RIDs.
    for (const auto& [_, file_handle] : sm_manager_->fhs_) {
        file_handle->rebuild_file_header_from_pages();
    }
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    for (const auto lsn : log_order_) {
        auto it = log_records_.find(lsn);
        if (it == log_records_.end() || committed_txns_.count(it->second->log_tid_) == 0) {
            continue;
        }

        switch (it->second->log_type_) {
        case LogType::INSERT:
        case LogType::INSERT_TABLE_ID:
            redo_insert(*static_cast<InsertLogRecord*>(it->second.get()));
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::DELETE:
        case LogType::DELETE_TABLE_ID:
            redo_delete(*static_cast<DeleteLogRecord*>(it->second.get()));
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::UPDATE:
        case LogType::UPDATE_TABLE_ID:
            redo_update(*static_cast<UpdateLogRecord*>(it->second.get()));
            FaultInjector::Point("mid_recovery_redo");
            break;
        default:
            break;
        }
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    for (const auto& [txn_id, last_lsn] : active_txn_last_lsn_) {
        (void)txn_id;
        lsn_t current_lsn = last_lsn;
        while (current_lsn != INVALID_LSN) {
            auto it = log_records_.find(current_lsn);
            if (it == log_records_.end()) {
                break;
            }

            auto* record = it->second.get();
            switch (record->log_type_) {
            case LogType::INSERT:
            case LogType::INSERT_TABLE_ID:
                undo_insert(*static_cast<InsertLogRecord*>(record));
                FaultInjector::Point("mid_recovery_undo");
                break;
            case LogType::DELETE:
            case LogType::DELETE_TABLE_ID:
                undo_delete(*static_cast<DeleteLogRecord*>(record));
                FaultInjector::Point("mid_recovery_undo");
                break;
            case LogType::UPDATE:
            case LogType::UPDATE_TABLE_ID:
                undo_update(*static_cast<UpdateLogRecord*>(record));
                FaultInjector::Point("mid_recovery_undo");
                break;
            case LogType::BEGIN:
                current_lsn = INVALID_LSN;
                continue;
            default:
                break;
            }
            current_lsn = record->prev_lsn_;
        }
    }
    sm_manager_->reset_all_tuple_meta_after_recovery();
    rebuild_indexes();
    if (!sm_manager_->flush_all_table_and_index_pages()) {
        // Recovery results are not durable. Keep the complete WAL and refuse
        // normal startup so the next process can retry recovery.
        throw InternalError("recovery page flush failed; WAL retained");
    }
    // 表页与索引页已落盘后，截断日志文件并推进 global_lsn。
    // 这样已 undo 完毕的 loser 日志不再残留，避免下一次重启跨轮重复 undo
    // 同 RID 上的数据（尤其是 RID 复用且内容相同时，仅靠 undo 内容守卫无法区分）。
    if (log_manager_ != nullptr) {
        lsn_t next_lsn = (max_lsn_ == INVALID_LSN) ? 0 : max_lsn_ + 1;
        FaultInjector::Point("before_recovery_wal_reset");
        log_manager_->reset_log(next_lsn);
    }
}

bool RecoveryManager::record_exists(const std::string& table_name, const Rid& rid) const {
    auto it = sm_manager_->fhs_.find(table_name);
    if (it == sm_manager_->fhs_.end()) {
        return false;
    }
    if (rid.page_no < 0 || rid.page_no >= it->second->get_file_hdr().num_pages) {
        return false;
    }
    try {
        return it->second->is_record(rid);
    } catch (const std::exception&) {
        return false;
    }
}

std::unique_ptr<RmRecord> RecoveryManager::get_record_if_exists(const std::string& table_name, const Rid& rid) const {
    if (!record_exists(table_name, rid)) {
        return nullptr;
    }
    try {
        return sm_manager_->fhs_.at(table_name)->get_record(rid, nullptr);
    } catch (const std::exception&) {
        return nullptr;
    }
}

bool RecoveryManager::record_equals(const std::string& table_name, const Rid& rid, const RmRecord& expected) const {
    auto current = get_record_if_exists(table_name, rid);
    if (current == nullptr) {
        return false;
    }
    return current->size == expected.size && memcmp(current->data, expected.data, expected.size) == 0;
}

void RecoveryManager::reset_tuple_meta(const std::string& table_name, const Rid& rid) {
    auto it = sm_manager_->fhs_.find(table_name);
    if (it == sm_manager_->fhs_.end() || !record_exists(table_name, rid)) {
        return;
    }

    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    it->second->set_tuple_meta(rid, meta);
}

void RecoveryManager::redo_insert(const InsertLogRecord& log) {
    auto table_it = sm_manager_->fhs_.find(log.table_name_);
    if (table_it == sm_manager_->fhs_.end()) {
        return;
    }
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = log.log_tid_;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};

    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        table_it->second->insert_record(log.rid_, log.insert_value_.data, log.lsn_);
    } else if (current->size != log.insert_value_.size ||
               memcmp(current->data, log.insert_value_.data, log.insert_value_.size) != 0) {
        table_it->second->apply_tuple_update(log.rid_, log.insert_value_.data, meta, log.lsn_);
    }
    if (table_it->second->is_record(log.rid_)) {
        // Always install the committed metadata, even when the tuple body was
        // already present. A page LSN can be ahead because it also contains a
        // loser operation that recovery must not use to skip this redo.
        table_it->second->set_tuple_meta(log.rid_, meta, log.lsn_);
    }
}

void RecoveryManager::redo_delete(const DeleteLogRecord& log) {
    auto table_it = sm_manager_->fhs_.find(log.table_name_);
    if (table_it == sm_manager_->fhs_.end()) {
        return;
    }
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = log.log_tid_;
    meta.is_committed_ = true;
    meta.is_deleted_ = true;
    meta.version_chain_head_ = UndoLink{};

    if (record_exists(log.table_name_, log.rid_)) {
        table_it->second->apply_tuple_update(log.rid_, log.delete_value_.data, meta, log.lsn_);
    } else {
        table_it->second->insert_record(log.rid_, log.delete_value_.data, log.lsn_);
    }
    if (table_it->second->is_record(log.rid_)) {
        table_it->second->set_tuple_meta(log.rid_, meta, log.lsn_);
    }
}

void RecoveryManager::redo_update(const UpdateLogRecord& log) {
    auto table_it = sm_manager_->fhs_.find(log.table_name_);
    if (table_it == sm_manager_->fhs_.end()) {
        return;
    }
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = log.log_tid_;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    if (current == nullptr) {
        table_it->second->insert_record(log.rid_, log.new_value_.data, log.lsn_);
    } else {
        table_it->second->apply_tuple_update(log.rid_, log.new_value_.data, meta, log.lsn_);
    }
}

void RecoveryManager::undo_insert(const InsertLogRecord& log) {
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务插入的值时才删除。
    // 内容比较无法区分「loser 未刷盘的 insert」与「committed 事务在同一 RID 复用并写入
    // 相同内容」（RID 复用 + d_next_o_id 回退后复位会导致两者完全相同），故必须用
    // TupleMeta.writer_txn_id_ 判断所有权：仅当 slot 仍归属本 loser 事务时才删除。
    auto table_it = sm_manager_->fhs_.find(log.table_name_);
    if (table_it == sm_manager_->fhs_.end() || !record_exists(log.table_name_, log.rid_)) {
        return;
    }
    TupleMeta meta = table_it->second->get_tuple_meta(log.rid_);
    if (meta.writer_txn_id_ != log.log_tid_) {
        return;
    }
    table_it->second->delete_record(log.rid_, nullptr, log.lsn_);
}

void RecoveryManager::undo_delete(const DeleteLogRecord& log) {
    // 幂等守卫：仅当该 slot 当前为空，或仍是本 loser 写下的 MVCC tombstone 时才恢复。
    // 若 slot 已被后续 committed 事务重新写入为 live tuple，跳过，避免覆盖 committed 数据。
    auto table_it = sm_manager_->fhs_.find(log.table_name_);
    if (table_it == sm_manager_->fhs_.end()) {
        return;
    }
    if (record_exists(log.table_name_, log.rid_)) {
        TupleMeta meta = table_it->second->get_tuple_meta(log.rid_);
        if (meta.is_deleted_ && meta.writer_txn_id_ == log.log_tid_) {
            TupleMeta restored_meta;
            restored_meta.commit_ts_ = 0;
            restored_meta.writer_txn_id_ = log.log_tid_;
            restored_meta.is_committed_ = false;
            restored_meta.is_deleted_ = false;
            restored_meta.version_chain_head_ = UndoLink{};
            table_it->second->apply_tuple_update(log.rid_, log.delete_value_.data, restored_meta, log.lsn_);
        }
        return;
    }
    table_it->second->insert_record(log.rid_, log.delete_value_.data, log.lsn_);
    TupleMeta restored_meta;
    restored_meta.commit_ts_ = 0;
    restored_meta.writer_txn_id_ = log.log_tid_;
    restored_meta.is_committed_ = false;
    restored_meta.is_deleted_ = false;
    restored_meta.version_chain_head_ = UndoLink{};
    table_it->second->set_tuple_meta(log.rid_, restored_meta, log.lsn_);
}

void RecoveryManager::undo_update(const UpdateLogRecord& log) {
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务写入的 new_value 时才回滚到 old_value。
    // 若 rid 已被后续 committed 事务覆盖为其他值，跳过，避免覆盖 committed 数据。
    auto table_it = sm_manager_->fhs_.find(log.table_name_);
    if (table_it == sm_manager_->fhs_.end() || !record_exists(log.table_name_, log.rid_)) {
        return;
    }
    TupleMeta meta = table_it->second->get_tuple_meta(log.rid_);
    if (meta.writer_txn_id_ != log.log_tid_) {
        return;
    }
    if (!record_equals(log.table_name_, log.rid_, log.new_value_)) {
        return;
    }
    TupleMeta restored_meta;
    restored_meta.commit_ts_ = 0;
    restored_meta.writer_txn_id_ = log.log_tid_;
    restored_meta.is_committed_ = false;
    restored_meta.is_deleted_ = false;
    restored_meta.version_chain_head_ = UndoLink{};
    table_it->second->apply_tuple_update(log.rid_, log.old_value_.data, restored_meta, log.lsn_);
}

void RecoveryManager::rebuild_indexes() {
    sm_manager_->rebuild_all_indexes();
}
