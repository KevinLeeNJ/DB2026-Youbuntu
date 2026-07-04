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

#include <algorithm>
#include <fstream>
#include <vector>

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

        const lsn_t lsn = record->lsn_;
        const txn_id_t txn_id = record->log_tid_;
        switch (record->log_type_) {
        case LogType::BEGIN:
            active_txn_last_lsn_[txn_id] = lsn;
            break;
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE:
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
            redo_insert(*static_cast<InsertLogRecord*>(it->second.get()));
            break;
        case LogType::DELETE:
            redo_delete(*static_cast<DeleteLogRecord*>(it->second.get()));
            break;
        case LogType::UPDATE:
            redo_update(*static_cast<UpdateLogRecord*>(it->second.get()));
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
                undo_insert(*static_cast<InsertLogRecord*>(record));
                break;
            case LogType::DELETE:
                undo_delete(*static_cast<DeleteLogRecord*>(record));
                break;
            case LogType::UPDATE:
                undo_update(*static_cast<UpdateLogRecord*>(record));
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
    schema_manager_->reset_all_tuple_meta_after_recovery();
    rebuild_indexes();
    schema_manager_->flush_all_table_and_index_pages();
    // 表页与索引页已落盘后，截断日志文件并推进 global_lsn。
    // 这样已 undo 完毕的 loser 日志不再残留，避免下一次重启跨轮重复 undo
    // 同 RID 上的数据（尤其是 RID 复用且内容相同时，仅靠 undo 内容守卫无法区分）。
    if (log_manager_ != nullptr) {
        lsn_t next_lsn = (max_lsn_ == INVALID_LSN) ? 0 : max_lsn_ + 1;
        log_manager_->reset_log(next_lsn);
    }
}

bool RecoveryManager::record_exists(const std::string& table_name, const Rid& rid) const {
    auto* fh = schema_manager_->find_table_handle(table_name);
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

std::unique_ptr<RmRecord> RecoveryManager::get_record_if_exists(const std::string& table_name, const Rid& rid) const {
    if (!record_exists(table_name, rid)) {
        return nullptr;
    }
    try {
        return schema_manager_->get_table_handle(table_name)->get_record(rid, nullptr);
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
    auto* it = schema_manager_->find_table_handle(table_name);
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

void RecoveryManager::redo_insert(const InsertLogRecord& log) {
    schema_manager_->get_table_handle(log.table_name_)->insert_record(log.rid_, log.insert_value_.data);
    // 标记该 slot 由本 committed 事务重做写入，以便 undo_insert 能据此区分所有权：
    // 若某 loser 事务曾在同一 RID 插入过相同内容，内容比较无法区分，必须用 writer_txn_id 判断。
    auto* table_it = schema_manager_->find_table_handle(log.table_name_);
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

void RecoveryManager::redo_delete(const DeleteLogRecord& log) {
    auto* table_it = schema_manager_->find_table_handle(log.table_name_);
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

void RecoveryManager::redo_update(const UpdateLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        schema_manager_->get_table_handle(log.table_name_)->insert_record(log.rid_, log.new_value_.data);
    } else {
        schema_manager_->get_table_handle(log.table_name_)->update_record(log.rid_, log.new_value_.data, nullptr);
    }
    auto* table_it = schema_manager_->find_table_handle(log.table_name_);
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

void RecoveryManager::undo_insert(const InsertLogRecord& log) {
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务插入的值时才删除。
    // 内容比较无法区分「loser 未刷盘的 insert」与「committed 事务在同一 RID 复用并写入
    // 相同内容」（RID 复用 + d_next_o_id 回退后复位会导致两者完全相同），故必须用
    // TupleMeta.writer_txn_id_ 判断所有权：仅当 slot 仍归属本 loser 事务时才删除。
    auto* table_it = schema_manager_->find_table_handle(log.table_name_);
    if (table_it == nullptr || !table_it->is_record(log.rid_)) {
        return;
    }
    TupleMeta meta = table_it->get_tuple_meta(log.rid_);
    if (meta.writer_txn_id_ != log.log_tid_) {
        return;
    }
    table_it->delete_record(log.rid_, nullptr);
}

void RecoveryManager::undo_delete(const DeleteLogRecord& log) {
    // 幂等守卫：仅当该 slot 当前为空，或仍是本 loser 写下的 MVCC tombstone 时才恢复。
    // 若 slot 已被后续 committed 事务重新写入为 live tuple，跳过，避免覆盖 committed 数据。
    auto* table_it = schema_manager_->find_table_handle(log.table_name_);
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

void RecoveryManager::undo_update(const UpdateLogRecord& log) {
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务写入的 new_value 时才回滚到 old_value。
    // 若 rid 已被后续 committed 事务覆盖为其他值，跳过，避免覆盖 committed 数据。
    auto* table_it = schema_manager_->find_table_handle(log.table_name_);
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

void RecoveryManager::rebuild_indexes() {
    schema_manager_->rebuild_all_indexes();
}
