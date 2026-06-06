/* Copyright (c) 2023 Renmin University of China
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
    checkpoint_offset_ = 0;

    const int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return;
    }

    int scan_offset = 0;
    {
        std::ifstream restart_file(LogManager::RESTART_FILE_NAME);
        int restart_offset = 0;
        if (restart_file >> restart_offset) {
            if (restart_offset >= 0 && restart_offset + LOG_HEADER_SIZE <= file_size) {
                std::vector<char> header_buf(LOG_HEADER_SIZE);
                if (disk_manager_->read_log(header_buf.data(), LOG_HEADER_SIZE, restart_offset) == LOG_HEADER_SIZE) {
                    LogRecord header;
                    header.deserialize(header_buf.data());
                    if (header.log_type_ == LogType::CHECKPOINT && header.log_tot_len_ >= LOG_HEADER_SIZE &&
                        restart_offset + static_cast<int>(header.log_tot_len_) <= file_size) {
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

    int offset = scan_offset;
    while (offset + LOG_HEADER_SIZE <= file_size) {
        std::vector<char> header_buf(LOG_HEADER_SIZE);
        int header_bytes = disk_manager_->read_log(header_buf.data(), LOG_HEADER_SIZE, offset);
        if (header_bytes != LOG_HEADER_SIZE) {
            break;
        }

        LogRecord header;
        header.deserialize(header_buf.data());
        if (header.log_tot_len_ < LOG_HEADER_SIZE || offset + static_cast<int>(header.log_tot_len_) > file_size) {
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
            active_txn_last_lsn_.erase(txn_id);
            break;
        case LogType::CHECKPOINT:
            break;
        }

        log_order_.push_back(lsn);
        log_records_[lsn] = std::move(record);
        offset += static_cast<int>(header.log_tot_len_);
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
    sm_manager_->reset_all_tuple_meta_after_recovery();
    sm_manager_->flush_all_table_and_index_pages();
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
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current != nullptr && (current->size != log.insert_value_.size ||
                               memcmp(current->data, log.insert_value_.data, log.insert_value_.size) != 0)) {
        sm_manager_->update_record_with_indexes(log.table_name_, log.rid_, *current, log.insert_value_);
    }
    sm_manager_->insert_record_with_indexes(log.table_name_, log.rid_, log.insert_value_);
    reset_tuple_meta(log.table_name_, log.rid_);
}

void RecoveryManager::redo_delete(const DeleteLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        return;
    }
    sm_manager_->delete_record_with_indexes(log.table_name_, log.rid_, *current);
}

void RecoveryManager::redo_update(const UpdateLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        sm_manager_->insert_record_with_indexes(log.table_name_, log.rid_, log.new_value_);
    } else {
        sm_manager_->update_record_with_indexes(log.table_name_, log.rid_, *current, log.new_value_);
    }
    reset_tuple_meta(log.table_name_, log.rid_);
}

void RecoveryManager::undo_insert(const InsertLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        return;
    }
    sm_manager_->delete_record_with_indexes(log.table_name_, log.rid_, *current);
}

void RecoveryManager::undo_delete(const DeleteLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current != nullptr && (current->size != log.delete_value_.size ||
                               memcmp(current->data, log.delete_value_.data, log.delete_value_.size) != 0)) {
        sm_manager_->update_record_with_indexes(log.table_name_, log.rid_, *current, log.delete_value_);
    }
    sm_manager_->insert_record_with_indexes(log.table_name_, log.rid_, log.delete_value_);
    reset_tuple_meta(log.table_name_, log.rid_);
}

void RecoveryManager::undo_update(const UpdateLogRecord& log) {
    auto current = get_record_if_exists(log.table_name_, log.rid_);
    if (current == nullptr) {
        sm_manager_->insert_record_with_indexes(log.table_name_, log.rid_, log.old_value_);
    } else {
        sm_manager_->update_record_with_indexes(log.table_name_, log.rid_, *current, log.old_value_);
    }
    reset_tuple_meta(log.table_name_, log.rid_);
}
