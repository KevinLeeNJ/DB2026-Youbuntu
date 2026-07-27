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
#include <cstring>
#include <unordered_set>
#include <vector>

#include "minilog.h"

namespace {

std::vector<char> MakeRecoveryIndexKey(const IndexMeta& index, const char* record_data) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (const auto& col : index.cols) {
        std::memcpy(key.data() + offset, record_data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

void DeleteRecoveryIndexEntry(SmManager* sm_manager, const std::string& table_name, const IndexMeta& index,
                              const RmRecord& record, const Rid& rid) {
    const auto index_name = sm_manager->get_ix_manager()->get_index_name(table_name, index.cols);
    auto index_it = sm_manager->ihs_.find(index_name);
    if (index_it == sm_manager->ihs_.end()) {
        return;
    }
    auto key = MakeRecoveryIndexKey(index, record.data);
    index_it->second->delete_entry(key.data(), rid, nullptr);
}

void InsertRecoveryIndexEntry(SmManager* sm_manager, const std::string& table_name, const IndexMeta& index,
                              const RmRecord& record, const Rid& rid) {
    const auto index_name = sm_manager->get_ix_manager()->get_index_name(table_name, index.cols);
    auto index_it = sm_manager->ihs_.find(index_name);
    if (index_it == sm_manager->ihs_.end()) {
        return;
    }
    auto key = MakeRecoveryIndexKey(index, record.data);
    std::vector<Rid> existing;
    if (index_it->second->get_value(key.data(), &existing, nullptr)) {
        for (const auto& existing_rid : existing) {
            if (existing_rid == rid) {
                return;
            }
        }
    }
    index_it->second->insert_entry(key.data(), rid, nullptr, true);
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
    touched_rids_.clear();
    touched_tables_.clear();
    has_dml_records_ = false;
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
            has_dml_records_ = true;
            if (const auto* insert = dynamic_cast<const InsertLogRecord*>(record.get())) {
                touched_rids_[insert->table_name_].push_back(insert->rid_);
                touched_tables_.insert(insert->table_name_);
            } else if (const auto* del = dynamic_cast<const DeleteLogRecord*>(record.get())) {
                touched_rids_[del->table_name_].push_back(del->rid_);
                touched_tables_.insert(del->table_name_);
            } else if (const auto* update = dynamic_cast<const UpdateLogRecord*>(record.get())) {
                touched_rids_[update->table_name_].push_back(update->rid_);
                touched_tables_.insert(update->table_name_);
            }
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

    if (has_dml_records_) {
        // A crash may persist newly allocated record pages before the short
        // file header update reaches disk. Reconcile only pages named by WAL
        // before redo/undo fetches their RIDs.
        repair_touched_file_headers();
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
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::DELETE:
            redo_delete(*static_cast<DeleteLogRecord*>(it->second.get()));
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::UPDATE:
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
    if (!has_dml_records_) {
        reset_wal_if_needed();
        return;
    }

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
                FaultInjector::Point("mid_recovery_undo");
                break;
            case LogType::DELETE:
                undo_delete(*static_cast<DeleteLogRecord*>(record));
                FaultInjector::Point("mid_recovery_undo");
                break;
            case LogType::UPDATE:
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
    reset_touched_tuple_meta();
    repair_touched_file_headers();
    // Repair only keys named by the WAL. Rebuilding every index from every heap
    // row makes recovery proportional to the whole database even when the
    // crash affected a handful of records. The repair is idempotent and
    // preserves the existing B+tree topology. A failure rebuilds only the
    // affected index; if that also fails, recovery stops with WAL retained.
    if (!touched_rids_.empty()) {
        FaultInjector::Point("mid_index_rebuild");
        repair_touched_indexes();
    }
    if (!sm_manager_->flush_recovery_pages(touched_tables_)) {
        // Recovery results are not durable. Keep the complete WAL and refuse
        // normal startup so the next process can retry recovery.
        throw InternalError("recovery page flush failed; WAL retained");
    }
    // 表页与索引页已落盘后，截断日志文件并推进 global_lsn。
    // 这样已 undo 完毕的 loser 日志不再残留，避免下一次重启跨轮重复 undo
    // 同 RID 上的数据（尤其是 RID 复用且内容相同时，仅靠 undo 内容守卫无法区分）。
    reset_wal_if_needed();
}

void RecoveryManager::repair_touched_file_headers() {
    for (const auto& [table_name, rids] : touched_rids_) {
        auto file_it = sm_manager_->fhs_.find(table_name);
        if (file_it == sm_manager_->fhs_.end()) {
            continue;
        }
        std::vector<page_id_t> touched_pages;
        touched_pages.reserve(rids.size());
        for (const auto& rid : rids) {
            touched_pages.push_back(rid.page_no);
        }
        file_it->second->repair_file_header_for_pages(touched_pages);
    }
}

void RecoveryManager::reset_touched_tuple_meta() {
    for (const auto& [table_name, rids] : touched_rids_) {
        auto table_it = sm_manager_->fhs_.find(table_name);
        if (table_it == sm_manager_->fhs_.end()) {
            continue;
        }
        std::unordered_set<int> touched_pages;
        for (const auto& rid : rids) {
            touched_pages.insert(rid.page_no);
        }
        for (const int page_no : touched_pages) {
            auto page_handle = table_it->second->fetch_page_handle(page_no);
            std::vector<int> live_slots;
            std::vector<int> deleted_slots;
            {
                std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (int slot_no = 0; slot_no < page_handle.file_hdr->num_records_per_page; ++slot_no) {
                    if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
                        continue;
                    }
                    if (page_handle.get_meta(slot_no).is_deleted_) {
                        deleted_slots.push_back(slot_no);
                    } else {
                        live_slots.push_back(slot_no);
                    }
                }
            }
            sm_manager_->get_bpm()->unpin_page(page_handle.page->get_page_id(), false);
            for (const int slot_no : deleted_slots) {
                table_it->second->delete_record(Rid{page_no, slot_no}, nullptr);
            }
            for (const int slot_no : live_slots) {
                reset_tuple_meta(table_name, Rid{page_no, slot_no});
            }
        }
    }
}

void RecoveryManager::repair_touched_indexes() {
    // Remove every old/new key mentioned by recovered DML for the affected
    // RID, then install exactly the final live tuple key. This is idempotent
    // and repairs both a missing index write and a stale index entry.
    std::unordered_set<std::string> indexes_to_rebuild;

    auto repair_index = [&](const std::string& table_name, const IndexMeta& index, const auto& repair) {
        const auto index_name = sm_manager_->get_ix_manager()->get_index_name(table_name, index.cols);
        if (indexes_to_rebuild.count(index_name) != 0) {
            return;
        }
        try {
            repair();
        } catch (const std::exception& error) {
            LOG_WARN("recovery found structurally inconsistent index %s: %s", index_name.c_str(), error.what());
            indexes_to_rebuild.insert(index_name);
        }
    };

    for (const auto lsn : log_order_) {
        auto it = log_records_.find(lsn);
        if (it == log_records_.end()) {
            continue;
        }
        const auto* record = it->second.get();
        switch (record->log_type_) {
        case LogType::INSERT: {
            const auto& log = *static_cast<const InsertLogRecord*>(record);
            if (sm_manager_->db_.is_table(log.table_name_)) {
                const auto& tab = sm_manager_->db_.get_table(log.table_name_);
                for (const auto& index : tab.indexes) {
                    repair_index(log.table_name_, index, [&] {
                        DeleteRecoveryIndexEntry(sm_manager_, log.table_name_, index, log.insert_value_, log.rid_);
                    });
                }
            }
            break;
        }
        case LogType::DELETE: {
            const auto& log = *static_cast<const DeleteLogRecord*>(record);
            if (sm_manager_->db_.is_table(log.table_name_)) {
                const auto& tab = sm_manager_->db_.get_table(log.table_name_);
                for (const auto& index : tab.indexes) {
                    repair_index(log.table_name_, index, [&] {
                        DeleteRecoveryIndexEntry(sm_manager_, log.table_name_, index, log.delete_value_, log.rid_);
                    });
                }
            }
            break;
        }
        case LogType::UPDATE: {
            const auto& log = *static_cast<const UpdateLogRecord*>(record);
            if (sm_manager_->db_.is_table(log.table_name_)) {
                const auto& tab = sm_manager_->db_.get_table(log.table_name_);
                for (const auto& index : tab.indexes) {
                    repair_index(log.table_name_, index, [&] {
                        DeleteRecoveryIndexEntry(sm_manager_, log.table_name_, index, log.old_value_, log.rid_);
                        DeleteRecoveryIndexEntry(sm_manager_, log.table_name_, index, log.new_value_, log.rid_);
                    });
                }
            }
            break;
        }
        default:
            break;
        }
    }

    for (const auto& [table_name, rids] : touched_rids_) {
        auto file_it = sm_manager_->fhs_.find(table_name);
        if (!sm_manager_->db_.is_table(table_name) || file_it == sm_manager_->fhs_.end()) {
            continue;
        }
        const auto& table = sm_manager_->db_.get_table(table_name);
        for (const auto& rid : rids) {
            if (!record_exists(table_name, rid)) {
                continue;
            }
            const auto meta = file_it->second->get_tuple_meta(rid);
            if (meta.is_deleted_) {
                continue;
            }
            auto record = file_it->second->get_record(rid, nullptr);
            for (const auto& index : table.indexes) {
                repair_index(table_name, index,
                             [&] { InsertRecoveryIndexEntry(sm_manager_, table_name, index, *record, rid); });
            }
        }
    }

    if (!indexes_to_rebuild.empty()) {
        sm_manager_->rebuild_indexes(indexes_to_rebuild);
    }
}

void RecoveryManager::reset_wal_if_needed() {
    if (log_manager_ == nullptr || max_lsn_ == INVALID_LSN) {
        return;
    }
    const lsn_t next_lsn = max_lsn_ + 1;
    FaultInjector::Point("before_recovery_wal_reset");
    log_manager_->reset_log(next_lsn);
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
        if (table_it->second->is_record(log.rid_)) {
            table_it->second->set_tuple_meta(log.rid_, meta, log.lsn_);
        }
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
