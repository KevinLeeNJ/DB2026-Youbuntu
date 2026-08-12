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
#include <chrono>
#include <cstring>
#include <limits>
#include <queue>
#include <string>
#include <sys/mman.h>
#include <unordered_set>
#include <vector>

#include "index/ix_index_handle.h"
#include "minilog.h"

uint16_t RecoveryManager::intern_table(std::string_view table_name) {
    table_name_scratch_.assign(table_name.data(), table_name.size());
    auto it = table_ids_.find(table_name_scratch_);
    if (it != table_ids_.end()) {
        return it->second;
    }

    RecoveryTable table;
    table.name = table_name_scratch_;
    auto file_it = sm_manager_->fhs_.find(table.name);
    if (file_it != sm_manager_->fhs_.end()) {
        table.file_handle = file_it->second.get();
        const RmFileHdr file_hdr = table.file_handle->get_file_hdr();
        table.records_per_page = file_hdr.num_records_per_page;
        table.record_size = file_hdr.record_size;
    }
    if (sm_manager_->db_.is_table(table.name)) {
        table.meta = &sm_manager_->db_.get_table(table.name);
    }
    const auto table_id = static_cast<uint16_t>(tables_.size());
    tables_.push_back(std::move(table));
    table_ids_.emplace(table_name_scratch_, table_id);
    return table_id;
}

int64_t RecoveryManager::offset_of_lsn(lsn_t lsn) const {
    if (lsn == INVALID_LSN) {
        return -1;
    }
    const auto it = std::lower_bound(record_locations_.begin(), record_locations_.end(), lsn,
                                     [](const WalRecordLocation& entry, lsn_t target) { return entry.lsn < target; });
    if (it == record_locations_.end() || it->lsn != lsn) {
        return -1;
    }
    return it->offset;
}

void RecoveryManager::build_touched_index() {
    touched_sorted_ = touched_;
    std::sort(touched_sorted_.begin(), touched_sorted_.end());
    touched_sorted_.erase(std::unique(touched_sorted_.begin(), touched_sorted_.end()), touched_sorted_.end());
}

WalRecordView RecoveryManager::mapped_heap_redo_record(const HeapRedoRecord& location, const char* wal_bytes) const {
    if (wal_bytes == nullptr || location.wal_offset < scan_begin_offset_ || location.wal_offset > scan_end_offset_ ||
        location.wal_length < static_cast<uint32_t>(LOG_HEADER_SIZE) ||
        static_cast<int64_t>(location.wal_length) > scan_end_offset_ - location.wal_offset) {
        throw InternalError("recovery heap-redo descriptor leaves the mapped WAL; WAL retained");
    }

    const char* header = wal_bytes + static_cast<size_t>(location.wal_offset);
    const uint32_t total_len = read_unaligned<uint32_t>(header + OFFSET_LOG_TOT_LEN);
    const LogType log_type = read_unaligned<LogType>(header + OFFSET_LOG_TYPE);
    const bool heap_dml = log_type == LogType::INSERT || log_type == LogType::DELETE || log_type == LogType::UPDATE;
    if (!heap_dml || total_len != location.wal_length) {
        throw InternalError("recovery heap-redo descriptor no longer names its analyzed WAL record at offset " +
                            std::to_string(location.wal_offset) + "; WAL retained");
    }

    WalRecordView record;
    record.log_type = log_type;
    record.lsn = read_unaligned<lsn_t>(header + OFFSET_LSN);
    record.total_len = total_len;
    record.txn_id = read_unaligned<txn_id_t>(header + OFFSET_LOG_TID);
    record.prev_lsn = read_unaligned<lsn_t>(header + OFFSET_PREV_LSN);
    record.offset = location.wal_offset;
    record.bytes = header;
    return record;
}

void RecoveryManager::validate_dml_rid(const RecoveryTable& table, const WalRecordView& record, const Rid& rid) const {
    // Page 0 holds the file header, never a record.
    if (rid.page_no < RM_FIRST_RECORD_PAGE) {
        throw InternalError("recovery found a WAL record naming page " + std::to_string(rid.page_no) + " of table " +
                            table.name + " at LSN " + std::to_string(record.lsn) + "; WAL retained");
    }
    // A table that is not open cannot supply its slot count. Such records are
    // skipped by every later phase, so the only thing still needed is that the
    // slot number survives being packed into TouchedTuple.
    const int slot_limit = table.records_per_page > 0 ? table.records_per_page : static_cast<int>(INT16_MAX) + 1;
    if (rid.slot_no < 0 || rid.slot_no >= slot_limit) {
        throw InternalError("recovery found a WAL record naming slot " + std::to_string(rid.slot_no) + " of table " +
                            table.name + ", which holds " + std::to_string(slot_limit) + " slots per page, at LSN " +
                            std::to_string(record.lsn) + "; WAL retained");
    }
}

void RecoveryManager::validate_touched_page_bounds() {
    // Every table's page numbers get an upper bound before any of them reaches
    // the record layer. A committed insert may legitimately name a page that
    // never made it to disk, so the file length alone is not the bound; but
    // each DML record can account for at most one such page, so the number of
    // DML records is. Without this, a corrupt page_no of 2^30 would send
    // RmFileHandle::insert_record into a billion create_new_page_handle calls.
    for (auto& table : tables_) {
        if (table.file_handle == nullptr) {
            continue;
        }
        const int64_t file_bytes =
            disk_manager_->get_file_size(disk_manager_->get_file_name(table.file_handle->GetFd()));
        const int64_t disk_pages = file_bytes > 0 ? file_bytes / PAGE_SIZE : 0;
        const int64_t header_pages = table.file_handle->get_file_hdr().num_pages;
        table.page_no_limit = static_cast<int32_t>(
            std::min<int64_t>(std::max(disk_pages, header_pages) + static_cast<int64_t>(touched_.size()) + 1,
                              std::numeric_limits<int32_t>::max()));
    }
    for (const auto& touched : touched_sorted_) {
        const RecoveryTable& table = tables_[touched.table_id];
        if (table.file_handle == nullptr) {
            continue;
        }
        if (touched.page_no >= table.page_no_limit) {
            throw InternalError("recovery found a WAL record naming page " + std::to_string(touched.page_no) +
                                " of table " + table.name + ", beyond the " + std::to_string(table.page_no_limit) +
                                " pages the WAL can account for; WAL retained");
        }
    }
}

void RecoveryManager::validate_installable_image(const RecoveryTable& table, const WalRecordView& record,
                                                 int image_size) const {
    if (image_size != table.record_size) {
        throw InternalError("recovery found a WAL image of " + std::to_string(image_size) + " bytes for table " +
                            table.name + ", whose records are " + std::to_string(table.record_size) +
                            " bytes, at LSN " + std::to_string(record.lsn) + "; WAL retained");
    }
}

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 *
 * Why this phase can tell a torn tail apart from real corruption, with no CRC
 * in the WAL at all:
 *
 *  1. DiskManager::write_log() is a single pwrite plus a short-write retry
 *     loop, always appending the log buffer's bytes in order. A pwrite that is
 *     cut short by process death has written a *prefix* of what was handed to
 *     it, never a hole and never bytes out of order. So the WAL file on disk is
 *     always a prefix of the byte stream the writer intended.
 *  2. Under the prefix property the only possible record-boundary anomaly is
 *     the very last record being cut short, and WalReader::next() catches
 *     exactly that with `next_offset + total_len > end_offset`.
 *  3. Therefore, if a record header passed the length and type checks, every
 *     byte of its payload is inside the file. A payload that then fails to
 *     parse is *not* a torn tail: it is real corruption. Treating it as a tail
 *     -- which is what this code used to do -- silently discards every
 *     committed record after it.
 *  4. LogManager::initialize_from_existing_log() already ran the identical
 *     header-level scan and truncated the file to the intact prefix, so when
 *     this phase starts, file_size is the end of a header-validated prefix. A
 *     second run of the same parser disagreeing with the first means
 *     corruption or a bug, never a tail.
 *
 * A CRC would buy nothing here. `final.md:349` fixes the crash model at SIGKILL
 * with a same-machine restart, not at whole-machine power loss, so the kernel
 * still owns every acknowledged byte and media-level rot is out of scope. The
 * prefix property already covers the only tearing that model produces, and a
 * CRC would cost a WAL header layout change plus every test that builds a WAL
 * by hand. If the crash model ever widens to power loss or to a WAL written
 * through O_DIRECT, points 1-3 above stop holding and a per-record checksum
 * becomes the right answer.
 */
void RecoveryManager::analyze() {
    active_txn_last_lsn_.clear();
    committed_txns_.clear();
    tables_.clear();
    table_ids_.clear();
    touched_.clear();
    touched_sorted_.clear();
    heap_redo_records_.clear();
    record_locations_.clear();
    record_locations_sorted_ = true;
    touched_tables_.clear();
    has_dml_records_ = false;
    has_index_smo_records_ = false;
    latest_index_bindings_.clear();
    max_lsn_ = INVALID_LSN;
    checkpoint_offset_ = 0;
    scan_begin_offset_ = 0;
    scan_end_offset_ = 0;
    scanned_record_count_ = 0;
    redo_applied_count_ = 0;
    redo_skipped_count_ = 0;
    redo_missing_table_count_ = 0;
    undo_applied_count_ = 0;
    pruned_no_undo_transaction_count_ = 0;
    undo_chain_record_read_count_ = 0;
    index_probe_count_ = 0;
    index_mutation_count_ = 0;
    index_unchanged_key_count_ = 0;
    index_duplicate_entry_count_ = 0;
    index_rebuild_count_ = 0;
    index_smo_prepare_count_ = 0;
    persisted_next_timestamp_ = 0;
    persisted_next_txn_id_ = 0;
    max_wal_commit_ts_ = INVALID_TS;
    max_wal_txn_id_ = INVALID_TXN_ID;

    // 重启清单先读：它是计数器恢复的第一个来源，而且必须在“WAL 为空所以无事可做”
    // 的提前返回之前读到——恰恰是刚做过 clean checkpoint（WAL 被截为空）的库最需要
    // 它。见 get_recovered_next_timestamp()。
    const RestartManifest manifest =
        log_manager_ != nullptr ? log_manager_->read_restart_manifest() : RestartManifest{};
    persisted_next_timestamp_ = manifest.next_timestamp;
    persisted_next_txn_id_ = manifest.next_txn_id;

    const int64_t file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return;
    }
    scan_end_offset_ = file_size;

    // Transactions restored from a checkpoint may already have undoable WAL
    // before scan_begin_offset_. They remain conservatively eligible for undo
    // unless a later COMMIT removes them. Transactions first seen as BEGIN in
    // this scan need undo only if one of the three DML WAL types follows.
    std::unordered_set<txn_id_t> checkpoint_active_txns;
    std::unordered_set<txn_id_t> txns_with_undoable_wal;

    // A published restart offset lets the scan start at the last checkpoint
    // instead of at the beginning of the file.
    {
        std::vector<char> scratch;
        WalRecordView checkpoint;
        const int64_t restart_offset = manifest.checkpoint_offset;
        if (restart_offset > 0 && ReadWalRecordAt(disk_manager_, restart_offset, file_size, &scratch, &checkpoint) &&
            checkpoint.log_type == LogType::CHECKPOINT) {
            auto record = DeserializeLogRecord(checkpoint.bytes, static_cast<int>(checkpoint.total_len));
            if (record != nullptr && record->log_type_ == LogType::CHECKPOINT) {
                scan_begin_offset_ = restart_offset;
                checkpoint_offset_ = restart_offset;
                const auto* checkpoint_log = static_cast<const CheckpointLogRecord*>(record.get());
                for (const auto& [txn_id, last_lsn] : checkpoint_log->active_txns_) {
                    active_txn_last_lsn_[txn_id] = last_lsn;
                    checkpoint_active_txns.insert(txn_id);
                }
            }
        }
    }

    WalReader reader(disk_manager_, scan_begin_offset_, file_size);
    WalRecordView record;
    WalDmlView dml;
    lsn_t previous_lsn = INVALID_LSN;
    while (reader.next(&record)) {
        ++scanned_record_count_;
        switch (record.log_type) {
        case LogType::BEGIN:
            active_txn_last_lsn_[record.txn_id] = record.lsn;
            break;
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE: {
            if (!ParseWalDml(record, &dml)) {
                // Point 3 of the comment on this function: the header already
                // proved the whole payload is inside the file, so this is
                // corruption, not the end of the log. Stopping here would drop
                // every committed record that follows.
                throw InternalError("recovery found a corrupt DML payload at WAL offset " +
                                    std::to_string(record.offset) + " (LSN " + std::to_string(record.lsn) +
                                    "); WAL retained");
            }
            active_txn_last_lsn_[record.txn_id] = record.lsn;
            txns_with_undoable_wal.insert(record.txn_id);
            has_dml_records_ = true;
            const uint16_t table_id = intern_table(dml.table_name);
            const RecoveryTable& table = tables_[table_id];
            // Before the narrowing cast below and before any later phase can
            // hand this RID to the record layer.
            validate_dml_rid(table, record, dml.rid);
            // Sparse UPDATE parsing materializes a complete before image, so
            // every later consumer keeps the same full-row contract. Check
            // that contract here, before redo, undo, or index-key extraction
            // can read a column offset from either image.
            if (table.file_handle != nullptr) {
                if (dml.before_image != nullptr) {
                    validate_installable_image(table, record, dml.before_size);
                }
                if (dml.after_image != nullptr) {
                    validate_installable_image(table, record, dml.after_size);
                }
            }
            touched_tables_.insert(table.name);
            TouchedTuple touched;
            touched.table_id = table_id;
            touched.page_no = dml.rid.page_no;
            touched.slot_no = static_cast<int16_t>(dml.rid.slot_no);
            touched_.push_back(touched);
            HeapRedoRecord redo_record;
            redo_record.wal_offset = record.offset;
            redo_record.wal_length = record.total_len;
            redo_record.table_id = table_id;
            redo_record.page_no = dml.rid.page_no;
            redo_record.slot_no = static_cast<int16_t>(dml.rid.slot_no);
            heap_redo_records_.push_back(redo_record);
            break;
        }
        case LogType::COMMIT: {
            committed_txns_.insert(record.txn_id);
            active_txn_last_lsn_.erase(record.txn_id);
            checkpoint_active_txns.erase(record.txn_id);
            txns_with_undoable_wal.erase(record.txn_id);
            // 8 字节载荷；旧 WAL 的 COMMIT 记录没有它，此时保持 INVALID_TS 并跳过。
            if (CommitLogRecord::HasCommitTs(record.total_len)) {
                const timestamp_t commit_ts = read_unaligned<timestamp_t>(record.bytes + OFFSET_LOG_DATA);
                if (commit_ts != INVALID_TS && (max_wal_commit_ts_ == INVALID_TS || commit_ts > max_wal_commit_ts_)) {
                    max_wal_commit_ts_ = commit_ts;
                }
            }
            break;
        }
        case LogType::ABORT:
            // ABORT only records that rollback was requested. This system does not write CLRs,
            // so recovery must still idempotently undo the transaction's original DML records.
            active_txn_last_lsn_[record.txn_id] = record.lsn;
            break;
        case LogType::CHECKPOINT:
            break;
        case LogType::INDEX_BIND: {
            std::string_view index_name;
            uint64_t generation = 0;
            if (!ParseIndexBindWal(record, &index_name, &generation)) {
                throw InternalError("recovery found a corrupt INDEX_BIND payload at WAL offset " +
                                    std::to_string(record.offset) + "; WAL retained");
            }
            latest_index_bindings_[std::string(index_name)] = generation;
            break;
        }
        case LogType::INDEX_SMO: {
            IndexSmoWalView smo;
            if (!ParseIndexSmoWal(record, &smo) || record.txn_id != INVALID_TXN_ID || record.prev_lsn != INVALID_LSN) {
                throw InternalError("recovery found a corrupt INDEX_SMO payload at WAL offset " +
                                    std::to_string(record.offset) + "; WAL retained");
            }
            has_index_smo_records_ = true;
            break;
        }
        }

        if (record.txn_id != INVALID_TXN_ID && (max_wal_txn_id_ == INVALID_TXN_ID || record.txn_id > max_wal_txn_id_)) {
            max_wal_txn_id_ = record.txn_id;
        }
        record_locations_.push_back(WalRecordLocation{record.lsn, record.offset});
        if (record.lsn != INVALID_LSN && previous_lsn != INVALID_LSN && record.lsn <= previous_lsn) {
            record_locations_sorted_ = false;
        }
        previous_lsn = record.lsn;
        if (record.lsn != INVALID_LSN && (max_lsn_ == INVALID_LSN || record.lsn > max_lsn_)) {
            max_lsn_ = record.lsn;
        }
    }
    if (reader.next_offset() != file_size) {
        // Point 4 of the comment on this function: the startup scan already
        // truncated the file to the end of the intact prefix using this exact
        // parser, so the two runs must agree. They do not, so either the file
        // was corrupted between the two scans or one of the two scans is buggy.
        // Either way, replaying only up to here would silently drop whatever
        // committed records lie beyond it.
        throw InternalError("recovery stopped at WAL offset " + std::to_string(reader.next_offset()) +
                            " but the file is " + std::to_string(file_size) +
                            " bytes; the startup scan should have truncated it to a record boundary; WAL retained");
    }

    // BEGIN, COMMIT, ABORT and CHECKPOINT carry no physical operation for undo.
    // A transaction first observed in this scan whose chain contains none of
    // UPDATE/INSERT/DELETE can therefore be dropped without following its
    // prev_lsn chain. Keep checkpoint-seeded transactions conservatively:
    // their undoable WAL may precede scan_begin_offset_.
    for (auto it = active_txn_last_lsn_.begin(); it != active_txn_last_lsn_.end();) {
        const txn_id_t txn_id = it->first;
        if (checkpoint_active_txns.count(txn_id) == 0 && txns_with_undoable_wal.count(txn_id) == 0) {
            it = active_txn_last_lsn_.erase(it);
            ++pruned_no_undo_transaction_count_;
        } else {
            ++it;
        }
    }
    scan_end_offset_ = file_size;

    if (!record_locations_sorted_) {
        // LSNs are handed out under the append latch, so the file is normally
        // already ordered. Sort defensively so the undo lookup stays valid.
        std::sort(record_locations_.begin(), record_locations_.end(),
                  [](const WalRecordLocation& left, const WalRecordLocation& right) { return left.lsn < right.lsn; });
    }
    build_touched_index();
    validate_touched_page_bounds();

    LOG_INFO("recovery analyze: %llu records, %llu dml, %zu distinct rids, %llu wal preads, loser txns %zu",
             static_cast<unsigned long long>(scanned_record_count_), static_cast<unsigned long long>(touched_.size()),
             touched_sorted_.size(), static_cast<unsigned long long>(reader.read_count()), active_txn_last_lsn_.size());
    // 这三个数一起解释了计数器为什么落在这个位置，否则“已提交的行为什么可见”只能靠猜。
    LOG_INFO("recovery timestamps: persisted next_timestamp %lld, max wal commit_ts %lld, seeding next_timestamp %lld, "
             "next_txn_id %lld",
             static_cast<long long>(persisted_next_timestamp_), static_cast<long long>(max_wal_commit_ts_),
             static_cast<long long>(get_recovered_next_timestamp()),
             static_cast<long long>(get_recovered_next_txn_id()));

    if (has_dml_records_) {
        // A crash may persist newly allocated record pages before the short
        // file header update reaches disk. Reconcile only pages named by WAL
        // before redo/undo fetches their RIDs.
        repair_touched_file_headers();
    }
}
