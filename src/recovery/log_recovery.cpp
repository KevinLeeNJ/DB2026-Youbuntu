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

namespace {

// Caps how many copies of one (key, rid) pair the repair will remove. The probe
// result says how many there are, so this only bounds the work if that result is
// itself absurd. A key can legitimately hold only a handful of duplicates.
constexpr int kMaxDuplicateDrain = 16;

// Number of repaired keys re-probed afterwards. A repair that did not take
// effect means the tree is not in a state this repair can fix, and the index
// has to be rebuilt.
constexpr size_t kRepairSpotCheckLimit = 64;

// Keep the redo WAL view bounded. Heap redo records are sorted by table/page,
// so their WAL offsets are not necessarily sequential; retaining the whole WAL
// mapping would make every touched file page resident during recovery. A
// single 64 MiB window is enough for normal records, while the bounded scratch
// path below handles the (rare) record that straddles a window boundary.
constexpr size_t kWalMappingWindowBytes = size_t{64} * 1024 * 1024;
constexpr uint32_t kMaxRedoRecordBytes = MAX_INDEX_SMO_RECORD_BYTES;

class ReadOnlyWalMapping {
public:
    ReadOnlyWalMapping(DiskManager* disk_manager, int fd, int64_t length) : disk_manager_(disk_manager), fd_(fd) {
        if (fd < 0 || length <= 0 ||
            static_cast<uint64_t>(length) > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            throw InternalError("recovery cannot read an invalid WAL range");
        }
        length_ = length;
    }

    ~ReadOnlyWalMapping() {
        unmap();
    }

    ReadOnlyWalMapping(const ReadOnlyWalMapping&) = delete;
    ReadOnlyWalMapping& operator=(const ReadOnlyWalMapping&) = delete;

    const char* record_bytes(int64_t offset, uint32_t length, std::vector<char>* scratch) {
        const int64_t record_length = static_cast<int64_t>(length);
        if (offset < 0 || length < static_cast<uint32_t>(LOG_HEADER_SIZE) || length > kMaxRedoRecordBytes ||
            offset > length_ - record_length) {
            throw InternalError("recovery heap-redo descriptor leaves the WAL range; WAL retained");
        }
        const int64_t record_end = offset + record_length;
        if (mapped_bytes_ != nullptr && offset >= mapped_offset_ &&
            record_end <= mapped_offset_ + static_cast<int64_t>(mapped_length_)) {
            return mapped_bytes_ + (offset - mapped_offset_);
        }

        const int64_t window_offset =
            (offset / static_cast<int64_t>(kWalMappingWindowBytes)) * static_cast<int64_t>(kWalMappingWindowBytes);
        const int64_t window_end = std::min<int64_t>(length_, window_offset + kWalMappingWindowBytes);
        if (record_end <= window_end) {
            map_window(window_offset, window_end - window_offset);
            return mapped_bytes_ + (offset - mapped_offset_);
        }

        // A WAL record is bounded by kMaxRedoRecordBytes, so this allocation is
        // bounded and only occurs at a window boundary. The record header and
        // length are revalidated by mapped_heap_redo_record() after copying.
        if (scratch == nullptr || disk_manager_ == nullptr) {
            throw InternalError("recovery cannot buffer a WAL record crossing its mapping window; WAL retained");
        }
        scratch->resize(length);
        if (disk_manager_->read_log_chunk(scratch->data(), static_cast<int>(length), offset) !=
            static_cast<int>(length)) {
            throw InternalError("recovery could not read a WAL record crossing its mapping window; WAL retained");
        }
        return scratch->data();
    }

private:
    void map_window(int64_t offset, int64_t length) {
        unmap();
        mapped_length_ = static_cast<size_t>(length);
        void* mapped = mmap(nullptr, mapped_length_, PROT_READ, MAP_PRIVATE, fd_, offset);
        if (mapped == MAP_FAILED) {
            throw UnixError();
        }
        mapped_bytes_ = static_cast<const char*>(mapped);
        mapped_offset_ = offset;
    }

    void unmap() {
        if (mapped_bytes_ != nullptr) {
            (void)munmap(const_cast<char*>(mapped_bytes_), mapped_length_);
            mapped_bytes_ = nullptr;
            mapped_offset_ = 0;
            mapped_length_ = 0;
        }
    }

    DiskManager* disk_manager_{nullptr};
    int fd_{-1};
    int64_t length_{0};
    const char* mapped_bytes_{nullptr};
    int64_t mapped_offset_{0};
    size_t mapped_length_{0};
};

TupleMeta MakeCommittedMeta(txn_id_t writer) {
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = writer;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    return meta;
}

TupleMeta MakeLoserMeta(txn_id_t writer) {
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = writer;
    meta.is_committed_ = false;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    return meta;
}

void BuildIndexKey(const IndexMeta& index, const char* record_data, char* key_out) {
    int offset = 0;
    for (const auto& col : index.cols) {
        std::memcpy(key_out + offset, record_data + col.offset, col.len);
        offset += col.len;
    }
}

} // namespace

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

WalRecordView RecoveryManager::mapped_heap_redo_record(const HeapRedoRecord& location, const char* record_bytes) const {
    if (record_bytes == nullptr || location.wal_offset < scan_begin_offset_ || location.wal_offset > scan_end_offset_ ||
        location.wal_length < static_cast<uint32_t>(LOG_HEADER_SIZE) ||
        static_cast<int64_t>(location.wal_length) > scan_end_offset_ - location.wal_offset) {
        throw InternalError("recovery heap-redo descriptor leaves the WAL range; WAL retained");
    }

    const char* header = record_bytes;
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
    deferred_committed_deltas_.clear();
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
                if (dml.update_delta.present()) {
                    validate_installable_image(table, record, static_cast<int>(dml.update_delta.row_size));
                }
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

/**
 * @description: 重做所有未落盘的操作
 *
 * No readahead is issued here, deliberately. A previous version handed the
 * pages of the next batch of records to posix_fadvise(WILLNEED) to lift the
 * queue depth above one. Measurement showed it could not help and did not: two
 * runs of the identical crash state issued the identical 421,528 page reads in
 * 2,852 ms and 15,445 ms, i.e. 6.8 and 36.6 microseconds per page read. A cold
 * NVMe random 4 KiB read at QD=1 costs 80-120 microseconds, so neither figure
 * can contain a real device round trip, and a 5.4x wall-clock spread at a fixed
 * read count says the phase is bound by CPU and memory bandwidth, not by queue
 * depth. The reason is that the buffer pool opens data files with a plain
 * open(O_RDWR): every read_page is a copy out of the page cache, and the
 * benchmark's POSIX_FADV_DONTNEED is advisory, so on a 30 GB machine an 820 MB
 * working set is simply never evicted.
 *
 * This is not just a benchmark artefact. `final.md:349` fixes the crash model at
 * SIGKILL with a same-machine restart, and SIGKILL does not drop the page cache.
 * After a 450-second workload, killing and restarting the server leaves the
 * whole hot working set in the kernel's cache, so recovery's page reads are
 * mostly cache hits on the graded machine too. The 80-100 microseconds/page
 * cold-cache figure in PLAN.md's budget model therefore probably overestimates
 * this phase by two orders of magnitude. Anything aimed at recovery's page reads
 * should be aimed at *how many* there are, not at their latency -- which is what
 * the next paragraph is about.
 *
 * Heap DML is replayed in (table, page, WAL-offset) order. There is no
 * cross-page dependency: record operations touch one RID, while file-header
 * repair and tuple-meta normalization are separate serial phases. WAL offset
 * preserves the original order of every operation that targets the same page.
 * Existing redo helpers still own pin/latch/free-list handling; grouping only
 * keeps the current page resident instead of repeatedly evicting and re-reading
 * it. INDEX_BIND and INDEX_SMO retain their original forward WAL order.
 */
void RecoveryManager::redo() {
    if (!has_dml_records_ && !has_index_smo_records_) {
        return;
    }

    WalReader reader(disk_manager_, scan_begin_offset_, scan_end_offset_);
    WalRecordView record;
    std::unordered_set<int> smo_fds;
    // INDEX_SMO full-page images must retain WAL order. INDEX_BIND has no
    // physical redo action; analyze() already retained the latest generation.
    while (reader.next(&record)) {
        if (record.log_type == LogType::INDEX_BIND) {
            continue;
        }
        if (record.log_type == LogType::INDEX_SMO) {
            IndexSmoWalView smo;
            if (!ParseIndexSmoWal(record, &smo)) {
                throw InternalError("recovery failed to re-parse INDEX_SMO at WAL offset " +
                                    std::to_string(record.offset) + "; WAL retained");
            }
            auto binding = latest_index_bindings_.find(std::string(smo.index_file_name));
            if (binding == latest_index_bindings_.end() || binding->second != smo.index_generation) {
                continue;
            }
            auto open_index = sm_manager_->ihs_.find(std::string(smo.index_file_name));
            if (open_index == sm_manager_->ihs_.end() || open_index->second == nullptr) {
                continue;
            }
            IxIndexHandle* index = open_index->second.get();
            const int index_fd = index->GetFd();
            // Dropping this fd's cached pages scans the whole buffer pool. Once
            // is enough: all SMO after-images below bypass the buffer pool and
            // are replayed in WAL order, so repeating the scan per record is
            // both redundant and quadratic in WAL records times pool frames.
            if (smo_fds.insert(index_fd).second) {
                index->prepare_for_smo_redo();
                ++index_smo_prepare_count_;
            }
            for (uint32_t page = 0; page < smo.page_count; ++page) {
                disk_manager_->write_page(index_fd, smo.page_no(page), smo.page_image(page), PAGE_SIZE);
            }
            // The header is the publication point and must always be written
            // after every node after-image.
            disk_manager_->write_page(index_fd, IX_FILE_HDR_PAGE, smo.header_image, PAGE_SIZE);
            index->install_recovered_smo_header(smo.header_image);
            continue;
        }
    }
    if (reader.next_offset() != scan_end_offset_) {
        throw InternalError("recovery INDEX_SMO pass stopped before the analyzed WAL end; WAL retained");
    }

    std::sort(heap_redo_records_.begin(), heap_redo_records_.end());
    std::unique_ptr<ReadOnlyWalMapping> wal_mapping;
    if (!heap_redo_records_.empty()) {
        wal_mapping = std::make_unique<ReadOnlyWalMapping>(disk_manager_, disk_manager_->GetLogFd(), scan_end_offset_);
    }
    std::vector<char> wal_scratch;
    WalDmlView dml;
    for (const HeapRedoRecord& location : heap_redo_records_) {
        const char* bytes = wal_mapping->record_bytes(location.wal_offset, location.wal_length, &wal_scratch);
        record = mapped_heap_redo_record(location, bytes);
        if (!ParseWalDmlForRedo(record, &dml)) {
            throw InternalError("recovery failed to parse mapped DML at WAL offset " + std::to_string(record.offset) +
                                " that analyze accepted; WAL retained");
        }
        if (location.table_id >= tables_.size()) {
            throw InternalError("recovery heap-redo descriptor has an invalid table id; WAL retained");
        }
        RecoveryTable* table = table_at(location.table_id);
        if (dml.table_name != table->name || dml.rid.page_no != location.page_no ||
            dml.rid.slot_no != location.slot_no) {
            throw InternalError("recovery mapped DML target disagrees with analyze at WAL offset " +
                                std::to_string(record.offset) + "; WAL retained");
        }
        if (committed_txns_.count(record.txn_id) == 0) {
            ++redo_skipped_count_; // a loser: undo() rolls it back instead
            continue;
        }
        if (table->file_handle == nullptr) {
            // The table is no longer open, so there is nothing to replay into.
            // Counted so that applied + skipped covers every DML record.
            ++redo_skipped_count_;
            ++redo_missing_table_count_;
            continue;
        }
        ++redo_applied_count_;
        const TouchedTuple target{location.table_id, location.slot_no, location.page_no};
        switch (record.log_type) {
        case LogType::INSERT:
            redo_insert(record, dml, *table);
            deferred_committed_deltas_.erase(target);
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::DELETE:
            redo_delete(record, dml, *table);
            deferred_committed_deltas_.erase(target);
            FaultInjector::Point("mid_recovery_redo");
            break;
        case LogType::UPDATE:
            if (!redo_update(record, dml, *table)) {
                deferred_committed_deltas_[target].push_back(location);
            } else if (!dml.update_delta.present()) {
                deferred_committed_deltas_.erase(target);
            }
            FaultInjector::Point("mid_recovery_redo");
            break;
        default:
            break;
        }
    }
    // No later recovery phase consults heap_redo_records_; undo follows the
    // LSN index and touched_sorted_ owns the page-repair work.
    std::vector<HeapRedoRecord>().swap(heap_redo_records_);
    for (int fd : smo_fds) {
        disk_manager_->sync_file(fd);
    }
    LOG_INFO("recovery redo: applied %llu, skipped %llu (%llu with no open table), %llu dml records, wal preads %llu",
             static_cast<unsigned long long>(redo_applied_count_), static_cast<unsigned long long>(redo_skipped_count_),
             static_cast<unsigned long long>(redo_missing_table_count_),
             static_cast<unsigned long long>(touched_.size()), static_cast<unsigned long long>(reader.read_count()));
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    if (!has_dml_records_) {
        LOG_INFO("recovery undo: 0 records, 0 loser txns, 0 chain records read, %llu no-undo txns pruned",
                 static_cast<unsigned long long>(pruned_no_undo_transaction_count_));
        reset_wal_if_needed();
        return;
    }

    // Undo every loser in one merged pass ordered by descending LSN, which is
    // the exact reverse of the order the records were written. Undoing whole
    // transactions one after another would let an older transaction's rollback
    // run before a newer transaction's write on the same slot is undone.
    //
    // Every record on a retained chain is fetched with its own ReadWalRecordAt
    // (header and body preads). analyze() has already removed transactions with
    // no undoable WAL, so high-frequency conflicts that wrote only BEGIN do not
    // expand this random-read phase. The remaining work is proportional to
    // DML-bearing losers, including an ABORT whose physical undo was interrupted.
    std::priority_queue<lsn_t> pending;
    for (const auto& [txn_id, last_lsn] : active_txn_last_lsn_) {
        (void)txn_id;
        if (last_lsn != INVALID_LSN) {
            pending.push(last_lsn);
        }
    }

    std::vector<char> scratch;
    WalRecordView record;
    WalDmlView dml;
    while (!pending.empty()) {
        const lsn_t current_lsn = pending.top();
        while (!pending.empty() && pending.top() == current_lsn) {
            pending.pop();
        }

        // Each of the three failures below used to `continue`, which broke that
        // loser's whole prev_lsn chain and left the rest of its writes rolled
        // forward, with no exception and nothing in the log. None of them is
        // reachable today: write_restart_offset() always writes 0, so
        // scan_begin_offset_ is always 0 and record_locations_ holds every LSN
        // in the file. They become reachable the day fuzzy checkpointing lands,
        // because a restart offset that is not <= the first LSN of every live
        // transaction turns a missing lookup into a partially rolled-back
        // transaction -- a direct violation of `final.md:342` clause 2, and one
        // that leaves no trace. Failing the recovery instead makes the next
        // process retry from the complete WAL.
        const int64_t offset = offset_of_lsn(current_lsn);
        if (offset < 0) {
            throw InternalError("recovery could not locate WAL LSN " + std::to_string(current_lsn) +
                                " on a loser's prev_lsn chain; the scan started at offset " +
                                std::to_string(scan_begin_offset_) + "; WAL retained");
        }
        if (!ReadWalRecordAt(disk_manager_, offset, scan_end_offset_, &scratch, &record)) {
            throw InternalError("recovery could not re-read the WAL record for LSN " + std::to_string(current_lsn) +
                                " at offset " + std::to_string(offset) + "; WAL retained");
        }
        ++undo_chain_record_read_count_;
        if (record.log_type == LogType::BEGIN) {
            continue;
        }
        if (record.log_type == LogType::INSERT || record.log_type == LogType::DELETE ||
            record.log_type == LogType::UPDATE) {
            if (!ParseWalDml(record, &dml)) {
                throw InternalError("recovery failed to re-parse the DML payload for LSN " +
                                    std::to_string(current_lsn) + " that analyze accepted; WAL retained");
            }
            const uint16_t table_id = intern_table(dml.table_name);
            RecoveryTable& table = tables_[table_id];
            if (table.file_handle != nullptr) {
                ++undo_applied_count_;
                switch (record.log_type) {
                case LogType::INSERT:
                    undo_insert(record, dml, table);
                    FaultInjector::Point("mid_recovery_undo");
                    break;
                case LogType::DELETE:
                    undo_delete(record, dml, table);
                    FaultInjector::Point("mid_recovery_undo");
                    break;
                default:
                    undo_update(record, dml, table);
                    FaultInjector::Point("mid_recovery_undo");
                    break;
                }
            }
        }
        if (record.prev_lsn != INVALID_LSN) {
            pending.push(record.prev_lsn);
        }
    }
    replay_deferred_committed_deltas();
    LOG_INFO("recovery undo: %llu records, %llu loser txns, %llu chain records read, %llu no-undo txns pruned",
             static_cast<unsigned long long>(undo_applied_count_),
             static_cast<unsigned long long>(active_txn_last_lsn_.size()),
             static_cast<unsigned long long>(undo_chain_record_read_count_),
             static_cast<unsigned long long>(pruned_no_undo_transaction_count_));

    // Headers first: reset_touched_tuple_meta() skips any page at or beyond
    // num_pages, so a touched page still outside the header's page count would
    // keep a loser's writer_txn_id_ and is_committed_=false on disk forever.
    // The window is not constructible today, but the order costs nothing.
    repair_touched_file_headers();
    reset_touched_tuple_meta();
    // Repair only keys named by the WAL. Rebuilding every index from every heap
    // row makes recovery proportional to the whole database even when the
    // crash affected a handful of records. The repair is idempotent and
    // preserves the existing B+tree topology. A failure rebuilds only the
    // affected index; if that also fails, recovery stops with WAL retained.
    if (!touched_sorted_.empty()) {
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
    std::vector<page_id_t> touched_pages;
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        size_t end = begin;
        touched_pages.clear();
        int32_t previous_page = -1;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            if (touched_sorted_[end].page_no != previous_page) {
                previous_page = touched_sorted_[end].page_no;
                touched_pages.push_back(previous_page);
            }
            ++end;
        }
        RmFileHandle* file_handle = tables_[table_id].file_handle;
        if (file_handle != nullptr) {
            file_handle->repair_file_header_for_pages(touched_pages);
        }
        begin = end;
    }
}

void RecoveryManager::reset_touched_tuple_meta() {
    // Every surviving tuple on a page the WAL touched is committed once redo
    // and undo are done, so the whole page's metadata is normalized inside the
    // page latch we already hold. Reaching back through set_tuple_meta once
    // per slot cost two extra buffer-pool round trips per slot.
    std::vector<int> deleted_slots;
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        RmFileHandle* file_handle = tables_[table_id].file_handle;
        size_t end = begin;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            ++end;
        }
        if (file_handle == nullptr) {
            begin = end;
            continue;
        }

        int32_t previous_page = -1;
        for (size_t i = begin; i < end; ++i) {
            const int32_t page_no = touched_sorted_[i].page_no;
            if (page_no == previous_page) {
                continue;
            }
            previous_page = page_no;
            if (page_no < 0 || page_no >= file_handle->get_file_hdr().num_pages) {
                continue;
            }

            deleted_slots.clear();
            RmPageHandle page_handle = file_handle->fetch_page_handle(page_no);
            bool dirtied = false;
            {
                std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (int slot_no = 0; slot_no < page_handle.file_hdr->num_records_per_page; ++slot_no) {
                    if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
                        continue;
                    }
                    TupleMeta& meta = page_handle.get_meta(slot_no);
                    if (meta.is_deleted_) {
                        deleted_slots.push_back(slot_no);
                        continue;
                    }
                    meta = MakeCommittedMeta(INVALID_TXN_ID);
                    dirtied = true;
                }
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), dirtied);
            // Removing the tombstones has to go through delete_record: it also
            // maintains the page's record count and the file's free list.
            for (const int slot_no : deleted_slots) {
                file_handle->delete_record(Rid{page_no, slot_no}, nullptr);
            }
        }
        begin = end;
    }
}

void RecoveryManager::collect_wal_index_keys() {
    // Every image the WAL mentions is a candidate stale entry: without index
    // page LSNs recovery cannot tell whether the matching index write reached
    // disk, so each one has to be reconciled against the tree.
    WalReader reader(disk_manager_, scan_begin_offset_, scan_end_offset_);
    WalRecordView record;
    WalDmlView dml;
    while (reader.next(&record)) {
        switch (record.log_type) {
        case LogType::INSERT:
        case LogType::DELETE:
        case LogType::UPDATE:
            break;
        default:
            continue;
        }
        if (!ParseWalDml(record, &dml)) {
            throw InternalError("recovery failed to re-parse the DML payload at WAL offset " +
                                std::to_string(record.offset) + " that analyze accepted; WAL retained");
        }
        // This encoding is emitted only when the UPDATE plan proves that no
        // indexed column can change. The final heap sweep below still supplies
        // the surviving key; there is no historical key for this record to
        // remove from an index.
        if (dml.update_delta.present()) {
            continue;
        }
        // index_plans was resolved once per table; this loop runs once per WAL
        // record, and rebuilding the index name here used to cost a string
        // concatenation plus a map lookup per record per index.
        const RecoveryTable& table = tables_[intern_table(dml.table_name)];
        for (IndexRepairPlan* plan : table.index_plans) {
            for (const char* image : {dml.before_image, dml.after_image}) {
                if (image == nullptr) {
                    continue;
                }
                const auto key_slot = static_cast<uint32_t>(plan->key_arena.size() / plan->key_len);
                plan->key_arena.resize(plan->key_arena.size() + static_cast<size_t>(plan->key_len));
                BuildIndexKey(*plan->index_meta, image,
                              plan->key_arena.data() + static_cast<size_t>(key_slot) * plan->key_len);
                plan->entries.push_back(IndexRepairEntry{key_slot, dml.rid, false});
            }
        }
    }
}

void RecoveryManager::collect_heap_index_keys() {
    // Read the final tuple of every touched RID one page at a time. The RIDs
    // are already ordered by page, so this is a sequential sweep and the keys
    // of every index on the table come out of the same page pin.
    for (size_t begin = 0; begin < touched_sorted_.size();) {
        const uint16_t table_id = touched_sorted_[begin].table_id;
        size_t end = begin;
        while (end < touched_sorted_.size() && touched_sorted_[end].table_id == table_id) {
            ++end;
        }
        RecoveryTable& table = tables_[table_id];
        if (table.file_handle == nullptr || table.index_plans.empty()) {
            begin = end;
            continue;
        }
        const auto& table_plans = table.index_plans;

        const int num_pages = table.file_handle->get_file_hdr().num_pages;
        for (size_t i = begin; i < end;) {
            const int32_t page_no = touched_sorted_[i].page_no;
            size_t page_end = i;
            while (page_end < end && touched_sorted_[page_end].page_no == page_no) {
                ++page_end;
            }
            if (page_no < 0 || page_no >= num_pages) {
                i = page_end;
                continue;
            }

            RmPageHandle page_handle = table.file_handle->fetch_page_handle(page_no);
            {
                std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (size_t j = i; j < page_end; ++j) {
                    const int slot_no = touched_sorted_[j].slot_no;
                    if (slot_no < 0 || slot_no >= page_handle.file_hdr->num_records_per_page) {
                        continue;
                    }
                    if (!Bitmap::is_set(page_handle.bitmap, slot_no) || page_handle.get_meta(slot_no).is_deleted_) {
                        continue;
                    }
                    const char* row = page_handle.get_slot(slot_no);
                    const Rid rid{page_no, slot_no};
                    for (IndexRepairPlan* plan : table_plans) {
                        const auto key_slot = static_cast<uint32_t>(plan->key_arena.size() / plan->key_len);
                        plan->key_arena.resize(plan->key_arena.size() + static_cast<size_t>(plan->key_len));
                        BuildIndexKey(*plan->index_meta, row,
                                      plan->key_arena.data() + static_cast<size_t>(key_slot) * plan->key_len);
                        plan->entries.push_back(IndexRepairEntry{key_slot, rid, true});
                    }
                }
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            i = page_end;
        }
        begin = end;
    }
}

void RecoveryManager::plan_touched_indexes(std::map<std::string, IndexRepairPlan>* plans) {
    // One plan per index that a touched table owns and that is actually open.
    // Every entry of tables_ was interned from a DML record, so iterating it
    // visits each touched table exactly once; the previous version iterated
    // touched_sorted_ instead and rebuilt every index name once per touched RID.
    for (const auto& table : tables_) {
        if (table.meta == nullptr) {
            continue;
        }
        for (const auto& index_meta : table.meta->indexes) {
            auto index_name = sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols);
            auto handle_it = sm_manager_->ihs_.find(index_name);
            if (handle_it == sm_manager_->ihs_.end() || plans->count(index_name) != 0) {
                continue;
            }
            IndexRepairPlan plan;
            plan.index_name = index_name;
            plan.index_meta = &index_meta;
            plan.index = handle_it->second.get();
            plan.key_len = index_meta.col_tot_len;
            plans->emplace(std::move(index_name), std::move(plan));
        }
    }
}

void RecoveryManager::bind_index_plans(std::map<std::string, IndexRepairPlan>* plans) {
    // Once per table per index, so the two collectors below can walk pointers.
    // plans is not mutated after this, so the addresses stay valid; the plans are
    // unbound again before anything can invalidate them.
    for (auto& table : tables_) {
        table.index_plans.clear();
        if (table.meta == nullptr) {
            continue;
        }
        for (const auto& index_meta : table.meta->indexes) {
            auto plan_it = plans->find(sm_manager_->get_ix_manager()->get_index_name(table.name, index_meta.cols));
            if (plan_it != plans->end()) {
                table.index_plans.push_back(&plan_it->second);
            }
        }
    }
}

void RecoveryManager::collect_index_repair_keys(std::map<std::string, IndexRepairPlan>* plans) {
    if (plans->empty()) {
        return;
    }
    bind_index_plans(plans);
    collect_wal_index_keys();
    collect_heap_index_keys();
}

void RecoveryManager::sort_index_repair_entries(IndexRepairPlan* plan) {
    IxIndexHandle* index = plan->index;
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();
    const auto key_of = [arena, key_len](const IndexRepairEntry& entry) {
        return arena + static_cast<size_t>(entry.key_slot) * key_len;
    };

    // Group by key in B+tree order so the leaves are visited left to right and
    // the internal nodes stay hot, then let each group make one decision. Both
    // the structure gate and the repair below consume this order, and the gate
    // depends on it for its "the previous descent already covers this key" skip,
    // so it is established once, here, before either of them runs.
    std::sort(
        plan->entries.begin(), plan->entries.end(), [&](const IndexRepairEntry& left, const IndexRepairEntry& right) {
            const int cmp = ix_compare(key_of(left), key_of(right), index->get_col_types(), index->get_col_lens());
            if (cmp != 0) {
                return cmp < 0;
            }
            if (!(left.rid == right.rid)) {
                return left.rid.page_no != right.rid.page_no ? left.rid.page_no < right.rid.page_no
                                                             : left.rid.slot_no < right.rid.slot_no;
            }
            return static_cast<int>(left.from_heap) < static_cast<int>(right.from_heap);
        });
}

bool RecoveryManager::gate_index_change_set(IndexRepairPlan* plan, RecoveryIndexGate::Stats* totals) {
    IxIndexHandle* index = plan->index;
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();

    bool valid = true;
    bool gate_declined = false;
    {
        // Recovery is single threaded and runs before the listener opens, so the
        // latch is documentation rather than mutual exclusion - but the gate does
        // write to index pages, and taking the same latch every writer takes keeps
        // that from becoming an exception to the rule. It is released before
        // validate_structure() runs below: index_latch_ is a plain shared_mutex,
        // so re-entering it for a shared hold would deadlock.
        auto structure_guard = index->lock_exclusive();

        RecoveryIndexGate gate(disk_manager_, buffer_pool_manager_, index, plan->index_name);
        const char* previous_key = nullptr;
        for (const IndexRepairEntry& entry : plan->entries) {
            const char* key = arena + static_cast<size_t>(entry.key_slot) * key_len;
            // plan->entries holds one element per (key, rid, source); the gate
            // only cares about distinct keys, and they arrive grouped by sort
            // order.
            if (previous_key != nullptr &&
                ix_compare(previous_key, key, index->get_col_types(), index->get_col_lens()) == 0) {
                continue;
            }
            previous_key = key;
            if (!gate.check_key(key)) {
                valid = false;
                break;
            }
        }

        const RecoveryIndexGate::Stats& stats = gate.stats();
        totals->descents += stats.descents;
        totals->keys_covered += stats.keys_covered;
        totals->pages_validated += stats.pages_validated;
        totals->page_fetches += stats.page_fetches;
        totals->parent_pointers_repaired += stats.parent_pointers_repaired;
        totals->chain_bounds_unknown += stats.chain_bounds_unknown;
        index_parent_pointer_repair_count_ += stats.parent_pointers_repaired;
        // Unusable is not a verdict on the tree: the gate is saying it cannot
        // trust its own inputs (an unreadable or outdated page 0). Rebuilding on
        // that would turn a gate limitation into a table-sized recovery.
        gate_declined = gate.setup() == RecoveryIndexGate::Setup::Unusable;
    }
    if (gate_declined) {
        LOG_WARN("recovery index gate %s declined; falling back to whole-tree structure validation",
                 plan->index_name.c_str());
        return index->validate_structure();
    }
    return valid;
}

bool RecoveryManager::apply_index_repair_plan(IndexRepairPlan* plan) {
    IxIndexHandle* index = plan->index;
    const int key_len = plan->key_len;
    const char* arena = plan->key_arena.data();
    const auto key_of = [arena, key_len](const IndexRepairEntry& entry) {
        return arena + static_cast<size_t>(entry.key_slot) * key_len;
    };

    std::vector<Rid> existing;
    std::vector<Rid> required;   // must be present when the group is done
    std::vector<Rid> candidates; // WAL images that may be stale entries
    std::vector<const char*> spot_check_keys;
    std::vector<Rid> spot_check_rids;

    for (size_t begin = 0; begin < plan->entries.size();) {
        size_t end = begin + 1;
        const char* key = key_of(plan->entries[begin]);
        while (end < plan->entries.size() &&
               ix_compare(key_of(plan->entries[end]), key, index->get_col_types(), index->get_col_lens()) == 0) {
            ++end;
        }

        required.clear();
        candidates.clear();
        for (size_t i = begin; i < end; ++i) {
            auto& target = plan->entries[i].from_heap ? required : candidates;
            if (target.empty() || !(target.back() == plan->entries[i].rid)) {
                target.push_back(plan->entries[i].rid);
            }
        }

        existing.clear();
        ++index_probe_count_;
        index->get_value(key, &existing, nullptr);

        // `existing` is a multiset, not a set: lookup_equal() pushes back every
        // matching slot, insert_entry(..., allow_duplicate=true) will store the
        // same (key, rid) twice, and delete_entry() removes one copy per call.
        // Treating it as a set is what let E = {r, r}, C = {r}, R = {r} pass the
        // skip test below and leave a duplicated entry in the tree forever --
        // which makes an index scan return the same heap row twice and inflates
        // the per-partition row counts `final.md:345` cross-checks.
        const auto contains = [](const std::vector<Rid>& haystack, const Rid& needle) {
            return std::find(haystack.begin(), haystack.end(), needle) != haystack.end();
        };
        const auto multiplicity = [](const std::vector<Rid>& haystack, const Rid& needle) {
            return static_cast<int>(std::count(haystack.begin(), haystack.end(), needle));
        };
        for (size_t i = 0; i < existing.size(); ++i) {
            // Count each duplicated RID once, at its first occurrence. This is
            // the only measurement of how often a real crash leaves a duplicate;
            // the skip predicate used to hide them from every counter.
            if (multiplicity(existing, existing[i]) > 1 &&
                std::find(existing.begin(), existing.begin() + static_cast<std::ptrdiff_t>(i), existing[i]) ==
                    existing.begin() + static_cast<std::ptrdiff_t>(i)) {
                ++index_duplicate_entry_count_;
            }
        }

        // Draining every WAL image and then reinstalling the live keys leaves
        // this key holding exactly `required`, once each. When the tree already
        // holds exactly that, the sequence is a no-op and the traversals, page
        // dirtying and node merges it would cause are all pure waste.
        bool already_correct = true;
        for (const Rid& rid : required) {
            if (multiplicity(existing, rid) != 1) {
                already_correct = false;
                break;
            }
        }
        if (already_correct) {
            for (const Rid& rid : existing) {
                if (contains(candidates, rid) && !contains(required, rid)) {
                    already_correct = false;
                    break;
                }
            }
        }
        if (already_correct) {
            ++index_unchanged_key_count_;
            begin = end;
            continue;
        }

        // An interrupted index write can leave the same pair more than once, so
        // every removal drains rather than deleting a single copy.
        const auto drain = [&](const Rid& rid, int keep) {
            const int surplus = std::min(multiplicity(existing, rid) - keep, kMaxDuplicateDrain);
            for (int removed = 0; removed < surplus; ++removed) {
                if (!index->delete_entry(key, rid, IndexWriteWalContext::RecoveryDurable())) {
                    break;
                }
                ++index_mutation_count_;
            }
        };
        for (const Rid& rid : candidates) {
            drain(rid, 0);
        }
        for (const Rid& rid : required) {
            if (contains(candidates, rid)) {
                // Drained to zero above, so exactly one copy has to go back.
                index->insert_entry(key, rid, IndexWriteWalContext::RecoveryDurable(), true);
                ++index_mutation_count_;
            } else if (multiplicity(existing, rid) == 0) {
                index->insert_entry(key, rid, IndexWriteWalContext::RecoveryDurable(), true);
                ++index_mutation_count_;
            } else {
                // Present and never deleted; only the surplus copies have to go.
                drain(rid, 1);
                continue;
            }
            if (spot_check_keys.size() < kRepairSpotCheckLimit) {
                spot_check_keys.push_back(key);
                spot_check_rids.push_back(rid);
            }
        }
        begin = end;
    }

    // A repair that did not take is evidence the tree cannot be fixed in place.
    for (size_t i = 0; i < spot_check_keys.size(); ++i) {
        existing.clear();
        index->get_value(spot_check_keys[i], &existing, nullptr);
        if (std::find(existing.begin(), existing.end(), spot_check_rids[i]) == existing.end()) {
            LOG_WARN("recovery index %s did not accept a repaired key", plan->index_name.c_str());
            return false;
        }
    }
    return true;
}

void RecoveryManager::repair_touched_indexes() {
    std::map<std::string, IndexRepairPlan> plans;
    // Only names and key widths at this point. Collecting the keys costs a WAL
    // pass and a heap sweep, so it waits until the spine check below has dropped
    // the indexes that cannot be repaired in place at all.
    plan_touched_indexes(&plans);
    if (plans.empty()) {
        return;
    }
    const size_t total_indexes = plans.size();

    std::unordered_set<std::string> indexes_to_rebuild;

    // Stage 1: the spine. last_leaf_/first_leaf_ are append hints that reach disk
    // only when a checkpoint publishes the header, so after a crash they
    // routinely name leaves that have since been split - which alone makes the
    // leaf chain look broken and makes delete_entry stop scanning early.
    // refresh_leaf_chain_endpoint() recomputes both by descending the left and
    // right edges, one page read per level, and fails only when even that spine
    // is unusable.
    const auto spine_begin = std::chrono::steady_clock::now();
    size_t repaired_endpoints = 0;
    for (const auto& [index_name, plan] : plans) {
        if (!plan.index->refresh_leaf_chain_endpoint()) {
            LOG_ERROR("recovery could not follow the leaf chain of index %s", index_name.c_str());
            indexes_to_rebuild.insert(index_name);
            continue;
        }
        ++repaired_endpoints;
    }
    const auto spine_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - spine_begin).count();

    // Drop the plans for indexes that are going to be rebuilt anyway, then pay
    // for the key collection only for the rest.
    for (auto it = plans.begin(); it != plans.end();) {
        it = indexes_to_rebuild.count(it->first) != 0 ? plans.erase(it) : std::next(it);
    }
    collect_index_repair_keys(&plans);

    // Stage 2: the structure gate proper, over the change set rather than over
    // the tree. See src/recovery/index_structure_gate.h for why the distinct
    // repair keys are a sufficient cover for everything an interrupted SMO can
    // damage. It runs before any mutation, so a tree the repair cannot fix key
    // by key never gets written to.
    const auto gate_begin = std::chrono::steady_clock::now();
    RecoveryIndexGate::Stats gate_totals;
    for (auto& [index_name, plan] : plans) {
        sort_index_repair_entries(&plan);
        try {
            if (!gate_index_change_set(&plan, &gate_totals)) {
                LOG_ERROR("recovery found structurally invalid index %s", index_name.c_str());
                indexes_to_rebuild.insert(index_name);
            }
        } catch (const std::exception& error) {
            LOG_ERROR("recovery could not validate the structure of index %s: %s", index_name.c_str(), error.what());
            indexes_to_rebuild.insert(index_name);
        }
    }
    LOG_INFO("recovery index structure gate: %zu indexes, spine: %zu leaf endpoints refreshed, %lld ms; "
             "change set: %llu descents, "
             "%llu keys covered, %llu pages validated, %llu page fetches, "
             "%llu parent pointers repaired, %llu leaves with an empty successor, %zu to rebuild, %lld ms",
             total_indexes, repaired_endpoints, static_cast<long long>(spine_ms),
             static_cast<unsigned long long>(gate_totals.descents),
             static_cast<unsigned long long>(gate_totals.keys_covered),
             static_cast<unsigned long long>(gate_totals.pages_validated),
             static_cast<unsigned long long>(gate_totals.page_fetches),
             static_cast<unsigned long long>(gate_totals.parent_pointers_repaired),
             static_cast<unsigned long long>(gate_totals.chain_bounds_unknown), indexes_to_rebuild.size(),
             static_cast<long long>(
                 std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - gate_begin)
                     .count()));

    for (auto it = plans.begin(); it != plans.end();) {
        it = indexes_to_rebuild.count(it->first) != 0 ? plans.erase(it) : std::next(it);
    }

    for (auto& [index_name, plan] : plans) {
        try {
            if (!apply_index_repair_plan(&plan)) {
                indexes_to_rebuild.insert(index_name);
            }
        } catch (const std::exception& error) {
            LOG_ERROR("recovery found structurally inconsistent index %s: %s", index_name.c_str(), error.what());
            indexes_to_rebuild.insert(index_name);
        }
    }

    LOG_INFO("recovery index repair: %llu probes, %llu mutations, %llu keys already correct, %llu duplicated entries",
             static_cast<unsigned long long>(index_probe_count_),
             static_cast<unsigned long long>(index_mutation_count_),
             static_cast<unsigned long long>(index_unchanged_key_count_),
             static_cast<unsigned long long>(index_duplicate_entry_count_));

    // index_plans points into `plans`, which dies with this function.
    for (auto& table : tables_) {
        table.index_plans.clear();
    }
    rebuild_indexes(indexes_to_rebuild);
}

void RecoveryManager::rebuild_indexes(const std::unordered_set<std::string>& index_names) {
    if (index_names.empty()) {
        return;
    }
    // Logged at ERROR, not INFO. A rebuild reads the whole heap once per index,
    // so recovery time stops being proportional to the WAL and starts being
    // proportional to the table -- at ten million rows no rebuild fits the 90 s
    // readiness budget at all. It is a correctness backstop, not a normal
    // outcome, and the default log level is WARN, so anything quieter than this
    // would be invisible outside the recovery window.
    LOG_ERROR("recovery must rebuild %zu index(es) from the heap; this is NOT the normal recovery path and makes "
              "recovery time proportional to the table rather than to the WAL",
              index_names.size());
    for (const auto& index_name : index_names) {
        const auto begin = std::chrono::steady_clock::now();
        sm_manager_->rebuild_indexes({index_name});
        ++index_rebuild_count_;
        LOG_ERROR("recovery rebuilt index %s in %lld ms", index_name.c_str(),
                  static_cast<long long>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                          .count()));
    }
}

void RecoveryManager::reset_wal_if_needed() {
    if (log_manager_ == nullptr) {
        return;
    }

    // 截断 WAL 会连带丢掉 COMMIT 记录里的 commit_ts，因此在截断**之前**必须把本轮算出的
    // 计数器下界发布到 db.restart：恢复在这一刻接过了 checkpoint 的同一份责任。
    // 不发布的后果很具体：本轮恢复之后、下一次 checkpoint 之前再崩一次，第二轮恢复既
    // 没有 WAL 也没有清单，计数器又回到 0，那些没被 reset_touched_tuple_meta 归一化过
    // 的页上的已提交行会再次变成不可见。它同时给出重复恢复的幂等性：同一个崩溃状态恢复
    // 两次得到同一个计数器下界。
    // write_restart_manifest 自带 tmp + fdatasync + rename + 目录 fsync，所以它在
    // reset_log 之前就已经 durable。
    const bool truncating = max_lsn_ != INVALID_LSN;
    RestartManifest manifest;
    // 截断之后扫描只能从文件头开始；不截断时保持 analyze 本轮用过的起点。
    manifest.checkpoint_offset = truncating ? 0 : checkpoint_offset_;
    manifest.next_timestamp = get_recovered_next_timestamp();
    manifest.next_txn_id = get_recovered_next_txn_id();
    log_manager_->write_restart_manifest(manifest);
    if (!truncating) {
        return;
    }
    const lsn_t next_lsn = max_lsn_ + 1;
    FaultInjector::Point("before_recovery_wal_reset");
    log_manager_->reset_log(next_lsn);
}

bool RecoveryManager::record_exists(const RecoveryTable& table, const Rid& rid) const {
    if (table.file_handle == nullptr) {
        return false;
    }
    if (rid.page_no < 0 || rid.page_no >= table.file_handle->get_file_hdr().num_pages) {
        return false;
    }
    try {
        return table.file_handle->is_record(rid);
    } catch (const std::exception&) {
        return false;
    }
}

std::unique_ptr<RmRecord> RecoveryManager::get_record_if_exists(const RecoveryTable& table, const Rid& rid) const {
    if (!record_exists(table, rid)) {
        return nullptr;
    }
    try {
        return table.file_handle->get_record(rid, nullptr);
    } catch (const std::exception&) {
        return nullptr;
    }
}

bool RecoveryManager::record_equals(const RecoveryTable& table, const Rid& rid, const char* expected,
                                    int expected_size) const {
    auto current = get_record_if_exists(table, rid);
    if (current == nullptr) {
        return false;
    }
    return current->size == expected_size && memcmp(current->data, expected, static_cast<size_t>(expected_size)) == 0;
}

bool RecoveryManager::redo_existing_slot(RecoveryTable& table, const Rid& rid, const char* image, int image_size,
                                         const TupleMeta& meta, lsn_t lsn) {
    // Installing an image plus its metadata used to cost two to four separate
    // buffer-pool round trips per record: an existence probe, a body write and a
    // metadata write, each fetching and unpinning the same page. One pin under
    // one exclusive page latch does all of it. That matters because the table is
    // several times larger than the pool, so every extra fetch is an extra
    // random read.
    if (table.file_handle == nullptr || rid.page_no < 0 || rid.page_no >= table.file_handle->get_file_hdr().num_pages) {
        return false;
    }
    RmPageHandle page_handle;
    try {
        page_handle = table.file_handle->fetch_page_handle(rid.page_no);
    } catch (const std::exception&) {
        return false;
    }
    bool applied = false;
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        if (rid.slot_no >= 0 && rid.slot_no < page_handle.file_hdr->num_records_per_page &&
            Bitmap::is_set(page_handle.bitmap, rid.slot_no) && image_size == page_handle.file_hdr->record_size) {
            memcpy(page_handle.get_slot(rid.slot_no), image, static_cast<size_t>(image_size));
            page_handle.get_meta(rid.slot_no) = meta;
            if (lsn != INVALID_LSN && page_handle.page->get_page_lsn() < lsn) {
                page_handle.page->set_page_lsn(lsn);
            }
            applied = true;
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), applied);
    return applied;
}

// The three redo functions below all validate the image length before the fast
// path, not just before the fallback. redo_existing_slot() checks it itself and
// declines the record, but declining hands the same unchecked bytes to
// RmFileHandle::insert_record(), which copies exactly record_size bytes out of
// the pointer regardless of how long the WAL says the image is, sets the bitmap
// bit for the slot without bounds-checking it, and extends the file until
// num_pages exceeds page_no. The RID and the image length are unvalidated
// external input, so both have to be settled before either path runs.
// analyze() already bounded the RID; only the length is left.

void RecoveryManager::redo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    validate_installable_image(table, record, dml.after_size);
    // The committed metadata is installed even when the tuple body was already
    // present: a page LSN can be ahead because the same page also holds a loser
    // operation, so it must not be used to skip this redo.
    const TupleMeta meta = MakeCommittedMeta(record.txn_id);
    if (redo_existing_slot(table, dml.rid, dml.after_image, dml.after_size, meta, record.lsn)) {
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.after_image), record.lsn);
    if (table.file_handle->is_record(dml.rid)) {
        table.file_handle->set_tuple_meta(dml.rid, meta, record.lsn);
    }
}

void RecoveryManager::redo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    validate_installable_image(table, record, dml.before_size);
    TupleMeta meta = MakeCommittedMeta(record.txn_id);
    meta.is_deleted_ = true;
    if (redo_existing_slot(table, dml.rid, dml.before_image, dml.before_size, meta, record.lsn)) {
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.before_image), record.lsn);
    if (table.file_handle->is_record(dml.rid)) {
        table.file_handle->set_tuple_meta(dml.rid, meta, record.lsn);
    }
}

bool RecoveryManager::redo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table,
                                  bool defer_on_active_loser) {
    if (dml.update_delta.present()) {
        return redo_update_delta(record, dml, table, defer_on_active_loser);
    }
    validate_installable_image(table, record, dml.after_size);
    const TupleMeta meta = MakeCommittedMeta(record.txn_id);
    if (redo_existing_slot(table, dml.rid, dml.after_image, dml.after_size, meta, record.lsn)) {
        return true;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.after_image), record.lsn);
    if (table.file_handle->is_record(dml.rid)) {
        table.file_handle->set_tuple_meta(dml.rid, meta, record.lsn);
    }
    return true;
}

bool RecoveryManager::redo_update_delta(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table,
                                        bool defer_on_active_loser) {
    validate_installable_image(table, record, static_cast<int>(dml.update_delta.row_size));
    uint32_t validation_cursor = 0;
    for (uint32_t i = 0; i < dml.update_delta.span_count; ++i) {
        WalUpdateDeltaSpan span;
        if (!ReadWalUpdateDeltaSpan(dml.update_delta, &validation_cursor, &span)) {
            throw InternalError("recovery UPDATE delta changed after analyze at LSN " + std::to_string(record.lsn) +
                                "; WAL retained");
        }
    }
    if (validation_cursor != dml.update_delta.span_bytes_length) {
        throw InternalError("recovery UPDATE delta has trailing span bytes at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }
    if (table.file_handle == nullptr || dml.rid.page_no < 0 ||
        dml.rid.page_no >= table.file_handle->get_file_hdr().num_pages) {
        throw InternalError("recovery UPDATE delta has no durable base page at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }

    RmPageHandle page_handle;
    try {
        page_handle = table.file_handle->fetch_page_handle(dml.rid.page_no);
    } catch (const std::exception&) {
        throw InternalError("recovery UPDATE delta could not fetch its base page at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }

    bool occupied = false;
    bool skipped_for_loser = false;
    bool unknown_uncommitted_writer = false;
    bool dirtied = false;
    bool decoded = true;
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        occupied = dml.rid.slot_no >= 0 && dml.rid.slot_no < page_handle.file_hdr->num_records_per_page &&
                   Bitmap::is_set(page_handle.bitmap, dml.rid.slot_no);
        if (occupied) {
            TupleMeta& current_meta = page_handle.get_meta(dml.rid.slot_no);
            // A page-level LSN cannot identify which row version supplied the
            // bytes outside this delta. The active owner may be either an older
            // aborted writer whose dirty page survived or a later loser. In
            // both cases, applying only the changed spans and publishing
            // committed metadata could seal unrelated loser bytes into the
            // committed row. Preserve any active-loser-owned image for undo;
            // replay_deferred_committed_deltas() applies the retained deltas in
            // forward order after every loser is gone. A tuple may also retain
            // uncommitted metadata for a transaction whose COMMIT WAL is
            // durable; its body is a known committed image and is safe to use.
            // Any owner in neither analyzed set has no explainable WAL chain,
            // so recovery must fail instead of guessing which bytes are valid.
            if (defer_on_active_loser && !current_meta.is_committed_) {
                skipped_for_loser = active_txn_last_lsn_.count(current_meta.writer_txn_id_) != 0;
                const bool committed_writer = committed_txns_.count(current_meta.writer_txn_id_) != 0;
                unknown_uncommitted_writer = !skipped_for_loser && !committed_writer;
            }
            if (!skipped_for_loser && !unknown_uncommitted_writer) {
                char* row = page_handle.get_slot(dml.rid.slot_no);
                uint32_t cursor = 0;
                for (uint32_t i = 0; i < dml.update_delta.span_count; ++i) {
                    WalUpdateDeltaSpan span;
                    if (!ReadWalUpdateDeltaSpan(dml.update_delta, &cursor, &span)) {
                        decoded = false;
                        break;
                    }
                    memcpy(row + span.offset, span.after_bytes, span.length);
                }
                decoded = decoded && cursor == dml.update_delta.span_bytes_length;
                if (decoded) {
                    current_meta = MakeCommittedMeta(record.txn_id);
                    if (record.lsn != INVALID_LSN && page_handle.page->get_page_lsn() < record.lsn) {
                        page_handle.page->set_page_lsn(record.lsn);
                    }
                    dirtied = true;
                }
            }
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), dirtied);
    if (!decoded) {
        throw InternalError("recovery UPDATE delta changed during committed redo at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }
    if (unknown_uncommitted_writer) {
        throw InternalError("recovery UPDATE delta found an uncommitted base without an active WAL owner at LSN " +
                            std::to_string(record.lsn) + "; WAL retained");
    }
    if (skipped_for_loser) {
        return false;
    }
    if (!occupied) {
        // A committed INSERT earlier in the retained WAL is replayed before
        // this record by the per-page WAL-offset sort. If the slot is still
        // absent, the checkpoint/base-page invariant is broken; a delta cannot
        // safely synthesize the missing bytes.
        throw InternalError("recovery UPDATE delta has no base tuple at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }
    return true;
}

void RecoveryManager::replay_deferred_committed_deltas() {
    if (deferred_committed_deltas_.empty()) {
        return;
    }

    // Any later full-image INSERT/DELETE/UPDATE erased this RID's descriptors
    // during the first redo pass: that record completely anchors the resulting
    // body (or tombstone), including the effects of earlier committed deltas.
    // The remaining descriptors therefore form the suffix of committed v2
    // assignments after the last full anchor and can be applied directly in
    // their original per-RID WAL order once all loser effects are undone.
    ReadOnlyWalMapping wal_mapping(disk_manager_, disk_manager_->GetLogFd(), scan_end_offset_);
    std::vector<char> scratch;
    WalDmlView dml;
    for (const auto& [target, records] : deferred_committed_deltas_) {
        if (target.table_id >= tables_.size()) {
            throw InternalError("recovery deferred UPDATE has an invalid table id; WAL retained");
        }
        RecoveryTable& table = tables_[target.table_id];
        if (table.file_handle == nullptr) {
            throw InternalError("recovery lost an open table while replaying a deferred UPDATE; WAL retained");
        }
        int64_t previous_offset = -1;
        for (const HeapRedoRecord& location : records) {
            if (location.table_id != target.table_id || location.page_no != target.page_no ||
                location.slot_no != target.slot_no || location.wal_offset <= previous_offset) {
                throw InternalError("recovery deferred UPDATE descriptors are inconsistent; WAL retained");
            }
            previous_offset = location.wal_offset;
            const char* bytes = wal_mapping.record_bytes(location.wal_offset, location.wal_length, &scratch);
            const WalRecordView record = mapped_heap_redo_record(location, bytes);
            if (record.log_type != LogType::UPDATE || committed_txns_.count(record.txn_id) == 0 ||
                !ParseWalDmlForRedo(record, &dml) || !dml.update_delta.present() || dml.table_name != table.name ||
                dml.rid.page_no != target.page_no || dml.rid.slot_no != target.slot_no) {
                throw InternalError("recovery deferred UPDATE no longer matches analyze; WAL retained");
            }
            if (!redo_update(record, dml, table, false)) {
                throw InternalError("recovery re-deferred an UPDATE after loser undo; WAL retained");
            }
            FaultInjector::Point("mid_recovery_redo");
        }
    }
    deferred_committed_deltas_.clear();
}

void RecoveryManager::undo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务插入的值时才删除。
    // 内容比较无法区分「loser 未刷盘的 insert」与「committed 事务在同一 RID 复用并写入
    // 相同内容」（RID 复用 + d_next_o_id 回退后复位会导致两者完全相同），故必须用
    // TupleMeta.writer_txn_id_ 判断所有权：仅当 slot 仍归属本 loser 事务时才删除。
    if (!record_exists(table, dml.rid)) {
        return;
    }
    const TupleMeta meta = table.file_handle->get_tuple_meta(dml.rid);
    if (meta.writer_txn_id_ != record.txn_id) {
        return;
    }
    table.file_handle->delete_record(dml.rid, nullptr, record.lsn);
}

void RecoveryManager::undo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    // Both paths below copy record_size bytes out of before_image; see the note
    // above redo_insert().
    validate_installable_image(table, record, dml.before_size);
    // 幂等守卫：仅当该 slot 当前为空，或仍是本 loser 写下的 MVCC tombstone 时才恢复。
    // 若 slot 已被后续 committed 事务重新写入为 live tuple，跳过，避免覆盖 committed 数据。
    const TupleMeta restored_meta = MakeLoserMeta(record.txn_id);
    if (record_exists(table, dml.rid)) {
        const TupleMeta meta = table.file_handle->get_tuple_meta(dml.rid);
        if (meta.is_deleted_ && meta.writer_txn_id_ == record.txn_id) {
            table.file_handle->apply_tuple_update(dml.rid, dml.before_image, restored_meta, record.lsn);
        }
        return;
    }
    table.file_handle->insert_record(dml.rid, const_cast<char*>(dml.before_image), record.lsn);
    table.file_handle->set_tuple_meta(dml.rid, restored_meta, record.lsn);
}

void RecoveryManager::undo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    if (dml.update_delta.present()) {
        undo_update_delta(record, dml, table);
        return;
    }
    // apply_tuple_update() copies record_size bytes out of before_image; see the
    // note above redo_insert().
    validate_installable_image(table, record, dml.before_size);
    // 幂等守卫：仅当该 rid 仍持有本 loser 事务写入的 new_value 时才回滚到 old_value。
    // 若 rid 已被后续 committed 事务覆盖为其他值，跳过，避免覆盖 committed 数据。
    if (!record_exists(table, dml.rid)) {
        return;
    }
    const TupleMeta meta = table.file_handle->get_tuple_meta(dml.rid);
    if (meta.writer_txn_id_ != record.txn_id) {
        return;
    }
    if (!record_equals(table, dml.rid, dml.after_image, dml.after_size)) {
        return;
    }
    table.file_handle->apply_tuple_update(dml.rid, dml.before_image, MakeLoserMeta(record.txn_id), record.lsn);
}

void RecoveryManager::undo_update_delta(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table) {
    validate_installable_image(table, record, static_cast<int>(dml.update_delta.row_size));
    if (table.file_handle == nullptr || dml.rid.page_no < 0 ||
        dml.rid.page_no >= table.file_handle->get_file_hdr().num_pages) {
        // The loser's page may never have reached disk (notably for an INSERT
        // followed by UPDATE in the same transaction), so absence is a no-op.
        return;
    }

    RmPageHandle page_handle;
    try {
        page_handle = table.file_handle->fetch_page_handle(dml.rid.page_no);
    } catch (const std::exception&) {
        throw InternalError("recovery UPDATE delta could not fetch a loser page at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }

    bool dirtied = false;
    bool decoded = true;
    {
        std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
        const bool occupied = dml.rid.slot_no >= 0 && dml.rid.slot_no < page_handle.file_hdr->num_records_per_page &&
                              Bitmap::is_set(page_handle.bitmap, dml.rid.slot_no);
        if (occupied) {
            TupleMeta& meta = page_handle.get_meta(dml.rid.slot_no);
            if (meta.writer_txn_id_ == record.txn_id) {
                char* row = page_handle.get_slot(dml.rid.slot_no);
                uint32_t cursor = 0;
                bool after_matches = true;
                for (uint32_t i = 0; i < dml.update_delta.span_count; ++i) {
                    WalUpdateDeltaSpan span;
                    if (!ReadWalUpdateDeltaSpan(dml.update_delta, &cursor, &span)) {
                        decoded = false;
                        after_matches = false;
                        break;
                    }
                    if (memcmp(row + span.offset, span.after_bytes, span.length) != 0) {
                        after_matches = false;
                    }
                }
                decoded = decoded && cursor == dml.update_delta.span_bytes_length;
                if (decoded && after_matches) {
                    cursor = 0;
                    for (uint32_t i = 0; i < dml.update_delta.span_count; ++i) {
                        WalUpdateDeltaSpan span;
                        if (!ReadWalUpdateDeltaSpan(dml.update_delta, &cursor, &span)) {
                            decoded = false;
                            break;
                        }
                        memcpy(row + span.offset, span.before_bytes, span.length);
                    }
                    if (decoded && cursor == dml.update_delta.span_bytes_length) {
                        meta = MakeLoserMeta(record.txn_id);
                        if (record.lsn != INVALID_LSN && page_handle.page->get_page_lsn() < record.lsn) {
                            page_handle.page->set_page_lsn(record.lsn);
                        }
                        dirtied = true;
                    }
                }
            }
        }
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), dirtied);
    if (!decoded) {
        throw InternalError("recovery UPDATE delta changed during loser undo at LSN " + std::to_string(record.lsn) +
                            "; WAL retained");
    }
}
