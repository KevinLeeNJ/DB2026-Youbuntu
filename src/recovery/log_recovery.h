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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "log_manager.h"
#include "wal_reader.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

class RedoLogsInPage {
public:
    RedoLogsInPage() {
        table_file_ = nullptr;
    }
    RmFileHandle* table_file_;
    std::vector<lsn_t> redo_logs_; // 在该page上需要redo的操作的lsn
};

/**
 * Crash recovery over a WAL that is streamed, never materialized.
 *
 * analyze() makes one forward pass and keeps only what the later phases need:
 * the committed/loser transaction sets, an 8-byte descriptor per DML record,
 * and an lsn -> file offset index. redo() is a second forward pass that reuses
 * one buffer; undo() reads individual records through the offset index, which
 * costs one pread per loser record. Nothing holds a deserialized record map,
 * so peak recovery memory is a few tens of MB rather than twice the WAL size.
 *
 * Precondition: LogManager::initialize_from_existing_log() must have run on the
 * same WAL file first. It applies exactly the record-header validation this
 * class applies and truncates the file to the end of the intact prefix, which
 * is what lets analyze() treat every remaining anomaly as real corruption
 * instead of as a torn tail. See the comment on analyze() for why that
 * distinction is the difference between refusing to start and silently
 * discarding committed data.
 *
 * Failure policy: anything recovery cannot explain throws. The WAL is only
 * truncated on the success path, so a throw leaves the complete WAL on disk and
 * the next process retries recovery from the identical input. A recovery that
 * half-succeeds and then declares itself done is unrecoverable; one that
 * refuses to start is not.
 */
class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager,
                    LogManager* log_manager = nullptr) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        log_manager_ = log_manager;
    }

    void analyze();
    void redo();
    void undo();

    // Record-level counters for the recovery report. Recovery is single
    // threaded, so plain integers are enough. get_redo_applied_count() plus
    // get_redo_skipped_count() always equals the number of DML records the WAL
    // holds, which is what makes the report usable as a cross-check.
    uint64_t get_scanned_record_count() const {
        return scanned_record_count_;
    }
    uint64_t get_dml_record_count() const {
        return touched_.size();
    }
    uint64_t get_redo_applied_count() const {
        return redo_applied_count_;
    }
    uint64_t get_redo_skipped_count() const {
        return redo_skipped_count_;
    }
    uint64_t get_undo_applied_count() const {
        return undo_applied_count_;
    }
    uint64_t get_index_probe_count() const {
        return index_probe_count_;
    }
    uint64_t get_index_mutation_count() const {
        return index_mutation_count_;
    }
    uint64_t get_index_unchanged_key_count() const {
        return index_unchanged_key_count_;
    }
    // Number of distinct RIDs a real crash left stored twice under one key.
    // The skip predicate used to treat the probe result as a set, which hid
    // these entirely; this counter is the evidence base for kMaxDuplicateDrain.
    uint64_t get_index_duplicate_entry_count() const {
        return index_duplicate_entry_count_;
    }
    // Non-zero means the in-place repair was abandoned for at least one index
    // and the whole index was rebuilt from the heap. That is not the normal
    // path: it makes recovery time proportional to the table, not to the WAL.
    uint64_t get_index_rebuild_count() const {
        return index_rebuild_count_;
    }

private:
    // One DML record's target, packed to 8 bytes. A 256 MB WAL holds roughly
    // 800k DML records, so the whole array is about 6 MB.
    struct TouchedTuple {
        uint16_t table_id{0};
        int16_t slot_no{0};
        int32_t page_no{0};

        bool operator<(const TouchedTuple& other) const {
            if (table_id != other.table_id) {
                return table_id < other.table_id;
            }
            if (page_no != other.page_no) {
                return page_no < other.page_no;
            }
            return slot_no < other.slot_no;
        }
        bool operator==(const TouchedTuple& other) const {
            return table_id == other.table_id && page_no == other.page_no && slot_no == other.slot_no;
        }
    };

    // Where a record lives in the WAL file, so undo can follow prev_lsn links
    // without keeping the records themselves.
    struct WalRecordLocation {
        lsn_t lsn{INVALID_LSN};
        int64_t offset{0};
    };

    // One (key, rid) pair the index repair has to reconcile. `key_slot` indexes
    // the fixed-stride key arena of the index being repaired.
    struct IndexRepairEntry {
        uint32_t key_slot{0};
        Rid rid{};
        bool from_heap{false}; // true: must be present afterwards
    };

    // Reconciliation plan for all keys of one index. index_meta and index are
    // resolved once, when the plan is created, so no later phase has to rebuild
    // the index name to find either of them.
    struct IndexRepairPlan {
        std::string index_name;
        const IndexMeta* index_meta{nullptr};
        IxIndexHandle* index{nullptr};
        int key_len{0};
        std::vector<char> key_arena;
        std::vector<IndexRepairEntry> entries;
    };

    // Everything the apply phases need about one table, resolved once per
    // table instead of once per record.
    struct RecoveryTable {
        std::string name;
        RmFileHandle* file_handle{nullptr};
        TabMeta* meta{nullptr};
        // Cached because RmFileHandle::get_file_hdr() returns by value and the
        // guards below consult these on every DML record.
        int records_per_page{0};
        int record_size{0};
        // Exclusive upper bound on the page numbers the WAL can justify for
        // this table. See validate_touched_page_bounds().
        int32_t page_no_limit{0};
        // The plans for this table's indexes that are going to be repaired in
        // place. Resolved once per table: get_index_name() concatenates a string
        // and hits a map, and the key collection runs once per WAL record.
        std::vector<IndexRepairPlan*> index_plans;
    };

    uint16_t intern_table(std::string_view table_name);
    RecoveryTable* table_at(uint16_t table_id) {
        return &tables_[table_id];
    }
    int64_t offset_of_lsn(lsn_t lsn) const;
    void build_touched_index();

    // Guards against a corrupt RID reaching the record layer. The WAL carries
    // no per-record checksum, so a header that passed the length and type
    // checks can still be followed by a payload naming any slot on any page,
    // and everything downstream (Bitmap::set, memcpy into get_slot, the
    // file-extension loop in insert_record) trusts what it is handed. Both
    // throw, which keeps the WAL and lets the next process retry.
    void validate_dml_rid(const RecoveryTable& table, const WalRecordView& record, const Rid& rid) const;
    void validate_touched_page_bounds();
    // Rejects an image whose length is not the table's record size. The record
    // layer copies exactly record_size bytes out of the pointer it is given, so
    // a short image would read past the end of the WAL buffer.
    void validate_installable_image(const RecoveryTable& table, const WalRecordView& record, int image_size) const;

    bool record_exists(const RecoveryTable& table, const Rid& rid) const;
    // 当前 rid 处的记录是否与 expected 内容一致（rid 不存在视为不等）。
    // 用于 undo 幂等守卫：仅当页面仍反映该 loser 事务自身的效果时才回滚，
    // 避免跨轮 recovery 重复 undo 覆盖同 RID 上的后续 committed 数据。
    bool record_equals(const RecoveryTable& table, const Rid& rid, const char* expected, int expected_size) const;
    std::unique_ptr<RmRecord> get_record_if_exists(const RecoveryTable& table, const Rid& rid) const;

    // Writes an image and its metadata into an already occupied slot under a
    // single page pin. Returns false when the slot is not occupied, in which
    // case the caller has to go through the record layer's insert path so that
    // the bitmap, record count and free list stay correct.
    bool redo_existing_slot(RecoveryTable& table, const Rid& rid, const char* image, int image_size,
                            const TupleMeta& meta, lsn_t lsn);
    void redo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void redo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void redo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void undo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void undo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void undo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void repair_touched_file_headers();
    void reset_touched_tuple_meta();
    void repair_touched_indexes();
    // Names the indexes a touched table owns, without reading any key yet, and
    // resolves each one's IndexMeta and handle so no later phase has to.
    void plan_touched_indexes(std::map<std::string, IndexRepairPlan>* plans);
    // Publishes the surviving plans into RecoveryTable::index_plans, so the key
    // collection walks pointers instead of rebuilding index names.
    void bind_index_plans(std::map<std::string, IndexRepairPlan>* plans);
    // Collects, per index, every key the WAL mentions for a touched RID plus
    // the key its final live tuple carries. Costs one WAL pass and one sweep of
    // the touched heap pages, so it runs after the structure gate has dropped
    // the indexes that are going to be rebuilt.
    void collect_index_repair_keys(std::map<std::string, IndexRepairPlan>* plans);
    void collect_wal_index_keys();
    void collect_heap_index_keys();
    // Applies one index's plan; returns false when the index must be rebuilt.
    bool apply_index_repair_plan(IndexRepairPlan* plan);
    void rebuild_indexes(const std::unordered_set<std::string>& index_names);
    void reset_wal_if_needed();

    std::unordered_set<txn_id_t> committed_txns_;
    // Ordered so that recovery visits loser transactions in the same sequence
    // on every run: repeating recovery must reach the identical state.
    std::map<txn_id_t, lsn_t> active_txn_last_lsn_;
    std::vector<RecoveryTable> tables_;
    std::unordered_map<std::string, uint16_t> table_ids_;
    std::string table_name_scratch_;           // reused, so name lookups do not allocate
    std::vector<TouchedTuple> touched_;        // one entry per DML record, WAL order
    std::vector<TouchedTuple> touched_sorted_; // distinct, ordered by table and page
    std::vector<WalRecordLocation> record_locations_;
    bool record_locations_sorted_{true};
    std::unordered_set<std::string> touched_tables_;
    bool has_dml_records_{false};
    lsn_t max_lsn_{INVALID_LSN}; // analyze 扫描到的最大 lsn，用于 recovery 后推进 global_lsn
    int64_t checkpoint_offset_{0};
    int64_t scan_begin_offset_{0}; // first record analyze/redo replay
    int64_t scan_end_offset_{0};   // end of the intact WAL prefix

    uint64_t scanned_record_count_{0};
    uint64_t redo_applied_count_{0};
    uint64_t redo_skipped_count_{0};       // losers plus records with no open table
    uint64_t redo_missing_table_count_{0}; // subset of the above whose table is not open
    uint64_t undo_applied_count_{0};
    uint64_t index_probe_count_{0};
    uint64_t index_mutation_count_{0};
    uint64_t index_unchanged_key_count_{0};
    uint64_t index_duplicate_entry_count_{0};
    uint64_t index_rebuild_count_{0};

    DiskManager* disk_manager_;              // 用来读写文件
    BufferPoolManager* buffer_pool_manager_; // 对页面进行读写
    SmManager* sm_manager_;                  // 访问数据库元数据
    LogManager* log_manager_;                // recovery 完成后截断日志
};
