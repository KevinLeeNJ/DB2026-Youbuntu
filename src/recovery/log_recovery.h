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
    // threaded, so plain integers are enough.
    uint64_t get_scanned_record_count() const {
        return scanned_record_count_;
    }
    uint64_t get_redo_applied_count() const {
        return redo_applied_count_;
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

    // Everything the apply phases need about one table, resolved once per
    // table instead of once per record.
    struct RecoveryTable {
        std::string name;
        RmFileHandle* file_handle{nullptr};
        TabMeta* meta{nullptr};
    };

    // One (key, rid) pair the index repair has to reconcile. `key_slot` indexes
    // the fixed-stride key arena of the index being repaired.
    struct IndexRepairEntry {
        uint32_t key_slot{0};
        Rid rid{};
        bool from_heap{false}; // true: must be present afterwards
    };

    // Reconciliation plan for all keys of one index.
    struct IndexRepairPlan {
        std::string index_name;
        int key_len{0};
        std::vector<char> key_arena;
        std::vector<IndexRepairEntry> entries;
    };

    uint16_t intern_table(std::string_view table_name);
    RecoveryTable* table_at(uint16_t table_id) {
        return &tables_[table_id];
    }
    int64_t offset_of_lsn(lsn_t lsn) const;
    void build_touched_index();
    // Hands the kernel the pages the next batch of redo records will read.
    void prefetch_redo_batch(size_t from_index);

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
    // Names the indexes a touched table owns, without reading any key yet.
    void plan_touched_indexes(std::map<std::string, IndexRepairPlan>* plans);
    // Collects, per index, every key the WAL mentions for a touched RID plus
    // the key its final live tuple carries. Costs one WAL pass and one sweep of
    // the touched heap pages, so it runs after the structure gate has dropped
    // the indexes that are going to be rebuilt.
    void collect_index_repair_keys(std::map<std::string, IndexRepairPlan>* plans);
    void collect_wal_index_keys(std::map<std::string, IndexRepairPlan>* plans);
    void collect_heap_index_keys(std::map<std::string, IndexRepairPlan>* plans);
    // Applies one index's plan; returns false when the index must be rebuilt.
    bool apply_index_repair_plan(IxIndexHandle* index, const IndexMeta& index_meta, IndexRepairPlan* plan);
    void reset_wal_if_needed();

    std::unordered_set<txn_id_t> committed_txns_;
    // Ordered so that recovery visits loser transactions in the same sequence
    // on every run: repeating recovery must reach the identical state.
    std::map<txn_id_t, lsn_t> active_txn_last_lsn_;
    std::vector<RecoveryTable> tables_;
    std::unordered_map<std::string, uint16_t> table_ids_;
    std::string table_name_scratch_;             // reused, so name lookups do not allocate
    std::vector<TouchedTuple> touched_;          // one entry per DML record, WAL order
    std::vector<TouchedTuple> touched_sorted_;   // distinct, ordered by table and page
    std::vector<TouchedTuple> prefetch_scratch_; // reused by the readahead window
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
    uint64_t redo_skipped_count_{0};
    uint64_t undo_applied_count_{0};
    uint64_t index_probe_count_{0};
    uint64_t index_mutation_count_{0};
    uint64_t index_unchanged_key_count_{0};

    DiskManager* disk_manager_;              // 用来读写文件
    BufferPoolManager* buffer_pool_manager_; // 对页面进行读写
    SmManager* sm_manager_;                  // 访问数据库元数据
    LogManager* log_manager_;                // recovery 完成后截断日志
};
