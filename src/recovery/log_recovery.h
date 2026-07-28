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

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "index_structure_gate.h"
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
    uint64_t get_loser_transaction_count() const {
        return static_cast<uint64_t>(active_txn_last_lsn_.size());
    }
    uint64_t get_pruned_no_undo_transaction_count() const {
        return pruned_no_undo_transaction_count_;
    }
    uint64_t get_undo_chain_record_read_count() const {
        return undo_chain_record_read_count_;
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
    // How many child pages the structure gate found naming the wrong parent and
    // repaired in place (A-1). This is the invariant a real SIGKILL was measured
    // to break; before the in-place repair every occurrence cost a full rebuild
    // of the whole index.
    uint64_t get_index_parent_pointer_repair_count() const {
        return index_parent_pointer_repair_count_;
    }

    /**
     * 重启后 TransactionManager::next_timestamp_ 必须取的值。analyze() 之后可用。
     *
     * 为什么必须有这个函数（这是看代码看不出来、却决定正确性的知识）：
     * TupleMeta.commit_ts_ 持久化在数据页里，而计数器只活在内存里。若计数器每次
     * 启动都从 0 开始，上一世以高 commit_ts_ 提交的行会被 GetVisibleRecord 判成
     * “来自未来”，而版本链随进程消失后无从回退 ⇒ 已提交的行不可见，直接违反
     * final.md:342 第 1 条。
     *
     * 取值 = max(db.restart 里的计数器快照, 保留 WAL 中 COMMIT 记录的最大 commit_ts + 1)。
     * 为什么这两项取 max 就覆盖了**所有**已持久化的 commit_ts_ —— 令 C 为最后一次
     * 成功完成的 clean checkpoint，N_C 为它写进 db.restart 的计数器快照。对磁盘上
     * 任意一个 commit_ts_ = T：
     *   (a) T 是在 C 取快照之前分发的 ⇒ T <= N_C - 1（commit_ts 来自
     *       next_timestamp_.fetch_add(1)，已分发值严格小于计数器），被第一项覆盖；
     *   (b) T 是在 C 之后分发的 ⇒ 分发它的事务在 C 之后提交，其 COMMIT 记录写在
     *       C 的截断点之后，而 C 是最后一次 checkpoint，所以那条记录仍在保留的 WAL
     *       里；又 T 只可能由 mark_slots_committed() 写进页面，而它排在
     *       WriteCommitLog() 的 flush_log_to_disk_up_to() 之后，所以“页上出现 T”
     *       蕴含“带 T 的 COMMIT 记录已 durable” ⇒ 被第二项覆盖。
     * 注意分类依据是 T 的**分发时刻**而不是页面的写盘时刻，因此“回滚把一个更老的
     * committed 版本（带更老的 T）重新写回页面、该页在 C 之后才落盘”这类情况也落在
     * (a) 里。mark_slots_committed() 是 src/ 里唯一给页面写非零 commit_ts_ 的地方
     * （其余路径一律写 0），这一点是上述论证的前提。
     *
     * 缺省值为什么安全：全新库没有 db.restart，第一项为 0；此时 WAL 从未被截断过，
     * 所以每一个已持久化的 commit_ts_ 都有 COMMIT 记录留在 WAL 里，第二项本身就是
     * 精确上界。旧版本写的 db.restart 只有裸偏移、没有计数器字段，同样退化为 0——
     * 只有“旧版本已经完成过 checkpoint 并截断了 WAL”的库会因此欠抬（见报告的残留风险）。
     */
    timestamp_t get_recovered_next_timestamp() const {
        const timestamp_t from_wal = max_wal_commit_ts_ == INVALID_TS ? 0 : max_wal_commit_ts_ + 1;
        return std::max(persisted_next_timestamp_, from_wal);
    }

    /**
     * 重启后 TransactionManager::next_txn_id_ 必须取的值：max(db.restart 快照,
     * 保留 WAL 中最大 txn_id + 1)。
     *
     * writer_txn_id_ 同样持久化在页里，而 GetVisibleRecord 有一条
     * `!is_committed_ && writer_txn_id_ == self_id` 的“这是我自己的未提交写”分支。
     * 今天磁盘上不会残留 is_committed_ == false 的 meta（reset_touched_tuple_meta
     * 会把保留 WAL 触及页上的存活槽全部归一化，而未提交的写必然被 WAL 记录覆盖），
     * 所以那条分支不会被误判。这里仍然把计数器抬高，是为了让“页上的 writer_txn_id_
     * 与新事务 ID 不会撞车”成为一条不依赖归一化范围的独立性质：一旦归一化被收窄
     * （PLAN.md 的 fuzzy checkpoint / 索引修复窗口化都会动它），重用 ID 就会让上面
     * 那条分支把别人上一世的未提交残留当成自己的写而返回。同时它也避免新事务在
     * 未被截断的 WAL 上重用旧 ID，让 analyze() 的 committed/loser 集合失真。
     */
    txn_id_t get_recovered_next_txn_id() const {
        const txn_id_t from_wal = max_wal_txn_id_ == INVALID_TXN_ID ? 0 : max_wal_txn_id_ + 1;
        return std::max(persisted_next_txn_id_, from_wal);
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
    // Puts one plan's entries into B+tree key order. Both the structure gate and
    // the repair consume that order, so it is established exactly once.
    void sort_index_repair_entries(IndexRepairPlan* plan);
    // Structure gate for one index, priced against the change set: validates the
    // root->leaf descent path of every distinct repair key, repairing parent back
    // pointers in place. Returns false when the index must be rebuilt from the
    // heap. Requires sort_index_repair_entries() to have run on the plan.
    bool gate_index_change_set(IndexRepairPlan* plan, RecoveryIndexGate::Stats* totals);
    // Applies one index's plan; returns false when the index must be rebuilt.
    // Requires sort_index_repair_entries() to have run on the plan.
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
    uint64_t pruned_no_undo_transaction_count_{0};
    uint64_t undo_chain_record_read_count_{0};
    uint64_t index_probe_count_{0};
    uint64_t index_mutation_count_{0};
    uint64_t index_unchanged_key_count_{0};
    uint64_t index_duplicate_entry_count_{0};
    uint64_t index_rebuild_count_{0};
    uint64_t index_parent_pointer_repair_count_{0};

    // 计数器恢复的两个来源，见 get_recovered_next_timestamp()。
    timestamp_t persisted_next_timestamp_{0};
    txn_id_t persisted_next_txn_id_{0};
    timestamp_t max_wal_commit_ts_{INVALID_TS};
    txn_id_t max_wal_txn_id_{INVALID_TXN_ID};

    DiskManager* disk_manager_;              // 用来读写文件
    BufferPoolManager* buffer_pool_manager_; // 对页面进行读写
    SmManager* sm_manager_;                  // 访问数据库元数据
    LogManager* log_manager_;                // recovery 完成后截断日志
};
