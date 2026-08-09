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
#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "index_structure_gate.h"
#include "index_smo_log.h"
#include "log_manager.h"
#include "wal_reader.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

/**
 * Crash recovery over a WAL whose records are never deserialized en masse.
 *
 * analyze() makes one forward pass and keeps only what the later phases need:
 * the committed/loser transaction sets, an 8-byte descriptor per DML record,
 * a compact heap-redo location per DML record, and an lsn -> file offset index.
 * redo() keeps INDEX_SMO replay in WAL order, then reads heap DML through a
 * bounded read-only WAL window in (table, page, WAL-offset) order. undo() reads
 * individual records through the offset index. Nothing holds a deserialized
 * record map, so recovery memory remains proportional to record metadata rather
 * than to the WAL payload.
 *
 * Production order is LogManager::prepare_existing_log(), analyze(),
 * LogManager::finalize_existing_log(), prepare_pages_for_redo(), redo(), undo().
 * analyze is the only authoritative WAL scan. It distinguishes complete
 * semantic corruption (fail closed) from a physically incomplete tail, which
 * finalize truncates only after analyze has succeeded. The older
 * initialize_from_existing_log() remains a compatibility wrapper for fixtures
 * that do not run RecoveryManager.
 *
 * Failure policy: anything recovery cannot explain throws. The WAL is only
 * truncated on the success path, so a throw leaves the complete WAL on disk and
 * the next process retries recovery from the identical input. A recovery that
 * half-succeeds and then declares itself done is unrecoverable; one that
 * refuses to start is not.
 */
class RecoveryManager {
public:
    static constexpr size_t kLogTypeCount = static_cast<size_t>(LogType::INDEX_SMO) + 1;
    using IndexSmoRedoTestHook = std::function<void(std::string_view)>;
    // Test-only hook immediately before a committed heap descriptor is
    // re-read.  It exists solely to exercise a mutation after an earlier
    // record in the same pinned run has been applied.
    using HeapRedoRecordTestHook = std::function<void(int64_t)>;
    // Test-only rendezvous after a finalization task has acquired its page.
    // It is deliberately per RecoveryManager instance so production recovery
    // has no global test state or scheduling dependency.
    using RecoveryFinalizePinTestHook = std::function<void(page_id_t)>;
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager,
                    LogManager* log_manager = nullptr) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        log_manager_ = log_manager;
    }

    void analyze();
    // May only repair file headers after analyze has accepted the complete WAL
    // prefix and LogManager has made that prefix the append frontier.
    void prepare_pages_for_redo();
    void redo();
    void undo();

    // Tests use this boundary to prove the selected SMO descriptor is checked
    // again immediately before its after-images are installed. It is per
    // RecoveryManager instance and is never configured in production.
    void set_index_smo_redo_test_hook(IndexSmoRedoTestHook hook) {
        index_smo_redo_test_hook_ = std::move(hook);
    }
    void set_heap_redo_record_test_hook(HeapRedoRecordTestHook hook) {
        heap_redo_record_test_hook_ = std::move(hook);
    }
    // Test-only bounded-output override for exercising the verified-stream
    // fallback; zero keeps the production limit.
    void set_index_key_parallel_scratch_limit_for_test(size_t bytes) noexcept {
        index_key_parallel_scratch_limit_for_test_ = bytes;
    }
    void set_recovery_finalize_pin_test_hook(RecoveryFinalizePinTestHook hook) {
        recovery_finalize_pin_test_hook_ = std::move(hook);
    }

    // Record-level counters for the recovery report. Recovery is single
    // threaded, so plain integers are enough. Heap redo deliberately contains
    // committed DML only; applied plus skipped therefore equals the committed
    // descriptor count, while get_dml_record_count() includes loser DML kept
    // for undo and touched-tuple normalization.
    uint64_t get_scanned_record_count() const {
        return scanned_record_count_;
    }
    // Composition of the single authoritative analyze() WAL scan. The byte
    // count is the record's serialized length, including its common header.
    uint64_t get_log_type_record_count(LogType type) const {
        return log_type_record_counts_[static_cast<size_t>(type)];
    }
    uint64_t get_log_type_serialized_bytes(LogType type) const {
        return log_type_serialized_bytes_[static_cast<size_t>(type)];
    }
    // INDEX_SMO carries one logical full-page image per data page plus its
    // index-file header image. This intentionally reports logical bytes, so
    // V2 compressed records remain comparable with V1 records.
    uint64_t get_index_smo_logical_image_count() const {
        return index_smo_logical_image_count_;
    }
    uint64_t get_index_smo_logical_image_bytes() const {
        return index_smo_logical_image_bytes_;
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
    // Number of existing-page runs and pins consumed by the grouped heap-redo
    // path.  They are deliberately separate from extension anchors, whose
    // file-header publication must use RmFileHandle's serialized path.
    uint64_t get_redo_resident_page_run_count() const {
        return redo_resident_page_run_count_;
    }
    uint64_t get_redo_resident_page_pin_count() const {
        return redo_resident_page_pin_count_;
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
    uint64_t get_index_smo_prepare_count() const {
        return index_smo_prepare_count_;
    }
    int64_t get_scan_end_offset() const {
        return scan_end_offset_;
    }
    lsn_t get_max_lsn() const {
        return max_lsn_;
    }
    std::vector<std::pair<std::string, uint64_t>> get_latest_index_bindings() const;

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
        uint32_t length{0};
        uint32_t record_checksum{0};
    };

    // Location and target of one heap DML record. Sorting these by table/page
    // makes every page's records consecutive while WAL offset preserves their
    // original order within that page. The explicit target metadata also lets
    // redo fail closed if the mapped WAL changes after analyze().
    struct HeapRedoRecord {
        int64_t wal_offset{0};
        uint32_t wal_length{0};
        uint16_t table_id{0};
        int16_t slot_no{0};
        int32_t page_no{0};
        // Checksum of the complete serialized WAL record accepted by analyze.
        // Header identity alone is not enough: a same-length payload can name
        // the same RID while changing the row image (and, for INDEX_SMO, its
        // own CRC can be recomputed by a hostile/corrupt writer).
        uint32_t record_checksum{0};
        // Low 31 bits are the previous descriptor index plus one for this
        // transaction; zero ends the intrusive chain. The high bit records
        // that COMMIT visited this descriptor. This keeps the committed-only
        // catalogue compact without a vector<size_t> allocation per txn.
        uint32_t txn_prev_plus_one{0};

        bool operator<(const HeapRedoRecord& other) const {
            if (table_id != other.table_id) {
                return table_id < other.table_id;
            }
            if (page_no != other.page_no) {
                return page_no < other.page_no;
            }
            return wal_offset < other.wal_offset;
        }
    };
    static_assert(sizeof(HeapRedoRecord) <= 32, "heap redo descriptors must remain compact");

    struct IndexSmoRecord {
        int64_t wal_offset{0};
        uint32_t wal_length{0};
        lsn_t lsn{INVALID_LSN};
        txn_id_t txn_id{INVALID_TXN_ID};
        lsn_t prev_lsn{INVALID_LSN};
        // The prefix checksum excludes INDEX_SMO's stored trailing CRC. It
        // binds the complete analyzed record while still detecting a payload
        // rewrite whose trailing CRC was recomputed.
        uint32_t record_checksum{0};
        uint32_t payload_checksum{0}; // stored INDEX_SMO trailing CRC accepted by analyze()
        // analyze() has already parsed and validated this immutable metadata.
        // Keeping the compact page-number catalogue avoids parsing every large
        // SMO image a second time just to decide latest-wins during redo.
        std::string index_name;
        uint64_t index_generation{0};
        uint32_t page_catalog_begin{0};
        uint32_t page_count{0};
    };

    // One (key, rid) pair the index repair has to reconcile. `key_slot` indexes
    // the fixed-stride key arena of the index being repaired.
    struct IndexRepairEntry {
        uint32_t key_slot{0};
        Rid rid{};
        bool from_heap{false}; // true: must be present afterwards
    };

    // Analyze records immutable WAL-aligned ranges for the later key scan.
    // They hold only record boundaries and DML identity, never WAL payloads.
    struct IndexKeyStreamSegment {
        int64_t begin{0};
        int64_t end{0};
        uint64_t identity{0};
        uint32_t dml_records{0};
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

    // Index repair phases run independently per index. Keep their counters
    // worker-local and merge after every worker has joined; recovery metrics
    // are otherwise ordinary integers rather than synchronization primitives.
    struct IndexRepairMetrics {
        uint64_t probes{0};
        uint64_t mutations{0};
        uint64_t unchanged_keys{0};
        uint64_t duplicate_entries{0};
        uint64_t parent_pointer_repairs{0};
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
    const WalRecordLocation* location_of_lsn(lsn_t lsn) const;
    void build_touched_index();
    void finalize_touched_pages();
    WalRecordView mapped_heap_redo_record(const HeapRedoRecord& location, const char* record_bytes) const;

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
    bool redo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table,
                     bool defer_on_active_loser = true);
    bool redo_update_delta(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table,
                           bool defer_on_active_loser);
    void replay_deferred_committed_deltas();
    void undo_insert(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void undo_delete(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void undo_update(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void undo_update_delta(const WalRecordView& record, const WalDmlView& dml, RecoveryTable& table);
    void repair_touched_file_headers();
    void reset_touched_tuple_meta();
    std::unordered_set<std::string> repair_touched_indexes();
    // Names the indexes a touched table owns, without reading any key yet, and
    // resolves each one's IndexMeta and handle so no later phase has to.
    void plan_touched_indexes(std::map<std::string, IndexRepairPlan>* plans);
    // Publishes the surviving plans into RecoveryTable::index_plans, so the key
    // collection walks pointers instead of rebuilding index names.
    void bind_index_plans(std::map<std::string, IndexRepairPlan>* plans);
    // Collects, per index, every key the WAL mentions for a touched RID plus
    // the key its final live tuple carries. This is a complete sequential walk
    // of the accepted WAL prefix: retaining an all-DML catalogue for this
    // optional phase made recovery RSS scale with the WAL rather than the work
    // needed by redo/undo.
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
    bool gate_index_change_set(IndexRepairPlan* plan, RecoveryIndexGate::Stats* totals, IndexRepairMetrics* metrics);
    // Applies one index's plan; returns false when the index must be rebuilt.
    // Requires sort_index_repair_entries() to have run on the plan.
    bool apply_index_repair_plan(IndexRepairPlan* plan, IndexRepairMetrics* metrics);
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
    std::vector<HeapRedoRecord> heap_redo_records_;
    // Exact DML stream identity accepted by analyze. It remains available when
    // the optional index-key descriptor catalogue reaches its hard byte cap,
    // so the streaming fallback cannot silently use a changed/truncated WAL.
    uint64_t index_key_stream_identity_{0};
    uint64_t index_key_stream_records_{0};
    std::vector<IndexKeyStreamSegment> index_key_stream_segments_;
    bool index_key_stream_segments_complete_{true};
    size_t index_key_parallel_scratch_limit_for_test_{0};
    // txn id -> most recent descriptor index plus one. Descriptor links carry
    // the rest of each transaction's chain in 24 bytes per DML.
    std::unordered_map<txn_id_t, uint32_t> heap_redo_txn_heads_;
    std::vector<IndexSmoRecord> index_smo_records_; // WAL order, no third full scan
    std::vector<page_id_t> index_smo_page_catalog_; // flat, descriptor-addressed SMO page numbers
    // A delta cannot safely patch a tuple still owned by any active loser: the
    // page does not identify which WAL version supplied its unchanged bytes.
    // Keep the committed delta descriptors in WAL order until loser undo. A
    // later full-image DML erases the entry because it is a complete anchor.
    std::map<TouchedTuple, std::vector<HeapRedoRecord>> deferred_committed_deltas_;
    // Every analyzed record gets an identity entry because loser undo follows
    // prev_lsn links after analyze has released the streaming reader.
    std::vector<WalRecordLocation> record_locations_;
    std::unordered_set<std::string> touched_tables_;
    bool has_dml_records_{false};
    bool has_index_smo_records_{false};
    bool pages_prepared_for_redo_{false};
    std::unordered_map<std::string, uint64_t> latest_index_bindings_;
    lsn_t max_lsn_{INVALID_LSN}; // analyze 扫描到的最大 lsn，用于 recovery 后推进 global_lsn
    int64_t checkpoint_offset_{0};
    int64_t scan_begin_offset_{0}; // first record analyze/redo replay
    int64_t scan_end_offset_{0};   // end of the intact WAL prefix
    // Complete, gap-free record-boundary catalogue over
    // [scan_begin_offset_, scan_end_offset_). The immediately following
    // recovery phase may consume it for parallel work; Phase A only produces
    // it and keeps semantic analysis serial.
    std::vector<FramedSegment> framed_segments_;

    uint64_t scanned_record_count_{0};
    std::array<uint64_t, kLogTypeCount> log_type_record_counts_{};
    std::array<uint64_t, kLogTypeCount> log_type_serialized_bytes_{};
    uint64_t index_smo_logical_image_count_{0};
    uint64_t index_smo_logical_image_bytes_{0};
    uint64_t redo_applied_count_{0};
    uint64_t redo_skipped_count_{0};       // committed records with no open table
    uint64_t redo_missing_table_count_{0}; // subset of the above whose table is not open
    uint64_t redo_candidate_count_{0};     // committed descriptors after compaction
    uint64_t redo_loser_count_{0};         // DML retained for undo, not heap redo
    uint64_t redo_resident_page_run_count_{0};
    uint64_t redo_resident_page_pin_count_{0};
    uint64_t undo_applied_count_{0};
    uint64_t pruned_no_undo_transaction_count_{0};
    uint64_t undo_chain_record_read_count_{0};
    uint64_t index_probe_count_{0};
    uint64_t index_mutation_count_{0};
    uint64_t index_unchanged_key_count_{0};
    uint64_t index_duplicate_entry_count_{0};
    uint64_t index_rebuild_count_{0};
    uint64_t index_parent_pointer_repair_count_{0};
    uint64_t index_smo_prepare_count_{0};

    // 计数器恢复的两个来源，见 get_recovered_next_timestamp()。
    timestamp_t persisted_next_timestamp_{0};
    txn_id_t persisted_next_txn_id_{0};
    timestamp_t max_wal_commit_ts_{INVALID_TS};
    txn_id_t max_wal_txn_id_{INVALID_TXN_ID};

    IndexSmoRedoTestHook index_smo_redo_test_hook_;
    HeapRedoRecordTestHook heap_redo_record_test_hook_;
    RecoveryFinalizePinTestHook recovery_finalize_pin_test_hook_;

    DiskManager* disk_manager_;              // 用来读写文件
    BufferPoolManager* buffer_pool_manager_; // 对页面进行读写
    SmManager* sm_manager_;                  // 访问数据库元数据
    LogManager* log_manager_;                // recovery 完成后截断日志
};
