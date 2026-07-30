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
#include <atomic>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <optional>

#include "common/context.h"
#include "common/fault_injection.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"

class Context;

struct ColDef {
    std::string name; // Column name
    ColType type;     // Type of column
    int len;          // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
public:
    // A deleted tuple is indexed by its complete on-page record bytes.  The
    // cached hash is an unordered_map accelerator only: equality always
    // compares the complete byte string, including the NULL bitmap, CHAR
    // padding and FLOAT32 representation.
    struct DeletedTupleRowKey {
        std::string bytes;
        size_t hash{0};

        friend bool operator==(const DeletedTupleRowKey& lhs, const DeletedTupleRowKey& rhs) {
            return lhs.bytes == rhs.bytes;
        }
    };

    struct DeletedTupleCandidate {
        uint64_t candidate_id{0};
        Rid rid;
        txn_id_t writer_txn_id{INVALID_TXN_ID};
        UndoLink version_chain_head;
    };

    struct DeletedTupleCandidateStats {
        uint64_t lookups{0};
        uint64_t exact_hits{0};
        uint64_t exact_misses{0};
        uint64_t validated_candidates{0};
        uint64_t page_probes{0};
        uint64_t active_candidates{0};
        uint64_t active_buckets{0};
        uint64_t retire_pops{0};
        uint64_t retire_requeues{0};
        uint64_t retire_prunes{0};
        uint64_t retire_max_queue{0};
    };

    DbMeta db_; // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>>
        fhs_; // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>>
        ihs_; // file name -> index file handle, 当前数据库中每个索引的文件
private:
    struct DeletedTupleRowKeyHash {
        size_t operator()(const DeletedTupleRowKey& key) const noexcept {
            return key.hash;
        }
    };

    struct DeletedTupleRetireCandidate {
        std::string tab_name;
        DeletedTupleRowKey row_key;
        DeletedTupleCandidate candidate;
    };

    struct HistoricalKeyLess {
        std::vector<ColType> col_types;
        std::vector<int> col_lens;

        bool operator()(const std::string& lhs, const std::string& rhs) const {
            return ix_compare(lhs.data(), rhs.data(), col_types, col_lens) < 0;
        }
    };

    struct HistoricalIndexBucket {
        std::map<std::string, std::vector<Rid>, HistoricalKeyLess> entries;

        HistoricalIndexBucket() = default;
        HistoricalIndexBucket(std::vector<ColType> types, std::vector<int> lens)
            : entries(HistoricalKeyLess{std::move(types), std::move(lens)}) {}
    };
    struct HistoricalRetireCandidate {
        std::string bucket_key;
        std::string encoded_key;
        Rid rid;

        friend bool operator==(const HistoricalRetireCandidate& a, const HistoricalRetireCandidate& b) {
            return a.bucket_key == b.bucket_key && a.encoded_key == b.encoded_key && a.rid == b.rid;
        }
    };

    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    RmManager* rm_manager_;
    IxManager* ix_manager_;
    // Catalog lifetime is guarded independently from data/index/page latches.
    // Callers must acquire this outermost, before starting work that can take
    // transaction, row, index, page, or WAL locks.
    mutable std::shared_mutex catalog_latch_;
    std::atomic<std::uint64_t> catalog_generation_{0};
    // Runtime-only table incarnations make transient logical-row reservations
    // immune to a DROP/CREATE reusing a name or file descriptor. They are
    // protected by catalog_latch_ and intentionally never persisted.
    std::unordered_map<std::string, uint64_t> table_runtime_ids_;
    uint64_t next_table_runtime_id_{1};

    void bump_catalog_generation() noexcept {
        catalog_generation_.fetch_add(1, std::memory_order_release);
    }
    static std::string make_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                                 const std::vector<char>& key) {
        std::string combined;
        combined.reserve(tab_name.size() + index_name.size() + key.size() + 2);
        combined.append(tab_name);
        combined.push_back('\0');
        combined.append(index_name);
        combined.push_back('\0');
        combined.append(key.data(), key.size());
        return combined;
    }
    static bool historical_bucket_belongs_to_table(const std::string& bucket_key, const std::string& tab_name) {
        return bucket_key.size() > tab_name.size() && bucket_key.compare(0, tab_name.size(), tab_name) == 0 &&
               bucket_key[tab_name.size()] == '\0';
    }
    void clear_table_runtime_history(const std::string& tab_name);

    mutable std::shared_mutex historical_index_keys_latch_;
    std::unordered_map<std::string, HistoricalIndexBucket> historical_index_keys_;
    std::deque<HistoricalRetireCandidate> historical_retire_queue_;
    mutable std::mutex deleted_tuple_candidates_latch_;
    using DeletedTupleBucket =
        std::unordered_map<DeletedTupleRowKey, std::vector<DeletedTupleCandidate>, DeletedTupleRowKeyHash>;
    std::unordered_map<std::string, DeletedTupleBucket> deleted_tuple_candidates_;
    std::deque<DeletedTupleRetireCandidate> deleted_tuple_retire_queue_;
    uint64_t next_deleted_tuple_candidate_id_{1};

    // These counters are intentionally observational only. They are not part
    // of any persistent state or conflict decision, and callers may leave
    // diagnostics disabled in production.
    std::atomic<bool> deleted_tuple_candidate_diagnostics_enabled_{false};
    std::atomic<uint64_t> deleted_tuple_candidate_lookups_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_exact_hits_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_exact_misses_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_validated_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_page_probes_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_active_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_active_buckets_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_retire_pops_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_retire_requeues_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_retire_prunes_{0};
    std::atomic<uint64_t> deleted_tuple_candidate_retire_max_queue_{0};
    std::optional<size_t> deleted_tuple_candidate_test_hash_override_;

    DeletedTupleRowKey make_deleted_tuple_row_key(const char* data, size_t size) const {
        DeletedTupleRowKey key;
        key.bytes.assign(data, size);
        key.hash = deleted_tuple_candidate_test_hash_override_.value_or(std::hash<std::string>{}(key.bytes));
        return key;
    }

    static bool deleted_tuple_candidate_matches_meta(const DeletedTupleCandidate& candidate, const TupleMeta& meta) {
        return meta.is_deleted_ && candidate.writer_txn_id == meta.writer_txn_id_ &&
               candidate.version_chain_head == meta.version_chain_head_;
    }

    void update_deleted_tuple_retire_max_locked() {
        const uint64_t size = deleted_tuple_retire_queue_.size();
        uint64_t observed = deleted_tuple_candidate_retire_max_queue_.load(std::memory_order_relaxed);
        while (size > observed && !deleted_tuple_candidate_retire_max_queue_.compare_exchange_weak(
                                      observed, size, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }

    bool erase_deleted_tuple_candidate_locked(const std::string& tab_name, const DeletedTupleRowKey& row_key,
                                              const DeletedTupleCandidate& candidate) {
        auto table_it = deleted_tuple_candidates_.find(tab_name);
        if (table_it == deleted_tuple_candidates_.end()) {
            return false;
        }
        auto bucket_it = table_it->second.find(row_key);
        if (bucket_it == table_it->second.end()) {
            return false;
        }
        auto& candidates = bucket_it->second;
        auto candidate_it = std::find_if(candidates.begin(), candidates.end(), [&](const auto& current) {
            return current.candidate_id == candidate.candidate_id && current.writer_txn_id == candidate.writer_txn_id &&
                   current.version_chain_head == candidate.version_chain_head;
        });
        if (candidate_it == candidates.end()) {
            return false;
        }
        candidates.erase(candidate_it);
        deleted_tuple_candidate_active_.fetch_sub(1, std::memory_order_relaxed);
        if (candidates.empty()) {
            table_it->second.erase(bucket_it);
            deleted_tuple_candidate_active_buckets_.fetch_sub(1, std::memory_order_relaxed);
        }
        if (table_it->second.empty()) {
            deleted_tuple_candidates_.erase(table_it);
        }
        return true;
    }

    void clear_deleted_tuple_candidates_locked() {
        deleted_tuple_candidates_.clear();
        deleted_tuple_retire_queue_.clear();
        deleted_tuple_candidate_lookups_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_exact_hits_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_exact_misses_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_validated_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_page_probes_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_active_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_active_buckets_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_retire_pops_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_retire_requeues_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_retire_prunes_.store(0, std::memory_order_relaxed);
        deleted_tuple_candidate_retire_max_queue_.store(0, std::memory_order_relaxed);
    }

public:
    using CatalogSharedGuard = std::shared_lock<std::shared_mutex>;
    using CatalogExclusiveGuard = std::unique_lock<std::shared_mutex>;

    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

    ~SmManager() {}

    // Database-global toggle controlling whether query results are appended to
    // output.txt. Shared across all client connections on this server process.
    // Reset to true on every open_db. Toggled by "set output_file on|off".
    bool output_file_enabled_{true};

    BufferPoolManager* get_bpm() {
        return buffer_pool_manager_;
    }

    RmManager* get_rm_manager() {
        return rm_manager_;
    }

    IxManager* get_ix_manager() {
        return ix_manager_;
    }

    CatalogSharedGuard acquire_catalog_shared() const {
        return CatalogSharedGuard(catalog_latch_);
    }

    CatalogExclusiveGuard acquire_catalog_exclusive() {
        return CatalogExclusiveGuard(catalog_latch_);
    }

    // The caller must hold either catalog guard. Keeping this accessor
    // non-locking avoids recursively locking the non-recursive shared_mutex.
    const std::string& get_database_identity_under_catalog_guard() const noexcept {
        return db_.name_;
    }

    std::uint64_t get_catalog_generation() const noexcept {
        return catalog_generation_.load(std::memory_order_acquire);
    }

    uint64_t get_table_runtime_id_under_catalog_guard(const std::string& tab_name) const {
        auto it = table_runtime_ids_.find(tab_name);
        if (it == table_runtime_ids_.end()) {
            throw TableNotFoundError(tab_name);
        }
        return it->second;
    }

    bool is_dir(const std::string& db_name);

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context* context);

    void show_index(const std::string& tab_name, Context* context);

    void desc_table(const std::string& tab_name, Context* context);

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);

    void insert_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& rec);

    void delete_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec);

    void update_record_with_indexes(const std::string& tab_name, const Rid& rid, const RmRecord& old_rec,
                                    const RmRecord& new_rec);

    void remember_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                       const std::vector<char>& key, const Rid& rid, const IndexMeta& index) {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        std::vector<ColType> col_types;
        std::vector<int> col_lens;
        col_types.reserve(index.cols.size());
        col_lens.reserve(index.cols.size());
        for (const auto& col : index.cols) {
            col_types.push_back(col.type);
            col_lens.push_back(col.len);
        }
        auto bucket_key = make_historical_index_key(tab_name, index_name, {});
        auto bucket_it = historical_index_keys_.find(bucket_key);
        if (bucket_it == historical_index_keys_.end()) {
            bucket_it = historical_index_keys_
                            .emplace(bucket_key, HistoricalIndexBucket(std::move(col_types), std::move(col_lens)))
                            .first;
        }
        auto& rids = bucket_it->second.entries[std::string(key.data(), key.size())];
        if (std::find(rids.begin(), rids.end(), rid) == rids.end()) {
            rids.push_back(rid);
        }
        historical_retire_queue_.push_back(
            HistoricalRetireCandidate{bucket_key, std::string(key.data(), key.size()), rid});
    }

    std::vector<Rid> get_historical_index_key_rids(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& key) const {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        if (it == historical_index_keys_.end()) {
            return {};
        }
        auto key_it = it->second.entries.find(std::string(key.data(), key.size()));
        return key_it == it->second.entries.end() ? std::vector<Rid>{} : key_it->second;
    }

    std::vector<Rid> get_historical_index_rids(const std::string& tab_name, const std::string& index_name) const {
        std::vector<Rid> result;
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        if (it == historical_index_keys_.end()) {
            return result;
        }
        for (const auto& [_, rids] : it->second.entries) {
            result.insert(result.end(), rids.begin(), rids.end());
        }
        return result;
    }

    std::vector<Rid> get_historical_index_rids_in_range(const std::string& tab_name, const std::string& index_name,
                                                        const std::vector<char>& lower, const std::vector<char>& upper,
                                                        bool lower_exclusive, bool upper_inclusive) const {
        std::vector<Rid> result;
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        if (it == historical_index_keys_.end()) {
            return result;
        }
        const auto lower_key = std::string(lower.data(), lower.size());
        const auto upper_key = std::string(upper.data(), upper.size());
        auto begin =
            lower_exclusive ? it->second.entries.upper_bound(lower_key) : it->second.entries.lower_bound(lower_key);
        auto end =
            upper_inclusive ? it->second.entries.upper_bound(upper_key) : it->second.entries.lower_bound(upper_key);
        for (auto entry = begin; entry != end; ++entry) {
            result.insert(result.end(), entry->second.begin(), entry->second.end());
        }
        return result;
    }

    /** @brief Historical (key, rid) candidates inside a key range, in index-key
     *  order. The bucket is a std::map ordered by ix_compare, so the natural
     *  iteration order is the index order the caller needs in order to merge
     *  these candidates into an index range scan without destroying the scan's
     *  ordering (which min(col) pushdown depends on).
     *  Returning the key alongside the rid is what makes that merge possible;
     *  get_historical_index_rids_in_range() throws the key away. */
    void collect_historical_index_entries_in_range(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& lower, const std::vector<char>& upper,
                                                   bool lower_exclusive, bool upper_inclusive,
                                                   std::vector<std::pair<std::string, Rid>>& out) const {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        if (it == historical_index_keys_.end()) {
            return;
        }
        const auto lower_key = std::string(lower.data(), lower.size());
        const auto upper_key = std::string(upper.data(), upper.size());
        auto begin =
            lower_exclusive ? it->second.entries.upper_bound(lower_key) : it->second.entries.lower_bound(lower_key);
        auto end =
            upper_inclusive ? it->second.entries.upper_bound(upper_key) : it->second.entries.lower_bound(upper_key);
        for (auto entry = begin; entry != end; ++entry) {
            for (const Rid& rid : entry->second) {
                out.emplace_back(entry->first, rid);
            }
        }
    }

    bool has_historical_index_keys(const std::string& tab_name, const std::string& index_name) const {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        return it != historical_index_keys_.end() && !it->second.entries.empty();
    }

    void remember_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid, const RmRecord& record,
                                          const TupleMeta& tombstone) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto row_key = make_deleted_tuple_row_key(record.data, record.size);
        auto& buckets = deleted_tuple_candidates_[tab_name];
        auto [bucket_it, inserted] = buckets.try_emplace(row_key);
        if (inserted) {
            deleted_tuple_candidate_active_buckets_.fetch_add(1, std::memory_order_relaxed);
        }
        DeletedTupleCandidate candidate{next_deleted_tuple_candidate_id_++, rid, tombstone.writer_txn_id_,
                                        tombstone.version_chain_head_};
        bucket_it->second.push_back(candidate);
        deleted_tuple_candidate_active_.fetch_add(1, std::memory_order_relaxed);
        deleted_tuple_retire_queue_.push_back(DeletedTupleRetireCandidate{tab_name, std::move(row_key), candidate});
        update_deleted_tuple_retire_max_locked();
    }

    // Test-only entry point. A forced hash exercises the guarantee that hash
    // collisions cannot alter exact physical-row equality.
    void remember_deleted_tuple_candidate_for_test(const std::string& tab_name, const Rid& rid,
                                                   const std::string& record_bytes, const TupleMeta& tombstone) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto row_key = make_deleted_tuple_row_key(record_bytes.data(), record_bytes.size());
        auto& buckets = deleted_tuple_candidates_[tab_name];
        auto [bucket_it, inserted] = buckets.try_emplace(row_key);
        if (inserted) {
            deleted_tuple_candidate_active_buckets_.fetch_add(1, std::memory_order_relaxed);
        }
        DeletedTupleCandidate candidate{next_deleted_tuple_candidate_id_++, rid, tombstone.writer_txn_id_,
                                        tombstone.version_chain_head_};
        bucket_it->second.push_back(candidate);
        deleted_tuple_candidate_active_.fetch_add(1, std::memory_order_relaxed);
        deleted_tuple_retire_queue_.push_back(DeletedTupleRetireCandidate{tab_name, std::move(row_key), candidate});
        update_deleted_tuple_retire_max_locked();
    }

    std::vector<DeletedTupleCandidate> get_deleted_tuple_candidates(const std::string& tab_name,
                                                                    const RmRecord& record) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto row_key = make_deleted_tuple_row_key(record.data, record.size);
        if (deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed)) {
            deleted_tuple_candidate_lookups_.fetch_add(1, std::memory_order_relaxed);
        }
        auto table_it = deleted_tuple_candidates_.find(tab_name);
        if (table_it == deleted_tuple_candidates_.end()) {
            if (deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed)) {
                deleted_tuple_candidate_exact_misses_.fetch_add(1, std::memory_order_relaxed);
            }
            return {};
        }
        auto bucket_it = table_it->second.find(row_key);
        if (bucket_it == table_it->second.end()) {
            if (deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed)) {
                deleted_tuple_candidate_exact_misses_.fetch_add(1, std::memory_order_relaxed);
            }
            return {};
        }
        if (deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed)) {
            deleted_tuple_candidate_exact_hits_.fetch_add(1, std::memory_order_relaxed);
        }
        return bucket_it->second;
    }

    void remove_deleted_tuple_candidate_if_current(const std::string& tab_name, const RmRecord& record,
                                                   const DeletedTupleCandidate& candidate) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto row_key = make_deleted_tuple_row_key(record.data, record.size);
        erase_deleted_tuple_candidate_locked(tab_name, row_key, candidate);
    }

    void set_deleted_tuple_candidate_diagnostics_enabled(bool enabled) {
        deleted_tuple_candidate_diagnostics_enabled_.store(enabled, std::memory_order_relaxed);
    }

    void set_deleted_tuple_candidate_test_hash_override(std::optional<size_t> forced_hash) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        if (!deleted_tuple_candidates_.empty()) {
            throw InternalError("cannot change deleted tuple candidate hash while entries exist");
        }
        deleted_tuple_candidate_test_hash_override_ = forced_hash;
    }

    bool deleted_tuple_candidate_diagnostics_enabled() const {
        return deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed);
    }

    void note_deleted_tuple_candidate_validated(size_t page_probes) {
        if (!deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed)) {
            return;
        }
        deleted_tuple_candidate_validated_.fetch_add(1, std::memory_order_relaxed);
        deleted_tuple_candidate_page_probes_.fetch_add(page_probes, std::memory_order_relaxed);
    }

    void note_deleted_tuple_candidate_page_probe() {
        if (!deleted_tuple_candidate_diagnostics_enabled_.load(std::memory_order_relaxed)) {
            return;
        }
        deleted_tuple_candidate_page_probes_.fetch_add(1, std::memory_order_relaxed);
    }

    DeletedTupleCandidateStats get_deleted_tuple_candidate_stats() const {
        return DeletedTupleCandidateStats{deleted_tuple_candidate_lookups_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_exact_hits_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_exact_misses_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_validated_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_page_probes_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_active_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_active_buckets_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_retire_pops_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_retire_requeues_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_retire_prunes_.load(std::memory_order_relaxed),
                                          deleted_tuple_candidate_retire_max_queue_.load(std::memory_order_relaxed)};
    }

    /** @brief 按水位线回收版本链相关历史结构。
     *  移除当前 tuple 版本已提交且 commit_ts 严格小于水位线的 RID：
     *  此时任何活跃事务（read_ts >= 水位线）都不会再回溯该 RID 的版本链，
     *  对应的历史索引键/删除候选不再被冲突检测访问，可安全删除。
     *  由 TransactionManager::GarbageCollection 在 txn_map 回收后调用。 */
    void prune_version_history(timestamp_t watermark);

    bool flush_all_table_and_index_pages(FlushDependencyPolicy policy = FlushDependencyPolicy::Enforce());
    bool flush_recovery_pages(const std::unordered_set<std::string>& table_names);

    // Compatibility wrapper for the recovery scale harness. This remains
    // table-only and is not part of automatic checkpoint scheduling.
    size_t flush_dirty_pages(size_t max_pages);

    void rebuild_all_indexes();
    void rebuild_indexes(const std::unordered_set<std::string>& index_names);

    void refresh_index_residency();

    // Validate and freeze the publication set before a COMMIT record is made
    // durable. Publication must not discover a missing table or invalid RID
    // after durable COMMIT has already been written.
    void prepare_commit_publication(Transaction& txn) {
        auto& modified_slots = txn.get_modified_slots();
        std::sort(modified_slots.begin(), modified_slots.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.first != rhs.first) {
                return lhs.first < rhs.first;
            }
            if (lhs.second.page_no != rhs.second.page_no) {
                return lhs.second.page_no < rhs.second.page_no;
            }
            return lhs.second.slot_no < rhs.second.slot_no;
        });
        for (const auto& [tab_name, rid] : modified_slots) {
            auto table_it = fhs_.find(tab_name);
            if (table_it == fhs_.end()) {
                throw InternalError("commit publication table is missing: " + tab_name);
            }
            const auto file_hdr = table_it->second->get_file_hdr();
            if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr.num_pages || rid.slot_no < 0 ||
                rid.slot_no >= file_hdr.num_records_per_page) {
                throw InternalError("commit publication RID is invalid");
            }
        }
    }

    // MVCC: mark all slots modified by txn as committed with the given commit_ts
    void mark_slots_committed(Transaction& txn, timestamp_t commit_ts) {
        auto& modified_slots = txn.get_modified_slots();

        size_t offset = 0;
        while (offset < modified_slots.size()) {
            const auto& [tab_name, first_rid] = modified_slots[offset];
            auto table_it = fhs_.find(tab_name);
            size_t next = offset + 1;
            while (next < modified_slots.size() && modified_slots[next].first == tab_name &&
                   modified_slots[next].second.page_no == first_rid.page_no) {
                ++next;
            }
            if (table_it == fhs_.end()) {
                // Defensive check: prepare_commit_publication() should have
                // caught this before the COMMIT WAL became durable.
                throw InternalError("commit publication table is missing: " + tab_name);
            }
            const auto file_hdr = table_it->second->get_file_hdr();
            if (first_rid.page_no < RM_FIRST_RECORD_PAGE || first_rid.page_no >= file_hdr.num_pages) {
                throw InternalError("commit publication page is invalid");
            }
            for (size_t i = offset; i < next; ++i) {
                const Rid& rid = modified_slots[i].second;
                if (rid.slot_no < 0 || rid.slot_no >= file_hdr.num_records_per_page) {
                    throw InternalError("commit publication slot is invalid");
                }
            }
            auto page_handle = table_it->second->fetch_page_handle(first_rid.page_no);
            {
                std::unique_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                for (size_t i = offset; i < next; ++i) {
                    page_handle.get_meta(modified_slots[i].second.slot_no).is_committed_ = true;
                    page_handle.get_meta(modified_slots[i].second.slot_no).commit_ts_ = commit_ts;
                }
                if (txn.get_prev_lsn() != INVALID_LSN && page_handle.page->get_page_lsn() < txn.get_prev_lsn()) {
                    page_handle.page->set_page_lsn(txn.get_prev_lsn());
                }
                FaultInjector::Point("mid_tuple_publication");
            }
            buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
            offset = next;
        }
        txn.clear_modified_slots();
    }

    // Bulk-load a CSV file into an existing table. The path is relative to the
    // server's working directory. Reuses the insert path (WAL + index + MVCC
    // meta) in self-managed batched transactions, skipping conflict checks.
    void load_csv_data(const std::string& file_path, const std::string& tab_name, Context* context);
};
