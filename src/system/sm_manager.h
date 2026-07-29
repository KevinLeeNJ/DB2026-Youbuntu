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
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>

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
    DbMeta db_; // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>>
        fhs_; // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>>
        ihs_; // file name -> index file handle, 当前数据库中每个索引的文件
private:
    struct RidHash {
        std::size_t operator()(const Rid& rid) const noexcept {
            const auto page = static_cast<std::uint32_t>(rid.page_no);
            const auto slot = static_cast<std::uint32_t>(rid.slot_no);
            return static_cast<std::size_t>((static_cast<std::uint64_t>(page) << 32U) | slot);
        }
    };

    struct RetireEntryState {
        Rid rid;
        std::uint64_t token;
        std::uint64_t generation;
        timestamp_t retry_after_ts;
    };

    struct DeletedRetireEntryState {
        Rid rid;
        std::uint64_t token;
        std::uint64_t generation;
        timestamp_t retry_after_ts;
        std::uint64_t tuple_hash;
    };

    struct DeletedTupleCandidateTable {
        std::unordered_map<Rid, DeletedRetireEntryState, RidHash> entries;
        std::unordered_multimap<std::uint64_t, Rid> hashed_rids;
        std::unordered_set<Rid, RidHash> unhashed_rids;

        void add_to_hash_index(const Rid& rid, std::uint64_t tuple_hash) {
            if (tuple_hash == 0) {
                unhashed_rids.insert(rid);
            } else {
                hashed_rids.emplace(tuple_hash, rid);
            }
        }

        void remove_from_hash_index(const Rid& rid, std::uint64_t tuple_hash) {
            if (tuple_hash == 0) {
                unhashed_rids.erase(rid);
                return;
            }
            auto [begin, end] = hashed_rids.equal_range(tuple_hash);
            for (auto entry = begin; entry != end; ++entry) {
                if (entry->second == rid) {
                    hashed_rids.erase(entry);
                    return;
                }
            }
        }
    };

    struct HistoricalKeyLess {
        std::vector<ColType> col_types;
        std::vector<int> col_lens;

        bool operator()(const std::string& lhs, const std::string& rhs) const {
            return ix_compare(lhs.data(), rhs.data(), col_types, col_lens) < 0;
        }
    };

    struct HistoricalIndexBucket {
        using EntryStates = std::vector<std::shared_ptr<RetireEntryState>>;
        using EntryMap = std::map<std::string, EntryStates, HistoricalKeyLess>;

        EntryMap entries;

        HistoricalIndexBucket() = default;
        HistoricalIndexBucket(std::vector<ColType> types, std::vector<int> lens)
            : entries(HistoricalKeyLess{std::move(types), std::move(lens)}) {}
    };
    struct HistoricalRetireCandidate {
        std::string bucket_key;
        HistoricalIndexBucket* bucket;
        HistoricalIndexBucket::EntryMap::iterator entry;
        std::shared_ptr<RetireEntryState> state;
        Rid rid;
        std::uint64_t generation;
        timestamp_t last_observed_ts{INVALID_TS};
    };
    struct DeletedRetireCandidate {
        std::string tab_name;
        Rid rid;
        std::uint64_t token;
        std::uint64_t generation;
        timestamp_t last_observed_ts{INVALID_TS};
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

    mutable std::shared_mutex historical_index_keys_latch_;
    std::unordered_map<std::string, HistoricalIndexBucket> historical_index_keys_;
    std::unordered_map<std::uint64_t, std::shared_ptr<RetireEntryState>> historical_retire_states_;
    std::deque<HistoricalRetireCandidate> historical_retire_queue_;
    std::map<timestamp_t, std::deque<HistoricalRetireCandidate>> historical_deferred_retire_queue_;
    bool historical_retire_prefer_deferred_{false};
    std::uint64_t next_historical_retire_token_{1};
    mutable std::mutex deleted_tuple_candidates_latch_;
    std::unordered_map<std::string, DeletedTupleCandidateTable> deleted_tuple_candidates_;
    std::deque<DeletedRetireCandidate> deleted_retire_queue_;
    std::map<timestamp_t, std::deque<DeletedRetireCandidate>> deleted_deferred_retire_queue_;
    bool deleted_retire_prefer_deferred_{false};
    std::uint64_t next_deleted_retire_token_{1};

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
                                       const std::vector<char>& key, const Rid& rid, const IndexMeta& index,
                                       timestamp_t retry_after_ts = INVALID_TS) {
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
        auto [key_it, _] = bucket_it->second.entries.try_emplace(std::string(key.data(), key.size()));
        auto& states = key_it->second;
        auto entry =
            std::find_if(states.begin(), states.end(), [&rid](const auto& state) { return state->rid == rid; });
        if (entry == states.end()) {
            const std::uint64_t token = next_historical_retire_token_++;
            auto state = std::make_shared<RetireEntryState>(RetireEntryState{rid, token, 1, retry_after_ts});
            states.push_back(state);
            historical_retire_states_.emplace(token, state);
            HistoricalRetireCandidate candidate{bucket_key, &bucket_it->second, key_it, std::move(state), rid,
                                                1,          retry_after_ts};
            if (retry_after_ts == INVALID_TS) {
                historical_retire_queue_.push_back(std::move(candidate));
            } else {
                historical_deferred_retire_queue_[retry_after_ts].push_back(std::move(candidate));
            }
        } else {
            ++(*entry)->generation;
            if (retry_after_ts != INVALID_TS &&
                ((*entry)->retry_after_ts == INVALID_TS || retry_after_ts > (*entry)->retry_after_ts)) {
                (*entry)->retry_after_ts = retry_after_ts;
            }
        }
    }

    std::vector<Rid> get_historical_index_key_rids(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& key) const {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        if (it == historical_index_keys_.end()) {
            return {};
        }
        auto key_it = it->second.entries.find(std::string(key.data(), key.size()));
        if (key_it == it->second.entries.end()) {
            return {};
        }
        std::vector<Rid> result;
        result.reserve(key_it->second.size());
        for (const auto& entry : key_it->second) {
            result.push_back(entry->rid);
        }
        return result;
    }

    std::vector<Rid> get_historical_index_rids(const std::string& tab_name, const std::string& index_name) const {
        std::vector<Rid> result;
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        if (it == historical_index_keys_.end()) {
            return result;
        }
        for (const auto& [_, entries] : it->second.entries) {
            for (const auto& entry : entries) {
                result.push_back(entry->rid);
            }
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
            for (const auto& state : entry->second) {
                result.push_back(state->rid);
            }
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
            for (const auto& state : entry->second) {
                out.emplace_back(entry->first, state->rid);
            }
        }
    }

    bool has_historical_index_keys(const std::string& tab_name, const std::string& index_name) const {
        std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
        return it != historical_index_keys_.end() && !it->second.entries.empty();
    }

    void remember_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid,
                                          timestamp_t retry_after_ts = INVALID_TS, std::uint64_t tuple_hash = 0) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto& entries = deleted_tuple_candidates_[tab_name];
        auto entry = entries.entries.find(rid);
        if (entry == entries.entries.end()) {
            const std::uint64_t token = next_deleted_retire_token_++;
            entries.entries.emplace(rid, DeletedRetireEntryState{rid, token, 1, retry_after_ts, tuple_hash});
            entries.add_to_hash_index(rid, tuple_hash);
            DeletedRetireCandidate candidate{tab_name, rid, token, 1, retry_after_ts};
            if (retry_after_ts == INVALID_TS) {
                deleted_retire_queue_.push_back(std::move(candidate));
            } else {
                deleted_deferred_retire_queue_[retry_after_ts].push_back(std::move(candidate));
            }
        } else {
            ++entry->second.generation;
            if (retry_after_ts != INVALID_TS &&
                (entry->second.retry_after_ts == INVALID_TS || retry_after_ts > entry->second.retry_after_ts)) {
                entry->second.retry_after_ts = retry_after_ts;
            }
            if (tuple_hash != 0 && tuple_hash != entry->second.tuple_hash) {
                entries.remove_from_hash_index(rid, entry->second.tuple_hash);
                entry->second.tuple_hash = tuple_hash;
                entries.add_to_hash_index(rid, tuple_hash);
            }
        }
    }

    std::vector<Rid> get_deleted_tuple_candidates(const std::string& tab_name, std::uint64_t tuple_hash = 0) const {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto it = deleted_tuple_candidates_.find(tab_name);
        if (it == deleted_tuple_candidates_.end()) {
            return {};
        }
        std::vector<Rid> result;
        const auto& table = it->second;
        if (tuple_hash == 0) {
            result.reserve(table.entries.size());
            for (const auto& [rid, _] : table.entries) {
                result.push_back(rid);
            }
            return result;
        }
        auto [hashed_begin, hashed_end] = table.hashed_rids.equal_range(tuple_hash);
        result.reserve(static_cast<size_t>(std::distance(hashed_begin, hashed_end)) + table.unhashed_rids.size());
        for (auto entry = hashed_begin; entry != hashed_end; ++entry) {
            result.push_back(entry->second);
        }
        result.insert(result.end(), table.unhashed_rids.begin(), table.unhashed_rids.end());
        return result;
    }

    void remove_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        auto it = deleted_tuple_candidates_.find(tab_name);
        if (it == deleted_tuple_candidates_.end()) {
            return;
        }
        auto entry = it->second.entries.find(rid);
        if (entry == it->second.entries.end()) {
            return;
        }
        it->second.remove_from_hash_index(rid, entry->second.tuple_hash);
        it->second.entries.erase(entry);
        if (it->second.entries.empty()) {
            deleted_tuple_candidates_.erase(it);
        }
    }

    /** @brief 按水位线回收版本链相关历史结构。
     *  移除当前 tuple 版本已提交且 commit_ts 严格小于水位线的 RID：
     *  此时任何活跃事务（read_ts >= 水位线）都不会再回溯该 RID 的版本链，
     *  对应的历史索引键/删除候选不再被冲突检测访问，可安全删除。
     *  由 TransactionManager::GarbageCollection 在 txn_map 回收后调用。 */
    void prune_version_history(timestamp_t watermark);

    bool flush_all_table_and_index_pages(bool wal_preflushed = false);
    bool flush_recovery_pages(const std::unordered_set<std::string>& table_names);
    bool flush_dirty_data_pages(bool wal_preflushed = false);

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
        modified_slots.erase(std::unique(modified_slots.begin(), modified_slots.end()), modified_slots.end());

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
