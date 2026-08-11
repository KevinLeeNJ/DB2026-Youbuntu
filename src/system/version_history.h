/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "index/ix.h"
#include "record/rm_file_handle.h"

class SmManager;

class VersionHistory {
public:
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

    explicit VersionHistory(SmManager& sm_manager) : sm_manager_(sm_manager) {}

    void Clear();
    void ClearTable(const std::string& tab_name);
    void remember_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                       const std::vector<char>& key, const Rid& rid, const IndexMeta& index);
    std::vector<Rid> get_historical_index_key_rids(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& key) const;
    std::vector<Rid> get_historical_index_rids(const std::string& tab_name, const std::string& index_name) const;
    std::vector<Rid> get_historical_index_rids_in_range(const std::string& tab_name, const std::string& index_name,
                                                        const std::vector<char>& lower, const std::vector<char>& upper,
                                                        bool lower_exclusive, bool upper_inclusive) const;
    void collect_historical_index_entries_in_range(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& lower, const std::vector<char>& upper,
                                                   bool lower_exclusive, bool upper_inclusive,
                                                   std::vector<std::pair<std::string, Rid>>& out) const;
    bool has_historical_index_keys(const std::string& tab_name, const std::string& index_name) const;
    void remember_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid, const RmRecord& record,
                                          const TupleMeta& tombstone);
    void remember_deleted_tuple_candidate_for_test(const std::string& tab_name, const Rid& rid,
                                                   const std::string& record_bytes, const TupleMeta& tombstone);
    std::vector<DeletedTupleCandidate> get_deleted_tuple_candidates(const std::string& tab_name,
                                                                    const RmRecord& record);
    void remove_deleted_tuple_candidate_if_current(const std::string& tab_name, const RmRecord& record,
                                                   const DeletedTupleCandidate& candidate);
    void set_deleted_tuple_candidate_test_hash_override(std::optional<size_t> forced_hash);
    void prune(timestamp_t watermark);

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
        std::optional<timestamp_t> retry_after_watermark;
    };
    struct HistoricalKeyLess {
        std::vector<ColType> col_types;
        std::vector<int> col_lens;
        bool operator()(const std::string& lhs, const std::string& rhs) const {
            return ix_compare(lhs.data(), rhs.data(), col_types, col_lens) < 0;
        }
    };
    struct HistoricalIndexBucket {
        enum class RetireState : uint8_t { Queued, InFlight, Deferred };
        struct Entry {
            Rid rid;
            uint64_t generation{0};
            RetireState retire_state{RetireState::Queued};
        };
        std::map<std::string, std::vector<Entry>, HistoricalKeyLess> entries;
        HistoricalIndexBucket() = default;
        HistoricalIndexBucket(std::vector<ColType> types, std::vector<int> lens)
            : entries(HistoricalKeyLess{std::move(types), std::move(lens)}) {}
    };
    struct HistoricalRetireCandidate {
        std::string bucket_key;
        std::string encoded_key;
        Rid rid;
        uint64_t generation{0};
        std::optional<timestamp_t> retry_after_watermark;
    };
    struct HistoricalRetireCandidateCompare {
        bool operator()(const HistoricalRetireCandidate& lhs, const HistoricalRetireCandidate& rhs) const {
            return lhs.retry_after_watermark > rhs.retry_after_watermark;
        }
    };
    struct DeletedTupleRetireCandidateCompare {
        bool operator()(const DeletedTupleRetireCandidate& lhs, const DeletedTupleRetireCandidate& rhs) const {
            return lhs.retry_after_watermark > rhs.retry_after_watermark;
        }
    };
    struct HistoricalRetireKey {
        std::string bucket_key;
        std::string encoded_key;
        Rid rid;
        friend bool operator==(const HistoricalRetireKey& a, const HistoricalRetireKey& b) {
            return a.bucket_key == b.bucket_key && a.encoded_key == b.encoded_key && a.rid == b.rid;
        }
    };
    struct HistoricalRetireKeyHash {
        size_t operator()(const HistoricalRetireKey& key) const noexcept;
    };
    using DeletedTupleBucket =
        std::unordered_map<DeletedTupleRowKey, std::vector<DeletedTupleCandidate>, DeletedTupleRowKeyHash>;

    static std::string make_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                                 const std::vector<char>& key);
    static bool historical_bucket_belongs_to_table(const std::string& bucket_key, const std::string& tab_name);
    static HistoricalRetireKey make_historical_retire_key(const HistoricalRetireCandidate& candidate) {
        return HistoricalRetireKey{candidate.bucket_key, candidate.encoded_key, candidate.rid};
    }
    DeletedTupleRowKey make_deleted_tuple_row_key(const char* data, size_t size) const;
    static bool deleted_tuple_candidate_matches_meta(const DeletedTupleCandidate& candidate, const TupleMeta& meta);
    bool erase_deleted_tuple_candidate_locked(const std::string& tab_name, const DeletedTupleRowKey& row_key,
                                              const DeletedTupleCandidate& candidate);
    void clear_deleted_tuple_candidates_locked();

    SmManager& sm_manager_;
    mutable std::shared_mutex historical_index_keys_latch_;
    std::unordered_map<std::string, HistoricalIndexBucket> historical_index_keys_;
    std::deque<HistoricalRetireCandidate> historical_retire_queue_;
    std::priority_queue<HistoricalRetireCandidate, std::vector<HistoricalRetireCandidate>,
                        HistoricalRetireCandidateCompare>
        historical_deferred_retire_queue_;
    std::unordered_set<HistoricalRetireKey, HistoricalRetireKeyHash> historical_queued_generations_;
    uint64_t next_historical_index_generation_{1};
    mutable std::mutex deleted_tuple_candidates_latch_;
    std::unordered_map<std::string, DeletedTupleBucket> deleted_tuple_candidates_;
    std::deque<DeletedTupleRetireCandidate> deleted_tuple_retire_queue_;
    std::priority_queue<DeletedTupleRetireCandidate, std::vector<DeletedTupleRetireCandidate>,
                        DeletedTupleRetireCandidateCompare>
        deleted_tuple_deferred_retire_queue_;
    uint64_t next_deleted_tuple_candidate_id_{1};
    std::optional<size_t> deleted_tuple_candidate_test_hash_override_;
};
