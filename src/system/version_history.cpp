/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "version_history.h"

#include <cstring>

#include "system/sm_manager.h"

size_t VersionHistory::HistoricalRetireKeyHash::operator()(const HistoricalRetireKey& key) const noexcept {
    size_t hash = std::hash<std::string>{}(key.bucket_key);
    hash ^= std::hash<std::string>{}(key.encoded_key) + static_cast<size_t>(0x9e3779b9) + (hash << 6) + (hash >> 2);
    hash ^= std::hash<int>{}(key.rid.page_no) + static_cast<size_t>(0x9e3779b9) + (hash << 6) + (hash >> 2);
    return hash ^ (std::hash<int>{}(key.rid.slot_no) + static_cast<size_t>(0x9e3779b9) + (hash << 6) + (hash >> 2));
}

std::string VersionHistory::make_historical_index_key(const std::string& tab_name, const std::string& index_name,
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

bool VersionHistory::historical_bucket_belongs_to_table(const std::string& bucket_key, const std::string& tab_name) {
    return bucket_key.size() > tab_name.size() && bucket_key.compare(0, tab_name.size(), tab_name) == 0 &&
           bucket_key[tab_name.size()] == '\0';
}

VersionHistory::DeletedTupleRowKey VersionHistory::make_deleted_tuple_row_key(const char* data, size_t size) const {
    DeletedTupleRowKey key;
    key.bytes.assign(data, size);
    key.hash = deleted_tuple_candidate_test_hash_override_.value_or(std::hash<std::string>{}(key.bytes));
    return key;
}

bool VersionHistory::deleted_tuple_candidate_matches_meta(const DeletedTupleCandidate& candidate,
                                                          const TupleMeta& meta) {
    return meta.is_deleted_ && candidate.writer_txn_id == meta.writer_txn_id_ &&
           candidate.version_chain_head == meta.version_chain_head_;
}

bool VersionHistory::erase_deleted_tuple_candidate_locked(const std::string& tab_name,
                                                          const DeletedTupleRowKey& row_key,
                                                          const DeletedTupleCandidate& candidate) {
    auto table_it = deleted_tuple_candidates_.find(tab_name);
    if (table_it == deleted_tuple_candidates_.end())
        return false;
    auto bucket_it = table_it->second.find(row_key);
    if (bucket_it == table_it->second.end())
        return false;
    auto& candidates = bucket_it->second;
    auto candidate_it = std::find_if(candidates.begin(), candidates.end(), [&](const auto& current) {
        return current.candidate_id == candidate.candidate_id && current.writer_txn_id == candidate.writer_txn_id &&
               current.version_chain_head == candidate.version_chain_head;
    });
    if (candidate_it == candidates.end())
        return false;
    candidates.erase(candidate_it);
    if (candidates.empty())
        table_it->second.erase(bucket_it);
    if (table_it->second.empty())
        deleted_tuple_candidates_.erase(table_it);
    return true;
}

void VersionHistory::clear_deleted_tuple_candidates_locked() {
    deleted_tuple_candidates_.clear();
    deleted_tuple_retire_queue_.clear();
    deleted_tuple_deferred_retire_queue_ = {};
}

void VersionHistory::Clear() {
    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        historical_index_keys_.clear();
        historical_retire_queue_.clear();
        historical_queued_generations_.clear();
        historical_deferred_retire_queue_ = {};
        next_historical_index_generation_ = 1;
    }
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        clear_deleted_tuple_candidates_locked();
        next_deleted_tuple_candidate_id_ = 1;
        deleted_tuple_candidate_test_hash_override_.reset();
    }
}

void VersionHistory::ClearTable(const std::string& tab_name) {
    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        for (auto it = historical_index_keys_.begin(); it != historical_index_keys_.end();) {
            it = historical_bucket_belongs_to_table(it->first, tab_name) ? historical_index_keys_.erase(it)
                                                                         : std::next(it);
        }
        historical_retire_queue_.erase(std::remove_if(historical_retire_queue_.begin(), historical_retire_queue_.end(),
                                                      [&](const auto& candidate) {
                                                          return historical_bucket_belongs_to_table(
                                                              candidate.bucket_key, tab_name);
                                                      }),
                                       historical_retire_queue_.end());
        for (auto it = historical_queued_generations_.begin(); it != historical_queued_generations_.end();) {
            it = historical_bucket_belongs_to_table(it->bucket_key, tab_name) ? historical_queued_generations_.erase(it)
                                                                              : std::next(it);
        }
        std::priority_queue<HistoricalRetireCandidate, std::vector<HistoricalRetireCandidate>,
                            HistoricalRetireCandidateCompare>
            kept;
        while (!historical_deferred_retire_queue_.empty()) {
            auto candidate = historical_deferred_retire_queue_.top();
            historical_deferred_retire_queue_.pop();
            if (!historical_bucket_belongs_to_table(candidate.bucket_key, tab_name))
                kept.push(std::move(candidate));
        }
        historical_deferred_retire_queue_ = std::move(kept);
    }
    std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
    deleted_tuple_candidates_.erase(tab_name);
    deleted_tuple_retire_queue_.erase(
        std::remove_if(deleted_tuple_retire_queue_.begin(), deleted_tuple_retire_queue_.end(),
                       [&](const auto& candidate) { return candidate.tab_name == tab_name; }),
        deleted_tuple_retire_queue_.end());
    std::priority_queue<DeletedTupleRetireCandidate, std::vector<DeletedTupleRetireCandidate>,
                        DeletedTupleRetireCandidateCompare>
        kept;
    while (!deleted_tuple_deferred_retire_queue_.empty()) {
        auto candidate = deleted_tuple_deferred_retire_queue_.top();
        deleted_tuple_deferred_retire_queue_.pop();
        if (candidate.tab_name != tab_name)
            kept.push(std::move(candidate));
    }
    deleted_tuple_deferred_retire_queue_ = std::move(kept);
}

void VersionHistory::remember_historical_index_key(const std::string& tab_name, const std::string& index_name,
                                                   const std::vector<char>& key, const Rid& rid,
                                                   const IndexMeta& index) {
    std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
    auto bucket_key = make_historical_index_key(tab_name, index_name, {});
    auto bucket_it = historical_index_keys_.find(bucket_key);
    if (bucket_it == historical_index_keys_.end()) {
        std::vector<ColType> types;
        std::vector<int> lens;
        for (const auto& col : index.cols) {
            types.push_back(col.type);
            lens.push_back(col.len);
        }
        bucket_it =
            historical_index_keys_.emplace(bucket_key, HistoricalIndexBucket(std::move(types), std::move(lens))).first;
    }
    const std::string encoded_key(key.data(), key.size());
    auto& entries = bucket_it->second.entries[encoded_key];
    auto current = std::find_if(entries.begin(), entries.end(), [&](const auto& entry) { return entry.rid == rid; });
    const uint64_t generation = next_historical_index_generation_++;
    const HistoricalRetireKey retire_key{bucket_key, encoded_key, rid};
    if (current == entries.end()) {
        entries.push_back(HistoricalIndexBucket::Entry{rid, generation});
        historical_queued_generations_.insert(retire_key);
        historical_retire_queue_.push_back(HistoricalRetireCandidate{bucket_key, encoded_key, rid, generation, {}});
    } else {
        current->generation = generation;
        if (current->retire_state == HistoricalIndexBucket::RetireState::Queued &&
            historical_queued_generations_.insert(retire_key).second) {
            historical_retire_queue_.push_back(HistoricalRetireCandidate{bucket_key, encoded_key, rid, generation, {}});
        }
    }
}

std::vector<Rid> VersionHistory::get_historical_index_key_rids(const std::string& tab_name,
                                                               const std::string& index_name,
                                                               const std::vector<char>& key) const {
    std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
    auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
    if (it == historical_index_keys_.end())
        return {};
    auto key_it = it->second.entries.find(std::string(key.data(), key.size()));
    if (key_it == it->second.entries.end())
        return {};
    std::vector<Rid> result;
    result.reserve(key_it->second.size());
    for (const auto& entry : key_it->second)
        result.push_back(entry.rid);
    return result;
}

std::vector<Rid> VersionHistory::get_historical_index_rids(const std::string& tab_name,
                                                           const std::string& index_name) const {
    std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
    std::vector<Rid> result;
    auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
    if (it != historical_index_keys_.end())
        for (const auto& [_, entries] : it->second.entries)
            for (const auto& entry : entries)
                result.push_back(entry.rid);
    return result;
}

std::vector<Rid> VersionHistory::get_historical_index_rids_in_range(const std::string& tab_name,
                                                                    const std::string& index_name,
                                                                    const std::vector<char>& lower,
                                                                    const std::vector<char>& upper,
                                                                    bool lower_exclusive, bool upper_inclusive) const {
    std::vector<std::pair<std::string, Rid>> entries;
    collect_historical_index_entries_in_range(tab_name, index_name, lower, upper, lower_exclusive, upper_inclusive,
                                              entries);
    std::vector<Rid> result;
    result.reserve(entries.size());
    for (const auto& entry : entries)
        result.push_back(entry.second);
    return result;
}

void VersionHistory::collect_historical_index_entries_in_range(const std::string& tab_name,
                                                               const std::string& index_name,
                                                               const std::vector<char>& lower,
                                                               const std::vector<char>& upper, bool lower_exclusive,
                                                               bool upper_inclusive,
                                                               std::vector<std::pair<std::string, Rid>>& out) const {
    std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
    auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
    if (it == historical_index_keys_.end())
        return;
    const std::string lower_key(lower.data(), lower.size()), upper_key(upper.data(), upper.size());
    auto begin =
        lower_exclusive ? it->second.entries.upper_bound(lower_key) : it->second.entries.lower_bound(lower_key);
    auto end = upper_inclusive ? it->second.entries.upper_bound(upper_key) : it->second.entries.lower_bound(upper_key);
    for (; begin != end; ++begin)
        for (const auto& entry : begin->second)
            out.emplace_back(begin->first, entry.rid);
}

bool VersionHistory::has_historical_index_keys(const std::string& tab_name, const std::string& index_name) const {
    std::shared_lock<std::shared_mutex> lock(historical_index_keys_latch_);
    auto it = historical_index_keys_.find(make_historical_index_key(tab_name, index_name, {}));
    return it != historical_index_keys_.end() && !it->second.entries.empty();
}

void VersionHistory::remember_deleted_tuple_candidate(const std::string& tab_name, const Rid& rid,
                                                      const RmRecord& record, const TupleMeta& tombstone) {
    std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
    auto row_key = make_deleted_tuple_row_key(record.data, record.size);
    auto& bucket = deleted_tuple_candidates_[tab_name][row_key];
    DeletedTupleCandidate candidate{next_deleted_tuple_candidate_id_++, rid, tombstone.writer_txn_id_,
                                    tombstone.version_chain_head_};
    bucket.push_back(candidate);
    deleted_tuple_retire_queue_.push_back(DeletedTupleRetireCandidate{tab_name, std::move(row_key), candidate, {}});
}

void VersionHistory::remember_deleted_tuple_candidate_for_test(const std::string& tab_name, const Rid& rid,
                                                               const std::string& record_bytes,
                                                               const TupleMeta& tombstone) {
    RmRecord record(static_cast<int>(record_bytes.size()));
    std::memcpy(record.data, record_bytes.data(), record_bytes.size());
    remember_deleted_tuple_candidate(tab_name, rid, record, tombstone);
}

std::vector<VersionHistory::DeletedTupleCandidate>
VersionHistory::get_deleted_tuple_candidates(const std::string& tab_name, const RmRecord& record) {
    std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
    auto table_it = deleted_tuple_candidates_.find(tab_name);
    if (table_it == deleted_tuple_candidates_.end())
        return {};
    auto bucket_it = table_it->second.find(make_deleted_tuple_row_key(record.data, record.size));
    return bucket_it == table_it->second.end() ? std::vector<DeletedTupleCandidate>{} : bucket_it->second;
}

void VersionHistory::remove_deleted_tuple_candidate_if_current(const std::string& tab_name, const RmRecord& record,
                                                               const DeletedTupleCandidate& candidate) {
    std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
    erase_deleted_tuple_candidate_locked(tab_name, make_deleted_tuple_row_key(record.data, record.size), candidate);
}

void VersionHistory::set_deleted_tuple_candidate_test_hash_override(std::optional<size_t> forced_hash) {
    std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
    if (!deleted_tuple_candidates_.empty())
        throw InternalError("cannot change deleted tuple candidate hash while entries exist");
    deleted_tuple_candidate_test_hash_override_ = forced_hash;
}

void VersionHistory::prune(timestamp_t watermark) {
    constexpr size_t kHistoryPruneBatch = 512;
    // Hold the catalog lifetime guard while resolving table handles and
    // touching their pages.  This is deliberately acquired only here: callers
    // of this background GC path do not already own a catalog guard, avoiding
    // recursive locking of catalog_latch_.
    auto catalog_guard = sm_manager_.acquire_catalog_shared();
    // Historical entries are placed on a retire queue when created. Each GC
    // pass examines only a bounded queue prefix; entries that are still
    // visible to an active snapshot are requeued for a later watermark.
    struct HistoricalProbeItem {
        HistoricalRetireCandidate candidate;
        bool requeue{false};
    };
    std::vector<HistoricalProbeItem> hist_snapshot;
    {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        while (!historical_deferred_retire_queue_.empty()) {
            const auto& deferred = historical_deferred_retire_queue_.top();
            if (!deferred.retry_after_watermark.has_value() || *deferred.retry_after_watermark >= watermark) {
                break;
            }
            auto candidate = historical_deferred_retire_queue_.top();
            historical_deferred_retire_queue_.pop();
            const auto retire_key = make_historical_retire_key(candidate);
            auto bucket_it = historical_index_keys_.find(candidate.bucket_key);
            if (bucket_it == historical_index_keys_.end()) {
                continue;
            }
            auto key_it = bucket_it->second.entries.find(candidate.encoded_key);
            if (key_it == bucket_it->second.entries.end()) {
                continue;
            }
            auto current = std::find_if(key_it->second.begin(), key_it->second.end(),
                                        [&](const auto& entry) { return entry.rid == candidate.rid; });
            if (current == key_it->second.end() ||
                current->retire_state != HistoricalIndexBucket::RetireState::Deferred) {
                continue;
            }
            current->retire_state = HistoricalIndexBucket::RetireState::Queued;
            candidate.generation = current->generation;
            candidate.retry_after_watermark.reset();
            historical_queued_generations_.insert(retire_key);
            historical_retire_queue_.push_back(std::move(candidate));
        }
        const size_t count = std::min(kHistoryPruneBatch, historical_retire_queue_.size());
        hist_snapshot.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            auto candidate = std::move(historical_retire_queue_.front());
            historical_retire_queue_.pop_front();
            const auto retire_key = make_historical_retire_key(candidate);
            auto queued_it = historical_queued_generations_.find(retire_key);
            if (queued_it == historical_queued_generations_.end()) {
                continue;
            }
            historical_queued_generations_.erase(queued_it);
            auto bucket_it = historical_index_keys_.find(candidate.bucket_key);
            if (bucket_it == historical_index_keys_.end()) {
                continue;
            }
            auto key_it = bucket_it->second.entries.find(candidate.encoded_key);
            if (key_it == bucket_it->second.entries.end()) {
                continue;
            }
            auto current = std::find_if(key_it->second.begin(), key_it->second.end(),
                                        [&](const auto& entry) { return entry.rid == candidate.rid; });
            if (current == key_it->second.end() ||
                current->retire_state != HistoricalIndexBucket::RetireState::Queued) {
                continue;
            }
            // The queue node is only a logical ticket. A queued refresh may
            // have advanced the Entry generation after the node was appended.
            // Capture the current generation exactly when ownership changes to
            // InFlight; no deque update or scan is needed on the foreground path.
            candidate.generation = current->generation;
            current->retire_state = HistoricalIndexBucket::RetireState::InFlight;
            hist_snapshot.push_back(HistoricalProbeItem{std::move(candidate)});
        }
    }
    struct PageProbeGroup {
        RmFileHandle* file_handle;
        page_id_t page_no;
        std::vector<int> slots;
        std::vector<size_t> candidate_indices;
    };
    struct PageProbeKey {
        RmFileHandle* file_handle;
        page_id_t page_no;

        bool operator==(const PageProbeKey& other) const {
            return file_handle == other.file_handle && page_no == other.page_no;
        }
    };
    struct PageProbeKeyHash {
        size_t operator()(const PageProbeKey& key) const noexcept {
            const size_t handle_hash = std::hash<RmFileHandle*>{}(key.file_handle);
            const size_t page_hash = std::hash<page_id_t>{}(key.page_no);
            return handle_hash ^
                   (page_hash + static_cast<size_t>(0x9e3779b9) + (handle_hash << 6) + (handle_hash >> 2));
        }
    };
    std::vector<PageProbeGroup> hist_groups;
    std::unordered_map<PageProbeKey, size_t, PageProbeKeyHash> hist_group_indices;
    hist_groups.reserve(hist_snapshot.size());
    hist_group_indices.reserve(hist_snapshot.size());
    for (size_t i = 0; i < hist_snapshot.size(); ++i) {
        const auto& candidate = hist_snapshot[i].candidate;
        const size_t nul = candidate.bucket_key.find('\0');
        const std::string tab_name = (nul != std::string::npos) ? candidate.bucket_key.substr(0, nul) : std::string{};
        const auto fh_it = sm_manager_.fhs_.find(tab_name);
        if (fh_it == sm_manager_.fhs_.end()) {
            continue;
        }
        RmFileHandle* file_handle = fh_it->second.get();
        const PageProbeKey key{file_handle, candidate.rid.page_no};
        auto [group_it, inserted] = hist_group_indices.emplace(key, hist_groups.size());
        if (inserted) {
            hist_groups.push_back(PageProbeGroup{file_handle, candidate.rid.page_no, {}, {}});
        }
        auto& group = hist_groups[group_it->second];
        group.slots.push_back(candidate.rid.slot_no);
        group.candidate_indices.push_back(i);
    }
    for (const auto& group : hist_groups) {
        group.file_handle->visit_tuple_meta_batch(
            group.page_no, group.slots, [&](size_t j, RmTupleMetaProbeState state, const TupleMeta* meta) {
                const size_t candidate_index = group.candidate_indices[j];
                auto& item = hist_snapshot[candidate_index];
                if (state == RmTupleMetaProbeState::Retry) {
                    item.requeue = true;
                    return;
                }
                if (state == RmTupleMetaProbeState::Absent) {
                    return;
                }
                if (meta != nullptr && meta->is_committed_ && meta->commit_ts_ != INVALID_TS &&
                    meta->commit_ts_ >= watermark) {
                    item.candidate.retry_after_watermark = meta->commit_ts_;
                    return;
                }
                if (meta == nullptr ||
                    !(meta->is_committed_ && meta->commit_ts_ != INVALID_TS && meta->commit_ts_ < watermark)) {
                    item.requeue = true;
                }
            });
    }
    if (!hist_snapshot.empty()) {
        std::unique_lock<std::shared_mutex> lock(historical_index_keys_latch_);
        for (size_t i = 0; i < hist_snapshot.size(); ++i) {
            auto candidate = hist_snapshot[i].candidate;
            auto it = historical_index_keys_.find(candidate.bucket_key);
            if (it == historical_index_keys_.end()) {
                continue;
            }
            auto key_it = it->second.entries.find(candidate.encoded_key);
            if (key_it == it->second.entries.end()) {
                continue;
            }
            auto& entries = key_it->second;
            const auto current = std::find_if(entries.begin(), entries.end(),
                                              [&](const auto& entry) { return entry.rid == candidate.rid; });
            if (current == entries.end()) {
                continue;
            }
            if (current->generation != candidate.generation) {
                current->retire_state = HistoricalIndexBucket::RetireState::Queued;
                historical_queued_generations_.insert(make_historical_retire_key(candidate));
                historical_retire_queue_.push_back(HistoricalRetireCandidate{
                    candidate.bucket_key, candidate.encoded_key, candidate.rid, current->generation, {}});
            } else if (!hist_snapshot[i].requeue) {
                if (candidate.retry_after_watermark.has_value()) {
                    current->retire_state = HistoricalIndexBucket::RetireState::Deferred;
                    historical_deferred_retire_queue_.push(candidate);
                } else {
                    entries.erase(current);
                    if (entries.empty()) {
                        it->second.entries.erase(key_it);
                    }
                    if (it->second.entries.empty()) {
                        historical_index_keys_.erase(it);
                    }
                }
            } else {
                current->retire_state = HistoricalIndexBucket::RetireState::Queued;
                historical_queued_generations_.insert(make_historical_retire_key(candidate));
                candidate.retry_after_watermark.reset();
                historical_retire_queue_.push_back(candidate);
            }
        }
    }

    // Deleted-tuple candidates use a FIFO retire queue instead of repeatedly
    // scanning the first keys in an unordered map. Entries which remain visible
    // to an old snapshot rotate to the tail, so they cannot starve later safe
    // entries. The candidate latch is never held while a page is fetched.
    std::vector<DeletedTupleRetireCandidate> deleted_snapshot;
    {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        while (!deleted_tuple_deferred_retire_queue_.empty()) {
            const auto& deferred = deleted_tuple_deferred_retire_queue_.top();
            if (!deferred.retry_after_watermark.has_value() || *deferred.retry_after_watermark >= watermark) {
                break;
            }
            auto retired = deleted_tuple_deferred_retire_queue_.top();
            deleted_tuple_deferred_retire_queue_.pop();
            auto table_it = deleted_tuple_candidates_.find(retired.tab_name);
            if (table_it == deleted_tuple_candidates_.end()) {
                continue;
            }
            auto bucket_it = table_it->second.find(retired.row_key);
            if (bucket_it == table_it->second.end()) {
                continue;
            }
            const bool still_current =
                std::any_of(bucket_it->second.begin(), bucket_it->second.end(), [&](const auto& current) {
                    return current.candidate_id == retired.candidate.candidate_id &&
                           current.writer_txn_id == retired.candidate.writer_txn_id &&
                           current.version_chain_head == retired.candidate.version_chain_head;
                });
            if (!still_current) {
                continue;
            }
            retired.retry_after_watermark.reset();
            deleted_tuple_retire_queue_.push_back(std::move(retired));
        }
        const size_t count = std::min(kHistoryPruneBatch, deleted_tuple_retire_queue_.size());
        deleted_snapshot.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            deleted_snapshot.push_back(std::move(deleted_tuple_retire_queue_.front()));
            deleted_tuple_retire_queue_.pop_front();
        }
    }

    std::vector<bool> deleted_requeue(deleted_snapshot.size(), false);
    std::vector<std::optional<timestamp_t>> deleted_deferred_after(deleted_snapshot.size());
    std::vector<PageProbeGroup> deleted_groups;
    std::unordered_map<PageProbeKey, size_t, PageProbeKeyHash> deleted_group_indices;
    deleted_groups.reserve(deleted_snapshot.size());
    deleted_group_indices.reserve(deleted_snapshot.size());
    for (size_t i = 0; i < deleted_snapshot.size(); ++i) {
        const auto& retired = deleted_snapshot[i];
        const auto fh_it = sm_manager_.fhs_.find(retired.tab_name);
        if (fh_it == sm_manager_.fhs_.end()) {
            continue;
        }
        RmFileHandle* file_handle = fh_it->second.get();
        const PageProbeKey key{file_handle, retired.candidate.rid.page_no};
        auto [group_it, inserted] = deleted_group_indices.emplace(key, deleted_groups.size());
        if (inserted) {
            deleted_groups.push_back(PageProbeGroup{file_handle, retired.candidate.rid.page_no, {}, {}});
        }
        auto& group = deleted_groups[group_it->second];
        group.slots.push_back(retired.candidate.rid.slot_no);
        group.candidate_indices.push_back(i);
    }
    for (const auto& group : deleted_groups) {
        const auto probes = group.file_handle->probe_tuple_meta_batch(group.page_no, group.slots);
        for (size_t j = 0; j < probes.size(); ++j) {
            const auto& probe = probes[j];
            const size_t candidate_index = group.candidate_indices[j];
            if (probe.state == RmTupleMetaProbeState::Retry) {
                deleted_requeue[candidate_index] = true;
                continue;
            }
            if (probe.state == RmTupleMetaProbeState::Absent) {
                continue;
            }
            const TupleMeta& meta = probe.meta;
            // A stale queue item must not touch a newer deletion at a reused RID.
            if (!deleted_tuple_candidate_matches_meta(deleted_snapshot[candidate_index].candidate, meta)) {
                continue;
            }
            // Keep precisely the candidates that can still be seen as concurrent
            // by an active snapshot. Equality is intentionally unsafe here.
            if (meta.is_committed_ && meta.commit_ts_ != INVALID_TS && meta.commit_ts_ >= watermark) {
                deleted_deferred_after[candidate_index] = meta.commit_ts_;
            } else if (!(meta.is_committed_ && meta.commit_ts_ != INVALID_TS && meta.commit_ts_ < watermark)) {
                deleted_requeue[candidate_index] = true;
            }
        }
    }

    if (!deleted_snapshot.empty()) {
        std::lock_guard<std::mutex> lock(deleted_tuple_candidates_latch_);
        for (size_t i = 0; i < deleted_snapshot.size(); ++i) {
            auto retired = deleted_snapshot[i];
            if (deleted_requeue[i]) {
                // Do not resurrect an entry removed by a concurrent foreground
                // validation or by DROP TABLE while this batch touched pages.
                auto table_it = deleted_tuple_candidates_.find(retired.tab_name);
                if (table_it == deleted_tuple_candidates_.end()) {
                    continue;
                }
                auto bucket_it = table_it->second.find(retired.row_key);
                if (bucket_it == table_it->second.end()) {
                    continue;
                }
                const bool still_current =
                    std::any_of(bucket_it->second.begin(), bucket_it->second.end(), [&](const auto& current) {
                        return current.candidate_id == retired.candidate.candidate_id &&
                               current.writer_txn_id == retired.candidate.writer_txn_id &&
                               current.version_chain_head == retired.candidate.version_chain_head;
                    });
                if (still_current) {
                    retired.retry_after_watermark.reset();
                    deleted_tuple_retire_queue_.push_back(retired);
                }
            } else if (deleted_deferred_after[i].has_value()) {
                auto table_it = deleted_tuple_candidates_.find(retired.tab_name);
                if (table_it == deleted_tuple_candidates_.end()) {
                    continue;
                }
                auto bucket_it = table_it->second.find(retired.row_key);
                if (bucket_it == table_it->second.end()) {
                    continue;
                }
                const bool still_current =
                    std::any_of(bucket_it->second.begin(), bucket_it->second.end(), [&](const auto& current) {
                        return current.candidate_id == retired.candidate.candidate_id &&
                               current.writer_txn_id == retired.candidate.writer_txn_id &&
                               current.version_chain_head == retired.candidate.version_chain_head;
                    });
                if (still_current) {
                    retired.retry_after_watermark = deleted_deferred_after[i];
                    deleted_tuple_deferred_retire_queue_.push(std::move(retired));
                }
            } else {
                erase_deleted_tuple_candidate_locked(retired.tab_name, retired.row_key, retired.candidate);
            }
        }
    }
}
