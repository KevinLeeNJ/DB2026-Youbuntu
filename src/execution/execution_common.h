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

#include <cstring>
#include <memory>
#include <vector>
#include <optional>

#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include "common/common.h"
#include "record/rm_file_handle.h"
#include "record/rm_scan.h"

auto ReconstructTuple(const TabMeta* schema, const RmRecord& base_tuple, const TupleMeta& base_meta,
                      const std::vector<UndoLog>& undo_logs) -> std::optional<RmRecord>;

auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction* txn) -> bool;

inline void ReserveUniqueKey(Context* context, int index_fd, const std::vector<char>& key) {
    if (context == nullptr || context->txn_ == nullptr || context->lock_mgr_ == nullptr) {
        return;
    }
    if (!context->lock_mgr_->lock_exclusive_on_unique_key(context->txn_, index_fd, key)) {
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::UNIQUE_KEY_CONFLICT);
    }
}

// A no-index SI/SERIALIZABLE DELETE publishes an exact-row intent before its
// tombstone. INSERT atomically probes this registry before checking the slower
// candidate store. The full on-page bytes are authoritative; hashing only
// selects a shard. The caller holds the catalog shared guard.
inline void CheckLogicalRowDeleteIntentForInsert(Context* context, SmManager* sm_manager, const std::string& tab_name,
                                                 const RmRecord& record) {
    if (context == nullptr || context->txn_ == nullptr || context->lock_mgr_ == nullptr || sm_manager == nullptr ||
        context->txn_->get_isolation_level() == IsolationLevel::READ_COMMITTED) {
        return;
    }
    const uint64_t table_runtime_id = sm_manager->get_table_runtime_id_under_catalog_guard(tab_name);
    std::vector<char> record_bytes(record.data, record.data + record.size);
    if (context->lock_mgr_->logical_row_delete_intent_conflicts(context->txn_, table_runtime_id, record_bytes)) {
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
    }
}

inline void RegisterLogicalRowDeleteIntent(Context* context, SmManager* sm_manager, const std::string& tab_name,
                                           const RmRecord& record) {
    if (context == nullptr || context->txn_ == nullptr || context->lock_mgr_ == nullptr || sm_manager == nullptr ||
        context->txn_->get_isolation_level() == IsolationLevel::READ_COMMITTED) {
        return;
    }
    const uint64_t table_runtime_id = sm_manager->get_table_runtime_id_under_catalog_guard(tab_name);
    std::vector<char> record_bytes(record.data, record.data + record.size);
    if (!context->lock_mgr_->register_logical_row_delete_intent(context->txn_, table_runtime_id, record_bytes)) {
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
    }
}

inline std::unique_ptr<RmRecord> GetVisibleRecord(RmFileHandle* fh, const Rid& rid, Context* context) {
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr) {
        return fh->get_record(rid, context);
    }

    auto* txn = context->txn_;
    auto* txn_mgr = context->txn_mgr_;
    const timestamp_t read_ts = txn->get_read_ts();
    const txn_id_t self_id = txn->get_transaction_id();

    constexpr int MAX_DEPTH = 100;

    // A lock-free reader can observe an uncommitted tuple just as its writer
    // rolls back and retires its undo buffer. Re-read the tuple in that narrow
    // publication window instead of treating the transient link as corruption.
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto record_with_meta = fh->get_record_with_meta(rid, context);
        if (record_with_meta.record == nullptr) {
            return nullptr;
        }
        TupleMeta meta = record_with_meta.meta;
        auto base_record = std::move(record_with_meta.record);
        std::optional<UndoLog> current_undo;
        bool retry = false;

        for (int depth = 0; depth < MAX_DEPTH; ++depth) {
            if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
                if (meta.is_deleted_) {
                    return nullptr;
                }
                return base_record;
            }

            if (!meta.is_committed_) {
                if (!meta.version_chain_head_.IsValid()) {
                    return nullptr;
                }
                current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
                if (!current_undo.has_value()) {
                    retry = true;
                    break;
                }
                meta = current_undo->old_meta_;
                continue;
            }

            if (meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                return nullptr;
            }

            if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                if (!current_undo.has_value()) {
                    return base_record;
                }
                const auto& log = *current_undo;
                auto rec = std::make_unique<RmRecord>(static_cast<int>(log.old_tuple_data_.size()));
                memcpy(rec->data, log.old_tuple_data_.data(), log.old_tuple_data_.size());
                return rec;
            }

            if (!meta.version_chain_head_.IsValid()) {
                return nullptr;
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                retry = true;
                break;
            }
            meta = current_undo->old_meta_;
        }

        if (!retry) {
            return nullptr;
        }
    }
    return nullptr;
}

// Borrow the current visible heap version when it is still page-backed. Undo
// reconstruction remains owned because its bytes live in the transaction
// manager rather than in the record page.
inline RmRecordViewWithMeta GetVisibleTuple(RmFileHandle* fh, const Rid& rid, Context* context) {
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr) {
        return fh->get_record_view_with_meta(rid);
    }

    auto* txn = context->txn_;
    auto* txn_mgr = context->txn_mgr_;
    const timestamp_t read_ts = txn->get_read_ts();
    const txn_id_t self_id = txn->get_transaction_id();

    constexpr int MAX_DEPTH = 100;
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto base = fh->get_record_view_with_meta(rid);
        TupleMeta meta = base.meta;
        std::optional<UndoLog> current_undo;
        bool retry = false;

        for (int depth = 0; depth < MAX_DEPTH; ++depth) {
            if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
                if (meta.is_deleted_) {
                    return {};
                }
                base.meta = meta;
                return base;
            }

            if (!meta.is_committed_) {
                if (!meta.version_chain_head_.IsValid()) {
                    return {};
                }
                current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
                if (!current_undo.has_value()) {
                    retry = true;
                    break;
                }
                meta = current_undo->old_meta_;
                continue;
            }

            if (meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                return {};
            }

            if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                if (!current_undo.has_value()) {
                    base.meta = meta;
                    return base;
                }
                auto owned = std::make_unique<RmRecord>(static_cast<int>(current_undo->old_tuple_data_.size()));
                memcpy(owned->data, current_undo->old_tuple_data_.data(), current_undo->old_tuple_data_.size());
                RmRecordViewWithMeta result;
                result.meta = meta;
                result.view = RmRecordView{owned->data, static_cast<uint32_t>(owned->size)};
                result.owned = std::move(owned);
                return result;
            }

            if (!meta.version_chain_head_.IsValid()) {
                return {};
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                retry = true;
                break;
            }
            meta = current_undo->old_meta_;
        }

        if (!retry) {
            return {};
        }
    }
    return {};
}

// Resolve visibility from a coherent page-latched copy of the current base
// tuple. A missing undo record asks the caller to retry through GetVisibleTuple
// so the existing publication-window handling remains authoritative.
inline RmRecordViewWithMeta GetVisibleTupleFromCopiedBase(const TupleMeta& base_meta, const char* base_data,
                                                          uint32_t base_size, Context* context,
                                                          bool* needs_heap_reread) {
    *needs_heap_reread = false;
    const auto copied_base = [&]() {
        RmRecordViewWithMeta result;
        result.meta = base_meta;
        result.view = RmRecordView{base_data, base_size};
        return result;
    };
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr) {
        return copied_base();
    }

    auto* txn = context->txn_;
    auto* txn_mgr = context->txn_mgr_;
    const timestamp_t read_ts = txn->get_read_ts();
    const txn_id_t self_id = txn->get_transaction_id();
    TupleMeta meta = base_meta;
    std::optional<UndoLog> current_undo;

    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH; ++depth) {
        if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
            if (meta.is_deleted_) {
                return {};
            }
            auto result = copied_base();
            result.meta = meta;
            return result;
        }

        if (!meta.is_committed_) {
            if (!meta.version_chain_head_.IsValid()) {
                return {};
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                *needs_heap_reread = true;
                return {};
            }
            meta = current_undo->old_meta_;
            continue;
        }

        if (meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
            return {};
        }

        if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
            if (!current_undo.has_value()) {
                auto result = copied_base();
                result.meta = meta;
                return result;
            }
            auto owned = std::make_unique<RmRecord>(static_cast<int>(current_undo->old_tuple_data_.size()));
            memcpy(owned->data, current_undo->old_tuple_data_.data(), current_undo->old_tuple_data_.size());
            RmRecordViewWithMeta result;
            result.meta = meta;
            result.view = RmRecordView{owned->data, static_cast<uint32_t>(owned->size)};
            result.owned = std::move(owned);
            return result;
        }

        if (!meta.version_chain_head_.IsValid()) {
            return {};
        }
        current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
        if (!current_undo.has_value()) {
            *needs_heap_reread = true;
            return {};
        }
        meta = current_undo->old_meta_;
    }
    return {};
}

/* The caller must already hold this transaction's record X lock. */
inline std::optional<RmRecordWithMeta> GetCurrentRecordForRcWrite(RmFileHandle* fh, const Rid& rid, Transaction* txn,
                                                                  Context* context) {
    auto record_with_meta = fh->get_record_with_meta(rid, context);
    if (record_with_meta.record == nullptr) {
        return std::nullopt;
    }
    const TupleMeta& meta = record_with_meta.meta;
    if (meta.is_deleted_ || (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id())) {
        return std::nullopt;
    }
    return record_with_meta;
}

inline bool IndexKeyEquals(const IndexMeta& index, const char* rec_data, const std::vector<char>& key) {
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        if (std::memcmp(rec_data + index.cols[i].offset, key.data() + offset, index.cols[i].len) != 0) {
            return false;
        }
        offset += index.cols[i].len;
    }
    return true;
}

inline bool RecordDataEquals(const RmRecord& lhs, const RmRecord& rhs) {
    return lhs.size == rhs.size && std::memcmp(lhs.data, rhs.data, lhs.size) == 0;
}

inline bool DeletedTupleCandidatesConflictWithInsert(RmFileHandle* fh, SmManager* sm_manager,
                                                     const std::string& tab_name, const RmRecord& inserted_rec,
                                                     Context* context) {
    if (fh == nullptr || sm_manager == nullptr || context == nullptr || context->txn_ == nullptr) {
        return false;
    }

    auto* txn = context->txn_;
    // This is an exact physical-row bucket lookup. The returned vector is a
    // copy, so neither page access nor transaction work is ever performed
    // while SmManager's candidate latch is held.
    auto candidates = sm_manager->get_deleted_tuple_candidates(tab_name, inserted_rec);
    for (const auto& candidate : candidates) {
        if (!fh->is_record(candidate.rid)) {
            sm_manager->remove_deleted_tuple_candidate_if_current(tab_name, inserted_rec, candidate);
            continue;
        }

        TupleMeta meta = fh->get_tuple_meta(candidate.rid);
        // candidate_id + delete version identity makes this cleanup ABA-safe:
        // a reused RID or a later delete cannot be removed by an old lookup.
        if (!meta.is_deleted_ || meta.writer_txn_id_ != candidate.writer_txn_id ||
            meta.version_chain_head_ != candidate.version_chain_head) {
            sm_manager->remove_deleted_tuple_candidate_if_current(tab_name, inserted_rec, candidate);
            continue;
        }
        if (meta.writer_txn_id_ == txn->get_transaction_id()) {
            continue;
        }

        bool concurrent_delete = !meta.is_committed_ || meta.commit_ts_ > txn->get_read_ts();
        if (!concurrent_delete) {
            continue;
        }

        auto deleted_rec = fh->get_record(candidate.rid, context);
        if (deleted_rec != nullptr && RecordDataEquals(*deleted_rec, inserted_rec)) {
            return true;
        }
    }
    return false;
}

inline bool HistoricalIndexKeyConflictsWithTxn(RmFileHandle* fh, const Rid& rid, const IndexMeta& index,
                                               const std::vector<char>& key, Context* context) {
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr || !fh->is_record(rid)) {
        return false;
    }

    auto* txn = context->txn_;
    TupleMeta meta = fh->get_tuple_meta(rid);
    if (meta.writer_txn_id_ == txn->get_transaction_id()) {
        return false;
    }

    auto current_rec = fh->get_record(rid, context);
    bool current_key_matches = current_rec != nullptr && IndexKeyEquals(index, current_rec->data, key);

    if (!meta.is_committed_) {
        if (current_key_matches) {
            return true;
        }
    } else if (meta.commit_ts_ <= txn->get_read_ts()) {
        return !meta.is_deleted_ && current_key_matches;
    } else if (current_key_matches) {
        return true;
    }

    UndoLink link = meta.version_chain_head_;
    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH && link.IsValid(); ++depth) {
        UndoLog undo = context->txn_mgr_->GetUndoLog(link);
        const TupleMeta& old_meta = undo.old_meta_;
        bool old_key_matches = !old_meta.is_deleted_ && !undo.old_tuple_data_.empty() &&
                               IndexKeyEquals(index, undo.old_tuple_data_.data(), key);

        if (!old_meta.is_committed_) {
            if (old_meta.writer_txn_id_ == txn->get_transaction_id()) {
                return old_key_matches;
            }
            link = undo.prev_version_;
            continue;
        }

        if (old_meta.commit_ts_ <= txn->get_read_ts()) {
            return old_key_matches;
        }

        link = undo.prev_version_;
    }
    return false;
}
