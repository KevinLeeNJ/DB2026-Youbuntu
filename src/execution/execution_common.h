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

inline uint64_t ObservationTableRuntimeId(Context* context, SmManager* sm_manager, const std::string& tab_name) noexcept {
    return context != nullptr && context->abort_metrics_enabled_ && sm_manager != nullptr
               ? sm_manager->try_get_table_runtime_id_under_catalog_guard(tab_name)
               : 0;
}

inline void ReserveUniqueKey(Context* context, SmManager* sm_manager, const std::string& tab_name, int index_fd,
                             const std::vector<char>& key) {
    if (context == nullptr || context->txn_ == nullptr || context->lock_mgr_ == nullptr) {
        return;
    }
    if (!context->lock_mgr_->lock_exclusive_on_unique_key(context->txn_, index_fd, key)) {
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::UNIQUE_KEY_CONFLICT,
                                        AbortDetail::UNKNOWN, ObservationTableRuntimeId(context, sm_manager, tab_name));
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
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT,
                                        AbortDetail::UNKNOWN, ObservationTableRuntimeId(context, sm_manager, tab_name));
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
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT,
                                        AbortDetail::UNKNOWN, ObservationTableRuntimeId(context, sm_manager, tab_name));
    }
}

// Fully independent, non-authoritative resolver used only by explicit shadow
// mode. It follows the same in-memory UndoLink chain as legacy visibility but
// applies the immutable active-writer ReadView plus TST status at every
// version. No record returned to the executor is moved into this path.
inline TransactionManager::ShadowVisibilityResult ResolveReadViewShadowCandidate(
    TransactionManager* txn_mgr, const Transaction& txn, const TupleMeta& initial_meta, const char* base_data,
    uint32_t base_size) {
    TransactionManager::ShadowVisibilityResult result;
    TupleMeta meta = initial_meta;
    std::optional<UndoLog> current_undo;
    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH; ++depth) {
        if (txn_mgr->ShadowVersionVisible(txn, meta)) {
            result.visible = !meta.is_deleted_;
            result.deleted = meta.is_deleted_;
            result.meta = meta;
            result.depth = depth;
            if (result.visible) {
                if (current_undo.has_value())
                    result.payload = current_undo->old_tuple_data_;
                else if (base_data != nullptr && base_size != 0)
                    result.payload.assign(base_data, base_data + base_size);
            }
            return result;
        }
        if (!meta.version_chain_head_.IsValid()) {
            result.meta = meta;
            result.depth = depth;
            return result;
        }
        current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
        if (!current_undo.has_value()) {
            result.undo_missing = true;
            result.meta = meta;
            result.depth = depth;
            return result;
        }
        meta = current_undo->old_meta_;
    }
    result.meta = meta;
    result.depth = MAX_DEPTH;
    return result;
}

inline std::optional<TransactionManager::ShadowVisibilityResult>
PrepareReadViewShadowCandidate(TransactionManager* txn_mgr, const Transaction& txn, const TupleMeta& meta,
                               const char* base_data, uint32_t base_size) {
    if (!txn_mgr->read_view_shadow_enabled()) return std::nullopt;
    if (!txn.get_read_view()) return std::nullopt;
    try {
        return ResolveReadViewShadowCandidate(txn_mgr, txn, meta, base_data, base_size);
    } catch (...) {
        TransactionManager::ShadowVisibilityResult failed;
        failed.undo_missing = true;
        return failed;
    }
}

inline void FinishReadViewShadowCandidate(TransactionManager* txn_mgr,
                                          const std::optional<TransactionManager::ShadowVisibilityResult>& candidate,
                                          const RmRecordViewWithMeta& legacy, bool legacy_deleted, int legacy_depth) {
    if (!candidate.has_value()) return;
    try {
        std::vector<char> payload;
        if (legacy.view.data != nullptr) payload.assign(legacy.view.data, legacy.view.data + legacy.view.size);
        txn_mgr->ObserveReadViewShadow(*candidate, legacy.view.data != nullptr, legacy_deleted, legacy.meta,
                                       legacy_depth, legacy.view.data == nullptr ? nullptr : &payload);
    } catch (...) {
        // A diagnostic allocation/copy failure must not affect the selected
        // legacy tuple or its retry behavior.
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
    std::optional<TransactionManager::ShadowVisibilityResult> final_shadow_candidate;
    TupleMeta final_shadow_meta{};

    // A lock-free reader can observe an uncommitted tuple just as its writer
    // rolls back and retires its undo buffer. Re-read the tuple in that narrow
    // publication window instead of treating the transient link as corruption.
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto record_with_meta = fh->get_record_with_meta(rid, context);
        if (record_with_meta.record == nullptr) {
            return nullptr;
        }
        TupleMeta meta = record_with_meta.meta;
        auto shadow_candidate = PrepareReadViewShadowCandidate(txn_mgr, *txn, meta, record_with_meta.record->data,
                                                                static_cast<uint32_t>(record_with_meta.record->size));
        final_shadow_candidate = shadow_candidate;
        final_shadow_meta = meta;
        auto base_record = std::move(record_with_meta.record);
        std::optional<UndoLog> current_undo;
        bool retry = false;
        auto finish = [&](std::unique_ptr<RmRecord> result, const TupleMeta& legacy_meta, int legacy_depth,
                          bool legacy_deleted) {
            // Shadow observation is deliberately after the legacy answer has
            // been chosen. It has no authority over visibility or retries.
            RmRecordViewWithMeta view;
            view.meta = legacy_meta;
            if (result != nullptr) view.view = RmRecordView{result->data, static_cast<uint32_t>(result->size)};
            FinishReadViewShadowCandidate(txn_mgr, shadow_candidate, view, legacy_deleted, legacy_depth);
            return result;
        };

        for (int depth = 0; depth < MAX_DEPTH; ++depth) {
            if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
                if (meta.is_deleted_) {
                    return finish(nullptr, meta, depth, true);
                }
                return finish(std::move(base_record), meta, depth, false);
            }

            if (!meta.is_committed_) {
                if (!meta.version_chain_head_.IsValid()) {
                    return finish(nullptr, meta, depth, false);
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
                return finish(nullptr, meta, depth, true);
            }

            if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                if (!current_undo.has_value()) {
                    return finish(std::move(base_record), meta, depth, false);
                }
                const auto& log = *current_undo;
                auto rec = std::make_unique<RmRecord>(static_cast<int>(log.old_tuple_data_.size()));
                memcpy(rec->data, log.old_tuple_data_.data(), log.old_tuple_data_.size());
                return finish(std::move(rec), meta, depth, false);
            }

            if (!meta.version_chain_head_.IsValid()) {
                return finish(nullptr, meta, depth, false);
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                retry = true;
                break;
            }
            meta = current_undo->old_meta_;
        }

        if (!retry) {
            return finish(nullptr, meta, MAX_DEPTH, false);
        }
    }
    RmRecordViewWithMeta empty;
    empty.meta = final_shadow_meta;
    FinishReadViewShadowCandidate(txn_mgr, final_shadow_candidate, empty, false, MAX_DEPTH);
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
    std::optional<TransactionManager::ShadowVisibilityResult> final_shadow_candidate;
    TupleMeta final_shadow_meta{};
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto base = fh->get_record_view_with_meta(rid);
        TupleMeta meta = base.meta;
        auto shadow_candidate = PrepareReadViewShadowCandidate(txn_mgr, *txn, meta, base.view.data, base.view.size);
        final_shadow_candidate = shadow_candidate;
        final_shadow_meta = meta;
        std::optional<UndoLog> current_undo;
        bool retry = false;
        auto finish = [&](RmRecordViewWithMeta result, const TupleMeta& legacy_meta, int depth, bool deleted) {
            result.meta = legacy_meta;
            FinishReadViewShadowCandidate(txn_mgr, shadow_candidate, result, deleted, depth);
            return result;
        };

        for (int depth = 0; depth < MAX_DEPTH; ++depth) {
            if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
                if (meta.is_deleted_) {
                    return finish({}, meta, depth, true);
                }
                base.meta = meta;
                return finish(std::move(base), meta, depth, false);
            }

            if (!meta.is_committed_) {
                if (!meta.version_chain_head_.IsValid()) {
                    return finish({}, meta, depth, false);
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
                return finish({}, meta, depth, true);
            }

            if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
                if (!current_undo.has_value()) {
                    base.meta = meta;
                    return finish(std::move(base), meta, depth, false);
                }
                auto owned = std::make_unique<RmRecord>(static_cast<int>(current_undo->old_tuple_data_.size()));
                memcpy(owned->data, current_undo->old_tuple_data_.data(), current_undo->old_tuple_data_.size());
                RmRecordViewWithMeta result;
                result.meta = meta;
                result.view = RmRecordView{owned->data, static_cast<uint32_t>(owned->size)};
                result.owned = std::move(owned);
                return finish(std::move(result), meta, depth, false);
            }

            if (!meta.version_chain_head_.IsValid()) {
                return finish({}, meta, depth, false);
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                retry = true;
                break;
            }
            meta = current_undo->old_meta_;
        }

        if (!retry) {
            return finish({}, meta, MAX_DEPTH, false);
        }
    }
    RmRecordViewWithMeta empty;
    empty.meta = final_shadow_meta;
    FinishReadViewShadowCandidate(txn_mgr, final_shadow_candidate, empty, false, MAX_DEPTH);
    return empty;
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
    auto shadow_candidate = PrepareReadViewShadowCandidate(txn_mgr, *txn, base_meta, base_data, base_size);
    std::optional<UndoLog> current_undo;
    auto finish = [&](RmRecordViewWithMeta result, const TupleMeta& legacy_meta, int depth, bool deleted) {
        result.meta = legacy_meta;
        FinishReadViewShadowCandidate(txn_mgr, shadow_candidate, result, deleted, depth);
        return result;
    };

    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH; ++depth) {
        if (!meta.is_committed_ && meta.writer_txn_id_ == self_id) {
            if (meta.is_deleted_) {
                return finish({}, meta, depth, true);
            }
            auto result = copied_base();
            result.meta = meta;
            return finish(std::move(result), meta, depth, false);
        }

        if (!meta.is_committed_) {
            if (!meta.version_chain_head_.IsValid()) {
                return finish({}, meta, depth, false);
            }
            current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
            if (!current_undo.has_value()) {
                *needs_heap_reread = true;
                // The heap reread owns the sole final shadow observation.
                return {};
            }
            meta = current_undo->old_meta_;
            continue;
        }

        if (meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
            return finish({}, meta, depth, true);
        }

        if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
            if (!current_undo.has_value()) {
                auto result = copied_base();
                result.meta = meta;
                return finish(std::move(result), meta, depth, false);
            }
            auto owned = std::make_unique<RmRecord>(static_cast<int>(current_undo->old_tuple_data_.size()));
            memcpy(owned->data, current_undo->old_tuple_data_.data(), current_undo->old_tuple_data_.size());
            RmRecordViewWithMeta result;
            result.meta = meta;
            result.view = RmRecordView{owned->data, static_cast<uint32_t>(owned->size)};
            result.owned = std::move(owned);
            return finish(std::move(result), meta, depth, false);
        }

        if (!meta.version_chain_head_.IsValid()) {
            return finish({}, meta, depth, false);
        }
        current_undo = txn_mgr->GetUndoLogOptional(meta.version_chain_head_);
        if (!current_undo.has_value()) {
            *needs_heap_reread = true;
            // The heap reread owns the sole final shadow observation.
            return {};
        }
        meta = current_undo->old_meta_;
    }
    return finish({}, meta, MAX_DEPTH, false);
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
