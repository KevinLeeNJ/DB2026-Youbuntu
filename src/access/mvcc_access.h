/* Copyright (c) 2026 Team Youbuntu
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
#include <optional>
#include <vector>

#include "common/common.h"
#include "record/rm_file_handle.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

namespace rmdb::access {

auto ReconstructTuple(const TabMeta* schema, const RmRecord& base_tuple, const TupleMeta& base_meta,
                      const std::vector<UndoLog>& undo_logs) -> std::optional<RmRecord>;

auto IsWriteWriteConflict(timestamp_t tuple_ts, Transaction* txn) -> bool;

inline std::unique_ptr<RmRecord> GetVisibleRecord(RmFileHandle* fh, const Rid& rid,
                                                  rmdb::statement::StatementContext* context) {
    if (context == nullptr || context->txn == nullptr || context->txn_mgr == nullptr) {
        return fh->get_record(rid, context);
    }

    auto* txn = context->txn;
    auto* txn_mgr = context->txn_mgr;
    const timestamp_t read_ts =
        txn->get_isolation_level() == IsolationLevel::READ_COMMITTED ? txn->get_read_ts() : txn->get_start_ts();
    const txn_id_t self_id = txn->get_transaction_id();

    auto record_with_meta = fh->get_record_with_meta(rid, context);
    TupleMeta meta = record_with_meta.meta;
    auto base_record = std::move(record_with_meta.record);
    std::vector<UndoLog> undo_stack;
    constexpr int MAX_DEPTH = 100;

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
            undo_stack.push_back(txn_mgr->GetUndoLog(meta.version_chain_head_));
            meta = undo_stack.back().old_meta_;
            continue;
        }

        if (meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
            return nullptr;
        }

        if (!meta.is_deleted_ && meta.commit_ts_ <= read_ts) {
            if (undo_stack.empty()) {
                return base_record;
            }
            const auto& log = undo_stack.back();
            auto rec = std::make_unique<RmRecord>(static_cast<int>(log.old_tuple_data_.size()));
            memcpy(rec->data, log.old_tuple_data_.data(), log.old_tuple_data_.size());
            return rec;
        }

        if (!meta.version_chain_head_.IsValid()) {
            return nullptr;
        }
        undo_stack.push_back(txn_mgr->GetUndoLog(meta.version_chain_head_));
        meta = undo_stack.back().old_meta_;
    }
    return nullptr;
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

inline bool DeletedTupleCandidatesConflictWithInsert(RmFileHandle* fh, const std::string& tab_name,
                                                     const RmRecord& inserted_rec,
                                                     rmdb::statement::StatementContext* context) {
    if (fh == nullptr || context == nullptr || context->txn == nullptr || context->txn_mgr == nullptr) {
        return false;
    }

    auto& ssi = context->txn_mgr->ssi_registry();
    auto* txn = context->txn;
    auto candidate_rids = ssi.get_deleted_tuple_candidates(tab_name);
    for (const auto& rid : candidate_rids) {
        if (!fh->is_record(rid)) {
            ssi.remove_deleted_tuple_candidate(tab_name, rid);
            continue;
        }

        TupleMeta meta = fh->get_tuple_meta(rid);
        if (!meta.is_deleted_) {
            ssi.remove_deleted_tuple_candidate(tab_name, rid);
            continue;
        }
        if (meta.writer_txn_id_ == txn->get_transaction_id()) {
            continue;
        }

        bool concurrent_delete = !meta.is_committed_ || meta.commit_ts_ > txn->get_start_ts();
        if (!concurrent_delete) {
            continue;
        }

        auto deleted_rec = fh->get_record(rid, context);
        if (deleted_rec != nullptr && RecordDataEquals(*deleted_rec, inserted_rec)) {
            return true;
        }
    }
    return false;
}

inline bool HistoricalIndexKeyConflictsWithTxn(RmFileHandle* fh, const Rid& rid, const IndexMeta& index,
                                               const std::vector<char>& key,
                                               rmdb::statement::StatementContext* context) {
    if (context == nullptr || context->txn == nullptr || context->txn_mgr == nullptr || !fh->is_record(rid)) {
        return false;
    }

    auto* txn = context->txn;
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
    } else if (meta.commit_ts_ <= txn->get_start_ts()) {
        return !meta.is_deleted_ && current_key_matches;
    } else if (current_key_matches) {
        return true;
    }

    UndoLink link = meta.version_chain_head_;
    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH && link.IsValid(); ++depth) {
        UndoLog undo = context->txn_mgr->GetUndoLog(link);
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

        if (old_meta.commit_ts_ <= txn->get_start_ts()) {
            return old_key_matches;
        }

        link = undo.prev_version_;
    }
    return false;
}

} // namespace rmdb::access

namespace rmdb {
using access::DeletedTupleCandidatesConflictWithInsert;
using access::GetVisibleRecord;
using access::HistoricalIndexKeyConflictsWithTxn;
using access::IndexKeyEquals;
using access::IsWriteWriteConflict;
using access::ReconstructTuple;
using access::RecordDataEquals;
} // namespace rmdb
