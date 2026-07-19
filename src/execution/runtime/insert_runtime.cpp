/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#include "insert_runtime.h"

namespace {

void CheckMvccUniqueKeyConflict(const InsertRuntimeInfo& info, const RowMutationIndex& index,
                                const std::vector<char>& key, Context* context,
                                std::vector<Rid>* historical_rids = nullptr) {
    if (context == nullptr || context->txn_ == nullptr || context->txn_mgr_ == nullptr) {
        return;
    }
    std::vector<Rid> local_rids;
    auto* candidates = historical_rids != nullptr ? historical_rids : &local_rids;
    info.sm_manager->get_historical_index_key_rids(*info.tab_name, index.name, key, candidates);
    for (const auto& existing_rid : *candidates) {
        if (HistoricalIndexKeyConflictsWithTxn(info.fh, existing_rid, *index.meta, key, context)) {
            throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
    }
}

} // namespace

Rid InsertRuntime::InsertOne(RmRecord& record, const InsertRuntimeInfo& info, Context* context) {
    auto* txn = context == nullptr ? nullptr : context->txn_;
    if (txn != nullptr && txn->get_isolation_level() != IsolationLevel::READ_COMMITTED &&
        DeletedTupleCandidatesConflictWithInsert(info.fh, info.sm_manager, *info.tab_name, record, context)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
    }

    RowMutationKeyScratchLease key_scratch_lease;
    auto& key_scratch = key_scratch_lease.get();
    key_scratch.PrepareSingle(info.indexes->size());
    auto& index_keys = key_scratch.keys;
    std::vector<Rid> historical_rids;
    historical_rids.reserve(1);
    for (size_t index_idx = 0; index_idx < info.indexes->size(); ++index_idx) {
        const auto& index = (*info.indexes)[index_idx];
        auto& key = index_keys[index_idx];
        MakeRowMutationIndexKey(*index.meta, record.data, key);
        ReserveUniqueKey(context, index.handle->GetFd(), key);
        CheckMvccUniqueKeyConflict(info, index, key, context, &historical_rids);
    }

    std::vector<size_t> inserted_indexes;
    inserted_indexes.reserve(info.indexes->size());
    auto prepared_insert = info.fh->prepare_insert_record();
    Rid rid = prepared_insert.rid;
    bool insert_finished = false;
    try {
        if (txn != nullptr) {
            auto write_record = std::make_unique<WriteRecord>(WType::INSERT_TUPLE, *info.tab_name, rid);
            txn->append_write_record(std::move(write_record));
            txn->append_modified_slot(*info.tab_name, rid);
        }
        MutationFaultPoint("insert_after_rollback_registration");

        lsn_t log_lsn = INVALID_LSN;
        TupleMeta pending_meta;
        if (txn != nullptr) {
            pending_meta.writer_txn_id_ = txn->get_transaction_id();
            pending_meta.is_committed_ = false;
            pending_meta.is_deleted_ = false;
            pending_meta.version_chain_head_ = UndoLink{};
        }
        if (context != nullptr && context->log_mgr_ != nullptr && txn != nullptr) {
            InsertLogRecord log_record(txn->get_transaction_id(), record, rid, *info.tab_name);
            log_record.prev_lsn_ = txn->get_prev_lsn();
            lsn_t lsn = context->log_mgr_->add_log_to_buffer(&log_record);
            txn->set_prev_lsn(lsn);
            log_lsn = lsn;
        }
        info.fh->finish_insert_record(prepared_insert, record.data, txn == nullptr ? nullptr : &pending_meta, log_lsn);
        insert_finished = true;
        MutationFaultPoint("insert_after_heap_finish");
        for (size_t i = 0; i < info.indexes->size(); ++i) {
            (*info.indexes)[i].handle->insert_entry(index_keys[i].data(), rid, txn);
            inserted_indexes.push_back(i);
            MutationFaultPoint("insert_after_index_insert");
        }
        MutationFaultPoint("insert_after_all_index_inserts");

        if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
            context->txn_mgr_ != nullptr) {
            txn_id_t writer_id = txn->get_transaction_id();
            if (context->txn_mgr_->CheckWriteAgainstReaders(writer_id, rid, *info.tab_name, std::nullopt,
                                                            std::optional<RmRecord>(record), info.tab->cols)) {
                throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
            }
        }
    } catch (...) {
        for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
            (*info.indexes)[*it].handle->delete_entry(index_keys[*it].data(), txn);
        }
        if (insert_finished) {
            info.fh->delete_record(rid, context);
        } else {
            info.fh->abort_prepared_insert(prepared_insert);
        }
        throw;
    }
    return rid;
}
