/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
*/

#include "row_mutation.h"

#include "common/phase_metrics.h"

#include <algorithm>
#ifndef NDEBUG
#include <atomic>
#endif
#include <cstring>
#include <memory>
#include <optional>
#include <string_view>

namespace {

#ifndef NDEBUG
std::atomic<MutationFaultHook> mutation_fault_hook{nullptr};
#endif

const ColMeta& FindColumn(const TabMeta& tab, const TabCol& target) {
    for (const auto& col : tab.cols) {
        if (col.tab_name == target.tab_name && col.name == target.col_name) {
            return col;
        }
    }
    throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
}

BoundMutationColumn BindColumn(const ColMeta& col) {
    return BoundMutationColumn{static_cast<uint32_t>(col.offset), static_cast<uint32_t>(col.len), col.type};
}

bool CanCast(ColType lhs, ColType rhs) {
    if (lhs == rhs) {
        return true;
    }
    if ((lhs == TYPE_INT && rhs == TYPE_FLOAT) || (lhs == TYPE_FLOAT && rhs == TYPE_INT)) {
        return true;
    }
    return (lhs == TYPE_STRING && rhs == TYPE_DATETIME) || (lhs == TYPE_DATETIME && rhs == TYPE_STRING);
}

bool CompareCondition(const Condition& condition, const BoundMutationCondition& bound, const RmRecord& record) {
    const ColType rhs_type = condition.is_rhs_val ? condition.rhs_val.type : bound.rhs.type;
    const char* lhs_data = record.data + bound.lhs.offset;
    const char* rhs_data = condition.is_rhs_val ? nullptr : record.data + bound.rhs.offset;
    if (!CanCast(bound.lhs.type, rhs_type)) {
        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(rhs_type));
    }

    if (bound.lhs.type == TYPE_INT || bound.lhs.type == TYPE_FLOAT) {
        const double lhs_value = bound.lhs.type == TYPE_INT ? static_cast<double>(read_unaligned<int>(lhs_data))
                                                            : read_unaligned<double>(lhs_data);
        const double rhs_value =
            condition.is_rhs_val
                ? (rhs_type == TYPE_INT ? static_cast<double>(condition.rhs_val.int_val) : condition.rhs_val.float_val)
                : (rhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(rhs_data))
                                        : read_unaligned<double>(rhs_data));
        switch (condition.op) {
        case OP_EQ:
            return lhs_value == rhs_value;
        case OP_NE:
            return lhs_value != rhs_value;
        case OP_LT:
            return lhs_value < rhs_value;
        case OP_GT:
            return lhs_value > rhs_value;
        case OP_LE:
            return lhs_value <= rhs_value;
        case OP_GE:
            return lhs_value >= rhs_value;
        }
    } else {
        const std::string_view lhs_value(lhs_data, strnlen(lhs_data, bound.lhs.len));
        const std::string_view rhs_value = condition.is_rhs_val
                                               ? std::string_view(condition.rhs_val.str_val)
                                               : std::string_view(rhs_data, strnlen(rhs_data, bound.rhs.len));
        switch (condition.op) {
        case OP_EQ:
            return lhs_value == rhs_value;
        case OP_NE:
            return lhs_value != rhs_value;
        case OP_LT:
            return lhs_value < rhs_value;
        case OP_GT:
            return lhs_value > rhs_value;
        case OP_LE:
            return lhs_value <= rhs_value;
        case OP_GE:
            return lhs_value >= rhs_value;
        }
    }
    return false;
}

bool Matches(const std::vector<Condition>* conditions, const std::vector<BoundMutationCondition>* bound_conditions,
             const RmRecord& record) {
    if (conditions == nullptr) {
        return true;
    }
    if (bound_conditions == nullptr || conditions->size() != bound_conditions->size()) {
        throw InternalError("mutation condition binding is out of date");
    }
    for (size_t i = 0; i < conditions->size(); ++i) {
        if (!CompareCondition((*conditions)[i], (*bound_conditions)[i], record)) {
            return false;
        }
    }
    return true;
}

bool PrepareWrite(const Rid& rid, RmRecord& visible_record, const RowMutationRuntimeInfo& info, Context* context,
                  std::optional<TupleMeta>* prepared_meta = nullptr) {
    if (!Matches(info.conditions, info.bound_conditions, visible_record)) {
        return false;
    }
    if (context == nullptr || context->txn_ == nullptr) {
        return true;
    }

    auto* txn = context->txn_;
    if (context->lock_mgr_ != nullptr && !context->lock_mgr_->lock_exclusive_on_record(txn, rid, info.fh->GetFd())) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    std::optional<TupleMeta> current_meta;
    if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED && context->txn_mgr_ != nullptr) {
        auto current_record = GetCurrentRecordForRcWrite(info.fh, rid, txn, context);
        if (!current_record.has_value()) {
            return false;
        }
        current_meta = current_record->meta;
        visible_record = *current_record->record;
        if (!Matches(info.conditions, info.bound_conditions, visible_record)) {
            return false;
        }
    }

    const TupleMeta meta = current_meta.has_value() ? *current_meta : info.fh->get_tuple_meta(rid);
    if (prepared_meta != nullptr) {
        *prepared_meta = meta;
    }
    if (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id()) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
    }
    const IsolationLevel level = txn->get_isolation_level();
    const bool snapshot_conflict_check = level == IsolationLevel::SNAPSHOT_ISOLATION ||
                                         level == IsolationLevel::REPEATABLE_READ ||
                                         level == IsolationLevel::SERIALIZABLE;
    if (snapshot_conflict_check && meta.is_committed_ && meta.commit_ts_ > txn->get_start_ts() &&
        meta.writer_txn_id_ != txn->get_transaction_id()) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
    }
    return true;
}

void ApplyUpdate(RmRecord& record, const RmRecord& old_record, const UpdateRuntimeInfo& info) {
    if (info.bound_set_clauses == nullptr || info.set_clauses->size() != info.bound_set_clauses->size()) {
        throw InternalError("mutation SET binding is out of date");
    }
    for (size_t i = 0; i < info.set_clauses->size(); ++i) {
        const auto& set_clause = (*info.set_clauses)[i];
        const auto& bound = (*info.bound_set_clauses)[i];
        char* data = record.data + bound.lhs.offset;
        if (set_clause.is_self_ref) {
            if (set_clause.op == UpdateOp::ASSIGNMENT) {
                if (bound.lhs.type == TYPE_INT && bound.rhs.type == TYPE_FLOAT) {
                    write_unaligned(data, static_cast<int>(read_unaligned<double>(old_record.data + bound.rhs.offset)));
                } else if (bound.lhs.type == TYPE_FLOAT && bound.rhs.type == TYPE_INT) {
                    write_unaligned(data, static_cast<double>(read_unaligned<int>(old_record.data + bound.rhs.offset)));
                } else if (bound.lhs.type == TYPE_STRING || bound.lhs.type == TYPE_DATETIME) {
                    if (bound.rhs.type != TYPE_STRING && bound.rhs.type != TYPE_DATETIME) {
                        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(bound.rhs.type));
                    }
                    std::memset(data, 0, bound.lhs.len);
                    std::memcpy(data, old_record.data + bound.rhs.offset, std::min(bound.lhs.len, bound.rhs.len));
                } else if (bound.lhs.type == TYPE_INT) {
                    if (bound.rhs.type != TYPE_INT) {
                        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(bound.rhs.type));
                    }
                    write_unaligned(data, read_unaligned<int>(old_record.data + bound.rhs.offset));
                } else if (bound.lhs.type == TYPE_FLOAT) {
                    if (bound.rhs.type != TYPE_FLOAT) {
                        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(bound.rhs.type));
                    }
                    write_unaligned(data, read_unaligned<double>(old_record.data + bound.rhs.offset));
                }
                continue;
            }

            if ((bound.rhs.type != TYPE_INT && bound.rhs.type != TYPE_FLOAT) ||
                (set_clause.rhs.type != TYPE_INT && set_clause.rhs.type != TYPE_FLOAT)) {
                throw IncompatibleTypeError(coltype2str(bound.rhs.type), coltype2str(set_clause.rhs.type));
            }
            const double base = bound.rhs.type == TYPE_INT
                                    ? static_cast<double>(read_unaligned<int>(old_record.data + bound.rhs.offset))
                                    : read_unaligned<double>(old_record.data + bound.rhs.offset);
            const double delta = set_clause.rhs.type == TYPE_INT ? static_cast<double>(set_clause.rhs.int_val)
                                                                 : set_clause.rhs.float_val;
            double result = base;
            switch (set_clause.op) {
            case UpdateOp::SELF_ADD:
                result = base + delta;
                break;
            case UpdateOp::SELF_SUB:
                result = base - delta;
                break;
            case UpdateOp::SELF_MUL:
                result = base * delta;
                break;
            case UpdateOp::SELF_DIV:
                if (delta == 0.0F) {
                    throw InternalError("division by zero in UPDATE");
                }
                result = base / delta;
                break;
            case UpdateOp::ASSIGNMENT:
                result = base;
                break;
            }
            switch (bound.lhs.type) {
            case TYPE_INT:
                write_unaligned(data, static_cast<int>(result));
                break;
            case TYPE_FLOAT:
                write_unaligned(data, result);
                break;
            case TYPE_STRING:
            case TYPE_DATETIME:
                throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(bound.rhs.type));
            }
            continue;
        }

        if (!CanCast(bound.lhs.type, set_clause.rhs.type)) {
            throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(set_clause.rhs.type));
        }
        switch (bound.lhs.type) {
        case TYPE_INT:
            write_unaligned(data, set_clause.rhs.type == TYPE_INT ? set_clause.rhs.int_val
                                                                  : static_cast<int>(set_clause.rhs.float_val));
            break;
        case TYPE_FLOAT:
            write_unaligned(data, set_clause.rhs.type == TYPE_FLOAT ? set_clause.rhs.float_val
                                                                    : static_cast<double>(set_clause.rhs.int_val));
            break;
        case TYPE_STRING:
        case TYPE_DATETIME:
            std::memset(data, 0, bound.lhs.len);
            std::memcpy(data, set_clause.rhs.str_val.data(),
                        std::min(static_cast<size_t>(bound.lhs.len), set_clause.rhs.str_val.size()));
            break;
        }
    }
}

void CheckHistoricalIndexConflicts(const RowMutationRuntimeInfo& info, const RowMutationIndex& index,
                                   const std::vector<char>& key, const Rid& rid, Context* context,
                                   std::vector<Rid>* historical_rids = nullptr) {
    auto* txn = context == nullptr ? nullptr : context->txn_;
    std::vector<Rid> local_rids;
    auto* candidates = historical_rids != nullptr ? historical_rids : &local_rids;
    info.sm_manager->get_historical_index_key_rids(*info.tab_name, index.name, key, candidates);
    for (const auto& candidate_rid : *candidates) {
        if (candidate_rid != rid &&
            HistoricalIndexKeyConflictsWithTxn(info.fh, candidate_rid, *index.meta, key, context)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
    }
}

using IndexKeyRef = std::pair<const RowMutationIndex*, const std::vector<char>*>;

struct RowMutationKeyScratchPool {
    std::vector<std::unique_ptr<RowMutationKeyScratch>> slots;
    size_t depth{0};
};

thread_local RowMutationKeyScratchPool row_mutation_key_scratch_pool;

void RollbackIndexUpdates(const std::vector<IndexKeyRef>& deleted, const std::vector<IndexKeyRef>& inserted,
                          const Rid& rid, Transaction* txn) {
    for (auto it = inserted.rbegin(); it != inserted.rend(); ++it) {
        it->first->handle->delete_entry(it->second->data(), rid, txn);
    }
    for (auto it = deleted.rbegin(); it != deleted.rend(); ++it) {
        it->first->handle->insert_entry(it->second->data(), rid, txn, true);
    }
}

} // namespace

RowMutationKeyScratchLease::RowMutationKeyScratchLease() {
    const size_t depth = row_mutation_key_scratch_pool.depth;
    if (row_mutation_key_scratch_pool.slots.size() == depth) {
        row_mutation_key_scratch_pool.slots.push_back(std::make_unique<RowMutationKeyScratch>());
    }
    scratch_ = row_mutation_key_scratch_pool.slots[depth].get();
    row_mutation_key_scratch_pool.depth = depth + 1;
}

RowMutationKeyScratchLease::~RowMutationKeyScratchLease() {
    --row_mutation_key_scratch_pool.depth;
}

void SetMutationFaultHookForTesting(MutationFaultHook hook) noexcept {
#ifndef NDEBUG
    mutation_fault_hook.store(hook, std::memory_order_release);
#else
    (void)hook;
#endif
}

#ifndef NDEBUG
void MutationFaultPoint(const char* point) {
    auto hook = mutation_fault_hook.load(std::memory_order_acquire);
    if (hook != nullptr) {
        hook(point);
    }
}
#endif

void MakeRowMutationIndexKey(const IndexMeta& index, const char* record_data, std::vector<char>& out) {
    out.resize(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        std::memcpy(out.data() + offset, record_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
}

std::vector<char> MakeRowMutationIndexKey(const IndexMeta& index, const char* record_data) {
    std::vector<char> key;
    MakeRowMutationIndexKey(index, record_data, key);
    return key;
}

std::vector<BoundMutationCondition> BindMutationConditions(const TabMeta& tab,
                                                           const std::vector<Condition>& conditions) {
    std::vector<BoundMutationCondition> bound;
    bound.reserve(conditions.size());
    for (const auto& condition : conditions) {
        BoundMutationCondition item;
        item.lhs = BindColumn(FindColumn(tab, condition.lhs_col));
        item.rhs = condition.is_rhs_val ? BoundMutationColumn{0, 0, condition.rhs_val.type}
                                        : BindColumn(FindColumn(tab, condition.rhs_col));
        bound.push_back(item);
    }
    return bound;
}

std::vector<BoundMutationSetClause> BindMutationSetClauses(const TabMeta& tab,
                                                           const std::vector<SetClause>& set_clauses) {
    std::vector<BoundMutationSetClause> bound;
    bound.reserve(set_clauses.size());
    for (const auto& set_clause : set_clauses) {
        BoundMutationSetClause item;
        item.lhs = BindColumn(FindColumn(tab, set_clause.lhs));
        item.rhs = set_clause.is_self_ref ? BindColumn(FindColumn(tab, set_clause.rhs_col))
                                          : BoundMutationColumn{0, 0, set_clause.rhs.type};
        bound.push_back(item);
    }
    return bound;
}

bool RowMutationEngine::UpdateOne(const Rid& rid, RmRecord& visible_record, const UpdateRuntimeInfo& info,
                                  Context* context) {
    auto owned_record = std::make_unique<RmRecord>(visible_record);
    std::optional<TupleMeta> prepared_meta;
    if (!PrepareWrite(rid, *owned_record, info, context, &prepared_meta)) {
        return false;
    }
    auto* txn = context == nullptr ? nullptr : context->txn_;
    PreparedUpdate prepared(rid, std::move(owned_record),
                            prepared_meta.has_value() ? *prepared_meta : info.fh->get_tuple_meta(rid),
                            txn == nullptr ? INVALID_TXN_ID : txn->get_transaction_id(), info.fh->GetFd(),
                            info.sm_manager->get_catalog_generation());
    auto proposed = ComputeLegacyUpdate(prepared, info);
    CommitUpdate(std::move(prepared), *proposed, info, context);
    return true;
}

std::optional<PreparedUpdate> RowMutationEngine::PrepareUpdate(const Rid& rid, const UpdateRuntimeInfo& info,
                                                               Context* context) {
    auto visible_record = GetVisibleRecord(info.fh, rid, context);
    if (visible_record == nullptr) {
        if (context != nullptr && context->txn_ != nullptr &&
            context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED) {
            throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
        return std::nullopt;
    }
    std::optional<TupleMeta> prepared_meta;
    if (!PrepareWrite(rid, *visible_record, info, context, &prepared_meta)) {
        return std::nullopt;
    }
    auto* txn = context == nullptr ? nullptr : context->txn_;
    return PreparedUpdate(rid, std::move(visible_record),
                          prepared_meta.has_value() ? *prepared_meta : info.fh->get_tuple_meta(rid),
                          txn == nullptr ? INVALID_TXN_ID : txn->get_transaction_id(), info.fh->GetFd(),
                          info.sm_manager->get_catalog_generation());
}

std::unique_ptr<RmRecord> RowMutationEngine::ComputeLegacyUpdate(const PreparedUpdate& prepared,
                                                                 const UpdateRuntimeInfo& info) {
    auto new_record = std::make_unique<RmRecord>(prepared.old_record());
    {
        phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::UPDATE_ARITHMETIC,
                                                   phase_metrics::sample_rate(phase_metrics::Phase::UPDATE_ARITHMETIC));
        ApplyUpdate(*new_record, prepared.old_record(), info);
    }
    return new_record;
}

void RowMutationEngine::CommitUpdate(PreparedUpdate&& prepared, RmRecord& proposed, const UpdateRuntimeInfo& info,
                                     Context* context) {
    auto* txn = context == nullptr ? nullptr : context->txn_;
    const txn_id_t txn_id = txn == nullptr ? INVALID_TXN_ID : txn->get_transaction_id();
    if (prepared.consumed_) {
        throw InternalError("prepared UPDATE was already consumed");
    }
    if (prepared.old_record_ == nullptr || prepared.old_record_->size != proposed.size ||
        proposed.size != info.fh->get_file_hdr().record_size) {
        throw InternalError("prepared UPDATE record size mismatch");
    }
    if (prepared.txn_id_ != txn_id || prepared.table_fd_ != info.fh->GetFd() ||
        prepared.catalog_generation_ != info.sm_manager->get_catalog_generation()) {
        throw InternalError("prepared UPDATE execution context mismatch");
    }
    prepared.consumed_ = true;

    const Rid& rid = prepared.rid_;
    RmRecord& visible_record = *prepared.old_record_;

    if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context->txn_mgr_ != nullptr) {
        if (context->txn_mgr_->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, *info.tab_name,
                                                        std::optional<RmRecord>(visible_record),
                                                        std::optional<RmRecord>(proposed), info.tab->cols)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    struct IndexUpdate {
        const RowMutationIndex* index;
        const std::vector<char>* old_key;
        const std::vector<char>* new_key;
    };
    RowMutationKeyScratchLease key_scratch_lease;
    auto& key_scratch = key_scratch_lease.get();
    key_scratch.PrepareUpdate(info.indexes->size());
    std::vector<IndexUpdate> index_updates;
    index_updates.reserve(info.indexes->size());
    std::vector<Rid> matching_rids;
    matching_rids.reserve(1);
    for (size_t index_idx = 0; index_idx < info.indexes->size(); ++index_idx) {
        if (!(*info.affected_index_bitmap)[index_idx]) {
            continue;
        }
        const auto& index = (*info.indexes)[index_idx];
        auto& old_key = key_scratch.old_keys[index_idx];
        auto& new_key = key_scratch.new_keys[index_idx];
        MakeRowMutationIndexKey(*index.meta, visible_record.data, old_key);
        MakeRowMutationIndexKey(*index.meta, proposed.data, new_key);
        if (old_key == new_key) {
            continue;
        }
        ReserveUniqueKey(context, index.handle->GetFd(), old_key);
        ReserveUniqueKey(context, index.handle->GetFd(), new_key);
        if (index.handle->get_value(new_key.data(), &matching_rids, txn) &&
            std::any_of(matching_rids.begin(), matching_rids.end(), [&](const Rid& found) { return found != rid; })) {
            throw IndexEntryExistsError();
        }
        CheckHistoricalIndexConflicts(info, index, new_key, rid, context, &matching_rids);
        index_updates.push_back(IndexUpdate{&index, &old_key, &new_key});
    }

    std::vector<const IndexUpdate*> deleted;
    std::vector<const IndexUpdate*> inserted;
    deleted.reserve(index_updates.size());
    inserted.reserve(index_updates.size());

    lsn_t log_lsn = INVALID_LSN;
    if (context != nullptr && context->log_mgr_ != nullptr && txn != nullptr) {
        Rid log_rid = rid;
        UpdateLogRecord log_record(txn->get_transaction_id(), visible_record, proposed, log_rid, *info.tab_name);
        log_record.prev_lsn_ = txn->get_prev_lsn();
        log_lsn = context->log_mgr_->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(log_lsn);
    }
    if (txn != nullptr) {
        txn->append_write_record(
            std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, *info.tab_name, rid, visible_record));
        UndoLog undo;
        undo.is_deleted_ = false;
        undo.old_meta_ = prepared.old_meta_;
        undo.old_tuple_data_.assign(visible_record.data, visible_record.data + visible_record.size);
        undo.prev_version_ = undo.old_meta_.version_chain_head_;
        const UndoLink undo_link = txn->AppendUndoLog(undo);
        txn->append_modified_slot(*info.tab_name, rid);
        TupleMeta meta;
        meta.writer_txn_id_ = txn->get_transaction_id();
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = undo_link;
        info.fh->apply_tuple_update(rid, proposed.data, meta, log_lsn);
    }

    try {
        for (const auto& update : index_updates) {
            info.sm_manager->remember_historical_index_key(*info.tab_name, update.index->name, *update.old_key, rid,
                                                           *update.index->meta);
            update.index->handle->delete_entry(update.old_key->data(), rid, txn);
            deleted.push_back(&update);
            MutationFaultPoint("update_after_index_delete");
            update.index->handle->insert_entry(update.new_key->data(), rid, txn);
            inserted.push_back(&update);
        }
        MutationFaultPoint("update_after_all_index_updates");
    } catch (...) {
        for (auto it = inserted.rbegin(); it != inserted.rend(); ++it) {
            (*it)->index->handle->delete_entry((*it)->new_key->data(), rid, txn);
        }
        for (auto it = deleted.rbegin(); it != deleted.rend(); ++it) {
            (*it)->index->handle->insert_entry((*it)->old_key->data(), rid, txn, true);
        }
        throw;
    }
    if (txn == nullptr) {
        const TupleMeta committed_meta = info.fh->get_tuple_meta(rid);
        info.fh->apply_tuple_update(rid, proposed.data, committed_meta, log_lsn);
    }
}

bool RowMutationEngine::DeleteOne(const Rid& rid, RmRecord& visible_record, const DeleteRuntimeInfo& info,
                                  Context* context) {
    std::optional<TupleMeta> prepared_meta;
    if (!PrepareWrite(rid, visible_record, info, context, &prepared_meta)) {
        return false;
    }
    auto* txn = context == nullptr ? nullptr : context->txn_;
    if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context->txn_mgr_ != nullptr &&
        context->txn_mgr_->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, *info.tab_name,
                                                    std::optional<RmRecord>(visible_record), std::nullopt,
                                                    info.tab->cols)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
    }

    RowMutationKeyScratchLease key_scratch_lease;
    auto& key_scratch = key_scratch_lease.get();
    key_scratch.PrepareSingle(info.indexes->size());
    std::vector<IndexKeyRef> deleted;
    deleted.reserve(info.indexes->size());

    lsn_t log_lsn = INVALID_LSN;
    if (context != nullptr && context->log_mgr_ != nullptr && txn != nullptr) {
        Rid log_rid = rid;
        DeleteLogRecord log_record(txn->get_transaction_id(), visible_record, log_rid, *info.tab_name);
        log_record.prev_lsn_ = txn->get_prev_lsn();
        log_lsn = context->log_mgr_->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(log_lsn);
    }

    TupleMeta tombstone;
    if (txn != nullptr) {
        UndoLog undo;
        undo.is_deleted_ = true;
        undo.old_meta_ = prepared_meta.has_value() ? *prepared_meta : info.fh->get_tuple_meta(rid);
        undo.old_tuple_data_.assign(visible_record.data, visible_record.data + visible_record.size);
        undo.prev_version_ = undo.old_meta_.version_chain_head_;
        auto write_record = std::make_unique<WriteRecord>(WType::DELETE_TUPLE, *info.tab_name, rid, visible_record);
        const UndoLink undo_link = txn->AppendUndoLog(std::move(undo));
        txn->append_write_record(std::move(write_record));
        txn->append_modified_slot(*info.tab_name, rid);
        tombstone.writer_txn_id_ = txn->get_transaction_id();
        tombstone.is_committed_ = false;
        tombstone.is_deleted_ = true;
        tombstone.version_chain_head_ = undo_link;
    }

    try {
        for (size_t index_idx = 0; index_idx < info.indexes->size(); ++index_idx) {
            const auto& index = (*info.indexes)[index_idx];
            auto& key = key_scratch.keys[index_idx];
            MakeRowMutationIndexKey(*index.meta, visible_record.data, key);
            ReserveUniqueKey(context, index.handle->GetFd(), key);
            info.sm_manager->remember_historical_index_key(*info.tab_name, index.name, key, rid, *index.meta);
            index.handle->delete_entry(key.data(), rid, txn);
            deleted.emplace_back(&index, &key);
            MutationFaultPoint("delete_after_index_delete");
        }
        MutationFaultPoint("delete_before_heap_tombstone");
        if (txn != nullptr) {
            info.fh->set_tuple_meta(rid, tombstone, log_lsn);
            info.sm_manager->remember_deleted_tuple_candidate(*info.tab_name, rid);
            MutationFaultPoint("delete_after_heap_tombstone");
        } else {
            info.fh->delete_record(rid, context);
        }
    } catch (...) {
        std::vector<IndexKeyRef> inserted;
        RollbackIndexUpdates(deleted, inserted, rid, txn);
        throw;
    }
    return true;
}
