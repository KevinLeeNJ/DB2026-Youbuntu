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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>
#include <optional>
#include <string_view>

namespace {

const ColMeta& FindColumn(const TabMeta& tab, const TabCol& target) {
    for (const auto& col : tab.cols) {
        if (col.tab_name == target.tab_name && col.name == target.col_name) {
            return col;
        }
    }
    throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
}

BoundMutationColumn BindColumn(const ColMeta& col) {
    return BoundMutationColumn{static_cast<uint32_t>(col.offset), static_cast<uint32_t>(col.len), col.type,
                               col.null_byte, col.null_mask};
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
    // 与执行器 compare() 共用同一份三值逻辑，见 common/common.h
    switch (eval_condition_nulls(condition, record.data, bound.lhs.null_byte, bound.lhs.null_mask, bound.rhs.null_byte,
                                 bound.rhs.null_mask)) {
    case NullEval::DECIDED_TRUE:
        return true;
    case NullEval::DECIDED_FALSE:
        return false;
    case NullEval::COMPARE:
        break;
    }
    const ColType rhs_type = condition.is_rhs_val ? condition.rhs_val.type : bound.rhs.type;
    const char* lhs_data = record.data + bound.lhs.offset;
    const char* rhs_data = condition.is_rhs_val ? nullptr : record.data + bound.rhs.offset;
    if (!CanCast(bound.lhs.type, rhs_type)) {
        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(rhs_type));
    }

    if (bound.lhs.type == TYPE_INT || bound.lhs.type == TYPE_FLOAT) {
        const double lhs_value = bound.lhs.type == TYPE_INT ? static_cast<double>(read_unaligned<int>(lhs_data))
                                                            : static_cast<double>(read_float(lhs_data));
        const double rhs_value =
            condition.is_rhs_val
                ? (rhs_type == TYPE_INT ? static_cast<double>(condition.rhs_val.int_val) : condition.rhs_val.float_val)
                : (rhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(rhs_data))
                                        : static_cast<double>(read_float(rhs_data)));
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

double ApplyNumericUpdateOp(double base, double delta, UpdateOp op) {
    double result = base;
    switch (op) {
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
        if (delta == 0.0) {
            throw InternalError("division by zero in UPDATE");
        }
        result = base / delta;
        break;
    case UpdateOp::ASSIGNMENT:
        throw InternalError("assignment is not an arithmetic UPDATE operation");
    }
    if (!std::isfinite(result)) {
        throw RMDBError("FLOAT arithmetic result must be finite");
    }
    return result;
}

bool PrepareWrite(const Rid& rid, RmRecord& visible_record, const RowMutationRuntimeInfo& info, Context* context) {
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
    if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED && context->txn_mgr_ != nullptr) {
        auto current_record = GetCurrentRecordForRcWrite(info.fh, rid, txn, context);
        if (!current_record.has_value()) {
            return false;
        }
        visible_record = *current_record->record;
        if (!Matches(info.conditions, info.bound_conditions, visible_record)) {
            return false;
        }
    }

    const TupleMeta meta = info.fh->get_tuple_meta(rid);
    if (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id()) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
    }
    const IsolationLevel level = txn->get_isolation_level();
    const bool snapshot_conflict_check = level == IsolationLevel::SNAPSHOT_ISOLATION ||
                                         level == IsolationLevel::REPEATABLE_READ ||
                                         level == IsolationLevel::SERIALIZABLE;
    if (snapshot_conflict_check && meta.is_committed_ && meta.commit_ts_ > txn->get_read_ts() &&
        meta.writer_txn_id_ != txn->get_transaction_id()) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
    }
    return true;
}

std::vector<char> MakeIndexKey(const IndexMeta& index, const char* record_data) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        std::memcpy(key.data() + offset, record_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
    return key;
}

void ApplyUpdate(RmRecord& record, const RmRecord& old_record, const UpdateRuntimeInfo& info) {
    if (info.bound_set_clauses == nullptr || info.set_clauses->size() != info.bound_set_clauses->size()) {
        throw InternalError("mutation SET binding is out of date");
    }
    for (size_t i = 0; i < info.set_clauses->size(); ++i) {
        const auto& set_clause = (*info.set_clauses)[i];
        const auto& bound = (*info.bound_set_clauses)[i];
        char* data = record.data + bound.lhs.offset;
        // 目标列的 NULL 位由本次赋值重新决定：先算出新值是否为 NULL，是则置位
        // 并把数据字节清零，否则清位后走下面原有的写入路径。
        // SET col = NULL 与 SET col = <非 NULL> 都要经过这里，才能双向切换。
        bool assigns_null = set_clause.is_self_ref ? is_null_at(old_record.data, bound.rhs.null_byte,
                                                                bound.rhs.null_mask) // NULL 参与算术仍是 NULL
                                                   : set_clause.rhs.is_null;
        if (set_clause.is_self_ref && set_clause.op != UpdateOp::ASSIGNMENT && set_clause.rhs.is_null) {
            assigns_null = true;
        }
        for (const auto& term : set_clause.additional_terms) {
            if (term.rhs.is_null) {
                assigns_null = true;
                break;
            }
        }
        if (assigns_null) {
            std::memset(data, 0, bound.lhs.len);
            set_null_at(record.data, bound.lhs.null_byte, bound.lhs.null_mask);
            continue;
        }
        clear_null_at(record.data, bound.lhs.null_byte, bound.lhs.null_mask);
        if (set_clause.is_self_ref) {
            if (set_clause.op == UpdateOp::ASSIGNMENT) {
                if (bound.lhs.type == TYPE_INT && bound.rhs.type == TYPE_FLOAT) {
                    write_unaligned(data, static_cast<int>(read_float(old_record.data + bound.rhs.offset)));
                } else if (bound.lhs.type == TYPE_FLOAT && bound.rhs.type == TYPE_INT) {
                    write_float(data, static_cast<float>(read_unaligned<int>(old_record.data + bound.rhs.offset)));
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
                    write_float(data, read_float(old_record.data + bound.rhs.offset));
                }
                continue;
            }

            if ((bound.rhs.type != TYPE_INT && bound.rhs.type != TYPE_FLOAT) ||
                (set_clause.rhs.type != TYPE_INT && set_clause.rhs.type != TYPE_FLOAT)) {
                throw IncompatibleTypeError(coltype2str(bound.rhs.type), coltype2str(set_clause.rhs.type));
            }
            const double base = bound.rhs.type == TYPE_INT
                                    ? static_cast<double>(read_unaligned<int>(old_record.data + bound.rhs.offset))
                                    : static_cast<double>(read_float(old_record.data + bound.rhs.offset));
            const double delta = set_clause.rhs.type == TYPE_INT ? static_cast<double>(set_clause.rhs.int_val)
                                                                 : set_clause.rhs.float_val;
            switch (bound.lhs.type) {
            case TYPE_INT: {
                double result = ApplyNumericUpdateOp(base, delta, set_clause.op);
                for (const auto& term : set_clause.additional_terms) {
                    if (term.rhs.type != TYPE_INT && term.rhs.type != TYPE_FLOAT) {
                        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(term.rhs.type));
                    }
                    const double term_value =
                        term.rhs.type == TYPE_INT ? static_cast<double>(term.rhs.int_val) : term.rhs.float_val;
                    result = ApplyNumericUpdateOp(result, term_value, term.op);
                }
                write_unaligned(data, static_cast<int>(result));
                break;
            }
            case TYPE_FLOAT: {
                float float_result = static_cast<float>(ApplyNumericUpdateOp(base, delta, set_clause.op));
                if (!std::isfinite(float_result)) {
                    throw RMDBError("FLOAT arithmetic result must be finite");
                }
                // Each SQL +/- node consumes the previous binary32 result and
                // rounds once back to binary32. Do not combine the scalar terms.
                for (const auto& term : set_clause.additional_terms) {
                    if (term.rhs.type != TYPE_INT && term.rhs.type != TYPE_FLOAT) {
                        throw IncompatibleTypeError(coltype2str(bound.lhs.type), coltype2str(term.rhs.type));
                    }
                    const double term_value =
                        term.rhs.type == TYPE_INT ? static_cast<double>(term.rhs.int_val) : term.rhs.float_val;
                    float_result = static_cast<float>(
                        ApplyNumericUpdateOp(static_cast<double>(float_result), term_value, term.op));
                    if (!std::isfinite(float_result)) {
                        throw RMDBError("FLOAT arithmetic result must be finite");
                    }
                }
                write_float(data, float_result);
                break;
            }
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
            write_float(data, set_clause.rhs.type == TYPE_FLOAT ? set_clause.rhs.float_val
                                                                : static_cast<float>(set_clause.rhs.int_val));
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
                                   const std::vector<char>& key, const Rid& rid, Context* context) {
    auto* txn = context == nullptr ? nullptr : context->txn_;
    for (const auto& candidate_rid : info.sm_manager->get_historical_index_key_rids(*info.tab_name, index.name, key)) {
        if (candidate_rid != rid &&
            HistoricalIndexKeyConflictsWithTxn(info.fh, candidate_rid, *index.meta, key, context)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
    }
}

void RollbackIndexUpdates(const std::vector<std::pair<const RowMutationIndex*, std::vector<char>>>& deleted,
                          const std::vector<std::pair<const RowMutationIndex*, std::vector<char>>>& inserted,
                          const Rid& rid, Transaction* txn) {
    for (auto it = inserted.rbegin(); it != inserted.rend(); ++it) {
        it->first->handle->delete_entry(it->second.data(), rid, txn);
    }
    for (auto it = deleted.rbegin(); it != deleted.rend(); ++it) {
        it->first->handle->insert_entry(it->second.data(), rid, txn, true);
    }
}

} // namespace

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
    if (!PrepareWrite(rid, visible_record, info, context)) {
        return false;
    }

    auto new_record = std::make_unique<RmRecord>(visible_record);
    ApplyUpdate(*new_record, visible_record, info);
    auto* txn = context == nullptr ? nullptr : context->txn_;

    if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context->txn_mgr_ != nullptr) {
        if (context->txn_mgr_->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, *info.tab_name,
                                                        std::optional<RmRecord>(visible_record),
                                                        std::optional<RmRecord>(*new_record), info.tab->cols)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    struct IndexUpdate {
        const RowMutationIndex* index;
        std::vector<char> old_key;
        std::vector<char> new_key;
    };
    std::vector<IndexUpdate> index_updates;
    for (size_t index_idx = 0; index_idx < info.indexes->size(); ++index_idx) {
        if (!(*info.affected_index_bitmap)[index_idx]) {
            continue;
        }
        const auto& index = (*info.indexes)[index_idx];
        auto old_key = MakeIndexKey(*index.meta, visible_record.data);
        auto new_key = MakeIndexKey(*index.meta, new_record->data);
        if (old_key == new_key) {
            continue;
        }
        ReserveUniqueKey(context, index.handle->GetFd(), old_key);
        ReserveUniqueKey(context, index.handle->GetFd(), new_key);
        std::vector<Rid> result;
        if (index.handle->get_value(new_key.data(), &result, txn) &&
            std::any_of(result.begin(), result.end(), [&](const Rid& found) { return found != rid; })) {
            // Losing a race for a unique key inside an explicit transaction is
            // a retryable conflict. A duplicate produced by an autocommit
            // statement, CREATE INDEX or LOAD is deterministic: retrying it
            // cannot help, so it stays a permanent SQL error.
            if (txn != nullptr && txn->get_txn_mode()) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UNIQUE_KEY_CONFLICT);
            }
            throw IndexEntryExistsError();
        }
        CheckHistoricalIndexConflicts(info, index, new_key, rid, context);
        index_updates.push_back(IndexUpdate{&index, std::move(old_key), std::move(new_key)});
    }

    lsn_t log_lsn = INVALID_LSN;
    if (context != nullptr && context->log_mgr_ != nullptr && txn != nullptr) {
        Rid log_rid = rid;
        UpdateLogRecord log_record(txn->get_transaction_id(), visible_record, *new_record, log_rid, *info.tab_name);
        log_record.prev_lsn_ = txn->get_prev_lsn();
        log_lsn = context->log_mgr_->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(log_lsn);
    }
    if (txn != nullptr) {
        txn->append_write_record(
            std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, *info.tab_name, rid, visible_record));
        UndoLog undo;
        undo.is_deleted_ = false;
        undo.old_meta_ = info.fh->get_tuple_meta(rid);
        undo.old_tuple_data_.assign(visible_record.data, visible_record.data + visible_record.size);
        undo.prev_version_ = undo.old_meta_.version_chain_head_;
        const UndoLink undo_link = txn->AppendUndoLog(undo);
        txn->append_modified_slot(*info.tab_name, rid);
        TupleMeta meta;
        meta.writer_txn_id_ = txn->get_transaction_id();
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = undo_link;
        info.fh->apply_tuple_update(rid, new_record->data, meta, log_lsn);
    }

    std::vector<std::pair<const RowMutationIndex*, std::vector<char>>> deleted;
    std::vector<std::pair<const RowMutationIndex*, std::vector<char>>> inserted;
    try {
        for (const auto& update : index_updates) {
            info.sm_manager->remember_historical_index_key(*info.tab_name, update.index->name, update.old_key, rid,
                                                           *update.index->meta,
                                                           txn == nullptr ? INVALID_TS : txn->get_read_ts());
            update.index->handle->delete_entry(update.old_key.data(), rid, txn);
            deleted.emplace_back(update.index, update.old_key);
            update.index->handle->insert_entry(update.new_key.data(), rid, txn);
            inserted.emplace_back(update.index, update.new_key);
        }
    } catch (const IndexEntryExistsError&) {
        // The B+tree has no transaction context, so translate here.
        RollbackIndexUpdates(deleted, inserted, rid, txn);
        if (txn != nullptr && txn->get_txn_mode()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UNIQUE_KEY_CONFLICT);
        }
        throw;
    } catch (...) {
        RollbackIndexUpdates(deleted, inserted, rid, txn);
        throw;
    }
    if (txn == nullptr) {
        const TupleMeta committed_meta = info.fh->get_tuple_meta(rid);
        info.fh->apply_tuple_update(rid, new_record->data, committed_meta, log_lsn);
    }
    return true;
}

bool RowMutationEngine::DeleteOne(const Rid& rid, RmRecord& visible_record, const DeleteRuntimeInfo& info,
                                  Context* context) {
    if (!PrepareWrite(rid, visible_record, info, context)) {
        return false;
    }
    auto* txn = context == nullptr ? nullptr : context->txn_;
    if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && context->txn_mgr_ != nullptr &&
        context->txn_mgr_->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, *info.tab_name,
                                                    std::optional<RmRecord>(visible_record), std::nullopt,
                                                    info.tab->cols)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
    }

    lsn_t log_lsn = INVALID_LSN;
    if (context != nullptr && context->log_mgr_ != nullptr && txn != nullptr) {
        Rid log_rid = rid;
        DeleteLogRecord log_record(txn->get_transaction_id(), visible_record, log_rid, *info.tab_name);
        log_record.prev_lsn_ = txn->get_prev_lsn();
        log_lsn = context->log_mgr_->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(log_lsn);
    }

    std::vector<std::pair<const RowMutationIndex*, std::vector<char>>> deleted;
    try {
        for (const auto& index : *info.indexes) {
            auto key = MakeIndexKey(*index.meta, visible_record.data);
            ReserveUniqueKey(context, index.handle->GetFd(), key);
            info.sm_manager->remember_historical_index_key(*info.tab_name, index.name, key, rid, *index.meta,
                                                           txn == nullptr ? INVALID_TS : txn->get_read_ts());
            index.handle->delete_entry(key.data(), rid, txn);
            deleted.emplace_back(&index, std::move(key));
        }
    } catch (...) {
        std::vector<std::pair<const RowMutationIndex*, std::vector<char>>> inserted;
        RollbackIndexUpdates(deleted, inserted, rid, txn);
        throw;
    }

    if (txn != nullptr) {
        UndoLog undo;
        undo.is_deleted_ = true;
        undo.old_meta_ = info.fh->get_tuple_meta(rid);
        undo.old_tuple_data_.assign(visible_record.data, visible_record.data + visible_record.size);
        undo.prev_version_ = undo.old_meta_.version_chain_head_;
        const UndoLink undo_link = txn->AppendUndoLog(undo);
        txn->append_write_record(
            std::make_unique<WriteRecord>(WType::DELETE_TUPLE, *info.tab_name, rid, visible_record));
        txn->append_modified_slot(*info.tab_name, rid);
        TupleMeta tombstone;
        tombstone.writer_txn_id_ = txn->get_transaction_id();
        tombstone.is_committed_ = false;
        tombstone.is_deleted_ = true;
        tombstone.version_chain_head_ = undo_link;
        info.fh->set_tuple_meta(rid, tombstone, log_lsn);
        info.sm_manager->remember_deleted_tuple_candidate(*info.tab_name, rid, txn->get_read_ts(),
                                                          RecordDataHash(visible_record));
    } else {
        info.fh->delete_record(rid, context);
    }
    return true;
}

Rid RowInsertEngine::InsertOne(SmManager* sm_manager, const std::string& tab_name, const TabMeta& tab, RmFileHandle* fh,
                               RmRecord& record, Context* context, Rid* prepared_rid) {
    if (sm_manager == nullptr || fh == nullptr || record.size != fh->get_file_hdr().record_size) {
        throw InternalError("invalid row insert runtime metadata");
    }
    if (context != nullptr && context->txn_ != nullptr &&
        context->txn_->get_isolation_level() != IsolationLevel::READ_COMMITTED &&
        DeletedTupleCandidatesConflictWithInsert(fh, sm_manager, tab_name, record, context)) {
        throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
    }

    std::vector<std::vector<char>> index_keys;
    std::vector<std::string> index_names;
    index_keys.reserve(tab.indexes.size());
    index_names.reserve(tab.indexes.size());
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, record.data);
        std::string index_name = sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager->ihs_.at(index_name).get();
        ReserveUniqueKey(context, ih->GetFd(), key);
        if (context != nullptr && context->txn_ != nullptr && context->txn_mgr_ != nullptr) {
            for (const auto& existing_rid : sm_manager->get_historical_index_key_rids(tab_name, index_name, key)) {
                if (HistoricalIndexKeyConflictsWithTxn(fh, existing_rid, index, key, context)) {
                    throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::WW_CONFLICT);
                }
            }
        }
        index_keys.push_back(std::move(key));
        index_names.push_back(std::move(index_name));
    }

    auto prepared_insert = fh->prepare_insert_record();
    Rid rid = prepared_insert.rid;
    if (prepared_rid != nullptr) {
        *prepared_rid = rid;
    }
    bool insert_finished = false;
    try {
        lsn_t log_lsn = INVALID_LSN;
        TupleMeta pending_meta;
        if (context != nullptr && context->txn_ != nullptr) {
            pending_meta.writer_txn_id_ = context->txn_->get_transaction_id();
            pending_meta.is_committed_ = false;
            pending_meta.is_deleted_ = false;
            pending_meta.version_chain_head_ = UndoLink{};
        }
        if (context != nullptr && context->log_mgr_ != nullptr && context->txn_ != nullptr) {
            InsertLogRecord log_record(context->txn_->get_transaction_id(), record, rid, tab_name);
            log_record.prev_lsn_ = context->txn_->get_prev_lsn();
            log_lsn = context->log_mgr_->add_log_to_buffer(&log_record);
            context->txn_->set_prev_lsn(log_lsn);
        }
        fh->finish_insert_record(prepared_insert, record.data,
                                 context != nullptr && context->txn_ != nullptr ? &pending_meta : nullptr, log_lsn);
        insert_finished = true;
    } catch (...) {
        if (!insert_finished) {
            fh->abort_prepared_insert(prepared_insert);
        }
        throw;
    }

    std::vector<size_t> inserted_indexes;
    const auto rollback_index_inserts = [&] {
        for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
            sm_manager->ihs_.at(index_names[*it])
                ->delete_entry(index_keys[*it].data(), rid, context == nullptr ? nullptr : context->txn_);
        }
        fh->delete_record(rid, context);
    };
    try {
        for (size_t i = 0; i < tab.indexes.size(); ++i) {
            sm_manager->ihs_.at(index_names[i])
                ->insert_entry(index_keys[i].data(), rid, context == nullptr ? nullptr : context->txn_);
            inserted_indexes.push_back(i);
        }
    } catch (const IndexEntryExistsError&) {
        rollback_index_inserts();
        if (context != nullptr && context->txn_ != nullptr && context->txn_->get_txn_mode()) {
            throw TransactionAbortException(context->txn_->get_transaction_id(), AbortReason::UNIQUE_KEY_CONFLICT);
        }
        throw;
    } catch (...) {
        rollback_index_inserts();
        throw;
    }
    if (context != nullptr && context->txn_ != nullptr) {
        context->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_name, rid));
        context->txn_->append_modified_slot(tab_name, rid);
        if (context->txn_->get_isolation_level() == IsolationLevel::SERIALIZABLE && context->txn_mgr_ != nullptr) {
            const txn_id_t writer_id = context->txn_->get_transaction_id();
            if (context->txn_mgr_->CheckWriteAgainstReaders(writer_id, rid, tab_name, std::nullopt,
                                                            std::optional<RmRecord>(record), tab.cols)) {
                throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
            }
        }
    }
    return rid;
}
