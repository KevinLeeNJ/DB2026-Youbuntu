/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "access/table_write_service.h"

#include <cstring>
#include <mutex>
#include <vector>

#include "execution/execution_common.h"
#include "index/ix_index_handle.h"
#include "index/ix_manager.h"
#include "recovery/log_manager.h"
#include "system/sm_meta.h"
#include "common/type_utils.h"

namespace rmdb::access {

namespace {

// 由记录数据构造索引键。
std::vector<char> MakeIndexKey(const IndexMeta& index, const char* rec_data) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
    return key;
}

// 列名 -> ColMeta 查找（与原 AbstractExecutor::get_col_offset 一致）。
ColMeta find_col(const TabMeta& tab, const TabCol& target) {
    for (const auto& col : tab.cols) {
        if (col.tab_name == target.tab_name && col.name == target.col_name) {
            return col;
        }
    }
    throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
}

} // namespace

bool TableWriteService::record_matches_conds(const TabMeta& tab, const std::vector<Condition>& conds,
                                             const RmRecord& rec) {
    for (const auto& cond : conds) {
        ColMeta lhs_col_meta = find_col(tab, cond.lhs_col);
        char* lhs_data = rec.data + lhs_col_meta.offset;
        ColType lhs_type = lhs_col_meta.type;
        ColType rhs_type;
        char* rhs_data = nullptr;
        ColMeta rhs_col_meta;
        if (!cond.is_rhs_val) {
            rhs_col_meta = find_col(tab, cond.rhs_col);
            rhs_data = rec.data + rhs_col_meta.offset;
            rhs_type = rhs_col_meta.type;
        } else {
            rhs_type = cond.rhs_val.type;
        }
        if (!can_cast(lhs_type, rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        bool ok = false;
        switch (lhs_type) {
        case TYPE_INT:
        case TYPE_FLOAT: {
            float lhs_val = lhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<int*>(lhs_data))
                                                 : *reinterpret_cast<float*>(lhs_data);
            float rhs_val;
            if (cond.is_rhs_val) {
                rhs_val = rhs_type == TYPE_INT ? static_cast<float>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
            } else {
                rhs_val = rhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<int*>(rhs_data))
                                               : *reinterpret_cast<float*>(rhs_data);
            }
            switch (cond.op) {
            case OP_EQ:
                ok = lhs_val == rhs_val;
                break;
            case OP_NE:
                ok = lhs_val != rhs_val;
                break;
            case OP_LT:
                ok = lhs_val < rhs_val;
                break;
            case OP_GT:
                ok = lhs_val > rhs_val;
                break;
            case OP_LE:
                ok = lhs_val <= rhs_val;
                break;
            case OP_GE:
                ok = lhs_val >= rhs_val;
                break;
            }
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME: {
            std::string lhs_val(lhs_data, strnlen(lhs_data, lhs_col_meta.len));
            std::string rhs_val =
                cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col_meta.len));
            switch (cond.op) {
            case OP_EQ:
                ok = lhs_val == rhs_val;
                break;
            case OP_NE:
                ok = lhs_val != rhs_val;
                break;
            case OP_LT:
                ok = lhs_val < rhs_val;
                break;
            case OP_GT:
                ok = lhs_val > rhs_val;
                break;
            case OP_LE:
                ok = lhs_val <= rhs_val;
                break;
            case OP_GE:
                ok = lhs_val >= rhs_val;
                break;
            }
            break;
        }
        }
        if (!ok) {
            return false;
        }
    }
    return true;
}

TableWriteService::TableWriteService(SchemaManager* schema_mgr, LockManager* lock_mgr, LogManager* log_mgr,
                                     TransactionManager* txn_mgr)
    : schema_mgr_(schema_mgr), lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_mgr_(txn_mgr) {}

// 管理器优先取自 StatementContext（与原 executor 行为一致：ctx->lock_mgr 等），
// StatementContext 不存在或字段为空时回退到服务构造时注入的成员。这保证既有从 DBInstance
// 注入的运行时路径，也有测试中仅通过 StatementContext 传管理器的路径。
inline LockManager* TableWriteService::resolve_lock_mgr(StatementContext* ctx) const {
    if (ctx != nullptr && ctx->lock_mgr != nullptr)
        return ctx->lock_mgr;
    return lock_mgr_;
}
inline LogManager* TableWriteService::resolve_log_mgr(StatementContext* ctx) const {
    if (ctx != nullptr && ctx->log_mgr != nullptr)
        return ctx->log_mgr;
    return log_mgr_;
}
inline TransactionManager* TableWriteService::resolve_txn_mgr(StatementContext* ctx) const {
    if (ctx != nullptr && ctx->txn_mgr != nullptr)
        return ctx->txn_mgr;
    return txn_mgr_;
}

Rid TableWriteService::insert(const std::string& tab_name, const RmRecord& rec, Transaction* txn,
                              StatementContext* ctx) {
    auto& tab = schema_mgr_->catalog().get_table(tab_name);
    auto* fh = schema_mgr_->get_table_handle(tab_name);
    auto* txn_mgr = resolve_txn_mgr(ctx);
    auto* log_mgr = resolve_log_mgr(ctx);

    // 步骤 2：MVCC 初步可见性检查（仅非 RC 隔离级别下的 deleted-tuple 冲突预检）。
    if (txn != nullptr && txn->get_isolation_level() != IsolationLevel::READ_COMMITTED &&
        DeletedTupleCandidatesConflictWithInsert(fh, tab_name, rec, ctx)) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
    }

    // 步骤 7：唯一性 + SSI 历史键冲突预检（加锁前，减少无效写入）。
    std::vector<std::vector<char>> index_keys;
    index_keys.reserve(tab.indexes.size());
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        // SSI 历史键冲突检查。
        if (txn != nullptr && txn_mgr != nullptr) {
            const std::string index_name = schema_mgr_->get_ix_manager()->get_index_name(tab_name, index.cols);
            auto candidate_rids = txn_mgr->ssi_registry().get_historical_index_key_rids(tab_name, index_name, key);
            for (const auto& existing_rid : candidate_rids) {
                if (HistoricalIndexKeyConflictsWithTxn(fh, existing_rid, index, key, ctx)) {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                }
            }
        }
        // 物理唯一性检查。
        auto ih = schema_mgr_->get_index_handle(tab_name, index.cols);
        std::vector<Rid> result;
        if (ih->get_value(key.data(), &result, txn)) {
            throw IndexEntryExistsError();
        }
        index_keys.push_back(std::move(key));
    }

    // 步骤 8/10：写 WAL + 物理 insert（持 physical latch）。
    std::unique_lock<std::mutex> physical_lock(fh->get_physical_latch());
    auto prepared = fh->prepare_insert_record();
    Rid rid = prepared.rid;
    bool insert_finished = false;
    try {
        if (log_mgr != nullptr && txn != nullptr) {
            InsertLogRecord log_record(txn->get_transaction_id(), const_cast<RmRecord&>(rec), rid,
                                       const_cast<std::string&>(tab_name));
            log_record.prev_lsn_ = txn->get_prev_lsn();
            lsn_t lsn = log_mgr->add_log_to_buffer(&log_record);
            txn->set_prev_lsn(lsn);
            // 决策 G：不调用 set_page_lsn（独立 PR 修复）。
        }
        fh->finish_insert_record(prepared, rec.data);
        insert_finished = true;
    } catch (...) {
        if (!insert_finished) {
            fh->abort_prepared_insert(prepared);
        }
        physical_lock.unlock();
        throw;
    }
    physical_lock.unlock();

    // 步骤 11：索引写入，失败逆序补偿 + 回滚 heap。
    std::vector<size_t> inserted_indexes;
    try {
        for (size_t i = 0; i < tab.indexes.size(); ++i) {
            auto ih = schema_mgr_->get_index_handle(tab_name, tab.indexes[i].cols);
            ih->insert_entry(index_keys[i].data(), rid, txn);
            inserted_indexes.push_back(i);
        }
    } catch (...) {
        for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
            auto ih = schema_mgr_->get_index_handle(tab_name, tab.indexes[*it].cols);
            ih->delete_entry(index_keys[*it].data(), txn);
        }
        fh->delete_record(rid, ctx);
        throw;
    }

    // 步骤 9/12：Undo + TupleMeta（insert 无 undo log，仅写 write_record + meta）。
    if (txn != nullptr) {
        txn->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE, tab_name, rid));
        TupleMeta meta;
        meta.writer_txn_id_ = txn->get_transaction_id();
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = UndoLink{};
        fh->set_tuple_meta(rid, meta);
        txn->append_modified_slot(tab_name, rid);

        // 步骤 13：SERIALIZABLE SSI 写-读依赖检查。
        if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && txn_mgr != nullptr) {
            txn_id_t writer_id = txn->get_transaction_id();
            if (txn_mgr->CheckWriteAgainstReaders(writer_id, rid, tab_name, std::nullopt, std::optional<RmRecord>(rec),
                                                  tab.cols)) {
                throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
            }
        }
    }
    return rid;
}

bool TableWriteService::remove(const std::string& tab_name, const Rid& rid, const std::vector<Condition>& conds,
                               Transaction* txn, StatementContext* ctx) {
    auto& tab = schema_mgr_->catalog().get_table(tab_name);
    auto* fh = schema_mgr_->get_table_handle(tab_name);
    auto* txn_mgr = resolve_txn_mgr(ctx);
    auto* lock_mgr = resolve_lock_mgr(ctx);
    auto* log_mgr = resolve_log_mgr(ctx);

    // 步骤 2/3：MVCC 可见性预检 + 加锁前谓词预检。
    auto rec = GetVisibleRecord(fh, rid, ctx);
    if (rec == nullptr) {
        return false;
    }
    if (!record_matches_conds(tab, conds, *rec)) {
        return false;
    }

    if (txn != nullptr) {
        // 步骤 4：行级 X 锁。
        if (lock_mgr != nullptr && !lock_mgr->lock_exclusive_on_record(txn, rid, fh->GetFd())) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
        // 步骤 5：RC 加锁后重读 + 谓词复检（BeginStatement + 重新可见性判断）。
        if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED && txn_mgr != nullptr) {
            txn_mgr->BeginStatement(txn);
            rec = GetVisibleRecord(fh, rid, ctx);
            if (rec == nullptr) {
                return false;
            }
            if (!record_matches_conds(tab, conds, *rec)) {
                return false;
            }
        }
        // 步骤 6：WW 冲突检测。
        TupleMeta meta = fh->get_tuple_meta(rid);
        if (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
        IsolationLevel level = txn->get_isolation_level();
        bool snapshot_conflict_check = level == IsolationLevel::SNAPSHOT_ISOLATION ||
                                       level == IsolationLevel::REPEATABLE_READ ||
                                       level == IsolationLevel::SERIALIZABLE;
        if (snapshot_conflict_check && meta.is_committed_ && meta.commit_ts_ > txn->get_start_ts() &&
            meta.writer_txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
    }

    char* rec_data = rec->data;

    // 步骤 13（前置）：SERIALIZABLE SSI 检查（保持原 executor 时序：写之前检查）。
    if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && txn_mgr != nullptr) {
        if (txn_mgr->CheckWriteAgainstReaders(txn->get_transaction_id(), rid, tab_name, std::optional<RmRecord>(*rec),
                                              std::nullopt, tab.cols)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
        }
    }

    // 步骤 8：写 WAL。
    if (log_mgr != nullptr && txn != nullptr) {
        DeleteLogRecord log_record(txn->get_transaction_id(), *rec, const_cast<Rid&>(rid),
                                   const_cast<std::string&>(tab_name));
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_mgr->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(lsn);
    }

    // 步骤 9：Undo（DELETE_TUPLE write_record + undo log 版本链）。
    std::unique_ptr<WriteRecord> undo_record =
        txn != nullptr ? std::make_unique<WriteRecord>(WType::DELETE_TUPLE, tab_name, rid, *rec) : nullptr;

    // 步骤 11：索引删除，失败逆序补偿。
    struct DeletedIndex {
        const IndexMeta* index;
        std::vector<char> key;
    };
    std::vector<DeletedIndex> deleted_indexes;
    try {
        for (auto& index : tab.indexes) {
            auto ih = schema_mgr_->get_index_handle(tab_name, index.cols);
            std::vector<char> key(index.col_tot_len);
            int offset = 0;
            for (int i = 0; i < index.col_num; ++i) {
                std::memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            if (txn_mgr != nullptr) {
                txn_mgr->ssi_registry().remember_historical_index_key(
                    tab_name, schema_mgr_->get_ix_manager()->get_index_name(tab_name, index.cols), key, rid);
            }
            ih->delete_entry(key.data(), rid, txn);
            deleted_indexes.push_back(DeletedIndex{&index, std::move(key)});
        }
    } catch (...) {
        for (auto it = deleted_indexes.rbegin(); it != deleted_indexes.rend(); ++it) {
            auto ih = schema_mgr_->get_index_handle(tab_name, it->index->cols);
            ih->insert_entry(it->key.data(), rid, txn, true);
        }
        throw;
    }

    // 步骤 12：TupleMeta（tombstone）。
    if (undo_record != nullptr) {
        UndoLog undo;
        undo.is_deleted_ = true;
        undo.old_meta_ = fh->get_tuple_meta(rid);
        undo.old_tuple_data_.assign(rec->data, rec->data + rec->size);
        undo.prev_version_ = undo.old_meta_.version_chain_head_;
        UndoLink undo_link = txn->AppendUndoLog(undo);

        txn->append_write_record(std::move(undo_record));
        txn->append_modified_slot(tab_name, rid);

        TupleMeta tombstone;
        tombstone.writer_txn_id_ = txn->get_transaction_id();
        tombstone.is_committed_ = false;
        tombstone.is_deleted_ = true;
        tombstone.version_chain_head_ = undo_link;
        fh->set_tuple_meta(rid, tombstone);
        if (txn_mgr != nullptr) {
            txn_mgr->ssi_registry().remember_deleted_tuple_candidate(tab_name, rid);
        }
    } else {
        // 无事务场景：物理删除。
        fh->delete_record(rid, ctx);
    }
    return true;
}

bool TableWriteService::update(const std::string& tab_name, const Rid& rid, const std::vector<SetClause>& set_clauses,
                               const std::vector<Condition>& conds, Transaction* txn, StatementContext* ctx) {
    auto& tab = schema_mgr_->catalog().get_table(tab_name);
    auto* fh = schema_mgr_->get_table_handle(tab_name);
    auto* txn_mgr = resolve_txn_mgr(ctx);
    auto* lock_mgr = resolve_lock_mgr(ctx);
    auto* log_mgr = resolve_log_mgr(ctx);

    bool has_non_self_ref_set =
        std::any_of(set_clauses.begin(), set_clauses.end(), [](const SetClause& c) { return !c.is_self_ref; });

    // 步骤 2/3：MVCC 可见性预检 + 加锁前谓词预检。
    auto rec = GetVisibleRecord(fh, rid, ctx);
    if (rec == nullptr) {
        return false;
    }
    if (!record_matches_conds(tab, conds, *rec)) {
        return false;
    }

    if (txn != nullptr) {
        // 步骤 4：行级 X 锁。
        if (lock_mgr != nullptr && !lock_mgr->lock_exclusive_on_record(txn, rid, fh->GetFd())) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
        // 步骤 5/6：RC 加锁后重读 + 谓词复检 + WW 检测。
        if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED && txn_mgr != nullptr) {
            timestamp_t statement_read_ts = txn->get_read_ts();
            txn_mgr->BeginStatement(txn);
            rec = GetVisibleRecord(fh, rid, ctx);
            if (rec == nullptr) {
                return false;
            }
            if (!record_matches_conds(tab, conds, *rec)) {
                return false;
            }
            TupleMeta latest_meta = fh->get_tuple_meta(rid);
            if (has_non_self_ref_set && latest_meta.is_committed_ &&
                latest_meta.writer_txn_id_ != txn->get_transaction_id() && latest_meta.commit_ts_ > statement_read_ts) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
            }
        }
        TupleMeta meta = fh->get_tuple_meta(rid);
        if (!meta.is_committed_ && meta.writer_txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
        IsolationLevel level = txn->get_isolation_level();
        bool snapshot_conflict_check = level == IsolationLevel::SNAPSHOT_ISOLATION ||
                                       level == IsolationLevel::REPEATABLE_READ ||
                                       level == IsolationLevel::SERIALIZABLE;
        if (snapshot_conflict_check && meta.is_committed_ && meta.commit_ts_ > txn->get_start_ts() &&
            meta.writer_txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
        }
    }

    // 由重读后的 old_rec 计算 new_rec（保持与原 UpdateExecutor 一致）。
    auto new_rec = std::make_unique<RmRecord>(*rec);
    apply_set_clauses(tab, set_clauses, new_rec.get(), *rec);

    // 步骤 7：索引键变更预检 + SSI 历史键冲突检查。
    struct IndexUpdate {
        const IndexMeta* index;
        std::vector<char> old_key;
        std::vector<char> new_key;
    };
    std::vector<IndexUpdate> index_updates;
    for (const auto& index : tab.indexes) {
        auto old_key = MakeIndexKey(index, rec->data);
        auto new_key = MakeIndexKey(index, new_rec->data);
        if (old_key == new_key) {
            continue;
        }
        if (txn != nullptr && txn_mgr != nullptr) {
            txn_mgr->ssi_registry().remember_historical_index_key(
                tab_name, schema_mgr_->get_ix_manager()->get_index_name(tab_name, index.cols), old_key, rid);
        }
        auto ih = schema_mgr_->get_index_handle(tab_name, index.cols);
        std::vector<Rid> result;
        if (ih->get_value(new_key.data(), &result, txn) &&
            std::any_of(result.begin(), result.end(), [&](const Rid& found) { return found != rid; })) {
            throw IndexEntryExistsError();
        }
        if (txn != nullptr && txn_mgr != nullptr) {
            const std::string index_name = schema_mgr_->get_ix_manager()->get_index_name(tab_name, index.cols);
            auto candidate_rids = txn_mgr->ssi_registry().get_historical_index_key_rids(tab_name, index_name, new_key);
            for (const auto& candidate_rid : candidate_rids) {
                if (candidate_rid != rid &&
                    HistoricalIndexKeyConflictsWithTxn(fh, candidate_rid, index, new_key, ctx)) {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::WW_CONFLICT);
                }
            }
        }
        index_updates.push_back(IndexUpdate{&index, std::move(old_key), std::move(new_key)});
    }

    // 步骤 13（前置）：SERIALIZABLE SSI 检查。
    if (txn != nullptr && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE && txn_mgr != nullptr) {
        txn_id_t writer_id = txn->get_transaction_id();
        bool danger = txn_mgr->CheckWriteAgainstReaders(writer_id, rid, tab_name, std::optional<RmRecord>(*rec),
                                                        std::optional<RmRecord>(*new_rec), tab.cols);
        if (danger) {
            throw TransactionAbortException(writer_id, AbortReason::SSI_DANGER);
        }
    }

    // 步骤 8：写 WAL。
    if (log_mgr != nullptr && txn != nullptr) {
        UpdateLogRecord log_record(txn->get_transaction_id(), *rec, *new_rec, const_cast<Rid&>(rid),
                                   const_cast<std::string&>(tab_name));
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_mgr->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(lsn);
    }

    // 步骤 9：创建 UndoLog（追加到版本链，得到 undo_link，但暂不写 TupleMeta）。
    // TupleMeta 是行的逻辑可见性开关，应在 heap 与索引都物理就绪后才更新（步骤 12），
    // 避免"TupleMeta 已翻转但 heap 仍为旧值"的中间态。
    UndoLink undo_link;
    bool has_undo = false;
    if (txn != nullptr) {
        UndoLog undo;
        undo.is_deleted_ = false;
        undo.old_meta_ = fh->get_tuple_meta(rid);
        undo.old_tuple_data_.assign(rec->data, rec->data + rec->size);
        undo.prev_version_ = undo.old_meta_.version_chain_head_;
        undo_link = txn->AppendUndoLog(undo);
        has_undo = true;
    }

    // 步骤 10：heap mutation。
    fh->update_record(rid, new_rec->data, ctx);

    // 步骤 11：索引变更，失败逆序补偿。
    std::vector<size_t> deleted_indexes;
    std::vector<size_t> inserted_indexes;
    try {
        for (size_t i = 0; i < index_updates.size(); ++i) {
            const auto& update = index_updates[i];
            auto ih = schema_mgr_->get_index_handle(tab_name, update.index->cols);
            ih->delete_entry(update.old_key.data(), rid, txn);
            deleted_indexes.push_back(i);
            ih->insert_entry(update.new_key.data(), rid, txn);
            inserted_indexes.push_back(i);
        }
    } catch (...) {
        for (auto it = inserted_indexes.rbegin(); it != inserted_indexes.rend(); ++it) {
            const auto& update = index_updates[*it];
            auto ih = schema_mgr_->get_index_handle(tab_name, update.index->cols);
            ih->delete_entry(update.new_key.data(), rid, txn);
        }
        for (auto it = deleted_indexes.rbegin(); it != deleted_indexes.rend(); ++it) {
            const auto& update = index_updates[*it];
            auto ih = schema_mgr_->get_index_handle(tab_name, update.index->cols);
            ih->insert_entry(update.old_key.data(), rid, txn, true);
        }
        throw;
    }

    // 步骤 12：写 TupleMeta（version_chain_head = undo_link）+ 登记 modified_slot。
    if (has_undo) {
        txn->append_write_record(std::make_unique<WriteRecord>(WType::UPDATE_TUPLE, tab_name, rid, *rec));
        txn->append_modified_slot(tab_name, rid);
        TupleMeta meta;
        meta.writer_txn_id_ = txn->get_transaction_id();
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        meta.version_chain_head_ = undo_link;
        fh->set_tuple_meta(rid, meta);
    }
    return true;
}

// 由 old_rec 应用 set_clauses 计算 new_rec。逻辑迁移自原 UpdateExecutor::update_record，
// 保持赋值/自引用/类型转换语义不变。
void TableWriteService::apply_set_clauses(const TabMeta& tab, const std::vector<SetClause>& set_clauses, RmRecord* rec,
                                          const RmRecord& old_rec) {
    for (const auto& set_clause : set_clauses) {
        auto col_meta = find_col(tab, set_clause.lhs);
        char* data = rec->data + col_meta.offset;
        if (set_clause.is_self_ref) {
            auto rhs_col_meta = find_col(tab, set_clause.rhs_col);
            if (set_clause.op == UpdateOp::ASSIGNMENT) {
                if (col_meta.type == TYPE_INT && rhs_col_meta.type == TYPE_FLOAT) {
                    *reinterpret_cast<int*>(data) =
                        static_cast<int>(*reinterpret_cast<const float*>(old_rec.data + rhs_col_meta.offset));
                } else if (col_meta.type == TYPE_FLOAT && rhs_col_meta.type == TYPE_INT) {
                    *reinterpret_cast<float*>(data) =
                        static_cast<float>(*reinterpret_cast<const int*>(old_rec.data + rhs_col_meta.offset));
                } else if (col_meta.type == TYPE_STRING || col_meta.type == TYPE_DATETIME) {
                    if (rhs_col_meta.type != TYPE_STRING && rhs_col_meta.type != TYPE_DATETIME) {
                        throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                    }
                    std::memset(data, 0, col_meta.len);
                    std::memcpy(data, old_rec.data + rhs_col_meta.offset, std::min(col_meta.len, rhs_col_meta.len));
                } else if (col_meta.type == TYPE_INT) {
                    if (rhs_col_meta.type != TYPE_INT) {
                        throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                    }
                    *reinterpret_cast<int*>(data) = *reinterpret_cast<const int*>(old_rec.data + rhs_col_meta.offset);
                } else if (col_meta.type == TYPE_FLOAT) {
                    if (rhs_col_meta.type != TYPE_FLOAT) {
                        throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
                    }
                    *reinterpret_cast<float*>(data) =
                        *reinterpret_cast<const float*>(old_rec.data + rhs_col_meta.offset);
                }
                continue;
            }

            if ((rhs_col_meta.type != TYPE_INT && rhs_col_meta.type != TYPE_FLOAT) ||
                (set_clause.rhs.type != TYPE_INT && set_clause.rhs.type != TYPE_FLOAT)) {
                throw IncompatibleTypeError(coltype2str(rhs_col_meta.type), coltype2str(set_clause.rhs.type));
            }

            float base = rhs_col_meta.type == TYPE_INT
                             ? static_cast<float>(*reinterpret_cast<const int*>(old_rec.data + rhs_col_meta.offset))
                             : *reinterpret_cast<const float*>(old_rec.data + rhs_col_meta.offset);
            float delta =
                set_clause.rhs.type == TYPE_INT ? static_cast<float>(set_clause.rhs.int_val) : set_clause.rhs.float_val;
            float result = base;
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

            switch (col_meta.type) {
            case TYPE_INT:
                *reinterpret_cast<int*>(data) = static_cast<int>(result);
                break;
            case TYPE_FLOAT:
                *reinterpret_cast<float*>(data) = result;
                break;
            case TYPE_STRING:
            case TYPE_DATETIME:
                throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(rhs_col_meta.type));
            }
            continue;
        }
        if (!can_cast(col_meta.type, set_clause.rhs.type)) {
            throw IncompatibleTypeError(coltype2str(col_meta.type), coltype2str(set_clause.rhs.type));
        }
        switch (col_meta.type) {
        case TYPE_INT: {
            if (set_clause.rhs.type == TYPE_INT) {
                *(int*)data = set_clause.rhs.int_val;
            } else {
                *(int*)data = (int)set_clause.rhs.float_val;
            }
            break;
        }
        case TYPE_FLOAT: {
            if (set_clause.rhs.type == TYPE_FLOAT) {
                *(float*)data = set_clause.rhs.float_val;
            } else {
                *(float*)data = (float)set_clause.rhs.int_val;
            }
            break;
        }
        case TYPE_STRING:
        case TYPE_DATETIME: {
            int len = col_meta.len;
            std::memset(data, 0, len);
            std::memcpy(data, set_clause.rhs.str_val.c_str(), std::min(len, (int)set_clause.rhs.str_val.size()));
            break;
        }
        }
    }
}

void TableWriteService::bulk_insert(const std::string& tab_name, const std::vector<std::vector<char>>& rows,
                                    StatementContext* ctx) {
    // 无事务批量插入：跳过锁/WAL/Undo/MVCC，直接物理写 heap + index。
    auto& tab = schema_mgr_->catalog().get_table(tab_name);
    auto* fh = schema_mgr_->get_table_handle(tab_name);

    // 准备索引 PinnedInserter（批量插入走 pinned-leaf 路径以保持吞吐）。
    struct LoadIndexTarget {
        const IndexMeta* meta;
        std::vector<char> key;
        std::unique_ptr<IxIndexHandle::PinnedInserter> inserter;
    };
    std::vector<LoadIndexTarget> idx_inserters;
    idx_inserters.reserve(tab.indexes.size());
    for (const auto& index : tab.indexes) {
        auto ih = schema_mgr_->get_index_handle(tab_name, index.cols);
        idx_inserters.push_back(
            {&index, std::vector<char>(index.col_tot_len), std::make_unique<IxIndexHandle::PinnedInserter>(ih)});
    }

    RmFileHandle::PinnedInserter rm_inserter(fh);
    for (const auto& row : rows) {
        Rid rid = rm_inserter.insert(row.data());
        for (auto& target : idx_inserters) {
            int off = 0;
            for (int i = 0; i < target.meta->col_num; ++i) {
                std::memcpy(target.key.data() + off, row.data() + target.meta->cols[i].offset,
                            target.meta->cols[i].len);
                off += target.meta->cols[i].len;
            }
            target.inserter->insert(target.key.data(), rid, nullptr, true);
        }
    }

    // PinnedInserter 析构时 unpin；显式 clear 以先释放索引 inserter 再释放 rm inserter。
    idx_inserters.clear();
    { RmFileHandle::PinnedInserter tmp(std::move(rm_inserter)); }
    (void)ctx;
}

} // namespace rmdb::access
