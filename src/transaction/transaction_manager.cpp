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

#include "transaction_manager.h"
#include "common/fault_injection.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <vector>

std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> TransactionManager::txn_map = {};

TransactionManager::~TransactionManager() {
    {
        std::lock_guard<std::mutex> lock(latch_);
        gc_stop_ = true;
    }
    gc_cv_.notify_all();
    if (gc_thread_.joinable()) {
        gc_thread_.join();
    }
}

namespace {

constexpr uint64_t SSI_FULL_PRUNE_COMMIT_INTERVAL = 4096;
constexpr size_t SSI_RECENT_WRITE_PRUNE_THRESHOLD = 8192;
constexpr size_t SSI_EDGE_PRUNE_THRESHOLD = 8192;

// 垃圾回收节流：避免每次 commit 都全表扫描 txn_map
constexpr uint64_t GC_COMMIT_INTERVAL = 256;  // 每 N 次提交尝试一次 GC
constexpr size_t GC_TXN_MAP_THRESHOLD = 1024; // 或 txn_map 超过该阈值立即 GC
constexpr size_t GC_BATCH_SIZE = 128;
constexpr size_t GC_SCAN_LIMIT = 512;

[[noreturn]] void FailStopAfterCommitMayBePersistent() {
    std::fprintf(stderr, "FATAL: COMMIT WAL/publication failed; stopping for recovery\n");
    std::fflush(stderr);
    std::_Exit(134);
}

void ClearWriteSet(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    txn->get_write_set().clear();
}

void ReleaseLocks(Transaction* txn, LockManager* lock_manager) {
    if (txn == nullptr || lock_manager == nullptr) {
        return;
    }
    auto lock_set = txn->get_lock_set();
    std::vector<LockDataId> locks(lock_set->begin(), lock_set->end());
    for (const auto& lock_id : locks) {
        lock_manager->unlock(txn, lock_id);
    }
    lock_set->clear();

    auto unique_key_locks = *txn->get_unique_key_lock_set();
    for (const auto& lock_id : unique_key_locks) {
        lock_manager->unlock_unique_key(txn, lock_id);
    }
    txn->get_unique_key_lock_set()->clear();
}

std::vector<char> MakeIndexKey(const IndexMeta& index, const char* rec_data) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (int i = 0; i < index.col_num; ++i) {
        memcpy(key.data() + offset, rec_data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
    return key;
}

void DeleteIndexEntries(SmManager* sm_manager, const TabMeta& tab, const std::string& tab_name, const RmRecord& rec,
                        const Rid& rid, Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->delete_entry(key.data(), rid, txn);
    }
}

void InsertIndexEntries(SmManager* sm_manager, const TabMeta& tab, const std::string& tab_name, const RmRecord& rec,
                        const Rid& rid, Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->insert_entry(key.data(), rid, txn, true);
    }
}

bool CompareCondition(const Condition& cond, const RmRecord& rec, const std::vector<ColMeta>& cols) {
    auto get_col_meta = [&](const TabCol& target) -> const ColMeta& {
        auto it = std::find_if(cols.begin(), cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (it == cols.end()) {
            throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
        }
        return *it;
    };

    const ColMeta& lhs_col_meta = get_col_meta(cond.lhs_col);
    char* lhs_data = rec.data + lhs_col_meta.offset;
    ColType lhs_type = lhs_col_meta.type;
    ColType rhs_type;
    char* rhs_data = nullptr;
    const ColMeta* rhs_col_meta = nullptr;
    if (cond.is_rhs_val) {
        rhs_type = cond.rhs_val.type;
    } else {
        rhs_col_meta = &get_col_meta(cond.rhs_col);
        rhs_data = rec.data + rhs_col_meta->offset;
        rhs_type = rhs_col_meta->type;
    }

    // SSI 的谓词匹配必须与执行器 compare() 用同一份三值逻辑，否则会漏判/误判冲突
    switch (eval_condition_nulls(cond, rec.data, lhs_col_meta.null_byte, lhs_col_meta.null_mask,
                                 rhs_col_meta == nullptr ? -1 : rhs_col_meta->null_byte,
                                 rhs_col_meta == nullptr ? 0 : rhs_col_meta->null_mask)) {
    case NullEval::DECIDED_TRUE:
        return true;
    case NullEval::DECIDED_FALSE:
        return false;
    case NullEval::COMPARE:
        break;
    }

    bool can_cast = lhs_type == rhs_type || (lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
                    (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT) ||
                    ((lhs_type == TYPE_STRING || lhs_type == TYPE_DATETIME) &&
                     (rhs_type == TYPE_STRING || rhs_type == TYPE_DATETIME));
    if (!can_cast) {
        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
    }

    switch (lhs_type) {
    case TYPE_INT:
    case TYPE_FLOAT: {
        double lhs_val = lhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(lhs_data))
                                              : static_cast<double>(read_float(lhs_data));
        double rhs_val;
        if (cond.is_rhs_val) {
            rhs_val = rhs_type == TYPE_INT ? static_cast<double>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
        } else {
            rhs_val = rhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(rhs_data))
                                           : static_cast<double>(read_float(rhs_data));
        }
        switch (cond.op) {
        case OP_EQ:
            return lhs_val == rhs_val;
        case OP_NE:
            return lhs_val != rhs_val;
        case OP_LT:
            return lhs_val < rhs_val;
        case OP_GT:
            return lhs_val > rhs_val;
        case OP_LE:
            return lhs_val <= rhs_val;
        case OP_GE:
            return lhs_val >= rhs_val;
        }
        break;
    }
    case TYPE_STRING:
    case TYPE_DATETIME: {
        std::string lhs_val(lhs_data, strnlen(lhs_data, lhs_col_meta.len));
        std::string rhs_val =
            cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col_meta->len));
        switch (cond.op) {
        case OP_EQ:
            return lhs_val == rhs_val;
        case OP_NE:
            return lhs_val != rhs_val;
        case OP_LT:
            return lhs_val < rhs_val;
        case OP_GT:
            return lhs_val > rhs_val;
        case OP_LE:
            return lhs_val <= rhs_val;
        case OP_GE:
            return lhs_val >= rhs_val;
        }
    }
    }
    return false;
}

bool MatchesConditions(const std::vector<Condition>& conds, const RmRecord& rec, const std::vector<ColMeta>& cols) {
    for (const auto& cond : conds) {
        if (!CompareCondition(cond, rec, cols)) {
            return false;
        }
    }
    return true;
}

bool SsiTxnCanConflictWithWriter(Transaction* reader_txn, Transaction* writer_txn) {
    if (reader_txn == nullptr || writer_txn == nullptr) {
        return false;
    }
    if (reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    if (reader_txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (reader_txn->get_state() == TransactionState::GROWING) {
        return true;
    }
    if (reader_txn->get_state() != TransactionState::COMMITTED) {
        return false;
    }
    timestamp_t reader_commit_ts = reader_txn->get_commit_ts();
    return reader_commit_ts != INVALID_TS && reader_commit_ts > writer_txn->get_read_ts();
}

void WriteBeginLog(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || log_manager == nullptr) {
        return;
    }
    BeginLogRecord record(txn->get_transaction_id());
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(lsn);
}

void WriteCommitLog(Transaction* txn, LogManager* log_manager, timestamp_t commit_ts) {
    if (txn == nullptr || log_manager == nullptr) {
        return;
    }
    // commit_ts 进 WAL：它是恢复期重建时间戳计数器的第二个来源。两次 checkpoint
    // 之间被驱逐的数据页可能带着比 db.restart 里的快照更高的 commit_ts_，而这条
    // 记录在那次页写之前就已经 durable（下面的 flush_log_to_disk_up_to 保证），
    // 所以“db.restart 快照”与“保留 WAL 里 COMMIT 的最大 commit_ts”取 max 覆盖
    // 一切已持久化的 commit_ts_。完整论证见
    // RecoveryManager::get_recovered_next_timestamp()。
    CommitLogRecord record(txn->get_transaction_id(), commit_ts);
    record.prev_lsn_ = txn->get_prev_lsn();
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    FaultInjector::Point("after_commit_log_append");
    txn->set_prev_lsn(lsn);
    // Returning from COMMIT means the commit record survived an OS crash,
    // not merely that it reached the kernel page cache.
    log_manager->flush_log_to_disk_up_to(lsn);
    FaultInjector::Point("after_commit_wal_write");
}

lsn_t WriteAbortLog(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || log_manager == nullptr) {
        return INVALID_LSN;
    }
    AbortLogRecord record(txn->get_transaction_id());
    record.prev_lsn_ = txn->get_prev_lsn();
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(lsn);
    log_manager->flush_log_to_disk();
    return lsn;
}

std::optional<UndoLog> GetCurrentUndoLog(TransactionManager* txn_mgr, RmFileHandle* fh, const Rid& rid) {
    TupleMeta meta = fh->get_tuple_meta(rid);
    if (!meta.version_chain_head_.IsValid()) {
        return std::nullopt;
    }
    try {
        return txn_mgr->GetUndoLog(meta.version_chain_head_);
    } catch (const InternalError&) {
        return std::nullopt;
    }
}

TupleMeta FallbackCommittedMeta() {
    TupleMeta meta;
    meta.commit_ts_ = 0;
    meta.writer_txn_id_ = INVALID_TXN_ID;
    meta.is_committed_ = true;
    meta.is_deleted_ = false;
    meta.version_chain_head_ = UndoLink{};
    return meta;
}

void UndoWriteRecord(TransactionManager* txn_mgr, SmManager* sm_manager, WriteRecord* write_record, Transaction* txn,
                     lsn_t page_lsn) {
    const std::string tab_name = write_record->GetTableName();
    auto& tab = sm_manager->db_.get_table(tab_name);
    auto* fh = sm_manager->fhs_.at(tab_name).get();
    Rid rid = write_record->GetRid();

    switch (write_record->GetWriteType()) {
    case WType::INSERT_TUPLE: {
        // An inserted tuple has no index work to undo when the table has no
        // indexes. Avoid fetching and copying the record just to discover
        // that DeleteIndexEntries has nothing to do; this is the hot RC
        // rollback path for heap-only tables.
        if (tab.indexes.empty()) {
            fh->delete_record(rid, nullptr, page_lsn);
            break;
        }
        if (fh->is_record(rid)) {
            auto rec = fh->get_record(rid, nullptr);
            DeleteIndexEntries(sm_manager, tab, tab_name, *rec, rid, txn);
            fh->delete_record(rid, nullptr, page_lsn);
        }
        break;
    }
    case WType::DELETE_TUPLE: {
        RmRecord old_rec = write_record->GetRecord();
        auto undo = GetCurrentUndoLog(txn_mgr, fh, rid);
        if (fh->is_record(rid)) {
            fh->apply_tuple_update(rid, old_rec.data, undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta(),
                                   page_lsn);
        } else {
            fh->insert_record(rid, old_rec.data, page_lsn);
            fh->set_tuple_meta(rid, undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta(), page_lsn);
        }
        InsertIndexEntries(sm_manager, tab, tab_name, old_rec, rid, txn);
        break;
    }
    case WType::UPDATE_TUPLE: {
        RmRecord old_rec = write_record->GetRecord();
        std::unique_ptr<RmRecord> current_rec;
        if (fh->is_record(rid)) {
            current_rec = fh->get_record(rid, nullptr);
            for (const auto& index : tab.indexes) {
                auto current_key = MakeIndexKey(index, current_rec->data);
                auto old_key = MakeIndexKey(index, old_rec.data);
                if (current_key == old_key) {
                    continue;
                }
                auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                ih->delete_entry(current_key.data(), rid, txn);
            }
        }
        auto undo = GetCurrentUndoLog(txn_mgr, fh, rid);
        if (fh->is_record(rid)) {
            fh->apply_tuple_update(rid, old_rec.data, undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta(),
                                   page_lsn);
        }
        if (current_rec != nullptr) {
            for (const auto& index : tab.indexes) {
                auto current_key = MakeIndexKey(index, current_rec->data);
                auto old_key = MakeIndexKey(index, old_rec.data);
                if (current_key == old_key) {
                    continue;
                }
                auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                ih->insert_entry(old_key.data(), rid, txn, true);
            }
        }
        break;
    }
    }
}

} // namespace

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager, IsolationLevel isolation_level) {
    std::unique_ptr<Transaction> created;
    if (txn == nullptr) {
        txn_id_t txn_id = next_txn_id_.fetch_add(1);
        created = std::make_unique<Transaction>(txn_id, isolation_level);
        txn = created.get();
    }

    {
        std::unique_lock<std::mutex> checkpoint_lock(checkpoint_latch_);
        checkpoint_cv_.wait(checkpoint_lock, [&] { return !checkpoint_blocking_new_txns_; });
        active_txn_ids_.insert(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
    }

    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_.fetch_add(1));
    txn->set_read_ts(last_commit_ts_.load());
    // start_ts orders transaction lifecycle events, but a committer may already
    // have reserved a smaller commit_ts without publishing it. read_ts is the
    // published snapshot frontier and therefore drives visibility and SSI.
    // 用读时间戳维护水位线：RC 下每条语句的 read_ts 可能大于 start_ts，
    // 水位线必须反映当前真实 read_ts 才能安全驱动垃圾回收。
    txn->set_watermark_slot(running_txns_.AddTxnSlot(txn->get_read_ts()));
    WriteBeginLog(txn, log_manager);

    std::unique_lock<std::mutex> lock(latch_);
    if (created) {
        txn_map[txn->get_transaction_id()] = std::move(created);
    }
    if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE) {
        active_serializable_txns_.insert(txn->get_transaction_id());
    }
    return txn;
}

void TransactionManager::BeginStatement(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED) {
        // Atomically refresh the statement snapshot in the watermark. A
        // remove/add pair would briefly make an active RC transaction vanish
        // and allow concurrent GC to reclaim its visible version chain.
        timestamp_t old_read_ts = txn->get_read_ts();
        timestamp_t new_read_ts = last_commit_ts_.load();
        if (new_read_ts != old_read_ts) {
            running_txns_.UpdateTxnReadTsSlot(txn->get_watermark_slot(), new_read_ts);
            txn->set_read_ts(new_read_ts);
        }
    }
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
        RetireTransactionIfSafe(txn);
        return;
    }

    sm_manager_->prepare_commit_publication(*txn);
    TransactionState expected_state = TransactionState::GROWING;
    if (!txn->compare_exchange_state(expected_state, TransactionState::COMMITTING)) {
        if (expected_state == TransactionState::COMMITTED || expected_state == TransactionState::ABORTED) {
            RetireTransactionIfSafe(txn);
            return;
        }
        throw InternalError("transaction is not ready to commit");
    }

    FaultInjector::Point("before_commit_wal");
    try {
        timestamp_t commit_csn;
        timestamp_t commit_ts;
        {
            // CSN and commit timestamp allocation must be ordered together.
            // Otherwise an out-of-order publisher could move last_commit_ts_
            // backwards when the frontier is advanced.
            std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
            commit_csn = ++next_commit_csn_;
            commit_ts = next_timestamp_.fetch_add(1);
            txn->commit_ts_ = commit_ts;
        }

        // 时间戳分配移到 WAL 之前，只为让 COMMIT 记录能带上 commit_ts；它不发布
        // 任何东西，因此不改变可见性顺序。分配后若 WriteCommitLog 抛出，catch 分支
        // 直接 fail-stop 整个进程，不会留下一个占用了 CSN 却永不完成、卡住发布
        // 前沿的事务。
        //
        // Once COMMITTING is visible, neither a WAL failure nor a publication
        // failure may fall back to ordinary abort: the COMMIT record may
        // already be present in the WAL prefix recovered after restart.
        WriteCommitLog(txn, log_manager, commit_ts);
        FaultInjector::Point("after_commit_wal_sync");

        // Publish every modified slot outside the frontier mutex. A new RC
        // statement still cannot observe this commit until its CSN is part of
        // the contiguous completed frontier below.
        FaultInjector::Point("before_tuple_publication");
        sm_manager_->mark_slots_committed(*txn, commit_ts);
        FaultInjector::Point("after_tuple_publication");
        txn->set_state(TransactionState::COMMITTED);
        FaultInjector::Point("before_published_csn_store");
        {
            std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
            completed_commits_.emplace(commit_csn, commit_ts);
            while (true) {
                auto next = completed_commits_.find(published_commit_csn_ + 1);
                if (next == completed_commits_.end()) {
                    break;
                }
                last_commit_ts_.store(next->second, std::memory_order_release);
                ++published_commit_csn_;
                completed_commits_.erase(next);
            }
        }
        commit_frontier_cv_.notify_all();

        // Do not return from COMMIT until this transaction is covered by the
        // publication frontier. This provides read-your-commit even when a
        // later CSN finished publishing first.
        std::unique_lock<std::mutex> frontier_lock(commit_frontier_latch_);
        commit_frontier_cv_.wait(frontier_lock, [&] { return published_commit_csn_ >= commit_csn; });
    } catch (...) {
        // Once WriteCommitLog returned, recovery must treat this transaction
        // as committed. Ordinary abort would make durable WAL and in-memory
        // state disagree and may leave a partially published transaction.
        FailStopAfterCommitMayBePersistent();
    }

    // Keep the committing transaction in the watermark until publication is
    // complete; otherwise GC could reclaim its undo state in the publication
    // window.
    running_txns_.UpdateCommitTs(txn->get_commit_ts());
    running_txns_.RemoveTxnSlot(txn->get_watermark_slot());
    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE) {
        CleanupTxnSsiState(txn->get_transaction_id());
        bool run_full_prune = false;
        {
            std::unique_lock<std::mutex> lock(latch_);
            ++commits_since_full_ssi_prune_;
            run_full_prune = ShouldRunFullSsiPruneUnlocked();
        }
        if (run_full_prune) {
            PruneSsiState();
        }
    }
    RetireTransactionIfSafe(txn);
    MaybeRunGarbageCollection();
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::ABORTED || txn->get_state() == TransactionState::COMMITTED) {
        RetireTransactionIfSafe(txn);
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTING) {
        // The COMMIT WAL is already durable. Rolling this transaction back
        // would create a state that recovery cannot distinguish from a lost
        // committed transaction.
        throw InternalError("cannot abort a transaction during commit publication");
    }

    auto& write_set = txn->get_write_set();
    if (lock_manager_ != nullptr) {
        lock_manager_->cancel_transaction(txn);
    }
    // The abort record must precede the physical undo. This gives every page
    // modified by rollback a WAL record that can be used as its page LSN.
    lsn_t abort_lsn = WriteAbortLog(txn, log_manager);
    for (auto it = write_set.rbegin(); it != write_set.rend(); ++it) {
        UndoWriteRecord(this, sm_manager_, it->get(), txn, abort_lsn);
    }
    running_txns_.RemoveTxnSlot(txn->get_watermark_slot());
    ClearWriteSet(txn);
    ReleaseLocks(txn, lock_manager_);
    txn->set_state(TransactionState::ABORTED);
    if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE) {
        CleanupTxnSsiState(txn->get_transaction_id());
        bool run_full_prune = false;
        {
            std::unique_lock<std::mutex> lock(latch_);
            run_full_prune = ShouldRunFullSsiPruneUnlocked();
        }
        if (run_full_prune) {
            PruneSsiState();
        }
    }
    RetireTransactionIfSafe(txn);
    MaybeRunGarbageCollection();
}

void TransactionManager::block_new_transactions_for_checkpoint() {
    std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
    checkpoint_blocking_new_txns_ = true;
}

void TransactionManager::unblock_new_transactions_after_checkpoint() {
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        checkpoint_blocking_new_txns_ = false;
    }
    checkpoint_cv_.notify_all();
}

bool TransactionManager::wait_active_transactions_drained_for_checkpoint(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> checkpoint_lock(checkpoint_latch_);
    // A client that ran BEGIN and then went silent keeps active_txn_count_
    // above zero indefinitely. Waiting without a bound would keep every new
    // transaction in the process blocked behind the checkpoint.
    return checkpoint_cv_.wait_for(checkpoint_lock, timeout, [&] { return active_txn_count_ == 0; });
}

std::unordered_map<txn_id_t, lsn_t> TransactionManager::get_active_txn_lsn_snapshot() {
    std::vector<txn_id_t> active_ids;
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_ids.assign(active_txn_ids_.begin(), active_txn_ids_.end());
    }

    std::unordered_map<txn_id_t, lsn_t> snapshot;
    std::unique_lock<std::mutex> lock(latch_);
    for (txn_id_t txn_id : active_ids) {
        auto it = txn_map.find(txn_id);
        if (it != txn_map.end() && it->second != nullptr) {
            snapshot.emplace(txn_id, it->second->get_prev_lsn());
        }
    }
    return snapshot;
}

void TransactionManager::seed_counters_after_recovery(timestamp_t next_timestamp, txn_id_t next_txn_id) {
    // 单调抬高，绝不回退：本进程里可能已经跑过事务（测试里就会），把计数器往回拧
    // 会让新事务重用已经写进数据页的 commit_ts_。
    if (next_timestamp > next_timestamp_.load()) {
        next_timestamp_.store(next_timestamp);
    }
    if (next_txn_id > next_txn_id_.load()) {
        next_txn_id_.store(next_txn_id);
    }

    // read_ts 必须 >= 任何已持久化的 commit_ts_，而后者都 <= next_timestamp - 1
    // （commit_ts 由 next_timestamp_.fetch_add(1) 分发，故已分发值都严格小于计数器）。
    // 取到恰好 next_timestamp - 1 而不是 next_timestamp：否则本进程第一个提交拿到的
    // commit_ts 会等于早先开始的事务的 read_ts，让读者看见自己快照之后的提交。
    const timestamp_t seed_read_ts = next_timestamp_.load() - 1;
    if (seed_read_ts > last_commit_ts_.load(std::memory_order_acquire)) {
        last_commit_ts_.store(seed_read_ts, std::memory_order_release);
        // 水位线也一起抬高，否则在本进程第一次提交之前 GC 会以 0 为水位线，
        // 保守到什么都回收不了。
        running_txns_.UpdateCommitTs(seed_read_ts);
    }
}

UndoLog TransactionManager::GetUndoLog(UndoLink link) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map.find(link.undo_txn_id_);
    if (it == txn_map.end()) {
        throw InternalError("GetUndoLog: transaction not found");
    }
    return it->second->GetUndoLog(link.undo_slot_offset_);
}

std::optional<UndoLog> TransactionManager::GetUndoLogOptional(UndoLink link) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map.find(link.undo_txn_id_);
    if (it == txn_map.end()) {
        return std::nullopt;
    }
    return it->second->GetUndoLog(link.undo_slot_offset_);
}

std::optional<std::pair<TupleMeta, std::vector<char>>>
TransactionManager::FindVisibleVersion(const TupleMeta& start_meta, timestamp_t read_ts, txn_id_t self_txn_id) {
    const TupleMeta* cur = &start_meta;
    // Limit traversal depth to prevent infinite loops
    constexpr int MAX_DEPTH = 100;
    for (int depth = 0; depth < MAX_DEPTH; ++depth) {
        // Own uncommitted write is always visible
        if (!cur->is_committed_ && cur->writer_txn_id_ == self_txn_id) {
            if (cur->is_deleted_)
                return std::nullopt;
            return std::make_pair(*cur, std::vector<char>{});
        }
        // Other's uncommitted write — dirty, follow chain
        if (!cur->is_committed_) {
            if (cur->version_chain_head_.IsValid()) {
                UndoLog log = GetUndoLog(cur->version_chain_head_);
                cur = &log.old_meta_;
                continue;
            }
            return std::nullopt;
        }
        // Committed and not deleted — check read_ts
        if (!cur->is_deleted_ && cur->commit_ts_ <= read_ts) {
            return std::make_pair(*cur, std::vector<char>{});
        }
        // Not visible — follow chain
        if (cur->version_chain_head_.IsValid()) {
            UndoLog log = GetUndoLog(cur->version_chain_head_);
            cur = &log.old_meta_;
            continue;
        }
        return std::nullopt;
    }
    return std::nullopt;
}

// ---- SSI Helper ----

bool TransactionManager::TupleMatches(const std::string& tab_name, const std::vector<Condition>& conds,
                                      const RmRecord& rec) {
    if (conds.empty()) {
        return true;
    }
    const auto& tab = sm_manager_->db_.get_table(tab_name);
    auto get_col_meta = [&](const TabCol& target) -> const ColMeta& {
        auto iter = std::find_if(tab.cols.begin(), tab.cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (iter == tab.cols.end()) {
            throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
        }
        return *iter;
    };
    for (const auto& cond : conds) {
        const auto& lhs_col = get_col_meta(cond.lhs_col);
        const char* lhs_data = rec.data + lhs_col.offset;
        ColType rhs_type;
        const char* rhs_data = nullptr;
        ColMeta rhs_col{};
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
        } else {
            rhs_col = get_col_meta(cond.rhs_col);
            rhs_type = rhs_col.type;
            rhs_data = rec.data + rhs_col.offset;
        }
        if (!((lhs_col.type == rhs_type) || (lhs_col.type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
              (lhs_col.type == TYPE_FLOAT && rhs_type == TYPE_INT) ||
              ((lhs_col.type == TYPE_STRING || lhs_col.type == TYPE_DATETIME) &&
               (rhs_type == TYPE_STRING || rhs_type == TYPE_DATETIME)))) {
            throw IncompatibleTypeError(coltype2str(lhs_col.type), coltype2str(rhs_type));
        }
        // 同上：与执行器 compare() 共用三值逻辑
        switch (eval_condition_nulls(cond, rec.data, lhs_col.null_byte, lhs_col.null_mask,
                                     cond.is_rhs_val ? -1 : rhs_col.null_byte,
                                     cond.is_rhs_val ? 0 : rhs_col.null_mask)) {
        case NullEval::DECIDED_TRUE:
            continue;
        case NullEval::DECIDED_FALSE:
            return false;
        case NullEval::COMPARE:
            break;
        }
        int cmp = 0;
        if (lhs_col.type == TYPE_STRING || lhs_col.type == TYPE_DATETIME) {
            std::string lhs(lhs_data, strnlen(lhs_data, lhs_col.len));
            std::string rhs =
                cond.is_rhs_val ? cond.rhs_val.str_val : std::string(rhs_data, strnlen(rhs_data, rhs_col.len));
            cmp = lhs.compare(rhs);
        } else {
            double lhs = lhs_col.type == TYPE_INT ? static_cast<double>(read_unaligned<int>(lhs_data))
                                                  : static_cast<double>(read_float(lhs_data));
            double rhs;
            if (cond.is_rhs_val) {
                rhs = rhs_type == TYPE_INT ? static_cast<double>(cond.rhs_val.int_val) : cond.rhs_val.float_val;
            } else {
                rhs = rhs_type == TYPE_INT ? static_cast<double>(read_unaligned<int>(rhs_data))
                                           : static_cast<double>(read_float(rhs_data));
            }
            cmp = lhs == rhs ? 0 : (lhs < rhs ? -1 : 1);
        }
        bool ok = false;
        switch (cond.op) {
        case OP_EQ:
            ok = cmp == 0;
            break;
        case OP_NE:
            ok = cmp != 0;
            break;
        case OP_LT:
            ok = cmp < 0;
            break;
        case OP_GT:
            ok = cmp > 0;
            break;
        case OP_LE:
            ok = cmp <= 0;
            break;
        case OP_GE:
            ok = cmp >= 0;
            break;
        }
        if (!ok) {
            return false;
        }
    }
    return true;
}

TransactionManager::SsiRecordKey TransactionManager::MakeSsiRecordKey(const std::string& tab_name,
                                                                      const Rid& rid) const {
    return SsiRecordKey{tab_name, rid.page_no, rid.slot_no};
}

bool TransactionManager::HasActiveSsiReadersForWriteUnlocked(const std::string& tab_name,
                                                             const SsiRecordKey& key) const {
    auto record_it = active_record_readers_.find(key);
    if (record_it != active_record_readers_.end() && !record_it->second.empty()) {
        return true;
    }
    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    return predicate_it != active_predicate_readers_by_table_.end() && !predicate_it->second.empty();
}

bool TransactionManager::HasOtherActivePredicateReadersUnlocked(const std::string& tab_name, txn_id_t writer) const {
    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    if (predicate_it == active_predicate_readers_by_table_.end()) {
        return false;
    }
    for (txn_id_t reader : predicate_it->second) {
        if (reader != writer) {
            return true;
        }
    }
    return false;
}

bool TransactionManager::HasOtherActiveSerializableTxnUnlocked(txn_id_t writer) const {
    for (txn_id_t txn_id : active_serializable_txns_) {
        if (txn_id == writer) {
            continue;
        }
        auto txn_it = txn_map.find(txn_id);
        if (txn_it == txn_map.end() || txn_it->second == nullptr) {
            continue;
        }
        auto* txn = txn_it->second.get();
        if (txn->get_isolation_level() == IsolationLevel::SERIALIZABLE &&
            txn->get_state() != TransactionState::COMMITTED && txn->get_state() != TransactionState::ABORTED) {
            return true;
        }
    }
    return false;
}

size_t TransactionManager::RecentWriteCountUnlocked() const {
    size_t total = ssi_writes_.size();
    for (const auto& [_, writes] : recent_writes_by_table_) {
        total += writes.size();
    }
    return total;
}

size_t TransactionManager::RwEdgeCountUnlocked() const {
    size_t total = 0;
    for (const auto& [_, outs] : rw_edges_) {
        total += outs.size();
    }
    return total;
}

bool TransactionManager::HasActiveSerializableOverlapUnlocked(Transaction* txn) {
    if (txn == nullptr) {
        return false;
    }
    txn_id_t txn_id = txn->get_transaction_id();
    for (txn_id_t other_id : active_serializable_txns_) {
        if (other_id == txn_id) {
            continue;
        }
        auto other_it = txn_map.find(other_id);
        auto* other = other_it == txn_map.end() ? nullptr : other_it->second.get();
        if (other == nullptr) {
            continue;
        }
        if (other->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
            continue;
        }
        if (other->get_state() == TransactionState::COMMITTED || other->get_state() == TransactionState::ABORTED) {
            continue;
        }
        if (TransactionsOverlap(txn, other)) {
            return true;
        }
    }
    return false;
}

void TransactionManager::CleanupTxnReadIndexesUnlocked(txn_id_t txn_id) {
    auto key_it = txn_record_read_keys_.find(txn_id);
    if (key_it != txn_record_read_keys_.end()) {
        for (const auto& key : key_it->second) {
            auto readers_it = active_record_readers_.find(key);
            if (readers_it == active_record_readers_.end()) {
                continue;
            }
            readers_it->second.erase(txn_id);
            if (readers_it->second.empty()) {
                active_record_readers_.erase(readers_it);
            }
        }
        txn_record_read_keys_.erase(key_it);
    }

    auto table_it = txn_predicate_read_tables_.find(txn_id);
    if (table_it != txn_predicate_read_tables_.end()) {
        for (const auto& table : table_it->second) {
            auto readers_it = active_predicate_readers_by_table_.find(table);
            if (readers_it == active_predicate_readers_by_table_.end()) {
                continue;
            }
            readers_it->second.erase(txn_id);
            if (readers_it->second.empty()) {
                active_predicate_readers_by_table_.erase(readers_it);
            }
        }
        txn_predicate_read_tables_.erase(table_it);
    }

    ssi_record_reads_.erase(std::remove_if(ssi_record_reads_.begin(), ssi_record_reads_.end(),
                                           [&](const auto& read) { return read.txn_id_ == txn_id; }),
                            ssi_record_reads_.end());

    auto txn_it = txn_map.find(txn_id);
    if (txn_it != txn_map.end() && txn_it->second != nullptr) {
        txn_it->second->read_rids_.clear();
        txn_it->second->predicate_reads_.clear();
    }
}

void TransactionManager::CleanupTxnRwEdgesUnlocked(txn_id_t txn_id) {
    auto txn_it = txn_map.find(txn_id);
    if (txn_it != txn_map.end() && txn_it->second != nullptr) {
        auto* txn = txn_it->second.get();
        for (auto in_id : txn->in_rw_) {
            auto in_it = txn_map.find(in_id);
            if (in_it != txn_map.end() && in_it->second != nullptr) {
                in_it->second->out_rw_.erase(txn_id);
            }
        }
        for (auto out_id : txn->out_rw_) {
            auto out_it = txn_map.find(out_id);
            if (out_it != txn_map.end() && out_it->second != nullptr) {
                out_it->second->in_rw_.erase(txn_id);
            }
        }
        txn->in_rw_.clear();
        txn->out_rw_.clear();
    }

    rw_edges_.erase(txn_id);
    for (auto it = rw_edges_.begin(); it != rw_edges_.end();) {
        it->second.erase(txn_id);
        if (it->second.empty()) {
            it = rw_edges_.erase(it);
        } else {
            ++it;
        }
    }
}

void TransactionManager::CleanupTxnRecentWritesUnlocked(txn_id_t txn_id) {
    ssi_writes_.erase(std::remove_if(ssi_writes_.begin(), ssi_writes_.end(),
                                     [&](const auto& write) { return write.txn_id_ == txn_id; }),
                      ssi_writes_.end());

    for (auto it = recent_writes_by_table_.begin(); it != recent_writes_by_table_.end();) {
        auto& writes = it->second;
        writes.erase(
            std::remove_if(writes.begin(), writes.end(), [&](const auto& write) { return write.txn_id_ == txn_id; }),
            writes.end());
        if (writes.empty()) {
            it = recent_writes_by_table_.erase(it);
        } else {
            ++it;
        }
    }
}

void TransactionManager::CleanupTxnSsiState(txn_id_t txn_id) {
    std::unique_lock<std::mutex> lock(latch_);
    auto txn_it = txn_map.find(txn_id);
    if (txn_it == txn_map.end() || txn_it->second == nullptr) {
        active_serializable_txns_.erase(txn_id);
        CleanupTxnReadIndexesUnlocked(txn_id);
        CleanupTxnRwEdgesUnlocked(txn_id);
        CleanupTxnRecentWritesUnlocked(txn_id);
        return;
    }

    auto* txn = txn_it->second.get();
    const bool aborted = txn->get_state() == TransactionState::ABORTED;
    const bool committed = txn->get_state() == TransactionState::COMMITTED;
    if (!aborted && !committed) {
        return;
    }

    if (committed && TransactionHasRetainedSsiStateUnlocked(txn_id) && HasActiveSerializableOverlapUnlocked(txn)) {
        return;
    }

    active_serializable_txns_.erase(txn_id);
    CleanupTxnReadIndexesUnlocked(txn_id);
    CleanupTxnRwEdgesUnlocked(txn_id);
    CleanupTxnRecentWritesUnlocked(txn_id);
}

bool TransactionManager::ShouldRunFullSsiPruneUnlocked() const {
    const bool has_ssi_state = !recent_writes_by_table_.empty() || !ssi_writes_.empty() ||
                               !active_record_readers_.empty() || !active_predicate_readers_by_table_.empty() ||
                               !rw_edges_.empty() || !active_serializable_txns_.empty();
    if (!has_ssi_state) {
        return false;
    }
    if (commits_since_full_ssi_prune_ >= SSI_FULL_PRUNE_COMMIT_INTERVAL && has_ssi_state) {
        return true;
    }
    if (RecentWriteCountUnlocked() >= SSI_RECENT_WRITE_PRUNE_THRESHOLD) {
        return true;
    }
    if (RwEdgeCountUnlocked() >= SSI_EDGE_PRUNE_THRESHOLD) {
        return true;
    }
    return false;
}

bool TransactionManager::TransactionHasSsiStateUnlocked(txn_id_t txn_id) const {
    if (active_serializable_txns_.count(txn_id) > 0) {
        return true;
    }
    return TransactionHasRetainedSsiStateUnlocked(txn_id);
}

bool TransactionManager::TransactionHasRetainedSsiStateUnlocked(txn_id_t txn_id) const {
    if (txn_record_read_keys_.count(txn_id) > 0 || txn_predicate_read_tables_.count(txn_id) > 0) {
        return true;
    }
    for (const auto& [_, readers] : active_record_readers_) {
        if (readers.count(txn_id) > 0) {
            return true;
        }
    }
    for (const auto& [_, readers] : active_predicate_readers_by_table_) {
        if (readers.count(txn_id) > 0) {
            return true;
        }
    }
    for (const auto& write : ssi_writes_) {
        if (write.txn_id_ == txn_id) {
            return true;
        }
    }
    for (const auto& [_, writes] : recent_writes_by_table_) {
        for (const auto& write : writes) {
            if (write.txn_id_ == txn_id) {
                return true;
            }
        }
    }
    auto edge_it = rw_edges_.find(txn_id);
    if (edge_it != rw_edges_.end() && !edge_it->second.empty()) {
        return true;
    }
    for (const auto& [_, outs] : rw_edges_) {
        if (outs.count(txn_id) > 0) {
            return true;
        }
    }
    auto txn_it = txn_map.find(txn_id);
    if (txn_it != txn_map.end() && txn_it->second != nullptr) {
        return !txn_it->second->in_rw_.empty() || !txn_it->second->out_rw_.empty() ||
               !txn_it->second->read_rids_.empty() || !txn_it->second->predicate_reads_.empty();
    }
    return false;
}

bool TransactionManager::TransactionHasUndoNeededByVersionChain(Transaction* txn) const {
    return txn != nullptr && txn->GetUndoLogNum() > 0;
}

bool TransactionManager::CanRetireTransactionUnlocked(Transaction* txn) const {
    if (txn == nullptr) {
        return false;
    }
    TransactionState state = txn->get_state();
    if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
        return false;
    }
    txn_id_t txn_id = txn->get_transaction_id();
    if (active_txn_ids_.count(txn_id) > 0) {
        return false;
    }
    if (TransactionHasSsiStateUnlocked(txn_id)) {
        return false;
    }
    if (TransactionHasUndoNeededByVersionChain(txn)) {
        return false;
    }
    auto* non_const_this = const_cast<TransactionManager*>(this);
    return !non_const_this->HasActiveSerializableOverlapUnlocked(txn);
}

void TransactionManager::RetireTransactionIfSafe(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    bool no_active_transactions = false;
    {
        // Keep the transaction active-set transition and the txn_map lifetime
        // decision atomic with respect to GC's active-set snapshot.
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_txn_ids_.erase(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
        no_active_transactions = active_txn_count_ == 0;

        std::unique_lock<std::mutex> lock(latch_);
        if (CanRetireTransactionUnlocked(txn)) {
            auto it = txn_map.find(txn->get_transaction_id());
            if (it != txn_map.end() && it->second.get() == txn) {
                txn_map.erase(it);
            }
        }
    }
    checkpoint_cv_.notify_all();
    if (no_active_transactions) {
        gc_cv_.notify_one();
    }
}

timestamp_t TransactionManager::GetWatermark() {
    return running_txns_.GetWatermark();
}

void TransactionManager::MaybeRunGarbageCollection() {
    bool notify_gc = false;
    {
        std::lock_guard<std::mutex> lock(latch_);
        ++commits_since_gc_;
        gc_backlog_.store(txn_map.size(), std::memory_order_release);
        if (!gc_requested_ && (commits_since_gc_ >= GC_COMMIT_INTERVAL || txn_map.size() >= GC_TXN_MAP_THRESHOLD)) {
            gc_requested_ = true;
            commits_since_gc_ = 0;
            notify_gc = true;
        }
    }
    if (notify_gc) {
        gc_cv_.notify_one();
    }
}

void TransactionManager::GarbageCollectionLoop() {
    std::unique_lock<std::mutex> lock(latch_);
    while (!gc_stop_.load(std::memory_order_acquire)) {
        gc_cv_.wait(lock, [this] { return gc_stop_.load(std::memory_order_acquire) || gc_requested_; });
        if (gc_stop_.load(std::memory_order_acquire)) {
            break;
        }
        // GarbageCollectionBatch uses the oldest active read timestamp as
        // its safety boundary. It must not wait for a globally quiescent
        // transaction set, otherwise a sustained workload can starve GC.
        size_t batch_start_size = 0;
        batch_start_size = txn_map.size();
        gc_backlog_.store(batch_start_size, std::memory_order_release);
        gc_requested_ = false;
        gc_running_ = true;
        lock.unlock();

        bool more = false;
        try {
            more = GarbageCollectionBatch();
        } catch (...) {
            // GC is opportunistic. A transient metadata/storage error must
            // not terminate the server or strand the worker in gc_running_.
        }

        lock.lock();
        gc_running_ = false;
        const size_t batch_end_size = txn_map.size();
        gc_backlog_.store(batch_end_size, std::memory_order_release);
        gc_last_batch_size_.store(batch_start_size > batch_end_size ? batch_start_size - batch_end_size : 0,
                                  std::memory_order_release);
        if (more && !gc_stop_.load(std::memory_order_acquire)) {
            gc_requested_ = true;
        }
    }
}

void TransactionManager::GarbageCollection() {
    // Explicit callers (checkpoint/tests) ask for convergence. Keep the
    // background worker bounded by calling GarbageCollectionBatch() directly
    // there, while this synchronous API drains successive bounded batches.
    while (GarbageCollectionBatch()) {
    }
}

bool TransactionManager::GarbageCollectionBatch() {
    // 安全条件：水位线是所有活跃事务读时间戳的最小值。只有 commit_ts（已提交）
    // 或 start_ts（已中止）严格小于水位线的事务，其 undo log 才不会被任何活跃
    // 事务的版本链遍历访问到，因而可安全从 txn_map 回收。
    // 与 GetUndoLog 互斥：二者都持 latch_。
    timestamp_t watermark = running_txns_.GetWatermark();
    // active_txn_ids_ is maintained under checkpoint_latch_. Take a conservative
    // snapshot first, then release that lock before scanning txn_map_ so GC does
    // not block transaction begin/commit for the duration of the scan.
    std::unordered_set<txn_id_t> active_txn_snapshot;
    {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
        active_txn_snapshot = active_txn_ids_;
    }

    std::vector<txn_id_t> to_erase;
    {
        std::unique_lock<std::mutex> lock(latch_);
        size_t scanned = 0;
        for (auto it = txn_map.begin(); it != txn_map.end() && scanned < GC_SCAN_LIMIT; ++it, ++scanned) {
            Transaction* txn = it->second.get();
            if (txn == nullptr) {
                to_erase.push_back(it->first);
                continue;
            }
            TransactionState state = txn->get_state();
            if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
                continue;
            }
            if (active_txn_snapshot.find(it->first) != active_txn_snapshot.end()) {
                continue;
            }
            if (TransactionHasRetainedSsiStateUnlocked(it->first)) {
                continue;
            }
            if (state == TransactionState::COMMITTED) {
                if (txn->get_commit_ts() == INVALID_TS || txn->get_commit_ts() >= watermark) {
                    continue;
                }
            } else {
                // 已中止事务：commit_ts 无效，保守用 start_ts 判断。
                // 任何 read_ts >= start_ts 的活跃事务都已无法看到该事务的版本。
                if (txn->get_start_ts() >= watermark) {
                    continue;
                }
            }
            // 仅在无 SSI 活跃重叠时回收，避免误删仍被 SSI 依赖追踪的事务
            auto* non_const_this = const_cast<TransactionManager*>(this);
            if (non_const_this->HasActiveSerializableOverlapUnlocked(txn)) {
                continue;
            }
            to_erase.push_back(it->first);
            if (to_erase.size() >= GC_BATCH_SIZE) {
                break;
            }
        }
        for (txn_id_t txn_id : to_erase) {
            txn_map.erase(txn_id);
        }
    }
    // 回收 SmManager 侧随写操作单调增长的历史索引键/删除候选（需访问 tuple meta，
    // 不在 latch_ 下进行，避免与缓冲池操作死锁）
    // History/index candidates have the same watermark safety rule as txn_map
    // entries and must be pruned even when this batch found no transaction
    // object to erase.
    sm_manager_->prune_version_history(watermark);
    return to_erase.size() >= GC_BATCH_SIZE;
}

bool TransactionManager::TransactionsOverlap(Transaction* lhs, Transaction* rhs) {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    timestamp_t lhs_end = lhs->get_commit_ts();
    timestamp_t rhs_end = rhs->get_commit_ts();
    if (lhs->get_state() != TransactionState::COMMITTED) {
        lhs_end = std::numeric_limits<timestamp_t>::max();
    }
    if (rhs->get_state() != TransactionState::COMMITTED) {
        rhs_end = std::numeric_limits<timestamp_t>::max();
    }
    return lhs->get_read_ts() < rhs_end && rhs->get_read_ts() < lhs_end;
}

bool TransactionManager::CommittedBefore(txn_id_t lhs, txn_id_t rhs) {
    std::unique_lock<std::mutex> lock(latch_);
    return CommittedBeforeUnlocked(lhs, rhs);
}

bool TransactionManager::CommittedBeforeUnlocked(txn_id_t lhs, txn_id_t rhs) {
    auto lhs_it = txn_map.find(lhs);
    auto rhs_it = txn_map.find(rhs);
    if (lhs_it == txn_map.end() || rhs_it == txn_map.end()) {
        return false;
    }
    auto* lhs_txn = lhs_it->second.get();
    auto* rhs_txn = rhs_it->second.get();
    if (lhs_txn->get_state() != TransactionState::COMMITTED) {
        return false;
    }
    if (rhs_txn->get_state() != TransactionState::COMMITTED) {
        return true;
    }
    return lhs_txn->get_commit_ts() < rhs_txn->get_commit_ts();
}

bool TransactionManager::HasDangerousStructure(txn_id_t current_txn) {
    // Danger detection uses the centralized rw_edges_ map. It needs to be
    // consistent with txn_map state (e.g., not skip aborted txns). Hold both
    // ssi_latch_ and latch_ if needed, but since all edge mutations also hold
    // latch_, we only hold latch_ here.
    std::unique_lock<std::mutex> lock(latch_);
    return HasDangerousStructureUnlocked(current_txn);
}

bool TransactionManager::HasDangerousStructureUnlocked(txn_id_t current_txn) {
    for (const auto& [tin, pivots] : rw_edges_) {
        auto tin_it = txn_map.find(tin);
        if (tin_it == txn_map.end() || tin_it->second->get_state() == TransactionState::ABORTED) {
            continue;
        }
        for (txn_id_t pivot : pivots) {
            auto pivot_it = txn_map.find(pivot);
            if (pivot_it == txn_map.end() || pivot_it->second->get_state() == TransactionState::ABORTED) {
                continue;
            }
            auto pivot_out_it = rw_edges_.find(pivot);
            if (pivot_out_it == rw_edges_.end()) {
                continue;
            }
            for (txn_id_t tout : pivot_out_it->second) {
                auto tout_it = txn_map.find(tout);
                if (tout_it == txn_map.end() || tout_it->second->get_state() == TransactionState::ABORTED) {
                    continue;
                }
                // Only consider structures that involve current_txn
                if (tin != current_txn && pivot != current_txn && tout != current_txn) {
                    continue;
                }
                // 两个连续的 rw 反依赖，且执行区间存在重叠
                if (!TransactionsOverlap(tin_it->second.get(), pivot_it->second.get()) ||
                    !TransactionsOverlap(pivot_it->second.get(), tout_it->second.get())) {
                    continue;
                }
                // Tin = Tout 或 Tout 的提交顺序早于 Tin
                if (tin == tout || CommittedBeforeUnlocked(tout, tin)) {
                    return true;
                }
            }
        }
    }
    return false;
}

// ---- SSI Dependency Tracking ----

size_t TransactionManager::DebugSsiWriteCount() {
    std::unique_lock<std::mutex> lock(latch_);
    return RecentWriteCountUnlocked();
}

size_t TransactionManager::DebugActiveRecordReaderKeyCount() {
    std::unique_lock<std::mutex> lock(latch_);
    return active_record_readers_.size();
}

size_t TransactionManager::DebugActivePredicateTableCount() {
    std::unique_lock<std::mutex> lock(latch_);
    return active_predicate_readers_by_table_.size();
}

size_t TransactionManager::DebugTxnMapSize() {
    std::unique_lock<std::mutex> lock(latch_);
    return txn_map.size();
}

bool TransactionManager::AddRwEdge(txn_id_t from, txn_id_t to) {
    std::unique_lock<std::mutex> lock(latch_);
    return AddRwEdgeInternal(from, to, from);
}

bool TransactionManager::AddRwEdgeInternal(txn_id_t reader, txn_id_t writer, txn_id_t current_txn) {
    if (reader == writer) {
        return false;
    }
    auto reader_it = txn_map.find(reader);
    auto writer_it = txn_map.find(writer);
    if (reader_it == txn_map.end() || writer_it == txn_map.end()) {
        return false;
    }
    auto* reader_txn = reader_it->second.get();
    auto* writer_txn = writer_it->second.get();
    if (reader_txn->get_state() == TransactionState::ABORTED || writer_txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (!TransactionsOverlap(reader_txn, writer_txn)) {
        return false;
    }
    // Already exists in centralized rw_edges_?
    auto& outs = rw_edges_[reader];
    if (outs.find(writer) != outs.end()) {
        return false;
    }
    outs.insert(writer);
    // Also update per-txn state for CleanupSsiState
    reader_txn->out_rw_.insert(writer);
    writer_txn->in_rw_.insert(reader);
    // Check for SSI danger structure
    return HasDangerousStructureUnlocked(current_txn);
}

void TransactionManager::RecordRead(txn_id_t reader, const std::string& tab_name, const Rid& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map.find(reader);
    if (it == txn_map.end() || it->second == nullptr)
        return;
    if (it->second->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return;
    // Per-txn record read tracking (for write-side CheckWriteAgainstReaders)
    std::string rid_key = tab_name + ":" + std::to_string(rid.page_no) + ":" + std::to_string(rid.slot_no);
    it->second->read_rids_.insert(rid_key);
    // Also store in centralized read list for PruneSsiState
    ssi_record_reads_.push_back({reader, tab_name, rid});
    SsiRecordKey key = MakeSsiRecordKey(tab_name, rid);
    active_record_readers_[key].insert(reader);
    txn_record_read_keys_[reader].push_back(key);
    active_serializable_txns_.insert(reader);
}

bool TransactionManager::RecordPredicateRead(Transaction* txn, const std::string& tab_name,
                                             const std::vector<Condition>& conds) {
    if (txn == nullptr || txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    bool dangerous = false;
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn->predicate_reads_.push_back(Transaction::PredicateRead{tab_name, conds});
        txn_id_t reader_id = txn->get_transaction_id();
        active_predicate_readers_by_table_[tab_name].insert(reader_id);
        txn_predicate_read_tables_[reader_id].push_back(tab_name);
        active_serializable_txns_.insert(reader_id);

        auto writes_it = recent_writes_by_table_.find(tab_name);
        if (writes_it == recent_writes_by_table_.end()) {
            return false;
        }
        for (const auto& write : writes_it->second) {
            if (write.txn_id_ == reader_id || write.tab_name_ != tab_name) {
                continue;
            }
            auto writer_it = txn_map.find(write.txn_id_);
            if (writer_it == txn_map.end()) {
                continue;
            }
            auto* writer_txn = writer_it->second.get();
            TransactionState writer_state = writer_txn->get_state();
            timestamp_t writer_commit_ts = writer_txn->get_commit_ts();
            if (writer_state == TransactionState::ABORTED) {
                continue;
            }
            bool invisible = writer_state != TransactionState::COMMITTED || writer_commit_ts > txn->get_read_ts();
            if (!invisible) {
                continue;
            }
            bool matches = (write.old_rec_.has_value() && TupleMatches(tab_name, conds, *write.old_rec_)) ||
                           (write.new_rec_.has_value() && TupleMatches(tab_name, conds, *write.new_rec_));
            if (matches && AddRwEdgeInternal(reader_id, write.txn_id_, reader_id)) {
                dangerous = true;
                break;
            }
        }
    }
    if (dangerous) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::SSI_DANGER);
    }
    return false;
}

bool TransactionManager::CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name) {
    std::unique_lock<std::mutex> lock(latch_);
    auto writer_it = txn_map.find(writer);
    auto* writer_txn = (writer_it != txn_map.end()) ? writer_it->second.get() : nullptr;
    if (writer_txn == nullptr || writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }
    SsiRecordKey key = MakeSsiRecordKey(tab_name, rid);
    if (!HasActiveSsiReadersForWriteUnlocked(tab_name, key)) {
        return false;
    }

    bool danger = false;
    auto record_it = active_record_readers_.find(key);
    if (record_it != active_record_readers_.end()) {
        std::vector<txn_id_t> readers(record_it->second.begin(), record_it->second.end());
        for (txn_id_t tid : readers) {
            if (tid == writer) {
                continue;
            }
            auto reader_it = txn_map.find(tid);
            auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
            if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
                continue;
            }
            if (AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }

    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    if (predicate_it == active_predicate_readers_by_table_.end()) {
        return danger;
    }
    std::vector<txn_id_t> predicate_readers(predicate_it->second.begin(), predicate_it->second.end());
    for (txn_id_t tid : predicate_readers) {
        if (tid == writer) {
            continue;
        }
        auto reader_it = txn_map.find(tid);
        auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
        if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
            continue;
        }
        if (AddRwEdgeInternal(tid, writer, writer)) {
            danger = true;
        }
    }
    return danger;
}

bool TransactionManager::CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name,
                                                  const std::optional<RmRecord>& old_rec,
                                                  const std::optional<RmRecord>& new_rec,
                                                  const std::vector<ColMeta>& cols) {
    (void)cols;
    std::unique_lock<std::mutex> lock(latch_);
    auto writer_it = txn_map.find(writer);
    auto* writer_txn = (writer_it != txn_map.end()) ? writer_it->second.get() : nullptr;
    if (writer_txn == nullptr)
        return false;
    if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        // For non-SER writers: don't store writes, don't check
        return false;
    }
    SsiRecordKey key = MakeSsiRecordKey(tab_name, rid);
    const bool has_other_active_serializable = HasOtherActiveSerializableTxnUnlocked(writer);
    if (!HasActiveSsiReadersForWriteUnlocked(tab_name, key)) {
        if (has_other_active_serializable) {
            recent_writes_by_table_[tab_name].push_back({writer, tab_name, rid, old_rec, new_rec});
        }
        return false;
    }

    bool danger = false;
    auto record_it = active_record_readers_.find(key);
    if (record_it != active_record_readers_.end()) {
        std::vector<txn_id_t> readers(record_it->second.begin(), record_it->second.end());
        for (txn_id_t tid : readers) {
            if (tid == writer) {
                continue;
            }
            auto reader_it = txn_map.find(tid);
            auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
            if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
                continue;
            }
            if (AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }

    auto predicate_it = active_predicate_readers_by_table_.find(tab_name);
    if (predicate_it != active_predicate_readers_by_table_.end()) {
        std::vector<txn_id_t> predicate_readers(predicate_it->second.begin(), predicate_it->second.end());
        for (txn_id_t tid : predicate_readers) {
            if (tid == writer) {
                continue;
            }
            auto reader_it = txn_map.find(tid);
            auto* reader_txn = reader_it == txn_map.end() ? nullptr : reader_it->second.get();
            if (!SsiTxnCanConflictWithWriter(reader_txn, writer_txn)) {
                continue;
            }

            bool read_matches = false;
            for (const auto& predicate : reader_txn->predicate_reads_) {
                if (predicate.tab_name_ != tab_name) {
                    continue;
                }
                if ((old_rec.has_value() && TupleMatches(tab_name, predicate.conds_, *old_rec)) ||
                    (new_rec.has_value() && TupleMatches(tab_name, predicate.conds_, *new_rec))) {
                    read_matches = true;
                    break;
                }
            }
            if (read_matches && AddRwEdgeInternal(tid, writer, writer)) {
                danger = true;
            }
        }
    }

    if (!danger && (HasOtherActivePredicateReadersUnlocked(tab_name, writer) || has_other_active_serializable)) {
        recent_writes_by_table_[tab_name].push_back({writer, tab_name, rid, old_rec, new_rec});
    }

    return danger;
}

bool TransactionManager::CheckInvisibleWrites(txn_id_t reader, Rid rid, const std::string& tab_name) {
    (void)tab_name;
    std::unique_lock<std::mutex> lock(latch_);
    bool danger = false;
    auto reader_it = txn_map.find(reader);
    auto* reader_txn = (reader_it != txn_map.end()) ? reader_it->second.get() : nullptr;
    if (reader_txn == nullptr || reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;

    for (auto& [tid, txn] : txn_map) {
        if (tid == reader)
            continue;
        if (txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
            continue;
        if (txn->get_state() == TransactionState::ABORTED)
            continue;

        bool invisible_to_reader =
            txn->get_state() == TransactionState::GROWING ||
            (txn->get_state() == TransactionState::COMMITTED && txn->get_commit_ts() > reader_txn->get_read_ts());
        if (!invisible_to_reader)
            continue;

        // Check if this transaction has modified the given RID
        bool modified_this_rid = false;
        for (const auto& [tab, slot_rid] : txn->get_modified_slots()) {
            if (slot_rid.page_no == rid.page_no && slot_rid.slot_no == rid.slot_no) {
                modified_this_rid = true;
                break;
            }
        }

        if (modified_this_rid) {
            if (AddRwEdgeInternal(reader, tid, reader)) {
                danger = true;
            }
        }
    }
    return danger;
}

bool TransactionManager::CheckInvisibleWriteEdge(txn_id_t reader, txn_id_t page_writer) {
    if (page_writer == reader)
        return false;
    if (page_writer == INVALID_TXN_ID)
        return false;

    std::unique_lock<std::mutex> lock(latch_);
    auto reader_it = txn_map.find(reader);
    if (reader_it == txn_map.end())
        return false;
    auto* reader_txn = reader_it->second.get();
    if (reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;

    auto writer_it = txn_map.find(page_writer);
    if (writer_it == txn_map.end())
        return false;
    auto* writer_txn = writer_it->second.get();
    if (writer_txn == nullptr)
        return false;
    if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
        return false;
    if (writer_txn->get_state() == TransactionState::ABORTED)
        return false;

    return AddRwEdgeInternal(reader, page_writer, reader);
}

bool TransactionManager::CheckPredicateInvisibleWrites(txn_id_t reader, const std::string& tab_name,
                                                       const std::vector<Condition>& conds, RmFileHandle* fh,
                                                       const std::vector<ColMeta>& cols) {
    if (fh == nullptr) {
        return false;
    }

    std::unique_lock<std::mutex> lock(latch_);
    auto reader_it = txn_map.find(reader);
    if (reader_it == txn_map.end())
        return false;
    auto* reader_txn = reader_it->second.get();
    if (reader_txn == nullptr || reader_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE) {
        return false;
    }

    bool danger = false;
    for (auto& [tid, writer_txn] : txn_map) {
        if (tid == reader || writer_txn == nullptr)
            continue;
        if (writer_txn->get_isolation_level() != IsolationLevel::SERIALIZABLE)
            continue;
        if (writer_txn->get_state() == TransactionState::ABORTED)
            continue;

        bool invisible_to_reader = writer_txn->get_state() == TransactionState::GROWING ||
                                   (writer_txn->get_state() == TransactionState::COMMITTED &&
                                    writer_txn->get_commit_ts() > reader_txn->get_read_ts());
        if (!invisible_to_reader)
            continue;

        for (const auto& [slot_tab, rid] : writer_txn->get_modified_slots()) {
            if (slot_tab != tab_name || !fh->is_record(rid)) {
                continue;
            }
            TupleMeta meta = fh->get_tuple_meta(rid);
            if (meta.writer_txn_id_ != tid || meta.is_deleted_) {
                continue;
            }
            auto rec = fh->get_record(rid, nullptr);
            if (rec == nullptr || !MatchesConditions(conds, *rec, cols)) {
                continue;
            }

            if (AddRwEdgeInternal(reader, tid, reader)) {
                danger = true;
            }
        }
    }
    return danger;
}

void TransactionManager::CleanupSsiState(txn_id_t txn_id) {
    CleanupTxnSsiState(txn_id);
}

void TransactionManager::PruneSsiState() {
    std::unique_lock<std::mutex> lock(latch_);

    if (recent_writes_by_table_.empty() && ssi_writes_.empty() && active_record_readers_.empty() &&
        active_predicate_readers_by_table_.empty() && rw_edges_.empty() && active_serializable_txns_.empty()) {
        commits_since_full_ssi_prune_ = 0;
        return;
    }

    std::unordered_set<txn_id_t> candidates = active_serializable_txns_;
    for (const auto& write : ssi_writes_) {
        candidates.insert(write.txn_id_);
    }
    for (const auto& read : ssi_record_reads_) {
        candidates.insert(read.txn_id_);
    }
    for (const auto& [_, writes] : recent_writes_by_table_) {
        for (const auto& write : writes) {
            candidates.insert(write.txn_id_);
        }
    }
    for (const auto& [txn_id, _] : txn_record_read_keys_) {
        candidates.insert(txn_id);
    }
    for (const auto& [txn_id, _] : txn_predicate_read_tables_) {
        candidates.insert(txn_id);
    }
    for (const auto& [txn_id, _] : rw_edges_) {
        candidates.insert(txn_id);
    }
    for (const auto& [_, outs] : rw_edges_) {
        for (txn_id_t txn_id : outs) {
            candidates.insert(txn_id);
        }
    }

    for (txn_id_t txn_id : candidates) {
        auto txn_it = txn_map.find(txn_id);
        Transaction* txn = txn_it == txn_map.end() ? nullptr : txn_it->second.get();
        if (txn == nullptr || txn->get_state() == TransactionState::ABORTED ||
            (txn->get_state() == TransactionState::COMMITTED && !HasActiveSerializableOverlapUnlocked(txn))) {
            active_serializable_txns_.erase(txn_id);
            CleanupTxnReadIndexesUnlocked(txn_id);
            CleanupTxnRwEdgesUnlocked(txn_id);
            CleanupTxnRecentWritesUnlocked(txn_id);
        }
    }

    for (auto it = txn_map.begin(); it != txn_map.end();) {
        Transaction* txn = it->second.get();
        if (CanRetireTransactionUnlocked(txn)) {
            it = txn_map.erase(it);
        } else {
            ++it;
        }
    }

    commits_since_full_ssi_prune_ = 0;
}
