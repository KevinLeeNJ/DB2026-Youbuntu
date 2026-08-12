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
#include <stdexcept>
#include <string_view>
#include <vector>

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

    auto logical_row_delete_intents = *txn->get_logical_row_delete_intent_set();
    for (const auto& intent_id : logical_row_delete_intents) {
        lock_manager->unregister_logical_row_delete_intent(txn, intent_id);
    }
    txn->get_logical_row_delete_intent_set()->clear();
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
                        const Rid& rid, const IndexWriteWalContext& wal_context) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->delete_entry(key.data(), rid, wal_context);
    }
}

void InsertIndexEntries(SmManager* sm_manager, const TabMeta& tab, const std::string& tab_name, const RmRecord& rec,
                        const Rid& rid, const IndexWriteWalContext& wal_context) {
    for (const auto& index : tab.indexes) {
        auto key = MakeIndexKey(index, rec.data);
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        std::vector<Rid> existing;
        ih->get_value(key.data(), &existing, nullptr);
        if (std::find(existing.begin(), existing.end(), rid) == existing.end()) {
            // Preserve duplicate-key indexes while making repeated abort/local
            // compensation of the same physical (key,RID) a no-op.
            ih->insert_entry(key.data(), rid, wal_context, true);
        }
    }
}

void WriteBeginLog(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr || log_manager == nullptr) {
        return;
    }
    BeginLogRecord record(txn->get_transaction_id());
    lsn_t lsn = log_manager->add_log_to_buffer(&record);
    txn->set_begin_lsn(lsn);
    txn->set_prev_lsn(lsn);
}

lsn_t AppendCommitLog(Transaction* txn, LogManager* log_manager, timestamp_t commit_ts) {
    if (txn == nullptr || log_manager == nullptr) {
        return INVALID_LSN;
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
    return lsn;
}

void WaitForCommitLog(LogManager* log_manager, lsn_t lsn) {
    if (log_manager == nullptr || lsn == INVALID_LSN) {
        return;
    }
    // Returning from COMMIT means the commit record survived an OS crash,
    // not merely that it reached the kernel page cache.
    log_manager->flush_log_to_disk_up_to(lsn);
    FaultInjector::Point("after_commit_wal_write");
}

void WriteCommitLog(Transaction* txn, LogManager* log_manager, timestamp_t commit_ts) {
    WaitForCommitLog(log_manager, AppendCommitLog(txn, log_manager, commit_ts));
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

void UndoWriteRecord(TransactionManager* txn_mgr, SmManager* sm_manager, WriteRecord* write_record, lsn_t page_lsn,
                     const TransactionManager::AbortDeleteUndoPublicationTestHook& delete_undo_publication_hook) {
    const std::string tab_name = write_record->GetTableName();
    auto& tab = sm_manager->db_.get_table(tab_name);
    auto* fh = sm_manager->fhs_.at(tab_name).get();
    Rid rid = write_record->GetRid();
    std::optional<IndexWriteWalContext> index_wal_context;
    if (!tab.indexes.empty()) {
        // Heap-only rollback has no index page to protect and is valid in
        // embedded/no-WAL tests. Indexed rollback remains fail-closed: it must
        // have the ABORT LSN that covers every structural after-image.
        index_wal_context.emplace(IndexWriteWalContext::LiveRollback(page_lsn));
    }

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
            DeleteIndexEntries(sm_manager, tab, tab_name, *rec, rid, *index_wal_context);
            fh->delete_record(rid, nullptr, page_lsn);
        }
        break;
    }
    case WType::DELETE_TUPLE: {
        const RmRecord& old_rec = write_record->GetRecord();
        const TupleMeta current_meta = fh->get_tuple_meta(rid);
        auto undo = GetCurrentUndoLog(txn_mgr, fh, rid);
        const TupleMeta old_meta = undo.has_value() ? undo->old_meta_ : FallbackCommittedMeta();

        // A normal transactional delete leaves the record allocated under an
        // uncommitted tombstone. Keep that tombstone in place until every old
        // physical (key,RID) has been restored: GC must not observe the old
        // committed meta while the physical index still lacks the old key.
        // Recovery/legacy undo may find an absent bitmap slot. Recreate that
        // slot under the same unpublished tombstone, never under the default
        // committed meta supplied by insert_record().
        if (fh->is_record(rid)) {
            // The current heap record is already the delete tombstone.
        } else {
            TupleMeta unpublished_tombstone = current_meta;
            unpublished_tombstone.is_committed_ = false;
            unpublished_tombstone.is_deleted_ = true;
            fh->insert_record(rid, old_rec.data, page_lsn, &unpublished_tombstone);
        }
        if (index_wal_context.has_value()) {
            InsertIndexEntries(sm_manager, tab, tab_name, old_rec, rid, *index_wal_context);
        }
        // InsertIndexEntries has released all index latches. This test-only
        // seam deliberately runs before taking the heap page latch below.
        if (delete_undo_publication_hook) {
            delete_undo_publication_hook();
        }
        fh->apply_tuple_update(rid, old_rec.data, old_meta, page_lsn);
        break;
    }
    case WType::UPDATE_TUPLE: {
        const RmRecord& old_rec = write_record->GetRecord();
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
                ih->delete_entry(current_key.data(), rid, *index_wal_context);
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
                ih->insert_entry(old_key.data(), rid, *index_wal_context, true);
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
        const bool blocked = checkpoint_blocking_new_txns_;
        if (blocked) {
            InvokeCheckpointAdmissionTestHook("begin_waiting");
        }
        checkpoint_cv_.wait(checkpoint_lock, [&] { return !checkpoint_blocking_new_txns_; });
        active_txn_ids_.insert(txn->get_transaction_id());
        active_txn_count_ = static_cast<int>(active_txn_ids_.size());
        InvokeCheckpointAdmissionTestHook("begin_admitted");
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
        txn_map_[txn->get_transaction_id()] = std::move(created);
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

lsn_t TransactionManager::CompletedCommitLsn(const LogManager* log_manager) const {
    if (log_manager == nullptr) {
        return INVALID_LSN;
    }
    if (log_manager->durability_mode() == DurabilityMode::STRICT) {
        return log_manager->get_durable_lsn();
    }
    return log_manager->get_persist_lsn();
}

void TransactionManager::RunCommitPublicationLeader(timestamp_t target_csn, LogManager* log_manager) {
    for (;;) {
        std::shared_ptr<CommitPublicationRequest> request;
        {
            std::unique_lock<std::mutex> frontier_lock(commit_frontier_latch_);
            for (;;) {
                if (published_commit_csn_ >= target_csn) {
                    commit_publication_leader_active_ = false;
                    frontier_lock.unlock();
                    commit_frontier_cv_.notify_all();
                    return;
                }

                auto next = pending_commit_publications_.find(published_commit_csn_ + 1);
                if (next != pending_commit_publications_.end() && !next->second->publishing &&
                    next->second->commit_lsn <= CompletedCommitLsn(log_manager)) {
                    request = next->second;
                    request->publishing = true;
                    break;
                }

                std::shared_ptr<CommitPublicationRequest> waiting_for_wal;
                if (next != pending_commit_publications_.end() && !next->second->publishing) {
                    waiting_for_wal = next->second;
                }
                const timestamp_t waiting_for_csn = published_commit_csn_ + 1;
                const uint64_t observed_epoch = commit_publication_epoch_;
                frontier_lock.unlock();
                if (waiting_for_wal != nullptr) {
                    InvokeCommitPublicationTestHook("leader_waiting_for_wal", waiting_for_wal->commit_csn,
                                                    waiting_for_wal->commit_lsn);
                } else {
                    InvokeCommitPublicationTestHook("leader_waiting_for_request", waiting_for_csn, INVALID_LSN);
                }
                frontier_lock.lock();
                commit_frontier_cv_.wait(frontier_lock, [&] { return commit_publication_epoch_ != observed_epoch; });
            }
        }

        FaultInjector::Point("before_tuple_publication");
        sm_manager_->mark_slots_committed(*request->txn, request->commit_ts);
        FaultInjector::Point("after_tuple_publication");

        request->txn->set_state(TransactionState::COMMITTED);
        FaultInjector::Point("before_published_csn_store");
        {
            std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
            if (request->commit_csn != published_commit_csn_ + 1) {
                throw InternalError("commit publication frontier lost contiguous order");
            }
            last_commit_ts_.store(request->commit_ts, std::memory_order_release);
            ++published_commit_csn_;
        }
        FaultInjector::Point("after_commit_publication_frontier");
        InvokeCommitPublicationTestHook("after_frontier", request->commit_csn, request->commit_lsn);

        FaultInjector::Point("before_commit_helper_lock_release");
        ReleaseLocks(request->txn, lock_manager_);
        FaultInjector::Point("after_commit_helper_lock_release");
        InvokeCommitPublicationTestHook("after_lock_release", request->commit_csn, request->commit_lsn);
        {
            std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
            request->locks_released = true;
            request->done = true;
            pending_commit_publications_.erase(request->commit_csn);
        }
        commit_frontier_cv_.notify_all();
    }
}

void TransactionManager::PublishOrWaitForCommit(const std::shared_ptr<CommitPublicationRequest>& request,
                                                LogManager* log_manager) {
    {
        std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
        ++commit_publication_epoch_;
    }
    commit_frontier_cv_.notify_all();

    while (true) {
        timestamp_t target_csn = INVALID_TS;
        {
            std::unique_lock<std::mutex> frontier_lock(commit_frontier_latch_);
            if (request->done) {
                return;
            }
            if (commit_publication_leader_active_) {
                commit_frontier_cv_.wait(frontier_lock,
                                         [&] { return request->done || !commit_publication_leader_active_; });
                if (request->done) {
                    return;
                }
            }
            if (!commit_publication_leader_active_) {
                commit_publication_leader_active_ = true;
                target_csn = request->commit_csn;
            }
        }
        if (target_csn != INVALID_TS) {
            RunCommitPublicationLeader(target_csn, log_manager);
        }
    }
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    commit_impl(txn, log_manager);
}

void TransactionManager::commit_impl(Transaction* txn, LogManager* log_manager) {
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
    std::shared_ptr<CommitPublicationRequest> publication_request;
    bool locks_released_by_helper = false;
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
        InvokeCommitPublicationTestHook("after_csn_allocated", commit_csn, INVALID_LSN);

        // 时间戳分配移到 WAL 之前，只为让 COMMIT 记录能带上 commit_ts；它不发布
        // 任何东西，因此不改变可见性顺序。分配后若 WriteCommitLog 抛出，catch 分支
        // 直接 fail-stop 整个进程，不会留下一个占用了 CSN 却永不完成、卡住发布
        // 前沿的事务。
        //
        // Once COMMITTING is visible, neither a WAL failure nor a publication
        // failure may fall back to ordinary abort: the COMMIT record may
        // already be present in the WAL prefix recovered after restart.
        if (commit_publication_helping_enabled_) {
            publication_request = std::make_shared<CommitPublicationRequest>();
            publication_request->txn = txn;
            publication_request->commit_csn = commit_csn;
            publication_request->commit_ts = commit_ts;
            txn->pin_commit_publication();
            {
                publication_request->commit_lsn = AppendCommitLog(txn, log_manager, commit_ts);
                {
                    std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
                    auto [_, inserted] = pending_commit_publications_.emplace(commit_csn, publication_request);
                    if (!inserted) {
                        throw InternalError("duplicate commit publication CSN");
                    }
                    ++commit_publication_epoch_;
                }
                commit_frontier_cv_.notify_all();
                FaultInjector::Point("after_commit_publication_register");
                InvokeCommitPublicationTestHook("after_registered", commit_csn, publication_request->commit_lsn);
                WaitForCommitLog(log_manager, publication_request->commit_lsn);
            }
            // WAL completion is an independent publication prerequisite. Wake
            // a leader that may already be sleeping on this request before the
            // owner runs hooks or enters PublishOrWaitForCommit. Keep the
            // notification here instead of installing a LogManager callback,
            // which would couple LogManager lifetime to TransactionManager.
            {
                std::lock_guard<std::mutex> frontier_lock(commit_frontier_latch_);
                ++commit_publication_epoch_;
            }
            commit_frontier_cv_.notify_all();
            FaultInjector::Point("after_commit_wal_sync");
            InvokeCommitPublicationTestHook("after_wal_wait", commit_csn, publication_request->commit_lsn);
            PublishOrWaitForCommit(publication_request, log_manager);
            locks_released_by_helper = publication_request->locks_released;
        } else {
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
        }

        // Keep the committing transaction in the watermark and checkpoint
        // active set until publication and all owner-side cleanup complete.
        // Any failure from this point is also fail-stop: the COMMIT record may
        // already be durable and the transaction may already be visible.
        FaultInjector::Point("before_commit_owner_cleanup");
        InvokeCommitPublicationTestHook("before_owner_cleanup", commit_csn,
                                        publication_request == nullptr ? txn->get_prev_lsn()
                                                                       : publication_request->commit_lsn);
        {
            running_txns_.UpdateCommitTs(txn->get_commit_ts());
            running_txns_.RemoveTxnSlot(txn->get_watermark_slot());
            ClearWriteSet(txn);
            if (!locks_released_by_helper) {
                ReleaseLocks(txn, lock_manager_);
            }
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
            if (publication_request != nullptr) {
                txn->unpin_commit_publication();
            }
            RetireTransactionIfSafe(txn);
            MaybeRunGarbageCollection();
        }
    } catch (...) {
        // Once COMMITTING is visible, the COMMIT record may be persistent and
        // the transaction may be partially or fully published. Ordinary
        // exception return or abort would make recovery and in-memory state
        // disagree, including for failures in owner-side cleanup.
        FailStopAfterCommitMayBePersistent();
    }
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
        // A cancelled waiter still dereferences txn while it unregisters from
        // its queue. Do not retire this transaction until that handoff has
        // completed; otherwise abort races a waiter into use-after-free.
        lock_manager_->wait_for_transaction_lock_requests(txn->get_transaction_id());
    }
    // A row mutation keeps this pin through its post-lock checks and physical
    // write path.  Do not publish ABORTED, release its locks, or retire it
    // while that caller can still dereference txn.
    txn->wait_for_lock_operations();
    lsn_t abort_lsn = INVALID_LSN;
    if (!write_set.empty() || txn->get_prev_lsn() != txn->get_begin_lsn()) {
        // The abort record must precede the physical undo. This gives every
        // page modified by rollback a WAL record that can be used as its page
        // LSN. Transactions with no writes and no WAL after BEGIN have no
        // persistent state to undo and do not need to force an ABORT record.
        abort_lsn = WriteAbortLog(txn, log_manager);
    }
    for (auto it = write_set.rbegin(); it != write_set.rend(); ++it) {
        UndoWriteRecord(this, sm_manager_, it->get(), abort_lsn, abort_delete_undo_publication_test_hook_);
    }
    running_txns_.RemoveTxnSlot(txn->get_watermark_slot());
    ClearWriteSet(txn);
    // Physical undo is complete. Remove this transaction from the SSI
    // conflict surface before releasing a record lock can wake a serializable
    // writer; otherwise that writer can observe a rolled-back owner as a
    // GROWING reader and manufacture a false dangerous structure.
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
    if (abort_before_lock_release_test_hook_) {
        abort_before_lock_release_test_hook_(txn->get_transaction_id());
    }
    // Abort WAL and all heap/index undo precede this handoff. Waiters can now
    // proceed without seeing this transaction as an SSI reader or writer.
    ReleaseLocks(txn, lock_manager_);
    RetireTransactionIfSafe(txn);
    MaybeRunGarbageCollection();
}

void TransactionManager::block_new_transactions_for_checkpoint() {
    std::lock_guard<std::mutex> checkpoint_lock(checkpoint_latch_);
    checkpoint_blocking_new_txns_ = true;
}

void TransactionManager::InvokeCheckpointAdmissionTestHook(std::string_view event) {
    if (checkpoint_admission_test_hook_) {
        checkpoint_admission_test_hook_(event);
    }
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
    InvokeCheckpointAdmissionTestHook("drain_waiting");
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
        auto it = txn_map_.find(txn_id);
        if (it != txn_map_.end() && it->second != nullptr) {
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
    auto it = txn_map_.find(link.undo_txn_id_);
    if (it == txn_map_.end()) {
        throw InternalError("GetUndoLog: transaction not found");
    }
    return it->second->GetUndoLog(link.undo_slot_offset_);
}

std::optional<UndoLog> TransactionManager::GetUndoLogOptional(UndoLink link) {
    std::unique_lock<std::mutex> lock(latch_);
    auto it = txn_map_.find(link.undo_txn_id_);
    if (it == txn_map_.end()) {
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
