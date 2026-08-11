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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <functional>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

#include "transaction.h"
#include "watermark.h"
#include "recovery/log_manager.h"
#include "concurrency/lock_manager.h"
#include "system/sm_manager.h"
#include "common/exception.h"

/* 系统采用的并发控制算法，当前题目中要求两阶段封锁并发控制算法 */
enum class ConcurrencyMode { TWO_PHASE_LOCKING = 0, BASIC_TO, MVCC };

class TransactionManager {
public:
    using CommitPublicationTestHook = std::function<void(std::string_view, timestamp_t, lsn_t)>;
    using CheckpointAdmissionTestHook = std::function<void(std::string_view)>;
    using AbortBeforeLockReleaseTestHook = std::function<void(txn_id_t)>;
    using AbortDeleteUndoPublicationTestHook = std::function<void()>;

    // Test-only injection for deterministic publication scheduling and the
    // direct-publication negative oracle. Production construction does not
    // accept a helping toggle and always enables helping.
    struct CommitPublicationTestOptions {
        bool helping{true};
        CommitPublicationTestHook hook;
    };

    struct CheckpointAdmissionTestOptions {
        CheckpointAdmissionTestHook hook;
    };

    explicit TransactionManager(LockManager* lock_manager, SmManager* sm_manager,
                                ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING)
        : TransactionManager(lock_manager, sm_manager, concurrency_mode, CommitPublicationTestOptions{}) {}

    // Test-only overload. Callers must opt in with the explicitly named
    // options type; normal server/runtime construction cannot disable helping.
    TransactionManager(LockManager* lock_manager, SmManager* sm_manager, ConcurrencyMode concurrency_mode,
                       CommitPublicationTestOptions test_options) {
        sm_manager_ = sm_manager;
        lock_manager_ = lock_manager;
        concurrency_mode_ = concurrency_mode;
        commit_publication_helping_enabled_ = test_options.helping;
        commit_publication_test_hook_ = std::move(test_options.hook);
        gc_thread_ = std::thread(&TransactionManager::GarbageCollectionLoop, this);
    }

    // Test-only overload for deterministic checkpoint admission scheduling.
    // Production construction always leaves this hook empty.
    TransactionManager(LockManager* lock_manager, SmManager* sm_manager, ConcurrencyMode concurrency_mode,
                       CheckpointAdmissionTestOptions test_options) {
        sm_manager_ = sm_manager;
        lock_manager_ = lock_manager;
        concurrency_mode_ = concurrency_mode;
        checkpoint_admission_test_hook_ = std::move(test_options.hook);
        gc_thread_ = std::thread(&TransactionManager::GarbageCollectionLoop, this);
    }

    ~TransactionManager();

    bool commit_publication_helping_enabled_for_test() const noexcept {
        return commit_publication_helping_enabled_;
    }

    Transaction* begin(Transaction* txn, LogManager* log_manager,
                       IsolationLevel isolation_level = DEFAULT_ISOLATION_LEVEL);

    void BeginStatement(Transaction* txn);

    void commit(Transaction* txn, LogManager* log_manager);

    void abort(Transaction* txn, LogManager* log_manager);

    // Test-only ordering hook. It runs after abort undo and SSI cleanup, but
    // before ReleaseLocks can wake another transaction.
    void set_abort_before_lock_release_test_hook(AbortBeforeLockReleaseTestHook hook) {
        abort_before_lock_release_test_hook_ = std::move(hook);
    }

    // Test-only ordering hook. It runs after DELETE undo restores all old
    // (key,RID) index entries, but before the old committed tuple meta is
    // published. Callers must install and clear it outside the abort thread.
    void set_abort_delete_undo_publication_test_hook(AbortDeleteUndoPublicationTestHook hook) {
        abort_delete_undo_publication_test_hook_ = std::move(hook);
    }

    void block_new_transactions_for_checkpoint();

    void unblock_new_transactions_after_checkpoint();

    // Wait until no transaction is active. Returns false when the timeout
    // expires with transactions still running, so the caller can abandon the
    // checkpoint instead of holding the "block new transactions" window open.
    bool wait_active_transactions_drained_for_checkpoint(std::chrono::milliseconds timeout);

    std::unordered_map<txn_id_t, lsn_t> get_active_txn_lsn_snapshot();

    /**
     * @brief 重启后把时间戳/事务 ID 计数器抬到持久化状态之上。恢复结束、任何事务开始
     * 之前调用一次。
     *
     * 为什么必须做：TupleMeta.commit_ts_ 是**持久化**在数据页里的，而 RC 的 read_ts
     * 取自 last_commit_ts_。如果计数器每次进程启动都从 0 开始，上一世以
     * commit_ts_ = 50000 提交的行，面对一个从 0 开始的 read_ts 会被判成“来自未来”；
     * 此时版本链（只存在于内存里）已经随进程消失，GetVisibleRecord 无从回退，
     * 于是**已提交的行变得不可见**——违反 final.md:342。PostgreSQL 把 nextXid 记在
     * pg_control、InnoDB 把 max trx id 记在系统表空间，都是同一件事。
     *
     * next_timestamp 由 RecoveryManager::get_recovered_next_timestamp() 计算，
     * 那里有“为什么这个值一定大于任何已持久化的 commit_ts_”的完整论证。
     */
    void seed_counters_after_recovery(timestamp_t next_timestamp, txn_id_t next_txn_id);

    /** @brief 当前计数器快照。checkpoint 用它写 db.restart；见 seed_counters_after_recovery。 */
    timestamp_t peek_next_timestamp() const {
        return next_timestamp_.load();
    }

    txn_id_t peek_next_txn_id() const {
        return next_txn_id_.load();
    }

    timestamp_t get_last_commit_ts() const {
        return last_commit_ts_.load(std::memory_order_acquire);
    }

    ConcurrencyMode get_concurrency_mode() {
        return concurrency_mode_;
    }

    void set_concurrency_mode(ConcurrencyMode concurrency_mode) {
        concurrency_mode_ = concurrency_mode;
    }

    LockManager* get_lock_manager() {
        return lock_manager_;
    }

    /**
     * @description: 获取事务ID为txn_id的事务对象
     * @return {Transaction*} 事务对象的指针
     * @param {txn_id_t} txn_id 事务ID
     */
    Transaction* get_transaction(txn_id_t txn_id) {
        if (txn_id == INVALID_TXN_ID)
            return nullptr;

        // Counts only the lookups that reach this manager's txn_map mutex, which is
        // the cost a session avoids by caching the transaction it is running.
        std::unique_lock<std::mutex> lock(latch_);
        auto it = txn_map_.find(txn_id);
        if (it == txn_map_.end())
            return nullptr;
        auto* res = it->second.get();
        lock.unlock();
        assert(res != nullptr);
        // Note: MVCC undo chain traversal may access other threads' transactions

        return res;
    }

    /** @brief 访问事务撤销日志缓冲区并获取撤销日志。如果事务不存在，返回 nullopt。
     * 如果索引超出范围仍然会抛出异常。 */
    std::optional<UndoLog> GetUndoLogOptional(UndoLink link);

    /** @brief 访问事务撤销日志缓冲区并获取撤销日志。除非访问当前事务缓冲区，
     * 否则应该始终调用此函数以获取撤销日志，而不是手动检索事务 shared_ptr 并访问缓冲区。 */
    UndoLog GetUndoLog(UndoLink link);

    /** @brief 获取系统中的最低读时间戳。 */
    timestamp_t GetWatermark();

    /** @brief Traverse the undo chain to find a visible version of a tuple. */
    std::optional<std::pair<TupleMeta, std::vector<char>>>
    FindVisibleVersion(const TupleMeta& start_meta, timestamp_t read_ts, txn_id_t self_txn_id);

    // ---- SSI Dependency Tracking (SER only) ----

    /** @brief Add a rw dependency edge FROM ->rw TO. Returns true if a danger structure is formed. */
    bool AddRwEdge(txn_id_t from, txn_id_t to);

    /** @brief Check and record rw-dependency when the current reader (executing SELECT) sees
     *  a page version written by another SER transaction that is invisible to the reader.
     *  Creates reader ->rw writer edge. Returns true if danger structure formed. */
    bool CheckInvisibleWriteEdge(txn_id_t reader, txn_id_t page_writer);

    /** @brief Check if a write to RID on a table conflicts with any active SER transaction's read/predicate set.
     *  Also stores the write info in ssi_writes_ for future predicate reads.
     *  Thread-safe. Returns true if danger structure formed. */
    bool CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name);

    /** @brief Consolidated check: checks both old_rec and new_rec atomically and stores
     *  a single ssi_writes_ entry only if no danger is found.
     *  For INSERT: old_rec=nullopt. For DELETE: new_rec=nullopt.
     *  For UPDATE: both old_rec and new_rec are provided.
     *  Thread-safe. Returns true if danger structure formed. */
    bool CheckWriteAgainstReaders(txn_id_t writer, Rid rid, const std::string& tab_name,
                                  const std::optional<RmRecord>& old_rec, const std::optional<RmRecord>& new_rec,
                                  const std::vector<ColMeta>& cols);

    /** @brief Check if any active SER transaction has an invisible write that would change
     *  the current reader's SELECT result for the given RID. Creates current_reader ->rw writer edges.
     *  Thread-safe. Returns true if danger structure formed. */
    bool CheckInvisibleWrites(txn_id_t reader, Rid rid, const std::string& tab_name);

    bool CheckPredicateInvisibleWrites(txn_id_t reader, const std::string& tab_name,
                                       const std::vector<Condition>& conds, RmFileHandle* fh,
                                       const std::vector<ColMeta>& cols);

    /** @brief Record that a transaction read a record (for SSI read-set tracking).
     *  Also stores the read in the centralized record-read list for write-side checking. */
    void RecordRead(txn_id_t reader, const std::string& tab_name, const Rid& rid);

    /** @brief Record a predicate read for a transaction and check invisible writes.
     *  Checks ssi_writes_ for invisible writes that match the predicate, creating
     *  reader ->rw writer edges. Returns true if SSI danger structure formed. */
    bool RecordPredicateRead(Transaction* txn, const std::string& tab_name, const std::vector<Condition>& conds);

    /** @brief Check for SSI danger: two consecutive rw-edges (Tin->rw->Tpivot, Tpivot->rw->Tout)
     *  where intervals overlap and Tin==Tout or Tout committed before Tin. */
    bool HasDangerousStructure(txn_id_t current_txn);

    /** @brief Prune SSI state for transactions that are no longer relevant. */
    void PruneSsiState();

    /** @brief Clean up SSI state for an aborted/rolled-back transaction. */
    void CleanupSsiState(txn_id_t txn_id);

    size_t DebugSsiWriteCount();
    size_t DebugActiveRecordReaderKeyCount();
    size_t DebugActivePredicateTableCount();
    size_t DebugTxnMapSize();

    // GC observability: the backlog is the current txn-map population waiting
    // for bounded background collection (including entries not yet eligible).
    /** @brief 垃圾回收。仅在所有事务都未访问时调用。 */
    void GarbageCollection();

private:
    struct CommitPublicationRequest {
        Transaction* txn{nullptr};
        timestamp_t commit_csn{0};
        timestamp_t commit_ts{INVALID_TS};
        lsn_t commit_lsn{INVALID_LSN};
        bool publishing{false};
        bool locks_released{false};
        bool done{false};
    };

    void commit_impl(Transaction* txn, LogManager* log_manager);
    void InvokeCheckpointAdmissionTestHook(std::string_view event);
    void PublishOrWaitForCommit(const std::shared_ptr<CommitPublicationRequest>& request, LogManager* log_manager);
    void RunCommitPublicationLeader(timestamp_t target_csn, LogManager* log_manager);
    lsn_t CompletedCommitLsn(const LogManager* log_manager) const;
    void InvokeCommitPublicationTestHook(std::string_view event, timestamp_t commit_csn, lsn_t commit_lsn) {
        if (commit_publication_test_hook_) {
            commit_publication_test_hook_(event, commit_csn, commit_lsn);
        }
    }

    bool commit_publication_helping_enabled_{true};
    CommitPublicationTestHook commit_publication_test_hook_;
    AbortBeforeLockReleaseTestHook abort_before_lock_release_test_hook_;
    AbortDeleteUndoPublicationTestHook abort_delete_undo_publication_test_hook_;
    ConcurrencyMode concurrency_mode_;           // 事务使用的并发控制算法，目前只需要考虑2PL
    std::atomic<txn_id_t> next_txn_id_{0};       // 用于分发事务ID
    std::atomic<timestamp_t> next_timestamp_{0}; // 用于分发事务时间戳
    std::mutex latch_;                           // 用于txn_map的并发
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
    // Commit publication is ordered by a small completion frontier rather
    // than by holding one mutex while touching every tuple page. A commit may
    // publish outside this mutex; readers advance only through contiguous
    // completed CSNs, so an out-of-order publisher remains invisible.
    std::mutex commit_frontier_latch_;
    std::condition_variable commit_frontier_cv_;
    timestamp_t next_commit_csn_{0};
    timestamp_t published_commit_csn_{0};
    std::map<timestamp_t, timestamp_t> completed_commits_;
    std::map<timestamp_t, std::shared_ptr<CommitPublicationRequest>> pending_commit_publications_;
    bool commit_publication_leader_active_{false};
    uint64_t commit_publication_epoch_{0};
    SmManager* sm_manager_;
    LockManager* lock_manager_;

    std::atomic<timestamp_t> last_commit_ts_{0}; // 最后提交的时间戳,仅用于MVCC
    Watermark running_txns_{0}; // 存储所有正在运行事务的读取时间戳，以便于垃圾回收，仅用于MVCC

    // 垃圾回收节流计数（在 latch_ 下维护）
    uint64_t commits_since_gc_{0};
    bool gc_running_{false};
    bool gc_requested_{false};

    /** 节流式触发垃圾回收：按提交计数或 txn_map 规模决定是否真正执行。 */
    void MaybeRunGarbageCollection();

    void GarbageCollectionLoop();
    bool GarbageCollectionBatch();

    std::mutex checkpoint_latch_;
    std::condition_variable checkpoint_cv_;
    bool checkpoint_blocking_new_txns_{false};
    int active_txn_count_{0};
    std::unordered_set<txn_id_t> active_txn_ids_;
    CheckpointAdmissionTestHook checkpoint_admission_test_hook_;

    std::condition_variable gc_cv_;
    std::thread gc_thread_;
    std::atomic<bool> gc_stop_{false};

    // ---- SSI State (centralized) — protected by latch_ ----
    struct SsiRecordKey {
        std::string tab_name_;
        int page_no_;
        int slot_no_;

        bool operator==(const SsiRecordKey& other) const {
            return tab_name_ == other.tab_name_ && page_no_ == other.page_no_ && slot_no_ == other.slot_no_;
        }
    };

    struct SsiRecordKeyHash {
        size_t operator()(const SsiRecordKey& key) const {
            size_t h = std::hash<std::string>{}(key.tab_name_);
            h ^= std::hash<int>{}(key.page_no_) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<int>{}(key.slot_no_) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };

    struct SsiWriteEntry {
        txn_id_t txn_id_;
        std::string tab_name_;
        std::optional<Rid> rid_;
        std::optional<RmRecord> old_rec_;
        std::optional<RmRecord> new_rec_;
    };
    std::vector<SsiWriteEntry> ssi_writes_;

    struct SsiRecordReadEntry {
        txn_id_t txn_id_;
        std::string tab_name_;
        Rid rid_;
    };
    std::vector<SsiRecordReadEntry> ssi_record_reads_;

    std::unordered_set<txn_id_t> active_serializable_txns_;
    std::unordered_map<SsiRecordKey, std::unordered_set<txn_id_t>, SsiRecordKeyHash> active_record_readers_;
    std::unordered_map<txn_id_t, std::vector<SsiRecordKey>> txn_record_read_keys_;
    std::unordered_map<std::string, std::unordered_set<txn_id_t>> active_predicate_readers_by_table_;
    std::unordered_map<txn_id_t, std::vector<std::string>> txn_predicate_read_tables_;
    std::unordered_map<std::string, std::vector<SsiWriteEntry>> recent_writes_by_table_;
    uint64_t commits_since_full_ssi_prune_{0};

    // reader -> {writers} (rw anti-dependency edges)
    std::unordered_map<txn_id_t, std::unordered_set<txn_id_t>> rw_edges_;

    SsiRecordKey MakeSsiRecordKey(const std::string& tab_name, const Rid& rid) const;

    bool HasActiveSsiReadersForWriteUnlocked(const std::string& tab_name, const SsiRecordKey& key) const;
    bool HasOtherActivePredicateReadersUnlocked(const std::string& tab_name, txn_id_t writer) const;
    bool HasOtherActiveSerializableTxnUnlocked(txn_id_t writer) const;
    bool HasActiveSerializableOverlapUnlocked(Transaction* txn);

    size_t RecentWriteCountUnlocked() const;
    size_t RwEdgeCountUnlocked() const;

    void CleanupTxnReadIndexesUnlocked(txn_id_t txn_id);
    void CleanupTxnRwEdgesUnlocked(txn_id_t txn_id);
    void CleanupTxnRecentWritesUnlocked(txn_id_t txn_id);
    void CleanupTxnSsiState(txn_id_t txn_id);
    bool ShouldRunFullSsiPruneUnlocked() const;

    bool TransactionHasSsiStateUnlocked(txn_id_t txn_id) const;
    bool TransactionHasRetainedSsiStateUnlocked(txn_id_t txn_id) const;
    bool TransactionHasUndoNeededByVersionChain(Transaction* txn) const;
    bool CanRetireTransactionUnlocked(Transaction* txn) const;
    void RetireTransactionIfSafe(Transaction* txn);

    bool TransactionsOverlap(Transaction* lhs, Transaction* rhs);

    bool CommittedBefore(txn_id_t lhs, txn_id_t rhs);
    bool CommittedBeforeUnlocked(txn_id_t lhs, txn_id_t rhs);

    bool HasDangerousStructureUnlocked(txn_id_t current_txn);

    bool TupleMatches(const std::string& tab_name, const std::vector<Condition>& conds, const RmRecord& rec);

    bool AddRwEdgeInternal(txn_id_t reader, txn_id_t writer, txn_id_t current_txn);
};
