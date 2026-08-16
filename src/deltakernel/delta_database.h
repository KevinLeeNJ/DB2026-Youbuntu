#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <shared_mutex>
#include <set>
#include <tuple>
#include <vector>

#include "checkpoint_db.h"
#include "common/context.h"
#include "parser/ast.h"

namespace deltakernel {

enum class DeltaValueType : uint8_t { Int, Float, Char };
struct DeltaParameter {
    DeltaValueType type = DeltaValueType::Int;
    bool present = false;
    int32_t integer = 0;
    uint32_t float_bits = 0;
    std::string text;
};
using DeltaParameterFrame = std::vector<DeltaParameter>;

class DeltaPreparedProgram {
public:
    DeltaPreparedProgram(std::unique_ptr<ast::TreeNode> tree, bool query, uint64_t catalog_generation)
        : tree_(std::move(tree)), query_(query), catalog_generation_(catalog_generation) {}
    DeltaPreparedProgram(const DeltaPreparedProgram&) = delete;
    DeltaPreparedProgram& operator=(const DeltaPreparedProgram&) = delete;

    bool query() const noexcept {
        return query_;
    }
    uint64_t catalog_generation() const noexcept {
        return catalog_generation_;
    }

private:
    friend class DeltaDatabase;
    std::unique_ptr<ast::TreeNode> tree_;
    bool query_ = false;
    uint64_t catalog_generation_ = 0;
};

struct PreparedDescription {
    bool query = false;
    std::vector<std::string> names;
    std::vector<DeltaValueType> types;
    uint64_t catalog_generation = 0;
};

class DeltaTransactionAbort : public std::runtime_error {
public:
    explicit DeltaTransactionAbort(const std::string& message) : std::runtime_error(message) {}
};

using EncodedKey = std::vector<uint8_t>;
using DeltaOverlayKey = std::tuple<epoch_si_poc::TableId, epoch_si_poc::ConstraintId, EncodedKey>;
using DeltaOverlay = std::map<DeltaOverlayKey, std::vector<uint64_t>>;
using CommittedOverlay = std::multimap<DeltaOverlayKey, std::vector<uint64_t>>;

struct DeltaSession {
    DeltaSession() = default;
    ~DeltaSession() = default;
    DeltaSession(const DeltaSession&) = delete;
    DeltaSession& operator=(const DeltaSession&) = delete;
    DeltaSession(DeltaSession&&) = delete;
    DeltaSession& operator=(DeltaSession&&) = delete;

    std::optional<epoch_si_poc::EpochSiEngine::Txn> txn;
    bool explicit_txn = false;
    DeltaOverlay overlay;
    DeltaOverlay removed_overlay;
    std::set<epoch_si_poc::RowId> private_insert_ids;
    // Held for the lifetime of an explicit transaction so DDL/checkpoint/LOAD
    // cannot pass between its writes and durable commit.
    std::optional<std::shared_lock<std::shared_mutex>> admission;

private:
    friend class DeltaDatabase;
    struct QueryCensus {
        size_t last_overlay_nodes_probed = 0;
        size_t last_row_ids_probed = 0;
        size_t last_row_reads_probed = 0;
        size_t last_base_entries_probed = 0;
        size_t last_live_summary_words_probed = 0;
        size_t last_overlay_refs_examined = 0;
        size_t last_overlay_refs_copied = 0;
        size_t last_overlay_order_ops = 0;
        size_t last_sidecar_query_opens = 0;
        size_t last_sidecar_query_preads = 0;
        size_t last_sidecar_binary_comparisons = 0;
        size_t last_parameterized_join_probes = 0;
        size_t last_join_inner_rows_resolved = 0;
        size_t last_join_pairs_rechecked = 0;
        size_t last_join_full_scan_rows = 0;
        size_t last_join_right_rows_visited = 0;
        size_t last_join_base_entries_examined = 0;
        size_t last_join_overlay_entries_examined = 0;
        size_t last_join_overlay_refs_examined = 0;
        size_t last_join_outer_indexed_queries = 0;
        size_t last_join_outer_candidates = 0;
        size_t last_join_outer_full_scan_rows = 0;
        size_t last_join_inferred_literal_components = 0;
        size_t last_join_inferred_literal_probes = 0;
        size_t last_ordered_candidates_examined = 0;
        size_t last_ordered_rows_decoded = 0;
        size_t last_ordered_sort_input_rows = 0;
        size_t last_ordered_early_stops = 0;
    };
    // The public C++ API permits Abort and Execute from different threads; one session remains a sequential stream.
    mutable std::mutex operation_mutex;
    QueryCensus census;
};

class DeltaDatabase {
public:
    ~DeltaDatabase();
    static std::unique_ptr<DeltaDatabase> Create(const std::string& directory);
    static std::unique_ptr<DeltaDatabase> Open(const std::string& directory);
    static bool IsDeltaDirectory(const std::string& directory);

    bool Execute(std::unique_ptr<ast::TreeNode> tree, DeltaSession& session, QueryResultSink* sink);
    bool ExecutePrepared(const DeltaPreparedProgram& program, const DeltaParameterFrame& parameters,
                         DeltaSession& session, QueryResultSink* sink);
    std::unique_ptr<DeltaPreparedProgram> CompilePrepared(std::unique_ptr<ast::TreeNode> tree,
                                                          const std::vector<DeltaValueType>& declared_parameters) const;
    uint64_t CatalogGeneration() const;
    PreparedDescription DescribePrepared(const ast::TreeNode& tree,
                                         const std::vector<DeltaValueType>& declared_parameters) const;
    void Abort(DeltaSession& session) noexcept;
    void Checkpoint();
    bool DiagnosticsEnabled() const noexcept {
        return diagnostics_ != nullptr;
    }
    void RecordPreparedClone(uint64_t elapsed_ns) noexcept;
    void RecordPreparedNative() noexcept;
    void RecordPreparedFallback() noexcept;
    // Test controls are configured only while no database operation is running.
    void SetCatalogSaveFailureForTest(bool fail) {
        fail_catalog_save_for_test_ = fail;
    }
    void SetCatalogPostRenameFailureForTest(bool fail) {
        fail_catalog_post_rename_for_test_ = fail;
    }
    void SetLoadBeforePublishHookForTest(std::function<void()> hook) {
        load_before_publish_hook_for_test_ = std::move(hook);
    }
    void SetExecuteLockHookForTest(std::function<void()> hook) {
        execute_lock_hook_for_test_ = std::move(hook);
    }
    void SetExecuteBlockedHookForTest(std::function<void()> hook) {
        execute_blocked_hook_for_test_ = std::move(hook);
    }
    void SetExecutionWriterWaitHookForTest(std::function<void()> hook) {
        execution_writer_wait_hook_for_test_ = std::move(hook);
    }
    void SetStateWriterWaitHookForTest(std::function<void()> hook) {
        state_writer_wait_hook_for_test_ = std::move(hook);
    }
    void SetAbortLockAttemptHookForTest(std::function<void()> hook) {
        abort_lock_attempt_hook_for_test_ = std::move(hook);
    }
    void SetCommitBatchHookForTest(std::function<void()> hook) {
        commit_batch_hook_for_test_ = std::move(hook);
    }
    void SetCommitEnqueueFailureForTest(bool fail) {
        fail_commit_enqueue_for_test_ = fail;
    }
    void SetCommitInstallHookForTest(std::function<void()> hook) {
        commit_install_hook_for_test_ = std::move(hook);
    }
    void SetCommitSyncHookForTest(std::function<void()> hook) {
        commit_sync_hook_for_test_ = std::move(hook);
    }
    void SetCommitReacquireHookForTest(std::function<void()> hook) {
        commit_reacquire_hook_for_test_ = std::move(hook);
    }
    void CloseWalForTest();
    std::array<uint64_t, 4> RotateWalForTest();
    void SetWalRotationCrashPointForTest(epoch_si_poc::CheckpointCrashPoint point) {
        rotation_crash_point_for_test_ = point;
    }
    void EnableDiagnosticsForTest();
    std::array<uint64_t, 6> QueryDiagnosticsForTest() const;
    std::array<uint64_t, 5> RouteDiagnosticsForTest() const;
    std::array<uint64_t, 8> TailDiagnosticsForTest() const;
    std::array<uint64_t, 5> InflightDiagnosticsForTest() const;
    std::array<uint64_t, 6> ConcurrencyDiagnosticsForTest() const;
    size_t WalFrameCountForTest() const;
    size_t CommitQueueDepthForTest() const;
    std::array<size_t, 3> IndexProbeCensusForTest(const DeltaSession& session) const;
    std::array<size_t, 8> JoinProbeCensusForTest(const DeltaSession& session) const;
    std::array<size_t, 5> JoinOuterProbeCensusForTest(const DeltaSession& session) const;
    std::array<uint64_t, 5> JoinOuterDiagnosticsForTest() const;
    std::array<size_t, 8> OrderedProbeCensusForTest(const DeltaSession& session) const;
    size_t LiveSummaryProbeCensusForTest(const DeltaSession& session) const;
    std::array<size_t, 4> SidecarIoCensusForTest(const DeltaSession& session) const;
    std::array<size_t, 3> CurrentIndexCensusForTest() const;
    size_t SidecarValidationCountForTest() const;
    std::optional<uint64_t> SidecarLiveTailForTest(epoch_si_poc::ConstraintId constraint_id) const;

private:
    bool ExecuteImpl(const ast::TreeNode* tree, DeltaSession& session, QueryResultSink* sink,
                     uint64_t expected_catalog_generation = 0);
    enum class ColumnType : uint8_t { Int, Float, Char };
    struct Column {
        std::string name;
        ColumnType type;
        uint32_t length;
        bool nullable;
    };
    struct Index {
        epoch_si_poc::ConstraintId constraint_id;
        std::vector<uint32_t> columns;
        bool unique;
    };
    struct TableSchema {
        epoch_si_poc::TableId id;
        uint32_t version;
        std::string name;
        std::vector<Column> columns;
        std::vector<Index> indexes;
    };
    struct Cell {
        bool is_null = true;
        int32_t integer = 0;
        float floating = 0;
        std::string text;
    };
    using Catalog = std::map<std::string, TableSchema>;
    struct SidecarBuildEntry {
        std::vector<uint8_t> key;
        uint64_t local_id;
    };
    struct SidecarDescriptor {
        epoch_si_poc::TableId table_id = 0;
        epoch_si_poc::ConstraintId constraint_id = 0;
        uint64_t generation = 0;
        epoch_si_poc::Epoch snapshot_epoch = 0;
        uint64_t count = 0;
        uint64_t key_bytes = 0;
        uint64_t row_order_offset = 0;
        uint64_t max_local_id = 0;
        size_t mapped_bytes = 0;
        void* mapping = nullptr;
        // Derived current-state state. It is intentionally not persisted with the immutable sidecar.
        std::vector<uint64_t> live_bitmap;
        std::vector<std::vector<uint64_t>> live_summary;

        SidecarDescriptor() = default;
        SidecarDescriptor(epoch_si_poc::TableId table_id, epoch_si_poc::ConstraintId constraint_id, uint64_t generation,
                          epoch_si_poc::Epoch snapshot_epoch, uint64_t count, uint64_t key_bytes,
                          uint64_t row_order_offset, size_t mapped_bytes, void* mapping);
        ~SidecarDescriptor();
        SidecarDescriptor(const SidecarDescriptor&) = delete;
        SidecarDescriptor& operator=(const SidecarDescriptor&) = delete;
        SidecarDescriptor(SidecarDescriptor&& other) noexcept;
        SidecarDescriptor& operator=(SidecarDescriptor&& other) noexcept;
    };
    struct OrderedIndexAccess {
        const Index* index;
        EncodedKey first;
        EncodedKey last;
    };

    explicit DeltaDatabase(epoch_si_poc::CheckpointDb db);
    void RequireUsable() const;
    void AbortLocked(DeltaSession& session) noexcept;
    void SaveCatalog(const Catalog& tables, epoch_si_poc::TableId next_table_id,
                     epoch_si_poc::ConstraintId next_constraint_id, uint64_t catalog_generation);
    void LoadCatalog();
    const TableSchema& Table(const std::string& name) const;
    const TableSchema* TableById(epoch_si_poc::TableId id) const;
    epoch_si_poc::EpochSiEngine::Txn& Txn(DeltaSession& session);
    struct CommitTicket {
        std::optional<epoch_si_poc::EpochSiEngine::Txn> txn;
        CommittedOverlay overlay;
        CommittedOverlay active_additions;
        CommittedOverlay active_removals;
        epoch_si_poc::CommitResult result{};
        std::exception_ptr error;
        bool done = false;
        std::condition_variable ready;
    };
    struct ExecutionCensus {
        bool captured = false;
        size_t base_entries = 0;
        size_t overlay_refs = 0;
        size_t parameterized_join_probes = 0;
        bool join_parameterized = false;
        bool join_fallback = false;
        size_t join_outer_candidates = 0;
        size_t join_outer_full_scan_rows = 0;
        size_t join_inferred_literal_components = 0;
        size_t join_inferred_literal_probes = 0;
        bool join_outer_indexed = false;
        size_t ordered_early_stops = 0;
        bool ordered_stream = false;
        bool ordered_materialize = false;
    };
    struct IndexEqualities {
        std::vector<std::optional<Cell>> values;
        std::vector<bool> inferred;
    };
    void Commit(DeltaSession& session, std::shared_lock<std::shared_mutex>& state_lock,
                ExecutionCensus* census = nullptr);
    void CaptureQueryCensus(const DeltaSession& session, ExecutionCensus& census) const;
    void DrainCommitQueue();
    static CommittedOverlay PrepareCommittedOverlay(const DeltaOverlay& overlay);
    void RunCommitInstallHookForTest() noexcept;
    void InstallCommittedOverlay(CommittedOverlay& overlay) noexcept;
    void InstallCurrentOverlay(CommittedOverlay& additions, CommittedOverlay& removals) noexcept;
    void ApplyCurrentBaseState(const TableSchema& schema, epoch_si_poc::ConstraintId constraint_id,
                               const EncodedKey& key, uint64_t local_id, bool live) noexcept;
    void ClearCurrentBaseRow(const TableSchema& schema, uint64_t local_id) noexcept;
    void SetCurrentBaseBit(SidecarDescriptor& descriptor, uint64_t local_id, bool live) noexcept;
    epoch_si_poc::RowImage EncodeRow(const TableSchema& schema, const std::vector<Cell>& cells) const;
    std::vector<Cell> DecodeRow(const TableSchema& schema, const epoch_si_poc::RowImage& image) const;
    std::vector<uint8_t> EncodeKey(const TableSchema& schema, const Index& index, const std::vector<Cell>& cells,
                                   size_t columns = std::numeric_limits<size_t>::max()) const;
    std::string DistinctKey(const Column& column, const Cell& cell) const;
    Cell Literal(const Column& column, const ast::Value& value) const;
    bool Matches(const TableSchema& schema, const std::vector<Cell>& cells,
                 const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions) const;
    void EmitRows(const TableSchema& schema, const ast::SelectStmt& select, const std::vector<std::vector<Cell>>& rows,
                  QueryResultSink* sink, bool aggregate_values = false, bool query_started = false) const;
    void EmitCells(const std::vector<Column>& columns, const std::vector<std::vector<Cell>>& rows,
                   QueryResultSink* sink) const;
    void EmitTables(QueryResultSink* sink) const;
    void LoadCsv(const ast::LoadStmt& load, DeltaSession& session);
    void BuildSidecars(const TableSchema& schema, std::vector<std::vector<SidecarBuildEntry>> entries,
                       uint64_t generation);
    bool ValidateSidecar(const TableSchema& schema, const Index& index);
    void RebuildSidecars(const TableSchema& schema);
    void CheckpointSidecars();
    std::vector<epoch_si_poc::RowId> IndexedCandidates(DeltaSession& session, const TableSchema& schema,
                                                       const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                                                       bool* usable, const IndexEqualities* equalities = nullptr,
                                                       size_t* inferred_used = nullptr) const;
    // Emits a forward-ordered, duplicate-free (encoded key, row id) union of the immutable sidecar and overlays.
    // An ordered/early-stop consumer must Read its snapshot/private row and require the visible row's EncodeKey to
    // equal the candidate key before treating the ordering as authoritative: overlay entries are add-only.
    void VisitIndexInterval(DeltaSession& session, const TableSchema& schema, const Index& index,
                            const EncodedKey& first, const EncodedKey& last,
                            const std::function<void(const EncodedKey&, epoch_si_poc::RowId)>& visitor,
                            bool* usable) const;
    void VisitOrderedIndexInterval(DeltaSession& session, const TableSchema& schema, const Index& index,
                                   const EncodedKey& first, const EncodedKey& last, bool reverse,
                                   const std::function<bool(const EncodedKey&, epoch_si_poc::RowId)>& visitor,
                                   bool* usable) const;
    std::optional<OrderedIndexAccess>
    FindOrderedIndexAccess(const TableSchema& schema, const std::string& alias,
                           const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                           const std::vector<size_t>& ordered_columns) const;
    template <typename Overlay>
    void AddOverlay(Overlay& overlay, const TableSchema& schema, const std::vector<Cell>& cells, epoch_si_poc::RowId id,
                    const std::vector<Cell>* previous = nullptr);
    void RemoveOverlay(DeltaOverlay& overlay, const TableSchema& schema, const std::vector<Cell>& cells,
                       epoch_si_poc::RowId id);
    void RemoveOverlay(DeltaOverlay& overlay, const TableSchema& schema, const Index& index,
                       const std::vector<Cell>& cells, epoch_si_poc::RowId id);
    void VisitRows(DeltaSession& session, const TableSchema& schema,
                   const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                   const std::function<void(epoch_si_poc::RowId, const epoch_si_poc::RowImage&)>& visitor,
                   bool* used_index = nullptr, const IndexEqualities* equalities = nullptr,
                   size_t* inferred_used = nullptr);
    void ReportDiagnostics(bool periodic = false) const noexcept;
    void MaybeReportDiagnostics() const noexcept;
    std::shared_lock<std::shared_mutex> LockExecutionShared();
    std::unique_lock<std::shared_mutex> LockExecutionUnique();
    std::shared_lock<std::shared_mutex> LockStateShared(bool report_blocked = false) const;
    std::unique_lock<std::shared_mutex> LockStateUnique() const;

    std::string directory_;
    std::shared_ptr<epoch_si_poc::DeltaDiagnostics> diagnostics_;
    bool report_diagnostics_ = false;
    std::array<std::atomic<uint64_t>, 5> execute_calls_{};
    std::array<std::atomic<uint64_t>, 5> execute_ns_{};
    std::array<std::atomic<uint64_t>, 5> execute_max_ns_{};
    std::array<std::atomic<uint64_t>, 5> execute_inflight_{};
    std::atomic<uint64_t> execute_shared_wait_ns_{0};
    std::atomic<uint64_t> execute_shared_wait_max_ns_{0};
    std::atomic<uint64_t> execute_shared_calls_{0};
    std::atomic<uint64_t> state_unique_commit_wait_ns_{0};
    std::atomic<uint64_t> state_unique_commit_batches_{0};
    mutable std::atomic<uint64_t> writer_turnstile_wait_ns_{0};
    std::atomic<uint64_t> inflight_execute_{0};
    std::atomic<uint64_t> peak_inflight_execute_{0};
    std::atomic<uint64_t> commit_queue_wait_ns_{0};
    std::atomic<uint64_t> commit_ready_wait_ns_{0};
    std::atomic<uint64_t> commit_ready_wait_max_ns_{0};
    std::atomic<uint64_t> commit_leader_reacquire_wait_ns_{0};
    std::atomic<uint64_t> commit_initial_unique_wait_max_ns_{0};
    std::atomic<uint64_t> commit_publish_unique_wait_max_ns_{0};
    std::atomic<uint64_t> commit_prepare_unique_ns_{0};
    std::atomic<uint64_t> commit_prepare_unique_max_ns_{0};
    std::atomic<uint64_t> commit_sync_unlocked_ns_{0};
    std::atomic<uint64_t> commit_sync_unlocked_max_ns_{0};
    std::atomic<uint64_t> commit_reacquire_ns_{0};
    std::atomic<uint64_t> commit_publish_unique_ns_{0};
    std::atomic<uint64_t> commit_publish_unique_max_ns_{0};
    // Diagnostics-only leader phase: 0 idle, 1 initial unique wait, 2 prepare,
    // 3 WAL sync, 4 publish unique wait, 5 publish/install.
    std::atomic<uint64_t> commit_phase_{0};
    uint64_t diagnostics_period_ns_ = 0;
    mutable std::atomic<uint64_t> diagnostics_last_report_ns_{0};
    std::atomic<uint64_t> prepared_clone_count_{0};
    std::atomic<uint64_t> prepared_clone_ns_{0};
    std::atomic<uint64_t> prepared_native_count_{0};
    std::atomic<uint64_t> prepared_fallback_count_{0};
    std::atomic<uint64_t> sidecar_base_entries_{0};
    std::atomic<uint64_t> sidecar_overlay_refs_{0};
    std::atomic<uint64_t> join_parameterized_probes_{0};
    std::atomic<uint64_t> join_parameterized_{0};
    std::atomic<uint64_t> join_fallback_{0};
    std::atomic<uint64_t> join_outer_indexed_{0};
    std::atomic<uint64_t> join_outer_candidates_{0};
    std::atomic<uint64_t> join_outer_full_scan_rows_{0};
    std::atomic<uint64_t> join_inferred_literal_components_{0};
    std::atomic<uint64_t> join_inferred_literal_probes_{0};
    std::atomic<uint64_t> ordered_early_stop_{0};
    std::atomic<uint64_t> ordered_stream_{0};
    std::atomic<uint64_t> ordered_materialize_{0};
    epoch_si_poc::CheckpointDb db_;
    Catalog tables_;
    std::map<epoch_si_poc::TableId, const TableSchema*> table_by_id_;
    epoch_si_poc::TableId next_table_id_ = 1;
    epoch_si_poc::ConstraintId next_constraint_id_ = 1;
    uint64_t catalog_generation_ = 1;
    bool fail_catalog_save_for_test_ = false;
    bool fail_catalog_post_rename_for_test_ = false;
    bool fail_commit_enqueue_for_test_ = false;
    std::atomic<bool> poisoned_{false};
    std::function<void()> load_before_publish_hook_for_test_;
    std::function<void()> execute_lock_hook_for_test_;
    std::function<void()> execute_blocked_hook_for_test_;
    std::function<void()> execution_writer_wait_hook_for_test_;
    std::function<void()> state_writer_wait_hook_for_test_;
    std::function<void()> abort_lock_attempt_hook_for_test_;
    std::function<void()> commit_batch_hook_for_test_;
    std::function<void()> commit_install_hook_for_test_;
    std::function<void()> commit_sync_hook_for_test_;
    std::function<void()> commit_reacquire_hook_for_test_;
    epoch_si_poc::CheckpointCrashPoint rotation_crash_point_for_test_ = epoch_si_poc::CheckpointCrashPoint::kNone;
    mutable std::map<epoch_si_poc::ConstraintId, SidecarDescriptor> sidecars_;
    CommittedOverlay overlay_;
    CommittedOverlay current_overlay_;
    std::atomic<uint64_t> current_base_row_order_comparisons_{0};
    size_t sidecar_validation_count_ = 0;
    mutable std::mutex execution_turnstile_;
    std::shared_mutex execution_gate_;
    mutable std::mutex commit_mutex_;
    mutable std::mutex commit_epoch_gate_;
    std::deque<std::shared_ptr<CommitTicket>> commit_queue_;
    std::condition_variable commit_slot_available_;
    bool commit_leader_ = false;
    mutable std::mutex state_turnstile_;
    mutable std::shared_mutex state_mutex_;
};

} // namespace deltakernel
