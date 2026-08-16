#pragma once

#include <array>
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
#include <tuple>
#include <vector>

#include "checkpoint_db.h"
#include "common/context.h"
#include "parser/ast.h"

namespace deltakernel {

enum class DeltaValueType : uint8_t { Int, Float, Char };
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
    std::optional<epoch_si_poc::EpochSiEngine::Txn> txn;
    bool explicit_txn = false;
    DeltaOverlay overlay;
    // Held for the lifetime of an explicit transaction so DDL/checkpoint/LOAD
    // cannot pass between its writes and durable commit.
    std::optional<std::shared_lock<std::shared_mutex>> admission;
};

class DeltaDatabase {
public:
    static std::unique_ptr<DeltaDatabase> Create(const std::string& directory);
    static std::unique_ptr<DeltaDatabase> Open(const std::string& directory);
    static bool IsDeltaDirectory(const std::string& directory);

    bool Execute(std::unique_ptr<ast::TreeNode> tree, DeltaSession& session, QueryResultSink* sink);
    uint64_t CatalogGeneration() const;
    PreparedDescription DescribePrepared(const ast::TreeNode& tree,
                                         const std::vector<DeltaValueType>& declared_parameters) const;
    void Abort(DeltaSession& session) noexcept;
    void Checkpoint();
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
    void SetCommitBatchHookForTest(std::function<void()> hook) {
        commit_batch_hook_for_test_ = std::move(hook);
    }
    void SetCommitInstallHookForTest(std::function<void()> hook) {
        commit_install_hook_for_test_ = std::move(hook);
    }
    size_t WalFrameCountForTest() const;
    size_t CommitQueueDepthForTest() const;
    std::array<size_t, 3> IndexProbeCensusForTest() const {
        return {last_overlay_nodes_probed_, last_row_ids_probed_, last_row_reads_probed_};
    }
    std::array<size_t, 5> JoinProbeCensusForTest() const {
        return {last_parameterized_join_probes_, last_join_inner_rows_resolved_, last_join_pairs_rechecked_,
                last_join_full_scan_rows_, last_join_right_rows_visited_};
    }
    size_t SidecarValidationCountForTest() const {
        return sidecar_validation_count_;
    }

private:
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
    struct SidecarEntry {
        uint64_t key_offset;
        uint64_t local_id;
    };
    struct SidecarBuildEntry {
        std::vector<uint8_t> key;
        uint64_t local_id;
    };
    struct SidecarDescriptor {
        uint64_t count;
        uint64_t key_bytes;
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
        epoch_si_poc::CommitResult result{};
        std::exception_ptr error;
        bool done = false;
        std::condition_variable ready;
    };
    void Commit(DeltaSession& session, std::unique_lock<std::mutex>& interpreter_lock);
    void DrainCommitQueue();
    static CommittedOverlay PrepareCommittedOverlay(const DeltaOverlay& overlay);
    void RunCommitInstallHookForTest() noexcept;
    void InstallCommittedOverlay(CommittedOverlay& overlay) noexcept;
    epoch_si_poc::RowImage EncodeRow(const TableSchema& schema, const std::vector<Cell>& cells) const;
    std::vector<Cell> DecodeRow(const TableSchema& schema, const epoch_si_poc::RowImage& image) const;
    std::vector<uint8_t> EncodeKey(const TableSchema& schema, const Index& index, const std::vector<Cell>& cells,
                                   size_t columns = std::numeric_limits<size_t>::max()) const;
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
    std::vector<epoch_si_poc::RowId> IndexedCandidates(const DeltaSession& session, const TableSchema& schema,
                                                       const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                                                       bool* usable) const;
    // Emits a forward-ordered, duplicate-free (encoded key, row id) union of the immutable sidecar and overlays.
    // An ordered/early-stop consumer must Read its snapshot/private row and require the visible row's EncodeKey to
    // equal the candidate key before treating the ordering as authoritative: overlay entries are add-only.
    void VisitIndexInterval(const DeltaSession& session, const TableSchema& schema, const Index& index,
                            const EncodedKey& first, const EncodedKey& last,
                            const std::function<void(const EncodedKey&, epoch_si_poc::RowId)>& visitor,
                            bool* usable) const;
    void VisitOverlayInterval(const DeltaSession& session, epoch_si_poc::TableId table_id,
                              epoch_si_poc::ConstraintId constraint_id, const EncodedKey& first, const EncodedKey& last,
                              const std::function<void(const EncodedKey&, epoch_si_poc::RowId)>& visitor) const;
    template <typename Overlay>
    void AddOverlay(Overlay& overlay, const TableSchema& schema, const std::vector<Cell>& cells, epoch_si_poc::RowId id,
                    const std::vector<Cell>* previous = nullptr);
    void VisitRows(DeltaSession& session, const TableSchema& schema,
                   const std::vector<std::unique_ptr<ast::BinaryExpr>>& conditions,
                   const std::function<void(epoch_si_poc::RowId, const epoch_si_poc::RowImage&)>& visitor,
                   bool* used_index = nullptr);

    std::string directory_;
    epoch_si_poc::CheckpointDb db_;
    Catalog tables_;
    std::map<epoch_si_poc::TableId, const TableSchema*> table_by_id_;
    epoch_si_poc::TableId next_table_id_ = 1;
    epoch_si_poc::ConstraintId next_constraint_id_ = 1;
    uint64_t catalog_generation_ = 1;
    bool fail_catalog_save_for_test_ = false;
    bool fail_catalog_post_rename_for_test_ = false;
    bool poisoned_ = false;
    std::function<void()> load_before_publish_hook_for_test_;
    std::function<void()> execute_lock_hook_for_test_;
    std::function<void()> execute_blocked_hook_for_test_;
    std::function<void()> commit_batch_hook_for_test_;
    std::function<void()> commit_install_hook_for_test_;
    mutable std::map<epoch_si_poc::ConstraintId, SidecarDescriptor> sidecars_;
    CommittedOverlay overlay_;
    size_t sidecar_validation_count_ = 0;
    mutable size_t last_overlay_nodes_probed_ = 0;
    mutable size_t last_row_ids_probed_ = 0;
    mutable size_t last_row_reads_probed_ = 0;
    size_t last_parameterized_join_probes_ = 0;
    size_t last_join_inner_rows_resolved_ = 0;
    size_t last_join_pairs_rechecked_ = 0;
    size_t last_join_full_scan_rows_ = 0;
    size_t last_join_right_rows_visited_ = 0;
    std::shared_mutex execution_gate_;
    mutable std::mutex commit_mutex_;
    std::deque<std::shared_ptr<CommitTicket>> commit_queue_;
    std::condition_variable commit_slot_available_;
    bool commit_leader_ = false;
    mutable std::mutex mutex_; // ponytail: one interpreter lock; split only when measured contention requires it.
};

} // namespace deltakernel
