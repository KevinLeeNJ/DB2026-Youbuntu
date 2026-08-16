#pragma once

#include "epoch_si_engine.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace deltakernel {
class DeltaDatabase;
class DeltaSnapshotTestPeer;
} // namespace deltakernel

namespace epoch_si_poc {

enum class CheckpointCrashPoint {
    kNone,
    kDuringBaseTemp,
    kAfterBaseRename,
    kAfterWalCreate,
    kBeforeNextEngineOpen,
    kDuringManifestTemp,
    kAfterManifestRenameBeforeDirSync,
    kAfterSuccess,
    kRotationAfterHeaderWrite,
    kRotationAfterHeaderSync,
    kRotationAfterDirSync,
    kRotationDuringManifestTemp,
    kRotationAfterManifestRenameBeforeDirSync,
    kRotationAfterSwitch,
    kSnapshotAfterManifestDurable,
    kSnapshotBeforeSourceInstall,
    kSnapshotAfterSourceInstall
};

class TableBaseWriter {
public:
    ~TableBaseWriter();
    TableBaseWriter(TableBaseWriter&&) noexcept;
    TableBaseWriter& operator=(TableBaseWriter&&) noexcept;
    TableBaseWriter(const TableBaseWriter&) = delete;
    TableBaseWriter& operator=(const TableBaseWriter&) = delete;
    void Append(RowImage row);
    uint64_t row_count() const;

private:
    friend class CheckpointDb;
    struct Impl;
    explicit TableBaseWriter(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

class CheckpointDb {
public:
    ~CheckpointDb();
    CheckpointDb(CheckpointDb&&) noexcept;
    CheckpointDb& operator=(CheckpointDb&&) noexcept;
    CheckpointDb(const CheckpointDb&) = delete;
    CheckpointDb& operator=(const CheckpointDb&) = delete;
    struct TableRef {
        TableId table_id = 0;
        uint64_t file_generation = 0;
        Epoch visible_from = 0;
        uint64_t row_count = 0;
        uint64_t file_bytes = 0;
        uint64_t next_local_id = 0;
    };

    static CheckpointDb Create(const std::string& directory, BaseImage initial_rows);
    static CheckpointDb Open(const std::string& directory);

    EpochSiEngine& engine() {
        RequireUsable();
        return engine_;
    }
    const EpochSiEngine& engine() const {
        RequireUsable();
        return engine_;
    }
    uint64_t generation() const {
        RequireUsable();
        return generation_;
    }
    std::optional<uint64_t> TableGeneration(TableId table_id) const {
        RequireUsable();
        const auto found = tables_.find(table_id);
        return found == tables_.end() ? std::nullopt : std::optional<uint64_t>(found->second.file_generation);
    }
    std::optional<Epoch> TableVisibleFrom(TableId table_id) const {
        RequireUsable();
        const auto found = tables_.find(table_id);
        return found == tables_.end() ? std::nullopt : std::optional<Epoch>(found->second.visible_from);
    }
    Epoch base_epoch() const {
        RequireUsable();
        return base_epoch_;
    }
    size_t durable_wal_bytes() const;

    TableBaseWriter BeginTableBase(TableId table_id);
    void PublishTableBase(TableBaseWriter&& writer);
    void OfflineCheckpoint();
    void SetCrashPointForTest(CheckpointCrashPoint point) {
        RequireUsable();
        crash_point_ = point;
    }
    size_t immutable_table_count() const {
        return engine_.immutable_table_count();
    }
    size_t immutable_index_bytes() const {
        return engine_.immutable_index_bytes();
    }
    size_t wal_open_directory_syncs_for_test() const {
        return wal_open_directory_syncs_;
    }

    struct SnapshotCutBoundary {
        Epoch epoch = 0;
        uint64_t next_commit_seq = 1;
        uint64_t wal_lineage = 0;
        uint64_t new_active_segment_id = 0;
    };

private:
    class SnapshotArtifact {
    public:
        SnapshotArtifact() = default;
        ~SnapshotArtifact();
        SnapshotArtifact(const SnapshotArtifact&) = delete;
        SnapshotArtifact& operator=(const SnapshotArtifact&) = delete;
        SnapshotArtifact(SnapshotArtifact&&) noexcept;
        SnapshotArtifact& operator=(SnapshotArtifact&&) noexcept;

    private:
        friend class CheckpointDb;
        friend class SnapshotCursorTestPeer;
        friend class deltakernel::DeltaDatabase;
        friend class deltakernel::DeltaSnapshotTestPeer;
        std::vector<TableRef> tables_;
        std::vector<TableBaseWriter> writers_;
    };
    class PreparedSnapshotArtifacts {
    public:
        PreparedSnapshotArtifacts() = default;
        ~PreparedSnapshotArtifacts() = default;
        PreparedSnapshotArtifacts(const PreparedSnapshotArtifacts&) = delete;
        PreparedSnapshotArtifacts& operator=(const PreparedSnapshotArtifacts&) = delete;
        PreparedSnapshotArtifacts(PreparedSnapshotArtifacts&&) noexcept;
        PreparedSnapshotArtifacts& operator=(PreparedSnapshotArtifacts&&) noexcept;

    private:
        friend class CheckpointDb;
        friend class SnapshotCursorTestPeer;
        friend class deltakernel::DeltaDatabase;
        friend class deltakernel::DeltaSnapshotTestPeer;
        void KeepPublishedFiles() noexcept;
        void UnkeepPublishedFiles() noexcept;
        void MarkPublished() noexcept;
        std::vector<TableRef> tables_;
        std::vector<TableBaseWriter> writers_;
        ImmutableTables readers_;
    };
    class PreparedSnapshotPublication {
    public:
        PreparedSnapshotPublication() = default;
        ~PreparedSnapshotPublication() = default;
        PreparedSnapshotPublication(const PreparedSnapshotPublication&) = delete;
        PreparedSnapshotPublication& operator=(const PreparedSnapshotPublication&) = delete;
        PreparedSnapshotPublication(PreparedSnapshotPublication&&) noexcept;
        PreparedSnapshotPublication& operator=(PreparedSnapshotPublication&&) noexcept;

    private:
        friend class CheckpointDb;
        friend class deltakernel::DeltaDatabase;
        PreparedSnapshotArtifacts artifacts_;
        EpochSiEngine::PreparedSourcePublication source_;
        std::map<TableId, TableRef> tables_;
        std::vector<uint8_t> manifest_bytes_;
        uint64_t generation_ = 0;
        Epoch base_epoch_ = 0;
        uint64_t base_next_commit_seq_ = 1;
        uint64_t wal_lineage_ = 0;
        uint64_t first_segment_id_ = 0;
        uint64_t active_segment_id_ = 0;
    };
    struct WalChain;
    friend class deltakernel::DeltaDatabase;
    friend class SnapshotCursorTestPeer;
    friend class deltakernel::DeltaSnapshotTestPeer;
    CheckpointDb(std::string directory, uint64_t generation, uint64_t wal_generation, Epoch base_epoch,
                 std::map<TableId, TableRef> tables, EpochSiEngine engine, size_t wal_open_directory_syncs);
    SnapshotCutBoundary RotateWalAtGate(CheckpointCrashPoint point);
    TableBaseWriter BeginSnapshotTableBase(TableId table_id, Epoch visible_from);
    void AppendSnapshotRows(TableBaseWriter& writer, std::vector<std::pair<RowId, Row>> rows);
    void FinishSnapshotTableBase(TableBaseWriter& writer, uint64_t next_local_id, SnapshotArtifact& artifact);
    PreparedSnapshotArtifacts AdoptForPublication(SnapshotArtifact&& artifact);
    PreparedSnapshotPublication PrepareSnapshotPublication(const SnapshotCutBoundary& boundary,
                                                           uint64_t expected_source_identity,
                                                           PreparedSnapshotArtifacts&& artifacts);
    void PublishSnapshotPublication(PreparedSnapshotPublication&& publication);
    void GarbageCollectExcludedWal() noexcept;
    void GarbageCollectExcludedTables() noexcept;
    void RequireUsable() const;

    std::string directory_;
    uint64_t generation_;
    uint64_t wal_generation_;
    std::unique_ptr<WalChain> wal_chain_;
    Epoch base_epoch_;
    std::map<TableId, TableRef> tables_;
    EpochSiEngine engine_;
    size_t wal_open_directory_syncs_;
    size_t sealed_wal_bytes_ = 0;
    CheckpointCrashPoint crash_point_ = CheckpointCrashPoint::kNone;
    bool poisoned_ = false;
};

} // namespace epoch_si_poc
