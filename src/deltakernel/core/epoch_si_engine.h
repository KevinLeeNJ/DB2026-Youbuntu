#pragma once

#include <cstddef>
#include <cstdint>
#include <array>
#include <atomic>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <vector>

namespace epoch_si_poc {

class FileWal;
class CheckpointDb;
class ImmutableTable;

using Epoch = uint64_t;
using TableId = uint32_t;
using ConstraintId = uint32_t;

struct RowId {
    TableId table_id = 0;
    uint64_t local_id = 0;

    bool operator<(const RowId& other) const {
        return table_id != other.table_id ? table_id < other.table_id : local_id < other.local_id;
    }
    bool operator==(const RowId& other) const {
        return table_id == other.table_id && local_id == other.local_id;
    }
    bool operator!=(const RowId& other) const {
        return !(*this == other);
    }
};

struct ConstraintClaim {
    ConstraintId constraint_id = 0;
    std::vector<uint8_t> bytes;

    bool operator<(const ConstraintClaim& other) const {
        return constraint_id != other.constraint_id ? constraint_id < other.constraint_id : bytes < other.bytes;
    }
    bool operator==(const ConstraintClaim& other) const {
        return constraint_id == other.constraint_id && bytes == other.bytes;
    }
};

// The durable row payload deliberately has no schema or type knowledge.  Claims are
// separate because only declared constraints participate in SI certification.
struct RowImage {
    std::vector<uint8_t> bytes;
    std::vector<ConstraintClaim> claims;
    bool deleted = false;

    bool operator==(const RowImage& other) const {
        return bytes == other.bytes && claims == other.claims && deleted == other.deleted;
    }
};

using Row = RowImage;

using BaseImage = std::map<RowId, Row>;
using ImmutableTables = std::map<TableId, std::shared_ptr<const ImmutableTable>>;

// Installed only when RMDB_DELTA_DIAGNOSTICS is set.  The normal path keeps this
// pointer null, so it performs no counter writes.
struct DeltaDiagnostics {
    std::atomic<uint64_t> immutable_reads{0};
    std::atomic<uint64_t> immutable_bytes{0};
    std::atomic<uint64_t> immutable_pread_ns{0};
    std::atomic<uint64_t> immutable_decode_ns{0};
    std::atomic<uint64_t> immutable_scan_calls{0};
    std::atomic<uint64_t> immutable_scan_rows{0};
    std::atomic<uint64_t> immutable_scan_bytes{0};
    std::atomic<uint64_t> immutable_scan_pread_calls{0};
    std::atomic<uint64_t> immutable_scan_pread_ns{0};
    std::atomic<uint64_t> immutable_scan_decode_calls{0};
    std::atomic<uint64_t> immutable_scan_decode_ns{0};
    std::atomic<uint64_t> scan_version_entries_examined{0};
    std::atomic<uint64_t> scan_private_entries_examined{0};
    std::atomic<uint64_t> private_hits{0};
    std::atomic<uint64_t> version_hits{0};
    std::atomic<uint64_t> base_hits{0};
    std::atomic<uint64_t> wal_pwrite_calls{0};
    std::atomic<uint64_t> wal_pwrite_bytes{0};
    std::atomic<uint64_t> wal_pwrite_ns{0};
    std::atomic<uint64_t> wal_fdatasync_calls{0};
    std::atomic<uint64_t> wal_fdatasync_ns{0};
    std::atomic<uint64_t> wal_fdatasync_max_ns{0};
    std::atomic<uint64_t> commit_encode_ns{0};
    std::atomic<uint64_t> commit_prepare_ns{0};
    std::atomic<uint64_t> commit_install_ns{0};
    std::atomic<uint64_t> commit_publish_ns{0};
    std::atomic<uint64_t> commit_frames{0};
    std::atomic<uint64_t> commit_tickets{0};
    // These counters describe the latest-snapshot sidecar accelerator only.
    // The immutable base and historical overlay remain the authority for old snapshots.
    std::atomic<uint64_t> current_index_latest_routes{0};
    std::atomic<uint64_t> current_index_historical_routes{0};
    std::atomic<uint64_t> current_live_base_candidates{0};
    std::atomic<uint64_t> current_live_summary_skips{0};
    std::atomic<uint64_t> current_live_summary_words{0};
    std::atomic<uint64_t> current_overlay_addition_ids{0};
    std::atomic<uint64_t> current_overlay_removal_ids{0};
    std::atomic<uint64_t> current_base_bit_flips{0};
    std::array<std::atomic<uint64_t>, 33> commit_batch_hist{};
};

class ImmutableTable {
public:
    ~ImmutableTable();
    ImmutableTable(const ImmutableTable&) = delete;
    ImmutableTable& operator=(const ImmutableTable&) = delete;

    TableId table_id() const {
        return table_id_;
    }
    Epoch visible_from() const {
        return visible_from_;
    }
    uint64_t row_count() const {
        return row_count_;
    }
    uint64_t file_bytes() const {
        return file_bytes_;
    }
    uint64_t next_local_id() const {
        return next_local_id_;
    }
    size_t index_bytes() const {
        return (block_first_ids_.size() + block_offsets_.size()) * sizeof(uint64_t);
    }
    std::optional<Row> Read(uint64_t local_id) const;
    void Visit(const std::function<void(uint64_t, Row&&)>& visitor) const;
    void ValidateRowsForInstall();
    void SetDiagnostics(std::shared_ptr<DeltaDiagnostics> diagnostics) const;

    // CheckpointDb is the only production caller; public keeps the concrete file reader factory trivial.
    ImmutableTable(std::string path, int fd, TableId table_id, uint64_t generation, Epoch visible_from,
                   uint64_t row_count, uint64_t payload_bytes, uint64_t file_bytes,
                   std::vector<uint64_t> block_first_ids, std::vector<uint64_t> block_offsets);

private:
    friend class EpochSiEngine;
    std::optional<bool> RecoveryContains(uint64_t local_id) const;

    std::string path_;
    int fd_ = -1;
    TableId table_id_ = 0;
    uint64_t generation_ = 0;
    Epoch visible_from_ = 0;
    uint64_t row_count_ = 0;
    uint64_t payload_bytes_ = 0;
    uint64_t file_bytes_ = 0;
    uint64_t next_local_id_ = 0;
    std::vector<uint64_t> block_first_ids_;
    std::vector<uint64_t> block_offsets_;
    std::vector<uint64_t> recovery_membership_;
    bool recovery_membership_complete_ = false;
    mutable std::shared_ptr<DeltaDiagnostics> diagnostics_;
};

enum class CommitStatus { kCommitted, kWriteConflict, kUniqueConflict };

struct CommitResult {
    CommitStatus status;
    Epoch epoch = 0;
    uint64_t commit_seq = 0;
};

enum class CrashPoint {
    kNone,
    kBeforeAppend,
    kAfterPartialAppend,
    kAfterAppendBeforeSync,
    kAfterSync,
    kDuringInstall,
    kAfterInstallBeforePublish,
    kAfterPublishBeforeReturn
};

class SimulatedCrash : public std::runtime_error {
public:
    SimulatedCrash() : std::runtime_error("simulated crash") {}
};

class EpochSiEngine {
private:
    friend class CheckpointDb;
    struct PendingCommit;
    struct OwnerState {
        struct SnapshotSlot {
            Epoch epoch = 0;
            bool active = false;
        };

        size_t RegisterSnapshot(Epoch epoch);
        void UnregisterSnapshot(size_t slot) noexcept;
        std::optional<Epoch> OldestSnapshot() const noexcept;

        mutable std::mutex snapshot_mutex;
        std::vector<SnapshotSlot> snapshot_slots;
        std::vector<size_t> free_snapshot_slots;
        std::atomic<size_t> active_count{0};
        std::atomic<bool> valid{true};
    };

public:
    // The caller holds the engine's external state lock while preparing and
    // publishing.  Syncing the already-built frame intentionally needs none.
    class PreparedCommit {
    public:
        PreparedCommit();
        ~PreparedCommit();
        PreparedCommit(const PreparedCommit&) = delete;
        PreparedCommit& operator=(const PreparedCommit&) = delete;
        PreparedCommit(PreparedCommit&&) noexcept;
        PreparedCommit& operator=(PreparedCommit&&) = delete;

        bool needs_sync() const noexcept;
        bool needs_publish() const noexcept;
        const std::vector<CommitResult>& results() const noexcept;

    private:
        friend class EpochSiEngine;
        EpochSiEngine* engine_ = nullptr;
        std::unique_ptr<PendingCommit> pending_;
    };

    struct Txn {
        Txn() = default;
        ~Txn();
        Txn(const Txn&) = delete;
        Txn& operator=(const Txn&) = delete;
        Txn(Txn&& other) noexcept;
        Txn& operator=(Txn&&) noexcept = delete;

    private:
        friend class EpochSiEngine;
        void Finish() noexcept;
        std::shared_ptr<OwnerState> owner_;
        size_t snapshot_slot_ = std::numeric_limits<size_t>::max();
        Epoch start_epoch_ = 0;
        bool active_ = false;
        bool prepared_ = false;
        std::map<RowId, Row> writes_;
        std::set<RowId> inserts_;
    };

    explicit EpochSiEngine(BaseImage base, Epoch base_epoch = 0);
    ~EpochSiEngine();
    EpochSiEngine(const EpochSiEngine&) = delete;
    EpochSiEngine& operator=(const EpochSiEngine&) = delete;
    EpochSiEngine(EpochSiEngine&& other) noexcept;
    EpochSiEngine& operator=(EpochSiEngine&& other) noexcept;
    static EpochSiEngine Recover(BaseImage base, const std::vector<uint8_t>& durable_wal, Epoch base_epoch = 0);
    static EpochSiEngine Recover(BaseImage base, ImmutableTables tables, const std::vector<uint8_t>& durable_wal,
                                 Epoch base_epoch = 0);
    static EpochSiEngine OpenFile(BaseImage base, const std::string& wal_path, Epoch base_epoch = 0);
    static EpochSiEngine OpenFile(BaseImage base, ImmutableTables tables, const std::string& wal_path,
                                  Epoch base_epoch = 0);
    static EpochSiEngine OpenFileAt(BaseImage base, ImmutableTables tables, int directory_fd,
                                    const std::string& wal_name, Epoch base_epoch = 0);
    static EpochSiEngine OpenWalChain(BaseImage base, ImmutableTables tables, std::unique_ptr<FileWal> legacy,
                                      std::vector<std::unique_ptr<FileWal>> segments, Epoch base_epoch,
                                      uint64_t base_next_commit_seq,
                                      const std::map<TableId, uint64_t>& manifest_frontiers);
    static EpochSiEngine CreateFile(BaseImage base, const std::string& wal_path, Epoch base_epoch = 0);

    Txn Begin();
    std::optional<Row> Read(const Txn& txn, RowId row_id) const;
    std::vector<std::pair<RowId, Row>> Scan(const Txn& txn, TableId table_id) const;
    void VisitScan(const Txn& txn, TableId table_id, const std::function<void(RowId, const Row&)>& visitor) const;
    RowId InsertImage(Txn& txn, TableId table_id, RowImage row);
    void PutImage(Txn& txn, RowId row_id, RowImage row);
    void Erase(Txn& txn, RowId row_id);
    void Abort(Txn& txn);
    std::vector<CommitResult> CommitBatch(const std::vector<Txn*>& txns);
    PreparedCommit PrepareCommitBatch(const std::vector<Txn*>& txns);
    void SyncPreparedCommit(PreparedCommit& prepared);
    void PublishPreparedCommit(PreparedCommit& prepared);
    void SetDiagnostics(std::shared_ptr<DeltaDiagnostics> diagnostics);
    uint64_t next_local_id(TableId table_id) const;
    uint64_t next_commit_seq() const {
        return next_commit_seq_;
    }
    void SetCrashPointForTest(CrashPoint point, size_t position = 0);
    void SetFileMaxWriteChunkForTest(size_t bytes);
    void CloseFileForTest();
    Epoch published_epoch() const {
        return published_epoch_;
    }
    size_t durable_wal_bytes() const;
    size_t wal_write_calls_for_test() const;
    size_t wal_sync_calls_for_test() const;
    size_t version_count() const {
        return version_count_;
    }
    size_t retained_version_count_for_test() const {
        return OccupiedRowStateCount();
    }
    size_t explicit_row_metadata_count() const {
        return OccupiedRowStateCount();
    }
    size_t last_publication_staged_entries_for_test() const {
        return last_publication_staged_entries_;
    }
    size_t last_publication_staged_versions_for_test() const {
        return last_publication_staged_versions_;
    }
    size_t last_install_version_nodes_for_test() const {
        return last_install_version_nodes_;
    }
    const std::vector<uint8_t>& recovery_wal_image_for_test() const {
        return recovery_wal_image_;
    }
    size_t retained_recovery_wal_capacity_for_test() const {
        return volatile_wal_.capacity() + recovery_wal_image_.capacity();
    }
    size_t wal_frame_count() const {
        return wal_frame_count_;
    }
    size_t wal_transaction_count() const {
        return wal_transaction_count_;
    }
    size_t active_transaction_count() const {
        return identity_ ? identity_->active_count.load(std::memory_order_acquire) : 0;
    }
    std::optional<Epoch> oldest_active_snapshot_for_test() const noexcept {
        return identity_ ? identity_->OldestSnapshot() : std::nullopt;
    }
    BaseImage MaterializePublished() const;
    bool CanInstallPristineTable(TableId table_id) const;
    size_t resident_base_row_count() const {
        return base_.size();
    }
    size_t immutable_index_bytes() const;
    size_t immutable_read_probes_for_test() const;
    std::optional<bool> immutable_recovery_membership_for_test(RowId row_id) const {
        const auto table = immutable_tables_.find(row_id.table_id);
        return table == immutable_tables_.end() ? std::optional<bool>(false)
                                                : table->second->RecoveryContains(row_id.local_id);
    }
    size_t immutable_table_count() const {
        return immutable_tables_.size();
    }
    void VisitLatestVersions(const std::function<void(RowId, const Row&)>& visitor) const;
    void VisitLatestVersionHeads(const std::function<void(RowId, Epoch, const Row&)>& visitor) const;
    bool IsLatestSnapshot(const Txn& txn) const;
    std::set<TableId> DirtyTableIds() const {
        return dirty_tables_;
    }

private:
    struct Version {
        Version(Epoch epoch, const Row& row, std::unique_ptr<Version> older)
            : epoch(epoch), row(row), older(std::move(older)) {}
        ~Version() {
            while (older) {
                auto next = std::move(older->older);
                older.reset();
                older = std::move(next);
            }
        }

        Epoch epoch;
        Row row;
        std::unique_ptr<Version> older;
    };

    // Row metadata is sparse by construction: immutable/base rows do not get a
    // slot until a committed version is created.  Chunks are ordered, while
    // their fixed slots make point lookup independent of the number of rows.
    static constexpr uint64_t kRowStateChunkSize = 256;
    struct RowState {
        std::unique_ptr<Version> head;
        Epoch last_epoch = 0;
        Epoch last_pruned_watermark = 0;
        size_t version_count = 0;
        bool occupied = false;
    };
    struct RowStateChunk {
        std::array<RowState, kRowStateChunkSize> slots;
        size_t occupied = 0;
    };
    struct RowStateChunkKey {
        TableId table_id = 0;
        uint64_t chunk_id = 0;

        bool operator<(const RowStateChunkKey& other) const {
            return table_id != other.table_id ? table_id < other.table_id : chunk_id < other.chunk_id;
        }
    };
    using RowStateDirectory = std::map<RowStateChunkKey, std::unique_ptr<RowStateChunk>>;

    static RowStateChunkKey ChunkKey(RowId row_id) {
        return {row_id.table_id, row_id.local_id / kRowStateChunkSize};
    }
    static size_t ChunkSlot(RowId row_id) {
        return static_cast<size_t>(row_id.local_id % kRowStateChunkSize);
    }
    const RowState* FindRowState(RowId row_id) const noexcept;
    RowState* FindRowState(RowId row_id) noexcept;
    size_t OccupiedRowStateCount() const noexcept;
    bool HasRowStateTable(TableId table_id) const noexcept;

    struct PreparedState {
        RowStateDirectory row_states;
        std::map<ConstraintClaim, Epoch> last_claim_epoch;
        std::map<ConstraintClaim, RowId> claim_owner;
        std::map<TableId, uint64_t> next_row_id;
        std::set<TableId> dirty_tables;
        std::map<ConstraintClaim, RowId> claim_owner_erases;
        std::vector<RowId> touched_rows;
        std::vector<std::unique_ptr<Version>> retired_versions;
        size_t version_count;
        size_t staged_entries;
        size_t staged_versions;
    };
    struct PendingCommit {
        std::vector<CommitResult> results;
        std::vector<Txn*> accepted;
        std::vector<uint8_t> frame;
        PreparedState publication;
        Epoch epoch = 0;
        size_t accepted_count = 0;
        bool synced = false;
        bool published = false;
    };
    struct PreparedTableInstall {
        ImmutableTables tables;
        std::map<ConstraintClaim, Epoch> last_claim_epoch;
        std::map<ConstraintClaim, RowId> claim_owner;
        std::map<TableId, uint64_t> next_row_id;
    };
    struct DecodedFrameHeader {
        uint32_t version;
        uint32_t frame_bytes;
        Epoch epoch;
        uint64_t first_commit_seq;
        uint32_t txn_count;
        uint32_t operation_count;
        uint32_t payload_bytes;
    };

    EpochSiEngine(BaseImage base, ImmutableTables tables, Epoch base_epoch);
    void SyncFileWalForRotation();
    void InstallFileWalForRotation(std::unique_ptr<FileWal> file) noexcept;
    static EpochSiEngine RecoverFile(BaseImage base, ImmutableTables tables, std::unique_ptr<FileWal> file,
                                     Epoch base_epoch);
    void RecoverFileFrames(FileWal& file, bool allow_torn_tail);
    static DecodedFrameHeader DecodeFrameHeader(const std::vector<uint8_t>& wal_image, size_t frame_start);
    void RecoverFrame(const std::vector<uint8_t>& wal_image, size_t frame_start, const DecodedFrameHeader& header);
    std::optional<Row> ReadCommitted(RowId row_id, Epoch snapshot) const;
    bool RecoveryRowExists(RowId row_id) const;
    Epoch LastExistingRowEpoch(RowId row_id) const;
    static std::vector<uint8_t> EncodeFrame(Epoch epoch, const std::vector<Txn*>& txns,
                                            const std::vector<uint64_t>& commit_seqs);
    void RequireActive(const Txn& txn) const;
    void InstallRecoveredLatest(const std::vector<Txn*>& accepted, Epoch epoch);
    PreparedState PreparePublication(const std::vector<Txn*>& accepted, Epoch epoch) const;
    bool InstallPrepared(PreparedState&& prepared) noexcept;
    void PruneTouchedVersions(PreparedState& prepared, Epoch low_watermark) noexcept;
    PreparedTableInstall PrepareTableInstall(std::shared_ptr<const ImmutableTable> table) const;
    void InstallTablePrepared(PreparedTableInstall&& prepared) noexcept;
    void VisitPublished(TableId table_id, const std::function<void(RowId, const Row&)>& visitor);
    const std::set<TableId>& dirty_tables() const {
        return dirty_tables_;
    }
    void PoisonAndCrash();
    void Poison() noexcept;
    CommitStatus Certify(const Txn& txn, std::set<RowId>& batch_rows, std::set<ConstraintClaim>& batch_claims) const;

    BaseImage base_;
    ImmutableTables immutable_tables_;
    RowStateDirectory row_states_;
    std::map<ConstraintClaim, Epoch> last_claim_epoch_;
    std::map<ConstraintClaim, RowId> claim_owner_;
    std::map<TableId, uint64_t> recovery_persisted_frontiers_;
    // Insert execution only reserves a private RowId. Publication still owns all committed structural state.
    std::mutex row_id_allocator_mutex_;
    std::map<TableId, uint64_t> next_row_id_;
    std::shared_ptr<OwnerState> identity_;
    Epoch base_epoch_ = 0;
    Epoch published_epoch_ = 0;
    uint64_t next_commit_seq_ = 1;
    size_t version_count_ = 0;
    std::vector<uint8_t> volatile_wal_;
    std::vector<uint8_t> recovery_wal_image_;
    std::unique_ptr<FileWal> file_wal_;
    size_t wal_frame_count_ = 0;
    size_t wal_transaction_count_ = 0;
    std::set<TableId> dirty_tables_;
    size_t last_publication_staged_entries_ = 0;
    size_t last_publication_staged_versions_ = 0;
    size_t last_install_version_nodes_ = 0;
    CrashPoint crash_point_ = CrashPoint::kNone;
    size_t crash_position_ = 0;
    std::atomic<bool> poisoned_{false};
    std::atomic<bool> pending_prepared_{false};
    std::shared_ptr<DeltaDiagnostics> diagnostics_;
};

} // namespace epoch_si_poc
