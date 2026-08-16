#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
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
    bool Contains(uint64_t local_id) const;
    void Visit(const std::function<void(uint64_t, Row&&)>& visitor) const;
    void ValidateRowsForInstall();

    // CheckpointDb is the only production caller; public keeps the concrete file reader factory trivial.
    ImmutableTable(std::string path, int fd, TableId table_id, uint64_t generation, Epoch visible_from,
                   uint64_t row_count, uint64_t payload_bytes, uint64_t file_bytes,
                   std::vector<uint64_t> block_first_ids, std::vector<uint64_t> block_offsets);

private:
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
    struct OwnerState {
        size_t active_count = 0;
        bool valid = true;
    };

public:
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
        Epoch start_epoch_ = 0;
        bool active_ = false;
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
    size_t explicit_row_metadata_count() const {
        return last_row_epoch_.size();
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
    size_t wal_frame_count() const {
        return wal_frame_count_;
    }
    size_t wal_transaction_count() const {
        return wal_transaction_count_;
    }
    size_t active_transaction_count() const {
        return identity_ ? identity_->active_count : 0;
    }
    BaseImage MaterializePublished() const;
    bool CanInstallPristineTable(TableId table_id) const;
    size_t resident_base_row_count() const {
        return base_.size();
    }
    size_t immutable_index_bytes() const;
    size_t immutable_table_count() const {
        return immutable_tables_.size();
    }
    void VisitLatestVersions(const std::function<void(RowId, const Row&)>& visitor) const;
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

    struct PreparedState {
        std::map<RowId, std::unique_ptr<Version>> versions;
        std::map<RowId, Epoch> last_row_epoch;
        std::map<ConstraintClaim, Epoch> last_claim_epoch;
        std::map<ConstraintClaim, RowId> claim_owner;
        std::map<TableId, uint64_t> next_row_id;
        std::set<TableId> dirty_tables;
        std::map<ConstraintClaim, RowId> claim_owner_erases;
        size_t version_count;
        size_t staged_entries;
        size_t staged_versions;
    };
    struct PreparedTableInstall {
        ImmutableTables tables;
        std::map<RowId, Epoch> last_row_epoch;
        std::map<ConstraintClaim, Epoch> last_claim_epoch;
        std::map<ConstraintClaim, RowId> claim_owner;
        std::map<TableId, uint64_t> next_row_id;
    };

    EpochSiEngine(BaseImage base, ImmutableTables tables, Epoch base_epoch);
    std::optional<Row> ReadCommitted(RowId row_id, Epoch snapshot) const;
    std::optional<Epoch> LastRowEpoch(RowId row_id) const;
    static std::vector<uint8_t> EncodeFrame(Epoch epoch, const std::vector<Txn*>& txns,
                                            const std::vector<uint64_t>& commit_seqs);
    void RequireActive(const Txn& txn) const;
    void Install(const std::vector<Txn*>& accepted, Epoch epoch);
    PreparedState PreparePublication(const std::vector<Txn*>& accepted, Epoch epoch) const;
    bool InstallPrepared(PreparedState&& prepared) noexcept;
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
    std::map<RowId, std::unique_ptr<Version>> versions_;
    std::map<RowId, Epoch> last_row_epoch_;
    std::map<ConstraintClaim, Epoch> last_claim_epoch_;
    std::map<ConstraintClaim, RowId> claim_owner_;
    std::map<TableId, uint64_t> next_row_id_;
    std::shared_ptr<OwnerState> identity_;
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
    bool poisoned_ = false;
};

} // namespace epoch_si_poc
