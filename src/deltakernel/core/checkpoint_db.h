#pragma once

#include "epoch_si_engine.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace epoch_si_poc {

enum class CheckpointCrashPoint {
    kNone,
    kDuringBaseTemp,
    kAfterBaseRename,
    kAfterWalCreate,
    kBeforeNextEngineOpen,
    kDuringManifestTemp,
    kAfterManifestRenameBeforeDirSync,
    kAfterSuccess
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
    struct TableRef {
        TableId table_id = 0;
        uint64_t file_generation = 0;
        Epoch visible_from = 0;
        uint64_t row_count = 0;
        uint64_t file_bytes = 0;
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
    Epoch base_epoch() const {
        RequireUsable();
        return base_epoch_;
    }

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

private:
    CheckpointDb(std::string directory, uint64_t generation, uint64_t wal_generation, Epoch base_epoch,
                 std::map<TableId, TableRef> tables, EpochSiEngine engine);
    void RequireUsable() const;

    std::string directory_;
    uint64_t generation_;
    uint64_t wal_generation_;
    Epoch base_epoch_;
    std::map<TableId, TableRef> tables_;
    EpochSiEngine engine_;
    CheckpointCrashPoint crash_point_ = CheckpointCrashPoint::kNone;
    bool poisoned_ = false;
};

} // namespace epoch_si_poc
