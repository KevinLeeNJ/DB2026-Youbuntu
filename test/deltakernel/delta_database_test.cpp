#include "deltakernel/delta_database.h"
#include "parser/parser.h"
#include "server/database_instance.h"
#include "system/sm_meta.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <csignal>
#include <thread>
#include <type_traits>
#include <sys/wait.h>
#include <unistd.h>

namespace deltakernel {
class DeltaSnapshotTestPeer {
public:
    struct PinHandle {
        epoch_si_poc::EpochSiEngine::SnapshotPin pin;
    };
    static void Build(DeltaDatabase& database) {
        database.BuildSnapshotBases();
    }
    static bool SnapshotBuilderBusy(DeltaDatabase& database) {
        if (!database.snapshot_builder_mutex_.try_lock())
            return true;
        database.snapshot_builder_mutex_.unlock();
        return false;
    }
    static void SetIoHook(DeltaDatabase& database, std::function<void()> hook) {
        database.snapshot_io_hook_for_test_ = std::move(hook);
    }
    static std::unique_lock<std::shared_mutex> HoldExecution(DeltaDatabase& database) {
        return std::unique_lock<std::shared_mutex>(database.execution_gate_);
    }
    static void SetSidecarFinishHook(DeltaDatabase& database, std::function<void(size_t)> hook) {
        database.sidecar_finish_hook_for_test_ = std::move(hook);
    }
    static void SetSidecarArtifactHook(
        DeltaDatabase& database,
        std::function<void(epoch_si_poc::TableId, const std::vector<std::array<uint64_t, 4>>&)> hook) {
        database.sidecar_artifact_hook_for_test_ = std::move(hook);
    }
    static void SetRowsHook(DeltaDatabase& database, const std::string& table,
                            std::function<void(epoch_si_poc::Epoch, std::vector<int32_t>)> hook) {
        const auto schema = database.tables_.at(table);
        database.snapshot_rows_hook_for_test_ =
            [&database, schema, hook = std::move(hook)](
                epoch_si_poc::Epoch cut,
                const std::map<epoch_si_poc::TableId, std::vector<std::pair<epoch_si_poc::RowId, epoch_si_poc::Row>>>&
                    rows) {
                std::vector<int32_t> values;
                if (const auto found = rows.find(schema.id); found != rows.end()) {
                    for (const auto& [id, row] : found->second) {
                        (void)id;
                        const auto cells = database.DecodeRow(schema, row);
                        values.push_back(cells.at(1).integer);
                    }
                }
                hook(cut, std::move(values));
            };
    }
    static void SetCrashPoint(DeltaDatabase& database, epoch_si_poc::CheckpointCrashPoint point) {
        database.db_.SetCrashPointForTest(point);
    }
    static std::string FutureSnapshotTemp(const DeltaDatabase& database, const std::string& table) {
        return database.directory_ + "/tablebase." + std::to_string(database.tables_.at(table).id) + "." +
               std::to_string(database.db_.generation() + 2) + ".tmp";
    }
    static std::array<uint64_t, 4> IndexState(const DeltaDatabase& database) {
        const auto& sidecar = database.sidecars_.begin()->second;
        return {sidecar.generation, sidecar.snapshot_epoch, database.overlay_.size(), database.current_overlay_.size()};
    }
    static std::array<uint64_t, 4> OverlayState(const DeltaDatabase& database) {
        return {database.overlay_.size(), database.frozen_overlay_.size(), database.current_overlay_.size(),
                database.frozen_current_overlay_.size()};
    }
    static std::array<uint64_t, 6> ReclamationState(const DeltaDatabase& database) {
        return {database.db_.engine().version_count(),
                database.db_.engine().retained_version_count_for_test(),
                database.db_.engine().row_state_chunk_count_for_test(),
                database.retired_sources_.size(),
                database.retired_collapse_cursor_.done,
                database.retired_tables_collected_};
    }
    static void SetRetiredStateReleaseHook(DeltaDatabase& database, std::function<void()> hook) {
        database.retired_state_release_hook_for_test_ = std::move(hook);
    }
    static std::unique_ptr<PinHandle> HoldSnapshotPin(DeltaDatabase& database) {
        return std::make_unique<PinHandle>(PinHandle{database.db_.engine().PinSnapshot()});
    }
    static void AppendRowCopies(DeltaDatabase& database, const std::string& table, size_t count) {
        const auto table_id = database.tables_.at(table).id;
        auto reader = database.db_.engine().Begin();
        auto image = database.db_.engine().Read(reader, {table_id, 0});
        database.db_.engine().Abort(reader);
        if (!image)
            throw std::logic_error("copy source missing");
        auto writer = database.db_.engine().Begin();
        for (size_t position = 0; position < count; ++position)
            database.db_.engine().InsertImage(writer, table_id, *image);
        const auto result = database.db_.engine().CommitBatch({&writer});
        if (result.size() != 1 || result[0].status != epoch_si_poc::CommitStatus::kCommitted)
            throw std::logic_error("copy rows not committed");
    }
    static std::array<uint64_t, 3> SnapshotState(const DeltaDatabase& database) {
        return {database.db_.generation(), database.db_.engine().DirtyTableIds().size(),
                database.db_.immutable_table_count()};
    }
    static std::array<uint64_t, 3> AuthorityState(const DeltaDatabase& database) {
        return {database.db_.generation(), database.db_.base_epoch(), database.db_.engine().published_epoch()};
    }
    static DeltaDatabase::StagedSidecarArtifact
    BuildStaged(DeltaDatabase& database, const std::string& table, size_t index,
                const std::vector<std::pair<std::vector<uint8_t>, uint64_t>>& entries, uint64_t generation,
                uint64_t snapshot_epoch) {
        const auto& schema = database.tables_.at(table);
        std::vector<DeltaDatabase::SidecarBuildEntry> input;
        input.reserve(entries.size());
        for (const auto& entry : entries)
            input.push_back({entry.first, entry.second});
        return database.BuildStagedSidecarForTest(schema, schema.indexes.at(index), input, generation, snapshot_epoch);
    }
    static uint64_t Generation(const DeltaDatabase& database, const std::string& table) {
        return *database.db_.TableGeneration(database.tables_.at(table).id);
    }
    static std::string TableBasePath(const DeltaDatabase& database, const std::string& table, uint64_t generation) {
        return database.directory_ + "/tablebase." + std::to_string(database.tables_.at(table).id) + "." +
               std::to_string(generation);
    }
    static std::string GenerationSidecarPath(const DeltaDatabase& database, const std::string& table, size_t index) {
        const auto& schema = database.tables_.at(table);
        return database.directory_ + "/deltaidx." + std::to_string(schema.id) + "." +
               std::to_string(schema.indexes.at(index).constraint_id) + ".g" +
               std::to_string(*database.db_.TableGeneration(schema.id));
    }
    static uint64_t Snapshot(const DeltaDatabase& database) {
        return std::max<uint64_t>(database.db_.engine().published_epoch(),
                                  *database.db_.TableVisibleFrom(database.tables_.begin()->second.id));
    }
    static bool ValidateStaged(DeltaDatabase& database, const std::string& table, const std::string& path,
                               uint64_t generation, uint64_t snapshot) {
        const auto& schema = database.tables_.at(table);
        const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0)
            return false;
        const bool valid = database.ValidateSidecarFd(schema, schema.indexes.at(0), fd, false, generation, snapshot);
        close(fd);
        return valid;
    }
};
} // namespace deltakernel

namespace {
static_assert(!std::is_copy_constructible_v<deltakernel::DeltaSession>);
static_assert(!std::is_move_constructible_v<deltakernel::DeltaSession>);
thread_local int delta_concurrency_role = 0;

template <typename Predicate>
bool WaitForCondition(std::condition_variable& condition, std::mutex& mutex, Predicate predicate) {
    std::unique_lock<std::mutex> lock(mutex);
    return condition.wait_for(lock, std::chrono::seconds(2), predicate);
}

struct CapturingSink : QueryResultSink {
    void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& names) override {
        ++begin_count;
        metadata = columns;
        column_names = names;
    }
    void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) override {
        ASSERT_EQ(columns.size(), metadata.size());
        std::vector<std::string> row;
        for (const auto& column : columns) {
            ASSERT_LE(static_cast<size_t>(column.offset + column.len), size);
            if (is_null(data, column))
                row.emplace_back("NULL");
            else if (column.type == TYPE_INT)
                row.push_back(std::to_string(read_unaligned<int32_t>(data + column.offset)));
            else if (column.type == TYPE_FLOAT) {
                uint32_t bits;
                const float value = read_float(data + column.offset);
                std::memcpy(&bits, &value, sizeof(bits));
                row.push_back(std::to_string(bits));
            } else
                row.emplace_back(data + column.offset, static_cast<size_t>(column.len));
        }
        rows.push_back(std::move(row));
    }
    std::vector<ColMeta> metadata;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
    int begin_count = 0;
};

class TempDelta {
public:
    TempDelta() {
        char path[] = "/tmp/rmdb-delta-test-XXXXXX";
        if (mkdtemp(path) == nullptr)
            throw std::runtime_error("mkdtemp failed");
        path_ = path;
    }
    ~TempDelta() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    const std::string& path() const {
        return path_;
    }

private:
    std::string path_;
};

uint32_t DeltaTestCrc32(const uint8_t* data, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
    return ~crc;
}

template <typename T> void PutDeltaLe(std::vector<uint8_t>& bytes, size_t offset, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        bytes.at(offset + i) = static_cast<uint8_t>(value >> (8 * i));
}

template <typename T> T GetDeltaLe(const std::vector<uint8_t>& bytes, size_t offset) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<T>(bytes.at(offset + i)) << (8 * i);
    return value;
}

std::vector<uint8_t> ReadDeltaFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void WriteDeltaFile(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

void RebaseDeltaWalSegment(const TempDelta& temp, uint64_t new_id) {
    const std::string old_path = temp.path() + "/db.log.s.0.0";
    const std::string new_path = temp.path() + "/db.log.s.0." + std::to_string(new_id);
    auto segment = ReadDeltaFile(old_path);
    ASSERT_EQ(segment.size(), 72U);
    PutDeltaLe<uint64_t>(segment, 24, new_id);
    PutDeltaLe<uint64_t>(segment, 32, new_id - 1);
    PutDeltaLe<uint32_t>(segment, 68, DeltaTestCrc32(segment.data(), 68));
    WriteDeltaFile(old_path, segment);
    std::filesystem::rename(old_path, new_path);

    auto manifest = ReadDeltaFile(temp.path() + "/MANIFEST");
    const uint32_t count = GetDeltaLe<uint32_t>(manifest, 36);
    const size_t chain = 44 + static_cast<size_t>(count) * 40;
    PutDeltaLe<uint64_t>(manifest, chain + 32, new_id);
    PutDeltaLe<uint64_t>(manifest, chain + 40, new_id);
    PutDeltaLe<uint64_t>(manifest, chain + 48, 1);
    PutDeltaLe<uint32_t>(manifest, manifest.size() - 4, DeltaTestCrc32(manifest.data(), manifest.size() - 4));
    WriteDeltaFile(temp.path() + "/MANIFEST", manifest);
}

void ConvertSegmentedManifestToLegacy(const TempDelta& temp) {
    constexpr size_t kManifestHeaderBytes = 44;
    constexpr size_t kTableRefBytes = 40;
    constexpr size_t kSegmentHeaderBytes = 72;
    auto manifest = ReadDeltaFile(temp.path() + "/MANIFEST");
    const uint32_t count = GetDeltaLe<uint32_t>(manifest, 36);
    const size_t chain = kManifestHeaderBytes + static_cast<size_t>(count) * kTableRefBytes;
    ASSERT_EQ(manifest.size(), 112U + static_cast<size_t>(count) * 56U);
    ASSERT_EQ(GetDeltaLe<uint64_t>(manifest, chain + 32), 0U);
    ASSERT_EQ(GetDeltaLe<uint64_t>(manifest, chain + 40), 0U);
    const uint64_t lineage = GetDeltaLe<uint64_t>(manifest, 20);
    auto segment = ReadDeltaFile(temp.path() + "/db.log.s." + std::to_string(lineage) + ".0");
    ASSERT_GE(segment.size(), kSegmentHeaderBytes);

    std::vector<uint8_t> legacy(kManifestHeaderBytes + static_cast<size_t>(count) * kTableRefBytes + 4);
    std::copy(manifest.begin(), manifest.begin() + chain, legacy.begin());
    PutDeltaLe<uint32_t>(legacy, 8, static_cast<uint32_t>(legacy.size()));
    PutDeltaLe<uint32_t>(legacy, 40, 0);
    PutDeltaLe<uint32_t>(legacy, legacy.size() - 4, DeltaTestCrc32(legacy.data(), legacy.size() - 4));
    WriteDeltaFile(temp.path() + "/db.log." + std::to_string(lineage),
                   std::vector<uint8_t>(segment.begin() + kSegmentHeaderBytes, segment.end()));
    ASSERT_TRUE(std::filesystem::remove(temp.path() + "/db.log.s." + std::to_string(lineage) + ".0"));
    WriteDeltaFile(temp.path() + "/MANIFEST", legacy);
}

class ScopedStorageEngine {
public:
    explicit ScopedStorageEngine(const char* value) {
        if (const char* previous = std::getenv("RMDB_STORAGE_ENGINE")) {
            had_previous_ = true;
            previous_ = previous;
        }
        if ((value == nullptr ? unsetenv("RMDB_STORAGE_ENGINE") : setenv("RMDB_STORAGE_ENGINE", value, 1)) != 0)
            throw std::runtime_error("set RMDB_STORAGE_ENGINE failed");
    }
    ~ScopedStorageEngine() {
        if (had_previous_)
            setenv("RMDB_STORAGE_ENGINE", previous_.c_str(), 1);
        else
            unsetenv("RMDB_STORAGE_ENGINE");
    }

private:
    bool had_previous_{false};
    std::string previous_;
};

void RunSql(deltakernel::DeltaDatabase& db, deltakernel::DeltaSession& session, const char* sql) {
    db.Execute(ast::parse_sql(sql), session, nullptr);
}

CapturingSink Query(deltakernel::DeltaDatabase& db, deltakernel::DeltaSession& session, const char* sql) {
    CapturingSink sink;
    EXPECT_TRUE(db.Execute(ast::parse_sql(sql), session, &sink));
    return sink;
}

bool WaitForCommitQueueDepth(deltakernel::DeltaDatabase& db, size_t depth) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (db.CommitQueueDepthForTest() == depth)
            return true;
        std::this_thread::yield();
    }
    return db.CommitQueueDepthForTest() == depth;
}

uint32_t CatalogCrc32(const std::string& bytes) {
    uint32_t crc = 0xffffffffU;
    for (unsigned char byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
    return ~crc;
}

void WriteCatalog(const std::string& path, const std::string& body) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << body << "CRC32 " << CatalogCrc32(body) << '\n';
}

uint32_t Crc32(const uint8_t* bytes, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t n = 0; n < size; ++n) {
        crc ^= bytes[n];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
    return ~crc;
}

template <typename T> T ReadLeAt(const std::array<uint8_t, 96>& bytes, size_t offset) {
    using U = std::make_unsigned_t<T>;
    U value = 0;
    for (size_t n = 0; n < sizeof(T); ++n)
        value |= static_cast<U>(bytes[offset + n]) << (n * 8);
    return static_cast<T>(value);
}

template <typename T> void WriteLeAt(std::array<uint8_t, 96>& bytes, size_t offset, T value) {
    using U = std::make_unsigned_t<T>;
    const U bits = static_cast<U>(value);
    for (size_t n = 0; n < sizeof(T); ++n)
        bytes[offset + n] = static_cast<uint8_t>(bits >> (n * 8));
}

std::array<uint8_t, 96> ReadSidecarHeader(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    std::array<uint8_t, 96> header{};
    input.read(reinterpret_cast<char*>(header.data()), header.size());
    EXPECT_TRUE(input);
    return header;
}

void WriteSidecarHeader(const std::string& path, std::array<uint8_t, 96> header) {
    WriteLeAt<uint32_t>(header, 92, Crc32(header.data(), 92));
    std::fstream output(path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(output);
    output.write(reinterpret_cast<const char*>(header.data()), header.size());
    ASSERT_TRUE(output);
}

TEST(DeltaDatabaseTest, PrivateCommitAbortCheckpointAndRestart) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into t values(1, 7);");
    RunSql(*db, session, "rollback;");
    RunSql(*db, session, "insert into t values(1, 9);");
    db->Checkpoint();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "update t set v = 10 where k = 1;");
    RunSql(*db, session, "commit;");
}

TEST(DeltaDatabaseTest, MaintenanceCutsReadOnlyWalAndReopens) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t (id int);\n");
    RunSql(*db, session, "begin;\n");
    RunSql(*db, session, "select * from t;\n");
    RunSql(*db, session, "commit;\n");
    db->SetMaintenanceThresholdsForTest(1, 2);
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
    auto reopened = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(reopened->WalFrameCountForTest(), 0U);
}

TEST(DeltaDatabaseTest, MaintenanceCooldownAndHardThresholdAreGeneric) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t (id int);\n");
    auto readonly_commit = [&] {
        RunSql(*db, session, "begin;\n");
        RunSql(*db, session, "select * from t;\n");
        RunSql(*db, session, "commit;\n");
    };
    db->SetMaintenanceThresholdsForTest(1, 1024);
    db->SetMaintenanceCooldownForTest(std::chrono::hours(1));
    readonly_commit();
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
    readonly_commit();
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kReclaimed);
    db->SetMaintenanceThresholdsForTest(1, 1);
    readonly_commit();
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
}

TEST(DeltaDatabaseTest, MaintenanceCancelRestoresAuthorityBeforePublication) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t (id int, v int);\n");
    RunSql(*db, session, "insert into t values (1, 7);\n");
    db->SetMaintenanceThresholdsForTest(1, 2);
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] { db->StopMaintenance(); });
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCancelled);
    auto reopened = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession reopened_session;
    auto rows = Query(*reopened, reopened_session, "select * from t;\n");
    ASSERT_EQ(rows.rows.size(), 1U);
    EXPECT_EQ(rows.rows[0][1], "7");
}

TEST(DeltaDatabaseTest, ReadOnlyCutPreservesPinnedRetiredChain) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 7);");
    auto old_pin = deltakernel::DeltaSnapshotTestPeer::HoldSnapshotPin(*db);
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    ASSERT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 1U);

    db->SetMaintenanceThresholdsForTest(1, 2);
    RunSql(*db, setup, "begin;");
    RunSql(*db, setup, "select * from t;");
    RunSql(*db, setup, "commit;");
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
    const auto pinned = deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db);
    EXPECT_EQ(pinned[3], 2U);
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"7"}}));

    old_pin.reset();
    for (size_t attempt = 0; attempt < 8 && deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3] != 0;
         ++attempt)
        EXPECT_NE(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCancelled);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 0U);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, MaintenanceBuildsDirtyTablesOnceAtThreshold) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t (id int, v int);\n");
    RunSql(*db, session, "insert into t values (1, 7);\n");
    db->SetMaintenanceThresholdsForTest(1, 2);
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
    const auto generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    EXPECT_EQ(db->MaintenanceTick(), deltakernel::DeltaDatabase::MaintenanceResult::kReclaimed);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t"), generation);
}

TEST(DeltaDatabaseTest, MaintenanceBuilderPacesAtForegroundBatchBoundary) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t (id int);\n");
    for (int32_t id = 0; id < 2048; ++id) {
        const auto sql = "insert into t values (" + std::to_string(id) + ");\n";
        RunSql(*db, session, sql.c_str());
    }

    std::mutex gate;
    std::condition_variable changed;
    size_t batches = 0;
    bool release_first = false;
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        std::unique_lock<std::mutex> lock(gate);
        ++batches;
        changed.notify_all();
        changed.wait(lock, [&] { return release_first; });
    });
    std::exception_ptr error;
    std::thread builder([&] {
        try {
            deltakernel::DeltaSnapshotTestPeer::Build(*db);
        } catch (...) {
            error = std::current_exception();
        }
    });
    bool reached_first_batch = false;
    {
        std::unique_lock<std::mutex> lock(gate);
        reached_first_batch = changed.wait_for(lock, std::chrono::seconds(5), [&] { return batches != 0; });
    }
    if (!reached_first_batch) {
        {
            std::lock_guard<std::mutex> release_lock(gate);
            release_first = true;
        }
        changed.notify_all();
        db->StopMaintenance();
        builder.join();
        deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
        FAIL() << "builder did not reach its first batch";
    }
    auto foreground = deltakernel::DeltaSnapshotTestPeer::HoldExecution(*db);
    {
        std::lock_guard<std::mutex> lock(gate);
        release_first = true;
    }
    changed.notify_all();
    {
        std::unique_lock<std::mutex> lock(gate);
        EXPECT_FALSE(changed.wait_for(lock, std::chrono::milliseconds(5), [&] { return batches > 1; }));
    }
    foreground.unlock();
    builder.join();
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    ASSERT_FALSE(error);
    EXPECT_GT(batches, 1U);
}

TEST(DeltaDatabaseTest, MaintenanceBuilderCancelDuringPacingReturnsPromptly) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t (id int);\n");
    for (int32_t id = 0; id < 2048; ++id) {
        const auto sql = "insert into t values (" + std::to_string(id) + ");\n";
        RunSql(*db, session, sql.c_str());
    }

    std::mutex gate;
    std::condition_variable changed;
    bool release_first = false;
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        std::unique_lock<std::mutex> lock(gate);
        changed.notify_all();
        changed.wait(lock, [&] { return release_first; });
    });
    std::exception_ptr error;
    std::thread builder([&] {
        try {
            deltakernel::DeltaSnapshotTestPeer::Build(*db);
        } catch (...) {
            error = std::current_exception();
        }
    });
    bool reached_first_batch = false;
    {
        std::unique_lock<std::mutex> lock(gate);
        reached_first_batch = changed.wait_for(lock, std::chrono::seconds(5)) != std::cv_status::timeout;
    }
    if (!reached_first_batch) {
        db->StopMaintenance();
        {
            std::lock_guard<std::mutex> release_lock(gate);
            release_first = true;
        }
        changed.notify_all();
        builder.join();
        deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
        FAIL() << "builder did not reach its first batch";
    }
    auto foreground = deltakernel::DeltaSnapshotTestPeer::HoldExecution(*db);
    {
        std::lock_guard<std::mutex> lock(gate);
        release_first = true;
    }
    changed.notify_all();
    const auto started = std::chrono::steady_clock::now();
    db->StopMaintenance();
    foreground.unlock();
    builder.join();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    ASSERT_FALSE(error);
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

TEST(DeltaDatabaseTest, MaintenanceRechecksDirtyTablesAfterWaitingForCommit) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession writer;
    RunSql(*db, writer, "create table t (id int, v int);\n");
    const auto initial_generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    deltakernel::DeltaSession debt;
    RunSql(*db, debt, "begin;\n");
    RunSql(*db, debt, "select * from t;\n");
    RunSql(*db, debt, "commit;\n");
    RunSql(*db, writer, "begin;\n");
    RunSql(*db, writer, "insert into t values (1, 7);\n");
    db->SetMaintenanceThresholdsForTest(1, 2);

    std::mutex gate;
    std::condition_variable changed;
    bool writer_holding_execution = false;
    bool release_writer = false;
    bool maintenance_waiting = false;
    db->SetExecuteLockHookForTest([&] {
        std::unique_lock<std::mutex> lock(gate);
        writer_holding_execution = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release_writer; });
    });
    db->SetExecutionWriterWaitHookForTest([&] {
        std::lock_guard<std::mutex> lock(gate);
        maintenance_waiting = true;
        changed.notify_all();
    });

    std::exception_ptr writer_error;
    std::thread writer_thread([&] {
        try {
            RunSql(*db, writer, "commit;\n");
        } catch (...) {
            writer_error = std::current_exception();
        }
    });
    bool reached_writer = false;
    {
        std::unique_lock<std::mutex> lock(gate);
        reached_writer = changed.wait_for(lock, std::chrono::seconds(2), [&] { return writer_holding_execution; });
    }

    std::optional<deltakernel::DeltaDatabase::MaintenanceResult> maintenance_result;
    std::exception_ptr maintenance_error;
    std::thread maintenance_thread;
    if (reached_writer) {
        maintenance_thread = std::thread([&] {
            try {
                maintenance_result = db->MaintenanceTick();
            } catch (...) {
                maintenance_error = std::current_exception();
            }
        });
    }
    bool reached_maintenance = false;
    if (reached_writer) {
        std::unique_lock<std::mutex> lock(gate);
        reached_maintenance = changed.wait_for(lock, std::chrono::seconds(2), [&] { return maintenance_waiting; });
    }
    {
        std::lock_guard<std::mutex> lock(gate);
        release_writer = true;
        changed.notify_all();
    }
    writer_thread.join();
    if (maintenance_thread.joinable())
        maintenance_thread.join();
    db->SetExecuteLockHookForTest({});
    db->SetExecutionWriterWaitHookForTest({});

    ASSERT_TRUE(reached_writer);
    ASSERT_TRUE(reached_maintenance);
    ASSERT_EQ(writer_error, nullptr);
    ASSERT_EQ(maintenance_error, nullptr);
    ASSERT_EQ(maintenance_result, deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
    EXPECT_GT(deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t"), initial_generation);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::SnapshotState(*db)[1], 0U);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(db->WalFrameCountForTest(), 0U);
    deltakernel::DeltaSession reader;
    EXPECT_EQ(Query(*db, reader, "select v from t where id = 1;\n").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, MaintenanceDebtReadWaitsForCommitSync) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession writer;
    RunSql(*db, writer, "create table t (id int, v int);\n");
    RunSql(*db, writer, "begin;\n");
    RunSql(*db, writer, "insert into t values (1, 7);\n");
    db->SetMaintenanceThresholdsForTest(1, 2);

    std::mutex gate;
    std::condition_variable changed;
    bool sync_waiting = false;
    bool release_sync = false;
    bool maintenance_execution_wait = false;
    db->SetCommitSyncHookForTest([&] {
        std::unique_lock<std::mutex> lock(gate);
        sync_waiting = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release_sync; });
    });
    db->SetExecutionWriterWaitHookForTest([&] {
        std::lock_guard<std::mutex> lock(gate);
        maintenance_execution_wait = true;
        changed.notify_all();
    });

    std::exception_ptr writer_error;
    std::thread writer_thread([&] {
        try {
            RunSql(*db, writer, "commit;\n");
        } catch (...) {
            writer_error = std::current_exception();
        }
    });
    const bool reached_sync = WaitForCondition(changed, gate, [&] { return sync_waiting; });

    std::optional<deltakernel::DeltaDatabase::MaintenanceResult> maintenance_result;
    std::exception_ptr maintenance_error;
    std::atomic<bool> maintenance_done{false};
    std::thread maintenance_thread;
    if (reached_sync) {
        maintenance_thread = std::thread([&] {
            try {
                maintenance_result = db->MaintenanceTick();
            } catch (...) {
                maintenance_error = std::current_exception();
            }
            maintenance_done.store(true, std::memory_order_release);
        });
    }
    bool maintenance_started = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (reached_sync && std::chrono::steady_clock::now() < deadline) {
        if (deltakernel::DeltaSnapshotTestPeer::SnapshotBuilderBusy(*db)) {
            maintenance_started = true;
            break;
        }
        std::this_thread::yield();
    }
    bool passed_debt_read = false;
    const bool completed_while_sync_blocked = maintenance_done.load(std::memory_order_acquire);
    {
        std::lock_guard<std::mutex> lock(gate);
        passed_debt_read = maintenance_execution_wait;
        release_sync = true;
        changed.notify_all();
    }
    writer_thread.join();
    if (maintenance_thread.joinable())
        maintenance_thread.join();
    db->SetCommitSyncHookForTest({});
    db->SetExecutionWriterWaitHookForTest({});

    ASSERT_TRUE(reached_sync);
    ASSERT_TRUE(maintenance_started);
    EXPECT_FALSE(completed_while_sync_blocked);
    EXPECT_FALSE(passed_debt_read);
    ASSERT_FALSE(writer_error);
    ASSERT_FALSE(maintenance_error);
    EXPECT_EQ(maintenance_result, deltakernel::DeltaDatabase::MaintenanceResult::kCheckpointed);
}

TEST(DeltaDatabaseTest, SqlCheckpointWaitsForSnapshotBuilder) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t (id int, v int);\n");
    RunSql(*db, setup, "insert into t values (1, 7);\n");

    std::mutex gate;
    std::condition_variable changed;
    bool snapshot_io_waiting = false;
    bool release_snapshot = false;
    bool checkpoint_waiting = false;
    std::atomic<bool> checkpoint_entered{false};
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        std::unique_lock<std::mutex> lock(gate);
        snapshot_io_waiting = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release_snapshot; });
    });
    db->SetSnapshotBuilderWaitHookForTest([&] {
        std::lock_guard<std::mutex> lock(gate);
        checkpoint_waiting = true;
        changed.notify_all();
    });
    db->SetExecuteLockHookForTest([&] { checkpoint_entered.store(true, std::memory_order_release); });

    std::exception_ptr snapshot_error;
    std::thread snapshot_thread([&] {
        try {
            deltakernel::DeltaSnapshotTestPeer::Build(*db);
        } catch (...) {
            snapshot_error = std::current_exception();
        }
    });
    bool reached_snapshot_io = false;
    {
        std::unique_lock<std::mutex> lock(gate);
        reached_snapshot_io = changed.wait_for(lock, std::chrono::seconds(2), [&] { return snapshot_io_waiting; });
    }

    std::exception_ptr checkpoint_error;
    std::thread checkpoint_thread;
    if (reached_snapshot_io) {
        checkpoint_thread = std::thread([&] {
            try {
                deltakernel::DeltaSession checkpoint;
                RunSql(*db, checkpoint, "create static_checkpoint;\n");
            } catch (...) {
                checkpoint_error = std::current_exception();
            }
        });
    }
    bool reached_checkpoint_wait = false;
    if (reached_snapshot_io) {
        std::unique_lock<std::mutex> lock(gate);
        reached_checkpoint_wait = changed.wait_for(lock, std::chrono::seconds(2), [&] { return checkpoint_waiting; });
    }
    EXPECT_FALSE(checkpoint_entered.load(std::memory_order_acquire));
    {
        std::lock_guard<std::mutex> lock(gate);
        release_snapshot = true;
        changed.notify_all();
    }
    snapshot_thread.join();
    if (checkpoint_thread.joinable())
        checkpoint_thread.join();
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    db->SetSnapshotBuilderWaitHookForTest({});
    db->SetExecuteLockHookForTest({});

    ASSERT_TRUE(reached_snapshot_io);
    ASSERT_TRUE(reached_checkpoint_wait);
    ASSERT_FALSE(snapshot_error);
    ASSERT_FALSE(checkpoint_error);
    EXPECT_TRUE(checkpoint_entered.load(std::memory_order_acquire));
    const auto authority = deltakernel::DeltaSnapshotTestPeer::AuthorityState(*db);
    EXPECT_EQ(authority[1], authority[2]);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 0U);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession reader;
    EXPECT_EQ(Query(*db, reader, "select v from t where id = 1;\n").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, StagedSidecarUsesBoundedSortedLayout) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    const auto artifact = deltakernel::DeltaSnapshotTestPeer::BuildStaged(
        *db, "t", 0, {{{0xff, 0x00}, 9}, {{0x00, 0xfe}, 3}, {{0x00, 0xff}, 7}},
        deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t"), deltakernel::DeltaSnapshotTestPeer::Snapshot(*db));
    ASSERT_TRUE(std::filesystem::exists(artifact.path));
    const auto header = ReadSidecarHeader(artifact.path);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 48), 3U);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 64), 6U);
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/deltaidx.1.1"));
}

TEST(DeltaDatabaseTest, StagedSidecarRejectsDuplicateLocalId) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::BuildStaged(*db, "t", 0, {{{1}, 4}, {{2}, 4}}, 1, 0),
                 std::runtime_error);
}

TEST(DeltaDatabaseTest, StagedSidecarSharedValidatorRejectsCorruption) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    const auto generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    const auto snapshot = deltakernel::DeltaSnapshotTestPeer::Snapshot(*db);
    auto artifact = deltakernel::DeltaSnapshotTestPeer::BuildStaged(*db, "t", 0, {{{1, 2}, 4}}, generation, snapshot);
    std::fstream file(artifact.path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file);
    file.seekp(0);
    char byte = 0;
    file.read(&byte, 1);
    byte ^= 1;
    file.seekp(0);
    file.write(&byte, 1);
    file.close();
    EXPECT_FALSE(deltakernel::DeltaSnapshotTestPeer::ValidateStaged(*db, "t", artifact.path, generation, snapshot));
}

TEST(DeltaDatabaseTest, StagedSidecarTokenCleansOwnedFile) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    std::string path;
    {
        auto artifact = deltakernel::DeltaSnapshotTestPeer::BuildStaged(
            *db, "t", 0, {{{1}, 4}}, deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t"),
            deltakernel::DeltaSnapshotTestPeer::Snapshot(*db));
        path = artifact.path;
        ASSERT_TRUE(std::filesystem::exists(path));
    }
    EXPECT_FALSE(std::filesystem::exists(path));
}

TEST(DeltaDatabaseTest, StaleSameKeyCommitAborts) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, stale;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 1);");
    RunSql(*db, first, "begin;");
    RunSql(*db, stale, "begin;");
    RunSql(*db, first, "update t set v = 2 where k = 1;");
    RunSql(*db, stale, "update t set v = 3 where k = 1;");
    RunSql(*db, first, "commit;");
    EXPECT_THROW(RunSql(*db, stale, "commit;"), deltakernel::DeltaTransactionAbort);
}

TEST(DeltaDatabaseTest, ManualWalRotationReopensAndContinues) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "insert into t values(1, 7);");
    db->RotateWalForTest();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    auto result = Query(*db, session, "select v from t where k = 1;");
    ASSERT_EQ(result.rows.size(), 1U);
    EXPECT_EQ(result.rows[0][0], "7");
    db->RotateWalForTest();
    RunSql(*db, session, "insert into t values(2, 8);");
}

TEST(DeltaDatabaseTest, ConsecutiveHeaderOnlyRotationsKeepExactCutBoundary) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    const auto first = db->RotateWalForTest();
    const std::string first_path =
        temp.path() + "/db.log.s." + std::to_string(first[2]) + "." + std::to_string(first[3]);
    EXPECT_EQ(std::filesystem::file_size(first_path), 72U);
    const auto second = db->RotateWalForTest();
    EXPECT_EQ(second[0], first[0]);
    EXPECT_EQ(second[1], first[1]);
    EXPECT_EQ(second[2], first[2]);
    EXPECT_EQ(second[3], first[3] + 1);
    EXPECT_EQ(std::filesystem::file_size(first_path), 72U);
    const std::string second_path =
        temp.path() + "/db.log.s." + std::to_string(second[2]) + "." + std::to_string(second[3]);
    EXPECT_EQ(std::filesystem::file_size(second_path), 72U);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "insert into t values(1);");
    EXPECT_GT(std::filesystem::file_size(second_path), 72U);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
}

TEST(DeltaDatabaseTest, RotationRetainsCheckpointFrontierForPriorSegmentInsert) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "insert into t values(0);");
    db->Checkpoint();
    RunSql(*db, session, "insert into t values(1);");
    db->RotateWalForTest();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select k from t order by k;").rows,
              (std::vector<std::vector<std::string>>{{"0"}, {"1"}}));
}

TEST(DeltaDatabaseTest, FirstRotationRefreshesLegacyReplayBoundaryFrontier) {
    TempDelta temp;
    {
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        deltakernel::DeltaSession session;
        RunSql(*db, session, "create table t(k int);");
        RunSql(*db, session, "insert into t values(0);");
        db->Checkpoint();
    }
    ConvertSegmentedManifestToLegacy(temp);
    {
        auto db = deltakernel::DeltaDatabase::Open(temp.path());
        deltakernel::DeltaSession session;
        RunSql(*db, session, "insert into t values(1);");
        db->RotateWalForTest();
    }
    auto reopened = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession session;
    EXPECT_EQ(Query(*reopened, session, "select k from t order by k;").rows,
              (std::vector<std::vector<std::string>>{{"0"}, {"1"}}));
}

TEST(DeltaDatabaseTest, LegacyRotationCrashRetainsReplayAuthority) {
    TempDelta temp;
    {
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        deltakernel::DeltaSession session;
        RunSql(*db, session, "create table t(k int);");
        RunSql(*db, session, "insert into t values(7);");
        db->Checkpoint();
    }
    ConvertSegmentedManifestToLegacy(temp);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        auto db = deltakernel::DeltaDatabase::Open(temp.path());
        db->SetWalRotationCrashPointForTest(
            epoch_si_poc::CheckpointCrashPoint::kRotationAfterManifestRenameBeforeDirSync);
        try {
            db->RotateWalForTest();
        } catch (...) {
            kill(getpid(), SIGKILL);
        }
        _exit(1);
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    auto reopened = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession reader;
    EXPECT_EQ(Query(*reopened, reader, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, SnapshotPublishCannotRestoreLegacyReplayOnLaterRotation) {
    TempDelta temp;
    {
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        deltakernel::DeltaSession setup;
        RunSql(*db, setup, "create table t(k int);");
        RunSql(*db, setup, "insert into t values(7);");
        db->Checkpoint();
    }
    ConvertSegmentedManifestToLegacy(temp);
    auto db = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession writer;
    RunSql(*db, writer, "update t set k = 7 where k = 7;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    db->RotateWalForTest();
    db.reset();
    const auto manifest = ReadDeltaFile(temp.path() + "/MANIFEST");
    const uint32_t table_count = GetDeltaLe<uint32_t>(manifest, 36);
    const size_t chain = 44 + static_cast<size_t>(table_count) * 40;
    EXPECT_EQ(GetDeltaLe<uint32_t>(manifest, chain + 8), 0U);
    db = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession reader;
    EXPECT_EQ(Query(*db, reader, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, RetainedHighSegmentRangeCanRotate) {
    TempDelta temp;
    { auto db = deltakernel::DeltaDatabase::Create(temp.path()); }
    RebaseDeltaWalSegment(temp, 5000);
    auto db = deltakernel::DeltaDatabase::Open(temp.path());
    const auto boundary = db->RotateWalForTest();
    EXPECT_EQ(boundary[3], 5001U);
    EXPECT_TRUE(std::filesystem::is_regular_file(temp.path() + "/db.log.s.0.5001"));
    db.reset();
    EXPECT_NO_THROW(db = deltakernel::DeltaDatabase::Open(temp.path()));
}

TEST(DeltaDatabaseTest, RotationRejectsNonRegularFutureResidue) {
    {
        TempDelta temp;
        TempDelta external;
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        const std::string future = temp.path() + "/db.log.s.0.1";
        ASSERT_EQ(symlink(external.path().c_str(), future.c_str()), 0);
        EXPECT_THROW(db->RotateWalForTest(), std::runtime_error);
        EXPECT_TRUE(std::filesystem::is_symlink(future));
        db.reset();
        EXPECT_NO_THROW(db = deltakernel::DeltaDatabase::Open(temp.path()));
    }
    {
        TempDelta temp;
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        const std::string future = temp.path() + "/db.log.s.0.1";
        ASSERT_TRUE(std::filesystem::create_directory(future));
        EXPECT_THROW(db->RotateWalForTest(), std::runtime_error);
        EXPECT_TRUE(std::filesystem::is_directory(future));
        db.reset();
        EXPECT_NO_THROW(db = deltakernel::DeltaDatabase::Open(temp.path()));
    }
}

TEST(DeltaDatabaseTest, ManualWalRotationCrashPointsLeaveAnAuthoritativeGeneration) {
    for (const auto point : {epoch_si_poc::CheckpointCrashPoint::kRotationAfterHeaderWrite,
                             epoch_si_poc::CheckpointCrashPoint::kRotationAfterHeaderSync,
                             epoch_si_poc::CheckpointCrashPoint::kRotationAfterDirSync,
                             epoch_si_poc::CheckpointCrashPoint::kRotationDuringManifestTemp,
                             epoch_si_poc::CheckpointCrashPoint::kRotationAfterManifestRenameBeforeDirSync,
                             epoch_si_poc::CheckpointCrashPoint::kRotationAfterSwitch}) {
        TempDelta temp;
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        deltakernel::DeltaSession session;
        RunSql(*db, session, "create table t(k int, v int);");
        RunSql(*db, session, "insert into t values(1, 7);");
        db->SetWalRotationCrashPointForTest(point);
        EXPECT_THROW(db->RotateWalForTest(), epoch_si_poc::SimulatedCrash);
        db.reset();
        db = deltakernel::DeltaDatabase::Open(temp.path());
        auto result = Query(*db, session, "select v from t where k = 1;");
        ASSERT_EQ(result.rows.size(), 1U);
        EXPECT_EQ(result.rows[0][0], "7");
        if (point == epoch_si_poc::CheckpointCrashPoint::kRotationAfterHeaderWrite ||
            point == epoch_si_poc::CheckpointCrashPoint::kRotationAfterHeaderSync ||
            point == epoch_si_poc::CheckpointCrashPoint::kRotationAfterDirSync ||
            point == epoch_si_poc::CheckpointCrashPoint::kRotationDuringManifestTemp) {
            EXPECT_NO_THROW(db->RotateWalForTest());
            db.reset();
            db = deltakernel::DeltaDatabase::Open(temp.path());
            EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows,
                      (std::vector<std::vector<std::string>>{{"7"}}));
        }
    }
}

TEST(DeltaDatabaseTest, ConcurrentCommitsShareOneWalFrame) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    deltakernel::DeltaSession first;
    deltakernel::DeltaSession second;
    RunSql(*db, first, "begin;");
    RunSql(*db, first, "insert into t values(1, 10);");
    RunSql(*db, second, "begin;");
    RunSql(*db, second, "insert into t values(1, 20);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool leader_waiting = false;
    bool release = false;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        leader_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    const size_t frames_before = db->WalFrameCountForTest();
    std::thread first_committer([&] { RunSql(*db, first, "commit;"); });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return leader_waiting; });
    }
    std::thread second_committer([&] { RunSql(*db, second, "commit;"); });
    const bool both_queued = WaitForCommitQueueDepth(*db, 2);
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    first_committer.join();
    second_committer.join();
    db->SetCommitBatchHookForTest({});
    ASSERT_TRUE(both_queued);
    EXPECT_EQ(db->WalFrameCountForTest(), frames_before + 1);
    EXPECT_EQ(Query(*db, setup, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"20"}}));
}

TEST(DeltaDatabaseTest, ConcurrentConflictCommitsSharePublication) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, stale, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 1);");
    RunSql(*db, first, "begin;");
    RunSql(*db, stale, "begin;");
    RunSql(*db, first, "update t set v = 2 where k = 1;");
    RunSql(*db, stale, "update t set v = 3 where k = 1;");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool leader_waiting = false;
    bool release = false;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        leader_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    const size_t frames_before = db->WalFrameCountForTest();
    std::exception_ptr first_error;
    std::exception_ptr stale_error;
    std::thread first_committer([&] {
        try {
            RunSql(*db, first, "commit;");
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return leader_waiting; });
    }
    std::thread stale_committer([&] {
        try {
            RunSql(*db, stale, "commit;");
        } catch (...) {
            stale_error = std::current_exception();
        }
    });
    const bool both_queued = WaitForCommitQueueDepth(*db, 2);
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    first_committer.join();
    stale_committer.join();
    db->SetCommitBatchHookForTest({});
    ASSERT_TRUE(both_queued);
    EXPECT_FALSE(first_error);
    EXPECT_TRUE(stale_error);
    EXPECT_EQ(db->WalFrameCountForTest(), frames_before + 1);
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, IndexedCommitPublishesRowsAndOverlayTogether) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, writer, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(7, 9);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool installing = false;
    bool release = false;
    std::atomic<bool> reader_done{false};
    CapturingSink result;
    db->SetCommitInstallHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        installing = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    bool reader_blocked = false;
    db->SetExecuteBlockedHookForTest([&] {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        reader_blocked = true;
        barrier.notify_all();
    });
    std::thread committer([&] { RunSql(*db, writer, "commit;"); });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return installing; });
    }
    std::thread reader_thread([&] {
        result = Query(*db, reader, "select v from t where k = 7;");
        reader_done = true;
    });
    bool observed_blocked;
    bool completed_before_release;
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        observed_blocked = barrier.wait_for(lock, std::chrono::seconds(2), [&] { return reader_blocked; });
        completed_before_release = reader_done;
        release = true;
    }
    barrier.notify_all();
    committer.join();
    reader_thread.join();
    db->SetCommitInstallHookForTest({});
    db->SetExecuteBlockedHookForTest({});
    EXPECT_TRUE(observed_blocked);
    EXPECT_FALSE(completed_before_release);
    EXPECT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"9"}}));
}

TEST(DeltaDatabaseTest, WalSyncRunsWithoutStateLockAndExclusiveWorkWaitsForCommit) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, writer, reader, disjoint, ddl, checkpoint;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 1);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "update t set v = 9 where k = 1;");
    RunSql(*db, disjoint, "begin;");
    RunSql(*db, disjoint, "insert into t values(2, 2);");
    RunSql(*db, reader, "begin;");

    std::mutex mutex;
    std::condition_variable condition;
    bool syncing = false;
    bool release = false;
    int exclusive_waiters = 0;
    int sync_calls = 0;
    std::atomic<bool> ddl_done{false};
    std::atomic<bool> checkpoint_done{false};
    std::exception_ptr writer_error, disjoint_error, ddl_error, checkpoint_error;
    db->SetCommitSyncHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (sync_calls++ != 0)
            return;
        syncing = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release; });
    });
    db->SetExecutionWriterWaitHookForTest([&] {
        std::lock_guard<std::mutex> lock(mutex);
        ++exclusive_waiters;
        condition.notify_all();
    });

    const size_t frames_before = db->WalFrameCountForTest();
    std::thread first([&] {
        try {
            RunSql(*db, writer, "commit;");
        } catch (...) {
            writer_error = std::current_exception();
        }
    });
    const bool saw_sync = WaitForCondition(condition, mutex, [&] { return syncing; });
    CapturingSink old;
    if (saw_sync) {
        old = Query(*db, reader, "select v from t where k = 1;");
        RunSql(*db, reader, "rollback;");
    }

    std::thread second([&] {
        try {
            RunSql(*db, disjoint, "commit;");
        } catch (...) {
            disjoint_error = std::current_exception();
        }
    });
    const bool second_queued = saw_sync && WaitForCommitQueueDepth(*db, 1);
    const size_t frames_while_syncing = db->WalFrameCountForTest();

    std::thread ddl_thread([&] {
        try {
            RunSql(*db, ddl, "create table later(k int);");
            ddl_done = true;
        } catch (...) {
            ddl_error = std::current_exception();
        }
    });
    std::thread checkpoint_thread([&] {
        try {
            db->Checkpoint();
            checkpoint_done = true;
        } catch (...) {
            checkpoint_error = std::current_exception();
        }
    });
    const bool saw_exclusive_wait =
        second_queued && WaitForCondition(condition, mutex, [&] { return exclusive_waiters >= 1; });
    const bool ddl_finished_early = ddl_done.load();
    const bool checkpoint_finished_early = checkpoint_done.load();

    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    condition.notify_all();
    first.join();
    second.join();
    ddl_thread.join();
    checkpoint_thread.join();
    db->SetCommitSyncHookForTest({});
    db->SetExecutionWriterWaitHookForTest({});
    EXPECT_TRUE(saw_sync);
    EXPECT_EQ(old.rows, (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_TRUE(second_queued);
    EXPECT_EQ(frames_while_syncing, frames_before); // The queued second commit has not appended a frame.
    EXPECT_TRUE(saw_exclusive_wait);
    EXPECT_FALSE(ddl_finished_early);
    EXPECT_FALSE(checkpoint_finished_early);
    EXPECT_FALSE(writer_error);
    EXPECT_FALSE(disjoint_error);
    EXPECT_FALSE(ddl_error);
    EXPECT_FALSE(checkpoint_error);
    EXPECT_TRUE(ddl_done.load());
    EXPECT_TRUE(checkpoint_done.load());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, CommitQueuedDuringUnlockedSyncCertifiesSameRowAgainstPublication) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, winner, stale, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 1);");
    RunSql(*db, winner, "begin;");
    RunSql(*db, stale, "begin;");
    RunSql(*db, winner, "update t set v = 2 where k = 1;");
    RunSql(*db, stale, "update t set v = 3 where k = 1;");

    std::mutex mutex;
    std::condition_variable condition;
    bool syncing = false;
    bool release = false;
    db->SetCommitSyncHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        syncing = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release; });
    });
    std::exception_ptr winner_error, stale_error;
    std::thread first([&] {
        try {
            RunSql(*db, winner, "commit;");
        } catch (...) {
            winner_error = std::current_exception();
        }
    });
    const bool saw_sync = WaitForCondition(condition, mutex, [&] { return syncing; });
    std::thread second([&] {
        try {
            RunSql(*db, stale, "commit;");
        } catch (...) {
            stale_error = std::current_exception();
        }
    });
    const bool second_queued = saw_sync && WaitForCommitQueueDepth(*db, 1);
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    condition.notify_all();
    first.join();
    second.join();
    db->SetCommitSyncHookForTest({});
    EXPECT_TRUE(saw_sync);
    EXPECT_TRUE(second_queued);
    EXPECT_FALSE(winner_error);
    EXPECT_TRUE(stale_error);
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, ReacquireFailureAfterDurableSyncTerminates) {
    EXPECT_DEATH(([&] {
                     TempDelta temp;
                     auto db = deltakernel::DeltaDatabase::Create(temp.path());
                     deltakernel::DeltaSession setup, writer;
                     RunSql(*db, setup, "create table t(k int);");
                     RunSql(*db, writer, "begin;");
                     RunSql(*db, writer, "insert into t values(1);");
                     db->SetCommitReacquireHookForTest([] { throw std::runtime_error("test reacquire failure"); });
                     RunSql(*db, writer, "commit;");
                 }()),
                 "");
}

TEST(DeltaDatabaseTest, WalSyncFailurePoisonsDatabaseBeforeAnyFurtherAck) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, writer, next;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(1);");
    const size_t frames_before = db->WalFrameCountForTest();
    db->CloseWalForTest();
    EXPECT_THROW(RunSql(*db, writer, "commit;"), std::system_error);
    EXPECT_EQ(db->WalFrameCountForTest(), frames_before);
    EXPECT_THROW(Query(*db, next, "show tables;"), std::logic_error);
    EXPECT_THROW(RunSql(*db, next, "begin;"), std::logic_error);
    EXPECT_THROW(RunSql(*db, next, "create table later(k int);"), std::logic_error);
    EXPECT_THROW(db->Checkpoint(), std::logic_error);
}

TEST(DeltaDatabaseTest, CommitLeaderFailureWakesQueuedWaiters) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, second;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, first, "begin;");
    RunSql(*db, first, "insert into t values(1);");
    RunSql(*db, second, "begin;");
    RunSql(*db, second, "insert into t values(2);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool leader_waiting = false;
    bool release = false;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        leader_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
        throw std::runtime_error("test leader failure");
    });
    std::exception_ptr first_error;
    std::exception_ptr second_error;
    std::thread first_committer([&] {
        try {
            RunSql(*db, first, "commit;");
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return leader_waiting; });
    }
    std::thread second_committer([&] {
        try {
            RunSql(*db, second, "commit;");
        } catch (...) {
            second_error = std::current_exception();
        }
    });
    const bool both_queued = WaitForCommitQueueDepth(*db, 2);
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    first_committer.join();
    second_committer.join();
    db->SetCommitBatchHookForTest({});
    ASSERT_TRUE(both_queued);
    EXPECT_TRUE(first_error);
    EXPECT_TRUE(second_error);
    EXPECT_EQ(db->CommitQueueDepthForTest(), 0U);
    EXPECT_TRUE(Query(*db, setup, "select k from t;").rows.empty());
}

TEST(DeltaDatabaseTest, DurableInstallFailureTerminates) {
    TempDelta temp;
    EXPECT_DEATH(
        {
            auto db = deltakernel::DeltaDatabase::Create(temp.path());
            deltakernel::DeltaSession session;
            RunSql(*db, session, "create table t(k int);");
            RunSql(*db, session, "create index t_k on t(k);");
            RunSql(*db, session, "begin;");
            RunSql(*db, session, "insert into t values(1);");
            db->SetCommitInstallHookForTest([] { throw std::runtime_error("test install failure"); });
            RunSql(*db, session, "commit;");
        },
        "");
}

TEST(DeltaDatabaseTest, StreamingAggregateAndBoundedOrder) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v float, s char(8));");
    RunSql(*db, session, "insert into t values(2, 1.25, 'b');");
    RunSql(*db, session, "insert into t values(1, 2.50, 'a');");
    RunSql(*db, session, "insert into t values(3, 1.25, 'a');");
    auto aggregate = Query(*db, session, "select count(*), count(distinct s), min(k), sum(v) from t where k >= 1;");
    ASSERT_EQ(aggregate.rows.size(), 1);
    ASSERT_EQ(aggregate.rows[0][0], "3");
    ASSERT_EQ(aggregate.rows[0][1], "2");
    ASSERT_EQ(aggregate.rows[0][2], "1");
    uint32_t sum_bits;
    float sum = 5.0F;
    std::memcpy(&sum_bits, &sum, sizeof(sum_bits));
    ASSERT_EQ(aggregate.rows[0][3], std::to_string(sum_bits));
    auto ordered = Query(*db, session, "select k from t order by s asc, k desc limit 1 offset 1;");
    ASSERT_EQ(ordered.rows, (std::vector<std::vector<std::string>>{{"1"}}));
    const auto description =
        db->DescribePrepared(*ast::parse_sql("select min(k), sum(v), count(*) from t order by k desc limit 1;"), {});
    ASSERT_EQ(description.types, (std::vector<deltakernel::DeltaValueType>{deltakernel::DeltaValueType::Int,
                                                                           deltakernel::DeltaValueType::Float,
                                                                           deltakernel::DeltaValueType::Int}));
}

TEST(DeltaDatabaseTest, MultiTableDistinctCount) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table order_line(ol_w_id int, ol_d_id int, ol_o_id int, ol_i_id int);");
    RunSql(*db, session, "create table stock(s_w_id int, s_i_id int, s_quantity int);");
    RunSql(*db, session, "insert into order_line values(1, 1, 1, 10);");
    RunSql(*db, session, "insert into order_line values(1, 1, 2, 10);");
    RunSql(*db, session, "insert into order_line values(1, 1, 3, 11);");
    RunSql(*db, session, "insert into stock values(1, 10, 9);");
    RunSql(*db, session, "insert into stock values(1, 11, 20);");
    auto result = Query(*db, session,
                        "select count(distinct ol_i_id) from order_line, stock where ol_w_id = 1 and ol_d_id = 1 "
                        "and ol_o_id < 20 and s_w_id = 1 and ol_i_id = s_i_id and s_quantity < 15;");
    ASSERT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"1"}}));
}

TEST(DeltaDatabaseTest, MultiTableDistinctFloatUsesSqlEqualityWithoutDecimalCollapse) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table lhs(k int);");
    RunSql(*db, session, "create table rhs(k int, v float);");
    RunSql(*db, session, "insert into rhs values(0, 99.0);");
    RunSql(*db, session, "create index rhs_k on rhs(k);");
    RunSql(*db, session, "insert into lhs values(1);");
    RunSql(*db, session, "insert into lhs values(2);");
    RunSql(*db, session, "insert into lhs values(3);");
    RunSql(*db, session, "insert into lhs values(4);");
    RunSql(*db, session, "insert into rhs values(1, 1.00000011920928955078125);");
    RunSql(*db, session, "insert into rhs values(2, 1.0000002384185791015625);");
    RunSql(*db, session, "insert into rhs values(3, 0.0);");
    RunSql(*db, session, "insert into rhs values(4, -0.0);");

    const auto query = [&](const char* expression) {
        return Query(*db, session,
                     (std::string("select ") + expression + " from lhs l, rhs r where l.k > 0 and l.k = r.k;").c_str())
            .rows;
    };
    EXPECT_EQ(query("count(distinct r.v)"), (std::vector<std::vector<std::string>>{{"3"}}));
    EXPECT_EQ(query("count(distinct (r.v))"), (std::vector<std::vector<std::string>>{{"3"}}));
}

TEST(DeltaDatabaseTest, MultiTableRightAggregatesUseBoundSourceType) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table lhs(id int);");
    RunSql(*db, session, "create table rhs(id int, v int);");
    RunSql(*db, session, "insert into lhs values(1);");
    RunSql(*db, session, "insert into lhs values(2);");
    RunSql(*db, session, "insert into lhs values(3);");
    RunSql(*db, session, "insert into rhs values(1, 5);");
    RunSql(*db, session, "insert into rhs values(2, 7);");
    RunSql(*db, session, "insert into rhs values(4, 99);");

    EXPECT_EQ(Query(*db, session, "select count(r.v) from lhs l, rhs r where l.id = r.id;").rows,
              (std::vector<std::vector<std::string>>{{"2"}}));
    EXPECT_EQ(Query(*db, session, "select sum(r.v) from lhs l, rhs r where l.id = r.id;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
    EXPECT_EQ(Query(*db, session, "select min(r.v) from lhs l, rhs r where l.id = r.id;").rows,
              (std::vector<std::vector<std::string>>{{"5"}}));
    EXPECT_EQ(Query(*db, session, "select max(r.v) from lhs l, rhs r where l.id = r.id;").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));
    EXPECT_EQ(Query(*db, session, "select sum(r.v) from lhs l, rhs r where l.id = r.id and l.id < 0;").rows,
              (std::vector<std::vector<std::string>>{{"NULL"}}));
    EXPECT_THROW(Query(*db, session, "select count(id) from lhs l, rhs r where l.id = r.id;"), std::runtime_error);
}

TEST(DeltaDatabaseTest, MultiTableSelfJoinKeepsAmbiguousLiteralForFullResolution) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "insert into t values(1);");
    EXPECT_THROW(Query(*db, session, "select count(distinct k) from t, t where t.k < 0;"), std::runtime_error);
}

TEST(DeltaDatabaseTest, MultiTableStreamingProjection) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table a(k int, s char(8));");
    RunSql(*db, session, "create table b(k int, v float);");
    RunSql(*db, session, "insert into a values(1, 'one');");
    RunSql(*db, session, "insert into a values(2, 'two');");
    RunSql(*db, session, "insert into b values(2, 3.5);");
    RunSql(*db, session, "insert into b values(1, 9.5);");
    auto result = Query(*db, session, "select a.s, b.v from a, b where a.k = b.k and a.k > 1 and b.v < 9.0;");
    ASSERT_EQ(result.rows.size(), 1);
    ASSERT_EQ(result.rows[0][0], "two");
    EXPECT_EQ(Query(*db, session, "select a.s from a, b where a.k = b.k and a.k = 3;").begin_count, 1);
    CapturingSink invalid;
    EXPECT_THROW(db->Execute(ast::parse_sql("select a.s from a, b where a.missing = b.k;"), session, &invalid),
                 std::runtime_error);
    EXPECT_EQ(invalid.begin_count, 0);
}

TEST(DeltaDatabaseTest, MultiTablePairLoopContinuesAfterProjectionAndDistinctDuplicate) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table a(k int, v int);");
    RunSql(*db, session, "create table b(k int);");
    RunSql(*db, session, "insert into a values(1, 10);");
    RunSql(*db, session, "insert into a values(1, 10);");
    RunSql(*db, session, "insert into a values(1, 20);");
    RunSql(*db, session, "insert into a values(1, null);");
    RunSql(*db, session, "insert into b values(1);");
    ASSERT_EQ(Query(*db, session, "select a.v from a, b where a.k = b.k;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"10"}, {"20"}, {"NULL"}}));
    ASSERT_EQ(Query(*db, session, "select count(distinct a.v) from a, b where a.k = b.k;").rows,
              (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, MultiTableJoinAccountsForUniqueRowClaims) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table a(k int);");
    RunSql(*db, session, "create table b(k int);");
    db.reset();
    WriteCatalog(temp.path() + "/DELTA_CATALOG",
                 "DELTAKERNEL\n3 3 2 2\nTABLE 1 1 a 1 1\nCOLUMN 0 4 1 k\nINDEX 1 1 1 0\n"
                 "TABLE 2 1 b 1 0\nCOLUMN 0 4 1 k\n");
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "insert into a values(7);");
    RunSql(*db, session, "insert into b values(7);");
    ASSERT_EQ(Query(*db, session, "select a.k from a, b where a.k = b.k;").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, MultiTableCustomerWarehouseProjection) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table customer(c_w_id int, c_id int, c_last char(8));");
    RunSql(*db, session, "create table warehouse(w_id int, w_name char(8));");
    RunSql(*db, session, "insert into customer values(1, 7, 'smith');");
    RunSql(*db, session, "insert into customer values(2, 8, 'jones');");
    RunSql(*db, session, "insert into warehouse values(1, 'north');");
    RunSql(*db, session, "insert into warehouse values(2, 'south');");
    const auto result = Query(*db, session,
                              "select c.c_last, w.w_name from customer c, warehouse w "
                              "where c.c_w_id = w.w_id and c.c_id = 7;");
    ASSERT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"smith", "north"}}));
}

TEST(DeltaDatabaseTest, ParameterizedJoinBoundsStockLevelShapedInnerReads) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table facts(g int, b int, seq int, item int);");
    RunSql(*db, session, "create table dimensions(g int, item int, quantity int);");
    RunSql(*db, session, "insert into facts values(0, 0, -1, -1);");
    RunSql(*db, session, "insert into dimensions values(0, -1, 99);");
    RunSql(*db, session, "create index facts_gbs on facts(g, b, seq);");
    RunSql(*db, session, "create index dimensions_gi on dimensions(g, item);");
    RunSql(*db, session, "begin;");
    for (int item = 0; item < 10000; ++item)
        RunSql(*db, session, ("insert into dimensions values(1, " + std::to_string(item) + ", 1);").c_str());
    for (int item = 5000; item < 5200; ++item)
        RunSql(*db, session,
               ("insert into facts values(1, 2, " + std::to_string(item - 5000) + ", " + std::to_string(item) + ");")
                   .c_str());
    RunSql(*db, session, "commit;");

    EXPECT_EQ(Query(*db, session,
                    "select count(distinct facts.item) from facts, dimensions where facts.g = 1 and facts.b = 2 "
                    "and facts.seq >= 0 and facts.seq < 200 and dimensions.g = 1 and dimensions.item = facts.item "
                    "and dimensions.quantity < 2;")
                  .rows,
              (std::vector<std::vector<std::string>>{{"200"}}));
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{200, 200, 200, 0, 0, 0, 200, 200}));
}

TEST(DeltaDatabaseTest, ParameterizedJoinPreservesDuplicateOuterAndNonUniqueInnerRows) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table sources(k int);");
    RunSql(*db, session, "create table targets(k int, v int);");
    RunSql(*db, session, "insert into targets values(4, 10);");
    RunSql(*db, session, "create index targets_k on targets(k);");
    RunSql(*db, session, "insert into sources values(4);");
    RunSql(*db, session, "insert into sources values(4);");
    RunSql(*db, session, "insert into sources values(9);");
    RunSql(*db, session, "insert into targets values(4, 11);");

    EXPECT_EQ(Query(*db, session, "select targets.v from sources, targets where sources.k = targets.k;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"11"}, {"10"}, {"11"}}));
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{3, 4, 4, 3, 0, 2, 2, 2}));
    EXPECT_TRUE(
        Query(*db, session, "select targets.v from sources, targets where sources.k = targets.k and sources.k = 9;")
            .rows.empty());
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{1, 0, 0, 3, 0, 0, 0, 0}));
}

TEST(DeltaDatabaseTest, ParameterizedJoinCensusCountsEveryNonUniqueOverlayRowId) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table sources(k int);");
    RunSql(*db, session, "create table targets(k int, v int);");
    RunSql(*db, session, "insert into targets values(7, 0);");
    RunSql(*db, session, "create index targets_k on targets(k);");
    RunSql(*db, session, "insert into sources values(7);");
    RunSql(*db, session, "begin;");
    for (int value = 1; value <= 64; ++value)
        RunSql(*db, session, ("insert into targets values(7, " + std::to_string(value) + ");").c_str());
    RunSql(*db, session, "commit;");

    EXPECT_EQ(Query(*db, session, "select count(targets.v) from sources, targets where sources.k = targets.k;").rows,
              (std::vector<std::vector<std::string>>{{"65"}}));
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{1, 65, 65, 1, 0, 1, 1, 64}));
}

TEST(DeltaDatabaseTest, ParameterizedJoinBuildsTypedCompositeKeyFromLiteralAndOuterValues) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table sources(i int, f float, label char(8));");
    RunSql(*db, session, "create table targets(tag char(8), f float, i int, v int);");
    RunSql(*db, session, "insert into targets values('seed', -99.0, -1, -1);");
    RunSql(*db, session, "create index targets_tfi on targets(tag, f, i);");
    RunSql(*db, session, "insert into sources values(1, -2.5, 'low');");
    RunSql(*db, session, "insert into sources values(2, 0.0, 'zero');");
    RunSql(*db, session, "insert into targets values('bucket', -2.5, 1, 10);");
    RunSql(*db, session, "insert into targets values('bucket', -0.0, 2, 20);");
    RunSql(*db, session, "insert into targets values('other', -2.5, 1, 99);");

    EXPECT_EQ(Query(*db, session,
                    "select sources.label, targets.v from sources, targets where targets.tag = 'bucket' and "
                    "sources.f = targets.f and targets.i = sources.i;")
                  .rows,
              (std::vector<std::vector<std::string>>{{"low", "10"}, {"zero", "20"}}));
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{2, 2, 2, 2, 0, 0, 2, 2}));
}

TEST(DeltaDatabaseTest, ParameterizedJoinRechecksSnapshotAndPrivateKeyMoves) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, updater, fresh, local;
    RunSql(*db, setup, "create table sources(k int);");
    RunSql(*db, setup, "create table targets(k int, v int);");
    RunSql(*db, setup, "insert into targets values(0, 0);");
    RunSql(*db, setup, "create index targets_k on targets(k);");
    RunSql(*db, setup, "insert into sources values(1);");
    RunSql(*db, setup, "insert into sources values(2);");
    RunSql(*db, setup, "insert into sources values(3);");
    RunSql(*db, setup, "insert into targets values(1, 9);");
    RunSql(*db, old_snapshot, "begin;");
    RunSql(*db, updater, "update targets set k = 2 where k = 1;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);

    EXPECT_EQ(Query(*db, old_snapshot, "select sources.k from sources, targets where sources.k = targets.k;").rows,
              (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_EQ(Query(*db, fresh, "select sources.k from sources, targets where sources.k = targets.k;").rows,
              (std::vector<std::vector<std::string>>{{"2"}}));
    RunSql(*db, local, "begin;");
    RunSql(*db, local, "update targets set k = 3 where k = 2;");
    EXPECT_EQ(Query(*db, local, "select sources.k from sources, targets where sources.k = targets.k;").rows,
              (std::vector<std::vector<std::string>>{{"3"}}));
    RunSql(*db, local, "rollback;");
    EXPECT_EQ(Query(*db, fresh, "select sources.k from sources, targets where sources.k = targets.k;").rows,
              (std::vector<std::vector<std::string>>{{"2"}}));
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, ParameterizedJoinFallsBackWhenFullInnerKeyIsNotProven) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table sources(k int);");
    RunSql(*db, session, "create table targets(part int, k int);");
    RunSql(*db, session, "insert into targets values(99, -1);");
    RunSql(*db, session, "create index targets_pk on targets(part, k);");
    RunSql(*db, session, "insert into sources values(1);");
    RunSql(*db, session, "insert into sources values(2);");
    RunSql(*db, session, "insert into targets values(7, 1);");
    RunSql(*db, session, "insert into targets values(8, 2);");
    RunSql(*db, session, "insert into targets values(9, 3);");

    EXPECT_EQ(Query(*db, session, "select sources.k from sources, targets where targets.k = sources.k;").rows,
              (std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{0, 0, 8, 6, 4, 0, 0, 0}));
}

TEST(DeltaDatabaseTest, JoinPropagatesTransitiveLiteralToBoundOuterIndex) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table ledger_keys(shard int, bridge int, slot int, payload int);");
    RunSql(*db, session, "create table routing_keys(route int);");
    RunSql(*db, session, "begin;");
    for (int key = 100; key < 10100; ++key) {
        RunSql(*db, session,
               ("insert into ledger_keys values(" + std::to_string(key) + ", " + std::to_string(key) + ", 3, -1);")
                   .c_str());
    }
    RunSql(*db, session, "insert into ledger_keys values(7, 7, 3, 41);");
    RunSql(*db, session, "insert into ledger_keys values(7, 7, 3, 42);");
    RunSql(*db, session, "insert into routing_keys values(7);");
    RunSql(*db, session, "commit;");
    RunSql(*db, session, "create index ledger_keys_ss on ledger_keys(shard, slot);");
    RunSql(*db, session, "create index routing_keys_r on routing_keys(route);");

    const char* forward = "select l.payload from ledger_keys l, routing_keys r where l.shard = l.bridge and "
                          "l.bridge = r.route and r.route = 7 and l.slot = 3;";
    const auto before_forward = db->JoinOuterDiagnosticsForTest();
    EXPECT_EQ(Query(*db, session, forward).rows, (std::vector<std::vector<std::string>>{{"41"}, {"42"}}));
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{1, 2, 0, 1, 1}));
    if (!db->DiagnosticsEnabled()) {
        EXPECT_EQ(db->JoinOuterDiagnosticsForTest(), before_forward);
    }

    db->EnableDiagnosticsForTest();
    const auto before_reverse = db->JoinOuterDiagnosticsForTest();
    EXPECT_EQ(Query(*db, session,
                    "select l.payload from ledger_keys l, routing_keys r where r.route = l.bridge and "
                    "l.bridge = l.shard and l.slot = 3 and r.route = 7;")
                  .rows,
              (std::vector<std::vector<std::string>>{{"41"}, {"42"}}));
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{1, 2, 0, 1, 1}));
    const auto after_reverse = db->JoinOuterDiagnosticsForTest();
    EXPECT_EQ(after_reverse[0] - before_reverse[0], 1U);
    EXPECT_EQ(after_reverse[1] - before_reverse[1], 2U);
    EXPECT_EQ(after_reverse[2] - before_reverse[2], 0U);
    EXPECT_EQ(after_reverse[3] - before_reverse[3], 1U);
    EXPECT_EQ(after_reverse[4] - before_reverse[4], 1U);

    EXPECT_EQ(Query(*db, session,
                    "select l.payload from routing_keys r, ledger_keys l where l.bridge = r.route and "
                    "l.shard = l.bridge and r.route = 7 and l.slot = 3;")
                  .rows,
              (std::vector<std::vector<std::string>>{{"41"}, {"42"}}));
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{1, 1, 0, 1, 0}));

    EXPECT_TRUE(Query(*db, session,
                      "select l.payload from ledger_keys l, routing_keys r where l.shard = r.route and "
                      "r.route = 7 and l.shard = 8 and l.slot = 3;")
                    .rows.empty());
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{0, 0, 0, 0, 0}));
    EXPECT_TRUE(Query(*db, session,
                      "select l.payload from ledger_keys l, routing_keys r where l.shard = r.route and "
                      "r.route = null and l.slot = 3;")
                    .rows.empty());
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{0, 0, 0, 0, 0}));
}

TEST(DeltaDatabaseTest, JoinPropagatedTypedKeysPreserveSqlEqualityAndFallback) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table measurements(i int, f float, s char(8), v int);");
    RunSql(*db, session, "create table filters(i int, f float, s char(8));");
    RunSql(*db, session, "insert into measurements values(7, -0.0, 'key', 1);");
    RunSql(*db, session, "insert into measurements values(7, 0.0, 'key', 2);");
    RunSql(*db, session, "insert into measurements values(8, 1.0, 'other', 3);");
    RunSql(*db, session, "insert into filters values(7, 0.0, 'key');");
    RunSql(*db, session, "create index measurements_ifs on measurements(i, f, s);");
    RunSql(*db, session, "create index filters_ifs on filters(i, f, s);");

    EXPECT_EQ(Query(*db, session,
                    "select m.v from measurements m, filters q where m.i = q.i and m.f = q.f and m.s = q.s "
                    "and q.i = 7 and q.f = 0.0 and q.s = 'key';")
                  .rows,
              (std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{1, 2, 0, 3, 1}));

    EXPECT_TRUE(Query(*db, session,
                      "select m.v from measurements m, filters q where m.s = q.s and q.s = 'key' "
                      "and m.s = 'other';")
                    .rows.empty());
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{0, 0, 0, 0, 0}));

    EXPECT_EQ(
        Query(*db, session, "select m.v from measurements m, filters q where m.f = q.f and q.f = 0.0;").rows.size(),
        2U);
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{0, 0, 3, 1, 0}));
}

TEST(DeltaDatabaseTest, JoinPropagatedOuterIndexRechecksSnapshotAndPrivateMoves) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, updater, fresh, local;
    RunSql(*db, setup, "create table events(k int, v int);");
    RunSql(*db, setup, "create table selectors(k int);");
    RunSql(*db, setup, "insert into events values(7, 9);");
    RunSql(*db, setup, "insert into selectors values(7);");
    RunSql(*db, setup, "create index events_k on events(k);");
    RunSql(*db, setup, "create index selectors_k on selectors(k);");
    RunSql(*db, old_snapshot, "begin;");
    RunSql(*db, updater, "update events set k = 8 where k = 7;");
    const char* query = "select e.v from events e, selectors s where e.k = s.k and s.k = 7;";

    EXPECT_EQ(Query(*db, old_snapshot, query).rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_TRUE(Query(*db, fresh, query).rows.empty());
    RunSql(*db, local, "begin;");
    RunSql(*db, local, "update events set k = 7 where k = 8;");
    EXPECT_EQ(Query(*db, local, query).rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(local), (std::array<size_t, 5>{1, 1, 0, 1, 1}));
    RunSql(*db, local, "rollback;");
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, NewOrderShapeUsesPropagatedCustomerCompositeIndex) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session,
           "create table customer(c_id int, c_d_id int, c_w_id int, c_discount float, c_last char(8), "
           "c_credit char(2));");
    RunSql(*db, session, "create table warehouse(w_id int, w_tax float);");
    RunSql(*db, session, "begin;");
    for (int warehouse = 100; warehouse < 10100; ++warehouse) {
        RunSql(*db, session,
               ("insert into customer values(1, 2, " + std::to_string(warehouse) + ", 0.1, 'noise', 'GC');").c_str());
    }
    RunSql(*db, session, "insert into customer values(3, 2, 7, 0.25, 'target', 'BC');");
    RunSql(*db, session, "insert into warehouse values(7, 0.5);");
    RunSql(*db, session, "commit;");
    RunSql(*db, session, "create index customer_pk on customer(c_w_id, c_d_id, c_id);");
    RunSql(*db, session, "create index warehouse_pk on warehouse(w_id);");

    const auto result = Query(*db, session,
                              "select c_discount, c_last, c_credit, w_tax from customer, warehouse where "
                              "w_id = 7 and c_w_id = w_id and c_d_id = 2 and c_id = 3;");
    ASSERT_EQ(result.rows.size(), 1U);
    EXPECT_EQ(result.rows[0][1], "target");
    EXPECT_EQ(result.rows[0][2], "BC");
    EXPECT_EQ(db->JoinOuterProbeCensusForTest(session), (std::array<size_t, 5>{1, 1, 0, 1, 1}));
    EXPECT_EQ(db->JoinProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 1, 0, 0, 1, 0, 0}));
}

TEST(DeltaDatabaseTest, GroupByCountAndMax) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table g(w int, d int, v int);");
    RunSql(*db, session, "insert into g values(1, 1, 2);");
    RunSql(*db, session, "insert into g values(1, 1, 5);");
    RunSql(*db, session, "insert into g values(1, 2, 3);");
    auto result = Query(*db, session, "select w, d, count(*), min(v), max(v), sum(v) from g group by w, d;");
    ASSERT_EQ(result.rows,
              (std::vector<std::vector<std::string>>{{"1", "1", "2", "2", "5", "7"}, {"1", "2", "1", "3", "3", "3"}}));
    EXPECT_TRUE(Query(*db, session, "select w, count(*) from g where v < 0 group by w;").rows.empty());
}

TEST(DeltaDatabaseTest, EmptyStreamingSelectStartsQueryOnce) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "insert into t values(1);");
    const auto empty = Query(*db, session, "select * from t where k = 2;");
    EXPECT_EQ(empty.begin_count, 1);
    EXPECT_EQ(empty.column_names, (std::vector<std::string>{"k"}));
    EXPECT_TRUE(empty.rows.empty());
    EXPECT_EQ(Query(*db, session, "select * from t;").begin_count, 1);
}

TEST(DeltaDatabaseTest, DeltaTracesNeverFallBackToLegacy) {
    ScopedStorageEngine storage_engine(nullptr);
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    db.reset();
    ASSERT_TRUE(std::filesystem::remove(temp.path() + "/DELTA_CATALOG"));
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(temp.path()));
    DatabaseInstance instance;
    EXPECT_FALSE(instance.has_legacy());
    EXPECT_THROW(instance.open_and_recover(temp.path()), std::runtime_error);
    EXPECT_FALSE(instance.has_legacy());
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/db.meta"));

    TempDelta truncated;
    db = deltakernel::DeltaDatabase::Create(truncated.path());
    db.reset();
    std::ofstream catalog(truncated.path() + "/DELTA_CATALOG", std::ios::trunc);
    catalog << "OLD_DELTA_FORMAT\n2\n1";
    catalog.close();
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(truncated.path()));
    EXPECT_THROW(deltakernel::DeltaDatabase::Open(truncated.path()), std::runtime_error);

    TempDelta manifest_only;
    std::ofstream(manifest_only.path() + "/MANIFEST");
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(manifest_only.path()));

    TempDelta compliant_wal;
    std::ofstream(compliant_wal.path() + "/db.log.0");
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(compliant_wal.path()));

    TempDelta legacy_wal;
    std::ofstream(legacy_wal.path() + "/wal.0");
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(legacy_wal.path()));

    TempDelta catalog_only;
    std::ofstream(catalog_only.path() + "/DELTA_CATALOG");
    EXPECT_FALSE(deltakernel::DeltaDatabase::IsDeltaDirectory(catalog_only.path()));
}

TEST(DeltaDatabaseTest, FreshDeltaInstanceDoesNotConstructLegacy) {
    ScopedStorageEngine storage_engine(nullptr);
    TempDelta temp;
    const std::string database_path = temp.path() + "/fresh";
    DatabaseInstance instance;
    instance.open_and_recover(database_path);
    EXPECT_TRUE(instance.is_delta());
    EXPECT_FALSE(instance.has_legacy());
    EXPECT_TRUE(deltakernel::DeltaDatabase::IsDeltaDirectory(database_path));
    EXPECT_FALSE(std::filesystem::exists(database_path + "/db.meta"));
    instance.close();
}

TEST(DeltaDatabaseTest, ExistingLegacyInstanceDoesNotRequireEnvironment) {
    TempDelta temp;
    const std::string database_path = temp.path() + "/legacy";
    {
        ScopedStorageEngine storage_engine("legacy");
        DatabaseInstance instance;
        instance.open_and_recover(database_path);
        EXPECT_FALSE(instance.is_delta());
        EXPECT_TRUE(instance.has_legacy());
    }

    {
        ScopedStorageEngine storage_engine(nullptr);
        DatabaseInstance instance;
        instance.open_and_recover(database_path);
        EXPECT_FALSE(instance.is_delta());
        EXPECT_TRUE(instance.has_legacy());
    }
}

TEST(DeltaDatabaseTest, StorageEngineConflictsFailClosed) {
    ScopedStorageEngine storage_engine(nullptr);
    TempDelta temp;
    const std::string database_path = temp.path() + "/delta";
    DatabaseInstance instance;
    instance.open_and_recover(database_path);
    instance.close();

    {
        ScopedStorageEngine delta("delta");
        DatabaseInstance compatible;
        compatible.open_and_recover(database_path);
        EXPECT_TRUE(compatible.is_delta());
    }

    {
        ScopedStorageEngine legacy("legacy");
        DatabaseInstance conflicting;
        EXPECT_THROW(conflicting.open_and_recover(database_path), RMDBError);
    }
}

TEST(DeltaDatabaseTest, DualStorageMarkersFailClosed) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    db.reset();
    std::ofstream(temp.path() + "/db.meta");

    const auto expect_rejected = [&](const char* engine) {
        ScopedStorageEngine storage_engine(engine);
        DatabaseInstance instance;
        EXPECT_THROW(instance.open_and_recover(temp.path()), RMDBError);
        EXPECT_FALSE(instance.is_delta());
        EXPECT_FALSE(instance.has_legacy());
    };
    expect_rejected(nullptr);
    expect_rejected("delta");
    expect_rejected("legacy");
}

TEST(DeltaDatabaseTest, CatalogRejectsReusedNextTableId) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    db.reset();
    WriteCatalog(temp.path() + "/DELTA_CATALOG",
                 "DELTAKERNEL\n2 1 1 1\nTABLE 1 1 t 2 0 0\nCOLUMN 0 4 1 k\nCOLUMN 0 4 1 v\n");
    EXPECT_THROW(deltakernel::DeltaDatabase::Open(temp.path()), std::runtime_error);
}

TEST(DeltaDatabaseTest, CatalogChecksumAndGenerationExhaustionFailClosed) {
    TempDelta checksum;
    auto db = deltakernel::DeltaDatabase::Create(checksum.path());
    db.reset();
    {
        std::fstream catalog(checksum.path() + "/DELTA_CATALOG", std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(catalog);
        catalog.seekp(0);
        catalog.put('X');
    }
    EXPECT_THROW(deltakernel::DeltaDatabase::Open(checksum.path()), std::runtime_error);

    TempDelta exhausted;
    db = deltakernel::DeltaDatabase::Create(exhausted.path());
    db.reset();
    WriteCatalog(exhausted.path() + "/DELTA_CATALOG", "DELTAKERNEL\n18446744073709551615 1 1 0\n");
    db = deltakernel::DeltaDatabase::Open(exhausted.path());
    deltakernel::DeltaSession session;
    EXPECT_THROW(RunSql(*db, session, "create table no_id(k int);"), std::runtime_error);
}

TEST(DeltaDatabaseTest, FailedCatalogSaveDoesNotPublishTable) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    db->SetCatalogSaveFailureForTest(true);
    EXPECT_THROW(RunSql(*db, session, "create table missing(k int, v int);"), std::runtime_error);
    EXPECT_THROW(RunSql(*db, session, "insert into missing values(1, 1);"), std::runtime_error);
    db->SetCatalogSaveFailureForTest(false);
    RunSql(*db, session, "create table live(k int, v int);");
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "insert into live values(1, 1);");
}

TEST(DeltaDatabaseTest, PostRenameCatalogFailurePoisonsUntilReopen) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    db->SetCatalogPostRenameFailureForTest(true);
    EXPECT_THROW(RunSql(*db, session, "create table durable(k int);"), std::runtime_error);
    EXPECT_THROW(RunSql(*db, session, "show tables;"), std::logic_error);
    EXPECT_THROW(db->Checkpoint(), std::logic_error);
    EXPECT_THROW(db->CatalogGeneration(), std::logic_error);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    RunSql(*db, session, "insert into durable values(1);");
}

TEST(DeltaDatabaseTest, AbortSerializesWithExecute) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into t values(1, 1);");
    std::mutex barrier_mutex;
    std::condition_variable barrier;
    bool entered = false;
    bool release = false;
    std::atomic<bool> abort_done{false};
    db->SetExecuteLockHookForTest([&] {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        entered = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    std::thread executor([&] { RunSql(*db, session, "select * from t;"); });
    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        barrier.wait(lock, [&] { return entered; });
    }
    std::thread aborter([&] {
        db->Abort(session);
        abort_done.store(true);
    });
    std::this_thread::yield();
    EXPECT_FALSE(abort_done.load());
    {
        std::lock_guard<std::mutex> lock(barrier_mutex);
        release = true;
    }
    barrier.notify_all();
    executor.join();
    aborter.join();
    db->SetExecuteLockHookForTest({});
    EXPECT_FALSE(session.txn.has_value());
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "rollback;");
}

TEST(DeltaDatabaseTest, AbortWaitsAcrossExecuteCommitUnlock) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into t values(1);");
    std::mutex mutex;
    std::condition_variable barrier;
    bool commit_waiting = false;
    bool release = false;
    bool abort_attempted = false;
    std::atomic<bool> abort_done{false};
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        commit_waiting = true;
        barrier.notify_all();
        barrier.wait(lock, [&] { return release; });
    });
    db->SetAbortLockAttemptHookForTest([&] {
        std::lock_guard<std::mutex> lock(mutex);
        abort_attempted = true;
        barrier.notify_all();
    });
    std::thread committer([&] { RunSql(*db, session, "commit;"); });
    bool saw_commit_wait = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        saw_commit_wait = barrier.wait_for(lock, std::chrono::seconds(5), [&] { return commit_waiting; });
        if (!saw_commit_wait)
            release = true;
    }
    if (!saw_commit_wait) {
        barrier.notify_all();
        committer.join();
        db->SetCommitBatchHookForTest({});
        db->SetAbortLockAttemptHookForTest({});
        FAIL() << "commit hook was not reached";
    }
    std::thread aborter([&] {
        db->Abort(session);
        abort_done.store(true);
    });
    bool saw_abort_attempt = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        saw_abort_attempt = barrier.wait_for(lock, std::chrono::seconds(5), [&] { return abort_attempted; });
    }
    if (saw_abort_attempt) {
        EXPECT_FALSE(abort_done.load());
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    barrier.notify_all();
    committer.join();
    aborter.join();
    ASSERT_TRUE(saw_abort_attempt);
    db->SetCommitBatchHookForTest({});
    db->SetAbortLockAttemptHookForTest({});
    EXPECT_EQ(Query(*db, session, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
}

TEST(DeltaDatabaseTest, SharedScansAndPrivateDmlExecuteConcurrently) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, first, second, outsider;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(0, 0);");
    if (!db->DiagnosticsEnabled()) {
        EXPECT_EQ(db->ConcurrencyDiagnosticsForTest(), (std::array<uint64_t, 6>{}));
    }
    db->EnableDiagnosticsForTest();

    std::mutex mutex;
    std::condition_variable condition;
    int waiting = 0;
    bool release = false;
    bool overlapped = false;
    db->SetExecuteLockHookForTest([&] {
        if (delta_concurrency_role == 0)
            return;
        std::unique_lock<std::mutex> lock(mutex);
        ++waiting;
        if (waiting == 2) {
            overlapped = true;
            release = true;
            condition.notify_all();
        }
        if (!condition.wait_for(lock, std::chrono::seconds(2), [&] { return release; })) {
            release = true;
            condition.notify_all();
        }
        --waiting;
    });
    CapturingSink first_scan;
    CapturingSink second_scan;
    std::exception_ptr first_error;
    std::exception_ptr second_error;
    std::thread first_reader([&] {
        delta_concurrency_role = 1;
        try {
            first_scan = Query(*db, first, "select count(k) from t;");
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    std::thread second_reader([&] {
        delta_concurrency_role = 2;
        try {
            second_scan = Query(*db, second, "select count(k) from t;");
        } catch (...) {
            second_error = std::current_exception();
        }
    });
    first_reader.join();
    second_reader.join();
    EXPECT_TRUE(overlapped);
    EXPECT_FALSE(first_error);
    EXPECT_FALSE(second_error);
    EXPECT_EQ(first_scan.rows, (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_EQ(second_scan.rows, first_scan.rows);

    RunSql(*db, first, "begin;");
    RunSql(*db, second, "begin;");
    {
        std::lock_guard<std::mutex> lock(mutex);
        waiting = 0;
        release = false;
        overlapped = false;
    }
    first_error = nullptr;
    second_error = nullptr;
    std::thread first_writer([&] {
        delta_concurrency_role = 1;
        try {
            RunSql(*db, first, "insert into t values(1, 10);");
        } catch (...) {
            first_error = std::current_exception();
        }
    });
    std::thread second_writer([&] {
        delta_concurrency_role = 2;
        try {
            RunSql(*db, second, "insert into t values(2, 20);");
        } catch (...) {
            second_error = std::current_exception();
        }
    });
    first_writer.join();
    second_writer.join();
    db->SetExecuteLockHookForTest({});
    EXPECT_TRUE(overlapped);
    EXPECT_FALSE(first_error);
    EXPECT_FALSE(second_error);
    EXPECT_EQ(Query(*db, first, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, second, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"20"}}));
    EXPECT_TRUE(Query(*db, outsider, "select v from t where k > 0;").rows.empty());
    std::thread first_commit([&] { RunSql(*db, first, "commit;"); });
    std::thread second_commit([&] { RunSql(*db, second, "commit;"); });
    first_commit.join();
    second_commit.join();
    EXPECT_EQ(Query(*db, outsider, "select v from t where k > 0;").rows.size(), 2U);
    const auto diagnostics = db->ConcurrencyDiagnosticsForTest();
    EXPECT_GE(diagnostics[0], 8U);
    EXPECT_GE(diagnostics[2], 1U);
    EXPECT_GE(diagnostics[4], 2U);
}

TEST(DeltaDatabaseTest, PendingCommitPreventsReaderBargingAndPublishesIndexAtomically) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, holding_reader, writer, late_reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(7, 9);");

    std::mutex mutex;
    std::condition_variable condition;
    bool reader_holding = false;
    bool writer_waiting = false;
    bool late_reader_blocked = false;
    bool late_reader_acquired = false;
    bool release_reader = false;
    bool installing = false;
    bool release_install = false;
    int sequence = 0;
    int install_order = 0;
    int late_reader_order = 0;
    db->SetExecuteLockHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (delta_concurrency_role == 1) {
            reader_holding = true;
            condition.notify_all();
            condition.wait(lock, [&] { return release_reader; });
        } else if (delta_concurrency_role == 2) {
            late_reader_acquired = true;
            late_reader_order = ++sequence;
            condition.notify_all();
        }
    });
    db->SetStateWriterWaitHookForTest([&] {
        std::lock_guard<std::mutex> lock(mutex);
        writer_waiting = true;
        condition.notify_all();
    });
    db->SetExecuteBlockedHookForTest([&] {
        if (delta_concurrency_role != 2)
            return;
        std::lock_guard<std::mutex> lock(mutex);
        late_reader_blocked = true;
        condition.notify_all();
    });
    db->SetCommitInstallHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (install_order == 0)
            install_order = ++sequence;
        installing = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release_install; });
    });

    CapturingSink holding_result;
    CapturingSink late_result;
    std::exception_ptr commit_error;
    std::thread holder([&] {
        delta_concurrency_role = 1;
        holding_result = Query(*db, holding_reader, "select v from t where k = 7;");
    });
    const bool saw_reader = WaitForCondition(condition, mutex, [&] { return reader_holding; });
    std::thread committer([&] {
        try {
            RunSql(*db, writer, "commit;");
        } catch (...) {
            commit_error = std::current_exception();
        }
    });
    const bool saw_writer = saw_reader && WaitForCondition(condition, mutex, [&] { return writer_waiting; });
    {
        std::lock_guard<std::mutex> lock(mutex);
        release_reader = true;
    }
    condition.notify_all();
    const bool saw_install = WaitForCondition(condition, mutex, [&] { return installing; });
    std::thread late([&] {
        delta_concurrency_role = 2;
        late_result = Query(*db, late_reader, "select v from t where k = 7;");
    });
    const bool saw_late_block = saw_install && WaitForCondition(condition, mutex, [&] { return late_reader_blocked; });
    bool barged = false;
    {
        std::lock_guard<std::mutex> lock(mutex);
        barged = late_reader_acquired;
        release_install = true;
    }
    condition.notify_all();
    holder.join();
    committer.join();
    late.join();
    db->SetExecuteLockHookForTest({});
    db->SetStateWriterWaitHookForTest({});
    db->SetExecuteBlockedHookForTest({});
    db->SetCommitInstallHookForTest({});
    EXPECT_TRUE(saw_reader);
    EXPECT_TRUE(saw_writer);
    EXPECT_TRUE(saw_install);
    EXPECT_TRUE(saw_late_block);
    EXPECT_FALSE(barged);
    EXPECT_FALSE(commit_error);
    EXPECT_TRUE(holding_result.rows.empty());
    EXPECT_GT(install_order, 0);
    EXPECT_EQ(late_result.rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_GT(late_reader_order, install_order);
}

TEST(DeltaDatabaseTest, CheckpointDoesNotWaitForIdleExplicitTransaction) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_reader, writer, latest_reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, old_reader, "begin;");

    EXPECT_NO_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db));

    RunSql(*db, writer, "update t set v = 20 where k = 1;");
    EXPECT_EQ(Query(*db, old_reader, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, latest_reader, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"20"}}));
}

TEST(DeltaDatabaseTest, CatalogMutationRejectsOldTransactionBeforeIndexPublication) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_reader, deleter, ddl, latest_reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, old_reader, "begin;");
    RunSql(*db, deleter, "delete from t where k = 1;");

    EXPECT_THROW(RunSql(*db, ddl, "create table later(k int);"), std::runtime_error);
    EXPECT_THROW(RunSql(*db, ddl, "create index t_k on t(k);"), std::runtime_error);
    EXPECT_EQ(Query(*db, old_reader, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));

    RunSql(*db, old_reader, "rollback;");
    EXPECT_NO_THROW(RunSql(*db, ddl, "create table later(k int);"));
    EXPECT_NO_THROW(RunSql(*db, ddl, "create index t_k on t(k);"));
    EXPECT_TRUE(Query(*db, latest_reader, "select v from t where k = 1;").rows.empty());
}

TEST(DeltaDatabaseTest, SnapshotBuilderFileIoDoesNotBlockPostCutCommit) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, post_cut, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, setup, "insert into t values(2, 20);");
    std::mutex mutex;
    std::condition_variable condition;
    bool entered = false;
    bool release = false;
    epoch_si_poc::Epoch built_cut = 0;
    std::vector<int32_t> built_values;
    deltakernel::DeltaSnapshotTestPeer::SetRowsHook(*db, "t",
                                                    [&](epoch_si_poc::Epoch cut, std::vector<int32_t> values) {
                                                        built_cut = cut;
                                                        built_values = std::move(values);
                                                    });
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release; });
    });
    std::exception_ptr build_error;
    std::thread builder([&] {
        try {
            deltakernel::DeltaSnapshotTestPeer::Build(*db);
        } catch (...) {
            build_error = std::current_exception();
        }
    });
    const bool saw_io = WaitForCondition(condition, mutex, [&] { return entered; });
    if (saw_io) {
        RunSql(*db, post_cut, "update t set k = 4 where k = 1;");
        RunSql(*db, post_cut, "update t set k = 1 where k = 4;");
        RunSql(*db, post_cut, "update t set v = 99 where k = 1;");
        RunSql(*db, post_cut, "delete from t where k = 2;");
        RunSql(*db, post_cut, "insert into t values(3, 30);");
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    condition.notify_all();
    builder.join();
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    EXPECT_TRUE(saw_io);
    EXPECT_FALSE(build_error);
    EXPECT_EQ(built_cut, 2U);
    EXPECT_EQ(built_values, (std::vector<int32_t>{10, 20}));
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"99"}}));
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"30"}}));
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::IndexState(*db)[3], 2U);
    const auto post_cut_authority = deltakernel::DeltaSnapshotTestPeer::AuthorityState(*db);
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::AuthorityState(*db)[0], post_cut_authority[0] + 2);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::IndexState(*db)[3], 0U);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, reader, "select k, v from t order by k;").rows,
              (std::vector<std::vector<std::string>>{{"1", "99"}, {"3", "30"}}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderQueriesFrozenAndActiveIndexGenerations) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, historical, post_cut, latest;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    RunSql(*db, setup, "update t set k = 2 where k = 1;");
    RunSql(*db, historical, "begin;");

    bool changed = false;
    std::array<uint64_t, 4> before_value_update{};
    std::array<uint64_t, 4> after_value_update{};
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (changed)
            return;
        changed = true;
        RunSql(*db, post_cut, "update t set k = 1 where k = 2;");
        before_value_update = deltakernel::DeltaSnapshotTestPeer::OverlayState(*db);
        RunSql(*db, post_cut, "update t set v = 99 where k = 1;");
        after_value_update = deltakernel::DeltaSnapshotTestPeer::OverlayState(*db);
        EXPECT_EQ(Query(*db, latest, "select v from t where k = 1;").rows,
                  (std::vector<std::vector<std::string>>{{"99"}}));
        EXPECT_TRUE(Query(*db, latest, "select v from t where k = 2;").rows.empty());
        EXPECT_EQ(Query(*db, historical, "select v from t where k = 2;").rows,
                  (std::vector<std::vector<std::string>>{{"10"}}));
    });
    EXPECT_NO_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db));
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});

    EXPECT_TRUE(changed);
    EXPECT_EQ(before_value_update, after_value_update);
    EXPECT_GT(before_value_update[1], 0U);
    EXPECT_GT(before_value_update[2], 0U);
    EXPECT_GT(before_value_update[3], 0U);
    EXPECT_EQ(Query(*db, latest, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"99"}}));
    EXPECT_TRUE(Query(*db, latest, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(Query(*db, historical, "select v from t where k = 2;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    const auto published = deltakernel::DeltaSnapshotTestPeer::OverlayState(*db);
    EXPECT_EQ(published[1], 0U);
    EXPECT_EQ(published[3], 0U);
    EXPECT_GT(published[0], 0U);
    EXPECT_GT(published[2], 0U);
    RunSql(*db, historical, "rollback;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::OverlayState(*db), (std::array<uint64_t, 4>{0, 0, 0, 0}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderFailureRestoresBothOverlayGenerations) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, historical, post_cut, latest;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    RunSql(*db, setup, "update t set k = 2 where k = 1;");
    RunSql(*db, historical, "begin;");

    bool changed = false;
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (changed)
            return;
        changed = true;
        RunSql(*db, post_cut, "update t set k = 3 where k = 2;");
        RunSql(*db, post_cut, "insert into t values(9, 90);");
        RunSql(*db, post_cut, "delete from t where k = 9;");
        EXPECT_EQ(Query(*db, latest, "select v from t where k = 3;").rows,
                  (std::vector<std::vector<std::string>>{{"10"}}));
        EXPECT_EQ(Query(*db, historical, "select v from t where k = 2;").rows,
                  (std::vector<std::vector<std::string>>{{"10"}}));
        EXPECT_TRUE(Query(*db, latest, "select v from t where k = 9;").rows.empty());
    });
    deltakernel::DeltaSnapshotTestPeer::SetSidecarFinishHook(*db,
                                                             [](size_t) { throw std::runtime_error("stop build"); });
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), std::runtime_error);
    EXPECT_TRUE(changed);
    auto restored = deltakernel::DeltaSnapshotTestPeer::OverlayState(*db);
    EXPECT_EQ(restored[1], 0U);
    EXPECT_EQ(restored[3], 0U);
    EXPECT_GT(restored[0], 0U);
    EXPECT_GT(restored[2], 0U);
    EXPECT_EQ(Query(*db, latest, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, historical, "select v from t where k = 2;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_TRUE(Query(*db, latest, "select v from t where k = 9;").rows.empty());

    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), std::runtime_error);
    restored = deltakernel::DeltaSnapshotTestPeer::OverlayState(*db);
    EXPECT_EQ(restored[1], 0U);
    EXPECT_EQ(restored[3], 0U);
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    deltakernel::DeltaSnapshotTestPeer::SetSidecarFinishHook(*db, {});
    EXPECT_NO_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db));
    EXPECT_EQ(Query(*db, latest, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_TRUE(Query(*db, latest, "select v from t where k = 9;").rows.empty());
    RunSql(*db, historical, "rollback;");
}

TEST(DeltaDatabaseTest, SnapshotBuilderRejectsPostCutGenerationDrift) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(7);");
    bool rotated = false;
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (!rotated) {
            rotated = true;
            db->RotateWalForTest();
        }
    });
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), std::logic_error);
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    ASSERT_TRUE(rotated);
    for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
        EXPECT_EQ(entry.path().filename().string().find("deltaidx-staged"), std::string::npos);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, setup, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderKeepsOldTransactionSource) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_reader, writer, fresh;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, old_reader, "begin;");
    RunSql(*db, writer, "update t set v = 20 where k = 1;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    EXPECT_EQ(Query(*db, old_reader, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"20"}}));
    RunSql(*db, old_reader, "abort;");
}

TEST(DeltaDatabaseTest, SnapshotBuilderRejectsCatalogMutationWhilePinned) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, ddl;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(7);");
    bool attempted = false;
    bool rejected = false;
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (!attempted) {
            attempted = true;
            try {
                RunSql(*db, ddl, "create table u(k int);");
            } catch (const std::runtime_error&) {
                rejected = true;
            }
        }
    });
    EXPECT_NO_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db));
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    EXPECT_TRUE(attempted);
    EXPECT_TRUE(rejected);
    for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
        EXPECT_NE(entry.path().extension(), ".tmp");
    EXPECT_THROW(Query(*db, ddl, "select k from u;"), std::runtime_error);
    EXPECT_NO_THROW(RunSql(*db, ddl, "create table u(k int);"));
    EXPECT_EQ(Query(*db, setup, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderPublishesEmptyAndLegacyTables) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table empty_t(k int);");
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(7);");
    const auto before = deltakernel::DeltaSnapshotTestPeer::SnapshotState(*db);
    EXPECT_EQ(before[1], 1U);
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const auto after = deltakernel::DeltaSnapshotTestPeer::SnapshotState(*db);
    EXPECT_EQ(after[0], before[0] + 2);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_TRUE(Query(*db, setup, "select k from empty_t;").rows.empty());
    EXPECT_EQ(Query(*db, setup, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
    RunSql(*db, setup, "insert into empty_t values(9);");
    EXPECT_EQ(Query(*db, setup, "select k from empty_t;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
}

TEST(DeltaDatabaseTest, CleanSnapshotBuildIsAnExactNoOp) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(7);");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const auto before = deltakernel::DeltaSnapshotTestPeer::AuthorityState(*db);
    std::set<std::string> wal_before;
    for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
        if (const auto name = entry.path().filename().string();
            name.rfind("db.log", 0) == 0 || name.rfind("wal.", 0) == 0)
            wal_before.insert(name);
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    std::set<std::string> wal_after;
    for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
        if (const auto name = entry.path().filename().string();
            name.rfind("db.log", 0) == 0 || name.rfind("wal.", 0) == 0)
            wal_after.insert(name);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::AuthorityState(*db), before);
    EXPECT_EQ(wal_after, wal_before);
}

TEST(DeltaDatabaseTest, SnapshotBuilderCrashPointsRecoverPublishedAuthority) {
    for (const auto point : {epoch_si_poc::CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync,
                             epoch_si_poc::CheckpointCrashPoint::kSnapshotAfterManifestDurable,
                             epoch_si_poc::CheckpointCrashPoint::kSnapshotBeforeSourceInstall,
                             epoch_si_poc::CheckpointCrashPoint::kSnapshotAfterSourceInstall}) {
        TempDelta temp;
        {
            auto db = deltakernel::DeltaDatabase::Create(temp.path());
            deltakernel::DeltaSession setup;
            RunSql(*db, setup, "create table t(k int);");
            RunSql(*db, setup, "create index t_k on t(k);");
            RunSql(*db, setup, "insert into t values(7);");
        }
        const pid_t child = fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            std::signal(SIGABRT, [](int) { kill(getpid(), SIGKILL); });
            auto db = deltakernel::DeltaDatabase::Open(temp.path());
            deltakernel::DeltaSnapshotTestPeer::SetCrashPoint(*db, point);
            deltakernel::DeltaSnapshotTestPeer::Build(*db);
            _exit(1);
        }
        int status = 0;
        ASSERT_EQ(waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFSIGNALED(status)) << static_cast<int>(point);
        ASSERT_EQ(WTERMSIG(status), SIGKILL) << static_cast<int>(point);
        auto reopened = deltakernel::DeltaDatabase::Open(temp.path());
        deltakernel::DeltaSession reader;
        EXPECT_EQ(Query(*reopened, reader, "select k from t where k = 7;").rows,
                  (std::vector<std::vector<std::string>>{{"7"}}))
            << static_cast<int>(point);
        size_t segments = 0;
        for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
            segments += entry.path().filename().string().rfind("db.log.s.", 0) == 0 &&
                        !std::filesystem::is_symlink(entry.symlink_status());
        EXPECT_EQ(segments, 1U) << static_cast<int>(point);
    }
}

TEST(DeltaDatabaseTest, SnapshotBuilderAdoptsSidecarAndDropsFoldedHistoricalOverlay) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    db->Checkpoint();
    RunSql(*db, setup, "update t set k = 2 where k = 1;");
    const auto before = deltakernel::DeltaSnapshotTestPeer::IndexState(*db);
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const auto after = deltakernel::DeltaSnapshotTestPeer::IndexState(*db);
    EXPECT_GT(after[0], before[0]);
    EXPECT_GT(after[1], before[1]);
    EXPECT_GT(before[2], 0U);
    EXPECT_EQ(after[2], 0U);
    EXPECT_EQ(after[3], 0U);
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 1;").rows.empty());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, SnapshotSidecarKeepsOldSourceIndexSemantics) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_reader, writer, fresh;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, setup, "insert into t values(2, 20);");
    RunSql(*db, old_reader, "begin;");
    RunSql(*db, writer, "update t set k = 3 where k = 1;");
    RunSql(*db, writer, "delete from t where k = 2;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    EXPECT_EQ(Query(*db, old_reader, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, old_reader, "select v from t where k = 2;").rows,
              (std::vector<std::vector<std::string>>{{"20"}}));
    EXPECT_EQ(Query(*db, old_reader, "select k from t order by k limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 1;").rows.empty());
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    RunSql(*db, old_reader, "rollback;");
}

TEST(DeltaDatabaseTest, SnapshotSidecarPromotionIsAllOrNone) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "create index t_v on t(v);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    std::string first_final;
    std::string collision;
    deltakernel::DeltaSnapshotTestPeer::SetSidecarArtifactHook(
        *db, [&](epoch_si_poc::TableId table_id, const std::vector<std::array<uint64_t, 4>>& headers) {
            ASSERT_EQ(headers.size(), 2U);
            first_final = temp.path() + "/deltaidx." + std::to_string(table_id) + "." + std::to_string(headers[0][3]) +
                          ".g" + std::to_string(headers[0][1]);
            collision = temp.path() + "/deltaidx." + std::to_string(table_id) + "." + std::to_string(headers[1][3]) +
                        ".g" + std::to_string(headers[1][1]);
            std::ofstream output(collision);
            output << "owned by test";
        });
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), std::system_error);
    EXPECT_FALSE(std::filesystem::exists(first_final));
    EXPECT_TRUE(std::filesystem::exists(collision));
    EXPECT_TRUE(std::filesystem::remove(collision));
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, SnapshotSidecarManifestFailureRemovesPromotedFiles) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    deltakernel::DeltaSnapshotTestPeer::SetCrashPoint(*db, epoch_si_poc::CheckpointCrashPoint::kDuringManifestTemp);
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), epoch_si_poc::SimulatedCrash);
    for (const auto& entry : std::filesystem::directory_iterator(temp.path())) {
        const auto name = entry.path().filename().string();
        if (name.rfind("deltaidx.1.1.g", 0) == 0)
            ADD_FAILURE() << name;
    }
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, SnapshotSidecarMissingOrCorruptRebuildsSafely) {
    for (bool corrupt : {false, true}) {
        TempDelta temp;
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        deltakernel::DeltaSession setup, reader;
        RunSql(*db, setup, "create table t(k int, v int);");
        RunSql(*db, setup, "create index t_k on t(k);");
        RunSql(*db, setup, "insert into t values(1, 10);");
        deltakernel::DeltaSnapshotTestPeer::Build(*db);
        const std::string sidecar = deltakernel::DeltaSnapshotTestPeer::GenerationSidecarPath(*db, "t", 0);
        db.reset();
        if (corrupt) {
            std::fstream output(sidecar, std::ios::binary | std::ios::in | std::ios::out);
            ASSERT_TRUE(output);
            output.put('!');
        } else {
            ASSERT_TRUE(std::filesystem::remove(sidecar));
        }
        db = deltakernel::DeltaDatabase::Open(temp.path());
        EXPECT_EQ(Query(*db, reader, "select v from t where k = 1;").rows,
                  (std::vector<std::vector<std::string>>{{"10"}}))
            << corrupt;
    }
}

TEST(DeltaDatabaseTest, SnapshotSidecarSupportsConsecutivePublications) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const uint64_t first_generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    RunSql(*db, setup, "update t set k = 2 where k = 1;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const uint64_t second_generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    EXPECT_GT(second_generation, first_generation);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::IndexState(*db)[0], second_generation);
    size_t generation_sidecars = 0;
    for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
        generation_sidecars += entry.path().filename().string().rfind("deltaidx.1.1.g", 0) == 0;
    EXPECT_EQ(generation_sidecars, 1U);
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 1;").rows.empty());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, OpenPreservesReplacedGenerationSidecarResidue) {
    TempDelta temp;
    TempDelta external;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1);");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    db.reset();
    const std::string orphan = temp.path() + "/deltaidx.1.1.g999";
    {
        std::ofstream output(orphan);
        output << "orphan";
    }
    const std::string malformed = temp.path() + "/deltaidx.malformed.g";
    {
        std::ofstream output(malformed);
        output << "malformed";
    }
    const std::string target = external.path() + "/target";
    {
        std::ofstream output(target);
        output << "target";
    }
    const std::string replaced = temp.path() + "/deltaidx.1.1.g998";
    ASSERT_EQ(symlink(target.c_str(), replaced.c_str()), 0);
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_FALSE(std::filesystem::exists(orphan));
    EXPECT_TRUE(std::filesystem::exists(malformed));
    EXPECT_TRUE(std::filesystem::is_symlink(replaced));
    std::ifstream input(target);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()), "target");
}

TEST(DeltaDatabaseTest, GenerationReclamationWaitsForEveryOldSourceAndReleasesOutsideLocks) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, historical, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);

    const uint64_t first_generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    const std::string first_table = deltakernel::DeltaSnapshotTestPeer::TableBasePath(*db, "t", first_generation);
    std::unique_ptr<deltakernel::DeltaSnapshotTestPeer::PinHandle> oldest_pin;
    RunSql(*db, setup, "update t set k = 2 where k = 1;");
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (!oldest_pin)
            oldest_pin = deltakernel::DeltaSnapshotTestPeer::HoldSnapshotPin(*db);
    });
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    ASSERT_TRUE(oldest_pin);
    ASSERT_TRUE(std::filesystem::exists(first_table));

    const uint64_t middle_generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    const std::string middle_table = deltakernel::DeltaSnapshotTestPeer::TableBasePath(*db, "t", middle_generation);
    std::unique_ptr<deltakernel::DeltaSnapshotTestPeer::PinHandle> middle_pin;
    RunSql(*db, setup, "update t set k = 3 where k = 2;");
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (!middle_pin)
            middle_pin = deltakernel::DeltaSnapshotTestPeer::HoldSnapshotPin(*db);
    });
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    ASSERT_TRUE(middle_pin);
    ASSERT_TRUE(std::filesystem::exists(first_table));
    ASSERT_TRUE(std::filesystem::exists(middle_table));
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 2U);

    RunSql(*db, historical, "begin;");
    RunSql(*db, setup, "update t set k = 4 where k = 3;");
    middle_pin.reset();
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 2U);
    EXPECT_TRUE(std::filesystem::exists(first_table));
    EXPECT_TRUE(std::filesystem::exists(middle_table));

    size_t release_calls = 0;
    deltakernel::DeltaSnapshotTestPeer::SetRetiredStateReleaseHook(*db, [&] {
        ++release_calls;
        EXPECT_EQ(Query(*db, reader, "select v from t where k = 4;").rows,
                  (std::vector<std::vector<std::string>>{{"10"}}));
    });
    oldest_pin.reset();
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    deltakernel::DeltaSnapshotTestPeer::SetRetiredStateReleaseHook(*db, {});
    EXPECT_GT(release_calls, 0U);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 1U);
    EXPECT_FALSE(std::filesystem::exists(first_table));
    EXPECT_FALSE(std::filesystem::exists(middle_table));
    EXPECT_EQ(Query(*db, historical, "select v from t where k = 3;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_TRUE(Query(*db, historical, "select v from t where k = 4;").rows.empty());
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 4;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    RunSql(*db, historical, "rollback;");
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    EXPECT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[3], 0U);

    const uint64_t protected_generation = deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t");
    const std::string protected_table =
        deltakernel::DeltaSnapshotTestPeer::TableBasePath(*db, "t", protected_generation);
    std::unique_ptr<deltakernel::DeltaSnapshotTestPeer::PinHandle> protected_pin;
    RunSql(*db, setup, "update t set k = 5 where k = 4;");
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, [&] {
        if (!protected_pin)
            protected_pin = deltakernel::DeltaSnapshotTestPeer::HoldSnapshotPin(*db);
    });
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    deltakernel::DeltaSnapshotTestPeer::SetIoHook(*db, {});
    ASSERT_TRUE(std::filesystem::remove(protected_table));
    const std::string foreign = temp.path() + "/foreign-tablebase";
    {
        std::ofstream output(foreign);
        output << "not a table base";
    }
    ASSERT_NO_THROW(std::filesystem::create_symlink(foreign, protected_table));
    protected_pin.reset();
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    EXPECT_TRUE(std::filesystem::is_symlink(std::filesystem::symlink_status(protected_table)));
    EXPECT_TRUE(std::filesystem::exists(foreign));
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 5;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, GenerationCollapseAdvancesInBoundedChunkSteps) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(7);");
    deltakernel::DeltaSnapshotTestPeer::AppendRowCopies(*db, "t", 4096);
    ASSERT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[0], 4097U);
    ASSERT_EQ(deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db)[2], 17U);

    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const auto partial = deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db);
    EXPECT_EQ(partial[0], 1U);
    EXPECT_EQ(partial[1], 1U);
    EXPECT_EQ(partial[2], 1U);
    EXPECT_EQ(partial[3], 1U);

    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    const auto complete = deltakernel::DeltaSnapshotTestPeer::ReclamationState(*db);
    EXPECT_EQ(complete[0], 0U);
    EXPECT_EQ(complete[1], 0U);
    EXPECT_EQ(complete[2], 0U);
    EXPECT_EQ(complete[3], 0U);
    EXPECT_EQ(Query(*db, reader, "select count(*) from t;").rows, (std::vector<std::vector<std::string>>{{"4097"}}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderFailureKeepsRotatedAuthorityAndCleansFiles) {
    TempDelta temp;
    {
        auto db = deltakernel::DeltaDatabase::Create(temp.path());
        deltakernel::DeltaSession setup;
        RunSql(*db, setup, "create table t(k int);");
        RunSql(*db, setup, "insert into t values(7);");
        deltakernel::DeltaSnapshotTestPeer::SetCrashPoint(*db, epoch_si_poc::CheckpointCrashPoint::kAfterBaseRename);
        EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), epoch_si_poc::SimulatedCrash);
    }
    for (const auto& entry : std::filesystem::directory_iterator(temp.path())) {
        EXPECT_FALSE(entry.path().extension() == ".tmp");
        EXPECT_EQ(entry.path().filename().string().find("tablebase."), std::string::npos);
    }
    auto reopened = deltakernel::DeltaDatabase::Open(temp.path());
    deltakernel::DeltaSession reader;
    EXPECT_EQ(Query(*reopened, reader, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"7"}}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderCleansStreamingSidecarCohortOnFailure) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "insert into t values(1, 2);");
    deltakernel::DeltaSnapshotTestPeer::SetSidecarFinishHook(*db, [](size_t ordinal) {
        if (ordinal == 0)
            throw std::runtime_error("sidecar finish failure");
    });
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), std::runtime_error);
    for (const auto& entry : std::filesystem::directory_iterator(temp.path())) {
        const auto name = entry.path().filename().string();
        EXPECT_EQ(name.find("deltaidx-staged"), std::string::npos);
        EXPECT_EQ(name.find(".spool"), std::string::npos);
        EXPECT_EQ(name.find(".entries"), std::string::npos);
        EXPECT_EQ(name.find(".keys"), std::string::npos);
        EXPECT_EQ(name.find(".roworder"), std::string::npos);
    }
    EXPECT_EQ(Query(*db, reader, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
}

TEST(DeltaDatabaseTest, SnapshotBuilderStreamsAllIndexesFromOneCursor) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, reader;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "create index t_v on t(v);");
    for (int n = 0; n < 1100; ++n) {
        const std::string insert = "insert into t values(" + std::to_string(n) + "," + std::to_string(n + 1) + ");";
        RunSql(*db, setup, insert.c_str());
    }
    std::vector<std::array<uint64_t, 4>> headers;
    deltakernel::DeltaSnapshotTestPeer::SetSidecarArtifactHook(
        *db, [&](epoch_si_poc::TableId, const std::vector<std::array<uint64_t, 4>>& values) { headers = values; });
    deltakernel::DeltaSnapshotTestPeer::Build(*db);
    ASSERT_EQ(headers.size(), 2U);
    EXPECT_EQ(headers[0][0], 1100U);
    EXPECT_EQ(headers[1][0], 1100U);
    EXPECT_EQ(headers[0][1], deltakernel::DeltaSnapshotTestPeer::Generation(*db, "t"));
    EXPECT_EQ(headers[1][1], headers[0][1]);
    EXPECT_LE(headers[0][2], deltakernel::DeltaSnapshotTestPeer::Snapshot(*db));
    EXPECT_NE(headers[0][3], headers[1][3]);
    EXPECT_EQ(Query(*db, reader, "select count(*) from t;").rows, (std::vector<std::vector<std::string>>{{"1100"}}));
    for (const auto& entry : std::filesystem::directory_iterator(temp.path())) {
        const auto name = entry.path().filename().string();
        EXPECT_EQ(name.find("deltaidx-staged"), std::string::npos);
        EXPECT_EQ(name.find(".sort-run-"), std::string::npos);
        EXPECT_EQ(name.find(".spool"), std::string::npos);
        EXPECT_EQ(name.find(".entries"), std::string::npos);
        EXPECT_EQ(name.find(".keys"), std::string::npos);
        EXPECT_EQ(name.find(".roworder"), std::string::npos);
    }
}

TEST(DeltaDatabaseTest, OpenRemovesOnlyGeneratedStagedResidue) {
    TempDelta temp;
    auto created = deltakernel::DeltaDatabase::Create(temp.path());
    created.reset();
    auto make_file = [&](const std::string& name) {
        std::ofstream output(temp.path() + "/" + name);
        output << "residue";
    };
    const std::string prefix = "deltaidx-staged.1.2.g3.e4.5.6";
    make_file(prefix);
    make_file(prefix + ".entries");
    make_file(prefix + ".keys");
    make_file(prefix + ".roworder");
    make_file(prefix + ".spool");
    make_file(prefix + ".sort-sort-run-7");
    make_file(prefix + ".rowsort-sort-run-8");
    make_file("deltaidx-staged.malformed.entries");
    const std::string symlink_prefix = "deltaidx-staged.7.8.g9.e10.11.12";
    const auto symlink_path = temp.path() + "/" + symlink_prefix + ".spool";
    ASSERT_EQ(symlink("deltaidx-staged.malformed.entries", symlink_path.c_str()), 0);
    auto db = deltakernel::DeltaDatabase::Open(temp.path());
    (void)db;
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/" + prefix));
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/" + prefix + ".entries"));
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/" + prefix + ".sort-sort-run-7"));
    EXPECT_TRUE(std::filesystem::exists(temp.path() + "/deltaidx-staged.malformed.entries"));
    EXPECT_TRUE(std::filesystem::is_symlink(symlink_path));
}

TEST(DeltaDatabaseTest, SnapshotBuilderRejectsSymlinkResidueWithoutFollowingIt) {
    TempDelta temp;
    TempDelta external;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(7);");
    const std::string outside = external.path() + "/outside";
    {
        std::ofstream output(outside);
        output << "sentinel";
    }
    const std::string residue = deltakernel::DeltaSnapshotTestPeer::FutureSnapshotTemp(*db, "t");
    ASSERT_EQ(symlink(outside.c_str(), residue.c_str()), 0);
    EXPECT_THROW(deltakernel::DeltaSnapshotTestPeer::Build(*db), std::system_error);
    EXPECT_TRUE(std::filesystem::is_symlink(residue));
    std::ifstream input(outside);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()), "sentinel");
}

TEST(DeltaDatabaseTest, CommitEnqueueFailureEndsPrivateTransactionBeforeAdmissionRelease) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int);");
    db->SetCommitEnqueueFailureForTest(true);
    EXPECT_THROW(RunSql(*db, session, "insert into t values(1);"), std::runtime_error);
    EXPECT_FALSE(session.txn.has_value());
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into t values(2);");
    EXPECT_THROW(RunSql(*db, session, "commit;"), std::runtime_error);
    db->SetCommitEnqueueFailureForTest(false);
    EXPECT_FALSE(session.txn.has_value());
    db->Checkpoint();
    RunSql(*db, session, "insert into t values(3);");
    EXPECT_EQ(Query(*db, session, "select k from t;").rows, (std::vector<std::vector<std::string>>{{"3"}}));
}

TEST(DeltaDatabaseTest, TypedRowsNullArithmeticAndCatalogSurviveRestart) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    const uint64_t initial_generation = db->CatalogGeneration();
    RunSql(*db, session, "create table typed(id int, amount float, note char(8), missing int);");
    EXPECT_EQ(db->CatalogGeneration(), initial_generation + 1);
    RunSql(*db, session, "set transaction isolation level snapshot isolation;");
    RunSql(*db, session, "insert into typed values(1, 1.5, 'ab', null);");
    RunSql(*db, session, "update typed set amount = amount + 0.25, note = 'xy', missing = 7 where id = 1;");
    auto result = Query(*db, session, "select note, amount, missing from typed where id = 1;");
    ASSERT_EQ(result.column_names, (std::vector<std::string>{"note", "amount", "missing"}));
    ASSERT_EQ(result.rows.size(), 1U);
    uint32_t expected_bits;
    const float expected = 1.75F;
    std::memcpy(&expected_bits, &expected, sizeof(expected_bits));
    EXPECT_EQ(result.rows[0], (std::vector<std::string>{"xy", std::to_string(expected_bits), "7"}));
    RunSql(*db, session, "insert into typed values(2, 2.5, 'nullrow', null);");
    result = Query(*db, session, "select missing from typed where id = 2;");
    ASSERT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"NULL"}}));
    db->Checkpoint();
    const uint64_t generation = db->CatalogGeneration();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(db->CatalogGeneration(), generation);
    result = Query(*db, session, "select note, amount, missing from typed where id = 1;");
    EXPECT_EQ(result.rows[0], (std::vector<std::string>{"xy", std::to_string(expected_bits), "7"}));
    auto tables = Query(*db, session, "show tables;");
    EXPECT_EQ(tables.rows, (std::vector<std::vector<std::string>>{{"typed"}}));
}

TEST(DeltaDatabaseTest, NonUniqueIndexAllowsDuplicatesAndScanFallback) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table duplicate(id int, label char(8));");
    RunSql(*db, session, "create index duplicate(label);");
    RunSql(*db, session, "insert into duplicate values(1, 'same');");
    RunSql(*db, session, "insert into duplicate values(2, 'same');");
    auto result = Query(*db, session, "select id from duplicate where label = 'same';");
    EXPECT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    result = Query(*db, session, "select id from duplicate where label = 'same';");
    EXPECT_EQ(result.rows.size(), 2U);
}

TEST(DeltaDatabaseTest, CsvLoadUsesTypedRowCodec) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "1,1.25,alpha\n2,2.5,beta\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, amount float, note char(8));");
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    auto result = Query(*db, session, "select id, note from loaded where amount >= 1.25;");
    EXPECT_EQ(result.rows, (std::vector<std::vector<std::string>>{{"1", "alpha"}, {"2", "beta"}}));
}

TEST(DeltaDatabaseTest, CsvLoadResolvesRelativePathsFromDatabaseDirectory) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "1,alpha\n2,beta\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table relative_loaded(id int, note char(8));");
    RunSql(*db, session, "load ../rows.csv into relative_loaded;");
    EXPECT_EQ(Query(*db, session, "select id, note from relative_loaded;").rows,
              (std::vector<std::vector<std::string>>{{"1", "alpha"}, {"2", "beta"}}));

    RunSql(*db, session, "create table absolute_loaded(id int, note char(8));");
    RunSql(*db, session, ("load " + csv + " into absolute_loaded;").c_str());
    EXPECT_EQ(Query(*db, session, "select id, note from absolute_loaded;").rows,
              (std::vector<std::vector<std::string>>{{"1", "alpha"}, {"2", "beta"}}));
}

TEST(DeltaDatabaseTest, LoadPublishesImmutableTable) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv, std::ios::binary);
        output << " note ,id,missing,amount,\r\n"
                  "\"a,b\",1,,1.25,\r\n"
                  "\"a\"\"b\",2,7,2.5,\r\n"
                  ",3,,3.5,\r\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, amount float, note char(8), missing int);");
    const uint64_t before = db->CatalogGeneration();
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    EXPECT_EQ(db->CatalogGeneration(), before);
    auto result = Query(*db, session, "select id, note, missing from loaded;");
    EXPECT_EQ(result.rows,
              (std::vector<std::vector<std::string>>{{"1", "a,b", "NULL"}, {"2", "a\"b", "7"}, {"3", "", "NULL"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    result = Query(*db, session, "select id from loaded;");
    EXPECT_EQ(result.rows.size(), 3U);
}

TEST(DeltaDatabaseTest, FailedLoadLeavesTableEmpty) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "id,note\n1,one\n2,two\n3,three\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, note char(8));");
    {
        std::ofstream output(csv, std::ios::trunc);
        output << "id,note\n1,one\nbad,two\n";
    }
    EXPECT_THROW(RunSql(*db, session, ("load " + csv + " into loaded;").c_str()), std::runtime_error);
    EXPECT_TRUE(Query(*db, session, "select * from loaded;").rows.empty());
}

TEST(DeltaDatabaseTest, IndexedLoadUsesImmutableTable) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "id,note\n1,one\n2,two\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(id int, note char(8));");
    RunSql(*db, session, "create index loaded_id on loaded(id);");
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    EXPECT_EQ(Query(*db, session, "select id from loaded;").rows,
              (std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
}

TEST(DeltaDatabaseTest, LoadedCompositeIndexRechecksAndOverlayIsSafe) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "a,b,v\n1,2,10\n1,2,11\n1,3,12\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table loaded(a int, b int, v int);");
    RunSql(*db, session, "create index loaded_ab on loaded(a, b);");
    RunSql(*db, session, ("load " + csv + " into loaded;").c_str());
    const size_t validations = db->SidecarValidationCountForTest();
    EXPECT_EQ(Query(*db, session, "select v from loaded where b = 2 and a = 1 and v = 11;").rows,
              (std::vector<std::vector<std::string>>{{"11"}}));
    EXPECT_EQ(Query(*db, session, "select v from loaded where b = 2 and a = 1;").rows.size(), 2U);
    EXPECT_EQ(db->SidecarValidationCountForTest(), validations);
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1;").rows.size(), 3U);
    RunSql(*db, session, "begin;");
    RunSql(*db, session, "insert into loaded values(1, 2, 13);");
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1 and b = 2;").rows.size(), 3U);
    RunSql(*db, session, "rollback;");
    RunSql(*db, session, "update loaded set b = 4 where a = 1 and b = 3;");
    EXPECT_TRUE(Query(*db, session, "select v from loaded where a = 1 and b = 3;").rows.empty());
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1 and b = 4;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    EXPECT_EQ(Query(*db, session, "select v from loaded where a = 1 and b = 4;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
}

TEST(DeltaDatabaseTest, OrderedSidecarCoversPrefixRangeAggregateAndJoinSources) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table line(w int, d int, o int, n int, item int);");
    RunSql(*db, session, "create table stock(w int, item int, quantity int);");
    RunSql(*db, session, "create index line_wdo on line(w, d, o, n);");
    RunSql(*db, session, "create index stock_wi on stock(w, item);");
    RunSql(*db, session, "insert into line values(1, 1, 10, 1, 4);");
    RunSql(*db, session, "insert into line values(1, 1, 11, 1, 5);");
    RunSql(*db, session, "insert into line values(1, 1, 12, 1, 6);");
    RunSql(*db, session, "insert into line values(2, 1, 10, 1, 4);");
    RunSql(*db, session, "insert into stock values(1, 4, 7);");
    RunSql(*db, session, "insert into stock values(1, 5, 30);");
    db->Checkpoint();
    EXPECT_EQ(Query(*db, session, "select item from line where w = 1 and d = 1 and o >= 10 and o < 12;").rows,
              (std::vector<std::vector<std::string>>{{"4"}, {"5"}}));
    EXPECT_EQ(Query(*db, session, "select min(o) from line where w = 1 and d = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(Query(*db, session, "select o from line where w = 1 and d = 1 order by o desc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"12"}}));
    EXPECT_EQ(
        Query(*db, session,
              "select count(distinct line.item) from line, stock where line.w = 1 and line.d = 1 and line.o >= 10 "
              "and line.o < 12 and stock.w = 1 and stock.item = line.item and stock.quantity < 20;")
            .rows,
        (std::vector<std::vector<std::string>>{{"1"}}));
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select item from line where w = 1 and d = 1 and o >= 10 and o < 12;").rows.size(),
              2U);
}

TEST(DeltaDatabaseTest, IndexedTransactionOverlayPublishesOnlyOnCommit) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession writer, reader;
    RunSql(*db, writer, "create table t(k int, v int);");
    RunSql(*db, writer, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(7, 1);");
    EXPECT_EQ(Query(*db, writer, "select v from t where k = 7;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 7;").rows.empty());
    RunSql(*db, writer, "rollback;");
    EXPECT_TRUE(Query(*db, reader, "select v from t where k = 7;").rows.empty());
    RunSql(*db, writer, "insert into t values(7, 2);");
    EXPECT_EQ(Query(*db, reader, "select v from t where k = 7;").rows, (std::vector<std::vector<std::string>>{{"2"}}));
}

TEST(DeltaDatabaseTest, OrderedOverlayEqualityProbeIsBounded) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession load, local;
    RunSql(*db, load, "create table t(k int, v int);");
    RunSql(*db, load, "insert into t values(-1, -1);");
    RunSql(*db, load, "create index t_k on t(k);");
    RunSql(*db, load, "begin;");
    for (int key = 0; key < 10000; ++key)
        RunSql(*db, load, ("insert into t values(" + std::to_string(key) + ", " + std::to_string(key) + ");").c_str());
    RunSql(*db, load, "commit;");
    EXPECT_EQ(Query(*db, load, "select v from t where k = 5000;").rows,
              (std::vector<std::vector<std::string>>{{"5000"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(load), (std::array<size_t, 3>{1, 1, 1}));

    RunSql(*db, local, "begin;");
    for (int key = 10000; key < 11024; ++key)
        RunSql(*db, local, ("insert into t values(" + std::to_string(key) + ", " + std::to_string(key) + ");").c_str());
    EXPECT_EQ(Query(*db, local, "select v from t where k = 10512;").rows,
              (std::vector<std::vector<std::string>>{{"10512"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(local), (std::array<size_t, 3>{1, 1, 1}));
    RunSql(*db, local, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedOverlayPrefixRangeProbeIsBounded) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession committed, local;
    RunSql(*db, committed, "create table t(a int, b int, v int);");
    RunSql(*db, committed, "insert into t values(-1, -1, -1);");
    RunSql(*db, committed, "create index t_ab on t(a, b);");
    RunSql(*db, committed, "begin;");
    for (int key = 0; key < 100; ++key)
        RunSql(*db, committed,
               ("insert into t values(1, " + std::to_string(key) + ", " + std::to_string(key) + ");").c_str());
    RunSql(*db, committed, "insert into t values(7, 10, 10);");
    RunSql(*db, committed, "insert into t values(7, 11, 11);");
    RunSql(*db, committed, "insert into t values(7, 12, 12);");
    RunSql(*db, committed, "commit;");
    RunSql(*db, local, "begin;");
    RunSql(*db, local, "insert into t values(7, 11, 111);");
    EXPECT_EQ(Query(*db, local, "select v from t where a = 7 and b >= 10 and b < 13;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"11"}, {"12"}, {"111"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(local), (std::array<size_t, 3>{4, 4, 4}));
    RunSql(*db, local, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedOverlayKeyMoveRespectsOldNewAndPrivateSnapshots) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, updater, fresh, local, deleter;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 9);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, old_snapshot, "begin;");
    RunSql(*db, updater, "update t set k = 2 where k = 1;");
    EXPECT_EQ(Query(*db, old_snapshot, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_TRUE(Query(*db, old_snapshot, "select v from t where k = 2;").rows.empty());
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 1;").rows.empty());
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    RunSql(*db, local, "begin;");
    RunSql(*db, local, "update t set k = 3 where k = 2;");
    EXPECT_TRUE(Query(*db, local, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(Query(*db, local, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    RunSql(*db, local, "rollback;");
    RunSql(*db, deleter, "delete from t where k = 2;");
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(Query(*db, old_snapshot, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"9"}}));
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedOverlayCodecMatchesSqlOrdering) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(i int, f float, c char(8));");
    RunSql(*db, session, "insert into t values(-99, -99.0, 'seed');");
    RunSql(*db, session, "create index t_i on t(i);");
    RunSql(*db, session, "create index t_f on t(f);");
    RunSql(*db, session, "create index t_c on t(c);");
    RunSql(*db, session, "insert into t values(-2147483648, -2.0, '');");
    RunSql(*db, session, "insert into t values(-1, -0.0, 'a');");
    RunSql(*db, session, "insert into t values(0, 0.0, 'aa');");
    RunSql(*db, session, "insert into t values(2147483647, 2.0, 'b');");
    EXPECT_EQ(Query(*db, session, "select i from t where i >= -1 and i <= 0;").rows,
              (std::vector<std::vector<std::string>>{{"-1"}, {"0"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(session), (std::array<size_t, 3>{2, 2, 2}));
    EXPECT_EQ(Query(*db, session, "select i from t where f = 0.0;").rows,
              (std::vector<std::vector<std::string>>{{"-1"}, {"0"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(session), (std::array<size_t, 3>{2, 2, 2}));
    EXPECT_EQ(Query(*db, session, "select i from t where c >= 'a' and c < 'b';").rows,
              (std::vector<std::vector<std::string>>{{"-1"}, {"0"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(session), (std::array<size_t, 3>{2, 2, 2}));
}

TEST(DeltaDatabaseTest, OrderedOverlaySurvivesWalReopenAndCheckpointFold) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "insert into t values(-1, -1);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(4, 40);");
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select v from t where k = 4;").rows,
              (std::vector<std::vector<std::string>>{{"40"}}));
    db->Checkpoint();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select v from t where k = 4;").rows,
              (std::vector<std::vector<std::string>>{{"40"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(session), (std::array<size_t, 3>{0, 0, 1}));
}

TEST(DeltaDatabaseTest, CheckpointFoldsCurrentOverlayWithoutLatestDuplicateProbe) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "insert into t values(-1, -1);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(4, 40);");

    EXPECT_EQ(Query(*db, session, "select v from t where k = 4;").rows,
              (std::vector<std::vector<std::string>>{{"40"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(session), (std::array<size_t, 3>{1, 1, 1}));
    db->Checkpoint();
    EXPECT_EQ(Query(*db, session, "select v from t where k = 4;").rows,
              (std::vector<std::vector<std::string>>{{"40"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(session), (std::array<size_t, 3>{0, 0, 1}));
}

TEST(DeltaDatabaseTest, OrderedMinStopsAfterDeletedPrefixAndPreservesOldSnapshot) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, fresh;
    RunSql(*db, setup, "create table queue(w int, d int, o int);");
    RunSql(*db, setup, "begin;");
    for (int order = 0; order < 900; ++order)
        RunSql(*db, setup, ("insert into queue values(1, 1, " + std::to_string(order) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "create index queue_wdo on queue(w, d, o);");
    RunSql(*db, old_snapshot, "begin;");
    RunSql(*db, setup, "delete from queue where w = 1 and d = 1 and o < 100;");

    EXPECT_EQ(Query(*db, old_snapshot, "select min(o) from queue where w = 1 and d = 1;").rows,
              (std::vector<std::vector<std::string>>{{"0"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(old_snapshot), (std::array<size_t, 8>{1, 1, 0, 1}));
    EXPECT_EQ(Query(*db, fresh, "select min(o) from queue where w = 1 and d = 1;").rows,
              (std::vector<std::vector<std::string>>{{"100"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(fresh), (std::array<size_t, 8>{1, 1, 0, 1}));
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedLatestBitmapKeepsOldSnapshotAndReverseBounds) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, fresh;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(1);");
    RunSql(*db, setup, "insert into t values(2);");
    RunSql(*db, setup, "insert into t values(3);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, old_snapshot, "begin;");
    RunSql(*db, setup, "delete from t where k = 1;");
    db->EnableDiagnosticsForTest();
    const auto before_routes = db->RouteDiagnosticsForTest();

    EXPECT_EQ(Query(*db, old_snapshot, "select min(k) from t;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_EQ(Query(*db, fresh, "select k from t order by k desc limit 3;").rows,
              (std::vector<std::vector<std::string>>{{"3"}, {"2"}}));
    EXPECT_TRUE(Query(*db, fresh, "select k from t where k < 0 order by k desc limit 1;").rows.empty());
    const auto after_routes = db->RouteDiagnosticsForTest();
    EXPECT_EQ(after_routes[0] - before_routes[0], 2U); // fresh ordered probes use current state.
    EXPECT_EQ(after_routes[1] - before_routes[1], 1U); // pinned old snapshot uses historical state.
    EXPECT_EQ(after_routes[2] - before_routes[2], 2U);
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedLatestBitmapSkipsDeadPrefixWithSummary) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, latest;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "begin;");
    for (int key = 0; key <= 10000; ++key)
        RunSql(*db, setup, ("insert into t values(" + std::to_string(key) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "delete from t where k < 10000;");
    db->EnableDiagnosticsForTest();
    const auto before_routes = db->RouteDiagnosticsForTest();

    EXPECT_EQ(Query(*db, latest, "select min(k) from t;").rows, (std::vector<std::vector<std::string>>{{"10000"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(latest)[0], 1U);
    EXPECT_LT(db->LiveSummaryProbeCensusForTest(latest), 16U);
    const auto after_routes = db->RouteDiagnosticsForTest();
    EXPECT_EQ(after_routes[0] - before_routes[0], 1U);
    EXPECT_EQ(after_routes[2] - before_routes[2], 1U);
    EXPECT_GE(after_routes[3] - before_routes[3], 1U);
    EXPECT_GE(after_routes[4] - before_routes[4], 1U);
    const auto tail = db->TailDiagnosticsForTest();
    EXPECT_GT(tail[0], 0U);
}

TEST(DeltaDatabaseTest, OrderedReverseSummaryRetainsPreviousWordBit63Once) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, latest;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "begin;");
    for (int key = 0; key < 130; ++key)
        RunSql(*db, setup, ("insert into t values(" + std::to_string(key) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "delete from t where k < 63;");
    RunSql(*db, setup, "delete from t where k >= 64 and k < 128;");

    EXPECT_EQ(Query(*db, latest, "select k from t where k < 128 order by k desc limit 2;").rows,
              (std::vector<std::vector<std::string>>{{"63"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(latest)[0], 1U);
}

TEST(DeltaDatabaseTest, PrivateInsertKeyMovesAndDeleteLeaveNoCurrentGhosts) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession writer, fresh;
    RunSql(*db, writer, "create table t(k int);");
    RunSql(*db, writer, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(1);");
    RunSql(*db, writer, "update t set k = 2 where k = 1;");
    EXPECT_TRUE(Query(*db, writer, "select k from t where k = 1;").rows.empty());
    EXPECT_EQ(db->IndexProbeCensusForTest(writer), (std::array<size_t, 3>{0, 0, 0}));
    EXPECT_EQ(Query(*db, writer, "select k from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"2"}}));
    RunSql(*db, writer, "update t set k = 3 where k = 2;");
    EXPECT_TRUE(Query(*db, writer, "select k from t where k = 2;").rows.empty());
    EXPECT_EQ(db->IndexProbeCensusForTest(writer), (std::array<size_t, 3>{0, 0, 0}));
    EXPECT_EQ(Query(*db, writer, "select k from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"3"}}));
    RunSql(*db, writer, "delete from t where k = 3;");
    RunSql(*db, writer, "commit;");

    for (int key = 1; key <= 3; ++key) {
        EXPECT_TRUE(Query(*db, fresh, ("select k from t where k = " + std::to_string(key) + ";").c_str()).rows.empty());
        EXPECT_EQ(db->IndexProbeCensusForTest(fresh), (std::array<size_t, 3>{0, 0, 0}));
    }
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_TRUE(Query(*db, fresh, "select k from t where k >= 1 and k <= 3;").rows.empty());
}

TEST(DeltaDatabaseTest, ExistingKeyMovesCoalesceCurrentOverlay) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, writer, fresh;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "update t set k = 2 where k = 1;");
    RunSql(*db, writer, "update t set k = 3 where k = 2;");
    RunSql(*db, writer, "commit;");

    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());

    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(db->IndexProbeCensusForTest(fresh), (std::array<size_t, 3>{0, 0, 0}));
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"10"}}));

    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "update t set v = 11 where k = 3;");
    RunSql(*db, writer, "commit;");
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"11"}}));
    db->Checkpoint();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"11"}}));
}

TEST(DeltaDatabaseTest, ExistingKeyMoveThenDeleteLeavesNoCurrentGhost) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, writer, fresh;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(1);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "update t set k = 2 where k = 1;");
    RunSql(*db, writer, "delete from t where k = 2;");
    RunSql(*db, writer, "commit;");

    EXPECT_TRUE(Query(*db, fresh, "select k from t where k = 2;").rows.empty());
    EXPECT_EQ(db->IndexProbeCensusForTest(fresh), (std::array<size_t, 3>{0, 0, 0}));
}

TEST(DeltaDatabaseTest, CurrentBaseKeepsUnchangedIndexKeys) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, fresh;
    RunSql(*db, setup, "create table t(a int, b int, v int);");
    RunSql(*db, setup, "insert into t values(1, 2, 10);");
    RunSql(*db, setup, "create index t_a on t(a);");
    RunSql(*db, setup, "create index t_b on t(b);");
    RunSql(*db, setup, "update t set v = 11 where a = 1;");
    EXPECT_EQ(Query(*db, fresh, "select v from t where a = 1;").rows, (std::vector<std::vector<std::string>>{{"11"}}));
    EXPECT_EQ(Query(*db, fresh, "select min(a) from t;").rows, (std::vector<std::vector<std::string>>{{"1"}}));

    RunSql(*db, setup, "update t set a = 4 where b = 2;");
    EXPECT_EQ(Query(*db, fresh, "select v from t where b = 2;").rows, (std::vector<std::vector<std::string>>{{"11"}}));
    EXPECT_TRUE(Query(*db, fresh, "select v from t where a = 1;").rows.empty());
    EXPECT_EQ(Query(*db, fresh, "select v from t where a = 4;").rows, (std::vector<std::vector<std::string>>{{"11"}}));
}

TEST(DeltaDatabaseTest, CommitTransfersNonuniqueCandidatesAndSkipsNewIdsInBaseLookup) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, writer, fresh;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(0, 1);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, old_snapshot, "begin;");
    db->EnableDiagnosticsForTest();

    const auto before = db->CurrentIndexCensusForTest();
    RunSql(*db, writer, "begin;");
    RunSql(*db, writer, "insert into t values(7, 10);");
    RunSql(*db, writer, "insert into t values(7, 20);");
    RunSql(*db, writer, "commit;");
    const auto inserted = db->CurrentIndexCensusForTest();
    EXPECT_EQ(inserted[0], before[0]);
    EXPECT_EQ(inserted[1] - before[1], 2U);
    EXPECT_EQ(inserted[2] - before[2], 2U);

    EXPECT_TRUE(Query(*db, old_snapshot, "select v from t where k = 7;").rows.empty());
    EXPECT_EQ(db->IndexProbeCensusForTest(old_snapshot), (std::array<size_t, 3>{1, 2, 2}));
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 7;").rows,
              (std::vector<std::vector<std::string>>{{"10"}, {"20"}}));
    EXPECT_EQ(db->IndexProbeCensusForTest(fresh), (std::array<size_t, 3>{1, 2, 2}));

    RunSql(*db, writer, "update t set k = 1 where k = 0;");
    EXPECT_GT(db->CurrentIndexCensusForTest()[0], inserted[0]);
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 0;").rows.empty());
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"1"}}));
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedReverseBitmapHonorsSameWordLowerBound) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, fresh;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "begin;");
    for (int key = 0; key < 70; ++key)
        RunSql(*db, setup, ("insert into t values(" + std::to_string(key) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, setup, "delete from t where k >= 66;");

    EXPECT_TRUE(Query(*db, fresh, "select k from t where k >= 66 and k < 70 order by k desc limit 1;").rows.empty());
    EXPECT_EQ(db->OrderedProbeCensusForTest(fresh)[0], 0U);
}

TEST(DeltaDatabaseTest, OrderedCurrentStateSurvivesReopenAndPrivateKeyMoveAbort) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, local, fresh;
    RunSql(*db, setup, "create table t(k int, v int);");
    RunSql(*db, setup, "insert into t values(1, 10);");
    RunSql(*db, setup, "insert into t values(2, 20);");
    RunSql(*db, setup, "create index t_k on t(k);");
    RunSql(*db, local, "begin;");
    RunSql(*db, local, "update t set k = 3 where k = 1;");
    EXPECT_TRUE(Query(*db, local, "select v from t where k = 1;").rows.empty());
    EXPECT_EQ(Query(*db, local, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    RunSql(*db, local, "rollback;");
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 3;").rows.empty());

    RunSql(*db, setup, "update t set k = 3 where k = 1;");
    RunSql(*db, setup, "delete from t where k = 2;");
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 1;").rows.empty());
    EXPECT_TRUE(Query(*db, fresh, "select v from t where k = 2;").rows.empty());
    EXPECT_EQ(Query(*db, fresh, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, OrderedDescLimitStreamsReverseAndSurvivesDuplicateFold) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table orders(w int, d int, c int, o int);");
    RunSql(*db, session, "begin;");
    for (int order = 0; order < 1000; ++order)
        RunSql(*db, session, ("insert into orders values(1, 1, 7, " + std::to_string(order) + ");").c_str());
    RunSql(*db, session, "commit;");
    RunSql(*db, session, "create index orders_wdco on orders(w, d, c, o);");
    const char* latest = "select o from orders where w = 1 and d = 1 and c = 7 order by o desc limit 1;";

    EXPECT_EQ(Query(*db, session, latest).rows, (std::vector<std::vector<std::string>>{{"999"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1}));
    EXPECT_EQ(
        Query(*db, session, "select o from orders where w = 1 and d = 1 and c = 7 order by o desc limit 2 offset 3;")
            .rows,
        (std::vector<std::vector<std::string>>{{"996"}, {"995"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{5, 5, 0, 1}));
    RunSql(*db, session, "update orders set o = 1000 where w = 1 and d = 1 and c = 7 and o = 999;");
    RunSql(*db, session, "update orders set o = 999 where w = 1 and d = 1 and c = 7 and o = 1000;");
    EXPECT_EQ(Query(*db, session, latest).rows, (std::vector<std::vector<std::string>>{{"999"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1, 1, 1, 0, 0}));

    db->Checkpoint();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, latest).rows, (std::vector<std::vector<std::string>>{{"999"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1}));
}

TEST(DeltaDatabaseTest, DiagnosticsSnapshotQueryCensusBeforeCommitUnlock) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table orders(k int);");
    RunSql(*db, setup, "begin;");
    for (int key = 1; key <= 24; ++key)
        RunSql(*db, setup, ("insert into orders values(" + std::to_string(key) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "create index orders_k on orders(k);");
    db->EnableDiagnosticsForTest();

    std::mutex mutex;
    std::condition_variable ready;
    bool first_waiting = false;
    bool release = false;
    int hook_calls = 0;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (hook_calls++ != 0)
            return;
        first_waiting = true;
        ready.notify_all();
        ready.wait(lock, [&] { return release; });
    });
    const auto ordered_before = db->QueryDiagnosticsForTest();
    deltakernel::DeltaSession first;
    std::thread first_query([&] {
        EXPECT_EQ(Query(*db, first, "select k from orders order by k desc limit 2 offset 3;").rows.size(), 2U);
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ready.wait(lock, [&] { return first_waiting; });
    }
    deltakernel::DeltaSession second;
    std::thread second_query(
        [&] { EXPECT_EQ(Query(*db, second, "select k from orders order by k desc limit 3;").rows.size(), 3U); });
    ASSERT_TRUE(WaitForCommitQueueDepth(*db, 2));
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    ready.notify_all();
    first_query.join();
    second_query.join();
    EXPECT_EQ(db->OrderedProbeCensusForTest(first), (std::array<size_t, 8>{5, 5, 0, 1}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(second), (std::array<size_t, 8>{3, 3, 0, 1}));
    EXPECT_EQ(db->QueryDiagnosticsForTest()[0] - ordered_before[0], 8U); // 5 then 3; never reuse the latter.
    db->SetCommitBatchHookForTest({});

    RunSql(*db, setup, "create table sources(k int);");
    RunSql(*db, setup, "create table targets(k int, v int);");
    RunSql(*db, setup, "begin;");
    for (int key = 1; key <= 24; ++key) {
        RunSql(*db, setup, ("insert into sources values(" + std::to_string(key) + ");").c_str());
        RunSql(*db, setup,
               ("insert into targets values(" + std::to_string(key) + ", " + std::to_string(key) + ");").c_str());
    }
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "create index targets_k on targets(k);");

    first_waiting = false;
    release = false;
    hook_calls = 0;
    db->SetCommitBatchHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        if (hook_calls++ != 0)
            return;
        first_waiting = true;
        ready.notify_all();
        ready.wait(lock, [&] { return release; });
    });
    const auto join_before = db->QueryDiagnosticsForTest();
    deltakernel::DeltaSession first_join;
    std::thread first_join_query([&] {
        EXPECT_EQ(
            Query(*db, first_join,
                  "select count(targets.v) from sources, targets where sources.k = targets.k and sources.k <= 12;")
                .rows[0][0],
            "12");
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ready.wait(lock, [&] { return first_waiting; });
    }
    deltakernel::DeltaSession second_join;
    std::thread second_join_query([&] {
        EXPECT_EQ(Query(*db, second_join, "select count(targets.v) from sources, targets where sources.k = targets.k;")
                      .rows[0][0],
                  "24");
    });
    ASSERT_TRUE(WaitForCommitQueueDepth(*db, 2));
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    ready.notify_all();
    first_join_query.join();
    second_join_query.join();
    EXPECT_EQ(db->JoinProbeCensusForTest(first_join), (std::array<size_t, 8>{12, 12, 12, 24, 0, 12, 0, 0}));
    EXPECT_EQ(db->JoinProbeCensusForTest(second_join), (std::array<size_t, 8>{24, 24, 24, 24, 0, 24, 0, 0}));
    EXPECT_EQ(db->QueryDiagnosticsForTest()[1] - join_before[1], 36U); // 12 then 24; never reuse the latter.
}

TEST(DeltaDatabaseTest, DiagnosticsReportRoutesRatherThanQueryResults) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table ordered(k int);");
    RunSql(*db, session, "insert into ordered values(1);");
    RunSql(*db, session, "insert into ordered values(2);");
    RunSql(*db, session, "create index ordered_k on ordered(k);");
    db->EnableDiagnosticsForTest();
    const auto before_ordered = db->QueryDiagnosticsForTest();
    EXPECT_EQ(Query(*db, session, "select k from ordered order by k limit 10;").rows.size(), 2U);
    const auto after_ordered = db->QueryDiagnosticsForTest();
    EXPECT_EQ(after_ordered[4] - before_ordered[4], 1U); // ordered stream despite fewer than LIMIT rows.
    EXPECT_EQ(after_ordered[5] - before_ordered[5], 0U);

    RunSql(*db, session, "create table left_rows(k int);");
    RunSql(*db, session, "create table right_rows(k int);");
    RunSql(*db, session, "insert into left_rows values(1);");
    const auto before_join = db->QueryDiagnosticsForTest();
    EXPECT_EQ(
        Query(*db, session, "select count(right_rows.k) from left_rows, right_rows where left_rows.k = right_rows.k;")
            .rows[0][0],
        "0");
    const auto after_join = db->QueryDiagnosticsForTest();
    EXPECT_EQ(after_join[2] - before_join[2], 0U);
    EXPECT_EQ(after_join[3] - before_join[3], 1U); // Empty right side still selected fallback.

    db->SetExecuteLockHookForTest([] { throw std::runtime_error("diagnostic test hook"); });
    const auto before_error = db->QueryDiagnosticsForTest();
    EXPECT_THROW(Query(*db, session, "select k from ordered order by k limit 1;"), std::runtime_error);
    EXPECT_EQ(db->QueryDiagnosticsForTest(), before_error);
    db->SetExecuteLockHookForTest({});
}

TEST(DeltaDatabaseTest, DiagnosticsExposeBlockedExecuteBeforeCompletion) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, blocked;
    RunSql(*db, setup, "create table t(k int);");
    RunSql(*db, setup, "insert into t values(1);");
    db->EnableDiagnosticsForTest();

    std::mutex mutex;
    std::condition_variable condition;
    bool entered = false;
    bool release = false;
    db->SetExecuteLockHookForTest([&] {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        condition.notify_all();
        ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2), [&] { return release; }));
    });
    std::exception_ptr error;
    std::thread worker([&] {
        try {
            Query(*db, blocked, "select k from t;");
        } catch (...) {
            error = std::current_exception();
        }
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2), [&] { return entered; }));
    }
    EXPECT_EQ(db->InflightDiagnosticsForTest(), (std::array<uint64_t, 5>{0, 0, 1, 0, 0}));
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    condition.notify_all();
    worker.join();
    EXPECT_FALSE(error);
    EXPECT_EQ(db->InflightDiagnosticsForTest()[2], 0U);
    EXPECT_GT(db->TailDiagnosticsForTest()[0], 0U);
    db->SetExecuteLockHookForTest({});
}

TEST(DeltaDatabaseTest, OrderedMinMaxRecheckSnapshotAndPrivateKeyMoves) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, old_snapshot, fresh, local;
    RunSql(*db, setup, "create table valueset(g int, k int);");
    RunSql(*db, setup, "insert into valueset values(1, 10);");
    RunSql(*db, setup, "create index valueset_gk on valueset(g, k);");
    RunSql(*db, old_snapshot, "begin;");
    RunSql(*db, setup, "update valueset set k = 20 where g = 1 and k = 10;");

    EXPECT_EQ(Query(*db, old_snapshot, "select min(k) from valueset where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(old_snapshot), (std::array<size_t, 8>{1, 1, 0, 1, 1, 1, 0, 0}));
    EXPECT_EQ(Query(*db, old_snapshot, "select max(k) from valueset where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(old_snapshot), (std::array<size_t, 8>{2, 2, 0, 1, 1, 1, 0, 0}));
    EXPECT_EQ(Query(*db, fresh, "select min(k) from valueset where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"20"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(fresh), (std::array<size_t, 8>{1, 1, 0, 1, 1, 1, 0, 0}));
    EXPECT_EQ(Query(*db, fresh, "select max(k) from valueset where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"20"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(fresh), (std::array<size_t, 8>{1, 1, 0, 1, 1, 1, 0, 0}));

    RunSql(*db, local, "begin;");
    RunSql(*db, local, "update valueset set k = 5 where g = 1 and k = 20;");
    EXPECT_EQ(Query(*db, local, "select min(k) from valueset where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"5"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(local), (std::array<size_t, 8>{1, 1, 0, 1, 2, 2, 0, 0}));
    EXPECT_EQ(Query(*db, local, "select max(k) from valueset where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"5"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(local), (std::array<size_t, 8>{2, 2, 0, 1, 2, 2, 0, 0}));
    RunSql(*db, local, "rollback;");
    RunSql(*db, old_snapshot, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedMergeDeduplicatesBaseAndOverlayBeforePredicateRecheck) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table ranked(k int, wanted int);");
    RunSql(*db, session, "insert into ranked values(1, 1);");
    RunSql(*db, session, "insert into ranked values(2, 0);");
    RunSql(*db, session, "create index ranked_k on ranked(k);");
    RunSql(*db, session, "update ranked set k = 3 where k = 2;");
    RunSql(*db, session, "update ranked set k = 2 where k = 3;");

    EXPECT_EQ(Query(*db, session, "select k from ranked where wanted = 1 order by k desc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{2, 2, 0, 1, 1, 1, 0, 0}));
}

TEST(DeltaDatabaseTest, OrderedEarlyStopDoesNotMaterializeSameKeyOverlayBuckets) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup, local;
    RunSql(*db, setup, "create table ranked(k int, v int);");
    RunSql(*db, setup, "insert into ranked values(1, 0);");
    RunSql(*db, setup, "create index ranked_k on ranked(k);");
    RunSql(*db, setup, "begin;");
    for (int value = 1; value <= 512; ++value)
        RunSql(*db, setup, ("insert into ranked values(1, " + std::to_string(value) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, setup, "begin;");
    for (int value = 513; value <= 1024; ++value)
        RunSql(*db, setup, ("insert into ranked values(1, " + std::to_string(value) + ");").c_str());
    RunSql(*db, setup, "commit;");
    RunSql(*db, local, "begin;");
    for (int value = 1025; value <= 1536; ++value)
        RunSql(*db, local, ("insert into ranked values(1, " + std::to_string(value) + ");").c_str());

    EXPECT_EQ(Query(*db, local, "select v from ranked order by k asc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"0"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(local), (std::array<size_t, 8>{1, 1, 0, 1, 3, 3, 0, 0}));

    for (int value = 1537; value <= 2048; ++value)
        RunSql(*db, local, ("insert into ranked values(0, " + std::to_string(value) + ");").c_str());
    EXPECT_EQ(Query(*db, local, "select v from ranked order by k asc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"1537"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(local), (std::array<size_t, 8>{1, 1, 0, 1, 3, 3, 0, 0}));
    EXPECT_EQ(Query(*db, local, "select v from ranked where k = 1;").rows.size(), 1537U);
    RunSql(*db, local, "rollback;");
}

TEST(DeltaDatabaseTest, OrderedTypedRangeBoundsStopAtFirstValidCandidate) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table metrics(g int, f float, label char(8));");
    RunSql(*db, session, "insert into metrics values(1, 1.0, 'a');");
    RunSql(*db, session, "insert into metrics values(1, 1.5, 'b');");
    RunSql(*db, session, "insert into metrics values(1, 2.0, 'c');");
    RunSql(*db, session, "insert into metrics values(1, 3.0, 'd');");
    RunSql(*db, session, "create index metrics_gf on metrics(g, f);");
    RunSql(*db, session, "create index metrics_gl on metrics(g, label);");
    uint32_t one_point_five_bits;
    uint32_t two_bits;
    const float one_point_five = 1.5F;
    const float two = 2.0F;
    std::memcpy(&one_point_five_bits, &one_point_five, sizeof(one_point_five_bits));
    std::memcpy(&two_bits, &two, sizeof(two_bits));

    EXPECT_EQ(Query(*db, session, "select min(f) from metrics where g = 1 and f > 1.0 and f <= 2.0;").rows,
              (std::vector<std::vector<std::string>>{{std::to_string(one_point_five_bits)}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1}));
    EXPECT_EQ(Query(*db, session, "select max(f) from metrics where g = 1 and f > 1.0 and f <= 2.0;").rows,
              (std::vector<std::vector<std::string>>{{std::to_string(two_bits)}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1}));
    EXPECT_EQ(Query(*db, session, "select min(label) from metrics where g = 1 and label > 'a' and label <= 'c';").rows,
              (std::vector<std::vector<std::string>>{{"b"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1}));
    EXPECT_EQ(Query(*db, session, "select max(label) from metrics where g = 1 and label > 'a' and label <= 'c';").rows,
              (std::vector<std::vector<std::string>>{{"c"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1}));
}

TEST(DeltaDatabaseTest, OrderedLimitFallsBackWhenIndexSuffixDoesNotMatch) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table people(w int, d int, id int, surname char(8));");
    RunSql(*db, session, "insert into people values(1, 1, 1, 'smith');");
    RunSql(*db, session, "insert into people values(1, 1, 2, 'adams');");
    RunSql(*db, session, "insert into people values(1, 1, 3, 'jones');");
    RunSql(*db, session, "create index people_wdi on people(w, d, id);");

    EXPECT_EQ(
        Query(*db, session, "select surname from people where w = 1 and d = 1 order by surname asc limit 1;").rows,
        (std::vector<std::vector<std::string>>{{"adams"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{0, 0, 3, 0}));
}

TEST(DeltaDatabaseTest, OrderedCompositeSuffixRequiresUniformDirection) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table ranked(g int, a int, b int);");
    RunSql(*db, session, "insert into ranked values(1, 1, 2);");
    RunSql(*db, session, "insert into ranked values(1, 1, 1);");
    RunSql(*db, session, "insert into ranked values(1, 2, 0);");
    RunSql(*db, session, "create index ranked_gab on ranked(g, a, b);");

    EXPECT_EQ(Query(*db, session, "select a, b from ranked where g = 1 order by a asc, b asc limit 2;").rows,
              (std::vector<std::vector<std::string>>{{"1", "1"}, {"1", "2"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{2, 2, 0, 1}));
    EXPECT_EQ(Query(*db, session, "select a, b from ranked where g = 1 order by a asc, b desc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"1", "2"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{0, 0, 3, 0}));
}

TEST(DeltaDatabaseTest, OrderedLimitIncludesNullOverlayKeysWithIndexOrdering) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table nullable_values(g int, k int);");
    RunSql(*db, session, "insert into nullable_values values(1, 1);");
    RunSql(*db, session, "create index nullable_values_gk on nullable_values(g, k);");
    RunSql(*db, session, "insert into nullable_values values(1, null);");

    EXPECT_EQ(Query(*db, session, "select k from nullable_values where g = 1 order by k asc limit 1;").rows,
              (std::vector<std::vector<std::string>>{{"NULL"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{1, 1, 0, 1, 1, 1, 0, 0}));
    EXPECT_EQ(Query(*db, session, "select min(k) from nullable_values where g = 1;").rows,
              (std::vector<std::vector<std::string>>{{"1"}}));
    EXPECT_EQ(db->OrderedProbeCensusForTest(session), (std::array<size_t, 8>{2, 2, 0, 1, 1, 1, 0, 0}));
}

TEST(DeltaDatabaseTest, OpenRebuildsMissingSidecarOnce) {
    TempDelta temp;
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "k,v\n1,9\n";
    }
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, ("load " + csv + " into t;").c_str());
    db.reset();
    ASSERT_TRUE(std::filesystem::remove(temp.path() + "/db/deltaidx.1.1"));
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    const size_t validations = db->SidecarValidationCountForTest();
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows.size(), 1U);
    EXPECT_EQ(db->SidecarValidationCountForTest(), validations);
}

TEST(DeltaDatabaseTest, MappedSidecarPointProbesDoNoQueryIoAndRemapAfterCheckpoint) {
    TempDelta temp;
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    const std::string csv = temp.path() + "/rows.csv";
    {
        std::ofstream output(csv);
        output << "k,v\n";
        for (int value = 0; value < 10000; ++value)
            output << value << ',' << value * 2 << '\n';
    }
    RunSql(*db, session, ("load " + csv + " into t;").c_str());
    db->Checkpoint();
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");

    size_t mapped_bytes = 0;
    for (int repeat = 0; repeat < 3; ++repeat) {
        EXPECT_EQ(Query(*db, session, "select v from t where k = 7777;").rows,
                  (std::vector<std::vector<std::string>>{{"15554"}}));
        const auto census = db->SidecarIoCensusForTest(session);
        EXPECT_EQ(census[0], 0U);
        EXPECT_EQ(census[1], 0U);
        EXPECT_LE(census[2], 32U);
        EXPECT_GT(census[3], 10000U * sizeof(uint64_t));
        if (repeat == 0)
            mapped_bytes = census[3];
        else
            EXPECT_EQ(census[3], mapped_bytes);
    }

    const size_t validations = db->SidecarValidationCountForTest();
    RunSql(*db, session, "update t set k = 20000 where k = 7777;");
    db->Checkpoint();
    EXPECT_GT(db->SidecarValidationCountForTest(), validations);
    EXPECT_TRUE(Query(*db, session, "select v from t where k = 7777;").rows.empty());
    EXPECT_EQ(Query(*db, session, "select v from t where k = 20000;").rows,
              (std::vector<std::vector<std::string>>{{"15554"}}));
    const auto remapped = db->SidecarIoCensusForTest(session);
    EXPECT_EQ(remapped[0], 0U);
    EXPECT_EQ(remapped[1], 0U);
    EXPECT_LE(remapped[2], 32U);
    EXPECT_EQ(remapped[3], mapped_bytes);
}

TEST(DeltaDatabaseTest, CorruptSidecarIsRebuiltBeforeMapping) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(1, 9);");
    db->Checkpoint();
    db.reset();

    const std::string sidecar = temp.path() + "/deltaidx.1.1";
    {
        std::fstream file(sidecar, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file);
        const uint64_t invalid_first_offset = 1;
        file.seekp(96);
        file.write(reinterpret_cast<const char*>(&invalid_first_offset), sizeof(invalid_first_offset));
        ASSERT_TRUE(file);
    }
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_EQ(db->SidecarIoCensusForTest(session)[0], 0U);
    EXPECT_GT(db->SidecarIoCensusForTest(session)[3], 0U);
}

TEST(DeltaDatabaseTest, SidecarSymlinkRebuildDoesNotTouchTarget) {
    TempDelta temp;
    ASSERT_TRUE(std::filesystem::create_directory(temp.path() + "/db"));
    auto db = deltakernel::DeltaDatabase::Create(temp.path() + "/db");
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(1, 9);");
    db->Checkpoint();
    db.reset();

    const std::string sentinel = temp.path() + "/sentinel";
    {
        std::ofstream output(sentinel);
        output << "do-not-touch";
    }
    const std::string sidecar = temp.path() + "/db/deltaidx.1.1";
    ASSERT_TRUE(std::filesystem::remove(sidecar));
    ASSERT_NO_THROW(std::filesystem::create_symlink(sentinel, sidecar));
    db = deltakernel::DeltaDatabase::Open(temp.path() + "/db");
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows, (std::vector<std::vector<std::string>>{{"9"}}));
    EXPECT_FALSE(std::filesystem::is_symlink(sidecar));
    std::ifstream input(sentinel);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()), "do-not-touch");
}

TEST(DeltaDatabaseTest, CheckpointRebuildsDirtyIndexedTable) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(3, 8);");
    db->Checkpoint();
    EXPECT_EQ(Query(*db, session, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"8"}}));
}

TEST(DeltaDatabaseTest, V3SidecarSurvivesWalOnlyReopenAndRebuildsInvalidFormat) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(1, 9);");
    RunSql(*db, session, "insert into t values(2, 8);");
    db->Checkpoint();
    const std::string sidecar = temp.path() + "/deltaidx.1.1";
    auto header = ReadSidecarHeader(sidecar);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 0), 0x58444941544c4544ULL);
    EXPECT_EQ(ReadLeAt<uint32_t>(header, 8), 3U);
    EXPECT_EQ(ReadLeAt<uint32_t>(header, 12), 96U);
    EXPECT_EQ(ReadLeAt<uint32_t>(header, 16), 16U);
    EXPECT_EQ(ReadLeAt<uint32_t>(header, 28), 0U);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 48), 2U);
    EXPECT_EQ(std::vector<uint8_t>(header.begin(), header.begin() + 8),
              (std::vector<uint8_t>{'D', 'E', 'L', 'T', 'A', 'I', 'D', 'X'}));
    const uint64_t snapshot_epoch = ReadLeAt<uint64_t>(header, 40);
    {
        std::array<uint8_t, 16> second_entry{};
        std::ifstream input(sidecar, std::ios::binary);
        input.seekg(96 + 16);
        input.read(reinterpret_cast<char*>(second_entry.data()), second_entry.size());
        ASSERT_TRUE(input);
        EXPECT_EQ(second_entry[0], 5U);
        EXPECT_EQ(second_entry[8], 1U);
    }

    RunSql(*db, session, "update t set v = 10 where k = 1;");
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(ReadLeAt<uint64_t>(ReadSidecarHeader(sidecar), 40), snapshot_epoch);
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));

    {
        std::fstream file(sidecar, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file);
        const uint32_t v1 = 1;
        file.seekp(8);
        const std::array<uint8_t, 4> bytes{static_cast<uint8_t>(v1), 0, 0, 0};
        file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(ReadLeAt<uint32_t>(ReadSidecarHeader(sidecar), 8), 3U);
    EXPECT_EQ(Query(*db, session, "select v from t where k = 1;").rows,
              (std::vector<std::vector<std::string>>{{"10"}}));
}

TEST(DeltaDatabaseTest, V3SidecarPadsNonAlignedKeysAndMasksLiveTail) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table words(k char(8), v int);");
    RunSql(*db, session, "create index words_k on words(k);");
    RunSql(*db, session, "insert into words values('ab', 7);");
    db->Checkpoint();
    const std::string sidecar = temp.path() + "/deltaidx.1.1";
    const auto header = ReadSidecarHeader(sidecar);
    const uint64_t keys_end = 96 + ReadLeAt<uint64_t>(header, 48) * 16 + ReadLeAt<uint64_t>(header, 64);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 64), 5U);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 72), 120U);
    EXPECT_GT(ReadLeAt<uint64_t>(header, 72), keys_end);
    EXPECT_EQ(Query(*db, session, "select v from words where k = 'ab';").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));
    {
        const uint8_t nonzero = 0xa5;
        std::fstream output(sidecar, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(output);
        output.seekp(static_cast<std::streamoff>(keys_end));
        output.write(reinterpret_cast<const char*>(&nonzero), sizeof(nonzero));
        ASSERT_TRUE(output);
    }
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    {
        std::array<uint8_t, 3> padding{};
        std::ifstream input(sidecar, std::ios::binary);
        input.seekg(static_cast<std::streamoff>(keys_end));
        input.read(reinterpret_cast<char*>(padding.data()), padding.size());
        ASSERT_TRUE(input);
        EXPECT_EQ(padding[0], 0U);
        EXPECT_EQ(padding[1], 0U);
        EXPECT_EQ(padding[2], 0U);
    }
    EXPECT_EQ(Query(*db, session, "select v from words where k = 'ab';").rows,
              (std::vector<std::vector<std::string>>{{"7"}}));

    RunSql(*db, session, "create table bits(k int);");
    RunSql(*db, session, "create index bits_k on bits(k);");
    for (int n = 0; n < 65; ++n) {
        const std::string sql = "insert into bits values(" + std::to_string(n) + ");";
        RunSql(*db, session, sql.c_str());
    }
    db->Checkpoint();
    ASSERT_TRUE(db->SidecarLiveTailForTest(2));
    EXPECT_EQ(*db->SidecarLiveTailForTest(2), 1U);
}

TEST(DeltaDatabaseTest, V3SidecarRejectsInvalidSnapshotAndInvalidOrdinal) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(1, 9);");
    RunSql(*db, session, "insert into t values(2, 8);");
    db->Checkpoint();
    const std::string sidecar = temp.path() + "/deltaidx.1.1";

    auto header = ReadSidecarHeader(sidecar);
    WriteLeAt<uint64_t>(header, 40, 0);
    WriteSidecarHeader(sidecar, header);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(ReadLeAt<uint64_t>(ReadSidecarHeader(sidecar), 40), 2U);

    header = ReadSidecarHeader(sidecar);
    WriteLeAt<uint64_t>(header, 40, std::numeric_limits<uint64_t>::max());
    WriteSidecarHeader(sidecar, header);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(ReadLeAt<uint64_t>(ReadSidecarHeader(sidecar), 40), 2U);

    header = ReadSidecarHeader(sidecar);
    WriteLeAt<uint64_t>(header, 48, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1);
    WriteSidecarHeader(sidecar, header);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(ReadLeAt<uint64_t>(ReadSidecarHeader(sidecar), 48), 2U);

    header = ReadSidecarHeader(sidecar);
    const uint64_t row_order_offset = ReadLeAt<uint64_t>(header, 72);
    {
        std::array<uint8_t, 4> duplicate{};
        duplicate[0] = 1;
        std::fstream output(sidecar, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(output);
        output.seekp(static_cast<std::streamoff>(row_order_offset));
        output.write(reinterpret_cast<const char*>(duplicate.data()), duplicate.size());
        ASSERT_TRUE(output);
    }
    std::array<uint8_t, 8> row_order{};
    {
        std::ifstream input(sidecar, std::ios::binary);
        input.seekg(static_cast<std::streamoff>(row_order_offset));
        input.read(reinterpret_cast<char*>(row_order.data()), row_order.size());
        ASSERT_TRUE(input);
    }
    WriteLeAt<uint32_t>(header, 88, Crc32(row_order.data(), row_order.size()));
    WriteSidecarHeader(sidecar, header);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());

    header = ReadSidecarHeader(sidecar);
    {
        std::array<uint8_t, 4> invalid{};
        invalid[0] = 2;
        std::fstream output(sidecar, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(output);
        output.seekp(static_cast<std::streamoff>(row_order_offset));
        output.write(reinterpret_cast<const char*>(invalid.data()), invalid.size());
        ASSERT_TRUE(output);
    }
    {
        std::ifstream input(sidecar, std::ios::binary);
        input.seekg(static_cast<std::streamoff>(row_order_offset));
        input.read(reinterpret_cast<char*>(row_order.data()), row_order.size());
        ASSERT_TRUE(input);
    }
    WriteLeAt<uint32_t>(header, 88, Crc32(row_order.data(), row_order.size()));
    WriteSidecarHeader(sidecar, header);
    db.reset();
    db = deltakernel::DeltaDatabase::Open(temp.path());
    EXPECT_EQ(Query(*db, session, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"8"}}));
}

TEST(DeltaDatabaseTest, V3SidecarUsesFourByteOrdinalForSparseLocalIds) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(1, 1);");
    RunSql(*db, session, "insert into t values(2, 2);");
    RunSql(*db, session, "insert into t values(3, 3);");
    RunSql(*db, session, "delete from t where k = 2;");
    db->Checkpoint();
    const auto header = ReadSidecarHeader(temp.path() + "/deltaidx.1.1");
    const uint64_t count = ReadLeAt<uint64_t>(header, 48);
    const uint64_t row_order_offset = ReadLeAt<uint64_t>(header, 72);
    EXPECT_EQ(count, 2U);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 56), row_order_offset + count * 4);
    EXPECT_EQ(ReadLeAt<uint64_t>(header, 56) - row_order_offset, 8U);
    EXPECT_EQ(Query(*db, session, "select v from t where k = 3;").rows, (std::vector<std::vector<std::string>>{{"3"}}));
    EXPECT_TRUE(Query(*db, session, "select v from t where k = 2;").rows.empty());
}

TEST(DeltaDatabaseTest, V3SidecarRejectsWrappedRowOrderOffset) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "create index t_k on t(k);");
    RunSql(*db, session, "insert into t values(1, 9);");
    RunSql(*db, session, "insert into t values(2, 8);");
    db->Checkpoint();
    const std::string sidecar = temp.path() + "/deltaidx.1.1";
    auto header = ReadSidecarHeader(sidecar);
    constexpr uint64_t count = 100;
    constexpr uint64_t row_order_offset = std::numeric_limits<uint64_t>::max() - 1;
    const uint64_t wrapped_total = row_order_offset + count * 4;
    ASSERT_LT(wrapped_total, row_order_offset);
    ASSERT_GE(wrapped_total, 96U);
    WriteLeAt<uint64_t>(header, 48, count);
    WriteLeAt<uint64_t>(header, 56, wrapped_total);
    WriteLeAt<uint64_t>(header, 64, 0);
    WriteLeAt<uint64_t>(header, 72, row_order_offset);
    WriteSidecarHeader(sidecar, header);
    ASSERT_NO_THROW(std::filesystem::resize_file(sidecar, wrapped_total));

    db.reset();
    ASSERT_NO_THROW(db = deltakernel::DeltaDatabase::Open(temp.path()));
    EXPECT_EQ(ReadLeAt<uint64_t>(ReadSidecarHeader(sidecar), 48), 2U);
    EXPECT_EQ(Query(*db, session, "select v from t where k = 2;").rows, (std::vector<std::vector<std::string>>{{"8"}}));
}

TEST(DeltaDatabaseTest, CreateIndexRebuildsExistingTable) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table t(k int, v int);");
    RunSql(*db, session, "insert into t values(4, 6);");
    RunSql(*db, session, "create index t_k on t(k);");
    EXPECT_EQ(Query(*db, session, "select v from t where k = 4;").rows, (std::vector<std::vector<std::string>>{{"6"}}));
}

TEST(DeltaDatabaseTest, PreparedDescriptionDryValidatesSchemaAndParameters) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table typed(id int, amount float, note char(8));");
    auto select = ast::parse_sql("select note, amount from typed where id = $1;");
    auto description = db->DescribePrepared(*select, {deltakernel::DeltaValueType::Int});
    EXPECT_TRUE(description.query);
    EXPECT_EQ(description.names, (std::vector<std::string>{"note", "amount"}));
    EXPECT_EQ(description.types, (std::vector<deltakernel::DeltaValueType>{deltakernel::DeltaValueType::Char,
                                                                           deltakernel::DeltaValueType::Float}));
    EXPECT_THROW(db->DescribePrepared(*select, {deltakernel::DeltaValueType::Char}), std::runtime_error);
    auto unknown = ast::parse_sql("select note from typed where missing = $1;");
    EXPECT_THROW(db->DescribePrepared(*unknown, {deltakernel::DeltaValueType::Int}), std::runtime_error);
    auto ordered = ast::parse_sql("select note from typed order by id;");
    EXPECT_NO_THROW(db->DescribePrepared(*ordered, {}));
    auto insert = ast::parse_sql("insert into typed values($1, $2, $3);");
    EXPECT_NO_THROW(db->DescribePrepared(*insert, {deltakernel::DeltaValueType::Int, deltakernel::DeltaValueType::Float,
                                                   deltakernel::DeltaValueType::Char}));
}

TEST(DeltaDatabaseTest, PreparedProgramExecutesTypedParametersWithoutTreeClone) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table typed(id int, amount float, note char(8));");
    RunSql(*db, session, "insert into typed values(7, 1.5, 'old');");

    auto program = db->CompilePrepared(ast::parse_sql("update typed set amount = amount + $1 where id = $2;"),
                                       {deltakernel::DeltaValueType::Float, deltakernel::DeltaValueType::Int});
    ASSERT_TRUE(program);
    EXPECT_FALSE(program->query());
    deltakernel::DeltaParameterFrame parameters(2);
    parameters[0].type = deltakernel::DeltaValueType::Float;
    parameters[0].present = true;
    parameters[0].float_bits = 0x3f000000U; // 0.5f
    parameters[1].type = deltakernel::DeltaValueType::Int;
    parameters[1].present = true;
    parameters[1].integer = 7;
    EXPECT_FALSE(db->ExecutePrepared(*program, parameters, session, nullptr));
    EXPECT_EQ(Query(*db, session, "select amount from typed where id = 7;").rows,
              (std::vector<std::vector<std::string>>{{"1073741824"}})); // 2.0f bits
}

TEST(DeltaDatabaseTest, PreparedProgramRejectsCatalogMutation) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession setup;
    RunSql(*db, setup, "create table typed(id int);");
    auto old_program =
        db->CompilePrepared(ast::parse_sql("select id from typed where id = $1;"), {deltakernel::DeltaValueType::Int});
    ASSERT_TRUE(old_program);

    deltakernel::DeltaSession ddl_session;
    std::thread ddl([&] { RunSql(*db, ddl_session, "create index typed_id on typed(id);"); });
    ddl.join();

    deltakernel::DeltaParameterFrame parameters(1);
    parameters[0].type = deltakernel::DeltaValueType::Int;
    parameters[0].present = true;
    parameters[0].integer = 1;
    EXPECT_THROW(db->ExecutePrepared(*old_program, parameters, setup, nullptr), std::runtime_error);

    auto new_program =
        db->CompilePrepared(ast::parse_sql("select id from typed where id = $1;"), {deltakernel::DeltaValueType::Int});
    ASSERT_TRUE(new_program);
    EXPECT_TRUE(db->ExecutePrepared(*new_program, parameters, setup, nullptr));
}

TEST(DeltaDatabaseTest, PreparedProgramRejectsRuntimeLimitAndOffset) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table typed(id int);");

    EXPECT_EQ(db->CompilePrepared(ast::parse_sql("select id from typed limit $1;"), {deltakernel::DeltaValueType::Int}),
              nullptr);
    EXPECT_EQ(db->CompilePrepared(ast::parse_sql("select id from typed limit 1 offset $1;"),
                                  {deltakernel::DeltaValueType::Int}),
              nullptr);
    EXPECT_NE(db->CompilePrepared(ast::parse_sql("select id from typed limit 1 offset 2;"), {}), nullptr);
}

TEST(DeltaDatabaseTest, PreparedDescriptionResolvesNewOrderMultiTableColumns) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(
        *db, session,
        "create table customer(c_discount float, c_last char(16), c_credit char(2), c_w_id int, c_d_id int, c_id int, "
        "shared int);");
    RunSql(*db, session, "create table warehouse(w_id int, w_tax float, shared int);");
    auto select = ast::parse_sql("select c_discount,c_last,c_credit,w_tax from customer,warehouse where w_id=$1 and "
                                 "c_w_id=w_id and c_d_id=$2 and c_id=$3;");
    const auto description =
        db->DescribePrepared(*select, {deltakernel::DeltaValueType::Int, deltakernel::DeltaValueType::Int,
                                       deltakernel::DeltaValueType::Int});
    EXPECT_EQ(description.names, (std::vector<std::string>{"c_discount", "c_last", "c_credit", "w_tax"}));
    EXPECT_EQ(description.types, (std::vector<deltakernel::DeltaValueType>{
                                     deltakernel::DeltaValueType::Float, deltakernel::DeltaValueType::Char,
                                     deltakernel::DeltaValueType::Char, deltakernel::DeltaValueType::Float}));
    EXPECT_THROW(db->DescribePrepared(*select, {deltakernel::DeltaValueType::Char, deltakernel::DeltaValueType::Int,
                                                deltakernel::DeltaValueType::Int}),
                 std::runtime_error);
    auto ambiguous = ast::parse_sql("select shared from customer, warehouse where c_w_id = w_id;");
    EXPECT_THROW(db->DescribePrepared(*ambiguous, {}), std::runtime_error);
    auto star = ast::parse_sql("select * from customer, warehouse where c_w_id = w_id;");
    EXPECT_THROW(db->DescribePrepared(*star, {}), std::runtime_error);
    auto invalid_count = ast::parse_sql("select count(distinct missing) from customer, warehouse where c_w_id = w_id;");
    EXPECT_THROW(db->DescribePrepared(*invalid_count, {}), std::runtime_error);
}

TEST(DeltaDatabaseTest, CharResultPreservesEmptyAndEmbeddedNulLengths) {
    TempDelta temp;
    auto db = deltakernel::DeltaDatabase::Create(temp.path());
    deltakernel::DeltaSession session;
    RunSql(*db, session, "create table chars(id int, value char(8));");
    std::vector<std::unique_ptr<ast::Value>> values;
    values.push_back(std::make_unique<ast::IntLit>(1));
    values.push_back(std::make_unique<ast::StringLit>(std::string("a\0b", 3)));
    db->Execute(std::make_unique<ast::InsertStmt>("chars", std::move(values)), session, nullptr);
    RunSql(*db, session, "insert into chars values(2, '');");
    auto result = Query(*db, session, "select value from chars;");
    ASSERT_EQ(result.rows.size(), 2U);
    EXPECT_EQ(result.rows[0][0], std::string("a\0b", 3));
    EXPECT_TRUE(result.rows[1][0].empty());
}
} // namespace
