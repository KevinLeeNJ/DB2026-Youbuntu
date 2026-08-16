#include "checkpoint_db.h"
#include "test_row.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <sys/stat.h>
#include <string>
#include <system_error>
#include <sys/types.h>
#include <sys/wait.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace epoch_si_poc {

class SnapshotCursorTestPeer {
public:
    struct Handle {
        EpochSiEngine::SnapshotPin pin;
        EpochSiEngine::SnapshotCursor cursor;
    };

    static std::unique_ptr<Handle> Open(EpochSiEngine& engine, TableId table_id, size_t max_rows, size_t max_bytes,
                                        size_t max_probes = 16) {
        auto pin = engine.PinSnapshot();
        auto cursor = engine.BeginSnapshotCursor(pin, table_id, max_rows, max_bytes, max_probes);
        return std::make_unique<Handle>(Handle{std::move(pin), std::move(cursor)});
    }

    static EpochSiEngine::SnapshotCursorBatch Next(Handle& handle) {
        return handle.cursor.FinishBatch(handle.cursor.ProbeState());
    }
    static size_t SourceTableCount(const Handle& handle) {
        return handle.pin.source_->immutable_tables->size();
    }

    static std::shared_ptr<const SourceGeneration> TxnSource(const EpochSiEngine::Txn& txn) {
        return txn.source_;
    }

    static std::shared_ptr<const SourceGeneration> CurrentSource(const EpochSiEngine& engine) {
        return engine.current_source_;
    }

    static void ArtifactUnpublishedCleanup(CheckpointDb& db, TableId table_id, bool publish) {
        CheckpointDb::SnapshotArtifact artifact;
        auto writer = db.BeginSnapshotTableBase(table_id, db.engine().published_epoch());
        writer.Append(RowImage{{7}, {}, false});
        db.FinishSnapshotTableBase(writer, 1, artifact);
        const auto path =
            db.directory_ + "/tablebase." + std::to_string(table_id) + "." + std::to_string(db.generation() + 1);
        auto prepared = db.AdoptForPublication(std::move(artifact));
        EXPECT_TRUE(std::filesystem::exists(path));
        if (publish)
            prepared.MarkPublished();
    }

    static void InstallReplacement(CheckpointDb& db, TableId table_id, size_t row_count = 1) {
        CheckpointDb::SnapshotArtifact artifact;
        auto writer = db.BeginSnapshotTableBase(table_id, db.engine().published_epoch());
        for (size_t i = 0; i < row_count; ++i)
            writer.Append(RowImage{{static_cast<uint8_t>(i)}, {}, false});
        db.FinishSnapshotTableBase(writer, row_count, artifact);
        auto prepared_artifacts = db.AdoptForPublication(std::move(artifact));
        auto replacement = prepared_artifacts.readers_;
        const uint64_t identity = db.engine().current_source_->identity;
        auto prepared = db.engine().PrepareSourcePublication(identity, std::move(replacement));
        db.engine().ValidatePreparedSourcePublication(prepared);
        db.engine().InstallPreparedSource(prepared);
        prepared_artifacts.MarkPublished();
    }

    static void ExpectSourceTokenRejections(CheckpointDb& db, CheckpointDb& foreign_db) {
        const uint64_t identity = db.engine().current_source_->identity;
        auto foreign = db.engine().PrepareSourcePublication(identity, {});
        EXPECT_THROW(foreign_db.engine().ValidatePreparedSourcePublication(foreign), std::logic_error);

        auto stale = db.engine().PrepareSourcePublication(identity, {});
        auto installed = db.engine().PrepareSourcePublication(identity, {});
        db.engine().ValidatePreparedSourcePublication(installed);
        db.engine().InstallPreparedSource(installed);
        EXPECT_THROW(db.engine().ValidatePreparedSourcePublication(stale), std::logic_error);
        EXPECT_THROW(db.engine().ValidatePreparedSourcePublication(installed), std::logic_error);
    }

    static void ArtifactDoesNotDeleteForeignInode(CheckpointDb& db, TableId table_id, bool symlink) {
        CheckpointDb::SnapshotArtifact artifact;
        auto writer = db.BeginSnapshotTableBase(table_id, db.engine().published_epoch());
        writer.Append(RowImage{{7}, {}, false});
        db.FinishSnapshotTableBase(writer, 1, artifact);
        auto prepared = db.AdoptForPublication(std::move(artifact));
        const auto path =
            db.directory_ + "/tablebase." + std::to_string(table_id) + "." + std::to_string(db.generation() + 1);
        const auto target = db.directory_ + "/foreign-target." + std::to_string(table_id);
        ASSERT_TRUE(std::filesystem::remove(path));
        if (symlink) {
            std::ofstream output(target);
            output << "target";
            std::error_code error;
            std::filesystem::create_symlink(target, path, error);
            ASSERT_FALSE(error);
        } else {
            std::ofstream output(path);
            output << "foreign";
        }
        prepared = {};
        EXPECT_TRUE(std::filesystem::exists(path));
        if (symlink) {
            EXPECT_TRUE(std::filesystem::exists(target));
        }
        std::filesystem::remove(path);
        std::filesystem::remove(target);
    }

    static void PublishSnapshot(CheckpointDb& db, TableId table_id, CheckpointCrashPoint point) {
        auto pin = db.engine().PinSnapshot();
        const auto boundary = db.RotateWalAtGate(CheckpointCrashPoint::kNone);
        CheckpointDb::SnapshotArtifact artifact;
        auto writer = db.BeginSnapshotTableBase(table_id, boundary.epoch);
        std::vector<std::pair<RowId, Row>> rows;
        db.engine().VisitPublished(table_id, [&](RowId id, const Row& row) { rows.emplace_back(id, row); });
        db.AppendSnapshotRows(writer, std::move(rows));
        db.FinishSnapshotTableBase(writer, pin.next_local_id(table_id), artifact);
        auto artifacts = db.AdoptForPublication(std::move(artifact));
        auto publication =
            db.PrepareSnapshotPublication(boundary, db.engine().current_source_->identity, std::move(artifacts));
        db.SetCrashPointForTest(point);
        db.PublishSnapshotPublication(std::move(publication));
    }

    static CheckpointDb::SnapshotCutBoundary Rotate(CheckpointDb& db) {
        return db.RotateWalAtGate(CheckpointCrashPoint::kNone);
    }
};

namespace {

constexpr TableId kAccounts = 1;
constexpr TableId kOrders = 2;

RowImage Bare(RowImage row) {
    row.claims.clear();
    return row;
}

BaseImage CheckpointBase() {
    return {{{kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 10))},
            {{kAccounts, 2}, Bare(test_row::Make(kAccounts, "b", 20))},
            {{kOrders, 1}, Bare(test_row::Make(kOrders, "o", 30))},
            {{kOrders, 2}, Bare(test_row::Make(kOrders, "p", 40))}};
}

uint32_t CheckpointCrc32(const uint8_t* data, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
    return ~crc;
}

void PutLe32(std::vector<uint8_t>& bytes, size_t offset, uint32_t value) {
    for (size_t i = 0; i < 4; ++i)
        bytes.at(offset + i) = static_cast<uint8_t>(value >> (8 * i));
}

template <typename T> void PutLe(std::vector<uint8_t>& bytes, size_t offset, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        bytes.at(offset + i) = static_cast<uint8_t>(value >> (8 * i));
}

uint64_t GetLe64(const std::vector<uint8_t>& bytes, size_t offset) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(value); ++i)
        value |= static_cast<uint64_t>(bytes.at(offset + i)) << (8 * i);
    return value;
}

void RecomputeTableCrcs(std::vector<uint8_t>& bytes, bool payload, bool index) {
    constexpr size_t kHeaderBytes = 80;
    constexpr size_t kFooterBytes = 24;
    const size_t footer = bytes.size() - kFooterBytes;
    const size_t index_offset = static_cast<size_t>(GetLe64(bytes, 52));
    if (payload)
        PutLe32(bytes, footer + 12, CheckpointCrc32(bytes.data() + kHeaderBytes, index_offset - kHeaderBytes));
    if (index)
        PutLe32(bytes, footer + 16, CheckpointCrc32(bytes.data() + index_offset, footer - index_offset));
    PutLe32(bytes, footer + 20, CheckpointCrc32(bytes.data() + footer, 20));
}

std::vector<uint8_t> ReadFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

class TempDbDirectory {
public:
    TempDbDirectory() {
        char pattern[] = "/tmp/rmdb-checkpoint-XXXXXX";
        const char* created = mkdtemp(pattern);
        if (created == nullptr)
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        path_ = created;
    }

    ~TempDbDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    const std::string& path() const {
        return path_;
    }

private:
    std::string path_;
};

void PopulateTwoFrames(CheckpointDb& db) {
    auto first = db.engine().Begin();
    db.engine().PutImage(first, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 11)));
    db.engine().InsertImage(first, kOrders, Bare(test_row::Make(kOrders, "q", 50)));
    ASSERT_EQ(db.engine().CommitBatch({&first})[0].status, CommitStatus::kCommitted);

    auto second = db.engine().Begin();
    db.engine().Erase(second, {kAccounts, 2});
    db.engine().PutImage(second, {kOrders, 1}, Bare(test_row::Make(kOrders, "o", 31)));
    ASSERT_EQ(db.engine().CommitBatch({&second})[0].status, CommitStatus::kCommitted);
    ASSERT_EQ(db.engine().published_epoch(), 2U);
    ASSERT_EQ(db.engine().wal_frame_count(), 2U);
}

void ExpectPopulatedState(CheckpointDb& db) {
    auto txn = db.engine().Begin();
    const auto accounts = db.engine().Scan(txn, kAccounts);
    const auto orders = db.engine().Scan(txn, kOrders);
    ASSERT_EQ(accounts.size(), 1U);
    EXPECT_EQ(test_row::Value(accounts[0].second), 11);
    ASSERT_EQ(orders.size(), 3U);
    EXPECT_EQ(test_row::Value(*db.engine().Read(txn, {kOrders, 1})), 31);
    db.engine().Abort(txn);
}

void WaitForCheckpointChild(pid_t child) {
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
}

RowImage Plain(uint64_t value) {
    RowImage row;
    row.bytes.resize(sizeof(value));
    for (size_t i = 0; i < sizeof(value); ++i)
        row.bytes[i] = static_cast<uint8_t>(value >> (8 * i));
    return row;
}

std::string TablePath(const TempDbDirectory& temp, TableId id, uint64_t generation) {
    return temp.path() + "/tablebase." + std::to_string(id) + "." + std::to_string(generation);
}

std::string SegmentPath(const TempDbDirectory& temp, uint64_t lineage, uint64_t id) {
    return temp.path() + "/db.log.s." + std::to_string(lineage) + "." + std::to_string(id);
}

size_t CountSegmentFiles(const TempDbDirectory& temp) {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(temp.path()))
        count += entry.path().filename().string().rfind("db.log.s.", 0) == 0 &&
                 !std::filesystem::is_symlink(entry.symlink_status());
    return count;
}

void ConvertFreshSegmentedManifestToLegacy(const TempDbDirectory& temp) {
    constexpr size_t kManifestHeaderBytes = 44;
    constexpr size_t kTableRefBytes = 40;
    constexpr size_t kSegmentHeaderBytes = 72;
    auto manifest = ReadFile(temp.path() + "/MANIFEST");
    const uint32_t count = static_cast<uint32_t>(manifest.at(36)) | (static_cast<uint32_t>(manifest.at(37)) << 8) |
                           (static_cast<uint32_t>(manifest.at(38)) << 16) |
                           (static_cast<uint32_t>(manifest.at(39)) << 24);
    std::vector<uint8_t> legacy(kManifestHeaderBytes + static_cast<size_t>(count) * kTableRefBytes + 4);
    std::copy(manifest.begin(), manifest.begin() + kManifestHeaderBytes, legacy.begin());
    std::copy(manifest.begin() + kManifestHeaderBytes,
              manifest.begin() + kManifestHeaderBytes + static_cast<size_t>(count) * kTableRefBytes,
              legacy.begin() + kManifestHeaderBytes);
    PutLe32(legacy, 8, static_cast<uint32_t>(legacy.size()));
    PutLe32(legacy, 40, 0);
    PutLe32(legacy, legacy.size() - 4, CheckpointCrc32(legacy.data(), legacy.size() - 4));
    auto segment = ReadFile(SegmentPath(temp, 0, 0));
    ASSERT_GE(segment.size(), kSegmentHeaderBytes);
    WriteFile(temp.path() + "/db.log.0", std::vector<uint8_t>(segment.begin() + kSegmentHeaderBytes, segment.end()));
    ASSERT_TRUE(std::filesystem::remove(SegmentPath(temp, 0, 0)));
    WriteFile(temp.path() + "/MANIFEST", legacy);
}

TEST(CheckpointDbTest, OfflineCheckpointPreservesAuthorityAndEpoch) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    PopulateTwoFrames(db);
    ExpectPopulatedState(db);
    db.OfflineCheckpoint();

    EXPECT_EQ(db.generation(), 1U);
    EXPECT_EQ(db.base_epoch(), 2U);
    EXPECT_EQ(db.engine().published_epoch(), 2U);
    EXPECT_EQ(db.engine().durable_wal_bytes(), 0U);
    EXPECT_EQ(db.engine().wal_frame_count(), 0U);
    EXPECT_TRUE(std::filesystem::exists(temp.path() + "/tablebase.1.1"));
    EXPECT_TRUE(std::filesystem::exists(temp.path() + "/tablebase.2.1"));
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 1, 0)));
    ExpectPopulatedState(db);

    auto post_cut = db.engine().Begin();
    db.engine().PutImage(post_cut, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 99)));
    const auto result = db.engine().CommitBatch({&post_cut})[0];
    EXPECT_EQ(result.epoch, 3U);
    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_EQ(reopened.generation(), 1U);
    EXPECT_EQ(reopened.engine().published_epoch(), 3U);
    auto view = reopened.engine().Begin();
    EXPECT_EQ(test_row::Value(*reopened.engine().Read(view, {kAccounts, 1})), 99);
    reopened.engine().Abort(view);
}

TEST(CheckpointDbTest, DurableWalBytesIncludesSealedSegments) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    PopulateTwoFrames(db);
    const auto active_before = db.engine().durable_wal_bytes();
    EXPECT_EQ(db.durable_wal_bytes(), active_before);
    SnapshotCursorTestPeer::Rotate(db);
    EXPECT_EQ(db.durable_wal_bytes(), active_before + db.engine().durable_wal_bytes());

    auto txn = db.engine().Begin();
    db.engine().PutImage(txn, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 12)));
    ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    EXPECT_GT(db.durable_wal_bytes(), active_before);
    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_EQ(reopened.durable_wal_bytes(), db.durable_wal_bytes());
}

TEST(CheckpointDbTest, OfflineCheckpointClearsSealedWalDebt) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    PopulateTwoFrames(db);
    SnapshotCursorTestPeer::Rotate(db);
    ASSERT_GT(db.durable_wal_bytes(), db.engine().durable_wal_bytes());

    db.OfflineCheckpoint();
    EXPECT_EQ(db.durable_wal_bytes(), db.engine().durable_wal_bytes());
    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_EQ(reopened.durable_wal_bytes(), db.durable_wal_bytes());
}

TEST(CheckpointDbTest, SnapshotPinParticipatesInWatermarkAndBlocksSourceReplacement) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto handle = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 1, 1);
    EXPECT_EQ(db.engine().active_transaction_count(), 2U);
    EXPECT_EQ(db.engine().oldest_active_snapshot_for_test(), std::optional<Epoch>(0));
    EXPECT_THROW(db.BeginTableBase(1), std::logic_error);
    handle.reset();
    EXPECT_EQ(db.engine().active_transaction_count(), 0U);
    EXPECT_NO_THROW({
        auto writer = db.BeginTableBase(1);
        writer.Append(Plain(7));
        db.PublishTableBase(std::move(writer));
    });
}

TEST(CheckpointDbTest, SourceGenerationIsSharedAcrossReadersAndSwitchesAtomically) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    {
        auto writer = db.BeginTableBase(3);
        writer.Append(Plain(30));
        db.PublishTableBase(std::move(writer));
    }

    auto old = db.engine().Begin();
    const auto old_source = SnapshotCursorTestPeer::TxnSource(old);
    EXPECT_EQ(old_source, SnapshotCursorTestPeer::CurrentSource(db.engine()));
    const auto old_identity = old_source->identity;
    db.engine().Abort(old);

    {
        auto writer = db.BeginTableBase(4);
        writer.Append(Plain(40));
        db.PublishTableBase(std::move(writer));
    }

    auto current = db.engine().Begin();
    const auto current_source = SnapshotCursorTestPeer::TxnSource(current);
    EXPECT_EQ(current_source, SnapshotCursorTestPeer::CurrentSource(db.engine()));
    EXPECT_NE(current_source, old_source);
    EXPECT_GT(current_source->identity, old_identity);
    EXPECT_EQ(current_source->base, old_source->base);
    ASSERT_EQ(old_source->immutable_tables->count(3), 1U);
    EXPECT_EQ(old_source->immutable_tables->count(4), 0U);
    EXPECT_EQ(current_source->immutable_tables->count(3), 1U);
    EXPECT_EQ(current_source->immutable_tables->count(4), 1U);
    EXPECT_EQ(old_source->immutable_tables->at(3)->Read(0)->bytes, Plain(30).bytes);
    EXPECT_EQ(current_source->immutable_tables->at(4)->Read(0)->bytes, Plain(40).bytes);
    db.engine().Abort(current);
}

TEST(CheckpointDbTest, SnapshotArtifactAdoptionOwnsFilesUntilPublication) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    const std::string unpublished = temp.path() + "/tablebase.3.1";
    SnapshotCursorTestPeer::ArtifactUnpublishedCleanup(db, 3, false);
    EXPECT_FALSE(std::filesystem::exists(unpublished));
    const std::string published = temp.path() + "/tablebase.4.1";
    SnapshotCursorTestPeer::ArtifactUnpublishedCleanup(db, 4, true);
    EXPECT_TRUE(std::filesystem::exists(published));
}

TEST(CheckpointDbTest, PreparedSourcePublicationKeepsOldReadersAndSwapsWithoutActiveCheck) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(10)}});
    auto old = db.engine().Begin();
    const auto old_source = SnapshotCursorTestPeer::TxnSource(old);
    auto writer = db.engine().Begin();
    db.engine().PutImage(writer, {kAccounts, 1}, Plain(99));
    ASSERT_EQ(db.engine().CommitBatch({&writer})[0].status, CommitStatus::kCommitted);
    SnapshotCursorTestPeer::InstallReplacement(db, 3);
    auto current = db.engine().Begin();
    EXPECT_NE(SnapshotCursorTestPeer::TxnSource(current), old_source);
    EXPECT_EQ(db.engine().Read(old, {kAccounts, 1})->bytes, Plain(10).bytes);
    EXPECT_EQ(db.engine().Read(current, {kAccounts, 1})->bytes, Plain(99).bytes);
    db.engine().Abort(current);
    db.engine().Abort(old);
}

TEST(CheckpointDbTest, PreparedSourcePublicationRejectsForeignStaleAndDoubleTokens) {
    TempDbDirectory first;
    TempDbDirectory second;
    auto db = CheckpointDb::Create(first.path(), {});
    auto foreign = CheckpointDb::Create(second.path(), {});
    SnapshotCursorTestPeer::ExpectSourceTokenRejections(db, foreign);
}

TEST(CheckpointDbTest, PreparedSourcePublicationKeepsAllocatorFrontier) {
    TempDbDirectory higher;
    auto higher_db = CheckpointDb::Create(higher.path(), {{{kAccounts, 9}, Plain(9)}});
    SnapshotCursorTestPeer::InstallReplacement(higher_db, kAccounts);
    EXPECT_EQ(higher_db.engine().next_local_id(kAccounts), 10U);

    TempDbDirectory lower;
    auto lower_db = CheckpointDb::Create(lower.path(), {{{kAccounts, 1}, Plain(1)}});
    SnapshotCursorTestPeer::InstallReplacement(lower_db, kAccounts, 5);
    EXPECT_EQ(lower_db.engine().next_local_id(kAccounts), 5U);
}

TEST(CheckpointDbTest, PreparedSourcePublicationAllocatorIsUsedByInsert) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(1)}});
    SnapshotCursorTestPeer::InstallReplacement(db, kAccounts, 5);

    auto txn = db.engine().Begin();
    const RowId inserted = db.engine().InsertImage(txn, kAccounts, Plain(55));
    EXPECT_EQ(inserted.local_id, 5U);
    ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    auto view = db.engine().Begin();
    ASSERT_TRUE(db.engine().Read(view, inserted).has_value());
    EXPECT_EQ(db.engine().Read(view, inserted)->bytes, Plain(55).bytes);
    db.engine().Abort(view);
}

TEST(CheckpointDbTest, PreparedSourcePublicationPropagatesDiagnosticsToReplacement) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto diagnostics = std::make_shared<DeltaDiagnostics>();
    db.engine().SetDiagnostics(diagnostics);
    SnapshotCursorTestPeer::InstallReplacement(db, kAccounts);
    const uint64_t before = diagnostics->immutable_reads.load(std::memory_order_relaxed);
    auto txn = db.engine().Begin();
    ASSERT_TRUE(db.engine().Read(txn, {kAccounts, 0}).has_value());
    EXPECT_GT(diagnostics->immutable_reads.load(std::memory_order_relaxed), before);
    db.engine().Abort(txn);
}

TEST(CheckpointDbTest, SnapshotArtifactCleanupProtectsForeignInodesAndSymlinks) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    SnapshotCursorTestPeer::ArtifactDoesNotDeleteForeignInode(db, 5, false);
    SnapshotCursorTestPeer::ArtifactDoesNotDeleteForeignInode(db, 6, true);
}

TEST(CheckpointDbTest, SnapshotPublicationReclaimsOnlyExcludedWalAuthorities) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(1)}});
    auto update = db.engine().Begin();
    db.engine().PutImage(update, {kAccounts, 1}, Plain(2));
    ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    ASSERT_TRUE(std::filesystem::exists(SegmentPath(temp, 0, 0)));
    SnapshotCursorTestPeer::PublishSnapshot(db, kAccounts, CheckpointCrashPoint::kNone);
    EXPECT_FALSE(std::filesystem::exists(SegmentPath(temp, 0, 0)));
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 0, 1)));
    EXPECT_EQ(CountSegmentFiles(temp), 1U);

    auto post = db.engine().Begin();
    db.engine().PutImage(post, {kAccounts, 1}, Plain(3));
    ASSERT_EQ(db.engine().CommitBatch({&post})[0].status, CommitStatus::kCommitted);
    auto reopened = CheckpointDb::Open(temp.path());
    auto reader = reopened.engine().Begin();
    EXPECT_EQ(reopened.engine().Read(reader, {kAccounts, 1})->bytes, Plain(3).bytes);
    reopened.engine().Abort(reader);
}

TEST(CheckpointDbTest, SnapshotPublicationFailureRetainsManifestWalChain) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(1)}});
    auto update = db.engine().Begin();
    db.engine().PutImage(update, {kAccounts, 1}, Plain(2));
    ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    EXPECT_THROW(SnapshotCursorTestPeer::PublishSnapshot(db, kAccounts, CheckpointCrashPoint::kDuringManifestTemp),
                 SimulatedCrash);
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 0, 0)));
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 0, 1)));
    auto reopened = CheckpointDb::Open(temp.path());
    auto reader = reopened.engine().Begin();
    EXPECT_EQ(reopened.engine().Read(reader, {kAccounts, 1})->bytes, Plain(2).bytes);
    reopened.engine().Abort(reader);
}

TEST(CheckpointDbTest, OpenReclaimsExactWalOrphansWithoutFollowingSymlinks) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(1)}});
        auto update = db.engine().Begin();
        db.engine().PutImage(update, {kAccounts, 1}, Plain(2));
        ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
        SnapshotCursorTestPeer::PublishSnapshot(db, kAccounts, CheckpointCrashPoint::kNone);
    }
    const std::string segment_orphan = SegmentPath(temp, 9, 4);
    const std::string current_orphan = temp.path() + "/db.log.9";
    const std::string legacy_orphan = temp.path() + "/wal.9";
    const std::string malformed = temp.path() + "/db.log.s.9.bad";
    const std::string nonregular = SegmentPath(temp, 7, 2);
    const std::string target = temp.path() + "/outside";
    const std::string link = SegmentPath(temp, 8, 3);
    for (const auto& path : {segment_orphan, current_orphan, legacy_orphan, malformed, target}) {
        std::ofstream output(path);
        output << "residue";
    }
    ASSERT_EQ(symlink(target.c_str(), link.c_str()), 0);
    ASSERT_TRUE(std::filesystem::create_directory(nonregular));
    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_FALSE(std::filesystem::exists(segment_orphan));
    EXPECT_FALSE(std::filesystem::exists(current_orphan));
    EXPECT_FALSE(std::filesystem::exists(legacy_orphan));
    EXPECT_TRUE(std::filesystem::exists(malformed));
    EXPECT_TRUE(std::filesystem::is_directory(nonregular));
    EXPECT_TRUE(std::filesystem::is_symlink(link));
    EXPECT_TRUE(std::filesystem::exists(target));
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 0, 1)));
}

TEST(CheckpointDbTest, ConsecutiveSnapshotPublicationsKeepWalChainBounded) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(1)}});
    for (uint64_t value : {2U, 3U, 4U}) {
        auto update = db.engine().Begin();
        db.engine().PutImage(update, {kAccounts, 1}, Plain(value));
        ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
        SnapshotCursorTestPeer::PublishSnapshot(db, kAccounts, CheckpointCrashPoint::kNone);
        EXPECT_EQ(CountSegmentFiles(temp), 1U);
    }
    auto reopened = CheckpointDb::Open(temp.path());
    auto reader = reopened.engine().Begin();
    EXPECT_EQ(reopened.engine().Read(reader, {kAccounts, 1})->bytes, Plain(4).bytes);
    reopened.engine().Abort(reader);
}

TEST(CheckpointDbTest, SnapshotCursorKeepsCutBeforeDeleteUpdateAndInsert) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1}, Plain(1)}, {{kAccounts, 2}, Plain(2)}});
    auto handle = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 16, 1U << 20);
    auto next = db.engine().Begin();
    db.engine().PutImage(next, {kAccounts, 1}, Plain(11));
    db.engine().Erase(next, {kAccounts, 2});
    db.engine().InsertImage(next, kAccounts, Plain(3));
    ASSERT_EQ(db.engine().CommitBatch({&next})[0].status, CommitStatus::kCommitted);
    auto batch = SnapshotCursorTestPeer::Next(*handle);
    ASSERT_EQ(batch.rows.size(), 2U);
    EXPECT_EQ(batch.rows[0].first.local_id, 1U);
    EXPECT_EQ(batch.rows[1].first.local_id, 2U);
    EXPECT_EQ(batch.rows[0].second.bytes, Plain(1).bytes);
    EXPECT_EQ(batch.rows[1].second.bytes, Plain(2).bytes);
    EXPECT_FALSE(batch.has_more);
}

TEST(CheckpointDbTest, SnapshotCursorSparseStrictContinuationAndBudgets) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 1000000}, Plain(1)}, {{kAccounts, 2000000}, Plain(2)}});
    {
        auto row_bounded = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 1, 1U << 20, 4);
        auto first = SnapshotCursorTestPeer::Next(*row_bounded);
        ASSERT_EQ(first.rows.size(), 1U);
        EXPECT_EQ(first.rows[0].first.local_id, 1000000U);
        EXPECT_TRUE(first.has_more);
        auto second = SnapshotCursorTestPeer::Next(*row_bounded);
        ASSERT_EQ(second.rows.size(), 1U);
        EXPECT_EQ(second.rows[0].first.local_id, 2000000U);
        EXPECT_FALSE(second.has_more);
    }
    {
        auto byte_bounded = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 16, Plain(1).bytes.size(), 4);
        auto first = SnapshotCursorTestPeer::Next(*byte_bounded);
        ASSERT_EQ(first.rows.size(), 1U);
        EXPECT_TRUE(first.has_more);
        auto second = SnapshotCursorTestPeer::Next(*byte_bounded);
        ASSERT_EQ(second.rows.size(), 1U);
        EXPECT_FALSE(second.has_more);
    }
    {
        auto single_oversize = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 16, 1, 4);
        auto first = SnapshotCursorTestPeer::Next(*single_oversize);
        ASSERT_EQ(first.rows.size(), 1U);
        EXPECT_TRUE(first.rows[0].second.bytes.size() > 1U);
        EXPECT_TRUE(first.has_more);
    }
}

TEST(CheckpointDbTest, SnapshotCursorBoundsInvisiblePostCutProbesWithoutPinningRows) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto handle = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 8, 1024, 4);
    EXPECT_EQ(SnapshotCursorTestPeer::SourceTableCount(*handle), 0U);
    for (int value = 0; value < 20; ++value) {
        auto txn = db.engine().Begin();
        db.engine().InsertImage(txn, kAccounts, Plain(static_cast<uint64_t>(value)));
        ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    }
    size_t batches = 0;
    for (;;) {
        auto batch = SnapshotCursorTestPeer::Next(*handle);
        EXPECT_LE(batch.probes, 4U);
        EXPECT_TRUE(batch.rows.empty());
        ++batches;
        if (!batch.has_more)
            break;
        ASSERT_LT(batches, 20U);
    }
    EXPECT_GT(batches, 1U);
}

TEST(CheckpointDbTest, SnapshotPinRetainsAndReleaseAllowsVersionCollapse) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto insert = db.engine().Begin();
    const RowId id = db.engine().InsertImage(insert, kAccounts, Plain(1));
    ASSERT_EQ(db.engine().CommitBatch({&insert})[0].status, CommitStatus::kCommitted);
    auto handle = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 8, 1024);
    for (uint64_t value : {2U, 3U}) {
        auto update = db.engine().Begin();
        db.engine().PutImage(update, id, Plain(value));
        ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    }
    EXPECT_GE(db.engine().version_count(), 3U);
    handle.reset();
    auto update = db.engine().Begin();
    db.engine().PutImage(update, id, Plain(4));
    ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    EXPECT_LE(db.engine().version_count(), 2U);
}

TEST(CheckpointDbTest, SnapshotCursorOrdersTablesAndTombstoneOverridesImmutableBase) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(
        temp.path(),
        {{{kAccounts, 3}, Plain(3)}, {{kAccounts, 9}, Plain(9)}, {{kOrders, 2}, Plain(20)}, {{kOrders, 8}, Plain(80)}});
    db.OfflineCheckpoint();
    auto before_cut = db.engine().Begin();
    db.engine().Erase(before_cut, {kAccounts, 3});
    db.engine().PutImage(before_cut, {kAccounts, 9}, Plain(99));
    ASSERT_EQ(db.engine().CommitBatch({&before_cut})[0].status, CommitStatus::kCommitted);
    auto accounts = SnapshotCursorTestPeer::Open(db.engine(), kAccounts, 8, 1024);
    auto orders = SnapshotCursorTestPeer::Open(db.engine(), kOrders, 8, 1024);
    auto after_cut = db.engine().Begin();
    db.engine().PutImage(after_cut, {kAccounts, 9}, Plain(999));
    db.engine().InsertImage(after_cut, kAccounts, Plain(10));
    ASSERT_EQ(db.engine().CommitBatch({&after_cut})[0].status, CommitStatus::kCommitted);
    auto account_batch = SnapshotCursorTestPeer::Next(*accounts);
    ASSERT_EQ(account_batch.rows.size(), 1U);
    EXPECT_EQ(account_batch.rows[0].first.local_id, 9U);
    EXPECT_EQ(account_batch.rows[0].second.bytes, Plain(99).bytes);
    auto order_batch = SnapshotCursorTestPeer::Next(*orders);
    ASSERT_EQ(order_batch.rows.size(), 2U);
    EXPECT_EQ(order_batch.rows[0].first.local_id, 2U);
    EXPECT_EQ(order_batch.rows[1].first.local_id, 8U);
}

TEST(CheckpointDbTest, DiagnosticsContinueAcrossOfflineCheckpoint) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    auto diagnostics = std::make_shared<DeltaDiagnostics>();
    db.engine().SetDiagnostics(diagnostics);
    auto before = db.engine().Begin();
    db.engine().PutImage(before, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 71)));
    ASSERT_EQ(db.engine().CommitBatch({&before})[0].status, CommitStatus::kCommitted);
    const uint64_t writes_before = diagnostics->wal_pwrite_calls.load();
    ASSERT_GT(writes_before, 0U);

    db.OfflineCheckpoint();

    auto after = db.engine().Begin();
    db.engine().PutImage(after, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 72)));
    ASSERT_EQ(db.engine().CommitBatch({&after})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(diagnostics->commit_tickets.load(), 2U);
    EXPECT_EQ(diagnostics->commit_frames.load(), 2U);
    EXPECT_GT(diagnostics->wal_pwrite_calls.load(), writes_before);
    EXPECT_GT(diagnostics->wal_fdatasync_calls.load(), 1U);
}

TEST(CheckpointDbTest, SegmentedManifestHasExactBoundedSize) {
    TempDbDirectory empty;
    { auto db = CheckpointDb::Create(empty.path(), {}); }
    EXPECT_EQ(std::filesystem::file_size(empty.path() + "/MANIFEST"), 112U);

    TempDbDirectory populated;
    { auto db = CheckpointDb::Create(populated.path(), CheckpointBase()); }
    EXPECT_EQ(std::filesystem::file_size(populated.path() + "/MANIFEST"), 112U + 2U * 56U);
}

TEST(CheckpointDbTest, SegmentedManifestRejectsImpossibleCountsReservedBytesAndAuthorityMismatch) {
    for (const int corruption : {0, 1, 2, 3}) {
        TempDbDirectory temp;
        { auto db = CheckpointDb::Create(temp.path(), {}); }
        auto manifest = ReadFile(temp.path() + "/MANIFEST");
        ASSERT_EQ(manifest.size(), 112U);
        if (corruption == 0) {
            PutLe32(manifest, 36, std::numeric_limits<uint32_t>::max());
        } else if (corruption == 1) {
            PutLe<uint64_t>(manifest, 84, 4096);
            PutLe<uint64_t>(manifest, 92, 4097);
        } else if (corruption == 2) {
            PutLe<uint64_t>(manifest, 100, 1);
        } else {
            PutLe<uint64_t>(manifest, 20, 1);
        }
        PutLe32(manifest, manifest.size() - 4, CheckpointCrc32(manifest.data(), manifest.size() - 4));
        WriteFile(temp.path() + "/MANIFEST", manifest);
        EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error) << corruption;
    }
}

TEST(CheckpointDbTest, MigratesLegacyWalBeforeServingCommits) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
        auto txn = db.engine().Begin();
        db.engine().PutImage(txn, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 77)));
        ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    }
    ConvertFreshSegmentedManifestToLegacy(temp);
    ASSERT_EQ(rename((temp.path() + "/db.log.0").c_str(), (temp.path() + "/wal.0").c_str()), 0);

    auto legacy = CheckpointDb::Open(temp.path());
    EXPECT_EQ(legacy.wal_open_directory_syncs_for_test(), 1U);
    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/wal.0"));
    EXPECT_TRUE(std::filesystem::is_regular_file(temp.path() + "/db.log.0"));
    auto view = legacy.engine().Begin();
    EXPECT_EQ(test_row::Value(*legacy.engine().Read(view, {kAccounts, 1})), 77);
    ASSERT_EQ(legacy.engine().CommitBatch({&view})[0].status, CommitStatus::kCommitted);
    const size_t migrated_bytes = std::filesystem::file_size(temp.path() + "/db.log.0");
    EXPECT_GT(migrated_bytes, 0U);
    legacy.OfflineCheckpoint();

    EXPECT_FALSE(std::filesystem::exists(temp.path() + "/wal.0"));
    EXPECT_TRUE(std::filesystem::is_regular_file(SegmentPath(temp, 1, 0)));
    EXPECT_EQ(CheckpointDb::Open(temp.path()).engine().published_epoch(), 2U);
}

TEST(CheckpointDbTest, RejectsAmbiguousAndNonRegularWalAuthorities) {
    {
        TempDbDirectory temp;
        auto db = CheckpointDb::Create(temp.path(), {});
        ConvertFreshSegmentedManifestToLegacy(temp);
        std::filesystem::copy_file(temp.path() + "/db.log.0", temp.path() + "/wal.0");
        EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
    }
    for (const std::string name : {"db.log.0", "wal.0"}) {
        TempDbDirectory temp;
        TempDbDirectory external;
        { auto db = CheckpointDb::Create(temp.path(), {}); }
        ConvertFreshSegmentedManifestToLegacy(temp);
        ASSERT_TRUE(std::filesystem::remove(temp.path() + "/db.log.0"));
        const std::string target = external.path() + "/outside.log";
        WriteFile(target, {});
        ASSERT_EQ(symlink(target.c_str(), (temp.path() + "/" + name).c_str()), 0);
        EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error) << name;
        EXPECT_TRUE(std::filesystem::is_symlink(temp.path() + "/" + name));
    }
}

TEST(CheckpointDbTest, ReopenCompletesAlreadyRenamedLegacyMigration) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
        auto txn = db.engine().Begin();
        db.engine().PutImage(txn, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 88)));
        ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    }
    ConvertFreshSegmentedManifestToLegacy(temp);
    ASSERT_EQ(rename((temp.path() + "/db.log.0").c_str(), (temp.path() + "/wal.0").c_str()), 0);
    // Models a crash after renameat but before the migration directory fsync.
    ASSERT_EQ(rename((temp.path() + "/wal.0").c_str(), (temp.path() + "/db.log.0").c_str()), 0);

    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_EQ(reopened.wal_open_directory_syncs_for_test(), 1U);
    auto view = reopened.engine().Begin();
    EXPECT_EQ(test_row::Value(*reopened.engine().Read(view, {kAccounts, 1})), 88);
    ASSERT_EQ(reopened.engine().CommitBatch({&view})[0].status, CommitStatus::kCommitted);
}

TEST(CheckpointDbTest, LegacyLoadThenCheckpointCrashKeepsMigratedWalAuthority) {
    TempDbDirectory temp;
    { auto db = CheckpointDb::Create(temp.path(), {}); }
    ConvertFreshSegmentedManifestToLegacy(temp);
    {
        auto db = CheckpointDb::Open(temp.path());
        auto writer = db.BeginTableBase(kAccounts);
        writer.Append(Plain(7));
        db.PublishTableBase(std::move(writer));
        ASSERT_TRUE(std::filesystem::is_regular_file(SegmentPath(temp, 1, 0)));
        db.SetCrashPointForTest(CheckpointCrashPoint::kAfterWalCreate);
        EXPECT_THROW(db.OfflineCheckpoint(), SimulatedCrash);
        EXPECT_TRUE(std::filesystem::is_regular_file(SegmentPath(temp, 1, 0)));
        EXPECT_TRUE(std::filesystem::is_regular_file(SegmentPath(temp, 2, 0)));
    }
    {
        auto reopened = CheckpointDb::Open(temp.path());
        auto view = reopened.engine().Begin();
        ASSERT_EQ(reopened.engine().Read(view, {kAccounts, 0})->bytes, Plain(7).bytes);
        reopened.engine().Abort(view);
        reopened.OfflineCheckpoint();
    }
    EXPECT_EQ(CheckpointDb::Open(temp.path()).generation(), 2U);
}

TEST(CheckpointDbTest, LegacyPostBaseInsertSurvivesSegmentedManifestMigration) {
    TempDbDirectory temp;
    { auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 0}, Plain(1)}}); }
    ConvertFreshSegmentedManifestToLegacy(temp);
    {
        auto db = CheckpointDb::Open(temp.path());
        auto insert = db.engine().Begin();
        const RowId id = db.engine().InsertImage(insert, kAccounts, Plain(2));
        ASSERT_EQ(id.local_id, 1U);
        ASSERT_EQ(db.engine().CommitBatch({&insert})[0].status, CommitStatus::kCommitted);
        auto writer = db.BeginTableBase(kOrders);
        writer.Append(Plain(3));
        db.PublishTableBase(std::move(writer));
    }
    auto reopened = CheckpointDb::Open(temp.path());
    auto view = reopened.engine().Begin();
    EXPECT_EQ(reopened.engine().Read(view, {kAccounts, 0})->bytes, Plain(1).bytes);
    EXPECT_EQ(reopened.engine().Read(view, {kAccounts, 1})->bytes, Plain(2).bytes);
    EXPECT_EQ(reopened.engine().Read(view, {kOrders, 0})->bytes, Plain(3).bytes);
    reopened.engine().Abort(view);
}

TEST(CheckpointDbTest, LegacyTablePublishPreservesWalDebtAcrossMigration) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 0}, Plain(1)}});
        auto txn = db.engine().Begin();
        db.engine().PutImage(txn, {kAccounts, 0}, Plain(2));
        ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    }
    ConvertFreshSegmentedManifestToLegacy(temp);

    auto db = CheckpointDb::Open(temp.path());
    const size_t before = db.durable_wal_bytes();
    ASSERT_GT(before, 0U);
    auto writer = db.BeginTableBase(kOrders);
    writer.Append(Plain(3));
    db.PublishTableBase(std::move(writer));
    EXPECT_EQ(db.durable_wal_bytes(), before);

    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_EQ(reopened.durable_wal_bytes(), before);
}

TEST(CheckpointDbTest, SegmentHeaderRejectsIdentityBoundaryReservedAndCrcCorruption) {
    for (const size_t offset : {size_t{0}, size_t{8}, size_t{12}, size_t{16}, size_t{24}, size_t{32}, size_t{40},
                                size_t{48}, size_t{56}, size_t{60}, size_t{68}}) {
        TempDbDirectory temp;
        { auto db = CheckpointDb::Create(temp.path(), {}); }
        const std::string path = SegmentPath(temp, 0, 0);
        auto header = ReadFile(path);
        ASSERT_EQ(header.size(), 72U);
        header.at(offset) ^= 1;
        if (offset != 68)
            PutLe32(header, 68, CheckpointCrc32(header.data(), 68));
        WriteFile(path, header);
        EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error) << offset;
    }
}

TEST(CheckpointDbTest, CheckpointReplacesLegacyFutureWalOrphan) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    WriteFile(temp.path() + "/wal.1", {});

    EXPECT_NO_THROW(db.OfflineCheckpoint());
    EXPECT_TRUE(std::filesystem::is_regular_file(SegmentPath(temp, 1, 0)));
    EXPECT_TRUE(std::filesystem::exists(temp.path() + "/wal.1"));
    EXPECT_EQ(CheckpointDb::Open(temp.path()).generation(), 1U);
}

TEST(CheckpointDbTest, CheckpointRejectsNonRegularFutureWalOrphans) {
    for (const std::string& suffix : {std::string("db.log.s.1.0")}) {
        TempDbDirectory temp;
        TempDbDirectory external;
        auto db = CheckpointDb::Create(temp.path(), {});
        const std::string target = external.path() + "/outside.log";
        WriteFile(target, {});
        const std::string name = temp.path() + "/" + suffix;
        ASSERT_EQ(symlink(target.c_str(), name.c_str()), 0);

        EXPECT_THROW(db.OfflineCheckpoint(), std::runtime_error) << suffix;
        EXPECT_TRUE(std::filesystem::is_symlink(name));
        EXPECT_EQ(CheckpointDb::Open(temp.path()).generation(), 0U);
    }
    for (const std::string& suffix : {std::string("db.log.s.1.0")}) {
        TempDbDirectory temp;
        auto db = CheckpointDb::Create(temp.path(), {});
        const std::string name = temp.path() + "/" + suffix;
        ASSERT_TRUE(std::filesystem::create_directory(name));

        EXPECT_THROW(db.OfflineCheckpoint(), std::runtime_error) << suffix;
        EXPECT_TRUE(std::filesystem::is_directory(name));
        EXPECT_EQ(CheckpointDb::Open(temp.path()).generation(), 0U);
    }
}

TEST(CheckpointDbTest, RetryAfterPreManifestWalCrashCleansBothFutureNames) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    db.SetCrashPointForTest(CheckpointCrashPoint::kAfterWalCreate);
    EXPECT_THROW(db.OfflineCheckpoint(), SimulatedCrash);
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 1, 0)));
    WriteFile(temp.path() + "/wal.1", {});

    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_NO_THROW(reopened.OfflineCheckpoint());
    EXPECT_TRUE(std::filesystem::is_regular_file(SegmentPath(temp, 1, 0)));
    EXPECT_EQ(CheckpointDb::Open(temp.path()).generation(), 1U);
}

TEST(CheckpointDbTest, OpaqueRowsSurviveCheckpoint) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    RowImage row;
    row.bytes = {0, 3, 0xff, 4};
    auto txn = db.engine().Begin();
    const RowId id = db.engine().InsertImage(txn, kAccounts, row);
    ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    db.OfflineCheckpoint();

    auto reopened = CheckpointDb::Open(temp.path());
    auto view = reopened.engine().Begin();
    const auto recovered = reopened.engine().Read(view, id);
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ(recovered->bytes, row.bytes);
    EXPECT_TRUE(recovered->claims.empty());
    reopened.engine().Abort(view);
}

TEST(CheckpointDbTest, ActiveTransactionRejectsCheckpoint) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    auto active = db.engine().Begin();
    EXPECT_EQ(db.engine().active_transaction_count(), 1U);
    EXPECT_THROW(db.OfflineCheckpoint(), std::logic_error);
    db.engine().Abort(active);
    EXPECT_EQ(db.engine().active_transaction_count(), 0U);
    EXPECT_NO_THROW(db.OfflineCheckpoint());
}

TEST(CheckpointDbTest, ManifestSwitchNeverMixesGenerationsAtCrashPoints) {
    for (CheckpointCrashPoint point :
         {CheckpointCrashPoint::kDuringBaseTemp, CheckpointCrashPoint::kAfterBaseRename,
          CheckpointCrashPoint::kAfterWalCreate, CheckpointCrashPoint::kBeforeNextEngineOpen,
          CheckpointCrashPoint::kDuringManifestTemp, CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync,
          CheckpointCrashPoint::kAfterSuccess}) {
        TempDbDirectory temp;
        {
            auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
            PopulateTwoFrames(db);
        }
        const pid_t child = fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            try {
                auto db = CheckpointDb::Open(temp.path());
                db.SetCrashPointForTest(point);
                db.OfflineCheckpoint();
            } catch (const SimulatedCrash&) {
                _exit(0);
            }
            _exit(point == CheckpointCrashPoint::kNone ? 0 : 1);
        }
        WaitForCheckpointChild(child);
        auto opened = CheckpointDb::Open(temp.path());
        const bool manifest_was_published = point == CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync ||
                                            point == CheckpointCrashPoint::kAfterSuccess;
        EXPECT_EQ(opened.generation(), manifest_was_published ? 1U : 0U) << static_cast<int>(point);
        EXPECT_EQ(opened.base_epoch(), manifest_was_published ? 2U : 0U) << static_cast<int>(point);
        EXPECT_EQ(opened.engine().published_epoch(), 2U);
        EXPECT_EQ(opened.engine().wal_frame_count(), manifest_was_published ? 0U : 2U);
        ExpectPopulatedState(opened);
    }
}

TEST(CheckpointDbTest, CorruptManifestFailsWithoutFilenameFallback) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
        PopulateTwoFrames(db);
        db.OfflineCheckpoint();
    }
    std::fstream manifest(temp.path() + "/MANIFEST", std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(manifest.is_open());
    manifest.seekp(4);
    const char bad = 0;
    manifest.write(&bad, 1);
    manifest.close();
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

TEST(CheckpointDbTest, CorruptAuthoritativeBaseFailsOpen) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
        PopulateTwoFrames(db);
        db.OfflineCheckpoint();
    }
    std::fstream base(temp.path() + "/tablebase.1.1", std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(base.is_open());
    base.seekp(60);
    const char bad = 0;
    base.write(&bad, 1);
    base.close();
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

TEST(CheckpointDbTest, MissingAuthoritativeWalFailsWithoutRecreation) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
        PopulateTwoFrames(db);
        db.OfflineCheckpoint();
        auto txn = db.engine().Begin();
        db.engine().PutImage(txn, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 88)));
        ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    }
    const std::string wal = SegmentPath(temp, 1, 0);
    ASSERT_TRUE(std::filesystem::remove(wal));
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::system_error);
    EXPECT_FALSE(std::filesystem::exists(wal));
}

TEST(CheckpointDbTest, BaseRowCannotConsumeFooterEvenWithValidCrc) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
        db.OfflineCheckpoint();
    }
    const std::string path = temp.path() + "/tablebase.1.0";
    std::ifstream input(path, std::ios::binary);
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    input.close();
    ASSERT_GT(bytes.size(), 80U);
    bytes[72] = 0xff;
    bytes[73] = 0xff;
    const size_t footer = bytes.size() - 20;
    PutLe32(bytes, footer + 12, CheckpointCrc32(bytes.data() + 60, footer - 60));
    PutLe32(bytes, footer + 16, CheckpointCrc32(bytes.data() + footer, 16));
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

TEST(CheckpointDbTest, PreOpenFailureKeepsOldEngineAndPostPublishFailurePoisonsObject) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), CheckpointBase());
    PopulateTwoFrames(db);
    db.SetCrashPointForTest(CheckpointCrashPoint::kBeforeNextEngineOpen);
    EXPECT_THROW(db.OfflineCheckpoint(), SimulatedCrash);
    auto txn = db.engine().Begin();
    db.engine().PutImage(txn, {kAccounts, 1}, Bare(test_row::Make(kAccounts, "a", 77)));
    EXPECT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(CheckpointDb::Open(temp.path()).generation(), 0U);

    EpochSiEngine* held_engine = &db.engine();
    db.SetCrashPointForTest(CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync);
    EXPECT_THROW(db.OfflineCheckpoint(), SimulatedCrash);
    EXPECT_THROW(db.engine(), std::logic_error);
    EXPECT_THROW(held_engine->Begin(), std::logic_error);
    auto reopened = CheckpointDb::Open(temp.path());
    EXPECT_EQ(reopened.generation(), 1U);
    EXPECT_EQ(reopened.engine().published_epoch(), 3U);
}

TEST(CheckpointDbTest, DirectTablePublishUsesSparseDiskBaseWithoutWalOrMetadata) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto writer = db.BeginTableBase(kAccounts);
    constexpr uint64_t kRows = 8193;
    for (uint64_t i = 0; i < kRows; ++i)
        writer.Append(Plain(i));
    db.PublishTableBase(std::move(writer));
    EXPECT_EQ(db.engine().durable_wal_bytes(), 0U);
    EXPECT_EQ(db.engine().wal_frame_count(), 0U);
    EXPECT_EQ(db.engine().wal_transaction_count(), 0U);
    EXPECT_EQ(db.engine().version_count(), 0U);
    EXPECT_EQ(db.engine().explicit_row_metadata_count(), 0U);
    EXPECT_EQ(db.engine().resident_base_row_count(), 0U);
    EXPECT_LE(db.immutable_index_bytes(), ((kRows + 31) / 32) * 2U * sizeof(uint64_t));
    auto snapshot = db.engine().Begin();
    ASSERT_TRUE(db.engine().Read(snapshot, {kAccounts, 8192}).has_value());
    db.engine().Abort(snapshot);
}

TEST(CheckpointDbTest, ImmutableTableOpenPreservesNextIdAfterHoles) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), {});
        auto writer = db.BeginTableBase(kAccounts);
        writer.Append(Plain(1));
        writer.Append(Plain(2));
        writer.Append(Plain(3));
        db.PublishTableBase(std::move(writer));
    }
    auto reopened = CheckpointDb::Open(temp.path());
    auto txn = reopened.engine().Begin();
    reopened.engine().Erase(txn, {kAccounts, 1});
    const RowId inserted = reopened.engine().InsertImage(txn, kAccounts, Plain(4));
    EXPECT_EQ(inserted.local_id, 3U);
    ASSERT_EQ(reopened.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
}

TEST(CheckpointDbTest, CheckpointPreservesAllocatorAfterDeletingHighestAndAllRows) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {{{kAccounts, 0}, Plain(1)}, {{kAccounts, 1}, Plain(2)}});
    auto txn = db.engine().Begin();
    db.engine().Erase(txn, {kAccounts, 0});
    db.engine().Erase(txn, {kAccounts, 1});
    ASSERT_EQ(db.engine().CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    db.OfflineCheckpoint();
    auto reopened = CheckpointDb::Open(temp.path());
    auto next = reopened.engine().Begin();
    const RowId id = reopened.engine().InsertImage(next, kAccounts, Plain(3));
    EXPECT_EQ(id.local_id, 2U);
    ASSERT_EQ(reopened.engine().CommitBatch({&next})[0].status, CommitStatus::kCommitted);
}

TEST(CheckpointDbTest, ImmutablePointReadsCrossSparseBlocksAndMisses) {
    TempDbDirectory temp;
    BaseImage initial;
    for (uint64_t i = 0; i < 97; ++i)
        initial.emplace(RowId{kAccounts, i * 2}, Plain(i * 2));
    auto db = CheckpointDb::Create(temp.path(), std::move(initial));
    db.engine().SetDiagnostics(std::make_shared<DeltaDiagnostics>());

    auto snapshot = db.engine().Begin();
    for (uint64_t id : {62, 64, 126, 128, 192}) {
        const auto row = db.engine().Read(snapshot, {kAccounts, id});
        ASSERT_TRUE(row.has_value());
        EXPECT_EQ(row->bytes, Plain(id).bytes);
    }
    const size_t probes_before_sparse_miss = db.engine().immutable_read_probes_for_test();
    EXPECT_FALSE(db.engine().Read(snapshot, {kAccounts, 63}).has_value());
    EXPECT_EQ(db.engine().immutable_read_probes_for_test(), probes_before_sparse_miss + 1);
    const size_t probes_before_frontier_miss = db.engine().immutable_read_probes_for_test();
    EXPECT_FALSE(db.engine().Read(snapshot, {kAccounts, 193}).has_value());
    EXPECT_EQ(db.engine().immutable_read_probes_for_test(), probes_before_frontier_miss);
    db.engine().Abort(snapshot);
}

TEST(CheckpointDbTest, RecoveryMembershipIsBoundedForDenseSparseAndEmptyTables) {
    TempDbDirectory temp;
    constexpr TableId kEmpty = 3;
    BaseImage initial{
        {{kAccounts, 0}, Plain(1)}, {{kAccounts, 2}, Plain(2)}, {{kOrders, 0}, Plain(3)}, {{kOrders, 128}, Plain(4)}};
    auto db = CheckpointDb::Create(temp.path(), std::move(initial));
    auto empty = db.BeginTableBase(kEmpty);
    db.PublishTableBase(std::move(empty));

    EXPECT_EQ(db.engine().immutable_recovery_membership_for_test({kAccounts, 0}), std::optional<bool>(true));
    EXPECT_EQ(db.engine().immutable_recovery_membership_for_test({kAccounts, 1}), std::optional<bool>(false));
    EXPECT_EQ(db.engine().immutable_recovery_membership_for_test({kAccounts, 2}), std::optional<bool>(true));
    EXPECT_FALSE(db.engine().immutable_recovery_membership_for_test({kOrders, 0}).has_value());
    EXPECT_FALSE(db.engine().immutable_recovery_membership_for_test({kOrders, 128}).has_value());
    EXPECT_EQ(db.engine().immutable_recovery_membership_for_test({kEmpty, 0}), std::optional<bool>(false));
}

TEST(CheckpointDbTest, ImmutableTableRejectsCrcValidDisorderedPayloadIds) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), {});
        auto writer = db.BeginTableBase(kAccounts);
        writer.Append(Plain(1));
        writer.Append(Plain(2));
        db.PublishTableBase(std::move(writer));
    }
    const std::string path = TablePath(temp, kAccounts, 1);
    auto bytes = ReadFile(path);
    PutLe<uint64_t>(bytes, 80 + 26, 0);
    RecomputeTableCrcs(bytes, true, false);
    WriteFile(path, bytes);
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

TEST(CheckpointDbTest, ImmutableTableRejectsCrcValidSparseIndexMismatch) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), {});
        auto writer = db.BeginTableBase(kAccounts);
        for (uint64_t i = 0; i <= 4096; ++i)
            writer.Append(Plain(i));
        db.PublishTableBase(std::move(writer));
    }
    const std::string path = TablePath(temp, kAccounts, 1);
    auto bytes = ReadFile(path);
    const size_t index_offset = static_cast<size_t>(GetLe64(bytes, 52));
    PutLe<uint64_t>(bytes, index_offset + 16, 4095);
    RecomputeTableCrcs(bytes, false, true);
    WriteFile(path, bytes);
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

TEST(CheckpointDbTest, ImmutableTableVisitHandlesHeaderAcrossBufferBoundary) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto writer = db.BeginTableBase(kAccounts);
    RowImage large;
    large.bytes.resize((1U << 20) - 5 - 18, 7);
    writer.Append(std::move(large));
    writer.Append(Plain(2));
    db.PublishTableBase(std::move(writer));

    auto txn = db.engine().Begin();
    const auto rows = db.engine().Scan(txn, kAccounts);
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows[0].second.bytes.size(), (1U << 20) - 5 - 18);
    EXPECT_EQ(rows[1].second.bytes, Plain(2).bytes);
    db.engine().Abort(txn);
}

TEST(CheckpointDbTest, CheckpointReusesCleanTableAndPreservesBaseSnapshot) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    for (TableId id : {kAccounts, kOrders}) {
        auto writer = db.BeginTableBase(id);
        writer.Append(Plain(id));
        db.PublishTableBase(std::move(writer));
    }
    const std::string clean_path = TablePath(temp, kOrders, 2);
    struct stat before {};
    ASSERT_EQ(stat(clean_path.c_str(), &before), 0);
    auto old = db.engine().Begin();
    auto update = db.engine().Begin();
    db.engine().PutImage(update, {kAccounts, 0}, Plain(99));
    ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    ASSERT_EQ(db.engine().Read(old, {kAccounts, 0})->bytes, Plain(kAccounts).bytes);
    auto current = db.engine().Begin();
    ASSERT_EQ(db.engine().Read(current, {kAccounts, 0})->bytes, Plain(99).bytes);
    db.engine().Abort(old);
    db.engine().Abort(current);
    db.OfflineCheckpoint();
    struct stat after {};
    ASSERT_EQ(stat(clean_path.c_str(), &after), 0);
    EXPECT_EQ(before.st_ino, after.st_ino);
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 0, 0)));
    EXPECT_TRUE(std::filesystem::exists(SegmentPath(temp, 1, 0)));
    EXPECT_EQ(db.engine().durable_wal_bytes(), 0U);
    EXPECT_FALSE(std::filesystem::exists(TablePath(temp, kAccounts, 1)));
}

TEST(CheckpointDbTest, DirectPublishCrashSwitchAndAuthoritativeTableFailures) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto writer = db.BeginTableBase(kAccounts);
    writer.Append(Plain(1));
    db.SetCrashPointForTest(CheckpointCrashPoint::kDuringManifestTemp);
    EXPECT_THROW(db.PublishTableBase(std::move(writer)), SimulatedCrash);
    EXPECT_TRUE(CheckpointDb::Open(temp.path()).engine().MaterializePublished().empty());

    auto reopened = CheckpointDb::Open(temp.path());
    auto publish = reopened.BeginTableBase(kAccounts);
    publish.Append(Plain(1));
    reopened.SetCrashPointForTest(CheckpointCrashPoint::kAfterManifestRenameBeforeDirSync);
    EXPECT_THROW(reopened.PublishTableBase(std::move(publish)), SimulatedCrash);
    EXPECT_THROW(reopened.engine(), std::logic_error);
    auto visible = CheckpointDb::Open(temp.path());
    auto view = visible.engine().Begin();
    EXPECT_EQ(visible.engine().Read(view, {kAccounts, 0})->bytes, Plain(1).bytes);
    visible.engine().Abort(view);

    const std::string path = TablePath(temp, kAccounts, 1);
    ASSERT_TRUE(std::filesystem::remove(path));
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::system_error);
}

TEST(CheckpointDbTest, TruncatedAuthoritativeTableFailsOpen) {
    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(temp.path(), {});
        auto writer = db.BeginTableBase(kAccounts);
        writer.Append(Plain(1));
        db.PublishTableBase(std::move(writer));
    }
    const std::string path = TablePath(temp, kAccounts, 1);
    const uintmax_t size = std::filesystem::file_size(path);
    ASSERT_GT(size, 1U);
    std::filesystem::resize_file(path, size - 1);
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

} // namespace
} // namespace epoch_si_poc
