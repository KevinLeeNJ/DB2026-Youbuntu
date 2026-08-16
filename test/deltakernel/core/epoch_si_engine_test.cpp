#include "epoch_si_engine.h"
#include "checkpoint_db.h"
#include "file_wal.h"
#include "test_row.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <mutex>
#include <random>
#include <set>
#include <system_error>
#include <thread>
#include <type_traits>
#include <fcntl.h>
#include <unistd.h>

namespace epoch_si_poc {
namespace {

constexpr TableId kAccounts = 1;
constexpr TableId kOrders = 2;
constexpr size_t kHeaderBytes = 48;
constexpr size_t kFooterBytes = 16;

BaseImage Base() {
    return {{{kAccounts, 1}, test_row::Make(kAccounts, "a", 10)},
            {{kAccounts, 2}, test_row::Make(kAccounts, "b", 20)},
            {{kOrders, 1}, test_row::Make(kOrders, "o", 30)},
            {{kOrders, 2}, test_row::Make(kOrders, "p", 40)}};
}

class TempDbDirectory {
public:
    TempDbDirectory() {
        char pattern[] = "/tmp/rmdb-epoch-si-XXXXXX";
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

RowImage ImmutableRow(std::string key, int64_t value) {
    RowImage row = test_row::Make(kAccounts, std::move(key), value);
    row.claims.clear();
    return row;
}

uint32_t Crc32(const uint8_t* data, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
        }
    }
    return ~crc;
}

template <typename T> T ReadLe(const std::vector<uint8_t>& bytes, size_t offset) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(bytes.at(offset + i)) << (8 * i);
    }
    return value;
}

template <typename T> void WriteLe(std::vector<uint8_t>& bytes, size_t offset, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        bytes.at(offset + i) = static_cast<uint8_t>(value >> (8 * i));
    }
}

uint32_t FrameBytes(const std::vector<uint8_t>& bytes, size_t offset = 0) {
    return ReadLe<uint32_t>(bytes, offset + 12);
}

void RecomputePayloadAndFooterCrc(std::vector<uint8_t>& frame, size_t frame_start = 0) {
    const size_t frame_bytes = FrameBytes(frame, frame_start);
    const size_t footer = frame_start + frame_bytes - kFooterBytes;
    WriteLe<uint32_t>(frame, footer + 8,
                      Crc32(frame.data() + frame_start + kHeaderBytes, frame_bytes - kHeaderBytes - kFooterBytes));
    WriteLe<uint32_t>(frame, footer + 12, Crc32(frame.data() + footer, 12));
}

void RecomputeHeaderCrc(std::vector<uint8_t>& frame, size_t frame_start = 0) {
    WriteLe<uint32_t>(frame, frame_start + 44, Crc32(frame.data() + frame_start, 44));
}

std::vector<uint8_t> ReadBytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void WriteBytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
}

std::shared_ptr<const ImmutableTable> OpenImmutableForTest(const std::string& path) {
    const auto bytes = ReadBytes(path);
    const TableId table_id = ReadLe<uint32_t>(bytes, 12);
    const uint64_t generation = ReadLe<uint64_t>(bytes, 20);
    const Epoch visible_from = ReadLe<uint64_t>(bytes, 28);
    const uint64_t row_count = ReadLe<uint64_t>(bytes, 36);
    const uint64_t payload_bytes = ReadLe<uint64_t>(bytes, 44);
    const uint64_t index_offset = ReadLe<uint64_t>(bytes, 52);
    const uint64_t index_count = ReadLe<uint64_t>(bytes, 60);
    const uint64_t file_bytes = ReadLe<uint64_t>(bytes, 68);
    std::vector<uint64_t> first_ids;
    std::vector<uint64_t> offsets;
    for (uint64_t i = 0; i < index_count; ++i) {
        first_ids.push_back(ReadLe<uint64_t>(bytes, static_cast<size_t>(index_offset + i * 16)));
        offsets.push_back(ReadLe<uint64_t>(bytes, static_cast<size_t>(index_offset + i * 16 + 8)));
    }
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        throw std::system_error(errno, std::generic_category(), "open immutable table for test");
    auto table = std::make_shared<ImmutableTable>(path, fd, table_id, generation, visible_from, row_count,
                                                  payload_bytes, file_bytes, std::move(first_ids), std::move(offsets));
    table->ValidateRowsForInstall();
    return table;
}

TEST(EpochSiEngineTest, SnapshotPointScanReadYourWritesAndAbort) {
    EpochSiEngine engine(Base());
    auto old = engine.Begin();
    auto writer = engine.Begin();
    engine.PutImage(writer, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    ASSERT_EQ(engine.CommitBatch({&writer})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(test_row::Value(*engine.Read(old, {kAccounts, 1})), 10);

    auto later = engine.Begin();
    engine.PutImage(later, {kAccounts, 1}, test_row::Make(kAccounts, "a", 12));
    ASSERT_EQ(engine.CommitBatch({&later})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(test_row::Value(*engine.Read(old, {kAccounts, 1})), 10);

    auto local = engine.Begin();
    engine.PutImage(local, {kAccounts, 1}, test_row::Make(kAccounts, "a", 99));
    engine.Erase(local, {kAccounts, 2});
    const RowId inserted = engine.InsertImage(local, kAccounts, test_row::Make(kAccounts, "c", 33));
    ASSERT_EQ(test_row::Key(*engine.Read(local, inserted)), "c");
    const auto rows = engine.Scan(local, kAccounts);
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(test_row::Value(rows[0].second), 99);
    EXPECT_EQ(rows[1].first, inserted);
    engine.Abort(local);

    auto current = engine.Begin();
    EXPECT_EQ(test_row::Value(*engine.Read(current, {kAccounts, 1})), 12);
    EXPECT_TRUE(engine.Read(current, {kAccounts, 2}).has_value());
    EXPECT_FALSE(engine.Read(current, inserted).has_value());
    engine.Abort(old);
    engine.Abort(current);
}

TEST(EpochSiEngineTest, ImmutableScanUsesSnapshotVersionsAndDecodedBaseRows) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto writer = db.BeginTableBase(kAccounts);
    writer.Append(ImmutableRow("a", 10));
    writer.Append(ImmutableRow("b", 20));
    writer.Append(ImmutableRow("c", 30));
    db.PublishTableBase(std::move(writer));

    auto old = db.engine().Begin();
    auto update = db.engine().Begin();
    db.engine().PutImage(update, {kAccounts, 0}, ImmutableRow("a", 11));
    ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    auto erase = db.engine().Begin();
    db.engine().Erase(erase, {kAccounts, 1});
    ASSERT_EQ(db.engine().CommitBatch({&erase})[0].status, CommitStatus::kCommitted);

    const auto old_rows = db.engine().Scan(old, kAccounts);
    ASSERT_EQ(old_rows.size(), 3U);
    EXPECT_EQ(test_row::Value(old_rows[0].second), 10);
    EXPECT_EQ(test_row::Value(old_rows[1].second), 20);
    EXPECT_EQ(test_row::Value(old_rows[2].second), 30);

    auto current = db.engine().Begin();
    const auto current_rows = db.engine().Scan(current, kAccounts);
    ASSERT_EQ(current_rows.size(), 2U);
    EXPECT_EQ(test_row::Value(current_rows[0].second), 11);
    EXPECT_EQ(test_row::Value(current_rows[1].second), 30);

    db.engine().PutImage(current, {kAccounts, 0}, ImmutableRow("a", 99));
    const auto local_rows = db.engine().Scan(current, kAccounts);
    ASSERT_EQ(local_rows.size(), 2U);
    EXPECT_EQ(test_row::Value(local_rows[0].second), 99);
    EXPECT_EQ(test_row::Value(local_rows[1].second), 30);
    db.engine().Abort(old);
    db.engine().Abort(current);
}

TEST(EpochSiEngineTest, ClaimFreeImmutableCommitsDoNotReprobeBase) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto writer = db.BeginTableBase(kAccounts);
    writer.Append(ImmutableRow("a", 10));
    writer.Append(ImmutableRow("b", 20));
    writer.Append(ImmutableRow("c", 30));
    db.PublishTableBase(std::move(writer));
    db.engine().SetDiagnostics(std::make_shared<DeltaDiagnostics>());

    auto update = db.engine().Begin();
    auto erase = db.engine().Begin();
    auto insert = db.engine().Begin();
    db.engine().PutImage(update, {kAccounts, 0}, ImmutableRow("a", 11));
    db.engine().Erase(erase, {kAccounts, 1});
    const RowId inserted = db.engine().InsertImage(insert, kAccounts, ImmutableRow("d", 40));
    const size_t execution_probes = db.engine().immutable_read_probes_for_test();
    EXPECT_EQ(execution_probes, 2U);

    const auto batch = db.engine().CommitBatch({&update, &erase, &insert});
    ASSERT_EQ(batch.size(), 3U);
    EXPECT_EQ(batch[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(batch[1].status, CommitStatus::kCommitted);
    EXPECT_EQ(batch[2].status, CommitStatus::kCommitted);
    EXPECT_EQ(db.engine().immutable_read_probes_for_test(), execution_probes);

    auto stale = db.engine().Begin();
    auto winner = db.engine().Begin();
    db.engine().PutImage(stale, {kAccounts, 2}, ImmutableRow("c", 31));
    db.engine().PutImage(winner, {kAccounts, 2}, ImmutableRow("c", 32));
    const size_t winner_execution_probes = db.engine().immutable_read_probes_for_test();
    ASSERT_EQ(db.engine().CommitBatch({&winner})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(db.engine().immutable_read_probes_for_test(), winner_execution_probes);
    EXPECT_EQ(db.engine().CommitBatch({&stale})[0].status, CommitStatus::kWriteConflict);
    EXPECT_EQ(db.engine().immutable_read_probes_for_test(), winner_execution_probes);

    auto reopened = CheckpointDb::Open(temp.path());
    auto view = reopened.engine().Begin();
    EXPECT_EQ(test_row::Value(*reopened.engine().Read(view, {kAccounts, 0})), 11);
    EXPECT_FALSE(reopened.engine().Read(view, {kAccounts, 1}));
    EXPECT_EQ(test_row::Value(*reopened.engine().Read(view, {kAccounts, 2})), 32);
    EXPECT_EQ(test_row::Value(*reopened.engine().Read(view, inserted)), 40);
    reopened.engine().Abort(view);
}

TEST(EpochSiEngineTest, ImmutableReadDiagnosticsCountOneReadAcrossManyTables) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto accounts = db.BeginTableBase(kAccounts);
    accounts.Append(ImmutableRow("a", 10));
    db.PublishTableBase(std::move(accounts));
    auto orders = db.BeginTableBase(kOrders);
    orders.Append(ImmutableRow("o", 20));
    db.PublishTableBase(std::move(orders));
    auto diagnostics = std::make_shared<DeltaDiagnostics>();
    db.engine().SetDiagnostics(diagnostics);

    auto txn = db.engine().Begin();
    ASSERT_TRUE(db.engine().Read(txn, {kAccounts, 0}).has_value());
    EXPECT_EQ(db.engine().immutable_read_probes_for_test(), 1U);
    db.engine().Abort(txn);
}

TEST(EpochSiEngineTest, ImmutableScanIsTableLocalAndNeverPointProbes) {
    TempDbDirectory temp;
    BaseImage accounts;
    accounts.emplace(RowId{kAccounts, 0}, ImmutableRow("a", 10));
    accounts.emplace(RowId{kAccounts, 2}, ImmutableRow("c", 30)); // Preserve an immutable-base hole.
    auto db = CheckpointDb::Create(temp.path(), std::move(accounts));

    auto old = db.engine().Begin();
    auto update = db.engine().Begin();
    auto erase = db.engine().Begin();
    auto insert = db.engine().Begin();
    db.engine().PutImage(update, {kAccounts, 0}, ImmutableRow("a", 11));
    db.engine().Erase(erase, {kAccounts, 2});
    const RowId committed_insert = db.engine().InsertImage(insert, kAccounts, ImmutableRow("d", 40));
    ASSERT_EQ(committed_insert, (RowId{kAccounts, 3}));
    const auto committed = db.engine().CommitBatch({&update, &erase, &insert});
    ASSERT_EQ(committed[0].status, CommitStatus::kCommitted);
    ASSERT_EQ(committed[1].status, CommitStatus::kCommitted);
    ASSERT_EQ(committed[2].status, CommitStatus::kCommitted);

    auto old_diagnostics = std::make_shared<DeltaDiagnostics>();
    db.engine().SetDiagnostics(old_diagnostics);
    const auto old_rows = db.engine().Scan(old, kAccounts);
    ASSERT_EQ(old_rows.size(), 2U);
    EXPECT_EQ(old_rows[0].first, (RowId{kAccounts, 0}));
    EXPECT_EQ(test_row::Value(old_rows[0].second), 10);
    EXPECT_EQ(old_rows[1].first, (RowId{kAccounts, 2}));
    EXPECT_EQ(test_row::Value(old_rows[1].second), 30);
    EXPECT_EQ(old_diagnostics->immutable_reads.load(), 0U);
    db.engine().Abort(old);

    auto orders = db.BeginTableBase(kOrders); // A later table generation has a different visible_from epoch.
    orders.Append(ImmutableRow("o", 1));
    db.PublishTableBase(std::move(orders));
    auto order_versions = db.engine().Begin();
    for (int64_t value = 0; value < 128; ++value)
        db.engine().InsertImage(order_versions, kOrders, ImmutableRow("ov" + std::to_string(value), value));
    ASSERT_EQ(db.engine().CommitBatch({&order_versions})[0].status, CommitStatus::kCommitted);

    auto private_view = db.engine().Begin();
    db.engine().PutImage(private_view, {kAccounts, 0}, ImmutableRow("a", 99));
    db.engine().Erase(private_view, committed_insert);
    const RowId private_insert = db.engine().InsertImage(private_view, kAccounts, ImmutableRow("e", 50));
    ASSERT_EQ(private_insert, (RowId{kAccounts, 4}));
    for (int64_t value = 0; value < 128; ++value)
        db.engine().InsertImage(private_view, kOrders, ImmutableRow("pv" + std::to_string(value), value));

    auto diagnostics = std::make_shared<DeltaDiagnostics>();
    db.engine().SetDiagnostics(diagnostics);
    const auto private_rows = db.engine().Scan(private_view, kAccounts);
    ASSERT_EQ(private_rows.size(), 2U);
    EXPECT_EQ(private_rows[0].first, (RowId{kAccounts, 0}));
    EXPECT_EQ(test_row::Value(private_rows[0].second), 99);
    EXPECT_EQ(private_rows[1].first, private_insert);
    EXPECT_EQ(test_row::Value(private_rows[1].second), 50);
    EXPECT_EQ(diagnostics->immutable_reads.load(), 0U);
    EXPECT_EQ(diagnostics->immutable_scan_calls.load(), 1U);
    EXPECT_EQ(diagnostics->immutable_scan_rows.load(), 2U);
    EXPECT_EQ(diagnostics->immutable_scan_decode_calls.load(), 2U);
    EXPECT_EQ(diagnostics->immutable_scan_pread_calls.load(), 1U);
    EXPECT_GT(diagnostics->immutable_scan_bytes.load(), 0U);
    EXPECT_EQ(diagnostics->scan_version_entries_examined.load(), 2U); // Private id 0 short-circuits its version.
    EXPECT_EQ(diagnostics->scan_private_entries_examined.load(), 3U);

    db.engine().SetDiagnostics(nullptr);
    std::vector<std::pair<RowId, Row>> point_rows;
    for (uint64_t local_id = 0; local_id <= private_insert.local_id; ++local_id)
        if (auto row = db.engine().Read(private_view, {kAccounts, local_id}))
            point_rows.emplace_back(RowId{kAccounts, local_id}, std::move(*row));
    EXPECT_EQ(private_rows, point_rows);
    const uint64_t calls_with_diagnostics = diagnostics->immutable_scan_calls.load();
    EXPECT_EQ(db.engine().Scan(private_view, kAccounts), point_rows);
    EXPECT_EQ(diagnostics->immutable_scan_calls.load(), calls_with_diagnostics);
    db.engine().Abort(private_view);

    auto reopened = CheckpointDb::Open(temp.path());
    auto recovered_diagnostics = std::make_shared<DeltaDiagnostics>();
    reopened.engine().SetDiagnostics(recovered_diagnostics);
    auto recovered_view = reopened.engine().Begin();
    const auto recovered_rows = reopened.engine().Scan(recovered_view, kAccounts);
    ASSERT_EQ(recovered_rows.size(), 2U);
    EXPECT_EQ(test_row::Value(recovered_rows[0].second), 11);
    EXPECT_EQ(recovered_rows[1].first, committed_insert);
    EXPECT_EQ(recovered_diagnostics->immutable_reads.load(), 0U);
    reopened.engine().Abort(recovered_view);

    reopened.engine().SetDiagnostics(nullptr);
    reopened.OfflineCheckpoint();
    auto checkpointed = CheckpointDb::Open(temp.path());
    auto checkpoint_diagnostics = std::make_shared<DeltaDiagnostics>();
    checkpointed.engine().SetDiagnostics(checkpoint_diagnostics);
    auto checkpoint_view = checkpointed.engine().Begin();
    EXPECT_EQ(checkpointed.engine().Scan(checkpoint_view, kAccounts), recovered_rows);
    EXPECT_EQ(checkpoint_diagnostics->immutable_reads.load(), 0U);
    checkpointed.engine().Abort(checkpoint_view);
}

TEST(EpochSiEngineTest, ImmutableScanCrossesSparseBlockBoundary) {
    TempDbDirectory temp;
    auto db = CheckpointDb::Create(temp.path(), {});
    auto writer = db.BeginTableBase(kAccounts);
    for (int64_t value = 0; value <= 4096; ++value)
        writer.Append(ImmutableRow("row" + std::to_string(value), value));
    db.PublishTableBase(std::move(writer));

    auto txn = db.engine().Begin();
    const auto rows = db.engine().Scan(txn, kAccounts);
    ASSERT_EQ(rows.size(), 4097U);
    EXPECT_EQ(test_row::Value(rows[0].second), 0);
    EXPECT_EQ(test_row::Value(rows[4095].second), 4095);
    EXPECT_EQ(test_row::Value(rows[4096].second), 4096);
    db.engine().Abort(txn);
}

TEST(EpochSiEngineTest, ConflictsAreAtomicAndWriteSkewIsAllowed) {
    EpochSiEngine engine(Base());
    auto multirow = engine.Begin();
    auto winner = engine.Begin();
    engine.PutImage(multirow, {kAccounts, 1}, test_row::Make(kAccounts, "a", 101));
    engine.PutImage(multirow, {kAccounts, 2}, test_row::Make(kAccounts, "b", 202));
    engine.PutImage(winner, {kAccounts, 2}, test_row::Make(kAccounts, "b", 303));
    ASSERT_EQ(engine.CommitBatch({&winner})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.CommitBatch({&multirow})[0].status, CommitStatus::kWriteConflict);
    auto view = engine.Begin();
    EXPECT_EQ(test_row::Value(*engine.Read(view, {kAccounts, 1})), 10);
    EXPECT_EQ(test_row::Value(*engine.Read(view, {kAccounts, 2})), 303);
    engine.Abort(view);

    auto left = engine.Begin();
    auto right = engine.Begin();
    engine.PutImage(left, {kAccounts, 1}, test_row::Make(kAccounts, "a", 0));
    engine.PutImage(right, {kAccounts, 2}, test_row::Make(kAccounts, "b", 0));
    const auto result = engine.CommitBatch({&left, &right});
    EXPECT_EQ(result[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(result[1].status, CommitStatus::kCommitted);
    EXPECT_EQ(result[0].epoch, result[1].epoch);
    EXPECT_NE(result[0].commit_seq, result[1].commit_seq);
}

TEST(EpochSiEngineTest, UniqueKeyHistoryCoversChangeDeleteAndReinsert) {
    EpochSiEngine engine(Base());
    auto first = engine.Begin();
    auto second = engine.Begin();
    engine.InsertImage(first, kAccounts, test_row::Make(kAccounts, "same", 1));
    engine.InsertImage(second, kAccounts, test_row::Make(kAccounts, "same", 2));
    const auto race = engine.CommitBatch({&first, &second});
    EXPECT_EQ(race[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(race[1].status, CommitStatus::kUniqueConflict);

    auto stale = engine.Begin();
    auto rename = engine.Begin();
    engine.PutImage(rename, {kAccounts, 1}, test_row::Make(kAccounts, "renamed", 10));
    ASSERT_EQ(engine.CommitBatch({&rename})[0].status, CommitStatus::kCommitted);
    engine.InsertImage(stale, kAccounts, test_row::Make(kAccounts, "a", 1));
    EXPECT_EQ(engine.CommitBatch({&stale})[0].status, CommitStatus::kUniqueConflict);

    auto delete_b = engine.Begin();
    engine.Erase(delete_b, {kAccounts, 2});
    ASSERT_EQ(engine.CommitBatch({&delete_b})[0].status, CommitStatus::kCommitted);
    auto after_delete = engine.Begin();
    engine.InsertImage(after_delete, kAccounts, test_row::Make(kAccounts, "b", 200));
    EXPECT_EQ(engine.CommitBatch({&after_delete})[0].status, CommitStatus::kCommitted);

    auto release = engine.Begin();
    auto claim = engine.Begin();
    engine.Erase(release, {kAccounts, 1});
    engine.InsertImage(claim, kAccounts, test_row::Make(kAccounts, "renamed", 300));
    const auto conservative = engine.CommitBatch({&release, &claim});
    EXPECT_EQ(conservative[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(conservative[1].status, CommitStatus::kUniqueConflict);

    auto swap = engine.Begin();
    RowId same_owner;
    RowId b_owner;
    for (const auto& [id, row] : engine.Scan(swap, kAccounts)) {
        if (test_row::Key(row) == "same")
            same_owner = id;
        if (test_row::Key(row) == "b")
            b_owner = id;
    }
    engine.PutImage(swap, same_owner, test_row::Make(same_owner.table_id, "b", 1));
    engine.PutImage(swap, b_owner, test_row::Make(b_owner.table_id, "same", 2));
    EXPECT_EQ(engine.CommitBatch({&swap})[0].status, CommitStatus::kCommitted);
}

TEST(EpochSiEngineTest, PublicationStagesOnlyTouchedState) {
    const auto commit_one_update = [](size_t rows) {
        BaseImage base;
        for (size_t i = 0; i < rows; ++i) {
            base.emplace(RowId{kAccounts, i}, ImmutableRow("row" + std::to_string(i), static_cast<int64_t>(i)));
        }
        EpochSiEngine engine(std::move(base));
        auto txn = engine.Begin();
        engine.PutImage(txn, {kAccounts, 0}, ImmutableRow("updated", 1));
        EXPECT_EQ(engine.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
        return engine.last_publication_staged_entries_for_test();
    };

    const size_t one_row_work = commit_one_update(1);
    EXPECT_GT(one_row_work, 0U);
    EXPECT_EQ(one_row_work, commit_one_update(4096));
}

TEST(EpochSiEngineTest, SparseRowStateDirectoryKeepsChunkOrderAndTombstones) {
    const RowId low{kAccounts, 7};
    const RowId high{kAccounts, (1ULL << 40) + 3};
    BaseImage base{{low, test_row::Make(kAccounts, "low", 1)}, {high, test_row::Make(kAccounts, "high", 2)}};
    EpochSiEngine engine(std::move(base));
    auto before = engine.Begin();
    auto update = engine.Begin();
    engine.Erase(update, low);
    engine.PutImage(update, high, test_row::Make(kAccounts, "high", 3));
    ASSERT_EQ(engine.CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.explicit_row_metadata_count(), 2U);
    EXPECT_TRUE(engine.Read(before, low).has_value());
    EXPECT_EQ(test_row::Value(*engine.Read(before, high)), 2);
    engine.Abort(before);
    auto latest = engine.Begin();
    const auto rows = engine.Scan(latest, kAccounts);
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0].first, high);
    EXPECT_EQ(test_row::Value(rows[0].second), 3);
    engine.Abort(latest);
}

TEST(EpochSiEngineTest, SnapshotRegistryTransfersMovesAndReusesSlotsExactlyOnce) {
    EpochSiEngine engine(Base());
    {
        auto first = engine.Begin();
        auto second = engine.Begin();
        ASSERT_EQ(engine.active_transaction_count(), 2U);
        ASSERT_EQ(engine.oldest_active_snapshot_for_test(), std::optional<Epoch>(0));
        auto moved = std::move(first);
        EXPECT_EQ(engine.active_transaction_count(), 2U);
        engine.Abort(moved);
        EXPECT_EQ(engine.active_transaction_count(), 1U);
        EXPECT_EQ(engine.oldest_active_snapshot_for_test(), std::optional<Epoch>(0));
    }
    EXPECT_EQ(engine.active_transaction_count(), 0U);
    EXPECT_EQ(engine.oldest_active_snapshot_for_test(), std::nullopt);

    auto reused = engine.Begin();
    EXPECT_EQ(engine.active_transaction_count(), 1U);
    EXPECT_EQ(engine.CommitBatch({&reused})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.active_transaction_count(), 0U);
    EXPECT_EQ(engine.oldest_active_snapshot_for_test(), std::nullopt);
}

TEST(EpochSiEngineTest, SyncedCommitKeepsSnapshotBegunBeforePublicationPinned) {
    EpochSiEngine engine(Base());
    auto first = engine.Begin();
    engine.PutImage(first, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    auto prepared = engine.PrepareCommitBatch({&first});
    engine.SyncPreparedCommit(prepared);

    auto during_sync_window = engine.Begin();
    ASSERT_EQ(engine.oldest_active_snapshot_for_test(), std::optional<Epoch>(0));
    engine.PublishPreparedCommit(prepared);
    EXPECT_EQ(test_row::Value(*engine.Read(during_sync_window, {kAccounts, 1})), 10);

    auto second = engine.Begin();
    engine.PutImage(second, {kAccounts, 1}, test_row::Make(kAccounts, "a", 12));
    ASSERT_EQ(engine.CommitBatch({&second})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.version_count(), 2U);
    EXPECT_EQ(test_row::Value(*engine.Read(during_sync_window, {kAccounts, 1})), 10);
    engine.Abort(during_sync_window);

    auto collapse = engine.Begin();
    engine.PutImage(collapse, {kAccounts, 1}, test_row::Make(kAccounts, "a", 13));
    ASSERT_EQ(engine.CommitBatch({&collapse})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.version_count(), 1U);
}

TEST(EpochSiEngineTest, VersionCollapseVisitsOnlyRowsTouchedByPublication) {
    EpochSiEngine engine(Base());
    auto old = engine.Begin();
    for (int64_t value = 1; value <= 32; ++value) {
        auto update = engine.Begin();
        engine.PutImage(update, {kAccounts, 1}, test_row::Make(kAccounts, "a", value));
        ASSERT_EQ(engine.CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    }
    ASSERT_EQ(engine.version_count(), 32U);
    engine.Abort(old);

    auto disjoint = engine.Begin();
    engine.PutImage(disjoint, {kAccounts, 2}, test_row::Make(kAccounts, "b", 21));
    ASSERT_EQ(engine.CommitBatch({&disjoint})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.version_count(), 33U);

    auto touch_hot = engine.Begin();
    engine.PutImage(touch_hot, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
    ASSERT_EQ(engine.CommitBatch({&touch_hot})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.version_count(), 2U);
}

TEST(EpochSiEngineTest, TombstoneAndRecoveredHeadRemainCollapsed) {
    EpochSiEngine engine(Base());
    for (int64_t value = 11; value <= 20; ++value) {
        auto update = engine.Begin();
        engine.PutImage(update, {kAccounts, 1}, test_row::Make(kAccounts, "a", value));
        ASSERT_EQ(engine.CommitBatch({&update})[0].status, CommitStatus::kCommitted);
    }
    auto erase = engine.Begin();
    engine.Erase(erase, {kAccounts, 1});
    ASSERT_EQ(engine.CommitBatch({&erase})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.version_count(), 1U);
    auto latest = engine.Begin();
    EXPECT_FALSE(engine.Read(latest, {kAccounts, 1}).has_value());
    engine.Abort(latest);

    auto recovered = EpochSiEngine::Recover(Base(), engine.recovery_wal_image_for_test());
    ASSERT_EQ(recovered.version_count(), 1U);
    auto old_recovered = recovered.Begin();
    auto live = recovered.Begin();
    recovered.PutImage(live, {kAccounts, 2}, test_row::Make(kAccounts, "b", 22));
    ASSERT_EQ(recovered.CommitBatch({&live})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(test_row::Value(*recovered.Read(old_recovered, {kAccounts, 2})), 20);
    EXPECT_EQ(recovered.version_count(), 2U);
    recovered.Abort(old_recovered);
}

TEST(EpochSiEngineTest, HotRowPublicationStagesAndInstallsOneVersion) {
    EpochSiEngine engine(Base());
    auto old_snapshot = engine.Begin();
    constexpr int64_t kUpdates = 4096;
    for (int64_t value = 1; value <= kUpdates; ++value) {
        auto update = engine.Begin();
        engine.PutImage(update, {kAccounts, 1}, test_row::Make(kAccounts, "a", value));
        ASSERT_EQ(engine.CommitBatch({&update})[0].status, CommitStatus::kCommitted);
        EXPECT_EQ(engine.last_publication_staged_versions_for_test(), 1U);
        EXPECT_EQ(engine.last_install_version_nodes_for_test(), 1U);
    }

    EXPECT_EQ(test_row::Value(*engine.Read(old_snapshot, {kAccounts, 1})), 10);
    engine.Abort(old_snapshot);
    EXPECT_EQ(engine.version_count(), static_cast<size_t>(kUpdates));
    auto collapse = engine.Begin();
    engine.PutImage(collapse, {kAccounts, 1}, test_row::Make(kAccounts, "a", kUpdates + 1));
    ASSERT_EQ(engine.CommitBatch({&collapse})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.version_count(), 1U);
    auto latest = engine.Begin();
    EXPECT_EQ(test_row::Value(*engine.Read(latest, {kAccounts, 1})), kUpdates + 1);
    engine.Abort(latest);
}

TEST(EpochSiEngineTest, PublicationPreservesClaimReleaseAndAcquireRules) {
    EpochSiEngine engine(Base());
    auto swap = engine.Begin();
    engine.PutImage(swap, {kAccounts, 1}, test_row::Make(kAccounts, "b", 11));
    engine.PutImage(swap, {kAccounts, 2}, test_row::Make(kAccounts, "a", 22));
    EXPECT_EQ(engine.CommitBatch({&swap})[0].status, CommitStatus::kCommitted);

    auto release = engine.Begin();
    auto acquire = engine.Begin();
    engine.Erase(release, {kAccounts, 1});
    engine.InsertImage(acquire, kAccounts, test_row::Make(kAccounts, "b", 33));
    const auto result = engine.CommitBatch({&release, &acquire});
    EXPECT_EQ(result[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(result[1].status, CommitStatus::kUniqueConflict);
}

TEST(EpochSiEngineTest, PreflightOwnershipEmptyWritesAndMutationValidation) {
    static_assert(!std::is_copy_constructible_v<EpochSiEngine::Txn>);
    EpochSiEngine engine(Base());
    EpochSiEngine other(Base());
    auto txn = engine.Begin();
    auto foreign = other.Begin();
    EXPECT_THROW(engine.CommitBatch({nullptr}), std::invalid_argument);
    EXPECT_THROW(engine.CommitBatch({&txn, &txn}), std::invalid_argument);
    EXPECT_THROW(engine.CommitBatch({&txn, &foreign}), std::invalid_argument);
    EXPECT_EQ(test_row::Value(*engine.Read(txn, {kAccounts, 1})), 10); // Preflight failures did not consume it.
    EXPECT_THROW(engine.PutImage(txn, {kAccounts, 99}, test_row::Make(kAccounts, "missing", 1)), std::invalid_argument);
    EXPECT_THROW(engine.Erase(txn, {kAccounts, 99}), std::invalid_argument);
    const size_t before = engine.durable_wal_bytes();
    const auto empty = engine.CommitBatch({&txn})[0];
    EXPECT_EQ(empty.status, CommitStatus::kCommitted);
    EXPECT_EQ(empty.epoch, 1U);
    EXPECT_EQ(empty.commit_seq, 1U);
    EXPECT_GT(engine.durable_wal_bytes(), before);
    EXPECT_EQ(engine.wal_frame_count(), 1U);
    EXPECT_EQ(engine.wal_transaction_count(), 1U);

    auto aborted_insert = engine.Begin();
    const RowId hole = engine.InsertImage(aborted_insert, kAccounts, test_row::Make(kAccounts, "hole", 1));
    engine.Abort(aborted_insert);
    auto next_insert = engine.Begin();
    const RowId next = engine.InsertImage(next_insert, kAccounts, test_row::Make(kAccounts, "next", 2));
    EXPECT_GT(next.local_id, hole.local_id);
    engine.Abort(next_insert);
    other.Abort(foreign);
}

TEST(EpochSiEngineTest, MoveLeavesOneValidOwner) {
    EpochSiEngine engine(Base());
    auto source = engine.Begin();
    engine.PutImage(source, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    auto target = std::move(source);
    EXPECT_THROW(engine.CommitBatch({&source, &target}), std::invalid_argument);
    EXPECT_THROW(engine.Read(source, {kAccounts, 1}), std::logic_error);
    EXPECT_EQ(engine.CommitBatch({&target})[0].status, CommitStatus::kCommitted);

    EpochSiEngine original(Base());
    auto txn = original.Begin();
    original.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 22));
    EpochSiEngine moved(std::move(original));
    EXPECT_THROW(original.Begin(), std::logic_error);
    EXPECT_THROW(original.CommitBatch({&txn}), std::logic_error);
    EXPECT_EQ(moved.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
}

TEST(EpochSiEngineTest, ConcurrentBeginMoveFinishAndInsertIdsStayExact) {
    EpochSiEngine engine(BaseImage{});
    constexpr size_t kThreads = 8;
    constexpr size_t kInsertsPerThread = 128;
    std::array<std::vector<RowId>, kThreads> ids;
    std::mutex mutex;
    std::condition_variable ready;
    size_t active = 0;
    bool finish = false;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (size_t thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&, thread] {
            auto first = engine.Begin();
            for (size_t i = 0; i < kInsertsPerThread; ++i) {
                ids[thread].push_back(engine.InsertImage(
                    first, kAccounts, ImmutableRow(std::to_string(thread) + ":" + std::to_string(i), i)));
            }
            auto second = std::move(first);
            auto final = std::move(second);
            {
                std::unique_lock<std::mutex> lock(mutex);
                ++active;
                ready.notify_all();
                ready.wait(lock, [&] { return finish; });
            }
            if (thread % 2 == 0)
                engine.Abort(final);
        });
    }
    bool all_active = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        all_active = ready.wait_for(lock, std::chrono::seconds(5), [&] { return active == kThreads; });
        if (all_active) {
            EXPECT_EQ(engine.active_transaction_count(), kThreads);
        }
        finish = true;
    }
    ready.notify_all();
    for (auto& worker : workers)
        worker.join();
    ASSERT_TRUE(all_active);

    std::set<RowId> unique;
    for (const auto& thread_ids : ids)
        unique.insert(thread_ids.begin(), thread_ids.end());
    EXPECT_EQ(unique.size(), kThreads * kInsertsPerThread);
    uint64_t expected = 0;
    for (const RowId id : unique) {
        EXPECT_EQ(id, (RowId{kAccounts, expected}));
        ++expected;
    }
    EXPECT_EQ(engine.active_transaction_count(), 0U);
    auto next = engine.Begin();
    EXPECT_EQ(engine.InsertImage(next, kAccounts, ImmutableRow("next", 1)),
              (RowId{kAccounts, kThreads * kInsertsPerThread}));
    engine.Abort(next);
}

TEST(EpochSiEngineTest, PreparedPublicationDoesNotRewindConcurrentRowIdAllocation) {
    EpochSiEngine engine(BaseImage{});
    auto first = engine.Begin();
    EXPECT_EQ(engine.InsertImage(first, kAccounts, ImmutableRow("first", 1)), (RowId{kAccounts, 0}));
    auto prepared = engine.PrepareCommitBatch({&first});

    auto concurrent = engine.Begin();
    EXPECT_EQ(engine.InsertImage(concurrent, kAccounts, ImmutableRow("concurrent", 2)), (RowId{kAccounts, 1}));
    engine.SyncPreparedCommit(prepared);
    engine.PublishPreparedCommit(prepared);

    auto next = engine.Begin();
    EXPECT_EQ(engine.InsertImage(next, kAccounts, ImmutableRow("next", 3)), (RowId{kAccounts, 2}));
    engine.Abort(concurrent);
    engine.Abort(next);
}

TEST(EpochSiEngineTest, ReadOnlyCommitStillStabilizesAndRecovers) {
    TempDbDirectory temp;
    const std::string wal = temp.path() + "/db.log";
    EpochSiEngine engine = EpochSiEngine::CreateFile(Base(), wal);
    const size_t bytes_before = engine.durable_wal_bytes();
    const size_t syncs_before = engine.wal_sync_calls_for_test();
    auto read_only = engine.Begin();
    const auto result = engine.CommitBatch({&read_only});
    ASSERT_EQ(result.size(), 1U);
    EXPECT_EQ(result[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(result[0].epoch, 1U);
    EXPECT_EQ(result[0].commit_seq, 1U);
    EXPECT_GT(engine.durable_wal_bytes(), bytes_before);
    EXPECT_EQ(engine.wal_sync_calls_for_test(), syncs_before + 1);

    auto recovered = EpochSiEngine::OpenFile(Base(), wal);
    EXPECT_EQ(recovered.published_epoch(), 1U);
    auto write = recovered.Begin();
    recovered.PutImage(write, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    const auto next = recovered.CommitBatch({&write});
    EXPECT_EQ(next[0].commit_seq, 2U);
}

TEST(EpochSiEngineTest, MixedReadOnlyAndWriteCommitSharesOneStabilizedFrame) {
    TempDbDirectory temp;
    const std::string wal = temp.path() + "/db.log";
    EpochSiEngine engine = EpochSiEngine::CreateFile(Base(), wal);
    const size_t syncs_before = engine.wal_sync_calls_for_test();
    auto first = engine.Begin();
    auto write = engine.Begin();
    auto last = engine.Begin();
    engine.PutImage(write, {kAccounts, 1}, test_row::Make(kAccounts, "a", 22));
    const auto result = engine.CommitBatch({&first, &write, &last});
    ASSERT_EQ(result.size(), 3U);
    EXPECT_EQ(result[0].epoch, result[1].epoch);
    EXPECT_EQ(result[1].epoch, result[2].epoch);
    EXPECT_EQ(result[0].commit_seq + 1, result[1].commit_seq);
    EXPECT_EQ(result[1].commit_seq + 1, result[2].commit_seq);
    EXPECT_EQ(engine.wal_sync_calls_for_test(), syncs_before + 1);

    auto recovered = EpochSiEngine::OpenFile(Base(), wal);
    EXPECT_EQ(recovered.published_epoch(), result[0].epoch);
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), 22);
    recovered.Abort(view);
}

TEST(EpochSiEngineTest, SyncedPreparedCommitCannotBeDropped) {
    EXPECT_DEATH(
        {
            EpochSiEngine engine(Base());
            auto txn = engine.Begin();
            engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
            auto prepared = engine.PrepareCommitBatch({&txn});
            engine.SyncPreparedCommit(prepared);
        },
        "");
}

TEST(EpochSiEngineTest, PreparedTransactionRejectsFurtherMutationAndPrepare) {
    EpochSiEngine engine(Base());
    auto txn = engine.Begin();
    engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
    auto prepared = engine.PrepareCommitBatch({&txn});
    EXPECT_THROW(engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 34)), std::logic_error);
    EXPECT_THROW(engine.PrepareCommitBatch({&txn}), std::invalid_argument);
    EXPECT_THROW(engine.Abort(txn), std::logic_error);
    engine.SyncPreparedCommit(prepared);
    engine.PublishPreparedCommit(prepared);
}

TEST(EpochSiEngineTest, PreparedTransactionCannotMoveOrOutliveToken) {
    EXPECT_DEATH(
        {
            EpochSiEngine engine(Base());
            auto txn = engine.Begin();
            engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
            auto prepared = engine.PrepareCommitBatch({&txn});
            auto moved = std::move(txn);
        },
        "");
    EXPECT_DEATH(
        {
            EpochSiEngine engine(Base());
            auto txn = std::make_unique<EpochSiEngine::Txn>(engine.Begin());
            engine.PutImage(*txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
            auto prepared = engine.PrepareCommitBatch({txn.get()});
            txn.reset();
        },
        "");
}

TEST(EpochSiEngineTest, AbandonedUnsyncedPreparedCommitUnfreezesTransaction) {
    EpochSiEngine engine(Base());
    auto txn = engine.Begin();
    engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
    { auto prepared = engine.PrepareCommitBatch({&txn}); }
    EXPECT_EQ(engine.active_transaction_count(), 1U);
    EXPECT_EQ(engine.oldest_active_snapshot_for_test(), std::optional<Epoch>(0));
    EXPECT_NO_THROW(engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 34)));
    EXPECT_EQ(engine.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(engine.active_transaction_count(), 0U);
    EXPECT_EQ(engine.oldest_active_snapshot_for_test(), std::nullopt);
}

TEST(EpochSiEngineTest, PoisonedSyncCannotRetryPreparedCommit) {
    TempDbDirectory temp;
    const std::string wal = temp.path() + "/db.log";
    EpochSiEngine engine = EpochSiEngine::CreateFile(Base(), wal);
    auto txn = engine.Begin();
    engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
    auto prepared = engine.PrepareCommitBatch({&txn});
    engine.CloseFileForTest();
    EXPECT_THROW(engine.SyncPreparedCommit(prepared), std::system_error);
    EXPECT_THROW(engine.SyncPreparedCommit(prepared), std::logic_error);
    EXPECT_THROW(engine.PublishPreparedCommit(prepared), std::logic_error);
}

TEST(EpochSiEngineTest, MixedPreparedBatchDoesNotRetainRejectedTransaction) {
    TempDbDirectory temp;
    const std::string wal = temp.path() + "/db.log";
    EpochSiEngine engine = EpochSiEngine::CreateFile(Base(), wal);
    auto stale = std::make_unique<EpochSiEngine::Txn>(engine.Begin());
    auto winner = engine.Begin();
    engine.PutImage(winner, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    ASSERT_EQ(engine.CommitBatch({&winner})[0].status, CommitStatus::kCommitted);

    auto accepted = engine.Begin();
    engine.PutImage(*stale, {kAccounts, 1}, test_row::Make(kAccounts, "a", 12));
    engine.PutImage(accepted, {kAccounts, 2}, test_row::Make(kAccounts, "b", 22));
    auto prepared = engine.PrepareCommitBatch({stale.get(), &accepted});
    ASSERT_EQ(prepared.results()[0].status, CommitStatus::kWriteConflict);
    ASSERT_EQ(prepared.results()[1].status, CommitStatus::kCommitted);
    stale.reset();
    engine.SyncPreparedCommit(prepared);
    engine.PublishPreparedCommit(prepared);

    auto recovered = EpochSiEngine::OpenFile(Base(), wal);
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), 11);
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 2})), 22);
    recovered.Abort(view);
}

TEST(EpochSiEngineTest, AllRejectedPreparedCommitDoesNotHoldOutstandingGuard) {
    EpochSiEngine engine(Base());
    auto stale = engine.Begin();
    auto winner = engine.Begin();
    engine.PutImage(winner, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    ASSERT_EQ(engine.CommitBatch({&winner})[0].status, CommitStatus::kCommitted);
    engine.PutImage(stale, {kAccounts, 1}, test_row::Make(kAccounts, "a", 12));
    const Epoch before_epoch = engine.published_epoch();
    auto rejected = engine.PrepareCommitBatch({&stale});
    ASSERT_EQ(rejected.results()[0].status, CommitStatus::kWriteConflict);
    EXPECT_FALSE(rejected.needs_sync());
    EXPECT_FALSE(rejected.needs_publish());
    auto next = engine.Begin();
    auto accepted = engine.PrepareCommitBatch({&next});
    EXPECT_EQ(engine.published_epoch(), before_epoch);
}

TEST(EpochSiEngineTest, EngineMoveWithPreparedCommitFailsStop) {
    EXPECT_DEATH(
        {
            EpochSiEngine engine(Base());
            auto txn = engine.Begin();
            engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
            auto prepared = engine.PrepareCommitBatch({&txn});
            EpochSiEngine moved(std::move(engine));
        },
        "");
}

TEST(EpochSiEngineTest, WrongEngineCannotPublishPreparedCommit) {
    EpochSiEngine first(Base());
    EpochSiEngine second(Base());
    auto txn = first.Begin();
    first.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 33));
    {
        auto prepared = first.PrepareCommitBatch({&txn});
        EXPECT_THROW(second.SyncPreparedCommit(prepared), std::invalid_argument);
        EXPECT_THROW(second.PublishPreparedCommit(prepared), std::invalid_argument);
    }
    first.Abort(txn);
}

TEST(EpochSiEngineTest, MovedSyncedPreparedCommitPublishesAndRecovers) {
    TempDbDirectory temp;
    const std::string wal = temp.path() + "/db.log";
    EpochSiEngine engine = EpochSiEngine::CreateFile(Base(), wal);
    auto txn = engine.Begin();
    engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 44));
    auto prepared = engine.PrepareCommitBatch({&txn});
    engine.SyncPreparedCommit(prepared);
    auto moved = std::move(prepared);
    engine.PublishPreparedCommit(moved);

    auto recovered = EpochSiEngine::OpenFile(Base(), wal);
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), 44);
    recovered.Abort(view);
}

TEST(EpochSiEngineTest, PublishedPreparedCommitMayOutliveEngine) {
    auto engine = std::make_unique<EpochSiEngine>(Base());
    auto txn = std::make_unique<EpochSiEngine::Txn>(engine->Begin());
    engine->PutImage(*txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 44));
    auto prepared = engine->PrepareCommitBatch({txn.get()});
    engine->SyncPreparedCommit(prepared);
    engine->PublishPreparedCommit(prepared);
    EXPECT_THROW(engine->PublishPreparedCommit(prepared), std::logic_error);
    EXPECT_THROW(engine->SyncPreparedCommit(prepared), std::logic_error);
    txn.reset();
    engine.reset();
}

TEST(EpochSiEngineTest, PublishedTokenDestructionCannotClearNextOutstandingGuard) {
    EpochSiEngine engine(Base());
    auto first = engine.Begin();
    engine.PutImage(first, {kAccounts, 1}, test_row::Make(kAccounts, "a", 44));
    auto published = std::make_unique<EpochSiEngine::PreparedCommit>(engine.PrepareCommitBatch({&first}));
    engine.SyncPreparedCommit(*published);
    engine.PublishPreparedCommit(*published);

    auto second = engine.Begin();
    engine.PutImage(second, {kAccounts, 2}, test_row::Make(kAccounts, "b", 55));
    auto pending = std::make_unique<EpochSiEngine::PreparedCommit>(engine.PrepareCommitBatch({&second}));
    published.reset();
    auto third = engine.Begin();
    EXPECT_THROW(engine.PrepareCommitBatch({&third}), std::logic_error);
    pending.reset();
    { auto accepted = engine.PrepareCommitBatch({&second}); }
    engine.Abort(second);
    engine.Abort(third);
}

TEST(EpochSiEngineTest, SharedEpochCrossTableRecoveryIsAtomic) {
    EpochSiEngine engine(Base());
    auto account = engine.Begin();
    auto order = engine.Begin();
    engine.PutImage(account, {kAccounts, 1}, test_row::Make(kAccounts, "a", 100));
    engine.PutImage(order, {kOrders, 1}, test_row::Make(kOrders, "o", 300));
    const auto result = engine.CommitBatch({&account, &order});
    ASSERT_EQ(result[0].epoch, result[1].epoch);
    auto recovered = EpochSiEngine::Recover(Base(), engine.recovery_wal_image_for_test());
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), 100);
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kOrders, 1})), 300);
    EXPECT_EQ(recovered.published_epoch(), result[0].epoch);
    EXPECT_EQ(recovered.version_count(), 2U);
    recovered.Abort(view);
}

TEST(EpochSiEngineTest, PartialTailIsDiscardedBeforeContinuation) {
    EpochSiEngine complete(Base());
    auto first = complete.Begin();
    complete.PutImage(first, {kAccounts, 1}, test_row::Make(kAccounts, "a", 101));
    ASSERT_EQ(complete.CommitBatch({&first})[0].status, CommitStatus::kCommitted);
    const size_t first_bytes = complete.recovery_wal_image_for_test().size();
    auto second = complete.Begin();
    complete.PutImage(second, {kAccounts, 2}, test_row::Make(kAccounts, "b", 202));
    ASSERT_EQ(complete.CommitBatch({&second})[0].status, CommitStatus::kCommitted);
    const size_t second_bytes = complete.recovery_wal_image_for_test().size() - first_bytes;

    for (size_t partial = 0; partial <= second_bytes; ++partial) {
        EpochSiEngine crashed(Base());
        auto f1 = crashed.Begin();
        crashed.PutImage(f1, {kAccounts, 1}, test_row::Make(kAccounts, "a", 101));
        ASSERT_EQ(crashed.CommitBatch({&f1})[0].status, CommitStatus::kCommitted);
        auto f2 = crashed.Begin();
        crashed.PutImage(f2, {kAccounts, 2}, test_row::Make(kAccounts, "b", 202));
        crashed.SetCrashPointForTest(CrashPoint::kAfterPartialAppend, partial);
        EXPECT_THROW(crashed.CommitBatch({&f2}), SimulatedCrash);

        auto recovered = EpochSiEngine::Recover(Base(), crashed.recovery_wal_image_for_test());
        EXPECT_EQ(recovered.durable_wal_bytes(), first_bytes + (partial == second_bytes ? second_bytes : 0));
        auto f3 = recovered.Begin();
        recovered.PutImage(f3, {kOrders, 1}, test_row::Make(kOrders, "o", 303));
        ASSERT_EQ(recovered.CommitBatch({&f3})[0].status, CommitStatus::kCommitted);
        auto final = EpochSiEngine::Recover(Base(), recovered.recovery_wal_image_for_test());
        auto view = final.Begin();
        EXPECT_EQ(test_row::Value(*final.Read(view, {kAccounts, 1})), 101) << "partial=" << partial;
        EXPECT_EQ(test_row::Value(*final.Read(view, {kAccounts, 2})), partial == second_bytes ? 202 : 20)
            << "partial=" << partial;
        EXPECT_EQ(test_row::Value(*final.Read(view, {kOrders, 1})), 303) << "partial=" << partial;
        final.Abort(view);
    }
}

TEST(EpochSiEngineTest, CrashMediaAndInstallPoisoningRecoverTheDurableBatch) {
    for (CrashPoint point :
         {CrashPoint::kBeforeAppend, CrashPoint::kAfterSync, CrashPoint::kAfterInstallBeforePublish}) {
        EpochSiEngine engine(Base());
        auto txn = engine.Begin();
        engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 77));
        engine.SetCrashPointForTest(point);
        EXPECT_THROW(engine.CommitBatch({&txn}), SimulatedCrash);
        auto recovered = EpochSiEngine::Recover(Base(), engine.recovery_wal_image_for_test());
        auto view = recovered.Begin();
        EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), point == CrashPoint::kBeforeAppend ? 10 : 77);
        recovered.Abort(view);
        EXPECT_THROW(engine.Begin(), std::logic_error);
        EXPECT_THROW(engine.Read(txn, {kAccounts, 1}), std::logic_error);
    }

    EpochSiEngine engine(Base());
    auto txn = engine.Begin();
    engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 111));
    engine.PutImage(txn, {kAccounts, 2}, test_row::Make(kAccounts, "b", 222));
    engine.SetCrashPointForTest(CrashPoint::kDuringInstall, 1);
    EXPECT_THROW(engine.CommitBatch({&txn}), SimulatedCrash);
    EXPECT_EQ(engine.last_install_version_nodes_for_test(), 1U);
    EXPECT_THROW(engine.Begin(), std::logic_error);
    auto recovered = EpochSiEngine::Recover(Base(), engine.recovery_wal_image_for_test());
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), 111);
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 2})), 222);
    recovered.Abort(view);

    EpochSiEngine clean(Base());
    auto clean_txn = clean.Begin();
    clean.PutImage(clean_txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 111));
    clean.PutImage(clean_txn, {kAccounts, 2}, test_row::Make(kAccounts, "b", 222));
    ASSERT_EQ(clean.CommitBatch({&clean_txn})[0].status, CommitStatus::kCommitted);
    EXPECT_EQ(recovered.MaterializePublished(), clean.MaterializePublished());
}

TEST(EpochSiEngineTest, FramingDistinguishesTornTailFromCorruption) {
    EpochSiEngine engine(Base());
    auto f1 = engine.Begin();
    engine.PutImage(f1, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    ASSERT_EQ(engine.CommitBatch({&f1})[0].status, CommitStatus::kCommitted);
    const size_t first_bytes = engine.recovery_wal_image_for_test().size();
    auto f2 = engine.Begin();
    engine.PutImage(f2, {kAccounts, 2}, test_row::Make(kAccounts, "b", 22));
    ASSERT_EQ(engine.CommitBatch({&f2})[0].status, CommitStatus::kCommitted);
    const auto two_frames = engine.recovery_wal_image_for_test();
    const size_t second_bytes = two_frames.size() - first_bytes;

    for (size_t partial = 0; partial < first_bytes; ++partial) {
        std::vector<uint8_t> image(two_frames.begin(), two_frames.begin() + partial);
        auto recovered = EpochSiEngine::Recover(Base(), image);
        EXPECT_EQ(recovered.published_epoch(), 0U) << "first partial=" << partial;
        EXPECT_EQ(recovered.durable_wal_bytes(), 0U) << "first partial=" << partial;
    }
    for (size_t partial = 0; partial < second_bytes; ++partial) {
        std::vector<uint8_t> image(two_frames.begin(), two_frames.begin() + first_bytes + partial);
        auto recovered = EpochSiEngine::Recover(Base(), image);
        EXPECT_EQ(recovered.published_epoch(), 1U) << "partial=" << partial;
        EXPECT_EQ(recovered.durable_wal_bytes(), first_bytes) << "partial=" << partial;
    }
    auto valid_header_partial_frame = two_frames;
    valid_header_partial_frame.resize(first_bytes + kHeaderBytes + 1);
    EXPECT_NO_THROW(EpochSiEngine::Recover(Base(), valid_header_partial_frame));

    for (size_t offset : {size_t{0}, size_t{12}, first_bytes + size_t{0}, first_bytes + size_t{12},
                          first_bytes + kHeaderBytes, two_frames.size() - size_t{1}}) {
        auto corrupt = two_frames;
        corrupt[offset] ^= 1;
        EXPECT_THROW(EpochSiEngine::Recover(Base(), corrupt), std::runtime_error) << "offset=" << offset;
    }

    EpochSiEngine third_engine = EpochSiEngine::Recover(Base(), two_frames);
    auto f3 = third_engine.Begin();
    third_engine.PutImage(f3, {kOrders, 1}, test_row::Make(kOrders, "o", 33));
    ASSERT_EQ(third_engine.CommitBatch({&f3})[0].status, CommitStatus::kCommitted);
    auto three_frames = third_engine.recovery_wal_image_for_test();
    for (size_t offset :
         {first_bytes, first_bytes + size_t{12}, first_bytes + kHeaderBytes, first_bytes + second_bytes - size_t{1}}) {
        auto corrupt = three_frames;
        corrupt[offset] ^= 1;
        EXPECT_THROW(EpochSiEngine::Recover(Base(), corrupt), std::runtime_error) << "middle offset=" << offset;
    }
}

TEST(EpochSiEngineTest, RecoveryRejectsCrcValidLogicalUniqueViolation) {
    EpochSiEngine engine(Base());
    auto txn = engine.Begin();
    engine.InsertImage(txn, kAccounts, test_row::Make(kAccounts, "x", 1));
    engine.InsertImage(txn, kAccounts, test_row::Make(kAccounts, "y", 2));
    ASSERT_EQ(engine.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    auto forged = engine.recovery_wal_image_for_test();
    const size_t first_op = kHeaderBytes + 12;
    const size_t first_key = first_op + 19 + ReadLe<uint32_t>(forged, first_op + 13) + 8;
    const size_t second_op = first_key + 1;
    const size_t second_key = second_op + 19 + ReadLe<uint32_t>(forged, second_op + 13) + 8;
    ASSERT_EQ(forged[first_key], 'x');
    ASSERT_EQ(forged[second_key], 'y');
    forged[second_key] = 'x';
    RecomputePayloadAndFooterCrc(forged);
    EXPECT_THROW(EpochSiEngine::Recover(Base(), forged), std::runtime_error);
}

TEST(EpochSiEngineTest, RecoveryRejectsCrcValidStableRowIdResurrection) {
    EpochSiEngine engine({});
    auto inserted = engine.Begin();
    const RowId stable_id = engine.InsertImage(inserted, kAccounts, ImmutableRow("a", 1));
    ASSERT_EQ(engine.CommitBatch({&inserted})[0].status, CommitStatus::kCommitted);

    auto erased = engine.Begin();
    engine.Erase(erased, stable_id);
    ASSERT_EQ(engine.CommitBatch({&erased})[0].status, CommitStatus::kCommitted);

    auto replacement = engine.Begin();
    const RowId replacement_id = engine.InsertImage(replacement, kAccounts, ImmutableRow("b", 2));
    ASSERT_NE(replacement_id, stable_id);
    ASSERT_EQ(engine.CommitBatch({&replacement})[0].status, CommitStatus::kCommitted);

    auto forged = engine.recovery_wal_image_for_test();
    const size_t second_frame = FrameBytes(forged);
    const size_t third_frame = second_frame + FrameBytes(forged, second_frame);
    const size_t third_row_id = third_frame + kHeaderBytes + 12;
    WriteLe<uint64_t>(forged, third_row_id + sizeof(TableId), stable_id.local_id);
    RecomputePayloadAndFooterCrc(forged, third_frame);
    EXPECT_THROW(EpochSiEngine::Recover({}, forged), std::runtime_error);
}

TEST(EpochSiEngineTest, SegmentedRecoveryRejectsInsertBelowPersistedAllocatorFrontier) {
    for (const int deletion_shape : {0, 1, 2}) {
        SCOPED_TRACE(deletion_shape);
        TempDbDirectory temp;
        RowId ids[3];
        RowId post_cut;
        {
            auto db = CheckpointDb::Create(temp.path(), {});
            auto seed = db.engine().Begin();
            for (int i = 0; i < 3; ++i)
                ids[i] = db.engine().InsertImage(seed, kAccounts, ImmutableRow(std::to_string(i), i));
            ASSERT_EQ(db.engine().CommitBatch({&seed})[0].status, CommitStatus::kCommitted);
            auto erase = db.engine().Begin();
            if (deletion_shape == 0) {
                db.engine().Erase(erase, ids[2]);
            } else if (deletion_shape == 1) {
                for (const RowId id : ids)
                    db.engine().Erase(erase, id);
            } else {
                db.engine().Erase(erase, ids[1]);
            }
            ASSERT_EQ(db.engine().CommitBatch({&erase})[0].status, CommitStatus::kCommitted);
            db.OfflineCheckpoint();
            auto insert = db.engine().Begin();
            post_cut = db.engine().InsertImage(insert, kAccounts, ImmutableRow("new", 10));
            EXPECT_EQ(post_cut.local_id, 3U);
            ASSERT_EQ(db.engine().CommitBatch({&insert})[0].status, CommitStatus::kCommitted);
            auto update = db.engine().Begin();
            db.engine().PutImage(update, post_cut, ImmutableRow("new", 11));
            ASSERT_EQ(db.engine().CommitBatch({&update})[0].status, CommitStatus::kCommitted);
        }
        {
            auto reopened = CheckpointDb::Open(temp.path());
            auto view = reopened.engine().Begin();
            EXPECT_EQ(test_row::Value(*reopened.engine().Read(view, post_cut)), 11);
            reopened.engine().Abort(view);
        }
        const uint64_t reused_id = deletion_shape == 0 ? ids[2].local_id : ids[1].local_id;
        const std::string wal_path = temp.path() + "/db.log.s.1.0";
        auto segment = ReadBytes(wal_path);
        ASSERT_GT(segment.size(), FileWal::kSegmentHeaderBytes);
        std::vector<uint8_t> forged(segment.begin() + FileWal::kSegmentHeaderBytes, segment.end());
        const size_t first_op = kHeaderBytes + 12;
        WriteLe<uint64_t>(forged, first_op + sizeof(TableId), reused_id);
        RecomputePayloadAndFooterCrc(forged);
        const size_t second_frame = FrameBytes(forged);
        const size_t second_op = second_frame + kHeaderBytes + 12;
        WriteLe<uint64_t>(forged, second_op + sizeof(TableId), reused_id);
        RecomputePayloadAndFooterCrc(forged, second_frame);
        segment.resize(FileWal::kSegmentHeaderBytes);
        segment.insert(segment.end(), forged.begin(), forged.end());
        WriteBytes(wal_path, segment);
        EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error) << deletion_shape;
    }
}

TEST(EpochSiEngineTest, ClaimFreeRecoveryRejectsSameFrameCrossTransactionDuplicate) {
    EpochSiEngine engine({});
    auto seed = engine.Begin();
    const RowId first_id = engine.InsertImage(seed, kAccounts, ImmutableRow("a", 1));
    const RowId second_id = engine.InsertImage(seed, kAccounts, ImmutableRow("b", 2));
    ASSERT_EQ(engine.CommitBatch({&seed})[0].status, CommitStatus::kCommitted);

    auto first = engine.Begin();
    auto second = engine.Begin();
    engine.PutImage(first, first_id, ImmutableRow("a", 3));
    engine.PutImage(second, second_id, ImmutableRow("b", 4));
    const auto committed = engine.CommitBatch({&first, &second});
    ASSERT_EQ(committed[0].status, CommitStatus::kCommitted);
    ASSERT_EQ(committed[1].status, CommitStatus::kCommitted);

    auto forged = engine.recovery_wal_image_for_test();
    const size_t frame_start = FrameBytes(forged);
    const size_t first_op = frame_start + kHeaderBytes + 12;
    const size_t first_image_bytes = ReadLe<uint32_t>(forged, first_op + 13);
    const size_t second_txn = first_op + 19 + first_image_bytes;
    const size_t second_op = second_txn + 12;
    ASSERT_EQ(ReadLe<uint32_t>(forged, second_txn + 8), 1U);
    WriteLe<uint64_t>(forged, second_op + sizeof(TableId), first_id.local_id);
    RecomputePayloadAndFooterCrc(forged, frame_start);
    EXPECT_THROW(EpochSiEngine::Recover({}, forged), std::runtime_error);
}

TEST(EpochSiEngineTest, ImmutableRecoveryUsesSparseFallbackAndPriorVersionTombstone) {
    {
        TempDbDirectory temp;
        auto db = CheckpointDb::Create(
            temp.path(), {{{kAccounts, 0}, ImmutableRow("a", 1)}, {{kAccounts, 128}, ImmutableRow("b", 2)}});
        auto erased = db.engine().Begin();
        db.engine().Erase(erased, {kAccounts, 128});
        ASSERT_EQ(db.engine().CommitBatch({&erased})[0].status, CommitStatus::kCommitted);
        auto reopened = CheckpointDb::Open(temp.path());
        auto view = reopened.engine().Begin();
        EXPECT_FALSE(reopened.engine().Read(view, {kAccounts, 128}));
        reopened.engine().Abort(view);
    }

    TempDbDirectory temp;
    {
        auto db = CheckpointDb::Create(
            temp.path(), {{{kAccounts, 0}, ImmutableRow("a", 1)}, {{kAccounts, 1}, ImmutableRow("b", 2)}});
        auto first = db.engine().Begin();
        db.engine().Erase(first, {kAccounts, 0});
        ASSERT_EQ(db.engine().CommitBatch({&first})[0].status, CommitStatus::kCommitted);
        auto second = db.engine().Begin();
        db.engine().Erase(second, {kAccounts, 1});
        ASSERT_EQ(db.engine().CommitBatch({&second})[0].status, CommitStatus::kCommitted);
    }
    const std::string wal_path = temp.path() + "/db.log.s.0.0";
    auto segment = ReadBytes(wal_path);
    ASSERT_GE(segment.size(), FileWal::kSegmentHeaderBytes);
    std::vector<uint8_t> forged(segment.begin() + FileWal::kSegmentHeaderBytes, segment.end());
    const size_t second_frame = FrameBytes(forged);
    const size_t second_op = second_frame + kHeaderBytes + 12;
    WriteLe<uint64_t>(forged, second_op + sizeof(TableId), 0);
    RecomputePayloadAndFooterCrc(forged, second_frame);
    segment.resize(FileWal::kSegmentHeaderBytes);
    segment.insert(segment.end(), forged.begin(), forged.end());
    WriteBytes(wal_path, segment);
    EXPECT_THROW(CheckpointDb::Open(temp.path()), std::runtime_error);
}

TEST(EpochSiEngineTest, ImmutableRecoverySupersedesConflictingLegacyBaseRows) {
    TempDbDirectory temp;
    auto checkpoint = CheckpointDb::Create(
        temp.path(), {{{kAccounts, 0}, ImmutableRow("immutable", 1)}, {{kAccounts, 2}, ImmutableRow("other", 2)}});
    auto table = OpenImmutableForTest(temp.path() + "/tablebase.1.0");
    ImmutableTables tables{{kAccounts, std::move(table)}};

    EpochSiEngine delete_live({{{kAccounts, 0}, ImmutableRow("source", 1)}});
    auto erase_live = delete_live.Begin();
    delete_live.Erase(erase_live, {kAccounts, 0});
    ASSERT_EQ(delete_live.CommitBatch({&erase_live})[0].status, CommitStatus::kCommitted);

    RowImage stale_tombstone;
    stale_tombstone.deleted = true;
    BaseImage stale{{{kAccounts, 0}, stale_tombstone}, {{kAccounts, 1}, ImmutableRow("stale", 3)}};
    EXPECT_NO_THROW(EpochSiEngine::Recover(stale, tables, delete_live.recovery_wal_image_for_test()));

    EpochSiEngine delete_hole({{{kAccounts, 1}, ImmutableRow("source", 3)}});
    auto erase_hole = delete_hole.Begin();
    delete_hole.Erase(erase_hole, {kAccounts, 1});
    ASSERT_EQ(delete_hole.CommitBatch({&erase_hole})[0].status, CommitStatus::kCommitted);
    EXPECT_THROW(EpochSiEngine::Recover(std::move(stale), std::move(tables), delete_hole.recovery_wal_image_for_test()),
                 std::runtime_error);
}

TEST(EpochSiEngineTest, OpaqueRowImagesAndNamespacedClaimsRecover) {
    EpochSiEngine engine({});
    RowImage row;
    row.bytes = {0, 1, 2, 0xff, 3, 4, 5, 6, 7};
    row.claims = {{7, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}};
    auto first = engine.Begin();
    const RowId id = engine.InsertImage(first, kAccounts, row);
    ASSERT_EQ(engine.CommitBatch({&first})[0].status, CommitStatus::kCommitted);

    auto stale = engine.Begin();
    auto winner = engine.Begin();
    RowImage replacement;
    replacement.bytes = {8, 9, 10};
    replacement.claims = {{7, {3}}};
    engine.PutImage(winner, id, replacement);
    ASSERT_EQ(engine.CommitBatch({&winner})[0].status, CommitStatus::kCommitted);
    RowImage conflicting;
    conflicting.bytes = {4};
    conflicting.claims = {{7, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}};
    engine.InsertImage(stale, kOrders, conflicting);
    EXPECT_EQ(engine.CommitBatch({&stale})[0].status, CommitStatus::kUniqueConflict);

    auto recovered = EpochSiEngine::Recover({}, engine.recovery_wal_image_for_test());
    auto view = recovered.Begin();
    const auto visible = recovered.Read(view, id);
    ASSERT_TRUE(visible.has_value());
    EXPECT_EQ(visible->bytes, replacement.bytes);
    EXPECT_EQ(visible->claims, replacement.claims);
    recovered.Abort(view);
}

TEST(EpochSiEngineTest, RecoveryRejectsImpossibleCountsBeforeAllocation) {
    EpochSiEngine engine(Base());
    auto txn = engine.Begin();
    engine.PutImage(txn, {kAccounts, 1}, test_row::Make(kAccounts, "a", 11));
    engine.PutImage(txn, {kAccounts, 2}, test_row::Make(kAccounts, "b", 22));
    ASSERT_EQ(engine.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);

    auto more_txns_than_ops = engine.recovery_wal_image_for_test();
    WriteLe<uint32_t>(more_txns_than_ops, 32, 3);
    RecomputeHeaderCrc(more_txns_than_ops);
    EXPECT_THROW(EpochSiEngine::Recover(Base(), more_txns_than_ops), std::runtime_error);

    auto impossible_minimum = engine.recovery_wal_image_for_test();
    WriteLe<uint32_t>(impossible_minimum, 32, 2);
    RecomputeHeaderCrc(impossible_minimum);
    EXPECT_THROW(EpochSiEngine::Recover(Base(), impossible_minimum), std::runtime_error);
}

struct RefTxn {
    Epoch start = 0;
    bool active = false;
    std::map<RowId, Row> writes;
    std::set<RowId> inserts;
};

struct RefBatch {
    Epoch epoch;
    std::vector<std::map<RowId, Row>> txns;
};

class ReferenceState {
public:
    explicit ReferenceState(BaseImage base) : base_(std::move(base)) {
        for (const auto& [id, row] : base_) {
            next_[id.table_id] = std::max(next_[id.table_id], id.local_id + 1);
        }
        snapshots_.push_back(Replay());
    }

    RefTxn Begin() const {
        return RefTxn{published_, true, {}, {}};
    }

    std::optional<Row> Read(const RefTxn& txn, RowId id) const {
        const auto local = txn.writes.find(id);
        if (local != txn.writes.end()) {
            return local->second.deleted ? std::nullopt : std::optional<Row>(local->second);
        }
        return ReadCommitted(id, txn.start);
    }

    std::vector<std::pair<RowId, Row>> Scan(const RefTxn& txn, TableId table) const {
        std::set<RowId> ids;
        for (const auto& [id, row] : snapshots_.at(txn.start).current) {
            if (id.table_id == table)
                ids.insert(id);
        }
        for (const auto& [id, row] : txn.writes) {
            if (id.table_id == table)
                ids.insert(id);
        }
        std::vector<std::pair<RowId, Row>> out;
        for (RowId id : ids) {
            if (auto row = Read(txn, id))
                out.emplace_back(id, *row);
        }
        return out;
    }

    RowId Insert(RefTxn& txn, TableId table, std::string key, int64_t value) {
        RowId id{table, next_[table]++};
        txn.writes[id] = test_row::Make(id.table_id, key, value);
        txn.inserts.insert(id);
        return id;
    }

    void Put(RefTxn& txn, RowId id, std::string key, int64_t value) {
        if (!Read(txn, id))
            throw std::invalid_argument("missing reference Put");
        txn.writes[id] = test_row::Make(id.table_id, key, value);
    }

    RowId InsertImage(RefTxn& txn, TableId table, RowImage row) {
        RowId id{table, next_[table]++};
        txn.writes[id] = std::move(row);
        txn.inserts.insert(id);
        return id;
    }

    void PutImage(RefTxn& txn, RowId id, RowImage row) {
        if (!Read(txn, id))
            throw std::invalid_argument("missing reference PutImage");
        txn.writes[id] = std::move(row);
    }

    void Erase(RefTxn& txn, RowId id) {
        if (!Read(txn, id))
            throw std::invalid_argument("missing reference Erase");
        if (txn.inserts.erase(id)) {
            txn.writes.erase(id);
        } else {
            txn.writes[id] = RowImage{{}, {}, true};
        }
    }

    void Abort(RefTxn& txn) const {
        txn.active = false;
    }

    std::vector<CommitResult> CommitBatch(const std::vector<RefTxn*>& txns) {
        auto derived = snapshots_.back();
        std::vector<CommitResult> result(txns.size());
        std::vector<size_t> accepted;
        std::set<RowId> batch_rows;
        std::set<ConstraintClaim> batch_claims;
        for (size_t i = 0; i < txns.size(); ++i) {
            RefTxn& txn = *txns[i];
            if (txn.writes.empty()) {
                result[i].status = CommitStatus::kCommitted;
                accepted.push_back(i);
                continue;
            }
            bool row_conflict = false;
            bool claim_conflict = false;
            std::set<ConstraintClaim> footprint;
            auto owners = derived.owner;
            for (const auto& [id, row] : txn.writes) {
                row_conflict = row_conflict || derived.last_row[id] > txn.start || batch_rows.count(id);
                const auto old = derived.current.find(id);
                if (old != derived.current.end() && !old->second.deleted) {
                    footprint.insert(old->second.claims.begin(), old->second.claims.end());
                    for (const auto& claim : old->second.claims) {
                        const auto owner = owners.find(claim);
                        if (owner != owners.end() && owner->second == id)
                            owners.erase(owner);
                    }
                }
                footprint.insert(row.claims.begin(), row.claims.end());
            }
            for (const auto& claim : footprint) {
                claim_conflict = claim_conflict || derived.last_claim[claim] > txn.start || batch_claims.count(claim);
            }
            for (const auto& [id, row] : txn.writes) {
                for (const auto& claim : row.claims) {
                    const auto [owner, inserted] = owners.emplace(claim, id);
                    claim_conflict = claim_conflict || (!inserted && owner->second != id);
                }
            }
            if (row_conflict || claim_conflict) {
                result[i].status = row_conflict ? CommitStatus::kWriteConflict : CommitStatus::kUniqueConflict;
            } else {
                accepted.push_back(i);
                for (const auto& [id, row] : txn.writes) {
                    batch_rows.insert(id);
                    const auto old = derived.current.find(id);
                    if (old != derived.current.end() && !old->second.deleted)
                        batch_claims.insert(old->second.claims.begin(), old->second.claims.end());
                    batch_claims.insert(row.claims.begin(), row.claims.end());
                }
            }
        }
        if (!accepted.empty()) {
            ++published_;
            RefBatch batch{published_, {}};
            for (size_t index : accepted) {
                result[index] = {CommitStatus::kCommitted, published_, next_seq_++};
                batch.txns.push_back(txns[index]->writes);
                for (const auto& [id, row] : txns[index]->writes) {
                    const auto old = derived.current.find(id);
                    if (old != derived.current.end() && !old->second.deleted) {
                        for (const auto& claim : old->second.claims) {
                            derived.last_claim[claim] = published_;
                            derived.owner.erase(claim);
                        }
                    }
                    for (const auto& claim : row.claims) {
                        derived.last_claim[claim] = published_;
                        derived.owner[claim] = id;
                    }
                    derived.current[id] = row;
                    derived.last_row[id] = published_;
                }
            }
            batches_.push_back(std::move(batch));
            snapshots_.push_back(std::move(derived));
        }
        for (RefTxn* txn : txns)
            txn->active = false;
        return result;
    }

    Epoch published_epoch() const {
        return published_;
    }

    void RestartAllocatorFromCommittedBatches() {
        next_.clear();
        for (const auto& [id, row] : base_) {
            next_[id.table_id] = std::max(next_[id.table_id], id.local_id + 1);
        }
        for (const auto& batch : batches_) {
            for (const auto& writes : batch.txns) {
                for (const auto& [id, row] : writes) {
                    next_[id.table_id] = std::max(next_[id.table_id], id.local_id + 1);
                }
            }
        }
    }

private:
    std::optional<Row> ReadCommitted(RowId id, Epoch snapshot) const {
        const auto row = snapshots_.at(snapshot).current.find(id);
        return row == snapshots_.at(snapshot).current.end() || row->second.deleted ? std::nullopt
                                                                                   : std::optional<Row>(row->second);
    }

    struct Derived {
        std::map<RowId, Row> current;
        std::map<RowId, Epoch> last_row;
        std::map<ConstraintClaim, Epoch> last_claim;
        std::map<ConstraintClaim, RowId> owner;
    };

    Derived Replay() const {
        Derived out;
        out.current = base_;
        for (const auto& [id, row] : base_) {
            for (const auto& claim : row.claims)
                out.owner[claim] = id;
        }
        for (const auto& batch : batches_) {
            for (const auto& writes : batch.txns) {
                for (const auto& [id, row] : writes) {
                    const auto old = out.current.find(id);
                    if (old != out.current.end() && !old->second.deleted) {
                        for (const auto& claim : old->second.claims) {
                            out.last_claim[claim] = batch.epoch;
                            out.owner.erase(claim);
                        }
                    }
                    for (const auto& claim : row.claims) {
                        out.last_claim[claim] = batch.epoch;
                        out.owner[claim] = id;
                    }
                    out.current[id] = row;
                    out.last_row[id] = batch.epoch;
                }
            }
        }
        return out;
    }

    BaseImage base_;
    std::vector<RefBatch> batches_;
    std::vector<Derived> snapshots_;
    std::map<TableId, uint64_t> next_;
    Epoch published_ = 0;
    uint64_t next_seq_ = 1;
};

TEST(EpochSiEngineTest, DeterministicMultiActiveReferenceModel) {
    constexpr size_t kSlots = 8;
    constexpr size_t kStepsPerSeed = 25000;
    struct ActionCounters {
        size_t scans = 0;
        size_t points = 0;
        size_t updates = 0;
        size_t key_moves = 0;
        size_t inserts = 0;
        size_t erases = 0;
        size_t aborts = 0;
        size_t batches = 0;
        size_t recovers = 0;
    } counters;
    for (uint64_t seed : {1ULL, 7ULL, 99ULL, 20260812ULL}) {
        EpochSiEngine engine(Base());
        ReferenceState reference(Base());
        std::array<std::optional<EpochSiEngine::Txn>, kSlots> engine_txns;
        std::array<std::optional<RefTxn>, kSlots> ref_txns;
        std::mt19937_64 rng(seed);

        auto begin_slot = [&](size_t slot) {
            engine_txns[slot].emplace(engine.Begin());
            ref_txns[slot].emplace(reference.Begin());
        };
        auto compare_slot = [&](size_t slot, size_t step) {
            for (TableId table : {kAccounts, kOrders}) {
                EXPECT_EQ(engine.Scan(*engine_txns[slot], table), reference.Scan(*ref_txns[slot], table))
                    << "seed=" << seed << " step=" << step << " table=" << table;
                ++counters.scans;
            }
        };
        auto abort_all = [&] {
            for (size_t slot = 0; slot < kSlots; ++slot) {
                if (engine_txns[slot]) {
                    engine.Abort(*engine_txns[slot]);
                    reference.Abort(*ref_txns[slot]);
                    engine_txns[slot].reset();
                    ref_txns[slot].reset();
                }
            }
        };

        for (size_t step = 0; step < kStepsPerSeed; ++step) {
            SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
            const size_t slot = rng() % kSlots;
            const int action = rng() % 8;
            if (!engine_txns[slot]) {
                begin_slot(slot);
                continue;
            }
            RefTxn& ref_txn = *ref_txns[slot];
            EpochSiEngine::Txn& engine_txn = *engine_txns[slot];
            const TableId table = rng() % 2 == 0 ? kAccounts : kOrders;
            const auto visible = reference.Scan(ref_txn, table);
            if (action == 0 || action == 1) {
                compare_slot(slot, step);
            } else if (action == 2 && !visible.empty()) {
                const RowId id = visible[rng() % visible.size()].first;
                EXPECT_EQ(engine.Read(engine_txn, id), reference.Read(ref_txn, id));
                ++counters.points;
            } else if (action == 3 && !visible.empty()) {
                const auto& [id, row] = visible[rng() % visible.size()];
                const std::string key = rng() % 4 == 0 ? test_row::Key(visible.front().second) : test_row::Key(row);
                const int64_t value = static_cast<int64_t>(rng() % 100000);
                RowImage image = test_row::Random(id.table_id, key, value, rng());
                engine.PutImage(engine_txn, id, image);
                reference.PutImage(ref_txn, id, std::move(image));
                key == test_row::Key(row) ? ++counters.updates : ++counters.key_moves;
            } else if (action == 4 && visible.size() < 128) {
                const std::string key =
                    !visible.empty() && rng() % 4 == 0
                        ? test_row::Key(visible.front().second)
                        : "s" + std::to_string(seed) + "_n" + std::to_string(step) + "_" + std::to_string(slot);
                const int64_t value = static_cast<int64_t>(rng() % 100000);
                RowImage image = test_row::Random(table, key, value, rng());
                const RowId actual = engine.InsertImage(engine_txn, table, image);
                const RowId expected = reference.InsertImage(ref_txn, table, std::move(image));
                EXPECT_EQ(actual, expected);
                ++counters.inserts;
            } else if (action == 5 && !visible.empty()) {
                const RowId id = visible[rng() % visible.size()].first;
                engine.Erase(engine_txn, id);
                reference.Erase(ref_txn, id);
                ++counters.erases;
            } else if (action == 6) {
                engine.Abort(engine_txn);
                reference.Abort(ref_txn);
                engine_txns[slot].reset();
                ref_txns[slot].reset();
                ++counters.aborts;
            } else {
                std::vector<EpochSiEngine::Txn*> engine_batch;
                std::vector<RefTxn*> ref_batch;
                std::vector<size_t> selected;
                for (size_t candidate = 0; candidate < kSlots; ++candidate) {
                    if (engine_txns[candidate] && (candidate == slot || rng() % 2 == 0)) {
                        engine_batch.push_back(&*engine_txns[candidate]);
                        ref_batch.push_back(&*ref_txns[candidate]);
                        selected.push_back(candidate);
                    }
                }
                const auto actual = engine.CommitBatch(engine_batch);
                const auto expected = reference.CommitBatch(ref_batch);
                EXPECT_EQ(actual.size(), expected.size());
                for (size_t i = 0; i < actual.size(); ++i) {
                    EXPECT_EQ(actual[i].status, expected[i].status) << "seed=" << seed << " step=" << step;
                    EXPECT_EQ(actual[i].epoch, expected[i].epoch) << "seed=" << seed << " step=" << step;
                    EXPECT_EQ(actual[i].commit_seq, expected[i].commit_seq) << "seed=" << seed << " step=" << step;
                }
                ++counters.batches;
                for (size_t chosen : selected) {
                    engine_txns[chosen].reset();
                    ref_txns[chosen].reset();
                }
            }
            if (step % 4093 == 4092) {
                abort_all();
                engine = EpochSiEngine::Recover(Base(), engine.recovery_wal_image_for_test());
                reference.RestartAllocatorFromCommittedBatches();
                EXPECT_EQ(engine.published_epoch(), reference.published_epoch()) << "seed=" << seed << " step=" << step;
                ++counters.recovers;
                begin_slot(0);
                compare_slot(0, step);
                engine.Abort(*engine_txns[0]);
                reference.Abort(*ref_txns[0]);
                engine_txns[0].reset();
                ref_txns[0].reset();
            }
        }
        abort_all();
    }
    EXPECT_EQ(kStepsPerSeed * 4, 100000U);
    EXPECT_GT(counters.scans, 0U);
    EXPECT_GT(counters.points, 0U);
    EXPECT_GT(counters.updates, 0U);
    EXPECT_GT(counters.key_moves, 0U);
    EXPECT_GT(counters.inserts, 0U);
    EXPECT_GT(counters.erases, 0U);
    EXPECT_GT(counters.aborts, 0U);
    EXPECT_GT(counters.batches, 0U);
    EXPECT_GT(counters.recovers, 0U);
}

} // namespace
} // namespace epoch_si_poc
