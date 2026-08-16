#include "epoch_si_engine.h"
#include "checkpoint_db.h"
#include "test_row.h"

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <filesystem>
#include <limits>
#include <random>
#include <set>
#include <system_error>
#include <type_traits>
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
    EXPECT_EQ(empty.epoch, 0U);
    EXPECT_EQ(empty.commit_seq, 0U);
    EXPECT_EQ(engine.durable_wal_bytes(), before);

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
    EXPECT_THROW(engine.Begin(), std::logic_error);
    auto recovered = EpochSiEngine::Recover(Base(), engine.recovery_wal_image_for_test());
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 1})), 111);
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kAccounts, 2})), 222);
    recovered.Abort(view);
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
                result[i] = {CommitStatus::kCommitted, published_, 0};
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
