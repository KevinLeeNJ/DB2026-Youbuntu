#include "epoch_si_engine.h"
#include "test_row.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace epoch_si_poc {
namespace {

constexpr TableId kTable = 1;
constexpr size_t kHeaderBytes = 48;
constexpr size_t kFooterBytes = 16;

uint32_t Crc32(const uint8_t* data, size_t size) {
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < size; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
    return ~crc;
}

template <typename T> void WriteLe(std::vector<uint8_t>& bytes, size_t offset, T value) {
    for (size_t i = 0; i < sizeof(T); ++i)
        bytes.at(offset + i) = static_cast<uint8_t>(value >> (8 * i));
}

BaseImage FileBase() {
    return {{{kTable, 1}, test_row::Make(kTable, "a", 10)}, {{kTable, 2}, test_row::Make(kTable, "b", 20)}};
}

class TempWal {
public:
    TempWal() {
        char pattern[] = "/tmp/rmdb-epoch-si-XXXXXX";
        const char* created = mkdtemp(pattern);
        if (created == nullptr) {
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        }
        directory_ = created;
        path_ = directory_ + "/engine.log";
    }

    ~TempWal() {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    const std::string& path() const {
        return path_;
    }

private:
    std::string directory_;
    std::string path_;
};

void WaitForChild(pid_t child) {
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status) || WIFSIGNALED(status));
}

void ChildCommitAndCrash(const std::string& path, CrashPoint point, size_t position = 0) {
    auto engine = std::filesystem::exists(path) ? EpochSiEngine::OpenFile(FileBase(), path)
                                                : EpochSiEngine::CreateFile(FileBase(), path);
    auto txn = engine.Begin();
    engine.PutImage(txn, {kTable, 1}, test_row::Make(kTable, "a", 99));
    engine.SetCrashPointForTest(point, position);
    try {
        engine.CommitBatch({&txn});
    } catch (const SimulatedCrash&) {
        _exit(0);
    }
    _exit(1);
}

TEST(FileWalTest, ReopenPreservesFramesAndUsesRealFdatasync) {
    TempWal temp;
    auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
    engine.SetFileMaxWriteChunkForTest(3);
    auto first = engine.Begin();
    auto second = engine.Begin();
    engine.PutImage(first, {kTable, 1}, test_row::Make(kTable, "a", 11));
    engine.PutImage(second, {kTable, 2}, test_row::Make(kTable, "b", 22));
    const auto committed = engine.CommitBatch({&first, &second});
    ASSERT_EQ(committed[0].status, CommitStatus::kCommitted);
    ASSERT_EQ(committed[0].epoch, committed[1].epoch);
    EXPECT_GT(engine.durable_wal_bytes(), 0U);
    EXPECT_EQ(engine.wal_frame_count(), 1U);
    EXPECT_EQ(engine.wal_transaction_count(), 2U);

    auto reopened = EpochSiEngine::OpenFile(FileBase(), temp.path());
    auto view = reopened.Begin();
    EXPECT_EQ(test_row::Value(*reopened.Read(view, {kTable, 1})), 11);
    EXPECT_EQ(test_row::Value(*reopened.Read(view, {kTable, 2})), 22);
    EXPECT_EQ(reopened.wal_frame_count(), 1U);
    EXPECT_EQ(reopened.wal_transaction_count(), 2U);
    reopened.Abort(view);
}

TEST(FileWalTest, ReadOnlyCommitsAppendAndSyncInEveryAckWindow) {
    TempWal temp;
    auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
    size_t bytes = engine.durable_wal_bytes();
    size_t writes = engine.wal_write_calls_for_test();
    size_t syncs = engine.wal_sync_calls_for_test();
    for (uint64_t expected = 1; expected <= 2; ++expected) {
        auto read_only = engine.Begin();
        ASSERT_TRUE(engine.Read(read_only, {kTable, 1}).has_value());
        const auto result = engine.CommitBatch({&read_only})[0];
        EXPECT_EQ(result.status, CommitStatus::kCommitted);
        EXPECT_EQ(result.epoch, expected);
        EXPECT_EQ(result.commit_seq, expected);
        EXPECT_GT(engine.durable_wal_bytes(), bytes);
        EXPECT_EQ(engine.wal_write_calls_for_test(), writes + 1);
        EXPECT_EQ(engine.wal_sync_calls_for_test(), syncs + 1);
        bytes = engine.durable_wal_bytes();
        writes = engine.wal_write_calls_for_test();
        syncs = engine.wal_sync_calls_for_test();
    }

    auto reopened = EpochSiEngine::OpenFile(FileBase(), temp.path());
    EXPECT_EQ(reopened.published_epoch(), 2U);
    EXPECT_EQ(reopened.wal_frame_count(), 2U);
    EXPECT_EQ(reopened.wal_transaction_count(), 2U);
}

TEST(FileWalTest, MixedBatchPersistsReadOnlyTransactionsInOrder) {
    TempWal temp;
    auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
    auto first_read = engine.Begin();
    auto writer = engine.Begin();
    auto second_read = engine.Begin();
    engine.PutImage(writer, {kTable, 1}, test_row::Make(kTable, "a", 55));
    const auto result = engine.CommitBatch({&first_read, &writer, &second_read});
    ASSERT_EQ(result.size(), 3U);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_EQ(result[i].status, CommitStatus::kCommitted);
        EXPECT_EQ(result[i].epoch, 1U);
        EXPECT_EQ(result[i].commit_seq, i + 1);
    }
    EXPECT_EQ(engine.wal_write_calls_for_test(), 1U);
    EXPECT_EQ(engine.wal_transaction_count(), 3U);

    auto reopened = EpochSiEngine::OpenFile(FileBase(), temp.path());
    EXPECT_EQ(reopened.published_epoch(), 1U);
    EXPECT_EQ(reopened.wal_transaction_count(), 3U);
    auto view = reopened.Begin();
    EXPECT_EQ(test_row::Value(*reopened.Read(view, {kTable, 1})), 55);
    reopened.Abort(view);
}

TEST(FileWalTest, ZeroOperationFramesRecoverRejectCorruptionAndDiscardTornTail) {
    EpochSiEngine engine(FileBase());
    auto read_only = engine.Begin();
    ASSERT_EQ(engine.CommitBatch({&read_only})[0].status, CommitStatus::kCommitted);
    const auto frame = engine.recovery_wal_image_for_test();
    ASSERT_GT(frame.size(), kHeaderBytes + kFooterBytes);
    auto recovered = EpochSiEngine::Recover(FileBase(), frame);
    EXPECT_EQ(recovered.published_epoch(), 1U);
    EXPECT_EQ(recovered.wal_transaction_count(), 1U);

    auto torn = frame;
    torn.pop_back();
    auto discarded = EpochSiEngine::Recover(FileBase(), torn);
    EXPECT_EQ(discarded.published_epoch(), 0U);
    EXPECT_EQ(discarded.durable_wal_bytes(), 0U);

    auto corrupt = frame;
    WriteLe<uint32_t>(corrupt, 36, 1); // Claims one mutation absent from the payload.
    WriteLe<uint32_t>(corrupt, 44, Crc32(corrupt.data(), 44));
    EXPECT_THROW(EpochSiEngine::Recover(FileBase(), corrupt), std::runtime_error);
}

TEST(FileWalTest, LegacyV2MutationFrameStillRecovers) {
    EpochSiEngine engine(FileBase());
    auto txn = engine.Begin();
    engine.PutImage(txn, {kTable, 1}, test_row::Make(kTable, "a", 66));
    ASSERT_EQ(engine.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
    auto legacy = engine.recovery_wal_image_for_test();
    WriteLe<uint32_t>(legacy, 4, 2);
    WriteLe<uint32_t>(legacy, 44, Crc32(legacy.data(), 44));

    auto recovered = EpochSiEngine::Recover(FileBase(), legacy);
    auto view = recovered.Begin();
    EXPECT_EQ(test_row::Value(*recovered.Read(view, {kTable, 1})), 66);
    recovered.Abort(view);
}

TEST(FileWalTest, ReadOnlyAfterSyncCrashRecoversDurableCommit) {
    TempWal temp;
    auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
    auto read_only = engine.Begin();
    engine.SetCrashPointForTest(CrashPoint::kAfterSync);
    EXPECT_THROW(engine.CommitBatch({&read_only}), SimulatedCrash);

    auto recovered = EpochSiEngine::OpenFile(FileBase(), temp.path());
    EXPECT_EQ(recovered.published_epoch(), 1U);
    EXPECT_EQ(recovered.wal_transaction_count(), 1U);
}

TEST(FileWalTest, TornTailIsTruncatedBeforeNewCommit) {
    TempWal temp;
    size_t first_bytes = 0;
    {
        auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
        auto txn = engine.Begin();
        engine.PutImage(txn, {kTable, 1}, test_row::Make(kTable, "a", 11));
        ASSERT_EQ(engine.CommitBatch({&txn})[0].status, CommitStatus::kCommitted);
        first_bytes = engine.durable_wal_bytes();
    }
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0)
        ChildCommitAndCrash(temp.path(), CrashPoint::kAfterPartialAppend, 17);
    WaitForChild(child);
    ASSERT_GT(std::filesystem::file_size(temp.path()), first_bytes);

    auto recovered = EpochSiEngine::OpenFile(FileBase(), temp.path());
    EXPECT_EQ(recovered.durable_wal_bytes(), first_bytes);
    auto next = recovered.Begin();
    recovered.PutImage(next, {kTable, 2}, test_row::Make(kTable, "b", 33));
    ASSERT_EQ(recovered.CommitBatch({&next})[0].status, CommitStatus::kCommitted);

    auto final = EpochSiEngine::OpenFile(FileBase(), temp.path());
    auto view = final.Begin();
    EXPECT_EQ(test_row::Value(*final.Read(view, {kTable, 1})), 11);
    EXPECT_EQ(test_row::Value(*final.Read(view, {kTable, 2})), 33);
    EXPECT_EQ(final.wal_frame_count(), 2U);
    final.Abort(view);
}

TEST(FileWalTest, ProcessCrashBoundariesHonorDurability) {
    for (CrashPoint point :
         {CrashPoint::kAfterAppendBeforeSync, CrashPoint::kAfterSync, CrashPoint::kAfterPublishBeforeReturn}) {
        TempWal temp;
        const pid_t child = fork();
        ASSERT_GE(child, 0);
        if (child == 0)
            ChildCommitAndCrash(temp.path(), point);
        WaitForChild(child);
        auto recovered = EpochSiEngine::OpenFile(FileBase(), temp.path());
        auto view = recovered.Begin();
        const int64_t value = test_row::Value(*recovered.Read(view, {kTable, 1}));
        if (point == CrashPoint::kAfterAppendBeforeSync) {
            EXPECT_TRUE(value == 10 || value == 99);
        } else {
            EXPECT_EQ(value, 99);
        }
        recovered.Abort(view);
    }
}

TEST(FileWalTest, MidLogCorruptionFailsOpen) {
    TempWal temp;
    size_t first_bytes = 0;
    {
        auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
        auto first = engine.Begin();
        engine.PutImage(first, {kTable, 1}, test_row::Make(kTable, "a", 11));
        ASSERT_EQ(engine.CommitBatch({&first})[0].status, CommitStatus::kCommitted);
        first_bytes = engine.durable_wal_bytes();
        auto second = engine.Begin();
        engine.PutImage(second, {kTable, 2}, test_row::Make(kTable, "b", 22));
        ASSERT_EQ(engine.CommitBatch({&second})[0].status, CommitStatus::kCommitted);
    }
    std::fstream file(temp.path(), std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.seekp(static_cast<std::streamoff>(first_bytes + 4));
    char byte = 0;
    file.write(&byte, 1);
    file.close();
    EXPECT_THROW(EpochSiEngine::OpenFile(FileBase(), temp.path()), std::runtime_error);
}

TEST(FileWalTest, IoErrorPoisonsEngine) {
    TempWal temp;
    auto engine = EpochSiEngine::CreateFile(FileBase(), temp.path());
    auto txn = engine.Begin();
    engine.PutImage(txn, {kTable, 1}, test_row::Make(kTable, "a", 11));
    engine.CloseFileForTest();
    EXPECT_THROW(engine.CommitBatch({&txn}), std::system_error);
    EXPECT_THROW(engine.Begin(), std::logic_error);
}

} // namespace
} // namespace epoch_si_poc
