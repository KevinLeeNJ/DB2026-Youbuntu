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
