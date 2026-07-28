/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/context.h"
#include "execution/execution_common.h"
#include "index/ix.h"
#include "record/rm.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sys/wait.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

std::filesystem::path CurrentPath() {
    return std::filesystem::current_path();
}

class ScopedTestDir {
public:
    explicit ScopedTestDir(std::string dir) : old_path_(CurrentPath()), dir_(std::move(dir)) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directory(dir_);
        std::filesystem::current_path(dir_);
    }

    ~ScopedTestDir() {
        std::filesystem::current_path(old_path_);
        std::filesystem::remove_all(dir_);
    }

private:
    std::filesystem::path old_path_;
    std::filesystem::path dir_;
};

RmRecord MakeTuple(int id, int value) {
    // 必须与 CreateRecoveryTestDb 里 create_table 算出的 record_size 一致：
    // 2 个 INT 列的数据区 + 1 字节尾部 null bitmap。长度不一致会让
    // RecoveryManager::record_equals 的幂等守卫因 size 不同而跳过 undo。
    RmRecord rec(static_cast<int>(sizeof(int) * 2) + null_bitmap_bytes(2));
    memset(rec.data, 0, static_cast<size_t>(rec.size));
    memcpy(rec.data, &id, sizeof(int));
    memcpy(rec.data + sizeof(int), &value, sizeof(int));
    return rec;
}

std::vector<char> MakeIntKey(int value) {
    std::vector<char> key(sizeof(int));
    memcpy(key.data(), &value, sizeof(int));
    return key;
}

void CreateRecoveryTestDb(const std::string& db_name, const std::vector<std::string>& table_names = {"t"}) {
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);

    sm_mgr.create_db(db_name);
    sm_mgr.open_db(db_name);
    for (const auto& table_name : table_names) {
        sm_mgr.create_table(table_name, {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
        sm_mgr.create_index(table_name, {"id"}, nullptr);
    }
    sm_mgr.close_db();
}

class OpenRecoveryDb {
public:
    explicit OpenRecoveryDb(const std::string& db_name)
        : bpm_(64, &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_), sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_),
          log_mgr_(std::make_unique<LogManager>(&disk_)) {
        sm_mgr_.open_db(db_name);
    }

    ~OpenRecoveryDb() {
        if (opened_) {
            sm_mgr_.close_db();
        }
    }

    DiskManager disk_;
    BufferPoolManager bpm_;
    RmManager rm_mgr_;
    IxManager ix_mgr_;
    SmManager sm_mgr_;
    std::unique_ptr<LogManager> log_mgr_;
    bool opened_{true};
};

lsn_t AppendBegin(LogManager& log_mgr, txn_id_t txn_id) {
    BeginLogRecord begin(txn_id);
    return log_mgr.add_log_to_buffer(&begin);
}

lsn_t AppendInsert(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn, const Rid& rid, RmRecord& rec,
                   const std::string& table_name = "t") {
    Rid log_rid = rid;
    InsertLogRecord insert(txn_id, rec, log_rid, table_name);
    insert.prev_lsn_ = prev_lsn;
    return log_mgr.add_log_to_buffer(&insert);
}

lsn_t AppendDelete(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn, const Rid& rid, RmRecord& rec) {
    Rid log_rid = rid;
    DeleteLogRecord del(txn_id, rec, log_rid, "t");
    del.prev_lsn_ = prev_lsn;
    return log_mgr.add_log_to_buffer(&del);
}

lsn_t AppendUpdate(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn, const Rid& rid, RmRecord& old_rec,
                   RmRecord& new_rec) {
    Rid log_rid = rid;
    UpdateLogRecord update(txn_id, old_rec, new_rec, log_rid, "t");
    update.prev_lsn_ = prev_lsn;
    return log_mgr.add_log_to_buffer(&update);
}

void AppendCommit(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn) {
    CommitLogRecord commit(txn_id);
    commit.prev_lsn_ = prev_lsn;
    log_mgr.add_log_to_buffer(&commit);
}

// 带 MVCC 提交时间戳的 COMMIT 记录，就是生产提交路径写下的形状。
void AppendCommitWithTs(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn, timestamp_t commit_ts) {
    CommitLogRecord commit(txn_id, commit_ts);
    commit.prev_lsn_ = prev_lsn;
    log_mgr.add_log_to_buffer(&commit);
}

void AppendAbort(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn) {
    AbortLogRecord abort(txn_id);
    abort.prev_lsn_ = prev_lsn;
    log_mgr.add_log_to_buffer(&abort);
}

void FlushLogs(LogManager& log_mgr) {
    log_mgr.flush_log_to_disk();
}

// Mirrors what rmdb.cpp does at startup. Note that production calls
// LogManager::initialize_from_existing_log() first, which truncates the WAL to
// its intact prefix; RecoveryManager documents that as a precondition. Every WAL
// these tests build is complete, so analyze()'s "the file ends where the scan
// ended" check holds either way.
void RunRecovery(const std::string& db_name) {
    OpenRecoveryDb db(db_name);
    auto recovery = std::make_unique<RecoveryManager>(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery->analyze();
    recovery->redo();
    recovery->undo();
}

int RunRecoveryAfterInjectedProcessDeath(const std::string& db_name, const char* point) {
#ifndef RMDB_ENABLE_FAULT_INJECTION
    (void)db_name;
    (void)point;
    return -1;
#else
    // _exit(137) is the FaultInjector's deterministic crash action. Using it
    // avoids racing a SIGKILL against the block action, which has no
    // ready-at-point notification. The child still disappears without running
    // recovery cleanup, so the following process exercises the same replay.
    if (setenv("RMDB_FAULT_POINT", point, 1) != 0 || setenv("RMDB_FAULT_ACTION", "_exit", 1) != 0 ||
        unsetenv("RMDB_FAULT_SKIP") != 0) {
        return -1;
    }

    const pid_t child = fork();
    if (child == -1) {
        unsetenv("RMDB_FAULT_POINT");
        unsetenv("RMDB_FAULT_ACTION");
        return -1;
    }
    if (child == 0) {
        RunRecovery(db_name);
        _exit(0);
    }

    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited == -1 && errno == EINTR);

    unsetenv("RMDB_FAULT_POINT");
    unsetenv("RMDB_FAULT_ACTION");
    unsetenv("RMDB_FAULT_SKIP");
    if (waited != child) {
        return -1;
    }
    return status;
#endif
}

bool RecordExists(SmManager& sm_mgr, const Rid& rid, const std::string& table_name = "t") {
    try {
        auto* fh = sm_mgr.fhs_.at(table_name).get();
        if (rid.page_no < 0 || rid.page_no >= fh->get_file_hdr().num_pages) {
            return false;
        }
        return fh->is_record(rid);
    } catch (const std::exception&) {
        return false;
    }
}

int RecordValue(SmManager& sm_mgr, const Rid& rid, const std::string& table_name = "t") {
    auto record = sm_mgr.fhs_.at(table_name)->get_record(rid, nullptr);
    int value = 0;
    memcpy(&value, record->data + sizeof(int), sizeof(int));
    return value;
}

std::vector<Rid> IndexEntriesFor(SmManager& sm_mgr, int key, const std::string& table_name = "t") {
    auto* index = sm_mgr.ihs_.at(sm_mgr.get_ix_manager()->get_index_name(table_name, {"id"})).get();
    std::vector<Rid> result;
    index->get_value(MakeIntKey(key).data(), &result, nullptr);
    return result;
}

bool IndexPointsTo(SmManager& sm_mgr, int key, const Rid& rid, const std::string& table_name = "t") {
    const auto result = IndexEntriesFor(sm_mgr, key, table_name);
    return result.size() == 1 && result[0] == rid;
}

// Walks the WAL by its length prefixes and returns the file offsets of the
// records whose type matches, so a test can corrupt one specific record.
std::vector<int64_t> WalRecordOffsets(DiskManager& disk, LogType wanted) {
    std::vector<int64_t> offsets;
    const int64_t file_size = disk.get_file_size(LOG_FILE_NAME);
    std::vector<char> header(LOG_HEADER_SIZE);
    int64_t offset = 0;
    while (offset + LOG_HEADER_SIZE <= file_size) {
        if (disk.read_log_chunk(header.data(), LOG_HEADER_SIZE, offset) != LOG_HEADER_SIZE) {
            break;
        }
        const auto total_len = read_unaligned<uint32_t>(header.data() + OFFSET_LOG_TOT_LEN);
        if (total_len < static_cast<uint32_t>(LOG_HEADER_SIZE) ||
            offset + static_cast<int64_t>(total_len) > file_size) {
            break;
        }
        if (read_unaligned<LogType>(header.data() + OFFSET_LOG_TYPE) == wanted) {
            offsets.push_back(offset);
        }
        offset += total_len;
    }
    return offsets;
}

void PatchWalBytes(const std::string& log_path, int64_t offset, const void* bytes, size_t size) {
    std::fstream file(log_path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file.is_open());
    file.seekp(static_cast<std::streamoff>(offset));
    file.write(static_cast<const char*>(bytes), static_cast<std::streamsize>(size));
    file.flush();
    ASSERT_TRUE(static_cast<bool>(file));
}

} // namespace

TEST(RecoveryApplyTest, InsertUpdateDeleteKeepIndexConsistent) {
    ScopedTestDir test_dir("recovery_apply_test_root");
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);

    sm_mgr.create_db("recovery_apply_test_db");
    sm_mgr.open_db("recovery_apply_test_db");
    sm_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
    sm_mgr.create_index("t", {"id"}, nullptr);

    Rid rid{1, 0};
    auto rec1 = MakeTuple(1, 10);
    auto rec2 = MakeTuple(2, 20);
    auto* index = sm_mgr.ihs_.at(sm_mgr.get_ix_manager()->get_index_name("t", {"id"})).get();

    sm_mgr.insert_record_with_indexes("t", rid, rec1);
    std::vector<Rid> result;
    EXPECT_TRUE(index->get_value(MakeIntKey(1).data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], rid);

    sm_mgr.update_record_with_indexes("t", rid, rec1, rec2);
    result.clear();
    EXPECT_FALSE(index->get_value(MakeIntKey(1).data(), &result, nullptr));
    result.clear();
    EXPECT_TRUE(index->get_value(MakeIntKey(2).data(), &result, nullptr));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], rid);

    sm_mgr.delete_record_with_indexes("t", rid, rec2);
    result.clear();
    EXPECT_FALSE(index->get_value(MakeIntKey(2).data(), &result, nullptr));
    EXPECT_FALSE(sm_mgr.fhs_.at("t")->is_record(rid));

    sm_mgr.close_db();
}

TEST(RecoveryManagerTest, CommittedInsertSurvivesRecovery) {
    ScopedTestDir test_dir("recovery_committed_insert_root");
    const std::string db_name = "recovery_committed_insert_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto insert_lsn = AppendInsert(*db.log_mgr_, 100, begin_lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 100, insert_lsn);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);
    // A second recovery pass must be harmless after the first pass has
    // rebuilt pages/indexes and reset the WAL.
    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
    auto file_hdr = db.sm_mgr_.fhs_.at("t")->get_file_hdr();
    EXPECT_EQ(file_hdr.num_pages, 2);
    EXPECT_EQ(file_hdr.first_free_page_no, 1);
}

TEST(RecoveryManagerTest, InterruptedIndexSwapIsRepairedOnOpen) {
    ScopedTestDir test_dir("recovery_index_swap_repair_root");
    const std::string db_name = "recovery_index_swap_repair_db";
    CreateRecoveryTestDb(db_name);

    DiskManager disk;
    const std::string index_name = "t_id.idx";
    const std::string backup_name = index_name + ".rebuild.bak";
    ASSERT_TRUE(disk.is_file(db_name + "/" + index_name));
    std::filesystem::rename(db_name + "/" + index_name, db_name + "/" + backup_name);

    OpenRecoveryDb db(db_name);
    EXPECT_TRUE(disk.is_file(index_name));
    EXPECT_FALSE(disk.is_file(backup_name));
    EXPECT_NE(db.sm_mgr_.ihs_.find(index_name), db.sm_mgr_.ihs_.end());
}

TEST(RecoveryManagerTest, UncommittedInsertIsUndone) {
    ScopedTestDir test_dir("recovery_uncommitted_insert_root");
    const std::string db_name = "recovery_uncommitted_insert_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendInsert(*db.log_mgr_, 100, begin_lsn, rid, rec);
        FlushLogs(*db.log_mgr_);
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        // 模拟 executor_insert 写下的 MVCC meta：未提交、归属本事务。
        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, meta);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, UncommittedInsertBeyondStaleFileHeaderDoesNotAbortRecovery) {
    ScopedTestDir test_dir("recovery_stale_file_header_root");
    const std::string db_name = "recovery_stale_file_header_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{2, 0};
    auto rec = MakeTuple(1, 10);

    {
        DiskManager disk;
        BufferPoolManager bpm(64, &disk);
        RmManager rm_mgr(&disk, &bpm);
        IxManager ix_mgr(&disk, &bpm);
        SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
        sm_mgr.open_db(db_name);
        LogManager log_mgr(&disk);

        auto begin_lsn = AppendBegin(log_mgr, 100);
        AppendInsert(log_mgr, 100, begin_lsn, rid, rec);
        FlushLogs(log_mgr);

        auto* file_handle = sm_mgr.fhs_.at("t").get();
        const RmFileHdr stale_header = file_handle->get_file_hdr();
        for (int i = 0; i < 2; ++i) {
            auto page = file_handle->create_new_page_handle();
            ASSERT_TRUE(bpm.unpin_page(page.page->get_page_id(), true));
        }
        file_handle->insert_record(rid, rec.data);
        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        file_handle->set_tuple_meta(rid, meta);
        ASSERT_TRUE(bpm.flush_all_pages(file_handle->GetFd()));

        // Persist the record pages but deliberately leave the short file
        // header at its pre-allocation value, as can happen on kill -9.
        disk.write_page(file_handle->GetFd(), RM_FILE_HDR_PAGE, reinterpret_cast<const char*>(&stale_header),
                        sizeof(stale_header));
        bpm.delete_all_pages(file_handle->GetFd());
    }
    std::filesystem::current_path("..");

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, CommittedUpdateSurvivesRecovery) {
    ScopedTestDir test_dir("recovery_committed_update_root");
    const std::string db_name = "recovery_committed_update_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto update_lsn = AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        AppendCommit(*db.log_mgr_, 100, update_lsn);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

TEST(RecoveryManagerTest, RedoUpdateMissingRidInstallsCommittedTupleMeta) {
    ScopedTestDir test_dir("recovery_redo_update_missing_rid_root");
    const std::string db_name = "recovery_redo_update_missing_rid_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto update_lsn = AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        AppendCommit(*db.log_mgr_, 100, update_lsn);
        FlushLogs(*db.log_mgr_);
    }

    {
        OpenRecoveryDb db(db_name);
        auto recovery = std::make_unique<RecoveryManager>(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery->analyze();
        recovery->redo();

        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
        const TupleMeta meta = db.sm_mgr_.fhs_.at("t")->get_tuple_meta(rid);
        EXPECT_EQ(meta.writer_txn_id_, 100);
        EXPECT_TRUE(meta.is_committed_);
        EXPECT_FALSE(meta.is_deleted_);
    }
}

TEST(RecoveryManagerTest, UncommittedUpdateIsUndone) {
    ScopedTestDir test_dir("recovery_uncommitted_update_root");
    const std::string db_name = "recovery_uncommitted_update_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        FlushLogs(*db.log_mgr_);
        db.sm_mgr_.update_record_with_indexes("t", rid, old_rec, new_rec);
        // 模拟 executor_update 写下的 MVCC meta：未提交、归属本事务。
        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, meta);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

TEST(RecoveryManagerTest, AbortedUpdateDoesNotUndoLaterCommittedSameValueUpdate) {
    ScopedTestDir test_dir("recovery_aborted_update_same_value_root");
    const std::string db_name = "recovery_aborted_update_same_value_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(1, 20);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto loser_begin = AppendBegin(*db.log_mgr_, 100);
        auto loser_update = AppendUpdate(*db.log_mgr_, 100, loser_begin, rid, old_rec, new_rec);
        AppendAbort(*db.log_mgr_, 100, loser_update);

        auto committed_begin = AppendBegin(*db.log_mgr_, 200);
        auto committed_update = AppendUpdate(*db.log_mgr_, 200, committed_begin, rid, old_rec, new_rec);
        AppendCommit(*db.log_mgr_, 200, committed_update);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, CommittedDeleteSurvivesRecovery) {
    ScopedTestDir test_dir("recovery_committed_delete_root");
    const std::string db_name = "recovery_committed_delete_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto delete_lsn = AppendDelete(*db.log_mgr_, 100, begin_lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 100, delete_lsn);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, UncommittedDeleteIsUndone) {
    ScopedTestDir test_dir("recovery_uncommitted_delete_root");
    const std::string db_name = "recovery_uncommitted_delete_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendDelete(*db.log_mgr_, 100, begin_lsn, rid, rec);
        FlushLogs(*db.log_mgr_);
        db.sm_mgr_.delete_record_with_indexes("t", rid, rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, AbortedDeleteDoesNotRestoreLaterCommittedSameRowDelete) {
    ScopedTestDir test_dir("recovery_aborted_delete_same_row_root");
    const std::string db_name = "recovery_aborted_delete_same_row_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto loser_begin = AppendBegin(*db.log_mgr_, 100);
        auto loser_delete = AppendDelete(*db.log_mgr_, 100, loser_begin, rid, rec);
        AppendAbort(*db.log_mgr_, 100, loser_delete);

        auto committed_begin = AppendBegin(*db.log_mgr_, 200);
        auto committed_delete = AppendDelete(*db.log_mgr_, 200, committed_begin, rid, rec);
        AppendCommit(*db.log_mgr_, 200, committed_delete);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, UncommittedMvccDeleteTombstoneIsUndone) {
    ScopedTestDir test_dir("recovery_uncommitted_mvcc_delete_root");
    const std::string db_name = "recovery_uncommitted_mvcc_delete_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendDelete(*db.log_mgr_, 100, begin_lsn, rid, rec);
        FlushLogs(*db.log_mgr_);

        TupleMeta tombstone;
        tombstone.writer_txn_id_ = 100;
        tombstone.is_committed_ = false;
        tombstone.is_deleted_ = true;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, tombstone);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, AbortedInsertWithStaleFlushedPageIsUndone) {
    ScopedTestDir test_dir("recovery_aborted_insert_root");
    const std::string db_name = "recovery_aborted_insert_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto insert_lsn = AppendInsert(*db.log_mgr_, 100, begin_lsn, rid, rec);
        AppendAbort(*db.log_mgr_, 100, insert_lsn);
        FlushLogs(*db.log_mgr_);

        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        // 模拟 executor_insert 写下的 MVCC meta：未提交、归属本事务。
        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, meta);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

// The index repair no longer deletes and reinstalls every key the WAL mentions.
// It probes each key once and skips the delete/insert pair only when the tree
// already holds exactly the entries the pair would produce. The tests below pin
// down the cases where that condition must not hold, i.e. where a mutation is
// still mandatory, plus the case where skipping must preserve a correct entry.

// A loser update whose index write reached disk while its heap write did not.
// Both index sides need work: the new key is a stale entry, and the old key is
// missing even though the heap still holds the old row.
TEST(RecoveryManagerTest, LoserUpdateWithFlushedIndexWriteRestoresTheOldKey) {
    ScopedTestDir test_dir("recovery_loser_update_index_flushed_root");
    const std::string db_name = "recovery_loser_update_index_flushed_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        FlushLogs(*db.log_mgr_);

        // Only the index moved to the new key; the heap still holds the old row.
        auto* index = db.sm_mgr_.ihs_.at(db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
        index->delete_entry(MakeIntKey(1).data(), rid, nullptr);
        index->insert_entry(MakeIntKey(2).data(), rid, nullptr, true);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);
    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

// A committed update where the index already holds the new key and still holds
// the old one. The new key must survive the repair (the skip condition applies
// to it) while the old key must be removed (it does not).
TEST(RecoveryManagerTest, CommittedUpdateRemovesStaleKeyAndKeepsTheAlreadyWrittenOne) {
    ScopedTestDir test_dir("recovery_committed_update_stale_key_root");
    const std::string db_name = "recovery_committed_update_stale_key_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        // The new index entry reached disk but the old one was not removed, and
        // the heap page never made it out.
        auto* index = db.sm_mgr_.ihs_.at(db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
        index->insert_entry(MakeIntKey(2).data(), rid, nullptr, true);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto update_lsn = AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        AppendCommit(*db.log_mgr_, 100, update_lsn);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);
    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

// A loser insert whose index entry reached disk, followed by a committed
// transaction reusing the same RID with a different key. The loser's key must
// go and the committed key must be installed, and the tuple ownership guard
// must keep undo from deleting the committed row.
TEST(RecoveryManagerTest, LoserInsertFollowedByCommittedRidReuseKeepsOnlyTheCommittedKey) {
    ScopedTestDir test_dir("recovery_rid_reuse_root");
    const std::string db_name = "recovery_rid_reuse_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto loser_rec = MakeTuple(1, 10);
    auto committed_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        auto loser_begin = AppendBegin(*db.log_mgr_, 100);
        AppendInsert(*db.log_mgr_, 100, loser_begin, rid, loser_rec);

        auto committed_begin = AppendBegin(*db.log_mgr_, 200);
        auto committed_insert = AppendInsert(*db.log_mgr_, 200, committed_begin, rid, committed_rec);
        AppendCommit(*db.log_mgr_, 200, committed_insert);
        FlushLogs(*db.log_mgr_);

        // The loser's index entry is the only thing that reached disk.
        db.sm_mgr_.insert_record_with_indexes("t", rid, loser_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        db.sm_mgr_.fhs_.at("t")->delete_record(rid, nullptr);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);
    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

// Deduplicating the repair set must not weaken the cleanup of an index that
// somehow holds the same pair twice: the old repair removed one copy per WAL
// record that mentioned it, so the new one drains duplicates instead.
TEST(RecoveryManagerTest, DuplicateIndexEntriesForOneRidAreAllRemoved) {
    ScopedTestDir test_dir("recovery_duplicate_index_entry_root");
    const std::string db_name = "recovery_duplicate_index_entry_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        auto* index = db.sm_mgr_.ihs_.at(db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
        index->insert_entry(MakeIntKey(1).data(), rid, nullptr, true);
        std::vector<Rid> duplicated;
        ASSERT_TRUE(index->get_value(MakeIntKey(1).data(), &duplicated, nullptr));
        ASSERT_EQ(duplicated.size(), 2u);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto delete_lsn = AppendDelete(*db.log_mgr_, 100, begin_lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 100, delete_lsn);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    auto* index = db.sm_mgr_.ihs_.at(db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
    std::vector<Rid> remaining;
    index->get_value(MakeIntKey(1).data(), &remaining, nullptr);
    EXPECT_TRUE(remaining.empty());
}

// One slot written by a transaction that aborted at run time and then by a
// transaction that was still open at the crash, with the index key moving both
// times. Recovery has to roll the slot back to the value before either of them,
// and the index has to end up holding only the key that value carries. This is
// the reachable shape of "several losers on one RID": write-write conflict
// prevention means the second writer only sees the first one's rolled-back row.
TEST(RecoveryManagerTest, AbortedThenLoserUpdateOnOneSlotRollsBackToTheBaseRow) {
    ScopedTestDir test_dir("recovery_abort_then_loser_root");
    const std::string db_name = "recovery_abort_then_loser_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto base_rec = MakeTuple(1, 10);
    auto aborted_rec = MakeTuple(2, 20);
    auto loser_rec = MakeTuple(3, 30);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, base_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto aborted_begin = AppendBegin(*db.log_mgr_, 100);
        auto aborted_update = AppendUpdate(*db.log_mgr_, 100, aborted_begin, rid, base_rec, aborted_rec);
        AppendAbort(*db.log_mgr_, 100, aborted_update);

        // 100 rolled itself back at run time, so 200 reads the base row.
        auto loser_begin = AppendBegin(*db.log_mgr_, 200);
        AppendUpdate(*db.log_mgr_, 200, loser_begin, rid, base_rec, loser_rec);
        FlushLogs(*db.log_mgr_);

        // Only the open transaction's heap write and index write reached disk.
        TupleMeta meta;
        meta.writer_txn_id_ = 200;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->apply_tuple_update(rid, loser_rec.data, meta, INVALID_LSN);
        auto* index = db.sm_mgr_.ihs_.at(db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
        index->delete_entry(MakeIntKey(1).data(), rid, nullptr);
        index->insert_entry(MakeIntKey(3).data(), rid, nullptr, true);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);
    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 2, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 3, rid));
}

TEST(RecoveryManagerTest, CleanCheckpointTruncatesWalAndKeepsCommittedRows) {
    ScopedTestDir test_dir("recovery_clean_checkpoint_root");
    const std::string db_name = "recovery_clean_checkpoint_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        LockManager lock_mgr;
        TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
        CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, db.log_mgr_.get());

        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);

        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto insert_lsn = AppendInsert(*db.log_mgr_, 100, begin_lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 100, insert_lsn);
        FlushLogs(*db.log_mgr_);
        db.log_mgr_->write_restart_offset(64);

        ASSERT_GT(db.disk_.get_file_size(LOG_FILE_NAME), 0);
        ASSERT_TRUE(checkpoint_mgr.RunCleanCheckpoint());

        EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
        EXPECT_EQ(db.log_mgr_->read_restart_offset(), 0);
    }

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

// redo() must resolve every record's table from that record's own table name.
// A previous version looked the table up by the record's *position* in the redo
// pass, keeping the RID from the record but the table identity from analyze's
// parallel array. That is only correct while redo's record filter stays
// byte-for-byte identical to analyze's, and nothing enforces it: any divergence
// writes a valid RID into the wrong table's file, with no exception and nothing
// in the log. The divergence is injected here by retyping one DML record as a
// CHECKPOINT record after analyze() has already counted it, so redo sees one DML
// record fewer and positional lookup shifts by one.
TEST(RecoveryManagerTest, RedoRoutesEachRecordToTheTableItNames) {
    ScopedTestDir test_dir("recovery_redo_table_identity_root");
    const std::string db_name = "recovery_redo_table_identity_db";
    CreateRecoveryTestDb(db_name, {"t", "u"});
    const Rid rid{1, 0};
    auto t_rec = MakeTuple(1, 10);
    auto u_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, t_rec, "t");
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, u_rec, "u");
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    {
        OpenRecoveryDb db(db_name);
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();

        const auto dml_offsets = WalRecordOffsets(db.disk_, LogType::INSERT);
        ASSERT_EQ(dml_offsets.size(), 2u);
        const LogType retyped = LogType::CHECKPOINT;
        PatchWalBytes(LOG_FILE_NAME, dml_offsets[0] + OFFSET_LOG_TYPE, &retyped, sizeof(LogType));

        recovery.redo();

        // The one surviving DML record names "u", so it must land in u and
        // nowhere else.
        EXPECT_TRUE(RecordExists(db.sm_mgr_, rid, "u"));
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid, "u"), 20);
        EXPECT_FALSE(RecordExists(db.sm_mgr_, rid, "t"));
    }
}

// A DML payload that does not parse cannot be a torn tail: the record header
// already proved every payload byte is inside the file. Recovery used to treat
// it as the end of the log, silently discarding this committed insert and
// anything after it. It has to fail and keep the WAL instead.
TEST(RecoveryManagerTest, CorruptDmlPayloadFailsRecoveryInsteadOfEndingTheScan) {
    ScopedTestDir test_dir("recovery_corrupt_payload_root");
    const std::string db_name = "recovery_corrupt_payload_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);
    int64_t wal_size = 0;

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);

        // Only the payload's own image-length prefix is made impossible; the
        // header keeps its correct total_len and type.
        const auto dml_offsets = WalRecordOffsets(db.disk_, LogType::INSERT);
        ASSERT_EQ(dml_offsets.size(), 1u);
        const int impossible_image_size = 1 << 28;
        PatchWalBytes(LOG_FILE_NAME, dml_offsets[0] + OFFSET_LOG_DATA, &impossible_image_size, sizeof(int));
        wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);

    OpenRecoveryDb db(db_name);
    // Retained in full, so the next process retries from the same input rather
    // than starting up on a database missing a committed row.
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
}

// A loser whose prev_lsn chain reaches back before the scan's start offset must
// fail recovery. Walking off the end of the offset index used to just stop
// following that chain, leaving the transaction's earlier writes rolled forward
// -- a partially visible uncommitted transaction, which `final.md:342` clause 2
// forbids, and which leaves no trace at all. Unreachable while
// write_restart_offset() always writes 0; reachable the day fuzzy checkpointing
// publishes a restart offset that is not below every live transaction's first
// LSN, which is exactly what is constructed here.
TEST(RecoveryManagerTest, LoserChainReachingBeforeTheRestartOffsetFailsRecovery) {
    ScopedTestDir test_dir("recovery_restart_offset_chain_root");
    const std::string db_name = "recovery_restart_offset_chain_db";
    CreateRecoveryTestDb(db_name);
    auto loser_rec = MakeTuple(1, 10);
    auto committed_rec = MakeTuple(2, 20);
    int64_t wal_size = 0;

    {
        OpenRecoveryDb db(db_name);
        auto loser_lsn = AppendBegin(*db.log_mgr_, 100);
        loser_lsn = AppendInsert(*db.log_mgr_, 100, loser_lsn, Rid{1, 0}, loser_rec);
        FlushLogs(*db.log_mgr_);
        const int64_t checkpoint_offset = db.disk_.get_file_size(LOG_FILE_NAME);

        // A checkpoint naming the loser as still active, as a fuzzy checkpoint
        // would, published as the restart offset.
        CheckpointLogRecord checkpoint(std::unordered_map<txn_id_t, lsn_t>{{100, loser_lsn}});
        db.log_mgr_->add_log_to_buffer(&checkpoint);

        // Work after the checkpoint, so undo() runs at all.
        auto lsn = AppendBegin(*db.log_mgr_, 200);
        lsn = AppendInsert(*db.log_mgr_, 200, lsn, Rid{1, 1}, committed_rec);
        AppendCommit(*db.log_mgr_, 200, lsn);
        FlushLogs(*db.log_mgr_);
        db.log_mgr_->write_restart_offset(checkpoint_offset);
        wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);

    OpenRecoveryDb db(db_name);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
}

// The index probe returns a multiset, not a set: lookup_equal() pushes back
// every matching slot, insert_entry(allow_duplicate=true) really stores the same
// pair twice, and delete_entry() removes one copy per call. Treating it as a set
// made E = {r, r}, C = {r}, R = {r} look already correct, so the duplicate
// survived every recovery and made an index scan return the same heap row twice
// -- inflating the per-partition counts `final.md:345` cross-checks.
TEST(RecoveryManagerTest, DuplicateEntryForAStillLiveKeyIsDrainedToOne) {
    ScopedTestDir test_dir("recovery_duplicate_live_key_root");
    const std::string db_name = "recovery_duplicate_live_key_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(1, 20); // the indexed column does not move

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        auto* index = db.sm_mgr_.ihs_.at(db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"})).get();
        index->insert_entry(MakeIntKey(1).data(), rid, nullptr, true);
        ASSERT_EQ(IndexEntriesFor(db.sm_mgr_, 1).size(), 2u);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendUpdate(*db.log_mgr_, 100, lsn, rid, old_rec, new_rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    RunRecovery(db_name);
    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    const auto entries = IndexEntriesFor(db.sm_mgr_, 1);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0], rid);
}

// A well formed WAL record can still carry an image shorter than the table's
// record size, because nothing cross-checks the two. Both the fast and the
// fallback replay path copy exactly record_size bytes out of the image, so
// installing it would read past the end of the image inside the WAL buffer.
TEST(RecoveryManagerTest, WalImageShorterThanTheRecordSizeFailsRecovery) {
    ScopedTestDir test_dir("recovery_short_image_root");
    const std::string db_name = "recovery_short_image_db";
    CreateRecoveryTestDb(db_name);

    {
        OpenRecoveryDb db(db_name);
        RmRecord short_rec(4);
        memset(short_rec.data, 0, 4);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{1, 0}, short_rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);
}

// RmFileHandle::insert_record() sets the bitmap bit for rid.slot_no and memcpys
// into get_slot(rid.slot_no) with no bounds check at all, so an out-of-range
// slot number from the WAL writes outside the page. The WAL carries no checksum,
// so the RID is unvalidated external input and recovery has to bound it itself.
TEST(RecoveryManagerTest, WalRidWithAnOutOfRangeSlotFailsRecovery) {
    ScopedTestDir test_dir("recovery_bad_slot_root");
    const std::string db_name = "recovery_bad_slot_db";
    CreateRecoveryTestDb(db_name);
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{1, 100000}, rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);
}

// Same input class, the other field: insert_record() extends the file until
// num_pages passes rid.page_no, so an absurd page number turns into hundreds of
// millions of page allocations. The number of DML records in the WAL bounds how
// many unpersisted pages there can legitimately be.
TEST(RecoveryManagerTest, WalRidWithAnImpossiblePageFailsRecovery) {
    ScopedTestDir test_dir("recovery_bad_page_root");
    const std::string db_name = "recovery_bad_page_db";
    CreateRecoveryTestDb(db_name);
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{1 << 26, 0}, rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);
}

// Page 0 is the file header, never a record.
TEST(RecoveryManagerTest, WalRidNamingTheHeaderPageFailsRecovery) {
    ScopedTestDir test_dir("recovery_header_page_rid_root");
    const std::string db_name = "recovery_header_page_rid_db";
    CreateRecoveryTestDb(db_name);
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{0, 0}, rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);
}

TEST(RecoveryFaultInjectionTest, RedoProcessDeathIsRecoverable) {
    ScopedTestDir test_dir("recovery_fault_redo_root");
    const std::string db_name = "recovery_fault_redo_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto insert_lsn = AppendInsert(*db.log_mgr_, 100, begin_lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 100, insert_lsn);
        FlushLogs(*db.log_mgr_);
    }

    const int status = RunRecoveryAfterInjectedProcessDeath(db_name, "mid_recovery_redo");
#ifdef RMDB_ENABLE_FAULT_INJECTION
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 137);
#else
    (void)status;
#endif

    RunRecovery(db_name);
    RunRecovery(db_name);
    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryFaultInjectionTest, UndoProcessDeathIsRecoverable) {
    ScopedTestDir test_dir("recovery_fault_undo_root");
    const std::string db_name = "recovery_fault_undo_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendInsert(*db.log_mgr_, 100, begin_lsn, rid, rec);
        FlushLogs(*db.log_mgr_);

        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, meta);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    const int status = RunRecoveryAfterInjectedProcessDeath(db_name, "mid_recovery_undo");
#ifdef RMDB_ENABLE_FAULT_INJECTION
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 137);
#else
    (void)status;
#endif

    RunRecovery(db_name);
    RunRecovery(db_name);
    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryFaultInjectionTest, IndexRepairIsIdempotentAcrossRepeatedRecovery) {
    ScopedTestDir test_dir("recovery_fault_index_reentry_root");
    const std::string db_name = "recovery_fault_index_reentry_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeTuple(1, 10);
    auto new_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto update_lsn = AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        AppendCommit(*db.log_mgr_, 100, update_lsn);
        FlushLogs(*db.log_mgr_);
    }

    // This point is after redo/undo, incremental index repair, header writes,
    // and fdatasync of every affected data file, but before WAL truncation.
    // The next process must safely re-enter recovery.
    const int status = RunRecoveryAfterInjectedProcessDeath(db_name, "after_recovery_data_sync");
#ifdef RMDB_ENABLE_FAULT_INJECTION
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 137);
#else
    (void)status;
#endif

    RunRecovery(db_name);
    RunRecovery(db_name);
    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

// ---------------------------------------------------------------------------
// 重启后时间戳计数器的恢复。
//
// TupleMeta.commit_ts_ 持久化在数据页里，而 next_timestamp_/last_commit_ts_ 只活在
// 内存里。计数器每次启动都从 0 开始时，上一世以高 commit_ts_ 提交的行会被
// GetVisibleRecord 判成“来自未来”，而版本链已随进程消失、无从回退，于是**已提交的行
// 变得不可见**——违反 final.md:342 第 1 条。50 仓实测：恢复后 customer 只剩
// 1,488,859/1,500,000 可见，而磁盘上一行不缺。
// ---------------------------------------------------------------------------

// 只被 checkpoint 覆盖、不再出现在任何保留 WAL 里的已提交行：它的 commit_ts_ 不会被
// reset_touched_tuple_meta 归一化，所以可见性完全取决于计数器有没有被抬回去。
// 修复前这个测试在 GetVisibleRecord 处返回 nullptr。
TEST(RecoveryTimestampTest, CleanCheckpointPersistsTheTimestampCounterSoOldCommitsStayVisible) {
    ScopedTestDir test_dir("recovery_ts_counter_root");
    const std::string db_name = "recovery_ts_counter_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);
    timestamp_t persisted_commit_ts = 0;

    {
        OpenRecoveryDb db(db_name);
        LockManager lock_mgr;
        TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
        CheckpointManager checkpoint_mgr(&txn_mgr, &db.sm_mgr_, db.log_mgr_.get());

        // 把时间戳计数器推到高位。真实负载 60 秒就能推到几十万，这里 8 个事务足够让
        // commit_ts 明显大于一个从 0 重启的 read_ts。
        for (int i = 0; i < 8; ++i) {
            Transaction* txn = txn_mgr.begin(nullptr, db.log_mgr_.get(), IsolationLevel::READ_COMMITTED);
            txn_mgr.commit(txn, db.log_mgr_.get());
        }
        persisted_commit_ts = txn_mgr.get_last_commit_ts();
        ASSERT_GT(persisted_commit_ts, 0);

        // 一行已提交数据，元组头带着那个高位 commit_ts_——这正是 mark_slots_committed()
        // 在提交时写进页面、并随脏页落盘的东西。
        db.sm_mgr_.insert_record_with_indexes("t", rid, rec);
        TupleMeta committed_in_the_past;
        committed_in_the_past.commit_ts_ = persisted_commit_ts;
        committed_in_the_past.writer_txn_id_ = INVALID_TXN_ID;
        committed_in_the_past.is_committed_ = true;
        committed_in_the_past.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, committed_in_the_past);

        // clean checkpoint：脏页落盘 → 发布重启清单 → 截断 WAL。此后磁盘上没有任何日志
        // 提到这一行，恢复的归一化扫描也就不会碰它所在的页。
        ASSERT_TRUE(checkpoint_mgr.RunCleanCheckpoint());
        ASSERT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
    }

    // 重启：新的 TransactionManager，计数器从 0 开始，除非恢复把它抬回去。
    OpenRecoveryDb db(db_name);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.redo();
    recovery.undo();
    EXPECT_GT(recovery.get_recovered_next_timestamp(), persisted_commit_ts);

    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &db.sm_mgr_);
    txn_mgr.seed_counters_after_recovery(recovery.get_recovered_next_timestamp(), recovery.get_recovered_next_txn_id());
    EXPECT_GE(txn_mgr.get_last_commit_ts(), persisted_commit_ts);

    Transaction* txn = txn_mgr.begin(nullptr, db.log_mgr_.get(), IsolationLevel::READ_COMMITTED);
    int send_offset = 0;
    Context context(&lock_mgr, db.log_mgr_.get(), txn, nullptr, &send_offset, &txn_mgr);
    auto visible = GetVisibleRecord(db.sm_mgr_.fhs_.at("t").get(), rid, &context);
    ASSERT_NE(visible, nullptr) << "已提交的行在重启后必须仍然可见";
    int value = 0;
    memcpy(&value, visible->data + sizeof(int), sizeof(int));
    EXPECT_EQ(value, 10);
    txn_mgr.commit(txn, db.log_mgr_.get());
}

// 两次 checkpoint 之间被驱逐的页可能带着比清单快照更高的 commit_ts_。补齐它的是
// COMMIT 记录里的 8 字节载荷：analyze 取所有 COMMIT 的最大 commit_ts。
TEST(RecoveryTimestampTest, CommitRecordCommitTsRaisesTheCounterWithoutAnyCheckpoint) {
    ScopedTestDir test_dir("recovery_ts_wal_root");
    const std::string db_name = "recovery_ts_wal_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);
    constexpr timestamp_t kCommitTs = 4242;

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, rec);
        AppendCommitWithTs(*db.log_mgr_, 100, lsn, kCommitTs);
        FlushLogs(*db.log_mgr_);
    }

    // 没有任何 checkpoint，所以 db.restart 里没有计数器；下界只能来自 WAL。
    ASSERT_FALSE(std::filesystem::exists(std::filesystem::path(db_name) / LogManager::RESTART_FILE_NAME));
    {
        OpenRecoveryDb db(db_name);
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();
        EXPECT_EQ(recovery.get_recovered_next_timestamp(), kCommitTs + 1);
        // txn_id 同样不能重用：页上的 writer_txn_id_ 也是持久化的。
        EXPECT_EQ(recovery.get_recovered_next_txn_id(), 101);
        recovery.redo();
        recovery.undo();
    }

    // 恢复会截断 WAL，所以它必须把算出的下界发布到 db.restart，否则下一轮恢复
    // 既没有 WAL 也没有清单，计数器又回到 0。同一个崩溃状态恢复两次必须同值。
    {
        OpenRecoveryDb db(db_name);
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();
        EXPECT_EQ(recovery.get_recovered_next_timestamp(), kCommitTs + 1);
        EXPECT_EQ(recovery.get_recovered_next_txn_id(), 101);
        recovery.redo();
        recovery.undo();
    }
}

// 旧 WAL 的 COMMIT 记录没有时间戳载荷（log_tot_len_ == LOG_HEADER_SIZE），也可能是
// 手写日志的测试。此时下界退化为 0，而不是把 INVALID_TS(-1) 当成时间戳算进去。
TEST(RecoveryTimestampTest, CommitRecordWithoutCommitTsLeavesTheCounterAtZero) {
    ScopedTestDir test_dir("recovery_ts_legacy_root");
    const std::string db_name = "recovery_ts_legacy_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    {
        OpenRecoveryDb db(db_name);
        auto lsn = AppendBegin(*db.log_mgr_, 7);
        lsn = AppendInsert(*db.log_mgr_, 7, lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 7, lsn); // 不带 commit_ts
        FlushLogs(*db.log_mgr_);
    }

    OpenRecoveryDb db(db_name);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    EXPECT_EQ(recovery.get_recovered_next_timestamp(), 0);
    EXPECT_EQ(recovery.get_recovered_next_txn_id(), 8);
}
