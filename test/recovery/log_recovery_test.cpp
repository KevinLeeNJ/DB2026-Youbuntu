/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

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
    RmRecord rec(static_cast<int>(sizeof(int) * 2));
    memcpy(rec.data, &id, sizeof(int));
    memcpy(rec.data + sizeof(int), &value, sizeof(int));
    return rec;
}

std::vector<char> MakeIntKey(int value) {
    std::vector<char> key(sizeof(int));
    memcpy(key.data(), &value, sizeof(int));
    return key;
}

void CreateRecoveryTestDb(const std::string& db_name) {
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);

    sm_mgr.create_db(db_name);
    sm_mgr.open_db(db_name);
    sm_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
    sm_mgr.create_index("t", {"id"}, nullptr);
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

lsn_t AppendInsert(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn, const Rid& rid, RmRecord& rec) {
    Rid log_rid = rid;
    InsertLogRecord insert(txn_id, rec, log_rid, "t");
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

void AppendAbort(LogManager& log_mgr, txn_id_t txn_id, lsn_t prev_lsn) {
    AbortLogRecord abort(txn_id);
    abort.prev_lsn_ = prev_lsn;
    log_mgr.add_log_to_buffer(&abort);
}

void FlushLogs(LogManager& log_mgr) {
    log_mgr.flush_log_to_disk();
}

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

bool RecordExists(SmManager& sm_mgr, const Rid& rid) {
    try {
        auto* fh = sm_mgr.fhs_.at("t").get();
        if (rid.page_no < 0 || rid.page_no >= fh->get_file_hdr().num_pages) {
            return false;
        }
        return fh->is_record(rid);
    } catch (const std::exception&) {
        return false;
    }
}

int RecordValue(SmManager& sm_mgr, const Rid& rid) {
    auto record = sm_mgr.fhs_.at("t")->get_record(rid, nullptr);
    int value = 0;
    memcpy(&value, record->data + sizeof(int), sizeof(int));
    return value;
}

bool IndexPointsTo(SmManager& sm_mgr, int key, const Rid& rid) {
    auto* index = sm_mgr.ihs_.at(sm_mgr.get_ix_manager()->get_index_name("t", {"id"})).get();
    std::vector<Rid> result;
    if (!index->get_value(MakeIntKey(key).data(), &result, nullptr)) {
        return false;
    }
    return result.size() == 1 && result[0] == rid;
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
