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
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
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
    auto recovery = std::make_unique<RecoveryManager>(&db.disk_, &db.bpm_, &db.sm_mgr_);
    recovery->analyze();
    recovery->redo();
    recovery->undo();
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

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
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
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

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
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 10);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 2, rid));
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
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
}
