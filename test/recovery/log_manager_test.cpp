/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "recovery/log_manager.h"
#include "execution/executor_delete.h"
#include "execution/executor_insert.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "pager/pager.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "system/schema_manager.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rmdb;
namespace {

RmRecord MakeRecord(const std::string& text) {
    RmRecord rec(static_cast<int>(text.size()));
    memcpy(rec.data, text.data(), text.size());
    return rec;
}

void ExpectRecordEq(const RmRecord& actual, const RmRecord& expected) {
    ASSERT_EQ(actual.size, expected.size);
    EXPECT_EQ(memcmp(actual.data, expected.data, expected.size), 0);
}

std::vector<std::unique_ptr<LogRecord>> ReadAllLogs(DiskManager& disk) {
    std::vector<std::unique_ptr<LogRecord>> logs;
    int64_t offset = 0;
    const int64_t file_size = disk.get_file_size(LOG_FILE_NAME);
    while (offset + LOG_HEADER_SIZE <= file_size) {
        std::vector<char> header(LOG_HEADER_SIZE);
        if (disk.read_log(header.data(), LOG_HEADER_SIZE, offset) != LOG_HEADER_SIZE) {
            break;
        }
        LogRecord log_header;
        log_header.deserialize(header.data());
        if (log_header.log_tot_len_ < LOG_HEADER_SIZE ||
            offset + static_cast<int64_t>(log_header.log_tot_len_) > file_size) {
            break;
        }
        std::vector<char> buf(log_header.log_tot_len_);
        if (disk.read_log(buf.data(), static_cast<int>(buf.size()), offset) != static_cast<int>(buf.size())) {
            ADD_FAILURE() << "short log read at offset " << offset;
            return logs;
        }
        auto decoded = DeserializeLogRecord(buf.data(), static_cast<int>(buf.size()));
        if (decoded == nullptr) {
            ADD_FAILURE() << "failed to decode log at offset " << offset;
            return logs;
        }
        logs.emplace_back(std::move(decoded));
        offset += static_cast<int64_t>(log_header.log_tot_len_);
    }
    return logs;
}

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

} // namespace

TEST(LogRecordTest, InsertRoundTripKeepsPayload) {
    auto rec = MakeRecord("abc123");
    Rid rid{3, 7};
    InsertLogRecord log(42, rec, rid, "warehouse");
    log.lsn_ = 9;
    log.prev_lsn_ = 8;

    std::vector<char> buf(log.log_tot_len_);
    log.serialize(buf.data());

    InsertLogRecord decoded;
    decoded.deserialize(buf.data());

    EXPECT_EQ(decoded.log_type_, LogType::INSERT);
    EXPECT_EQ(decoded.log_tid_, 42);
    EXPECT_EQ(decoded.lsn_, 9);
    EXPECT_EQ(decoded.prev_lsn_, 8);
    EXPECT_EQ(decoded.rid_, rid);
    EXPECT_EQ(decoded.table_name_, "warehouse");
    ExpectRecordEq(decoded.insert_value_, rec);
}

TEST(LogRecordTest, DeleteRoundTripKeepsOldRecord) {
    auto rec = MakeRecord("deleted");
    Rid rid{5, 11};
    DeleteLogRecord log(77, rec, rid, "orders");
    log.lsn_ = 12;
    log.prev_lsn_ = 10;

    std::vector<char> buf(log.log_tot_len_);
    log.serialize(buf.data());

    DeleteLogRecord decoded;
    decoded.deserialize(buf.data());

    EXPECT_EQ(decoded.log_type_, LogType::DELETE);
    EXPECT_EQ(decoded.log_tid_, 77);
    EXPECT_EQ(decoded.lsn_, 12);
    EXPECT_EQ(decoded.prev_lsn_, 10);
    EXPECT_EQ(decoded.rid_, rid);
    EXPECT_EQ(decoded.table_name_, "orders");
    ExpectRecordEq(decoded.delete_value_, rec);
}

TEST(LogRecordTest, UpdateRoundTripKeepsOldAndNewRecord) {
    auto old_rec = MakeRecord("old-value");
    auto new_rec = MakeRecord("new-value");
    Rid rid{8, 13};
    UpdateLogRecord log(88, old_rec, new_rec, rid, "stock");
    log.lsn_ = 19;
    log.prev_lsn_ = 18;

    std::vector<char> buf(log.log_tot_len_);
    log.serialize(buf.data());

    UpdateLogRecord decoded;
    decoded.deserialize(buf.data());

    EXPECT_EQ(decoded.log_type_, LogType::UPDATE);
    EXPECT_EQ(decoded.log_tid_, 88);
    EXPECT_EQ(decoded.lsn_, 19);
    EXPECT_EQ(decoded.prev_lsn_, 18);
    EXPECT_EQ(decoded.rid_, rid);
    EXPECT_EQ(decoded.table_name_, "stock");
    ExpectRecordEq(decoded.old_value_, old_rec);
    ExpectRecordEq(decoded.new_value_, new_rec);
}

TEST(LogRecordTest, CheckpointRoundTripKeepsActiveTxnTable) {
    std::unordered_map<txn_id_t, lsn_t> active_txns{{1, 10}, {2, 20}};
    CheckpointLogRecord log(active_txns);
    log.lsn_ = 21;

    std::vector<char> buf(log.log_tot_len_);
    log.serialize(buf.data());

    CheckpointLogRecord decoded;
    decoded.deserialize(buf.data());

    EXPECT_EQ(decoded.log_type_, LogType::CHECKPOINT);
    EXPECT_EQ(decoded.lsn_, 21);
    EXPECT_EQ(decoded.active_txns_, active_txns);
}

TEST(LogRecordTest, DeserializeLogRecordConstructsDerivedTypeFromHeader) {
    auto rec = MakeRecord("factory");
    Rid rid{2, 4};
    InsertLogRecord log(9, rec, rid, "customer");

    std::vector<char> buf(log.log_tot_len_);
    log.serialize(buf.data());

    std::unique_ptr<LogRecord> decoded = DeserializeLogRecord(buf.data(), static_cast<int>(buf.size()));
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->log_type_, LogType::INSERT);

    auto* insert = dynamic_cast<InsertLogRecord*>(decoded.get());
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->rid_, rid);
    EXPECT_EQ(insert->table_name_, "customer");
    ExpectRecordEq(insert->insert_value_, rec);
}

TEST(LogManagerTest, AppendFlushAndReadBack) {
    ScopedTestDir test_dir("log_manager_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    BeginLogRecord begin(100);
    lsn_t begin_lsn = log_mgr.add_log_to_buffer(&begin);
    CommitLogRecord commit(100);
    commit.prev_lsn_ = begin_lsn;
    lsn_t commit_lsn = log_mgr.add_log_to_buffer(&commit);
    log_mgr.flush_log_to_disk();

    EXPECT_EQ(begin_lsn, 0);
    EXPECT_EQ(commit_lsn, 1);
    EXPECT_EQ(log_mgr.get_persist_lsn(), commit_lsn);
    EXPECT_GE(disk.get_file_size(LOG_FILE_NAME), LOG_HEADER_SIZE * 2);

    std::vector<char> first(LOG_HEADER_SIZE);
    ASSERT_EQ(disk.read_log(first.data(), static_cast<int>(first.size()), 0), LOG_HEADER_SIZE);
    auto decoded = DeserializeLogRecord(first.data(), static_cast<int>(first.size()));
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->log_type_, LogType::BEGIN);
    EXPECT_EQ(decoded->lsn_, begin_lsn);
}

TEST(LogManagerTest, RestartOffsetRoundTrip) {
    ScopedTestDir test_dir("log_manager_restart_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    EXPECT_EQ(log_mgr.read_restart_offset(), 0);
    log_mgr.write_restart_offset(128);
    EXPECT_EQ(log_mgr.read_restart_offset(), 128);
}

TEST(LogManagerTest, DiskManagerReportsFileSizePastTwoGb) {
    ScopedTestDir test_dir("log_manager_large_file_size_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);

    constexpr int64_t kLargeOffset = 2LL * 1024 * 1024 * 1024 + 4096;
    ASSERT_EQ(truncate(LOG_FILE_NAME.c_str(), static_cast<off_t>(kLargeOffset)), 0);

    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), kLargeOffset);
}

TEST(LogManagerTest, DiskManagerWritesLogPastTwoGb) {
    ScopedTestDir test_dir("log_manager_large_log_write_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    int fd = disk.open_file(LOG_FILE_NAME);

    constexpr int64_t kLargeOffset = 2LL * 1024 * 1024 * 1024 + 4096;
    disk.SetLogFd(fd);
    disk.SetLogOffset(kLargeOffset);

    char record[] = {'x', 'y', 'z'};
    ASSERT_NO_THROW(disk.write_log(record, static_cast<int>(sizeof(record))));
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), kLargeOffset + static_cast<int64_t>(sizeof(record)));
}

TEST(LogManagerTest, RestartOffsetRoundTripPastTwoGb) {
    ScopedTestDir test_dir("log_manager_large_restart_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    constexpr int64_t kLargeOffset = 2LL * 1024 * 1024 * 1024 + 4096;
    log_mgr.write_restart_offset(kLargeOffset);
    EXPECT_EQ(log_mgr.read_restart_offset(), kLargeOffset);
}

TEST(LogManagerTest, InvalidRestartOffsetReturnsZero) {
    ScopedTestDir test_dir("log_manager_invalid_restart_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    {
        std::ofstream restart(LogManager::RESTART_FILE_NAME, std::ios::trunc);
        restart << "not-an-offset";
    }
    EXPECT_EQ(log_mgr.read_restart_offset(), 0);

    {
        std::ofstream restart(LogManager::RESTART_FILE_NAME, std::ios::trunc);
        restart << -4;
    }
    EXPECT_EQ(log_mgr.read_restart_offset(), 0);
}

TEST(LogManagerTest, TransactionBeginCommitLogPrevLsn) {
    ScopedTestDir test_dir("transaction_log_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    BufferPoolManager bpm(32, &disk);
    RmManager rm_mgr(&disk, &bpm, nullptr);
    IxManager ix_mgr(&disk, &bpm, nullptr);
    SchemaManager schema_mgr(&disk, &bpm, &rm_mgr, &ix_mgr, nullptr);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &schema_mgr);
    LogManager log_mgr(&disk);

    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr);
    txn_mgr.commit(txn, &log_mgr);

    auto logs = ReadAllLogs(disk);
    ASSERT_GE(logs.size(), 2);
    EXPECT_EQ(logs[0]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[1]->log_type_, LogType::COMMIT);
    EXPECT_EQ(logs[1]->prev_lsn_, logs[0]->lsn_);
}

TEST(LogManagerTest, ExecutorDmlWritesWalSequence) {
    ScopedTestDir test_dir("executor_dml_log_test_root");
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    LogManager log_mgr(&disk);
    rmdb::pager::Pager pager(&bpm, &log_mgr);
    bpm.set_wal_guard(&pager);
    RmManager rm_mgr(&disk, &bpm, &pager);
    IxManager ix_mgr(&disk, &bpm, &pager);
    SchemaManager schema_mgr(&disk, &bpm, &rm_mgr, &ix_mgr, &pager);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &schema_mgr);

    schema_mgr.create_db("executor_dml_log_test_db");
    schema_mgr.open_db("executor_dml_log_test_db");
    rmdb::access::TableWriteService write_svc(&schema_mgr, &lock_mgr, &log_mgr, &txn_mgr);

    schema_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
    schema_mgr.create_index("t", {"id"}, nullptr);

    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr);
    Context context(&lock_mgr, &log_mgr, txn, nullptr, &const_offset, &txn_mgr);

    Value id;
    id.set_int(1);
    Value v;
    v.set_int(10);
    InsertExecutor insert_executor(&schema_mgr, &write_svc, "t", {id, v}, &context);
    insert_executor.Next();
    Rid rid = insert_executor.rid();

    Value new_v;
    new_v.set_int(20);
    SetClause set_clause{{"t", "v"}, new_v, false, {}, UpdateOp::ASSIGNMENT};
    UpdateExecutor update_executor(&schema_mgr, &write_svc, "t", {set_clause}, {}, {rid}, &context);
    update_executor.Next();

    DeleteExecutor delete_executor(&schema_mgr, &write_svc, "t", {}, {rid}, &context);
    delete_executor.Next();

    txn_mgr.commit(txn, &log_mgr);

    auto logs = ReadAllLogs(disk);
    ASSERT_GE(logs.size(), 5);
    EXPECT_EQ(logs[0]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[1]->log_type_, LogType::INSERT);
    EXPECT_EQ(logs[2]->log_type_, LogType::UPDATE);
    EXPECT_EQ(logs[3]->log_type_, LogType::DELETE);
    EXPECT_EQ(logs[4]->log_type_, LogType::COMMIT);

    auto* update_log = dynamic_cast<UpdateLogRecord*>(logs[2].get());
    ASSERT_NE(update_log, nullptr);
    ASSERT_EQ(update_log->old_value_.size, update_log->new_value_.size);
    EXPECT_NE(memcmp(update_log->old_value_.data, update_log->new_value_.data, update_log->old_value_.size), 0);

    auto* delete_log = dynamic_cast<DeleteLogRecord*>(logs[3].get());
    ASSERT_NE(delete_log, nullptr);
    ASSERT_EQ(delete_log->delete_value_.size, update_log->new_value_.size);
    EXPECT_EQ(memcmp(delete_log->delete_value_.data, update_log->new_value_.data, delete_log->delete_value_.size), 0);

    schema_mgr.close_db();
}
