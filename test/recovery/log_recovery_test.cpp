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
#include "execution/executor_insert.h"
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

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <sys/wait.h>
#include <string>
#include <thread>
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

RmRecord MakeWideTuple(int id, char fill) {
    constexpr int kPaddingBytes = 128;
    RmRecord rec(static_cast<int>(sizeof(int)) + kPaddingBytes + null_bitmap_bytes(2));
    memset(rec.data, fill, static_cast<size_t>(rec.size));
    memcpy(rec.data, &id, sizeof(int));
    rec.data[rec.size - 1] = 0;
    return rec;
}

RmRecord MakeSegmentTuple(int id, char fill) {
    constexpr int kPayloadBytes = 2000;
    RmRecord rec(static_cast<int>(sizeof(int)) + kPayloadBytes + null_bitmap_bytes(2));
    memset(rec.data, fill, static_cast<size_t>(rec.size));
    memcpy(rec.data, &id, sizeof(int));
    rec.data[rec.size - 1] = 0;
    return rec;
}

std::vector<char> MakeIntKey(int value) {
    std::vector<char> key(sizeof(int));
    memcpy(key.data(), &value, sizeof(int));
    return key;
}

void CreateRecoveryTestDb(const std::string& db_name, const std::vector<std::string>& table_names = {"t"},
                          bool create_indexes = true) {
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);

    sm_mgr.create_db(db_name);
    sm_mgr.open_db(db_name);
    for (const auto& table_name : table_names) {
        sm_mgr.create_table(table_name, {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
        if (create_indexes) {
            sm_mgr.create_index(table_name, {"id"}, nullptr);
        }
    }
    sm_mgr.close_db();
}

void CreateWideRecoveryTestDb(const std::string& db_name) {
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);

    sm_mgr.create_db(db_name);
    sm_mgr.open_db(db_name);
    sm_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"padding", TYPE_STRING, 128}}, nullptr);
    sm_mgr.create_index("t", {"id"}, nullptr);
    sm_mgr.close_db();
}

void CreateSegmentRecoveryTestDb(const std::string& db_name) {
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
    sm_mgr.create_db(db_name);
    sm_mgr.open_db(db_name);
    sm_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"payload", TYPE_STRING, 2000}}, nullptr);
    sm_mgr.create_index("t", {"id"}, nullptr);
    sm_mgr.close_db();
}

class OpenRecoveryDb {
public:
    explicit OpenRecoveryDb(const std::string& db_name, size_t pool_size = 64)
        : bpm_(pool_size, &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_),
          sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_), log_mgr_(std::make_unique<LogManager>(&disk_)) {
        sm_mgr_.open_db(db_name);
        bpm_.set_log_manager(log_mgr_.get());
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

class ScopedEnvironmentValue {
public:
    ScopedEnvironmentValue(const char* name, const char* value) : name_(name) {
        const char* previous = getenv(name_);
        had_previous_ = previous != nullptr;
        if (had_previous_)
            previous_ = previous;
        const int result = value == nullptr ? unsetenv(name_) : setenv(name_, value, 1);
        if (result != 0)
            throw std::runtime_error("could not set test environment value");
    }

    ~ScopedEnvironmentValue() {
        if (had_previous_) {
            (void)setenv(name_, previous_.c_str(), 1);
        } else {
            (void)unsetenv(name_);
        }
    }

    ScopedEnvironmentValue(const ScopedEnvironmentValue&) = delete;
    ScopedEnvironmentValue& operator=(const ScopedEnvironmentValue&) = delete;

private:
    const char* name_;
    bool had_previous_{false};
    std::string previous_;
};

class ScopedThrowFaultPoint {
public:
    explicit ScopedThrowFaultPoint(const char* point)
        : point_("RMDB_FAULT_POINT", point), action_("RMDB_FAULT_ACTION", "throw"), skip_("RMDB_FAULT_SKIP", nullptr) {
        FaultInjector::ResetForTest();
    }

    ~ScopedThrowFaultPoint() {
        FaultInjector::ResetForTest();
    }

private:
    ScopedEnvironmentValue point_;
    ScopedEnvironmentValue action_;
    ScopedEnvironmentValue skip_;
};

class ScopedBpmFinalizeHooks {
public:
    explicit ScopedBpmFinalizeHooks(BufferPoolManager* bpm) : bpm_(bpm) {}
    ~ScopedBpmFinalizeHooks() {
        bpm_->set_replacement_io_test_hook({});
        bpm_->set_load_io_test_hook({});
        bpm_->set_frame_operation_gate_test_hook({});
    }

private:
    BufferPoolManager* bpm_;
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
                   RmRecord& new_rec, bool index_keys_unchanged = false) {
    Rid log_rid = rid;
    UpdateLogRecord update(txn_id, old_rec, new_rec, log_rid, "t", index_keys_unchanged);
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

// Mirrors the direct RecoveryManager test contract. Production uses the
// prepare/analyze/finalize sequence in rmdb.cpp; these fixtures deliberately
// retain their explicitly constructed restart-manifest semantics.
void RunRecovery(const std::string& db_name) {
    OpenRecoveryDb db(db_name);
    db.bpm_.set_log_manager(db.log_mgr_.get());
    auto recovery = std::make_unique<RecoveryManager>(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery->analyze();
    recovery->redo();
    recovery->undo();
}

void RunPreparedRecovery(const std::string& db_name, size_t pool_size = 64) {
    OpenRecoveryDb db(db_name, pool_size);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.prepare_pages_for_redo();
    recovery.redo();
    recovery.undo();
}

void CreatePhysicalFinalizeSeed(const std::string& db_name) {
    CreateRecoveryTestDb(db_name, {"t"}, false);
    OpenRecoveryDb db(db_name);
    db.sm_mgr_.create_index("t", {"id"}, nullptr);
    db.sm_mgr_.create_index("t", {"v"}, nullptr);
    RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
    for (page_id_t page_no = 1; page_no <= 4; ++page_no) {
        RmPageHandle page = file_handle->create_new_page_handle();
        if (page.page->get_page_id().page_no != page_no || !db.bpm_.unpin_page(page.page->get_page_id(), true)) {
            throw std::runtime_error("could not create physical page-finalize seed page");
        }
    }
    if (!db.bpm_.flush_all_pages(file_handle->GetFd())) {
        throw std::runtime_error("could not flush physical page-finalize seed pages");
    }
    lsn_t lsn = AppendBegin(*db.log_mgr_, 800);
    for (page_id_t page_no = 1; page_no <= 2; ++page_no) {
        auto record = MakeTuple(800 + page_no, 8000 + page_no);
        lsn = AppendInsert(*db.log_mgr_, 800, lsn, Rid{page_no, 0}, record);
    }
    AppendCommit(*db.log_mgr_, 800, lsn);
    FlushLogs(*db.log_mgr_);
}

void InstallDirtyFinalizeVictims(OpenRecoveryDb& db) {
    RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
    if (!db.bpm_.flush_all_pages(file_handle->GetFd())) {
        throw std::runtime_error("could not flush physical finalize targets");
    }
    Page* first = db.bpm_.fetch_page(PageId{file_handle->GetFd(), 3});
    if (first == nullptr)
        throw std::runtime_error("could not pin first dirty finalize victim");
    Page* second = db.bpm_.fetch_page(PageId{file_handle->GetFd(), 4});
    if (second == nullptr) {
        (void)db.bpm_.unpin_page(first->get_page_id(), false);
        throw std::runtime_error("could not pin second dirty finalize victim");
    }
    {
        std::unique_lock first_lock(first->latch());
        BufferPoolManager::mark_dirty_locked(first);
    }
    {
        std::unique_lock second_lock(second->latch());
        BufferPoolManager::mark_dirty_locked(second);
    }
    if (!db.bpm_.unpin_page(first->get_page_id(), false) || !db.bpm_.unpin_page(second->get_page_id(), false)) {
        throw std::runtime_error("could not release dirty finalize victims");
    }
}

int RunRecoveryAfterInjectedProcessDeath(const std::string& db_name, const char* point,
                                         bool prepare_pages_for_redo = false, int skip_count = 0,
                                         size_t pool_size = 64) {
#ifndef RMDB_ENABLE_FAULT_INJECTION
    (void)db_name;
    (void)point;
    (void)prepare_pages_for_redo;
    (void)skip_count;
    (void)pool_size;
    return -1;
#else
    // _exit(137) is the FaultInjector's deterministic crash action. Using it
    // avoids racing a SIGKILL against the block action, which has no
    // ready-at-point notification. The child still disappears without running
    // recovery cleanup, so the following process exercises the same replay.
    const std::string skip = std::to_string(skip_count);
    if (setenv("RMDB_FAULT_POINT", point, 1) != 0 || setenv("RMDB_FAULT_ACTION", "_exit", 1) != 0 ||
        (skip_count > 0 ? setenv("RMDB_FAULT_SKIP", skip.c_str(), 1) : unsetenv("RMDB_FAULT_SKIP")) != 0) {
        return -1;
    }
    FaultInjector::ResetForTest();

    const pid_t child = fork();
    if (child == -1) {
        unsetenv("RMDB_FAULT_POINT");
        unsetenv("RMDB_FAULT_ACTION");
        return -1;
    }
    if (child == 0) {
        if (prepare_pages_for_redo) {
            RunPreparedRecovery(db_name, pool_size);
        } else {
            RunRecovery(db_name);
        }
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

int RecordId(SmManager& sm_mgr, const Rid& rid, const std::string& table_name = "t") {
    auto record = sm_mgr.fhs_.at(table_name)->get_record(rid, nullptr);
    return read_unaligned<int>(record->data);
}

std::vector<Rid> IndexEntriesFor(SmManager& sm_mgr, int key, const std::string& table_name = "t") {
    auto* index = sm_mgr.ihs_.at(sm_mgr.get_ix_manager()->get_index_name(table_name, {"id"})).get();
    std::vector<Rid> result;
    index->get_value(MakeIntKey(key).data(), &result, nullptr);
    return result;
}

std::vector<Rid> IndexEntriesForColumn(SmManager& sm_mgr, const std::string& column, int key,
                                       const std::string& table_name = "t") {
    auto* index = sm_mgr.ihs_.at(sm_mgr.get_ix_manager()->get_index_name(table_name, {column})).get();
    std::vector<Rid> result;
    index->get_value(MakeIntKey(key).data(), &result, nullptr);
    return result;
}

bool IndexColumnPointsTo(SmManager& sm_mgr, const std::string& column, int key, const Rid& rid,
                         const std::string& table_name = "t") {
    const auto result = IndexEntriesForColumn(sm_mgr, column, key, table_name);
    return result.size() == 1 && result[0] == rid;
}

bool IndexPointsTo(SmManager& sm_mgr, int key, const Rid& rid, const std::string& table_name = "t") {
    const auto result = IndexEntriesFor(sm_mgr, key, table_name);
    return result.size() == 1 && result[0] == rid;
}

Rid PrepareSinglePageIndexSmo(const std::string& db_name, int key, const Rid& requested_rid,
                              bool renew_binding_after_smo, bool add_loser_insert = false, int smo_record_count = 1) {
    OpenRecoveryDb db(db_name);
    db.bpm_.set_log_manager(db.log_mgr_.get());
    const std::string index_name = db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"});
    IxIndexHandle* index = db.sm_mgr_.ihs_.at(index_name).get();
    const int fd = index->GetFd();
    constexpr page_id_t root_page_no = IX_INIT_ROOT_PAGE;

    std::array<char, PAGE_SIZE> before{};
    std::array<char, PAGE_SIZE> header{};
    db.disk_.read_page(fd, root_page_no, before.data(), PAGE_SIZE);
    db.disk_.read_page(fd, IX_FILE_HDR_PAGE, header.data(), PAGE_SIZE);

    Rid rid = requested_rid;
    if (add_loser_insert) {
        auto record = MakeTuple(key, key * 10);
        rid = db.sm_mgr_.fhs_.at("t")->insert_record(record.data, nullptr);
        const txn_id_t txn_id = 901;
        const lsn_t begin_lsn = AppendBegin(*db.log_mgr_, txn_id);
        AppendInsert(*db.log_mgr_, txn_id, begin_lsn, rid, record);
        TupleMeta meta;
        meta.writer_txn_id_ = txn_id;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, meta);
    }
    EXPECT_GT(smo_record_count, 0);
    lsn_t smo_lsn = INVALID_LSN;
    for (int i = 0; i < smo_record_count; ++i) {
        index->insert_entry(MakeIntKey(key + i).data(), rid, IndexWriteWalContext::TestNoWal());
        Page* root = db.bpm_.fetch_page(PageId{fd, root_page_no});
        if (root == nullptr) {
            ADD_FAILURE() << "could not fetch root while preparing INDEX_SMO";
            return rid;
        }
        IndexSmoWalData smo;
        smo.index_file_name = index_name;
        smo.pages.resize(1);
        smo.pages[0].page_no = root_page_no;
        {
            std::shared_lock page_lock{root->latch()};
            std::memcpy(smo.pages[0].bytes.data(), root->get_data(), PAGE_SIZE);
        }
        EXPECT_TRUE(db.bpm_.unpin_page(PageId{fd, root_page_no}, false));
        smo.header = header;
        smo_lsn = db.log_mgr_->append_index_smo(smo);
    }
    db.log_mgr_->flush_log_to_disk_up_to_durable(smo_lsn);
    if (renew_binding_after_smo) {
        db.bpm_.set_log_manager(db.log_mgr_.get());
        db.sm_mgr_.drop_index("t", {"id"}, nullptr);
        db.sm_mgr_.create_index("t", {"id"}, nullptr);
        return rid;
    }

    // Model a crash before any page publication: retain only the pre-SMO page
    // on disk. flush_page first clears the in-memory dirty bit so close_db
    // cannot overwrite this deliberate old image.
    EXPECT_TRUE(db.bpm_.flush_page(PageId{fd, root_page_no}));
    db.disk_.write_page(fd, root_page_no, before.data(), PAGE_SIZE);
    return rid;
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

void CorruptIndexSmoChecksum(DiskManager& disk, int64_t record_offset) {
    std::array<char, LOG_HEADER_SIZE> header{};
    ASSERT_EQ(disk.read_log_chunk(header.data(), LOG_HEADER_SIZE, record_offset), LOG_HEADER_SIZE);
    const uint32_t total_length = read_unaligned<uint32_t>(header.data() + OFFSET_LOG_TOT_LEN);
    ASSERT_GT(total_length, LOG_HEADER_SIZE + sizeof(uint32_t));
    const uint32_t corrupt_checksum = 0;
    PatchWalBytes(LOG_FILE_NAME, record_offset + total_length - static_cast<int64_t>(sizeof(uint32_t)),
                  &corrupt_checksum, sizeof(corrupt_checksum));
}

} // namespace

TEST(RecoveryManagerTest, IndexSmoOnlyWalRestoresTheAfterImage) {
    ScopedTestDir test_dir("recovery_index_smo_only_root");
    const std::string db_name = "recovery_index_smo_only_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 17};
    PrepareSinglePageIndexSmo(db_name, 17, rid, false);

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 17, rid));
}

TEST(RecoveryManagerTest, UnpublishedCheckpointCutDoesNotInvalidateEarlierIndexSmo) {
    ScopedTestDir test_dir("recovery_unpublished_checkpoint_cut_root");
    const std::string db_name = "recovery_unpublished_checkpoint_cut_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 18};
    PrepareSinglePageIndexSmo(db_name, 18, rid, false);

    {
        OpenRecoveryDb db(db_name);
        db.log_mgr_->initialize_from_existing_log();
        const std::string index_name = db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"});
        const CheckpointWalCut cut = db.log_mgr_->create_checkpoint_wal_cut({index_name});
        db.log_mgr_->sync_checkpoint_wal_cut(cut);
        ASSERT_EQ(cut.index_bindings.size(), 1U);
        // Deliberately do not publish cut.checkpoint_offset. Recovery must use
        // the old boundary and still accept the earlier same-generation SMO.
        EXPECT_EQ(db.log_mgr_->read_restart_offset(), 0);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 18, rid));
}

TEST(RecoveryManagerTest, MultipleIndexSmoRecordsPrepareEachIndexOnlyOnce) {
    ScopedTestDir test_dir("recovery_index_smo_prepare_once_root");
    const std::string db_name = "recovery_index_smo_prepare_once_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 19};
    PrepareSinglePageIndexSmo(db_name, 19, rid, false, false, 3);

    {
        OpenRecoveryDb db(db_name);
        EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 21, rid));
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();
        recovery.redo();
        EXPECT_EQ(recovery.get_index_smo_prepare_count(), 1u);
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 19, rid));
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 21, rid));
    }

    OpenRecoveryDb db(db_name);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 19, rid));
}

TEST(RecoveryManagerTest, InvalidWorkerConfigurationJoinsConcurrentIndexSmoBeforeThrowing) {
    ScopedTestDir test_dir("recovery_invalid_workers_with_smo_root");
    const std::string db_name = "recovery_invalid_workers_with_smo_db";
    CreateRecoveryTestDb(db_name);
    PrepareSinglePageIndexSmo(db_name, 29, Rid{1, 29}, false, true);

    OpenRecoveryDb db(db_name);
    const int64_t wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
    ASSERT_GT(wal_size, 0);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    struct EnvGuard {
        EnvGuard() {
            const char* value = getenv("RMDB_RECOVERY_WORKERS");
            if (value != nullptr) {
                had_previous = true;
                previous = value;
            }
        }
        ~EnvGuard() {
            if (had_previous) {
                setenv("RMDB_RECOVERY_WORKERS", previous.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
        bool had_previous{false};
        std::string previous;
    } env_guard;
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "invalid", 1), 0);
    EXPECT_THROW(recovery.redo(), InternalError);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
}

TEST(RecoveryManagerTest, InterleavedIndexSmoCoalescesEachIndexIndependently) {
    ScopedTestDir test_dir("recovery_index_smo_interleaved_root");
    const std::string db_name = "recovery_index_smo_interleaved_db";
    CreateRecoveryTestDb(db_name, {"t", "u"});

    {
        OpenRecoveryDb db(db_name);
        struct IndexBeforeImage {
            int fd;
            std::array<char, PAGE_SIZE> root;
        };
        std::vector<IndexBeforeImage> before_images;
        const auto append_root_smo = [&](const std::string& table_name, int key, const Rid& rid) {
            const std::string index_name = db.sm_mgr_.get_ix_manager()->get_index_name(table_name, {"id"});
            IxIndexHandle* index = db.sm_mgr_.ihs_.at(index_name).get();
            const int fd = index->GetFd();
            std::array<char, PAGE_SIZE> before{};
            std::array<char, PAGE_SIZE> header{};
            db.disk_.read_page(fd, IX_INIT_ROOT_PAGE, before.data(), PAGE_SIZE);
            db.disk_.read_page(fd, IX_FILE_HDR_PAGE, header.data(), PAGE_SIZE);
            before_images.push_back(IndexBeforeImage{fd, before});
            index->insert_entry(MakeIntKey(key).data(), rid, IndexWriteWalContext::TestNoWal());
            Page* root = db.bpm_.fetch_page(PageId{fd, IX_INIT_ROOT_PAGE});
            if (root == nullptr) {
                ADD_FAILURE() << "could not fetch root while preparing interleaved INDEX_SMO";
                return INVALID_LSN;
            }
            IndexSmoWalData smo;
            smo.index_file_name = index_name;
            smo.pages.resize(1);
            smo.pages[0].page_no = IX_INIT_ROOT_PAGE;
            {
                std::shared_lock page_lock{root->latch()};
                memcpy(smo.pages[0].bytes.data(), root->get_data(), PAGE_SIZE);
            }
            if (!db.bpm_.unpin_page(PageId{fd, IX_INIT_ROOT_PAGE}, false)) {
                ADD_FAILURE() << "could not unpin root while preparing interleaved INDEX_SMO";
                return INVALID_LSN;
            }
            smo.header = header;
            return db.log_mgr_->append_index_smo(smo);
        };

        const Rid t_first{1, 71};
        const Rid u_first{1, 72};
        const Rid t_second{1, 73};
        const Rid u_second{1, 74};
        const lsn_t t_first_lsn = append_root_smo("t", 71, t_first);
        const lsn_t u_first_lsn = append_root_smo("u", 72, u_first);
        const lsn_t t_second_lsn = append_root_smo("t", 73, t_second);
        const lsn_t u_second_lsn = append_root_smo("u", 74, u_second);
        ASSERT_LT(t_first_lsn, u_first_lsn);
        ASSERT_LT(u_first_lsn, t_second_lsn);
        ASSERT_LT(t_second_lsn, u_second_lsn);
        db.log_mgr_->flush_log_to_disk_up_to_durable(u_second_lsn);
        for (const IndexBeforeImage& before : before_images) {
            ASSERT_TRUE(db.bpm_.flush_page(PageId{before.fd, IX_INIT_ROOT_PAGE}));
            db.disk_.write_page(before.fd, IX_INIT_ROOT_PAGE, before.root.data(), PAGE_SIZE);
        }

        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();
        recovery.redo();
        EXPECT_EQ(recovery.get_index_smo_prepare_count(), 2u);
    }

    OpenRecoveryDb verified(db_name);
    EXPECT_TRUE(IndexPointsTo(verified.sm_mgr_, 71, Rid{1, 71}, "t"));
    EXPECT_TRUE(IndexPointsTo(verified.sm_mgr_, 73, Rid{1, 73}, "t"));
    EXPECT_TRUE(IndexPointsTo(verified.sm_mgr_, 72, Rid{1, 72}, "u"));
    EXPECT_TRUE(IndexPointsTo(verified.sm_mgr_, 74, Rid{1, 74}, "u"));
}

TEST(RecoveryManagerTest, IndexSmoKeepsOlderUniquePageWhenNewerRecordOverridesAnotherPage) {
    ScopedTestDir test_dir("recovery_index_smo_partial_overlap_root");
    const std::string db_name = "recovery_index_smo_partial_overlap_db";
    CreateRecoveryTestDb(db_name);

    OpenRecoveryDb db(db_name);
    const std::string index_name = db.sm_mgr_.get_ix_manager()->get_index_name("t", {"id"});
    IxIndexHandle* index = db.sm_mgr_.ihs_.at(index_name).get();
    const int fd = index->GetFd();
    constexpr page_id_t root_page_no = IX_INIT_ROOT_PAGE;
    constexpr page_id_t older_only_page_no = IX_INIT_ROOT_PAGE + 1;
    std::array<char, PAGE_SIZE> root{};
    std::array<char, PAGE_SIZE> header{};
    db.disk_.read_page(fd, root_page_no, root.data(), PAGE_SIZE);
    db.disk_.read_page(fd, IX_FILE_HDR_PAGE, header.data(), PAGE_SIZE);

    IndexSmoWalData older;
    older.index_file_name = index_name;
    older.pages.resize(2);
    older.pages[0].page_no = root_page_no;
    older.pages[0].bytes = root;
    older.pages[0].bytes.back() = 'a';
    older.pages[1].page_no = older_only_page_no;
    older.pages[1].bytes = root;
    older.pages[1].bytes.back() = 'b';
    older.header = header;
    older.header.back() = 'c';

    IndexSmoWalData newer;
    newer.index_file_name = index_name;
    newer.pages.resize(1);
    newer.pages[0].page_no = root_page_no;
    newer.pages[0].bytes = root;
    newer.pages[0].bytes.back() = 'd';
    newer.header = header;
    newer.header.back() = 'e';

    const lsn_t older_lsn = db.log_mgr_->append_index_smo(older);
    const lsn_t newer_lsn = db.log_mgr_->append_index_smo(newer);
    ASSERT_GT(newer_lsn, older_lsn);
    db.log_mgr_->flush_log_to_disk_up_to_durable(newer_lsn);
    db.disk_.write_page(fd, root_page_no, root.data(), PAGE_SIZE);
    db.disk_.write_page(fd, older_only_page_no, root.data(), PAGE_SIZE);
    db.disk_.write_page(fd, IX_FILE_HDR_PAGE, header.data(), PAGE_SIZE);

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.redo();

    std::array<char, PAGE_SIZE> recovered_root{};
    std::array<char, PAGE_SIZE> recovered_older_only{};
    std::array<char, PAGE_SIZE> recovered_header{};
    db.disk_.read_page(fd, root_page_no, recovered_root.data(), PAGE_SIZE);
    db.disk_.read_page(fd, older_only_page_no, recovered_older_only.data(), PAGE_SIZE);
    db.disk_.read_page(fd, IX_FILE_HDR_PAGE, recovered_header.data(), PAGE_SIZE);
    EXPECT_EQ(recovered_root.back(), 'd');
    EXPECT_EQ(recovered_older_only.back(), 'b');
    EXPECT_EQ(recovered_header.back(), 'e');
}

TEST(RecoveryManagerTest, AnalyzeReportsWalCompositionWithoutExtraScan) {
    ScopedTestDir test_dir("recovery_wal_composition_root");
    const std::string db_name = "recovery_wal_composition_db";
    CreateRecoveryTestDb(db_name);
    PrepareSinglePageIndexSmo(db_name, 61, Rid{1, 61}, false, false, 3);

    OpenRecoveryDb db(db_name);
    // A new process must establish the WAL append frontier before appending;
    // otherwise a fresh LogManager would reuse LSN 0 after the SMO fixture.
    db.log_mgr_->initialize_from_existing_log();
    const txn_id_t txn_id = 717;
    auto tuple = MakeTuple(61, 610);
    const lsn_t begin_lsn = AppendBegin(*db.log_mgr_, txn_id);
    const lsn_t insert_lsn = AppendInsert(*db.log_mgr_, txn_id, begin_lsn, Rid{1, 61}, tuple);
    AppendCommit(*db.log_mgr_, txn_id, insert_lsn);
    FlushLogs(*db.log_mgr_);

    std::array<uint64_t, RecoveryManager::kLogTypeCount> expected_counts{};
    std::array<uint64_t, RecoveryManager::kLogTypeCount> expected_bytes{};
    WalReader reader(&db.disk_, 0, db.disk_.get_log_file_size());
    WalRecordView record;
    while (reader.next(&record)) {
        const size_t type = static_cast<size_t>(record.log_type);
        ++expected_counts[type];
        expected_bytes[type] += record.total_len;
    }
    const uint64_t expected_analyze_reads = reader.read_count();
    const uint64_t reads_before_analyze = db.disk_.get_log_read_count();

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    EXPECT_EQ(db.disk_.get_log_read_count() - reads_before_analyze, expected_analyze_reads);

    for (size_t type = 0; type < RecoveryManager::kLogTypeCount; ++type) {
        const auto log_type = static_cast<LogType>(type);
        EXPECT_EQ(recovery.get_log_type_record_count(log_type), expected_counts[type]);
        EXPECT_EQ(recovery.get_log_type_serialized_bytes(log_type), expected_bytes[type]);
    }
    EXPECT_EQ(recovery.get_index_smo_logical_image_count(), 6u);
    EXPECT_EQ(recovery.get_index_smo_logical_image_bytes(), 6u * PAGE_SIZE);
}

TEST(RecoveryManagerTest, NewBindingSkipsOldSameNameIndexSmo) {
    ScopedTestDir test_dir("recovery_index_smo_generation_root");
    const std::string db_name = "recovery_index_smo_generation_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 23};
    PrepareSinglePageIndexSmo(db_name, 23, rid, true);

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 23, rid));
}

TEST(RecoveryManagerTest, CorruptOldGenerationIndexSmoStillFailsClosed) {
    ScopedTestDir test_dir("recovery_index_smo_corrupt_old_generation_root");
    const std::string db_name = "recovery_index_smo_corrupt_old_generation_db";
    CreateRecoveryTestDb(db_name);
    PrepareSinglePageIndexSmo(db_name, 24, Rid{1, 24}, true);

    OpenRecoveryDb db(db_name);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    const auto smo_offsets = WalRecordOffsets(db.disk_, LogType::INDEX_SMO);
    ASSERT_EQ(smo_offsets.size(), 1u);
    CorruptIndexSmoChecksum(db.disk_, smo_offsets.front());

    EXPECT_THROW(recovery.redo(), InternalError);
}

TEST(RecoveryManagerTest, LoserIndexSmoIsRedoneBeforeTheLoserKeyIsUndone) {
    ScopedTestDir test_dir("recovery_loser_index_smo_root");
    const std::string db_name = "recovery_loser_index_smo_db";
    CreateRecoveryTestDb(db_name);
    Rid rid{1, 0};
    rid = PrepareSinglePageIndexSmo(db_name, 31, rid, false, true);

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 31, rid));
}

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

TEST(RecoveryManagerTest, BeginOnlyTransactionsDoNotExpandLoserUndoWork) {
    ScopedTestDir test_dir("recovery_begin_only_scale_root");
    const std::string db_name = "recovery_begin_only_scale_db";
    CreateRecoveryTestDb(db_name);

    constexpr txn_id_t kFirstBeginOnlyTxn = 1000;
    constexpr int kBeginOnlyTxnCount = 20000;
    constexpr txn_id_t kActiveLoserTxn = 30000;
    constexpr txn_id_t kAbortedLoserTxn = 30001;
    constexpr txn_id_t kCommittedTxn = 30002;
    const Rid active_rid{1, 0};
    const Rid aborted_rid{1, 1};
    const Rid committed_rid{1, 2};
    auto active_rec = MakeTuple(1, 10);
    auto aborted_rec = MakeTuple(2, 20);
    auto committed_rec = MakeTuple(3, 30);

    {
        OpenRecoveryDb db(db_name);
        for (int i = 0; i < kBeginOnlyTxnCount; ++i) {
            AppendBegin(*db.log_mgr_, kFirstBeginOnlyTxn + i);
        }

        auto active_lsn = AppendBegin(*db.log_mgr_, kActiveLoserTxn);
        AppendInsert(*db.log_mgr_, kActiveLoserTxn, active_lsn, active_rid, active_rec);

        auto aborted_lsn = AppendBegin(*db.log_mgr_, kAbortedLoserTxn);
        aborted_lsn = AppendInsert(*db.log_mgr_, kAbortedLoserTxn, aborted_lsn, aborted_rid, aborted_rec);
        AppendAbort(*db.log_mgr_, kAbortedLoserTxn, aborted_lsn);

        auto committed_lsn = AppendBegin(*db.log_mgr_, kCommittedTxn);
        committed_lsn = AppendInsert(*db.log_mgr_, kCommittedTxn, committed_lsn, committed_rid, committed_rec);
        AppendCommit(*db.log_mgr_, kCommittedTxn, committed_lsn);
        FlushLogs(*db.log_mgr_);

        db.sm_mgr_.insert_record_with_indexes("t", active_rid, active_rec);
        db.sm_mgr_.insert_record_with_indexes("t", aborted_rid, aborted_rec);
        for (const auto& [rid, writer] :
             std::vector<std::pair<Rid, txn_id_t>>{{active_rid, kActiveLoserTxn}, {aborted_rid, kAbortedLoserTxn}}) {
            TupleMeta meta;
            meta.writer_txn_id_ = writer;
            meta.is_committed_ = false;
            meta.is_deleted_ = false;
            db.sm_mgr_.fhs_.at("t")->set_tuple_meta(rid, meta);
        }
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    {
        OpenRecoveryDb db(db_name);
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();

        EXPECT_EQ(recovery.get_scanned_record_count(), static_cast<uint64_t>(kBeginOnlyTxnCount + 8));
        EXPECT_EQ(recovery.get_pruned_no_undo_transaction_count(), static_cast<uint64_t>(kBeginOnlyTxnCount));
        EXPECT_EQ(recovery.get_loser_transaction_count(), 2u);

        recovery.redo();
        recovery.undo();

        EXPECT_EQ(recovery.get_dml_record_count(), 3u);
        EXPECT_EQ(recovery.get_redo_applied_count(), 1u);
        // Loser descriptors are compacted out before heap redo. They remain
        // represented by touched_/the LSN index and are applied by undo().
        EXPECT_EQ(recovery.get_redo_skipped_count(), 0u);
        EXPECT_EQ(recovery.get_undo_applied_count(), 2u);
        EXPECT_EQ(recovery.get_undo_chain_record_read_count(), 5u);
        EXPECT_FALSE(RecordExists(db.sm_mgr_, active_rid));
        EXPECT_FALSE(RecordExists(db.sm_mgr_, aborted_rid));
        ASSERT_TRUE(RecordExists(db.sm_mgr_, committed_rid));
        EXPECT_EQ(RecordValue(db.sm_mgr_, committed_rid), 30);
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 3, committed_rid));
    }
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

TEST(RecoveryManagerTest, CommittedRedoIgnoresHigherPageLsnFromLoser) {
    ScopedTestDir test_dir("recovery_higher_loser_page_lsn_root");
    const std::string db_name = "recovery_higher_loser_page_lsn_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto base_rec = MakeTuple(1, 10);
    auto committed_rec = MakeTuple(2, 20);
    auto loser_rec = MakeTuple(3, 30);
    lsn_t committed_update_lsn = INVALID_LSN;
    lsn_t loser_update_lsn = INVALID_LSN;

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, base_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto committed_lsn = AppendBegin(*db.log_mgr_, 100);
        committed_update_lsn = AppendUpdate(*db.log_mgr_, 100, committed_lsn, rid, base_rec, committed_rec);
        AppendCommit(*db.log_mgr_, 100, committed_update_lsn);

        auto loser_lsn = AppendBegin(*db.log_mgr_, 200);
        loser_update_lsn = AppendUpdate(*db.log_mgr_, 200, loser_lsn, rid, committed_rec, loser_rec);
        FlushLogs(*db.log_mgr_);
        ASSERT_GT(loser_update_lsn, committed_update_lsn);

        // Model a page flushed after the later loser write. Its page LSN is
        // ahead of the committed update, but recovery must still install the
        // committed image before the ownership-guarded loser undo runs.
        TupleMeta loser_meta;
        loser_meta.writer_txn_id_ = 200;
        loser_meta.is_committed_ = false;
        loser_meta.is_deleted_ = false;
        auto* file_handle = db.sm_mgr_.fhs_.at("t").get();
        file_handle->apply_tuple_update(rid, loser_rec.data, loser_meta, loser_update_lsn);
        ASSERT_TRUE(db.sm_mgr_.flush_all_table_and_index_pages());
        EXPECT_EQ(file_handle->get_page_lsn(rid), loser_update_lsn);
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
    EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 20);
    EXPECT_GE(db.sm_mgr_.fhs_.at("t")->get_page_lsn(rid), loser_update_lsn);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 3, rid));
}

TEST(RecoveryManagerTest, CommittedSparseUpdateRedoesFullRowAndRepairsBothIndexKeys) {
    ScopedTestDir test_dir("recovery_committed_sparse_update_root");
    const std::string db_name = "recovery_committed_sparse_update_db";
    CreateWideRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeWideTuple(1, 'a');
    auto new_rec = MakeWideTuple(2, 'a');
    Rid log_rid = rid;
    UpdateLogRecord shape(100, old_rec, new_rec, log_rid, "t");
    ASSERT_TRUE(shape.sparse_encoding_);

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
    EXPECT_EQ(RecordId(db.sm_mgr_, rid), 2);
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

TEST(RecoveryManagerTest, CommittedBidirectionalDeltaRedoesAgainstTheDurableBase) {
    ScopedTestDir test_dir("recovery_committed_delta_update_root");
    const std::string db_name = "recovery_committed_delta_update_db";
    CreateWideRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto old_rec = MakeWideTuple(1, 'a');
    auto new_rec = MakeWideTuple(1, 'a');
    constexpr int kChangedOffset = sizeof(int) + 20;
    new_rec.data[kChangedOffset] = 'z';
    Rid log_rid = rid;
    UpdateLogRecord shape(100, old_rec, new_rec, log_rid, "t", true);
    ASSERT_TRUE(shape.bidirectional_delta_encoding_);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto update_lsn = AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec, true);
        AppendCommit(*db.log_mgr_, 100, update_lsn);
        FlushLogs(*db.log_mgr_);

        // Legal crash window: the UPDATE body and writer ownership reached the
        // page before the durable COMMIT was published into tuple metadata.
        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->apply_tuple_update(rid, new_rec.data, meta, update_lsn);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    auto recovered = db.sm_mgr_.fhs_.at("t")->get_record(rid, nullptr);
    EXPECT_EQ(recovered->data[kChangedOffset], 'z');
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, CommittedBidirectionalDeltaWithoutABaseFailsClosed) {
    ScopedTestDir test_dir("recovery_missing_delta_base_root");
    const std::string db_name = "recovery_missing_delta_base_db";
    CreateWideRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto old_rec = MakeWideTuple(1, 'a');
    auto new_rec = MakeWideTuple(1, 'a');
    new_rec.data[sizeof(int) + 20] = 'z';

    {
        OpenRecoveryDb db(db_name);
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        auto update_lsn = AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec, true);
        AppendCommit(*db.log_mgr_, 100, update_lsn);
        FlushLogs(*db.log_mgr_);
    }

    EXPECT_THROW(RunRecovery(db_name), InternalError);
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

TEST(RecoveryManagerTest, UncommittedSparseUpdateUndoesFullRowAndRepairsBothIndexKeys) {
    ScopedTestDir test_dir("recovery_uncommitted_sparse_update_root");
    const std::string db_name = "recovery_uncommitted_sparse_update_db";
    CreateWideRecoveryTestDb(db_name);
    Rid rid{1, 0};
    auto old_rec = MakeWideTuple(1, 'a');
    auto new_rec = MakeWideTuple(2, 'a');
    Rid log_rid = rid;
    UpdateLogRecord shape(100, old_rec, new_rec, log_rid, "t");
    ASSERT_TRUE(shape.sparse_encoding_);

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, old_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();
        auto begin_lsn = AppendBegin(*db.log_mgr_, 100);
        AppendUpdate(*db.log_mgr_, 100, begin_lsn, rid, old_rec, new_rec);
        FlushLogs(*db.log_mgr_);
        db.sm_mgr_.update_record_with_indexes("t", rid, old_rec, new_rec);
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
    EXPECT_EQ(RecordId(db.sm_mgr_, rid), 1);
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
    EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 2, rid));
}

TEST(RecoveryManagerTest, LaterLoserDeltaPreservesTheEarlierCommittedDeltaDuringRecovery) {
    ScopedTestDir test_dir("recovery_later_loser_delta_root");
    const std::string db_name = "recovery_later_loser_delta_db";
    CreateWideRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto base_rec = MakeWideTuple(1, 'a');
    auto committed_rec = MakeWideTuple(1, 'a');
    auto loser_rec = MakeWideTuple(1, 'a');
    constexpr int kCommittedOffset = sizeof(int) + 20;
    constexpr int kLoserOffset = sizeof(int) + 40;
    committed_rec.data[kCommittedOffset] = 'c';
    loser_rec.data[kCommittedOffset] = 'c';
    loser_rec.data[kLoserOffset] = 'l';

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, base_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto committed_lsn = AppendBegin(*db.log_mgr_, 100);
        committed_lsn = AppendUpdate(*db.log_mgr_, 100, committed_lsn, rid, base_rec, committed_rec, true);
        AppendCommit(*db.log_mgr_, 100, committed_lsn);

        auto loser_lsn = AppendBegin(*db.log_mgr_, 200);
        loser_lsn = AppendUpdate(*db.log_mgr_, 200, loser_lsn, rid, committed_rec, loser_rec, true);
        FlushLogs(*db.log_mgr_);

        TupleMeta meta;
        meta.writer_txn_id_ = 200;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->apply_tuple_update(rid, loser_rec.data, meta, loser_lsn);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    auto recovered = db.sm_mgr_.fhs_.at("t")->get_record(rid, nullptr);
    EXPECT_EQ(recovered->data[kCommittedOffset], 'c');
    EXPECT_EQ(recovered->data[kLoserOffset], 'a');
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
}

TEST(RecoveryManagerTest, OlderAbortedLoserBytesAreNotSealedByALaterCommittedDelta) {
    ScopedTestDir test_dir("recovery_older_aborted_loser_delta_root");
    const std::string db_name = "recovery_older_aborted_loser_delta_db";
    CreateWideRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto base_rec = MakeWideTuple(1, 'a');
    auto loser_rec = MakeWideTuple(1, 'a');
    auto committed_rec = MakeWideTuple(1, 'a');
    constexpr int kLoserOffset = sizeof(int) + 20;
    constexpr int kCommittedOffset = sizeof(int) + 40;
    loser_rec.data[kLoserOffset] = 'l';
    committed_rec.data[kCommittedOffset] = 'c';

    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.insert_record_with_indexes("t", rid, base_rec);
        db.sm_mgr_.flush_all_table_and_index_pages();

        auto loser_lsn = AppendBegin(*db.log_mgr_, 100);
        loser_lsn = AppendUpdate(*db.log_mgr_, 100, loser_lsn, rid, base_rec, loser_rec, true);
        AppendAbort(*db.log_mgr_, 100, loser_lsn);

        auto committed_lsn = AppendBegin(*db.log_mgr_, 200);
        committed_lsn = AppendUpdate(*db.log_mgr_, 200, committed_lsn, rid, base_rec, committed_rec, true);
        AppendCommit(*db.log_mgr_, 200, committed_lsn);
        FlushLogs(*db.log_mgr_);

        TupleMeta meta;
        meta.writer_txn_id_ = 100;
        meta.is_committed_ = false;
        meta.is_deleted_ = false;
        db.sm_mgr_.fhs_.at("t")->apply_tuple_update(rid, loser_rec.data, meta, loser_lsn);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    RunRecovery(db_name);

    OpenRecoveryDb db(db_name);
    auto recovered = db.sm_mgr_.fhs_.at("t")->get_record(rid, nullptr);
    EXPECT_EQ(recovered->data[kLoserOffset], 'a');
    EXPECT_EQ(recovered->data[kCommittedOffset], 'c');
    EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, rid));
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
        index->delete_entry(MakeIntKey(1).data(), rid, IndexWriteWalContext::TestNoWal());
        index->insert_entry(MakeIntKey(2).data(), rid, IndexWriteWalContext::TestNoWal(), true);
        db.sm_mgr_.flush_all_table_and_index_pages();
    }

    {
        OpenRecoveryDb db(db_name);
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();
        recovery.redo();
        recovery.undo();
        // This committed RID is live after finalization, so its index key must
        // have come from the pinned finalize task. The old second heap sweep
        // no longer exists; this test-only metric stays zero-cost in that path.
        EXPECT_EQ(recovery.get_fused_live_index_key_count_for_test(), 1u);
        EXPECT_GT(recovery.get_fused_live_index_key_bytes_for_test(), 0u);
    }
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
        index->insert_entry(MakeIntKey(2).data(), rid, IndexWriteWalContext::TestNoWal(), true);
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
        index->insert_entry(MakeIntKey(1).data(), rid, IndexWriteWalContext::TestNoWal(), true);
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
        index->delete_entry(MakeIntKey(1).data(), rid, IndexWriteWalContext::TestNoWal());
        index->insert_entry(MakeIntKey(3).data(), rid, IndexWriteWalContext::TestNoWal(), true);
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
        recovery.redo();

        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid, "t"));
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid, "t"), 10);
        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid, "u"));
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid, "u"), 20);
    }
}

TEST(RecoveryManagerTest, RecoveryWorkersOneAndEightKeepTableAndIndexResultsIdentical) {
    ScopedTestDir test_dir("recovery_workers_deterministic_root");
    const std::string single_worker_db = "recovery_workers_single";
    const std::string auto_worker_db = "recovery_workers_eight";
    CreateRecoveryTestDb(single_worker_db, {"t", "u"});
    const Rid rid{1, 0};
    auto t_rec = MakeTuple(101, 1001);
    auto u_rec = MakeTuple(202, 2002);
    {
        OpenRecoveryDb db(single_worker_db);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, t_rec, "t");
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, u_rec, "u");
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }
    std::filesystem::copy(single_worker_db, auto_worker_db, std::filesystem::copy_options::recursive);

    struct EnvGuard {
        ~EnvGuard() {
            unsetenv("RMDB_RECOVERY_WORKERS");
        }
    } env_guard;
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "1", 1), 0);
    RunRecovery(single_worker_db);
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "8", 1), 0);
    RunRecovery(auto_worker_db);
    ASSERT_EQ(unsetenv("RMDB_RECOVERY_WORKERS"), 0);

    for (const std::string& db_name : {single_worker_db, auto_worker_db}) {
        OpenRecoveryDb db(db_name);
        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid, "t"));
        EXPECT_EQ(RecordId(db.sm_mgr_, rid, "t"), 101);
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid, "t"), 1001);
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 101, rid, "t"));
        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid, "u"));
        EXPECT_EQ(RecordId(db.sm_mgr_, rid, "u"), 202);
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid, "u"), 2002);
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 202, rid, "u"));
    }
}

TEST(RecoveryManagerTest, LargeTableNinePageFinalizeMatchesWorkersOneAndEight) {
    ScopedTestDir test_dir("recovery_finalize_nine_page_workers_root");
    const std::string single_worker_db = "recovery_finalize_nine_page_single";
    const std::string eight_worker_db = "recovery_finalize_nine_page_eight";
    CreateRecoveryTestDb(single_worker_db, {"t"}, false);
    {
        OpenRecoveryDb db(single_worker_db);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 100);
        for (page_id_t page_no = 1; page_no <= 9; ++page_no) {
            auto record = MakeTuple(page_no, page_no * 101);
            lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{page_no, page_no % 3}, record);
        }
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }
    std::filesystem::copy(single_worker_db, eight_worker_db, std::filesystem::copy_options::recursive);

    const char* previous_workers_value = getenv("RMDB_RECOVERY_WORKERS");
    const bool had_previous_workers = previous_workers_value != nullptr;
    const std::string previous_workers = had_previous_workers ? previous_workers_value : "";
    struct WorkersEnvGuard {
        bool had_previous;
        std::string previous;
        ~WorkersEnvGuard() {
            if (had_previous) {
                setenv("RMDB_RECOVERY_WORKERS", previous.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
    } workers_guard{had_previous_workers, previous_workers};

    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "1", 1), 0);
    RunPreparedRecovery(single_worker_db, 16);
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "8", 1), 0);
    RunPreparedRecovery(eight_worker_db, 16);

    struct Snapshot {
        RmFileHdr header{};
        std::vector<std::array<char, PAGE_SIZE>> pages;
        std::vector<page_id_t> free_chain;
    };
    const auto snapshot = [](const std::string& db_name) {
        OpenRecoveryDb db(db_name, 16);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        Snapshot result;
        result.header = file_handle->get_file_hdr();
        for (page_id_t page_no = 1; page_no < result.header.num_pages; ++page_no) {
            RmPageHandle page = file_handle->fetch_page_handle(page_no);
            std::array<char, PAGE_SIZE> bytes{};
            std::memcpy(bytes.data(), page.page->get_data(), PAGE_SIZE);
            result.pages.push_back(bytes);
            if (!db.bpm_.unpin_page(page.page->get_page_id(), false)) {
                throw std::runtime_error("snapshot could not release record page pin");
            }
        }
        std::set<page_id_t> seen;
        for (page_id_t page_no = result.header.first_free_page_no; page_no != RM_NO_PAGE;) {
            if (!seen.insert(page_no).second) throw std::runtime_error("snapshot found a free-chain cycle");
            result.free_chain.push_back(page_no);
            RmPageHandle page = file_handle->fetch_page_handle(page_no);
            page_no = page.page_hdr->next_free_page_no;
            if (!db.bpm_.unpin_page(page.page->get_page_id(), false)) {
                throw std::runtime_error("snapshot could not release free-chain page pin");
            }
        }
        return result;
    };
    const Snapshot single = snapshot(single_worker_db);
    const Snapshot eight = snapshot(eight_worker_db);
    ASSERT_EQ(single.header.num_pages, 10);
    ASSERT_EQ(0, std::memcmp(&single.header, &eight.header, sizeof(RmFileHdr)));
    ASSERT_EQ(single.pages.size(), 9u);
    ASSERT_EQ(single.pages.size(), eight.pages.size());
    for (size_t page = 0; page < single.pages.size(); ++page) {
        EXPECT_EQ(single.pages[page], eight.pages[page]);
    }
    EXPECT_EQ(single.free_chain, eight.free_chain);
    EXPECT_EQ(single.free_chain.size(), 9u);
}

TEST(RecoveryManagerTest, FinalizeClampsWorkersToSmallBufferPool) {
    ScopedTestDir test_dir("recovery_finalize_small_pool_root");
    const std::string db_name = "recovery_finalize_small_pool_db";
    CreateRecoveryTestDb(db_name, {"t"}, false);
    {
        OpenRecoveryDb db(db_name);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 100);
        for (page_id_t page_no = 1; page_no <= 6; ++page_no) {
            auto record = MakeTuple(page_no, page_no * 11);
            lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{page_no, 0}, record);
        }
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }
    const char* previous_workers_value = getenv("RMDB_RECOVERY_WORKERS");
    const bool had_previous_workers = previous_workers_value != nullptr;
    const std::string previous_workers = had_previous_workers ? previous_workers_value : "";
    struct WorkersEnvGuard {
        bool had_previous;
        std::string previous;
        ~WorkersEnvGuard() {
            if (had_previous) {
                setenv("RMDB_RECOVERY_WORKERS", previous.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
    } workers_guard{had_previous_workers, previous_workers};
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "8", 1), 0);
    EXPECT_NO_THROW(RunPreparedRecovery(db_name, 2));

    OpenRecoveryDb verified(db_name, 2);
    EXPECT_EQ(verified.sm_mgr_.fhs_.at("t")->get_file_hdr().num_pages, 7);
    for (page_id_t page_no = 1; page_no <= 6; ++page_no) {
        const Rid rid{page_no, 0};
        ASSERT_TRUE(RecordExists(verified.sm_mgr_, rid));
        EXPECT_EQ(RecordValue(verified.sm_mgr_, rid), page_no * 11);
    }
    EXPECT_EQ(verified.disk_.get_file_size(LOG_FILE_NAME), 0);
}

void CreateFusedPhysicalMultiIndexScenario(const std::string& db_name) {
    CreateRecoveryTestDb(db_name, {"t"}, false);
    {
        OpenRecoveryDb db(db_name);
        db.sm_mgr_.create_index("t", {"id"}, nullptr);
        db.sm_mgr_.create_index("t", {"v"}, nullptr);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        for (page_id_t page_no = 1; page_no <= 8; ++page_no) {
            RmPageHandle page = file_handle->create_new_page_handle();
            if (page.page->get_page_id().page_no != page_no || !db.bpm_.unpin_page(page.page->get_page_id(), true)) {
                throw std::runtime_error("could not create fused physical finalize page");
            }
        }
        if (!db.bpm_.flush_all_pages(file_handle->GetFd())) {
            throw std::runtime_error("could not persist fused physical finalize pages");
        }

        lsn_t lsn = AppendBegin(*db.log_mgr_, 900);
        const auto append_insert = [&](const Rid& rid, int id, int value) {
            auto record = MakeTuple(id, value);
            lsn = AppendInsert(*db.log_mgr_, 900, lsn, rid, record);
        };
        append_insert(Rid{1, 0}, 101, 1001);
        auto old_record = MakeTuple(202, 2002);
        auto new_record = MakeTuple(203, 2003);
        lsn = AppendInsert(*db.log_mgr_, 900, lsn, Rid{2, 0}, old_record);
        lsn = AppendUpdate(*db.log_mgr_, 900, lsn, Rid{2, 0}, old_record, new_record);
        auto deleted_record = MakeTuple(303, 3003);
        lsn = AppendInsert(*db.log_mgr_, 900, lsn, Rid{3, 0}, deleted_record);
        lsn = AppendDelete(*db.log_mgr_, 900, lsn, Rid{3, 0}, deleted_record);
        for (page_id_t page_no = 4; page_no <= 8; ++page_no) {
            append_insert(Rid{page_no, 0}, 100 + page_no, 1100 + page_no);
        }
        AppendCommit(*db.log_mgr_, 900, lsn);
        FlushLogs(*db.log_mgr_);
    }
}

struct FusedPhysicalIndexResult {
    uint64_t fused_keys{0};
    uint64_t fused_bytes{0};
    std::set<page_id_t> physical_pages;
    size_t physical_worker_threads{0};
};

FusedPhysicalIndexResult RecoverFusedPhysicalMultiIndexScenario(const std::string& db_name, const char* workers,
                                                                  size_t pool_size, size_t expected_participants) {
    ScopedEnvironmentValue worker_count("RMDB_RECOVERY_WORKERS", workers);
    OpenRecoveryDb db(db_name, pool_size);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.prepare_pages_for_redo();
    recovery.redo();
    const page_id_t physical_frontier = db.sm_mgr_.fhs_.at("t")->recovery_physical_page_frontier();
    FusedPhysicalIndexResult result;
    std::mutex participant_latch;
    std::condition_variable participant_cv;
    std::set<std::thread::id> participant_threads;
    bool release_participants = false;
    recovery.set_recovery_finalize_pin_test_hook([&](page_id_t page_no) {
        if (page_no >= physical_frontier) {
            throw std::runtime_error("finalize hook observed a non-physical page");
        }
        std::unique_lock<std::mutex> lock(participant_latch);
        result.physical_pages.insert(page_no);
        participant_threads.insert(std::this_thread::get_id());
        if (participant_threads.size() >= expected_participants) {
            release_participants = true;
            participant_cv.notify_all();
            return;
        }
        if (!participant_cv.wait_for(lock, std::chrono::seconds(5), [&] { return release_participants; })) {
            release_participants = true;
            participant_cv.notify_all();
            throw std::runtime_error("physical finalize worker barrier timed out");
        }
    });
    recovery.undo();
    {
        std::lock_guard<std::mutex> lock(participant_latch);
        result.physical_worker_threads = participant_threads.size();
    }
    result.fused_keys = recovery.get_fused_live_index_key_count_for_test();
    result.fused_bytes = recovery.get_fused_live_index_key_bytes_for_test();
    return result;
}

void ExpectFusedPhysicalMultiIndexResult(const std::string& db_name) {
    OpenRecoveryDb db(db_name);
    const std::array<std::pair<Rid, std::pair<int, int>>, 7> live_rows{{
        {Rid{1, 0}, {101, 1001}}, {Rid{2, 0}, {203, 2003}}, {Rid{4, 0}, {104, 1104}},
        {Rid{5, 0}, {105, 1105}}, {Rid{6, 0}, {106, 1106}}, {Rid{7, 0}, {107, 1107}},
        {Rid{8, 0}, {108, 1108}},
    }};
    for (const auto& [rid, keys] : live_rows) {
        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
        EXPECT_EQ(RecordId(db.sm_mgr_, rid), keys.first);
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid), keys.second);
        EXPECT_TRUE(IndexColumnPointsTo(db.sm_mgr_, "id", keys.first, rid));
        EXPECT_TRUE(IndexColumnPointsTo(db.sm_mgr_, "v", keys.second, rid));
    }
    EXPECT_FALSE(RecordExists(db.sm_mgr_, Rid{3, 0}));
    for (const auto& [column, key] : {std::pair{"id", 202}, std::pair{"v", 2002}, std::pair{"id", 303},
                                      std::pair{"v", 3003}}) {
        EXPECT_TRUE(IndexEntriesForColumn(db.sm_mgr_, column, key).empty());
    }
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
}

TEST(RecoveryManagerTest, FusedPhysicalMultiIndexKeysAreDeterministicAcrossWorkersAndSmallPool) {
    ScopedTestDir test_dir("recovery_fused_physical_multi_index_root");
    const std::string single_worker_db = "recovery_fused_physical_multi_index_single";
    const std::string eight_worker_db = "recovery_fused_physical_multi_index_eight";
    const std::string small_pool_db = "recovery_fused_physical_multi_index_small_pool";
    CreateFusedPhysicalMultiIndexScenario(single_worker_db);
    std::filesystem::copy(single_worker_db, eight_worker_db, std::filesystem::copy_options::recursive);
    std::filesystem::copy(single_worker_db, small_pool_db, std::filesystem::copy_options::recursive);

    const FusedPhysicalIndexResult single = RecoverFusedPhysicalMultiIndexScenario(single_worker_db, "1", 16, 1);
    const FusedPhysicalIndexResult eight = RecoverFusedPhysicalMultiIndexScenario(eight_worker_db, "8", 16, 8);
    const FusedPhysicalIndexResult small_pool = RecoverFusedPhysicalMultiIndexScenario(small_pool_db, "8", 2, 2);

    const std::set<page_id_t> expected_physical_pages{1, 2, 3, 4, 5, 6, 7, 8};
    for (const FusedPhysicalIndexResult* result : {&single, &eight, &small_pool}) {
        EXPECT_EQ(result->physical_pages, expected_physical_pages);
        EXPECT_EQ(result->fused_keys, 14u);
        EXPECT_EQ(result->fused_bytes, 14u * sizeof(int));
    }
    EXPECT_EQ(single.physical_worker_threads, 1u);
    EXPECT_EQ(eight.physical_worker_threads, 8u);
    EXPECT_EQ(small_pool.physical_worker_threads, 2u);
    EXPECT_EQ(single.fused_keys, eight.fused_keys);
    EXPECT_EQ(single.fused_bytes, eight.fused_bytes);
    ExpectFusedPhysicalMultiIndexResult(single_worker_db);
    ExpectFusedPhysicalMultiIndexResult(eight_worker_db);
    ExpectFusedPhysicalMultiIndexResult(small_pool_db);
}

TEST(RecoveryManagerTest, PhysicalFinalizeOverlapsTwoDirtyVictimWrites) {
    ScopedTestDir test_dir("recovery_finalize_parallel_dirty_io_root");
    const std::string db_name = "recovery_finalize_parallel_dirty_io_db";
    CreatePhysicalFinalizeSeed(db_name);
    ScopedEnvironmentValue workers("RMDB_RECOVERY_WORKERS", "8");

    OpenRecoveryDb db(db_name, 2);
    RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.prepare_pages_for_redo();
    recovery.redo();
    InstallDirtyFinalizeVictims(db);

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    size_t arrivals = 0;
    size_t inflight = 0;
    size_t peak_inflight = 0;
    bool cancel_barrier = false;
    ScopedBpmFinalizeHooks hook_guard(&db.bpm_);
    db.bpm_.set_replacement_io_test_hook([&](PageId page_id) {
        if (page_id.fd != file_handle->GetFd() || (page_id.page_no != 3 && page_id.page_no != 4))
            return;
        std::unique_lock lock(hook_latch);
        ++arrivals;
        ++inflight;
        peak_inflight = std::max(peak_inflight, inflight);
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return arrivals == 2 || cancel_barrier; });
        --inflight;
    });

    std::exception_ptr recovery_failure;
    std::thread recovery_thread([&] {
        try {
            recovery.undo();
        } catch (...) {
            recovery_failure = std::current_exception();
        }
    });
    bool overlapped = false;
    {
        std::unique_lock lock(hook_latch);
        overlapped = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] { return arrivals == 2; });
        if (!overlapped)
            cancel_barrier = true;
    }
    hook_cv.notify_all();
    recovery_thread.join();
    if (recovery_failure)
        std::rethrow_exception(recovery_failure);
    EXPECT_TRUE(overlapped);
    EXPECT_EQ(arrivals, 2u);
    EXPECT_EQ(peak_inflight, 2u);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
    for (page_id_t page_no = 1; page_no <= 2; ++page_no) {
        const Rid rid{page_no, 0};
        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
        EXPECT_EQ(RecordValue(db.sm_mgr_, rid), 8000 + page_no);
    }
}

TEST(RecoveryManagerTest, PostGateFrontierPromotesCompletedDirtyExtensionWrite) {
    ScopedTestDir test_dir("recovery_finalize_post_gate_promotion_root");
    const std::string db_name = "recovery_finalize_post_gate_promotion_db";
    CreateRecoveryTestDb(db_name, {"t"}, false);
    {
        OpenRecoveryDb db(db_name);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        RmPageHandle page = file_handle->create_new_page_handle();
        ASSERT_EQ(page.page->get_page_id().page_no, 1);
        ASSERT_TRUE(db.bpm_.unpin_page(page.page->get_page_id(), true));
        ASSERT_TRUE(db.bpm_.flush_all_pages(file_handle->GetFd()));
        auto record = MakeTuple(902, 9020);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 902);
        lsn = AppendInsert(*db.log_mgr_, 902, lsn, Rid{2, 0}, record);
        AppendCommit(*db.log_mgr_, 902, lsn);
        FlushLogs(*db.log_mgr_);
    }
    ScopedEnvironmentValue workers("RMDB_RECOVERY_WORKERS", "8");

    OpenRecoveryDb db(db_name, 1);
    RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
    const PageId extension{file_handle->GetFd(), 2};
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.prepare_pages_for_redo();
    recovery.redo();
    ASSERT_TRUE(db.bpm_.is_page_resident(extension));
    ASSERT_EQ(db.disk_.get_file_size(file_handle->GetFd()) / PAGE_SIZE, 2);

    std::mutex hook_latch;
    std::condition_variable hook_cv;
    bool pwrite_blocked = false;
    bool gate_published = false;
    bool release_pwrite = false;
    size_t extension_reads = 0;
    bool extension_finalize_seen = false;
    size_t extension_reads_before_finalize = 0;
    ScopedBpmFinalizeHooks hook_guard(&db.bpm_);
    db.bpm_.set_replacement_io_test_hook([&](PageId page_id) {
        if (!(page_id == extension))
            return;
        std::unique_lock lock(hook_latch);
        pwrite_blocked = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_pwrite; });
    });
    db.bpm_.set_frame_operation_gate_test_hook([&] {
        // This callback owns the BPM global latch. Signal only: waiting here
        // would prevent the old-image completion that the gate must drain.
        std::lock_guard lock(hook_latch);
        gate_published = true;
        hook_cv.notify_all();
    });
    db.bpm_.set_load_io_test_hook([&](PageId page_id) {
        if (page_id == extension) {
            std::lock_guard lock(hook_latch);
            ++extension_reads;
        }
    });
    recovery.set_recovery_finalize_pin_test_hook([&](page_id_t page_no) {
        if (page_no == extension.page_no) {
            std::lock_guard lock(hook_latch);
            extension_finalize_seen = true;
            extension_reads_before_finalize = extension_reads;
        }
    });

    std::exception_ptr evictor_failure;
    std::thread evictor([&] {
        try {
            Page* page = db.bpm_.fetch_page(PageId{file_handle->GetFd(), 1});
            if (page == nullptr || !db.bpm_.unpin_page(page->get_page_id(), false)) {
                throw std::runtime_error("promotion evictor could not fetch physical page");
            }
        } catch (...) {
            evictor_failure = std::current_exception();
        }
    });
    bool saw_blocked_pwrite = false;
    {
        std::unique_lock lock(hook_latch);
        saw_blocked_pwrite = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] { return pwrite_blocked; });
        if (!saw_blocked_pwrite)
            release_pwrite = true;
    }
    hook_cv.notify_all();

    if (!saw_blocked_pwrite) {
        evictor.join();
        if (evictor_failure)
            std::rethrow_exception(evictor_failure);
        FAIL() << "extension replacement did not reach the pre-pwrite rendezvous";
    }

    std::exception_ptr recovery_failure;
    std::thread recovery_thread([&] {
        try {
            recovery.undo();
        } catch (...) {
            recovery_failure = std::current_exception();
        }
    });
    bool saw_published_gate = false;
    {
        std::unique_lock lock(hook_latch);
        saw_published_gate = hook_cv.wait_for(lock, std::chrono::seconds(2), [&] { return gate_published; });
        release_pwrite = true;
    }
    hook_cv.notify_all();
    evictor.join();
    recovery_thread.join();
    db.bpm_.set_replacement_io_test_hook({});
    db.bpm_.set_frame_operation_gate_test_hook({});
    db.bpm_.set_load_io_test_hook({});
    if (evictor_failure)
        std::rethrow_exception(evictor_failure);
    if (recovery_failure)
        std::rethrow_exception(recovery_failure);

    EXPECT_TRUE(saw_published_gate);
    EXPECT_TRUE(extension_finalize_seen);
    EXPECT_EQ(extension_reads_before_finalize, 1u);
    EXPECT_EQ(db.disk_.get_file_size(file_handle->GetFd()) / PAGE_SIZE, 3);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, Rid{2, 0}));
    EXPECT_EQ(RecordValue(db.sm_mgr_, Rid{2, 0}), 9020);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
}

TEST(RecoveryManagerTest, AbsentLoserExtensionDoesNotExpandHeaderWithNoPhysicalWorkers) {
    ScopedTestDir test_dir("recovery_finalize_absent_extension_root");
    const std::string db_name = "recovery_finalize_absent_extension_db";
    CreateRecoveryTestDb(db_name, {"t"}, false);
    {
        OpenRecoveryDb db(db_name);
        auto record = MakeTuple(903, 9030);
        const lsn_t begin_lsn = AppendBegin(*db.log_mgr_, 903);
        (void)AppendInsert(*db.log_mgr_, 903, begin_lsn, Rid{1, 0}, record);
        FlushLogs(*db.log_mgr_);
    }
    ScopedEnvironmentValue workers("RMDB_RECOVERY_WORKERS", "8");
    EXPECT_NO_THROW(RunPreparedRecovery(db_name, 1));

    OpenRecoveryDb verified(db_name, 1);
    RmFileHandle* file_handle = verified.sm_mgr_.fhs_.at("t").get();
    EXPECT_EQ(file_handle->get_file_hdr().num_pages, 1);
    EXPECT_LE(verified.disk_.get_file_size(file_handle->GetFd()), PAGE_SIZE);
    EXPECT_FALSE(RecordExists(verified.sm_mgr_, Rid{1, 0}));
    EXPECT_EQ(verified.disk_.get_file_size(LOG_FILE_NAME), 0);
}

TEST(RecoveryManagerTest, PhysicalFinalizePinFailureRetainsWalAndFailsClosed) {
    ScopedTestDir test_dir("recovery_finalize_physical_pin_fail_root");
    const std::string db_name = "recovery_finalize_physical_pin_fail_db";
    CreateRecoveryTestDb(db_name);
    Rid rid;
    {
        OpenRecoveryDb db(db_name);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        RmPageHandle page = file_handle->create_new_page_handle();
        rid = Rid{page.page->get_page_id().page_no, 0};
        ASSERT_TRUE(db.bpm_.unpin_page(page.page->get_page_id(), true));
        auto rec = MakeTuple(71, 710);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 71);
        lsn = AppendInsert(*db.log_mgr_, 71, lsn, rid, rec);
        AppendCommit(*db.log_mgr_, 71, lsn);
        FlushLogs(*db.log_mgr_);
    }
    OpenRecoveryDb db(db_name);
    const int64_t wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    recovery.redo();
    db.sm_mgr_.fhs_.at("t")->set_fail_recovery_physical_pin_for_test(true);
    EXPECT_THROW(recovery.undo(), InternalError);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
    Page* page = db.bpm_.fetch_page(PageId{db.sm_mgr_.fhs_.at("t")->GetFd(), rid.page_no});
    ASSERT_NE(page, nullptr);
    EXPECT_TRUE(db.bpm_.unpin_page(page->get_page_id(), false));
}

TEST(RecoveryManagerTest, FinalizeRechecksResidentOnlyExtensionAfterConcurrentPhysicalEviction) {
    ScopedTestDir test_dir("recovery_finalize_extension_eviction_root");
    const std::string db_name = "recovery_finalize_extension_eviction_db";
    constexpr page_id_t kLastPhysicalPage = 8;
    constexpr page_id_t kExtensionPage = kLastPhysicalPage + 1;
    const Rid deleted_extension{kExtensionPage, 0};
    const Rid live_extension{kExtensionPage, 1};
    CreateRecoveryTestDb(db_name, {"t"}, false);

    // Persist eight ordinary record pages first. The ninth page is deliberately
    // absent from the file and will be created by redo, matching the crash
    // window where a committed extension is resident-only.
    {
        OpenRecoveryDb db(db_name);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        for (page_id_t page_no = RM_FIRST_RECORD_PAGE; page_no <= kLastPhysicalPage; ++page_no) {
            RmPageHandle page = file_handle->create_new_page_handle();
            ASSERT_EQ(page.page->get_page_id().page_no, page_no);
            ASSERT_TRUE(db.bpm_.unpin_page(page.page->get_page_id(), true));
        }
        ASSERT_TRUE(db.bpm_.flush_all_pages(file_handle->GetFd()));
    }
    {
        OpenRecoveryDb db(db_name);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 100);
        for (page_id_t page_no = RM_FIRST_RECORD_PAGE; page_no <= kLastPhysicalPage; ++page_no) {
            auto record = MakeTuple(page_no, page_no * 10);
            lsn = AppendInsert(*db.log_mgr_, 100, lsn, Rid{page_no, 0}, record);
        }
        auto deleted = MakeTuple(901, 9010);
        auto live = MakeTuple(902, 9020);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, deleted_extension, deleted);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, live_extension, live);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    const char* previous_workers_value = getenv("RMDB_RECOVERY_WORKERS");
    const bool had_previous_workers = previous_workers_value != nullptr;
    const std::string previous_workers = had_previous_workers ? previous_workers_value : "";
    struct WorkersEnvGuard {
        bool had_previous;
        std::string previous;
        ~WorkersEnvGuard() {
            if (had_previous) {
                setenv("RMDB_RECOVERY_WORKERS", previous.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
    } workers_env_guard{had_previous_workers, previous_workers};

    {
        // Eight frames admit all eight physical finalize tasks, but not those
        // pages plus the resident-only extension. Redo itself stays serial so
        // page nine is reliably the last resident page before finalization.
        OpenRecoveryDb db(db_name, 8);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "1", 1), 0);
        recovery.analyze();
        recovery.prepare_pages_for_redo();
        recovery.redo();
        ASSERT_EQ(db.disk_.get_file_size(file_handle->GetFd()) / PAGE_SIZE, kExtensionPage);

        // Model stale surviving/deleted metadata after redo. The task must
        // reload this exact image after its first physical write and normalize
        // both bitmap and tuple metadata, rather than treating page nine as a
        // permanently resident-only page based on an earlier file-size view.
        RmPageHandle extension = file_handle->fetch_page_handle(kExtensionPage);
        {
            std::unique_lock<std::shared_mutex> page_lock(extension.page->latch());
            ASSERT_TRUE(Bitmap::is_set(extension.bitmap, deleted_extension.slot_no));
            ASSERT_TRUE(Bitmap::is_set(extension.bitmap, live_extension.slot_no));
            TupleMeta& tombstone = extension.get_meta(deleted_extension.slot_no);
            tombstone.is_deleted_ = true;
            tombstone.is_committed_ = false;
            tombstone.writer_txn_id_ = 701;
            tombstone.commit_ts_ = 71;
            tombstone.version_chain_head_ = UndoLink{7, 70, 701};
            TupleMeta& survivor = extension.get_meta(live_extension.slot_no);
            survivor.is_deleted_ = false;
            survivor.is_committed_ = false;
            survivor.writer_txn_id_ = 702;
            survivor.commit_ts_ = 72;
            survivor.version_chain_head_ = UndoLink{7, 71, 702};
            extension.page_hdr->num_records = 99;
            BufferPoolManager::mark_dirty_locked(extension.page);
        }
        ASSERT_TRUE(db.bpm_.unpin_page(extension.page->get_page_id(), false));

        std::mutex hook_latch;
        std::condition_variable hook_cv;
        size_t physical_pins = 0;
        bool extension_evicted = false;
        bool extension_prepass_seen = false;
        size_t extension_disk_reads = 0;
        size_t extension_reads_before_finalize = 0;
        ScopedBpmFinalizeHooks bpm_hook_guard(&db.bpm_);
        struct ReplacementHookGuard {
            ~ReplacementHookGuard() {
                BufferPoolManager::set_replacement_transition_test_hook({});
            }
        } replacement_hook_guard;
        BufferPoolManager::set_replacement_transition_test_hook([&](PageId page_id) {
            if (page_id == PageId{file_handle->GetFd(), kExtensionPage}) {
                std::lock_guard<std::mutex> lock(hook_latch);
                extension_evicted = true;
                hook_cv.notify_all();
            }
        });
        db.bpm_.set_load_io_test_hook([&](PageId page_id) {
            if (page_id == PageId{file_handle->GetFd(), kExtensionPage}) {
                std::lock_guard<std::mutex> lock(hook_latch);
                ++extension_disk_reads;
            }
        });
        recovery.set_recovery_finalize_pin_test_hook([&](page_id_t page_no) {
            if (page_no > kLastPhysicalPage) {
                std::lock_guard<std::mutex> lock(hook_latch);
                extension_prepass_seen = true;
                extension_reads_before_finalize = extension_disk_reads;
                return;
            }
            std::unique_lock<std::mutex> lock(hook_latch);
            ++physical_pins;
            hook_cv.notify_all();
            hook_cv.wait(lock, [&] { return physical_pins == kLastPhysicalPage; });
        });
        ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "8", 1), 0);
        recovery.undo();
        EXPECT_TRUE(extension_evicted);
        EXPECT_TRUE(extension_prepass_seen);
        EXPECT_EQ(extension_reads_before_finalize, 0u);
        EXPECT_EQ(physical_pins, static_cast<size_t>(kLastPhysicalPage));
    }

    OpenRecoveryDb verified(db_name);
    RmFileHandle* file_handle = verified.sm_mgr_.fhs_.at("t").get();
    const RmFileHdr file_hdr = file_handle->get_file_hdr();
    ASSERT_EQ(file_hdr.num_pages, kExtensionPage + 1);
    EXPECT_FALSE(RecordExists(verified.sm_mgr_, deleted_extension));
    ASSERT_TRUE(RecordExists(verified.sm_mgr_, live_extension));
    EXPECT_EQ(RecordValue(verified.sm_mgr_, live_extension), 9020);
    const TupleMeta meta = file_handle->get_tuple_meta(live_extension);
    EXPECT_EQ(meta.commit_ts_, 0);
    EXPECT_EQ(meta.writer_txn_id_, INVALID_TXN_ID);
    EXPECT_TRUE(meta.is_committed_);
    EXPECT_FALSE(meta.is_deleted_);
    EXPECT_EQ(meta.version_chain_head_, UndoLink{});
    RmPageHandle extension = file_handle->fetch_page_handle(kExtensionPage);
    {
        std::shared_lock<std::shared_mutex> page_lock(extension.page->latch());
        EXPECT_EQ(extension.page_hdr->num_records, 1);
    }
    ASSERT_TRUE(verified.bpm_.unpin_page(extension.page->get_page_id(), false));

    std::set<page_id_t> free_pages;
    page_id_t page_no = file_hdr.first_free_page_no;
    while (page_no != RM_NO_PAGE) {
        ASSERT_GE(page_no, RM_FIRST_RECORD_PAGE);
        ASSERT_LT(page_no, file_hdr.num_pages);
        ASSERT_TRUE(free_pages.insert(page_no).second) << "free-list contains a cycle";
        RmPageHandle page = file_handle->fetch_page_handle(page_no);
        page_no = page.page_hdr->next_free_page_no;
        ASSERT_TRUE(verified.bpm_.unpin_page(page.page->get_page_id(), false));
    }
    EXPECT_EQ(free_pages.size(), static_cast<size_t>(kExtensionPage));
    EXPECT_EQ(verified.disk_.get_file_size(LOG_FILE_NAME), 0);
}

TEST(RecoveryManagerTest, CrossTaskRangeIndexKeyCollectionMatchesWorkersOneAndFour) {
    ScopedTestDir test_dir("recovery_cross_segment_keys_root");
    const std::string single_worker_db = "recovery_cross_segment_single";
    const std::string four_worker_db = "recovery_cross_segment_four";
    const std::string fallback_db = "recovery_cross_segment_fallback";
    const std::string mutation_db = "recovery_cross_segment_mutation";
    CreateSegmentRecoveryTestDb(single_worker_db);
    const Rid rid{1, 0};
    constexpr int kUpdates = 10000; // > 17 MiB of full-row UPDATE WAL.
    {
        OpenRecoveryDb db(single_worker_db);
        auto initial = MakeSegmentTuple(1, 'a');
        ASSERT_EQ(db.sm_mgr_.fhs_.at("t")->insert_record(initial.data, nullptr), rid);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 100);
        auto before = initial;
        for (int value = 2; value < kUpdates + 2; ++value) {
            auto after = MakeSegmentTuple(value, static_cast<char>('a' + value % 19));
            lsn = AppendUpdate(*db.log_mgr_, 100, lsn, rid, before, after);
            before = std::move(after);
        }
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
        ASSERT_GT(db.disk_.get_file_size(LOG_FILE_NAME), 17LL * 1024 * 1024);
    }
    std::filesystem::copy(single_worker_db, four_worker_db, std::filesystem::copy_options::recursive);
    std::filesystem::copy(single_worker_db, fallback_db, std::filesystem::copy_options::recursive);
    std::filesystem::copy(single_worker_db, mutation_db, std::filesystem::copy_options::recursive);

    struct RecoveryStats {
        uint64_t probes;
        uint64_t mutations;
        uint64_t unchanged;
    };
    const char* previous_workers_value = getenv("RMDB_RECOVERY_WORKERS");
    const bool had_previous_workers = previous_workers_value != nullptr;
    const std::string previous_workers = had_previous_workers ? previous_workers_value : "";
    struct WorkersEnvGuard {
        bool had_previous;
        std::string previous;
        ~WorkersEnvGuard() {
            if (had_previous) {
                setenv("RMDB_RECOVERY_WORKERS", previous.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
    } workers_env_guard{had_previous_workers, previous_workers};
    const auto recover = [](const std::string& db_name, const char* workers, size_t scratch_limit = 0) {
        if (setenv("RMDB_RECOVERY_WORKERS", workers, 1) != 0) {
            throw std::runtime_error("setenv RMDB_RECOVERY_WORKERS failed");
        }
        OpenRecoveryDb db(db_name);
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.set_index_key_parallel_scratch_limit_for_test(scratch_limit);
        recovery.analyze();
        recovery.redo();
        recovery.undo();
        return RecoveryStats{recovery.get_index_probe_count(), recovery.get_index_mutation_count(),
                             recovery.get_index_unchanged_key_count()};
    };
    const RecoveryStats single = recover(single_worker_db, "1");
    const RecoveryStats four = recover(four_worker_db, "4");
    const RecoveryStats fallback = recover(fallback_db, "4", 1);
    ASSERT_EQ(unsetenv("RMDB_RECOVERY_WORKERS"), 0);

    EXPECT_EQ(single.probes, four.probes);
    EXPECT_EQ(single.mutations, four.mutations);
    EXPECT_EQ(single.unchanged, four.unchanged);
    EXPECT_EQ(single.probes, fallback.probes);
    EXPECT_EQ(single.mutations, fallback.mutations);
    EXPECT_EQ(single.unchanged, fallback.unchanged);
    for (const std::string& db_name : {single_worker_db, four_worker_db, fallback_db}) {
        OpenRecoveryDb db(db_name);
        ASSERT_TRUE(RecordExists(db.sm_mgr_, rid));
        EXPECT_EQ(RecordId(db.sm_mgr_, rid), kUpdates + 1);
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, kUpdates + 1, rid));
        EXPECT_FALSE(IndexPointsTo(db.sm_mgr_, 1, rid));
    }

    OpenRecoveryDb mutated(mutation_db);
    RecoveryManager recovery(&mutated.disk_, &mutated.bpm_, &mutated.sm_mgr_, mutated.log_mgr_.get());
    recovery.analyze();
    recovery.redo();
    const auto update_offsets = WalRecordOffsets(mutated.disk_, LogType::UPDATE);
    ASSERT_GT(update_offsets.size(), 9000u);
    const auto offset = update_offsets[9000];
    ASSERT_GT(offset, 16LL * 1024 * 1024);
    const int replacement_id = -999;
    PatchWalBytes(LOG_FILE_NAME, offset + OFFSET_LOG_DATA, &replacement_id, sizeof(replacement_id));
    const int64_t wal_size = mutated.disk_.get_file_size(LOG_FILE_NAME);
    EXPECT_THROW(recovery.undo(), InternalError);
    EXPECT_EQ(mutated.disk_.get_file_size(LOG_FILE_NAME), wal_size);
}

// The WAL file is immutable between analyze and redo in production. If it does
// change, the descriptor must fail closed instead of shifting positional table
// metadata onto a different DML record.
TEST(RecoveryManagerTest, WalMutationAfterAnalyzeFailsClosed) {
    ScopedTestDir test_dir("recovery_redo_wal_mutation_root");
    const std::string db_name = "recovery_redo_wal_mutation_db";
    CreateRecoveryTestDb(db_name, {"t", "u"});
    const Rid rid{1, 0};
    auto t_rec = MakeTuple(1, 10);
    auto u_rec = MakeTuple(2, 20);

    OpenRecoveryDb db(db_name);
    auto lsn = AppendBegin(*db.log_mgr_, 100);
    lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, t_rec, "t");
    lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, u_rec, "u");
    AppendCommit(*db.log_mgr_, 100, lsn);
    FlushLogs(*db.log_mgr_);

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    const auto dml_offsets = WalRecordOffsets(db.disk_, LogType::INSERT);
    ASSERT_EQ(dml_offsets.size(), 2u);
    const int64_t wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
    ASSERT_GT(wal_size, 0);
    const LogType retyped = LogType::CHECKPOINT;
    PatchWalBytes(LOG_FILE_NAME, dml_offsets[0] + OFFSET_LOG_TYPE, &retyped, sizeof(LogType));

    EXPECT_THROW(recovery.redo(), InternalError);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid, "t"));
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid, "u"));
}

// A compact heap descriptor must bind the complete serialized DML record, not
// just its offset, header and RID. This keeps a same-length row-image rewrite
// from being replayed after analyze (the INDEX_SMO companion test below covers
// the stronger case where the mutated record's own CRC is recomputed).
TEST(RecoveryManagerTest, WalPayloadMutationAfterAnalyzeFailsClosed) {
    ScopedTestDir test_dir("recovery_redo_wal_payload_mutation_root");
    const std::string db_name = "recovery_redo_wal_payload_mutation_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto tuple = MakeTuple(1, 10);

    OpenRecoveryDb db(db_name);
    auto lsn = AppendBegin(*db.log_mgr_, 100);
    lsn = AppendInsert(*db.log_mgr_, 100, lsn, rid, tuple, "t");
    AppendCommit(*db.log_mgr_, 100, lsn);
    FlushLogs(*db.log_mgr_);

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    const auto dml_offsets = WalRecordOffsets(db.disk_, LogType::INSERT);
    ASSERT_EQ(dml_offsets.size(), 1u);
    const int replacement = 11;
    PatchWalBytes(LOG_FILE_NAME, dml_offsets.front() + OFFSET_LOG_DATA + static_cast<int64_t>(sizeof(int)),
                  &replacement, sizeof(replacement));

    EXPECT_THROW(recovery.redo(), InternalError);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
}

TEST(RecoveryManagerTest, LoserNoIndexPayloadMutationAfterAnalyzeRetainsWal) {
    ScopedTestDir test_dir("recovery_loser_no_index_identity_root");
    const std::string db_name = "recovery_loser_no_index_identity_db";
    CreateRecoveryTestDb(db_name, {"t"}, false);
    const Rid rid{1, 0};
    auto tuple = MakeTuple(1, 10);

    OpenRecoveryDb db(db_name);
    const txn_id_t txn_id = 100;
    const lsn_t begin_lsn = AppendBegin(*db.log_mgr_, txn_id);
    const lsn_t insert_lsn = AppendInsert(*db.log_mgr_, txn_id, begin_lsn, rid, tuple, "t");
    AppendAbort(*db.log_mgr_, txn_id, insert_lsn);
    FlushLogs(*db.log_mgr_);

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    recovery.analyze();
    const auto dml_offsets = WalRecordOffsets(db.disk_, LogType::INSERT);
    ASSERT_EQ(dml_offsets.size(), 1u);
    const int replacement = 11;
    PatchWalBytes(LOG_FILE_NAME, dml_offsets.front() + OFFSET_LOG_DATA + static_cast<int64_t>(sizeof(int)),
                  &replacement, sizeof(replacement));
    const int64_t wal_size = db.disk_.get_file_size(LOG_FILE_NAME);

    EXPECT_THROW(recovery.undo(), InternalError);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
}

// The WAL intentionally alternates page 2 and page 1. Grouped redo must process
// page 1 first, but must retain INSERT-before-UPDATE order within each page.
TEST(RecoveryManagerTest, HeapRedoGroupsPagesAndPreservesWalOrderWithinEachPage) {
    ScopedTestDir test_dir("recovery_grouped_heap_redo_root");
    const std::string db_name = "recovery_grouped_heap_redo_db";
    CreateRecoveryTestDb(db_name);
    const Rid page_one{1, 0};
    const Rid page_two{2, 0};
    auto page_one_before = MakeTuple(1, 10);
    auto page_one_after = MakeTuple(1, 11);
    auto page_two_before = MakeTuple(2, 20);
    auto page_two_after = MakeTuple(2, 21);

    {
        OpenRecoveryDb db(db_name);
        auto* file_handle = db.sm_mgr_.fhs_.at("t").get();
        for (int page_no = 1; page_no <= 2; ++page_no) {
            auto page = file_handle->create_new_page_handle();
            ASSERT_EQ(page.page->get_page_id().page_no, page_no);
            ASSERT_TRUE(db.bpm_.unpin_page(page.page->get_page_id(), true));
        }
        ASSERT_TRUE(db.bpm_.flush_all_pages(file_handle->GetFd()));

        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, page_two, page_two_before);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, page_one, page_one_before);
        lsn = AppendUpdate(*db.log_mgr_, 100, lsn, page_two, page_two_before, page_two_after);
        lsn = AppendUpdate(*db.log_mgr_, 100, lsn, page_one, page_one_before, page_one_after);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    {
        OpenRecoveryDb db(db_name);
        db.bpm_.set_log_manager(db.log_mgr_.get());
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        recovery.analyze();
        recovery.redo();
        EXPECT_EQ(recovery.get_redo_applied_count(), 4u);
        EXPECT_EQ(recovery.get_redo_resident_page_run_count(), 2u);
        EXPECT_EQ(recovery.get_redo_resident_page_pin_count(), 2u);
        recovery.undo();
    }

    OpenRecoveryDb db(db_name);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, page_one));
    EXPECT_EQ(RecordValue(db.sm_mgr_, page_one), 11);
    ASSERT_TRUE(RecordExists(db.sm_mgr_, page_two));
    EXPECT_EQ(RecordValue(db.sm_mgr_, page_two), 21);
}

TEST(RecoveryManagerTest, ReversePageWalOrderExtendsExactRidsAndKeepsFreeListIdempotent) {
    ScopedTestDir test_dir("recovery_reverse_page_exact_rid_root");
    const std::string db_name = "recovery_reverse_page_exact_rid_db";
    CreateRecoveryTestDb(db_name);
    const Rid page_one{1, 1};
    const Rid page_two{2, 3};
    auto page_one_rec = MakeTuple(1, 10);
    auto page_two_rec = MakeTuple(2, 20);

    {
        OpenRecoveryDb db(db_name);
        ASSERT_EQ(db.sm_mgr_.fhs_.at("t")->get_file_hdr().num_pages, 1);
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, page_two, page_two_rec);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, page_one, page_one_rec);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
    }

    const auto verify_recovered_state = [&] {
        OpenRecoveryDb db(db_name);
        auto* file_handle = db.sm_mgr_.fhs_.at("t").get();
        const RmFileHdr file_hdr = file_handle->get_file_hdr();
        ASSERT_EQ(file_hdr.num_pages, 3);
        ASSERT_TRUE(RecordExists(db.sm_mgr_, page_one));
        EXPECT_EQ(RecordValue(db.sm_mgr_, page_one), 10);
        ASSERT_TRUE(RecordExists(db.sm_mgr_, page_two));
        EXPECT_EQ(RecordValue(db.sm_mgr_, page_two), 20);
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 1, page_one));
        EXPECT_TRUE(IndexPointsTo(db.sm_mgr_, 2, page_two));

        std::set<page_id_t> free_pages;
        page_id_t page_no = file_hdr.first_free_page_no;
        int steps = 0;
        while (page_no != RM_NO_PAGE && steps < file_hdr.num_pages) {
            ASSERT_GE(page_no, RM_FIRST_RECORD_PAGE);
            ASSERT_LT(page_no, file_hdr.num_pages);
            ASSERT_TRUE(free_pages.insert(page_no).second) << "free-list contains a cycle";

            RmPageHandle page_handle = file_handle->fetch_page_handle(page_no);
            page_id_t next_free_page_no = RM_NO_PAGE;
            {
                std::shared_lock<std::shared_mutex> page_lock(page_handle.page->latch());
                EXPECT_EQ(page_handle.page_hdr->num_records, 1);
                EXPECT_LT(page_handle.page_hdr->num_records, file_hdr.num_records_per_page);
                next_free_page_no = page_handle.page_hdr->next_free_page_no;
            }
            ASSERT_TRUE(db.bpm_.unpin_page(page_handle.page->get_page_id(), false));
            page_no = next_free_page_no;
            ++steps;
        }
        EXPECT_EQ(page_no, RM_NO_PAGE) << "free-list did not terminate";
        EXPECT_EQ(free_pages, (std::set<page_id_t>{1, 2}));
        EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), 0);
    };

    RunRecovery(db_name);
    verify_recovered_state();
    RunRecovery(db_name);
    verify_recovered_state();
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

    {
        OpenRecoveryDb db(db_name);
        db.log_mgr_->prepare_existing_log();
        RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
        EXPECT_THROW(recovery.analyze(), InternalError);
        // finalize is not reached, so a complete semantic corruption cannot
        // be silently converted into a physical-tail truncation.
        EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
    }

    OpenRecoveryDb db(db_name);
    // Retained in full, so the next process retries from the same input rather
    // than starting up on a database missing a committed row.
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
    EXPECT_FALSE(RecordExists(db.sm_mgr_, rid));
}

TEST(RecoveryManagerTest, EarlierReduceFailureWinsOverLaterWorkerFailureAndKeepsStagedState) {
    ScopedTestDir test_dir("recovery_reduce_failure_precedence_root");
    const std::string db_name = "recovery_reduce_failure_precedence_db";
    CreateRecoveryTestDb(db_name);
    const Rid rid{1, 0};
    auto rec = MakeTuple(1, 10);

    OpenRecoveryDb db(db_name);
    const lsn_t first_lsn = AppendBegin(*db.log_mgr_, 100);
    FlushLogs(*db.log_mgr_);

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    ASSERT_NO_THROW(recovery.analyze());
    const uint64_t old_scanned = recovery.get_scanned_record_count();
    const lsn_t old_max_lsn = recovery.get_max_lsn();
    const uint64_t old_dml = recovery.get_dml_record_count();

    const lsn_t second_lsn = AppendBegin(*db.log_mgr_, 200);
    const lsn_t insert_lsn = AppendInsert(*db.log_mgr_, 200, second_lsn, rid, rec);
    FlushLogs(*db.log_mgr_);
    const auto begin_offsets = WalRecordOffsets(db.disk_, LogType::BEGIN);
    const auto insert_offsets = WalRecordOffsets(db.disk_, LogType::INSERT);
    ASSERT_EQ(begin_offsets.size(), 2u);
    ASSERT_EQ(insert_offsets.size(), 1u);
    const int impossible_image_size = 1 << 28;
    PatchWalBytes(LOG_FILE_NAME, begin_offsets[1] + OFFSET_LSN, &first_lsn, sizeof(first_lsn));
    PatchWalBytes(LOG_FILE_NAME, insert_offsets[0] + OFFSET_LOG_DATA, &impossible_image_size,
                  sizeof(impossible_image_size));

    try {
        recovery.analyze();
        FAIL() << "analyze accepted an invalid ordered prefix";
    } catch (const InternalError& error) {
        EXPECT_NE(std::string(error.what()).find("non-increasing WAL LSN"), std::string::npos);
    }
    EXPECT_EQ(recovery.get_scanned_record_count(), old_scanned);
    EXPECT_EQ(recovery.get_max_lsn(), old_max_lsn);
    EXPECT_EQ(recovery.get_dml_record_count(), old_dml);

    PatchWalBytes(LOG_FILE_NAME, begin_offsets[1] + OFFSET_LSN, &second_lsn, sizeof(second_lsn));
    const int image_size = rec.size;
    PatchWalBytes(LOG_FILE_NAME, insert_offsets[0] + OFFSET_LOG_DATA, &image_size, sizeof(image_size));
    ASSERT_NO_THROW(recovery.analyze());
    EXPECT_EQ(recovery.get_scanned_record_count(), 3u);
    EXPECT_EQ(recovery.get_max_lsn(), insert_lsn);
    EXPECT_EQ(recovery.get_dml_record_count(), 1u);
}

TEST(RecoveryManagerTest, ProductionPrepareAnalyzeFinalizeDefersTornTailTruncation) {
    ScopedTestDir test_dir("recovery_prepare_finalize_torn_tail_root");
    const std::string db_name = "recovery_prepare_finalize_torn_tail_db";
    CreateRecoveryTestDb(db_name);
    int64_t physical_torn_size = 0;

    {
        OpenRecoveryDb db(db_name);
        const lsn_t begin_lsn = AppendBegin(*db.log_mgr_, 100);
        CheckpointLogRecord checkpoint(std::unordered_map<txn_id_t, lsn_t>{{100, begin_lsn}});
        db.log_mgr_->add_log_to_buffer(&checkpoint);
        FlushLogs(*db.log_mgr_);
        const int64_t complete_size = db.disk_.get_file_size(LOG_FILE_NAME);
        ASSERT_GT(complete_size, LOG_HEADER_SIZE + 1);
        physical_torn_size = complete_size - 1;
        ASSERT_EQ(::truncate(LOG_FILE_NAME.c_str(), physical_torn_size), 0);
    }

    OpenRecoveryDb db(db_name);
    db.log_mgr_->prepare_existing_log();
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    ASSERT_NO_THROW(recovery.analyze());
    const int64_t accepted_end = recovery.get_scan_end_offset();
    EXPECT_LT(accepted_end, physical_torn_size);
    // prepare/analyze never alter the physical WAL, so a failed semantic pass
    // would still have the exact original bytes to retry.
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), physical_torn_size);

    ASSERT_NO_THROW(
        db.log_mgr_->finalize_existing_log(accepted_end, recovery.get_max_lsn(), recovery.get_latest_index_bindings()));
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), accepted_end);
    EXPECT_EQ(db.log_mgr_->current_log_offset(), accepted_end);
    BeginLogRecord after_finalize(102);
    EXPECT_EQ(db.log_mgr_->add_log_to_buffer(&after_finalize), recovery.get_max_lsn() + 1);
}

TEST(RecoveryManagerTest, CheckpointInvalidLengthModuloPastEofFailsClosed) {
    ScopedTestDir test_dir("recovery_checkpoint_invalid_modulo_root");
    const std::string db_name = "recovery_checkpoint_invalid_modulo_db";
    CreateRecoveryTestDb(db_name);
    int64_t checkpoint_offset = 0;
    int64_t physical_size = 0;

    {
        OpenRecoveryDb db(db_name);
        const lsn_t begin_lsn = AppendBegin(*db.log_mgr_, 100);
        CheckpointLogRecord checkpoint(std::unordered_map<txn_id_t, lsn_t>{{100, begin_lsn}});
        checkpoint_offset = db.log_mgr_->current_log_offset();
        db.log_mgr_->add_log_to_buffer(&checkpoint);
        FlushLogs(*db.log_mgr_);
        const uint32_t invalid_length = checkpoint.log_tot_len_ + 1;
        PatchWalBytes(LOG_FILE_NAME, checkpoint_offset + OFFSET_LOG_TOT_LEN, &invalid_length,
                      sizeof(invalid_length));
        physical_size = checkpoint_offset + LOG_HEADER_SIZE;
        ASSERT_EQ(::truncate(LOG_FILE_NAME.c_str(), physical_size), 0);
    }

    OpenRecoveryDb db(db_name);
    db.log_mgr_->prepare_existing_log();
    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
    EXPECT_THROW(recovery.analyze(), InternalError);
    EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), physical_size);
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
        index->insert_entry(MakeIntKey(1).data(), rid, IndexWriteWalContext::TestNoWal(), true);
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

TEST(RecoveryFaultInjectionTest, HeapRunEveryRecordProcessDeathIsRecoverable) {
#ifndef RMDB_ENABLE_FAULT_INJECTION
    GTEST_SKIP() << "requires RMDB_ENABLE_FAULT_INJECTION";
#else
    ScopedTestDir test_dir("recovery_fault_heap_run_partial_pwrite_root");
    const std::string db_name = "recovery_fault_heap_run_partial_pwrite_db";
    CreateRecoveryTestDb(db_name, {"t"}, false);
    const Rid first{1, 0};
    const Rid second{2, 0};
    auto first_tuple = MakeTuple(1, 10);
    auto second_tuple = MakeTuple(2, 20);
    int64_t wal_size = 0;
    {
        OpenRecoveryDb db(db_name);
        RmFileHandle* file_handle = db.sm_mgr_.fhs_.at("t").get();
        for (page_id_t page_no = 1; page_no <= 2; ++page_no) {
            RmPageHandle page = file_handle->create_new_page_handle();
            ASSERT_EQ(page.page->get_page_id().page_no, page_no);
            ASSERT_TRUE(db.bpm_.unpin_page(page.page->get_page_id(), true));
        }
        ASSERT_TRUE(db.bpm_.flush_all_pages(file_handle->GetFd()));
        auto lsn = AppendBegin(*db.log_mgr_, 100);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, first, first_tuple);
        lsn = AppendInsert(*db.log_mgr_, 100, lsn, second, second_tuple);
        AppendCommit(*db.log_mgr_, 100, lsn);
        FlushLogs(*db.log_mgr_);
        wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
    }

    const char* old_workers_value = getenv("RMDB_RECOVERY_WORKERS");
    const bool had_old_workers = old_workers_value != nullptr;
    const std::string old_workers = had_old_workers ? old_workers_value : "";
    struct HeapRunWorkersGuard {
        bool had_old;
        std::string old;
        ~HeapRunWorkersGuard() {
            if (had_old) {
                setenv("RMDB_RECOVERY_WORKERS", old.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
    } workers_guard{had_old_workers, old_workers};
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "1", 1), 0);

    // One frame makes the second page-run fetch synchronously pwrite the first
    // run's dirty page.  Crash after applying the second run but before its
    // run-final dirty publication: exactly one committed page is durable.
    const int status = RunRecoveryAfterInjectedProcessDeath(db_name, "mid_recovery_redo", true, 1, 1);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 137);
    {
        OpenRecoveryDb interrupted(db_name, 1);
        RmFileHandle* file_handle = interrupted.sm_mgr_.fhs_.at("t").get();
        const RmFileHdr file_header = file_handle->get_file_hdr();
        const size_t bitmap_offset = RM_PAGE_META_OFFSET +
                                     static_cast<size_t>(file_header.num_records_per_page) * sizeof(TupleMeta);
        size_t committed_pages = 0;
        for (page_id_t page_no = 1; page_no <= 2; ++page_no) {
            std::array<char, PAGE_SIZE> bytes{};
            interrupted.disk_.read_page(file_handle->GetFd(), page_no, bytes.data(), PAGE_SIZE);
            if (Bitmap::is_set(bytes.data() + bitmap_offset, 0)) {
                ++committed_pages;
            }
        }
        EXPECT_EQ(committed_pages, 1u);
        EXPECT_EQ(interrupted.disk_.get_file_size(LOG_FILE_NAME), wal_size);
    }
    RunRecovery(db_name);
    RunRecovery(db_name);
    OpenRecoveryDb verified(db_name);
    ASSERT_TRUE(RecordExists(verified.sm_mgr_, first));
    ASSERT_TRUE(RecordExists(verified.sm_mgr_, second));
    EXPECT_EQ(RecordValue(verified.sm_mgr_, first), 10);
    EXPECT_EQ(RecordValue(verified.sm_mgr_, second), 20);
#endif
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

TEST(RecoveryFaultInjectionTest, PageFinalizeThrowFaultsRetainWalAndRemainReentrant) {
#ifndef RMDB_ENABLE_FAULT_INJECTION
    GTEST_SKIP() << "requires RMDB_ENABLE_FAULT_INJECTION";
#endif
    ScopedTestDir test_dir("recovery_fault_page_finalize_throw_root");
    const std::array<const char*, 6> fault_points{
        "during_data_page_pwrite",
        "during_data_page_pread",
        "during_recovery_page_normalize",
        "during_recovery_page_publish",
        "mid_recovery_page_finalize",
        "post_recovery_page_finalize",
    };
    ScopedEnvironmentValue workers("RMDB_RECOVERY_WORKERS", "8");
    for (size_t fault_index = 0; fault_index < fault_points.size(); ++fault_index) {
        SCOPED_TRACE(fault_points[fault_index]);
        const std::string db_name = "recovery_fault_page_finalize_throw_" + std::to_string(fault_index);
        CreatePhysicalFinalizeSeed(db_name);
        {
            OpenRecoveryDb db(db_name, 2);
            RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, db.log_mgr_.get());
            recovery.analyze();
            recovery.prepare_pages_for_redo();
            recovery.redo();
            InstallDirtyFinalizeVictims(db);
            const int64_t wal_size = db.disk_.get_file_size(LOG_FILE_NAME);
            ASSERT_GT(wal_size, 0);
            {
                ScopedThrowFaultPoint fault(fault_points[fault_index]);
                EXPECT_ANY_THROW(recovery.undo());
            }
            EXPECT_EQ(db.disk_.get_file_size(LOG_FILE_NAME), wal_size);
        }

        EXPECT_NO_THROW(RunPreparedRecovery(db_name, 2));
        EXPECT_NO_THROW(RunPreparedRecovery(db_name, 2));
        OpenRecoveryDb verified(db_name, 2);
        for (page_id_t page_no = 1; page_no <= 2; ++page_no) {
            const Rid rid{page_no, 0};
            ASSERT_TRUE(RecordExists(verified.sm_mgr_, rid));
            EXPECT_EQ(RecordValue(verified.sm_mgr_, rid), 8000 + page_no);
            EXPECT_TRUE(IndexColumnPointsTo(verified.sm_mgr_, "id", 800 + page_no, rid));
            EXPECT_TRUE(IndexColumnPointsTo(verified.sm_mgr_, "v", 8000 + page_no, rid));
        }
        EXPECT_EQ(verified.disk_.get_file_size(LOG_FILE_NAME), 0);
    }
}

TEST(RecoveryFaultInjectionTest, FinalizeProcessDeathRetainsWalAndRecoveryIsReentrant) {
#ifndef RMDB_ENABLE_FAULT_INJECTION
    GTEST_SKIP() << "requires RMDB_ENABLE_FAULT_INJECTION";
#endif
    ScopedTestDir test_dir("recovery_fault_finalize_root");
    const std::string db_name = "recovery_fault_finalize_db";
    CreateRecoveryTestDb(db_name);
    {
        OpenRecoveryDb db(db_name);
        lsn_t lsn = AppendBegin(*db.log_mgr_, 300);
        for (page_id_t page_no = 1; page_no <= 9; ++page_no) {
            auto record = MakeTuple(300 + page_no, 3000 + page_no);
            lsn = AppendInsert(*db.log_mgr_, 300, lsn, Rid{page_no, page_no % 2}, record);
        }
        AppendCommit(*db.log_mgr_, 300, lsn);
        FlushLogs(*db.log_mgr_);
    }

    const char* previous_workers_value = getenv("RMDB_RECOVERY_WORKERS");
    const bool had_previous_workers = previous_workers_value != nullptr;
    const std::string previous_workers = had_previous_workers ? previous_workers_value : "";
    struct WorkersEnvGuard {
        bool had_previous;
        std::string previous;
        ~WorkersEnvGuard() {
            if (had_previous) {
                setenv("RMDB_RECOVERY_WORKERS", previous.c_str(), 1);
            } else {
                unsetenv("RMDB_RECOVERY_WORKERS");
            }
        }
    } workers_guard{had_previous_workers, previous_workers};
    ASSERT_EQ(setenv("RMDB_RECOVERY_WORKERS", "1", 1), 0);

    // With one worker and one frame, fetching the second page must evict and
    // pwrite the first dirty, normalized page. Crash after normalizing page two
    // but before it is unpinned or any table metadata is published. This makes
    // page one the uniquely provable durable victim instead of depending on a
    // replacer's choice between two eligible frames.
    const int status =
        RunRecoveryAfterInjectedProcessDeath(db_name, "post_recovery_page_finalize", true, 1, 1);
#ifdef RMDB_ENABLE_FAULT_INJECTION
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 137);
#else
    (void)status;
#endif
    {
        OpenRecoveryDb interrupted(db_name);
        EXPECT_GT(interrupted.disk_.get_file_size(LOG_FILE_NAME), 0);
        RmFileHandle* file_handle = interrupted.sm_mgr_.fhs_.at("t").get();
        size_t normalized_on_disk = 0;
        for (page_id_t page_no = 1; page_no <= 9; ++page_no) {
            std::array<char, PAGE_SIZE> page{};
            interrupted.disk_.read_page(file_handle->GetFd(), page_no, page.data(), PAGE_SIZE);
            TupleMeta meta;
            const int slot_no = page_no % 2;
            std::memcpy(&meta, page.data() + RM_PAGE_META_OFFSET + slot_no * sizeof(TupleMeta), sizeof(meta));
            if (meta.writer_txn_id_ == INVALID_TXN_ID && meta.is_committed_ && !meta.is_deleted_ &&
                meta.version_chain_head_ == UndoLink{}) {
                ++normalized_on_disk;
            }
        }
        EXPECT_EQ(normalized_on_disk, 1u);
    }

    RunPreparedRecovery(db_name);
    RunPreparedRecovery(db_name);

    OpenRecoveryDb verified(db_name);
    RmFileHandle* file_handle = verified.sm_mgr_.fhs_.at("t").get();
    const RmFileHdr header = file_handle->get_file_hdr();
    ASSERT_EQ(header.num_pages, 10);
    std::set<page_id_t> free_pages;
    for (page_id_t page_no = header.first_free_page_no; page_no != RM_NO_PAGE;) {
        ASSERT_TRUE(free_pages.insert(page_no).second) << "free-list contains a cycle";
        RmPageHandle page = file_handle->fetch_page_handle(page_no);
        page_no = page.page_hdr->next_free_page_no;
        ASSERT_TRUE(verified.bpm_.unpin_page(page.page->get_page_id(), false));
    }
    EXPECT_EQ(free_pages.size(), 9u);
    for (page_id_t page_no = 1; page_no <= 9; ++page_no) {
        const Rid rid{page_no, page_no % 2};
        ASSERT_TRUE(RecordExists(verified.sm_mgr_, rid));
        EXPECT_EQ(RecordId(verified.sm_mgr_, rid), 300 + page_no);
        EXPECT_EQ(RecordValue(verified.sm_mgr_, rid), 3000 + page_no);
        EXPECT_TRUE(IndexPointsTo(verified.sm_mgr_, 300 + page_no, rid));
        const TupleMeta meta = file_handle->get_tuple_meta(rid);
        EXPECT_EQ(meta.commit_ts_, 0);
        EXPECT_EQ(meta.writer_txn_id_, INVALID_TXN_ID);
        EXPECT_TRUE(meta.is_committed_);
        EXPECT_FALSE(meta.is_deleted_);
        EXPECT_EQ(meta.version_chain_head_, UndoLink{});
    }
    EXPECT_EQ(verified.disk_.get_file_size(LOG_FILE_NAME), 0);
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
