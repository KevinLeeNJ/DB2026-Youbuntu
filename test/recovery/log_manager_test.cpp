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
#include "recovery/index_smo_log.h"
#include "execution/executor_delete.h"
#include "execution/executor_insert.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record/rm.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

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
        if (log_header.log_type_ == LogType::INDEX_BIND || log_header.log_type_ == LogType::INDEX_SMO) {
            offset += static_cast<int64_t>(log_header.log_tot_len_);
            continue;
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

class ScopedEnvVar {
public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            previous_ = previous;
        }
        const int result = value == nullptr ? unsetenv(name) : setenv(name, value, 1);
        if (result != 0) {
            throw std::runtime_error("environment update failed");
        }
    }

    ~ScopedEnvVar() {
        if (previous_.has_value()) {
            setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

private:
    std::string name_;
    std::optional<std::string> previous_;
};

} // namespace

TEST(LogManagerTest, DefaultsToStrictDurability) {
    DiskManager disk;
    LogManager log_mgr(&disk);
    EXPECT_EQ(log_mgr.durability_mode(), DurabilityMode::STRICT);
}

TEST(IndexSmoWalTest, RoundTripAndChecksumAreBounded) {
    IndexSmoWalData data;
    data.index_file_name = "t_id.idx";
    data.index_generation = 7;
    data.pages.resize(2);
    data.pages[0].page_no = 2;
    data.pages[1].page_no = 9;
    std::fill(data.pages[0].bytes.begin(), data.pages[0].bytes.end(), 'a');
    std::fill(data.pages[1].bytes.begin(), data.pages[1].bytes.end(), 'b');
    data.header.fill('h');

    IndexSmoLogRecord record(data);
    record.lsn_ = 11;
    std::vector<char> bytes(record.log_tot_len_);
    record.serialize(bytes.data());
    WalRecordView raw;
    raw.bytes = bytes.data();
    raw.total_len = record.log_tot_len_;
    raw.log_type = LogType::INDEX_SMO;
    raw.lsn = record.lsn_;
    raw.txn_id = record.log_tid_;
    raw.prev_lsn = record.prev_lsn_;

    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(raw, &parsed));
    EXPECT_EQ(parsed.index_file_name, data.index_file_name);
    EXPECT_EQ(parsed.index_generation, 7U);
    EXPECT_EQ(parsed.page_count, 2U);
    EXPECT_EQ(parsed.page_no(1), 9);
    EXPECT_EQ(std::memcmp(parsed.page_image(0), data.pages[0].bytes.data(), PAGE_SIZE), 0);

    bytes[OFFSET_LOG_DATA + 3] ^= 1;
    EXPECT_FALSE(ParseIndexSmoWal(raw, &parsed));
}

TEST(IndexSmoWalTest, AppendSupportsARecordLargerThanTheOrdinaryBuffer) {
    ScopedTestDir test_dir("large_index_smo_wal_test");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);
    IndexSmoWalData data;
    data.index_file_name = "large.idx";
    data.header.fill(0);
    const size_t page_count = static_cast<size_t>(LOG_BUFFER_SIZE / PAGE_SIZE) + 8;
    data.pages.resize(page_count);
    for (size_t i = 0; i < page_count; ++i) {
        data.pages[i].page_no = static_cast<page_id_t>(i + 1);
        uint32_t state = static_cast<uint32_t>(i + 1) * 0x9e3779b9U;
        for (char& byte : data.pages[i].bytes) {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            byte = static_cast<char>(state);
        }
    }

    EXPECT_EQ(log_mgr.get_fsync_count(), 0U);
    EXPECT_GT(log_mgr.ensure_index_binding(data.index_file_name), 0U);
    const uint64_t binding_fsyncs = log_mgr.get_fsync_count();
    EXPECT_EQ(binding_fsyncs, 1U);

    const lsn_t smo_lsn = log_mgr.append_index_smo(data);
    EXPECT_GE(smo_lsn, 1);
    EXPECT_EQ(log_mgr.get_fsync_count(), binding_fsyncs);
    EXPECT_LT(log_mgr.get_durable_lsn(), smo_lsn);

    log_mgr.flush_log_to_disk_up_to(smo_lsn);
    EXPECT_GE(log_mgr.get_durable_lsn(), smo_lsn);
    EXPECT_GT(log_mgr.get_fsync_count(), binding_fsyncs);
    const int64_t wal_bytes = disk.get_file_size(LOG_FILE_NAME);
    EXPECT_GT(wal_bytes, LOG_BUFFER_SIZE);

    WalReader reader(&disk, 0, wal_bytes);
    WalRecordView record;
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.log_type, LogType::INDEX_BIND);
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.log_type, LogType::INDEX_SMO);
    IndexSmoWalView parsed;
    ASSERT_TRUE(ParseIndexSmoWal(record, &parsed));
    EXPECT_EQ(parsed.page_count, page_count);
    EXPECT_FALSE(reader.next(&record));
    EXPECT_EQ(reader.next_offset(), wal_bytes);
}

TEST(IndexSmoWalTest, CompleteChecksumCorruptionFailsAndRetainsWal) {
    ScopedTestDir test_dir("corrupt_index_smo_wal_test");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager writer(&disk);
    IndexSmoWalData data;
    data.index_file_name = "corrupt.idx";
    data.pages.resize(1);
    data.pages[0].page_no = 2;
    const lsn_t smo_lsn = writer.append_index_smo(data);
    writer.flush_log_to_disk_up_to_durable(smo_lsn);
    const int64_t original_size = disk.get_file_size(LOG_FILE_NAME);
    ASSERT_GT(original_size, 0);

    std::fstream wal(LOG_FILE_NAME, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(wal.is_open());
    wal.seekg(original_size - 1);
    char byte = 0;
    wal.read(&byte, 1);
    byte ^= 1;
    wal.seekp(original_size - 1);
    wal.write(&byte, 1);
    wal.flush();
    ASSERT_TRUE(static_cast<bool>(wal));

    LogManager restarted(&disk);
    EXPECT_THROW(restarted.initialize_from_existing_log(), InternalError);
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), original_size);
}

TEST(IndexSmoWalTest, TruncatedPhysicalTailStopsAtAndTruncatesToTheLastCompleteRecord) {
    ScopedTestDir test_dir("truncated_index_smo_wal_test");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager writer(&disk);
    IndexSmoWalData data;
    data.index_file_name = "truncated.idx";
    data.pages.resize(1);
    data.pages[0].page_no = 2;
    const lsn_t smo_lsn = writer.append_index_smo(data);
    writer.flush_log_to_disk_up_to_durable(smo_lsn);
    const int64_t complete_size = disk.get_file_size(LOG_FILE_NAME);
    ASSERT_GT(complete_size, 32);
    ASSERT_EQ(::truncate(LOG_FILE_NAME.c_str(), complete_size - 17), 0);

    LogManager restarted(&disk);
    EXPECT_NO_THROW(restarted.initialize_from_existing_log());
    const int64_t retained_size = disk.get_file_size(LOG_FILE_NAME);
    EXPECT_GT(retained_size, 0);
    EXPECT_LT(retained_size, complete_size - 17);
    WalReader reader(&disk, 0, retained_size);
    WalRecordView record;
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.log_type, LogType::INDEX_BIND);
    EXPECT_FALSE(reader.next(&record));
    EXPECT_EQ(reader.next_offset(), retained_size);
}

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

TEST(LogRecordTest, SparseUpdateRoundTripMaterializesTheCompleteBeforeImage) {
    std::string old_text(128, 'a');
    std::string new_text = old_text;
    new_text[10] = 'x';
    new_text[13] = 'y';
    new_text[50] = 'z';
    auto old_rec = MakeRecord(old_text);
    auto new_rec = MakeRecord(new_text);
    Rid rid{9, 4};
    UpdateLogRecord log(89, old_rec, new_rec, rid, "customer");

    ASSERT_TRUE(log.sparse_encoding_);
    ASSERT_EQ(log.before_spans_.size(), 2U);
    EXPECT_EQ(log.before_spans_[0].offset, 10U);
    EXPECT_EQ(log.before_spans_[0].length, 4U); // two equal bytes are cheaper than a second span header
    const size_t legacy_bytes = LOG_HEADER_SIZE + sizeof(int) * 2 + old_text.size() + new_text.size() + sizeof(Rid) +
                                sizeof(size_t) + std::string("customer").size();
    EXPECT_LT(log.log_tot_len_, legacy_bytes);

    std::vector<char> bytes(log.log_tot_len_);
    log.serialize(bytes.data());
    EXPECT_EQ(read_unaligned<int>(bytes.data() + OFFSET_LOG_DATA), UpdateLogRecord::kSparseBeforeVersion);

    UpdateLogRecord decoded;
    decoded.deserialize(bytes.data());
    EXPECT_EQ(decoded.rid_, rid);
    EXPECT_EQ(decoded.table_name_, "customer");
    ExpectRecordEq(decoded.old_value_, old_rec);
    ExpectRecordEq(decoded.new_value_, new_rec);
}

TEST(LogRecordTest, UpdateFallsBackToLegacyWhenSparseEncodingIsNotStrictlySmaller) {
    auto old_rec = MakeRecord(std::string(64, 'a'));
    auto new_rec = MakeRecord(std::string(64, 'b'));
    Rid rid{10, 2};
    UpdateLogRecord log(90, old_rec, new_rec, rid, "stock");

    EXPECT_FALSE(log.sparse_encoding_);
    std::vector<char> bytes(log.log_tot_len_);
    log.serialize(bytes.data());
    EXPECT_EQ(read_unaligned<int>(bytes.data() + OFFSET_LOG_DATA), old_rec.size);
}

TEST(LogRecordTest, UpdateRejectsNegativeImagesAndOversizedRecordsBeforeLengthNarrowing) {
    auto old_rec = MakeRecord("a");
    auto new_rec = MakeRecord("b");
    Rid rid{10, 3};

    old_rec.size = -1;
    EXPECT_THROW(UpdateLogRecord(91, old_rec, new_rec, rid, "stock"), std::length_error);
    old_rec.size = 1;

    std::string oversized_name(MAX_INDEX_SMO_RECORD_BYTES, 't');
    EXPECT_THROW(UpdateLogRecord(91, old_rec, new_rec, rid, std::move(oversized_name)), std::length_error);
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
    EXPECT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);

    log_mgr.flush_log_to_disk_with_sync();
    EXPECT_EQ(log_mgr.get_durable_lsn(), commit_lsn);

    std::vector<char> first(LOG_HEADER_SIZE);
    ASSERT_EQ(disk.read_log(first.data(), static_cast<int>(first.size()), 0), LOG_HEADER_SIZE);
    auto decoded = DeserializeLogRecord(first.data(), static_cast<int>(first.size()));
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->log_type_, LogType::BEGIN);
    EXPECT_EQ(decoded->lsn_, begin_lsn);
}

TEST(LogManagerTest, FlushDurableUpToHonorsPageLsnTarget) {
    ScopedTestDir test_dir("log_manager_flush_up_to_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    BeginLogRecord begin(101);
    lsn_t begin_lsn = log_mgr.add_log_to_buffer(&begin);
    EXPECT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);

    log_mgr.flush_log_to_disk_up_to(begin_lsn);

    EXPECT_EQ(log_mgr.get_durable_lsn(), begin_lsn);
    EXPECT_GE(log_mgr.get_persist_lsn(), begin_lsn);
}

TEST(LogManagerTest, CurrentOffsetIncludesBufferedWal) {
    ScopedTestDir test_dir("log_manager_buffered_offset_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    BeginLogRecord begin(103);
    log_mgr.add_log_to_buffer(&begin);

    EXPECT_GE(log_mgr.current_log_offset(), static_cast<int64_t>(begin.log_tot_len_));
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), 0);

    log_mgr.flush_log_to_disk();
    EXPECT_EQ(log_mgr.current_log_offset(), disk.get_file_size(LOG_FILE_NAME));
}

TEST(LogManagerTest, AbortPageEvictionFlushesWalThroughPageLsn) {
    ScopedTestDir test_dir("abort_page_eviction_wal_barrier_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    disk.create_file("pages");
    const int page_fd = disk.open_file("pages");
    LogManager log_mgr(&disk);

    AbortLogRecord abort_record(104);
    const lsn_t abort_lsn = log_mgr.add_log_to_buffer(&abort_record);
    {
        BufferPoolManager bpm(1, &disk);
        bpm.set_log_manager(&log_mgr);

        PageId aborted_page{page_fd, INVALID_PAGE_ID};
        Page* page = bpm.new_page(&aborted_page);
        ASSERT_NE(page, nullptr);
        page->set_page_lsn(abort_lsn);
        ASSERT_TRUE(bpm.unpin_page(aborted_page, true));

        PageId replacement_page{page_fd, INVALID_PAGE_ID};
        ASSERT_NE(bpm.new_page(&replacement_page), nullptr);
        EXPECT_GE(log_mgr.get_durable_lsn(), abort_lsn);
        EXPECT_GE(log_mgr.get_persist_lsn(), abort_lsn);
        EXPECT_GT(disk.get_file_size(LOG_FILE_NAME), 0);
        EXPECT_TRUE(bpm.unpin_page(replacement_page, false));
    }
    disk.close_file(page_fd);
}

TEST(LogManagerTest, ProcessCrashCommitWaitsForPwriteWithoutFsync) {
    ScopedTestDir test_dir("log_manager_process_crash_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk, DurabilityMode::PROCESS_CRASH);

    CommitLogRecord commit(102);
    lsn_t commit_lsn = log_mgr.add_log_to_buffer(&commit);
    log_mgr.flush_log_to_disk_up_to(commit_lsn);

    EXPECT_EQ(log_mgr.get_persist_lsn(), commit_lsn);
    EXPECT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);
    EXPECT_EQ(log_mgr.get_commit_count(), 1u);
    EXPECT_EQ(log_mgr.get_pwrite_count(), 1u);
    EXPECT_GT(log_mgr.get_pwrite_bytes(), 0u);

    log_mgr.flush_log_to_disk_with_sync();
    EXPECT_EQ(log_mgr.get_durable_lsn(), commit_lsn);
}

TEST(LogManagerTest, ConcurrentDurableFlushesShareAGroup) {
    ScopedTestDir test_dir("log_manager_group_commit_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    constexpr int kWaiterCount = 16;
    lsn_t target_lsn = INVALID_LSN;
    for (int i = 0; i < kWaiterCount; ++i) {
        BeginLogRecord begin(200 + i);
        target_lsn = log_mgr.add_log_to_buffer(&begin);
    }

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> waiters;
    waiters.reserve(kWaiterCount);
    for (int i = 0; i < kWaiterCount; ++i) {
        waiters.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            log_mgr.flush_log_to_disk_up_to(target_lsn);
        });
    }
    while (ready.load(std::memory_order_acquire) != kWaiterCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& waiter : waiters) {
        waiter.join();
    }

    EXPECT_EQ(log_mgr.get_durable_lsn(), target_lsn);
    EXPECT_GE(log_mgr.get_group_commit_count(), 1u);
    EXPECT_GE(log_mgr.get_group_commit_waiter_count(), 1u);
    EXPECT_LT(log_mgr.get_fsync_count(), static_cast<uint64_t>(kWaiterCount));
}

// Under a continuously refilled queue the leader keeps extending the batch, so
// waiters are released across many flushes. Each one must be released only once
// its own target is covered by durable_lsn_ — never early, and never left
// behind, however the batches happen to line up.
TEST(LogManagerTest, SaturatedGroupCommitReleasesEveryWaiterOnlyWhenDurable) {
    ScopedTestDir test_dir("log_manager_group_commit_saturated_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    constexpr int kThreads = 8;
    constexpr int kCommitsPerThread = 40;
    std::atomic<int> completed{0};
    std::vector<std::thread> committers;
    committers.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        committers.emplace_back([&, i] {
            for (int n = 0; n < kCommitsPerThread; ++n) {
                CommitLogRecord commit(300 + i);
                const lsn_t lsn = log_mgr.add_log_to_buffer(&commit);
                log_mgr.flush_log_to_disk_up_to(lsn);
                // Returning from the flush must mean this record is durable.
                ASSERT_GE(log_mgr.get_durable_lsn(), lsn);
                completed.fetch_add(1, std::memory_order_acq_rel);
            }
        });
    }
    for (auto& committer : committers) {
        committer.join();
    }

    EXPECT_EQ(completed.load(), kThreads * kCommitsPerThread);
    EXPECT_GE(log_mgr.get_group_commit_leader_count(), 1u);
    // Coalescing must still happen: far fewer fdatasync calls than commits.
    EXPECT_LT(log_mgr.get_fsync_count(), static_cast<uint64_t>(kThreads * kCommitsPerThread));
}

TEST(LogManagerTest, GroupCommitPropagatesFlushErrorToEveryWaiter) {
    ScopedTestDir test_dir("log_manager_group_commit_error_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    constexpr int kWaiterCount = 8;
    lsn_t target_lsn = INVALID_LSN;
    for (int i = 0; i < kWaiterCount; ++i) {
        CommitLogRecord commit(400 + i);
        target_lsn = log_mgr.add_log_to_buffer(&commit);
    }
    log_mgr.flush_log_to_disk();
    ASSERT_EQ(log_mgr.get_persist_lsn(), target_lsn);
    ASSERT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);

    // An invalid descriptor makes every group fdatasync fail, exercising the
    // leader's error propagation to all callers.
    disk.SetLogFd(-2);
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> errors{0};
    std::vector<std::thread> waiters;
    waiters.reserve(kWaiterCount);
    for (int i = 0; i < kWaiterCount; ++i) {
        waiters.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            try {
                log_mgr.flush_log_to_disk_up_to(target_lsn);
            } catch (const UnixError&) {
                errors.fetch_add(1, std::memory_order_acq_rel);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kWaiterCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& waiter : waiters) {
        waiter.join();
    }

    EXPECT_EQ(errors.load(), kWaiterCount);
    EXPECT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);
}

TEST(LogManagerTest, RestartOffsetRoundTrip) {
    ScopedTestDir test_dir("log_manager_restart_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    EXPECT_EQ(log_mgr.read_restart_offset(), 0);
    log_mgr.write_restart_offset(128);
    EXPECT_EQ(log_mgr.read_restart_offset(), 128);
}

TEST(LogManagerTest, RestartManifestRoundTrip) {
    ScopedTestDir test_dir("log_manager_restart_manifest_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    RestartManifest written;
    written.checkpoint_offset = 4096;
    written.next_timestamp = 123456;
    written.next_txn_id = 789;
    log_mgr.write_restart_manifest(written);

    const RestartManifest read = log_mgr.read_restart_manifest();
    EXPECT_EQ(read.checkpoint_offset, 4096);
    EXPECT_EQ(read.next_timestamp, 123456);
    EXPECT_EQ(read.next_txn_id, 789);
    // 旧读者只读第一个 token，必须继续拿到偏移。
    EXPECT_EQ(log_mgr.read_restart_offset(), 4096);
}

// 旧版本写的 db.restart 只有裸偏移。缺失的计数器字段必须退回 0，而不是让整个清单
// 读失败——0 的语义是“本文件不提供计数器”，恢复会改用保留 WAL 里的 COMMIT 时间戳。
TEST(LogManagerTest, LegacyRestartFileWithoutCountersReadsAsZero) {
    ScopedTestDir test_dir("log_manager_legacy_restart_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    {
        std::ofstream restart(LogManager::RESTART_FILE_NAME, std::ios::trunc);
        restart << 512;
    }
    const RestartManifest read = log_mgr.read_restart_manifest();
    EXPECT_EQ(read.checkpoint_offset, 512);
    EXPECT_EQ(read.next_timestamp, 0);
    EXPECT_EQ(read.next_txn_id, 0);
}

// 认不出的键被忽略，认得出的键照旧生效：清单可以继续加字段而不破坏旧字段。
TEST(LogManagerTest, RestartManifestIgnoresUnknownAndMalformedEntries) {
    ScopedTestDir test_dir("log_manager_restart_unknown_test_db");
    DiskManager disk;
    LogManager log_mgr(&disk);

    {
        std::ofstream restart(LogManager::RESTART_FILE_NAME, std::ios::trunc);
        restart << "0\nfuture_key=1\nnext_timestamp=99\nnext_txn_id=not-a-number\n";
    }
    const RestartManifest read = log_mgr.read_restart_manifest();
    EXPECT_EQ(read.checkpoint_offset, 0);
    EXPECT_EQ(read.next_timestamp, 99);
    EXPECT_EQ(read.next_txn_id, 0);
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
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &sm_mgr);
    LogManager log_mgr(&disk);

    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr);
    txn_mgr.commit(txn, &log_mgr);

    auto logs = ReadAllLogs(disk);
    ASSERT_GE(logs.size(), 2);
    EXPECT_EQ(logs[0]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[1]->log_type_, LogType::COMMIT);
    EXPECT_EQ(logs[1]->prev_lsn_, logs[0]->lsn_);
}

TEST(LogManagerTest, BeginOnlyAbortSkipsWalWriteAndReleasesLocks) {
    ScopedTestDir test_dir("transaction_begin_only_abort_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    BufferPoolManager bpm(8, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &sm_mgr);
    LogManager log_mgr(&disk);

    Transaction* owner = txn_mgr.begin(nullptr, &log_mgr, IsolationLevel::SNAPSHOT_ISOLATION);
    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr, IsolationLevel::SNAPSHOT_ISOLATION);
    const Rid rid{3, 4};
    ASSERT_TRUE(lock_mgr.lock_exclusive_on_record(owner, rid, 42));
    ASSERT_FALSE(lock_mgr.lock_exclusive_on_record(txn, rid, 42));
    ASSERT_TRUE(txn->get_write_set().empty());
    ASSERT_EQ(txn->get_prev_lsn(), txn->get_begin_lsn());

    txn_mgr.abort(txn, &log_mgr);

    EXPECT_EQ(log_mgr.get_persist_lsn(), INVALID_LSN);
    EXPECT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);
    EXPECT_EQ(log_mgr.get_pwrite_count(), 0u);
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), 0);

    // The abort still performs the normal lock cleanup even though it does not
    // append an ABORT record. Releasing the owner then lets a third transaction
    // take the same lock, proving neither transaction left queue state behind.
    txn_mgr.abort(owner, &log_mgr);
    Transaction next(999);
    ASSERT_TRUE(lock_mgr.lock_exclusive_on_record(&next, rid, 42));
    ASSERT_FALSE(next.get_lock_set()->empty());
    EXPECT_TRUE(lock_mgr.unlock(&next, *next.get_lock_set()->begin()));

    log_mgr.flush_log_to_disk();
    auto logs = ReadAllLogs(disk);
    ASSERT_EQ(logs.size(), 2);
    EXPECT_EQ(logs[0]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[1]->log_type_, LogType::BEGIN);
}

TEST(LogManagerTest, EmptyWriteSetWithDmlWalStillFlushesAbort) {
    ScopedTestDir test_dir("transaction_empty_write_set_dml_abort_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    BufferPoolManager bpm(8, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &sm_mgr);
    LogManager log_mgr(&disk);

    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr);
    auto record = MakeRecord("pending");
    Rid rid{5, 6};
    InsertLogRecord insert(txn->get_transaction_id(), record, rid, "t");
    insert.prev_lsn_ = txn->get_prev_lsn();
    const lsn_t insert_lsn = log_mgr.add_log_to_buffer(&insert);
    txn->set_prev_lsn(insert_lsn);
    ASSERT_TRUE(txn->get_write_set().empty());
    ASSERT_NE(txn->get_prev_lsn(), txn->get_begin_lsn());

    txn_mgr.abort(txn, &log_mgr);

    EXPECT_EQ(log_mgr.get_pwrite_count(), 1u);
    EXPECT_GT(disk.get_file_size(LOG_FILE_NAME), 0);
    auto logs = ReadAllLogs(disk);
    ASSERT_EQ(logs.size(), 3);
    EXPECT_EQ(logs[0]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[1]->log_type_, LogType::INSERT);
    EXPECT_EQ(logs[2]->log_type_, LogType::ABORT);
    EXPECT_EQ(logs[2]->prev_lsn_, logs[1]->lsn_);
}

TEST(LogManagerTest, SecondIndexConflictWithEmptyWriteSetPersistsAbortWal) {
    ScopedTestDir test_dir("transaction_second_index_abort_test_root");
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &sm_mgr);

    sm_mgr.create_db("transaction_second_index_abort_test_db");
    sm_mgr.open_db("transaction_second_index_abort_test_db");
    LogManager log_mgr(&disk);
    bpm.set_log_manager(&log_mgr);

    sm_mgr.create_table(
        "t", {{"id", TYPE_INT, sizeof(int)}, {"key_col", TYPE_INT, sizeof(int)}, {"value", TYPE_INT, sizeof(int)}},
        nullptr);
    sm_mgr.create_index("t", {"id"}, nullptr);
    sm_mgr.create_index("t", {"key_col"}, nullptr);

    Value one;
    one.set_int(1);
    Value ten;
    ten.set_int(10);
    Value hundred;
    hundred.set_int(100);
    Transaction* seed_txn = txn_mgr.begin(nullptr, &log_mgr, IsolationLevel::SNAPSHOT_ISOLATION);
    seed_txn->set_txn_mode(true);
    int seed_offset = 0;
    Context seed_context(&lock_mgr, &log_mgr, seed_txn, nullptr, &seed_offset, &txn_mgr);
    InsertExecutor seed(&sm_mgr, "t", {one, ten, hundred}, &seed_context);
    seed.Next();
    const Rid seed_rid = seed.rid();
    txn_mgr.commit(seed_txn, &log_mgr);

    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr, IsolationLevel::SNAPSHOT_ISOLATION);
    txn->set_txn_mode(true);
    int offset = 0;
    Context context(&lock_mgr, &log_mgr, txn, nullptr, &offset, &txn_mgr);
    Value two;
    two.set_int(2);
    Value two_hundred;
    two_hundred.set_int(200);
    InsertExecutor losing_insert(&sm_mgr, "t", {two, ten, two_hundred}, &context);

    EXPECT_THROW(losing_insert.Next(), TransactionAbortException);
    const Rid losing_rid = losing_insert.rid();
    ASSERT_TRUE(txn->get_write_set().empty());
    ASSERT_NE(txn->get_prev_lsn(), txn->get_begin_lsn());

    txn_mgr.abort(txn, &log_mgr);

    // The real executor appended INSERT WAL before the heap insert, then the
    // first index accepted id=2 before the second index rejected key_col=10.
    // Even though the executor locally removed that partial work before it
    // could add a WriteRecord, abort must publish the complete loser chain.
    const lsn_t abort_lsn = log_mgr.get_persist_lsn();
    EXPECT_NE(abort_lsn, INVALID_LSN);
    EXPECT_GT(log_mgr.get_pwrite_count(), 0u);
    auto logs = ReadAllLogs(disk);
    ASSERT_EQ(logs.size(), 6);
    EXPECT_EQ(logs[0]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[1]->log_type_, LogType::INSERT);
    EXPECT_EQ(logs[2]->log_type_, LogType::COMMIT);
    EXPECT_EQ(logs[3]->log_type_, LogType::BEGIN);
    EXPECT_EQ(logs[4]->log_type_, LogType::INSERT);
    EXPECT_EQ(logs[5]->log_type_, LogType::ABORT);
    EXPECT_EQ(logs[1]->prev_lsn_, logs[0]->lsn_);
    EXPECT_EQ(logs[2]->prev_lsn_, logs[1]->lsn_);
    EXPECT_EQ(logs[4]->prev_lsn_, logs[3]->lsn_);
    EXPECT_EQ(logs[5]->prev_lsn_, logs[4]->lsn_);
    EXPECT_EQ(logs[5]->lsn_, abort_lsn);

    // ABORT does not need an fsync for the process-crash contract, but any
    // subsequent WAL durability boundary must include it.
    log_mgr.flush_log_to_disk_with_sync();
    EXPECT_GE(log_mgr.get_durable_lsn(), abort_lsn);

    auto* fh = sm_mgr.fhs_.at("t").get();
    EXPECT_TRUE(fh->is_record(seed_rid));
    EXPECT_FALSE(fh->is_record(losing_rid));

    const auto& tab = sm_mgr.db_.get_table("t");
    ASSERT_EQ(tab.indexes.size(), 2u);
    auto* id_index = sm_mgr.ihs_.at(ix_mgr.get_index_name("t", tab.indexes[0].cols)).get();
    auto* key_index = sm_mgr.ihs_.at(ix_mgr.get_index_name("t", tab.indexes[1].cols)).get();
    int id_key = 2;
    std::vector<Rid> rids;
    EXPECT_FALSE(id_index->get_value(reinterpret_cast<const char*>(&id_key), &rids, nullptr));
    int duplicate_key = 10;
    rids.clear();
    ASSERT_TRUE(key_index->get_value(reinterpret_cast<const char*>(&duplicate_key), &rids, nullptr));
    ASSERT_EQ(rids.size(), 1u);
    EXPECT_EQ(rids[0], seed_rid);

    sm_mgr.close_db();
}

TEST(LogManagerTest, ExecutorDmlWritesWalSequence) {
    ScopedTestDir test_dir("executor_dml_log_test_root");
    DiskManager disk;
    BufferPoolManager bpm(64, &disk);
    RmManager rm_mgr(&disk, &bpm);
    IxManager ix_mgr(&disk, &bpm);
    SmManager sm_mgr(&disk, &bpm, &rm_mgr, &ix_mgr);
    LockManager lock_mgr;
    TransactionManager txn_mgr(&lock_mgr, &sm_mgr);

    sm_mgr.create_db("executor_dml_log_test_db");
    sm_mgr.open_db("executor_dml_log_test_db");
    LogManager log_mgr(&disk);
    bpm.set_log_manager(&log_mgr);

    sm_mgr.create_table("t", {{"id", TYPE_INT, sizeof(int)}, {"v", TYPE_INT, sizeof(int)}}, nullptr);
    sm_mgr.create_index("t", {"id"}, nullptr);

    Transaction* txn = txn_mgr.begin(nullptr, &log_mgr);
    Context context(&lock_mgr, &log_mgr, txn, nullptr, &const_offset, &txn_mgr);

    Value id;
    id.set_int(1);
    Value v;
    v.set_int(10);
    InsertExecutor insert_executor(&sm_mgr, "t", {id, v}, &context);
    insert_executor.Next();
    Rid rid = insert_executor.rid();

    Value new_v;
    new_v.set_int(20);
    SetClause set_clause{{"t", "v"}, new_v, false, {}, UpdateOp::ASSIGNMENT, {}};
    UpdateExecutor update_executor(&sm_mgr, "t", {set_clause}, {}, {rid}, &context);
    update_executor.Next();

    DeleteExecutor delete_executor(&sm_mgr, "t", {}, {rid}, &context);
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

    sm_mgr.close_db();
}
