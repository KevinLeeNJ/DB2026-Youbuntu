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

#include <array>
#include <atomic>
#include <chrono>
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

std::string UniqueWalTestDir(const std::string& prefix) {
    static std::atomic<uint64_t> next_id{0};
    return prefix + "_" + std::to_string(getpid()) + "_" + std::to_string(next_id.fetch_add(1));
}

class ScopedWalFile {
public:
    explicit ScopedWalFile(DiskManager* disk_manager) : disk_manager_(disk_manager) {
        disk_manager_->create_file(LOG_FILE_NAME);
    }

    ~ScopedWalFile() {
        const int fd = disk_manager_->GetLogFd();
        if (fd >= 0) {
            try {
                disk_manager_->close_file(fd);
            } catch (...) {
            }
        }
        if (disk_manager_->is_file(LOG_FILE_NAME)) {
            try {
                disk_manager_->destroy_file(LOG_FILE_NAME);
            } catch (...) {
            }
        }
    }

private:
    DiskManager* disk_manager_;
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

TEST(IndexSmoWalTest, Crc32MatchesIeeeKnownAnswers) {
    const char empty[] = "";
    EXPECT_EQ(IndexSmoCrc32(empty, 0), 0U);

    constexpr char digits[] = "123456789";
    EXPECT_EQ(IndexSmoCrc32(digits, sizeof(digits) - 1), 0xcbf43926U);

    std::array<char, 256> every_byte{};
    for (size_t i = 0; i < every_byte.size(); ++i) {
        every_byte[i] = static_cast<char>(i);
    }
    EXPECT_EQ(IndexSmoCrc32(every_byte.data(), every_byte.size()), 0x29058c73U);
}

TEST(IndexSmoWalTest, IndexBindV2ReissuesAnExplicitGenerationAndRejectsMalformedRecords) {
    const auto make_view = [](const std::vector<char>& bytes) {
        WalRecordView view;
        view.bytes = bytes.data();
        view.total_len = static_cast<uint32_t>(bytes.size());
        view.log_type = read_unaligned<LogType>(bytes.data() + OFFSET_LOG_TYPE);
        view.lsn = read_unaligned<lsn_t>(bytes.data() + OFFSET_LSN);
        view.txn_id = read_unaligned<txn_id_t>(bytes.data() + OFFSET_LOG_TID);
        view.prev_lsn = read_unaligned<lsn_t>(bytes.data() + OFFSET_PREV_LSN);
        return view;
    };
    const auto rewrite_crc = [](std::vector<char>* bytes) {
        const uint32_t crc = IndexSmoCrc32(bytes->data(), bytes->size() - sizeof(uint32_t));
        std::memcpy(bytes->data() + bytes->size() - sizeof(uint32_t), &crc, sizeof(crc));
    };

    IndexBindLogRecord legacy("a.idx");
    legacy.lsn_ = 11;
    std::vector<char> legacy_bytes(legacy.log_tot_len_);
    legacy.serialize(legacy_bytes.data());
    std::string_view name;
    uint64_t generation = 0;
    WalRecordView view = make_view(legacy_bytes);
    ASSERT_TRUE(ParseIndexBindWal(view, &name, &generation));
    EXPECT_EQ(name, "a.idx");
    EXPECT_EQ(generation, 12U);

    IndexBindLogRecord reissue("a.idx", 7);
    reissue.lsn_ = 41;
    std::vector<char> reissue_bytes(reissue.log_tot_len_);
    reissue.serialize(reissue_bytes.data());
    view = make_view(reissue_bytes);
    ASSERT_TRUE(ParseIndexBindWal(view, &name, &generation));
    EXPECT_EQ(generation, 7U);
    EXPECT_THROW(IndexBindLogRecord("a.idx", 0), std::invalid_argument);

    std::vector<char> malformed = reissue_bytes;
    malformed.back() ^= 1;
    view = make_view(malformed);
    EXPECT_FALSE(ParseIndexBindWal(view, &name, &generation));

    malformed = reissue_bytes;
    const uint16_t unknown_version = INDEX_BIND_VERSION_V2 + 1;
    std::memcpy(malformed.data() + OFFSET_LOG_DATA + sizeof(uint32_t), &unknown_version, sizeof(unknown_version));
    rewrite_crc(&malformed);
    view = make_view(malformed);
    EXPECT_FALSE(ParseIndexBindWal(view, &name, &generation));

    malformed = reissue_bytes;
    const uint64_t zero_generation = 0;
    constexpr size_t kGenerationOffset = OFFSET_LOG_DATA + sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(uint32_t);
    std::memcpy(malformed.data() + kGenerationOffset, &zero_generation, sizeof(zero_generation));
    rewrite_crc(&malformed);
    view = make_view(malformed);
    EXPECT_FALSE(ParseIndexBindWal(view, &name, &generation));

    malformed = legacy_bytes;
    const uint64_t wrong_v1_generation = 99;
    std::memcpy(malformed.data() + kGenerationOffset, &wrong_v1_generation, sizeof(wrong_v1_generation));
    rewrite_crc(&malformed);
    view = make_view(malformed);
    EXPECT_FALSE(ParseIndexBindWal(view, &name, &generation));
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

    EXPECT_GT(log_mgr.ensure_index_binding(data.index_file_name), 0U);

    const lsn_t smo_lsn = log_mgr.append_index_smo(data);
    EXPECT_GE(smo_lsn, 1);
    EXPECT_LT(log_mgr.get_durable_lsn(), smo_lsn);

    log_mgr.flush_log_to_disk_up_to(smo_lsn);
    EXPECT_GE(log_mgr.get_durable_lsn(), smo_lsn);
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

TEST(LogRecordTest, BidirectionalDeltaRequiresProofThatIndexKeysAreUnchanged) {
    std::string old_text(128, 'a');
    std::string new_text = old_text;
    new_text[10] = 'x';
    new_text[50] = 'y';
    auto old_rec = MakeRecord(old_text);
    auto new_rec = MakeRecord(new_text);
    Rid rid{9, 5};

    UpdateLogRecord proven(90, old_rec, new_rec, rid, "stock", true);
    ASSERT_TRUE(proven.bidirectional_delta_encoding_);
    EXPECT_FALSE(proven.sparse_encoding_);
    std::vector<char> proven_bytes(proven.log_tot_len_);
    proven.serialize(proven_bytes.data());
    EXPECT_EQ(read_unaligned<int>(proven_bytes.data() + OFFSET_LOG_DATA), UpdateLogRecord::kBidirectionalDeltaVersion);

    UpdateLogRecord not_proven(90, old_rec, new_rec, rid, "stock");
    EXPECT_FALSE(not_proven.bidirectional_delta_encoding_);
    EXPECT_TRUE(not_proven.sparse_encoding_);
    EXPECT_LT(proven.log_tot_len_, not_proven.log_tot_len_);
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

TEST(LogManagerTest, WalFdSizeSurvivesPathRemovalAndClosedFdFailsExplicitly) {
    ScopedTestDir test_dir(UniqueWalTestDir("wal_fd_size_test"));
    DiskManager disk;
    ScopedWalFile wal(&disk);

    EXPECT_EQ(disk.get_log_file_size(), 0);
    char first = 'a';
    ASSERT_NO_THROW(disk.write_log(&first, 1));
    ASSERT_EQ(disk.get_log_file_size(), 1);

    ASSERT_EQ(unlink(LOG_FILE_NAME.c_str()), 0);
    EXPECT_EQ(disk.get_log_file_size(), 1);
    char read_back = 0;
    ASSERT_EQ(disk.read_log(&read_back, 1, 0), 1);
    EXPECT_EQ(read_back, first);
    char second = 'b';
    ASSERT_NO_THROW(disk.write_log(&second, 1));
    EXPECT_EQ(disk.get_log_file_size(), 2);
    ASSERT_EQ(disk.read_log(&read_back, 1, 1), 1);
    EXPECT_EQ(read_back, second);

    const int closed_fd = disk.GetLogFd();
    ASSERT_GE(closed_fd, 0);
    ASSERT_NO_THROW(disk.close_file(closed_fd));
    try {
        (void)disk.get_log_file_size();
        FAIL() << "closed WAL descriptor returned a size";
    } catch (const RMDBError& error) {
        EXPECT_NE(std::string(error.what()).find("get_file_size(fstat) failed"), std::string::npos);
    }
    disk.SetLogFd(-1);
}

TEST(LogManagerTest, WalFdReopenPreservesAppendOffset) {
    ScopedTestDir test_dir(UniqueWalTestDir("wal_fd_reopen_test"));
    DiskManager disk;
    ScopedWalFile wal(&disk);

    char first[] = {'a', 'b', 'c'};
    ASSERT_NO_THROW(disk.write_log(first, static_cast<int>(sizeof(first))));
    ASSERT_EQ(disk.get_log_file_size(), static_cast<int64_t>(sizeof(first)));
    const int initial_fd = disk.GetLogFd();
    ASSERT_GE(initial_fd, 0);
    ASSERT_NO_THROW(disk.close_file(initial_fd));
    disk.SetLogFd(-1);

    EXPECT_EQ(disk.get_log_file_size(), static_cast<int64_t>(sizeof(first)));
    char second[] = {'d', 'e'};
    ASSERT_NO_THROW(disk.write_log(second, static_cast<int>(sizeof(second))));
    EXPECT_EQ(disk.get_log_file_size(), static_cast<int64_t>(sizeof(first) + sizeof(second)));
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

TEST(LogManagerTest, WalFlushMetricsAreEnabledOnlyWhenSuppliedAndEnabled) {
    ScopedTestDir test_dir("log_manager_wal_metrics_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);

    WalFlushMetrics disabled(false);
    LogManager disabled_log_mgr(&disk, DurabilityMode::STRICT, &disabled);
    BeginLogRecord disabled_begin(201);
    const lsn_t disabled_lsn = disabled_log_mgr.add_log_to_buffer(&disabled_begin);
    disabled_log_mgr.flush_log_to_disk_up_to(disabled_lsn);
    EXPECT_EQ(disabled.snapshot().pwrite.count, 0U);

    WalFlushMetrics enabled(true);
    LogManager enabled_log_mgr(&disk, DurabilityMode::STRICT, &enabled);
    BeginLogRecord enabled_begin(202);
    const lsn_t enabled_lsn = enabled_log_mgr.add_log_to_buffer(&enabled_begin);
    enabled_log_mgr.flush_log_to_disk_up_to(enabled_lsn);
    enabled_log_mgr.flush_log_to_disk_up_to(enabled_lsn);

    const auto snapshot = enabled.snapshot();
    EXPECT_EQ(snapshot.leader_requests, 1U);
    EXPECT_EQ(snapshot.physical_flush_iterations, 1U);
    EXPECT_EQ(snapshot.pwrite.count, 1U);
    EXPECT_GT(snapshot.pwrite_bytes, 0U);
    EXPECT_EQ(snapshot.fdatasync.count, 1U);
    EXPECT_EQ(snapshot.completed_batch_histogram[1], 1U);
    EXPECT_EQ(snapshot.already_covered_fast_paths, 1U);
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

TEST(LogManagerTest, CheckpointWalCutIsContiguousDurableAndReissuesBindings) {
    ScopedTestDir test_dir("checkpoint_wal_cut_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    const uint64_t a_generation = log_mgr.ensure_index_binding("a.idx");
    const uint64_t b_generation = log_mgr.ensure_index_binding("b.idx");
    BeginLogRecord begin(200);
    ASSERT_EQ(log_mgr.add_log_to_buffer(&begin), 2);
    const int64_t expected_cut_offset = log_mgr.current_log_offset();
    const CheckpointWalCut cut = log_mgr.create_checkpoint_wal_cut({"a.idx", "b.idx"});

    EXPECT_EQ(cut.checkpoint_offset, expected_cut_offset);
    EXPECT_EQ(cut.checkpoint_lsn, 3);
    EXPECT_EQ(cut.last_lsn, 5);
    ASSERT_EQ(cut.index_bindings.size(), 2U);
    EXPECT_EQ(cut.index_bindings[0].first, "a.idx");
    EXPECT_EQ(cut.index_bindings[0].second, a_generation);
    EXPECT_EQ(cut.index_bindings[1].first, "b.idx");
    EXPECT_EQ(cut.index_bindings[1].second, b_generation);
    EXPECT_GE(log_mgr.get_durable_lsn(), cut.last_lsn);

    const int64_t cut_end = disk.get_file_size(LOG_FILE_NAME);
    WalReader reader(&disk, 0, cut_end);
    WalRecordView record;
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.log_type, LogType::INDEX_BIND);
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.log_type, LogType::INDEX_BIND);
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.log_type, LogType::BEGIN);
    ASSERT_TRUE(reader.next(&record));
    EXPECT_EQ(record.offset, cut.checkpoint_offset);
    EXPECT_EQ(record.log_type, LogType::CHECKPOINT);
    auto decoded_checkpoint = DeserializeLogRecord(record.bytes, static_cast<int>(record.total_len));
    ASSERT_NE(decoded_checkpoint, nullptr);
    const auto* checkpoint = dynamic_cast<const CheckpointLogRecord*>(decoded_checkpoint.get());
    ASSERT_NE(checkpoint, nullptr);
    EXPECT_TRUE(checkpoint->active_txns_.empty());
    int64_t expected_offset = record.offset + record.total_len;
    for (size_t i = 0; i < cut.index_bindings.size(); ++i) {
        ASSERT_TRUE(reader.next(&record));
        EXPECT_EQ(record.offset, expected_offset);
        EXPECT_EQ(record.log_type, LogType::INDEX_BIND);
        std::string_view name;
        uint64_t generation = 0;
        ASSERT_TRUE(ParseIndexBindWal(record, &name, &generation));
        EXPECT_EQ(name, cut.index_bindings[i].first);
        EXPECT_EQ(generation, cut.index_bindings[i].second);
        expected_offset += record.total_len;
    }
    EXPECT_FALSE(reader.next(&record));
    EXPECT_EQ(expected_offset, cut_end);

    IndexSmoWalData smo;
    smo.index_file_name = "a.idx";
    smo.pages.resize(1);
    smo.pages[0].page_no = 2;
    const lsn_t smo_lsn = log_mgr.append_index_smo(smo);
    log_mgr.flush_log_to_disk_up_to_durable(smo_lsn);

    WalReader with_smo(&disk, cut_end, disk.get_file_size(LOG_FILE_NAME));
    ASSERT_TRUE(with_smo.next(&record));
    EXPECT_EQ(record.log_type, LogType::INDEX_SMO);
    IndexSmoWalView parsed_smo;
    ASSERT_TRUE(ParseIndexSmoWal(record, &parsed_smo));
    EXPECT_EQ(parsed_smo.index_generation, cut.index_bindings[0].second);
}

TEST(LogManagerTest, FailedCheckpointWalCutKeepsTheCurrentBindingUsable) {
    ScopedTestDir test_dir("failed_checkpoint_wal_cut_binding_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    LogManager log_mgr(&disk);

    const uint64_t original_generation = log_mgr.ensure_index_binding("a.idx");
    const int valid_log_fd = disk.GetLogFd();
    ASSERT_GE(valid_log_fd, 0);
    disk.SetLogFd(-2);
    EXPECT_THROW(log_mgr.create_checkpoint_wal_cut({"a.idx"}), UnixError);
    disk.SetLogFd(valid_log_fd);

    IndexSmoWalData smo;
    smo.index_file_name = "a.idx";
    smo.pages.resize(1);
    smo.pages[0].page_no = 2;
    const lsn_t smo_lsn = log_mgr.append_index_smo(smo);
    ASSERT_NO_THROW(log_mgr.flush_log_to_disk_up_to_durable(smo_lsn));

    WalReader reader(&disk, 0, disk.get_file_size(LOG_FILE_NAME));
    WalRecordView record;
    size_t bind_count = 0;
    bool found_smo = false;
    while (reader.next(&record)) {
        if (record.log_type == LogType::INDEX_BIND) {
            std::string_view name;
            uint64_t generation = 0;
            ASSERT_TRUE(ParseIndexBindWal(record, &name, &generation));
            EXPECT_EQ(name, "a.idx");
            EXPECT_EQ(generation, original_generation);
            ++bind_count;
        } else if (record.log_type == LogType::INDEX_SMO) {
            IndexSmoWalView parsed;
            ASSERT_TRUE(ParseIndexSmoWal(record, &parsed));
            EXPECT_EQ(parsed.index_generation, original_generation);
            found_smo = true;
        }
    }
    EXPECT_EQ(bind_count, 2U);
    EXPECT_TRUE(found_smo);
}

TEST(LogManagerTest, ValidCheckpointWalCutSkipsAnObsoletePrefixAtStartup) {
    ScopedTestDir test_dir("valid_checkpoint_wal_cut_restart_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    CheckpointWalCut cut;
    int64_t wal_bytes = 0;
    {
        LogManager writer(&disk);
        BeginLogRecord obsolete(201);
        ASSERT_EQ(writer.add_log_to_buffer(&obsolete), 0);
        cut = writer.create_checkpoint_wal_cut({"a.idx"});
        writer.write_restart_offset(cut.checkpoint_offset);
        wal_bytes = disk.get_file_size(LOG_FILE_NAME);
    }

    // A valid restart boundary makes bytes before it obsolete. Make the old
    // record unrecognizable so scanning from zero would truncate immediately.
    {
        std::fstream wal(LOG_FILE_NAME, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(wal.is_open());
        const int unknown_type = 999;
        wal.seekp(OFFSET_LOG_TYPE);
        wal.write(reinterpret_cast<const char*>(&unknown_type), sizeof(unknown_type));
        wal.flush();
        ASSERT_TRUE(static_cast<bool>(wal));
    }

    LogManager restarted(&disk);
    ASSERT_NO_THROW(restarted.initialize_from_existing_log());
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), wal_bytes);
    EXPECT_EQ(restarted.get_global_lsn(), cut.last_lsn + 1);
    EXPECT_EQ(restarted.read_restart_offset(), cut.checkpoint_offset);

    IndexSmoWalData smo;
    smo.index_file_name = "a.idx";
    smo.pages.resize(1);
    smo.pages[0].page_no = 2;
    const lsn_t smo_lsn = restarted.append_index_smo(smo);
    EXPECT_EQ(smo_lsn, cut.last_lsn + 1);
}

TEST(LogManagerTest, NonCheckpointRestartOffsetFallsBackAndIsSanitized) {
    ScopedTestDir test_dir("non_checkpoint_restart_fallback_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    int64_t second_offset = 0;
    {
        LogManager writer(&disk);
        BeginLogRecord first(202);
        writer.add_log_to_buffer(&first);
        second_offset = writer.current_log_offset();
        BeginLogRecord second(203);
        writer.add_log_to_buffer(&second);
        writer.flush_log_to_disk_with_sync();
        RestartManifest manifest;
        manifest.checkpoint_offset = second_offset;
        manifest.next_timestamp = 77;
        writer.write_restart_manifest(manifest);
    }

    LogManager restarted(&disk);
    ASSERT_NO_THROW(restarted.initialize_from_existing_log());
    EXPECT_EQ(restarted.get_global_lsn(), 2);
    const RestartManifest manifest = restarted.read_restart_manifest();
    EXPECT_EQ(manifest.checkpoint_offset, 0);
    EXPECT_EQ(manifest.next_timestamp, 77);
}

TEST(LogManagerTest, NonEmptyCheckpointRestartOffsetFallsBackToZero) {
    ScopedTestDir test_dir("nonempty_checkpoint_restart_fallback_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    int64_t checkpoint_offset = 0;
    {
        LogManager writer(&disk);
        BeginLogRecord first(204);
        writer.add_log_to_buffer(&first);
        checkpoint_offset = writer.current_log_offset();
        CheckpointLogRecord checkpoint(std::unordered_map<txn_id_t, lsn_t>{{204, 0}});
        writer.add_log_to_buffer(&checkpoint);
        BeginLogRecord last(205);
        writer.add_log_to_buffer(&last);
        writer.flush_log_to_disk_with_sync();
        writer.write_restart_offset(checkpoint_offset);
    }

    // A high LSN before the invalid boundary makes fallback-to-zero
    // observable without exposing startup's internal scan offset.
    {
        std::fstream wal(LOG_FILE_NAME, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(wal.is_open());
        const lsn_t old_prefix_lsn = 100;
        wal.seekp(OFFSET_LSN);
        wal.write(reinterpret_cast<const char*>(&old_prefix_lsn), sizeof(old_prefix_lsn));
        wal.flush();
        ASSERT_TRUE(static_cast<bool>(wal));
    }

    LogManager restarted(&disk);
    ASSERT_NO_THROW(restarted.initialize_from_existing_log());
    EXPECT_EQ(restarted.get_global_lsn(), 101);
    EXPECT_EQ(restarted.read_restart_offset(), 0);
}

TEST(LogManagerTest, OutOfRangeRestartOffsetFallsBackToZero) {
    ScopedTestDir test_dir("out_of_range_restart_fallback_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    int64_t wal_bytes = 0;
    {
        LogManager writer(&disk);
        BeginLogRecord begin(206);
        writer.add_log_to_buffer(&begin);
        writer.flush_log_to_disk_with_sync();
        wal_bytes = disk.get_file_size(LOG_FILE_NAME);
        writer.write_restart_offset(wal_bytes + 1);
    }

    LogManager restarted(&disk);
    ASSERT_NO_THROW(restarted.initialize_from_existing_log());
    EXPECT_EQ(restarted.get_global_lsn(), 1);
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), wal_bytes);
    EXPECT_EQ(restarted.read_restart_offset(), 0);
}

TEST(LogManagerTest, CheckpointWalCutStartupTruncatesATornTailAtIntactEof) {
    ScopedTestDir test_dir("checkpoint_wal_cut_torn_tail_test_db");
    DiskManager disk;
    disk.create_file(LOG_FILE_NAME);
    CheckpointWalCut cut;
    int64_t cut_end = 0;
    {
        LogManager writer(&disk);
        BeginLogRecord obsolete(207);
        writer.add_log_to_buffer(&obsolete);
        cut = writer.create_checkpoint_wal_cut({"a.idx"});
        cut_end = disk.get_file_size(LOG_FILE_NAME);
        writer.write_restart_offset(cut.checkpoint_offset);
        BeginLogRecord torn(208);
        writer.add_log_to_buffer(&torn);
        writer.flush_log_to_disk_with_sync();
    }
    ASSERT_GT(disk.get_file_size(LOG_FILE_NAME), cut_end);
    ASSERT_EQ(::truncate(LOG_FILE_NAME.c_str(), disk.get_file_size(LOG_FILE_NAME) - 1), 0);

    LogManager restarted(&disk);
    ASSERT_NO_THROW(restarted.initialize_from_existing_log());
    EXPECT_EQ(disk.get_file_size(LOG_FILE_NAME), cut_end);
    EXPECT_EQ(restarted.current_log_offset(), cut_end);
    EXPECT_EQ(restarted.get_global_lsn(), cut.last_lsn + 1);

    BeginLogRecord after_restart(209);
    EXPECT_EQ(restarted.add_log_to_buffer(&after_restart), cut.last_lsn + 1);
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
    const Rid unrelated_rid{3, 5};
    const LockDataId unrelated_lock(42, unrelated_rid, LockDataType::RECORD);
    ASSERT_TRUE(lock_mgr.lock_exclusive_on_record(owner, rid, 42));
    ASSERT_TRUE(lock_mgr.lock_exclusive_on_record(txn, unrelated_rid, 42));
    LockAcquireResult lock_result = LockAcquireResult::Value::Granted;
    std::thread waiter([&] { lock_result = lock_mgr.lock_exclusive_on_record(txn, rid, 42); });
    const auto enqueue_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!lock_mgr.has_record_waiters_for_test() && std::chrono::steady_clock::now() < enqueue_deadline) {
        std::this_thread::yield();
    }
    if (!lock_mgr.has_record_waiters_for_test()) {
        txn_mgr.abort(txn, &log_mgr);
        waiter.join();
        txn_mgr.abort(owner, &log_mgr);
        FAIL() << "SI transaction that already owns another record lock did not enter FIFO";
        return;
    }
    ASSERT_TRUE(txn->get_write_set().empty());
    ASSERT_EQ(txn->get_prev_lsn(), txn->get_begin_lsn());

    txn_mgr.abort(txn, &log_mgr);
    waiter.join();
    ASSERT_EQ(lock_result.value(), LockAcquireResult::Value::Cancelled);

    EXPECT_EQ(log_mgr.get_persist_lsn(), INVALID_LSN);
    EXPECT_EQ(log_mgr.get_durable_lsn(), INVALID_LSN);
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
