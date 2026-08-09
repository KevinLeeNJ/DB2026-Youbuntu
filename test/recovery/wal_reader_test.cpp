/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "recovery/wal_reader.h"
#include "recovery/log_manager.h"
#include "storage/disk_manager.h"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

class ScopedWalDir {
public:
    explicit ScopedWalDir(std::string dir) : old_path_(std::filesystem::current_path()), dir_(std::move(dir)) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directory(dir_);
        std::filesystem::current_path(dir_);
    }

    ~ScopedWalDir() {
        std::filesystem::current_path(old_path_);
        std::filesystem::remove_all(dir_);
    }

private:
    std::filesystem::path old_path_;
    std::filesystem::path dir_;
};

RmRecord MakeRow(int size, char fill) {
    RmRecord record(size);
    memset(record.data, fill, static_cast<size_t>(size));
    return record;
}

// Appends a record with the given LSN to `bytes`, mimicking what
// add_log_to_buffer writes into the WAL file.
void Append(std::vector<char>* bytes, LogRecord& record, lsn_t lsn) {
    record.lsn_ = lsn;
    const size_t begin = bytes->size();
    bytes->resize(begin + record.log_tot_len_);
    record.serialize(bytes->data() + begin);
}

void AppendRaw(std::vector<char>* bytes, LogType type, uint32_t total_len, lsn_t lsn) {
    ASSERT_GE(total_len, static_cast<uint32_t>(LOG_HEADER_SIZE));
    const size_t begin = bytes->size();
    bytes->resize(begin + total_len, 0);
    const txn_id_t txn_id = lsn + 1;
    const lsn_t prev_lsn = INVALID_LSN;
    memcpy(bytes->data() + begin + OFFSET_LOG_TYPE, &type, sizeof(type));
    memcpy(bytes->data() + begin + OFFSET_LSN, &lsn, sizeof(lsn));
    memcpy(bytes->data() + begin + OFFSET_LOG_TOT_LEN, &total_len, sizeof(total_len));
    memcpy(bytes->data() + begin + OFFSET_LOG_TID, &txn_id, sizeof(txn_id));
    memcpy(bytes->data() + begin + OFFSET_PREV_LSN, &prev_lsn, sizeof(prev_lsn));
}

WalRecordView BorrowRecord(std::vector<char>* bytes) {
    WalRecordView view;
    view.bytes = bytes->data();
    view.log_type = read_unaligned<LogType>(view.bytes + OFFSET_LOG_TYPE);
    view.lsn = read_unaligned<lsn_t>(view.bytes + OFFSET_LSN);
    view.total_len = read_unaligned<uint32_t>(view.bytes + OFFSET_LOG_TOT_LEN);
    view.txn_id = read_unaligned<txn_id_t>(view.bytes + OFFSET_LOG_TID);
    view.prev_lsn = read_unaligned<lsn_t>(view.bytes + OFFSET_PREV_LSN);
    return view;
}

void WriteWal(const std::vector<char>& bytes) {
    std::ofstream out(LOG_FILE_NAME, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
}

struct ScannedRecord {
    LogType log_type;
    lsn_t lsn;
    lsn_t prev_lsn;
    txn_id_t txn_id;
    uint32_t total_len;
    int64_t offset;
    std::string table_name;
    Rid rid;
    std::string before_image;
    std::string after_image;
};

std::vector<ScannedRecord> ScanAll(DiskManager* disk, int64_t end_offset, int buffer_bytes) {
    WalReader reader(disk, 0, end_offset, buffer_bytes);
    WalRecordView view;
    std::vector<ScannedRecord> scanned;
    while (reader.next(&view)) {
        ScannedRecord entry;
        entry.log_type = view.log_type;
        entry.lsn = view.lsn;
        entry.prev_lsn = view.prev_lsn;
        entry.txn_id = view.txn_id;
        entry.total_len = view.total_len;
        entry.offset = view.offset;
        WalDmlView dml;
        if (ParseWalDml(view, &dml)) {
            entry.table_name.assign(dml.table_name);
            entry.rid = dml.rid;
            if (dml.before_image != nullptr) {
                entry.before_image.assign(dml.before_image, static_cast<size_t>(dml.before_size));
            }
            if (dml.after_image != nullptr) {
                entry.after_image.assign(dml.after_image, static_cast<size_t>(dml.after_size));
            }
        }
        scanned.push_back(std::move(entry));
    }
    return scanned;
}

// A WAL holding one record of every type, with row images wide enough that a
// small reader buffer is forced to splice across refills.
std::vector<char> BuildMixedWal() {
    std::vector<char> bytes;
    Rid rid{7, 3};
    auto old_row = MakeRow(40, 'o');
    auto new_row = MakeRow(40, 'n');
    auto inserted = MakeRow(24, 'i');
    auto deleted = MakeRow(56, 'd');

    BeginLogRecord begin(11);
    Append(&bytes, begin, 100);

    InsertLogRecord insert(11, inserted, rid, "warehouse");
    insert.prev_lsn_ = 100;
    Append(&bytes, insert, 101);

    UpdateLogRecord update(11, old_row, new_row, rid, "district");
    update.prev_lsn_ = 101;
    Append(&bytes, update, 102);

    DeleteLogRecord del(11, deleted, rid, "new_orders");
    del.prev_lsn_ = 102;
    Append(&bytes, del, 103);

    CommitLogRecord commit(11);
    commit.prev_lsn_ = 103;
    Append(&bytes, commit, 104);

    AbortLogRecord abort(12);
    Append(&bytes, abort, 105);

    CheckpointLogRecord checkpoint(std::unordered_map<txn_id_t, lsn_t>{{12, 105}});
    Append(&bytes, checkpoint, 106);
    return bytes;
}

} // namespace

TEST(WalReaderTest, StreamsEveryRecordTypeWithItsPayload) {
    ScopedWalDir dir("wal_reader_types_root");
    const auto bytes = BuildMixedWal();
    WriteWal(bytes);

    DiskManager disk;
    const auto scanned = ScanAll(&disk, static_cast<int64_t>(bytes.size()), WalReader::kDefaultBufferBytes);
    ASSERT_EQ(scanned.size(), 7u);

    EXPECT_EQ(scanned[0].log_type, LogType::BEGIN);
    EXPECT_EQ(scanned[0].lsn, 100);
    EXPECT_EQ(scanned[0].txn_id, 11);
    EXPECT_EQ(scanned[0].offset, 0);

    EXPECT_EQ(scanned[1].log_type, LogType::INSERT);
    EXPECT_EQ(scanned[1].table_name, "warehouse");
    EXPECT_EQ(scanned[1].rid.page_no, 7);
    EXPECT_EQ(scanned[1].rid.slot_no, 3);
    EXPECT_EQ(scanned[1].after_image, std::string(24, 'i'));
    EXPECT_TRUE(scanned[1].before_image.empty());
    EXPECT_EQ(scanned[1].prev_lsn, 100);

    EXPECT_EQ(scanned[2].log_type, LogType::UPDATE);
    EXPECT_EQ(scanned[2].table_name, "district");
    EXPECT_EQ(scanned[2].before_image, std::string(40, 'o'));
    EXPECT_EQ(scanned[2].after_image, std::string(40, 'n'));

    EXPECT_EQ(scanned[3].log_type, LogType::DELETE);
    EXPECT_EQ(scanned[3].table_name, "new_orders");
    EXPECT_EQ(scanned[3].before_image, std::string(56, 'd'));
    EXPECT_TRUE(scanned[3].after_image.empty());

    EXPECT_EQ(scanned[4].log_type, LogType::COMMIT);
    EXPECT_EQ(scanned[5].log_type, LogType::ABORT);
    EXPECT_EQ(scanned[5].txn_id, 12);
    EXPECT_EQ(scanned[6].log_type, LogType::CHECKPOINT);
}

TEST(WalReaderTest, RecordsSplicedAcrossBufferBoundariesAreIdentical) {
    ScopedWalDir dir("wal_reader_boundary_root");
    const auto bytes = BuildMixedWal();
    WriteWal(bytes);

    DiskManager disk;
    const auto reference = ScanAll(&disk, static_cast<int64_t>(bytes.size()), WalReader::kDefaultBufferBytes);
    ASSERT_FALSE(reference.empty());

    // Sweep every buffer size from "smaller than a header" upward, so the
    // refill boundary lands inside each field of each record in turn.
    for (int buffer_bytes = 1; buffer_bytes <= static_cast<int>(bytes.size()) + 4; ++buffer_bytes) {
        const auto scanned = ScanAll(&disk, static_cast<int64_t>(bytes.size()), buffer_bytes);
        ASSERT_EQ(scanned.size(), reference.size()) << "buffer_bytes=" << buffer_bytes;
        for (size_t i = 0; i < scanned.size(); ++i) {
            EXPECT_EQ(scanned[i].log_type, reference[i].log_type) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].lsn, reference[i].lsn) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].prev_lsn, reference[i].prev_lsn) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].txn_id, reference[i].txn_id) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].total_len, reference[i].total_len) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].offset, reference[i].offset) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].table_name, reference[i].table_name) << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].before_image, reference[i].before_image)
                << "buffer_bytes=" << buffer_bytes << " i=" << i;
            EXPECT_EQ(scanned[i].after_image, reference[i].after_image)
                << "buffer_bytes=" << buffer_bytes << " i=" << i;
        }
    }
}

TEST(WalReaderTest, TruncatedTailStopsAtTheIntactPrefix) {
    ScopedWalDir dir("wal_reader_tail_root");
    const auto bytes = BuildMixedWal();

    // Every physical cut that leaves a partial final header or body accepts
    // exactly the complete-record prefix before it; no other cut is silently
    // accepted as a torn tail.
    WriteWal(bytes);
    DiskManager full_disk;
    WalReader full_reader(&full_disk, 0, static_cast<int64_t>(bytes.size()));
    WalRecordView full_view;
    std::vector<int64_t> record_ends{0};
    while (full_reader.next(&full_view)) record_ends.push_back(full_reader.next_offset());
    ASSERT_EQ(record_ends.size(), 8u);

    for (size_t cut = 1; cut < bytes.size(); ++cut) {
        std::vector<char> truncated(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(cut));
        WriteWal(truncated);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(truncated.size()));
        WalRecordView view;
        size_t count = 0;
        while (reader.next(&view)) {
            ++count;
        }
        size_t expected_count = 0;
        while (expected_count + 1 < record_ends.size() &&
               record_ends[expected_count + 1] <= static_cast<int64_t>(cut)) {
            ++expected_count;
        }
        EXPECT_EQ(count, expected_count) << "cut=" << cut;
        EXPECT_EQ(reader.next_offset(), record_ends[expected_count]) << "cut=" << cut;
        EXPECT_EQ(reader.status(), record_ends[expected_count] == static_cast<int64_t>(cut)
                                       ? WalReadStatus::kCleanEnd
                                       : WalReadStatus::kTornTail)
            << "cut=" << cut;
    }
}

TEST(WalReaderTest, CompleteGarbageHeadersFailClosedInsteadOfEndingTheLog) {
    ScopedWalDir dir("wal_reader_garbage_root");
    auto bytes = BuildMixedWal();
    const int64_t second_offset = static_cast<int64_t>(LOG_HEADER_SIZE);

    {
        // A length that would run past the file must not be followed.
        auto corrupted = bytes;
        const uint32_t absurd = 0x7fffffffu;
        memcpy(corrupted.data() + second_offset + OFFSET_LOG_TOT_LEN, &absurd, sizeof(absurd));
        WriteWal(corrupted);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(corrupted.size()));
        WalRecordView view;
        size_t count = 0;
        while (reader.next(&view)) {
            ++count;
        }
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(reader.next_offset(), second_offset);
        EXPECT_EQ(reader.status(), WalReadStatus::kCorruption);
    }

    {
        // A zero length would otherwise make the scan spin in place.
        auto corrupted = bytes;
        const uint32_t zero = 0;
        memcpy(corrupted.data() + second_offset + OFFSET_LOG_TOT_LEN, &zero, sizeof(zero));
        WriteWal(corrupted);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(corrupted.size()));
        WalRecordView view;
        size_t count = 0;
        while (reader.next(&view)) {
            ++count;
        }
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(reader.next_offset(), second_offset);
        EXPECT_EQ(reader.status(), WalReadStatus::kCorruption);
    }

    {
        // An unrecognized type means the bytes were not written by this build.
        auto corrupted = bytes;
        const int unknown = 42;
        memcpy(corrupted.data() + second_offset + OFFSET_LOG_TYPE, &unknown, sizeof(unknown));
        WriteWal(corrupted);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(corrupted.size()));
        WalRecordView view;
        size_t count = 0;
        while (reader.next(&view)) {
            ++count;
        }
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(reader.next_offset(), second_offset);
        EXPECT_EQ(reader.status(), WalReadStatus::kCorruption);
    }
}

TEST(WalReaderTest, HeaderValidationPrecedesTornBodyClassification) {
    ScopedWalDir dir("wal_reader_header_before_torn_root");
    for (LogType type : {static_cast<LogType>(42), LogType::BEGIN, LogType::COMMIT}) {
        std::vector<char> bytes;
        AppendRaw(&bytes, LogType::INSERT, LOG_HEADER_SIZE + 10, 1);
        bytes.resize(LOG_HEADER_SIZE);
        const uint32_t declared = LOG_HEADER_SIZE + 10;
        memcpy(bytes.data() + OFFSET_LOG_TYPE, &type, sizeof(type));
        memcpy(bytes.data() + OFFSET_LOG_TOT_LEN, &declared, sizeof(declared));
        WriteWal(bytes);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(bytes.size()));
        WalRecordView view;
        EXPECT_FALSE(reader.next(&view));
        EXPECT_EQ(reader.status(), WalReadStatus::kCorruption) << static_cast<int>(type);
    }

    std::vector<char> valid_variable_header;
    AppendRaw(&valid_variable_header, LogType::INSERT, LOG_HEADER_SIZE + 10, 1);
    valid_variable_header.resize(LOG_HEADER_SIZE);
    WriteWal(valid_variable_header);
    DiskManager disk;
    WalReader reader(&disk, 0, static_cast<int64_t>(valid_variable_header.size()));
    WalRecordView view;
    EXPECT_FALSE(reader.next(&view));
    EXPECT_EQ(reader.status(), WalReadStatus::kTornTail);
}

TEST(WalReaderTest, Int64BoundaryRangesDoNotOverflow) {
    ScopedWalDir dir("wal_reader_int64_boundary_root");
    WriteWal({});
    DiskManager disk;
    WalRecordView view;
    WalReader empty_at_limit(&disk, INT64_MAX, INT64_MAX);
    EXPECT_FALSE(empty_at_limit.next(&view));
    EXPECT_EQ(empty_at_limit.next_offset(), INT64_MAX);
    EXPECT_EQ(empty_at_limit.status(), WalReadStatus::kCleanEnd);

    std::vector<char> scratch;
    EXPECT_FALSE(ReadWalRecordAt(&disk, INT64_MAX - LOG_HEADER_SIZE + 1, INT64_MAX, &scratch, &view));
    EXPECT_FALSE(ReadWalRecordAt(&disk, INT64_MAX, INT64_MAX, &scratch, &view));
}

TEST(WalReaderTest, MalformedCheckpointCountAndTrailingBytesFailClosed) {
    ScopedWalDir dir("wal_reader_checkpoint_malformed_root");
    auto bytes = BuildMixedWal();
    const auto checkpoint_offset = bytes.size() - (LOG_HEADER_SIZE + sizeof(size_t) + sizeof(txn_id_t) + sizeof(lsn_t));

    for (bool trailing : {false, true}) {
        auto corrupted = bytes;
        if (trailing) {
            const uint32_t longer = read_unaligned<uint32_t>(corrupted.data() + checkpoint_offset + OFFSET_LOG_TOT_LEN) + 1;
            corrupted.insert(corrupted.end(), 'x');
            memcpy(corrupted.data() + checkpoint_offset + OFFSET_LOG_TOT_LEN, &longer, sizeof(longer));
        } else {
            const size_t impossible = 2;
            memcpy(corrupted.data() + checkpoint_offset + OFFSET_LOG_DATA, &impossible, sizeof(impossible));
        }
        WriteWal(corrupted);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(corrupted.size()));
        WalRecordView view;
        while (reader.next(&view)) {
        }
        EXPECT_EQ(reader.next_offset(), static_cast<int64_t>(checkpoint_offset));
        EXPECT_EQ(reader.status(), WalReadStatus::kCorruption);
        std::vector<char> scratch;
        EXPECT_FALSE(ReadWalRecordAt(&disk, static_cast<int64_t>(checkpoint_offset),
                                     static_cast<int64_t>(corrupted.size()), &scratch, &view));
    }
}

TEST(WalReaderTest, FramerCatalogueIsContiguousAndNeverSplitsRecords) {
    ScopedWalDir dir("wal_framer_catalogue_root");
    const auto bytes = BuildMixedWal();
    WriteWal(bytes);
    DiskManager disk;
    WalFramer framer(&disk, 0, static_cast<int64_t>(bytes.size()), 17);
    WalRecordView view;
    while (framer.next(&view)) {
    }
    ASSERT_EQ(framer.status(), WalReadStatus::kCleanEnd);
    int64_t covered = 0;
    uint32_t records = 0;
    for (const FramedSegment& segment : framer.segments()) {
        EXPECT_EQ(segment.begin, covered);
        EXPECT_GT(segment.end, segment.begin);
        covered = segment.end;
        records += segment.record_count;
    }
    EXPECT_EQ(covered, static_cast<int64_t>(bytes.size()));
    EXPECT_EQ(records, 7u);
}

TEST(WalReaderTest, FramerHonorsProductionByteAndLargeRecordBoundaries) {
    ScopedWalDir dir("wal_framer_byte_targets_root");
    {
        std::vector<char> bytes;
        constexpr uint32_t kRecordBytes = 1024 * 1024;
        for (lsn_t lsn = 0; lsn < 17; ++lsn) AppendRaw(&bytes, LogType::INSERT, kRecordBytes, lsn);
        WriteWal(bytes);
        DiskManager disk;
        WalFramer framer(&disk, 0, static_cast<int64_t>(bytes.size()));
        WalRecordView view;
        while (framer.next(&view)) {
        }
        ASSERT_EQ(framer.segments().size(), 2u);
        EXPECT_EQ(framer.segments()[0].end - framer.segments()[0].begin, WalFramer::kTargetBytes);
        EXPECT_EQ(framer.segments()[0].record_count, 16u);
        EXPECT_EQ(framer.segments()[1].record_count, 1u);
    }
    {
        std::vector<char> bytes;
        BeginLogRecord before(1);
        Append(&bytes, before, 0);
        AppendRaw(&bytes, LogType::INSERT, static_cast<uint32_t>(WalFramer::kTargetBytes + 1), 1);
        BeginLogRecord after(3);
        Append(&bytes, after, 2);
        WriteWal(bytes);
        DiskManager disk;
        WalFramer framer(&disk, 0, static_cast<int64_t>(bytes.size()));
        WalRecordView view;
        while (framer.next(&view)) {
        }
        ASSERT_EQ(framer.segments().size(), 3u);
        EXPECT_EQ(framer.segments()[0].record_count, 1u);
        EXPECT_EQ(framer.segments()[1].record_count, 1u);
        EXPECT_EQ(framer.segments()[1].end - framer.segments()[1].begin, WalFramer::kTargetBytes + 1);
        EXPECT_EQ(framer.segments()[0].end, framer.segments()[1].begin);
        EXPECT_EQ(framer.segments()[1].end, framer.segments()[2].begin);
        EXPECT_EQ(framer.segments()[2].record_count, 1u);
    }
}

TEST(WalReaderTest, FramerHonorsProductionRecordCountBoundary) {
    ScopedWalDir dir("wal_framer_record_target_root");
    std::vector<char> bytes;
    bytes.reserve(static_cast<size_t>(WalFramer::kTargetRecords + 1) * LOG_HEADER_SIZE);
    for (uint32_t i = 0; i <= WalFramer::kTargetRecords; ++i) {
        BeginLogRecord begin(static_cast<txn_id_t>(i + 1));
        Append(&bytes, begin, static_cast<lsn_t>(i));
    }
    WriteWal(bytes);
    DiskManager disk;
    WalFramer framer(&disk, 0, static_cast<int64_t>(bytes.size()));
    WalRecordView view;
    while (framer.next(&view)) {
    }
    ASSERT_EQ(framer.segments().size(), 2u);
    EXPECT_EQ(framer.segments()[0].record_count, WalFramer::kTargetRecords);
    EXPECT_EQ(framer.segments()[1].record_count, 1u);
    EXPECT_EQ(framer.segments()[0].end, framer.segments()[1].begin);
    EXPECT_EQ(framer.segments()[1].end, static_cast<int64_t>(bytes.size()));
}

TEST(WalReaderTest, ParseWalDmlRejectsAPayloadCutShortByItsOwnLength) {
    ScopedWalDir dir("wal_reader_payload_root");
    std::vector<char> bytes;
    Rid rid{4, 1};
    auto row = MakeRow(32, 'x');
    InsertLogRecord insert(11, row, rid, "stock");
    Append(&bytes, insert, 200);
    // Claim a row image longer than the record can hold.
    const int lying_size = 4096;
    memcpy(bytes.data() + OFFSET_LOG_DATA, &lying_size, sizeof(lying_size));
    WriteWal(bytes);

    DiskManager disk;
    WalReader reader(&disk, 0, static_cast<int64_t>(bytes.size()));
    WalRecordView view;
    ASSERT_TRUE(reader.next(&view));
    WalDmlView dml;
    EXPECT_FALSE(ParseWalDml(view, &dml));
}

TEST(WalReaderTest, SparseUpdateMaterializesACompleteBeforeImage) {
    Rid rid{4, 2};
    auto old_row = MakeRow(64, 'a');
    auto new_row = MakeRow(64, 'a');
    new_row.data[5] = 'x';
    new_row.data[20] = 'y';
    UpdateLogRecord update(12, old_row, new_row, rid, "stock");
    ASSERT_TRUE(update.sparse_encoding_);
    update.lsn_ = 201;
    std::vector<char> bytes(update.log_tot_len_);
    update.serialize(bytes.data());

    WalRecordView view = BorrowRecord(&bytes);
    WalDmlView dml;
    ASSERT_TRUE(ParseWalDml(view, &dml));
    EXPECT_TRUE(dml.before_is_materialized);
    EXPECT_EQ(dml.before_image, dml.materialized_before.data());
    EXPECT_EQ(dml.before_size, 64);
    EXPECT_EQ(dml.after_size, 64);
    EXPECT_EQ(std::string(dml.before_image, dml.before_size), std::string(64, 'a'));
    EXPECT_EQ(std::string(dml.after_image, dml.after_size),
              std::string(5, 'a') + "x" + std::string(14, 'a') + "y" + std::string(43, 'a'));
    EXPECT_EQ(dml.rid, rid);
    EXPECT_EQ(dml.table_name, "stock");

    WalDmlView redo_dml;
    ASSERT_TRUE(ParseWalDmlForRedo(view, &redo_dml));
    EXPECT_FALSE(redo_dml.before_is_materialized);
    EXPECT_TRUE(redo_dml.materialized_before.empty());
    EXPECT_EQ(redo_dml.before_image, nullptr);
    EXPECT_EQ(redo_dml.after_size, 64);
    EXPECT_EQ(std::string(redo_dml.after_image, redo_dml.after_size),
              std::string(5, 'a') + "x" + std::string(14, 'a') + "y" + std::string(43, 'a'));
    EXPECT_EQ(redo_dml.rid, rid);
    EXPECT_EQ(redo_dml.table_name, "stock");
}

TEST(WalReaderTest, BidirectionalUpdateBorrowsValidatedBeforeAndAfterSpans) {
    Rid rid{4, 3};
    auto old_row = MakeRow(64, 'a');
    auto new_row = MakeRow(64, 'a');
    new_row.data[5] = 'x';
    new_row.data[20] = 'y';
    UpdateLogRecord update(13, old_row, new_row, rid, "stock", true);
    ASSERT_TRUE(update.bidirectional_delta_encoding_);
    update.lsn_ = 202;
    std::vector<char> bytes(update.log_tot_len_);
    update.serialize(bytes.data());

    WalRecordView view = BorrowRecord(&bytes);
    WalDmlView dml;
    ASSERT_TRUE(ParseWalDml(view, &dml));
    EXPECT_EQ(dml.before_image, nullptr);
    EXPECT_EQ(dml.after_image, nullptr);
    ASSERT_TRUE(dml.update_delta.present());
    EXPECT_EQ(dml.update_delta.row_size, 64U);
    EXPECT_EQ(dml.update_delta.flags, UpdateLogRecord::kIndexKeysUnchangedFlag);
    EXPECT_EQ(dml.update_delta.span_count, 2U);
    EXPECT_EQ(dml.rid, rid);
    EXPECT_EQ(dml.table_name, "stock");

    uint32_t cursor = 0;
    WalUpdateDeltaSpan span;
    ASSERT_TRUE(ReadWalUpdateDeltaSpan(dml.update_delta, &cursor, &span));
    EXPECT_EQ(span.offset, 5U);
    EXPECT_EQ(span.length, 1U);
    EXPECT_EQ(span.before_bytes[0], 'a');
    EXPECT_EQ(span.after_bytes[0], 'x');
    ASSERT_TRUE(ReadWalUpdateDeltaSpan(dml.update_delta, &cursor, &span));
    EXPECT_EQ(span.offset, 20U);
    EXPECT_EQ(span.length, 1U);
    EXPECT_EQ(span.before_bytes[0], 'a');
    EXPECT_EQ(span.after_bytes[0], 'y');
    EXPECT_EQ(cursor, dml.update_delta.span_bytes_length);

    auto malformed = bytes;
    const uint32_t unknown_flags = 2;
    const int flags_offset = OFFSET_LOG_DATA + sizeof(int) + sizeof(uint32_t);
    memcpy(malformed.data() + flags_offset, &unknown_flags, sizeof(unknown_flags));
    WalRecordView malformed_view = BorrowRecord(&malformed);
    EXPECT_FALSE(ParseWalDml(malformed_view, &dml));
}

TEST(WalReaderTest, SparseUpdateRejectsMalformedVersionSpansAndTrailingBytes) {
    Rid rid{4, 3};
    auto old_row = MakeRow(64, 'a');
    auto new_row = MakeRow(64, 'a');
    new_row.data[5] = 'x';
    new_row.data[20] = 'y';
    UpdateLogRecord update(13, old_row, new_row, rid, "stock");
    ASSERT_TRUE(update.sparse_encoding_);
    ASSERT_EQ(update.before_spans_.size(), 2U);
    update.lsn_ = 202;
    std::vector<char> valid(update.log_tot_len_);
    update.serialize(valid.data());

    const int span_count_offset = OFFSET_LOG_DATA + sizeof(int) + sizeof(int) + new_row.size;
    const int first_span_offset = span_count_offset + sizeof(uint32_t);
    const int second_span_offset =
        first_span_offset + sizeof(uint32_t) * 2 + static_cast<int>(update.before_spans_[0].length);
    const auto rejected = [&](std::vector<char> bytes) {
        WalRecordView view = BorrowRecord(&bytes);
        WalDmlView dml;
        WalDmlView redo_dml;
        return !ParseWalDml(view, &dml) && !ParseWalDmlForRedo(view, &redo_dml);
    };

    {
        auto bytes = valid;
        const int unknown_version = -3;
        memcpy(bytes.data() + OFFSET_LOG_DATA, &unknown_version, sizeof(unknown_version));
        EXPECT_TRUE(rejected(std::move(bytes)));
    }
    {
        auto bytes = valid;
        const uint32_t count_bomb = static_cast<uint32_t>(new_row.size + 1);
        memcpy(bytes.data() + span_count_offset, &count_bomb, sizeof(count_bomb));
        EXPECT_TRUE(rejected(std::move(bytes)));
    }
    {
        auto bytes = valid;
        const uint32_t overlap = update.before_spans_[0].offset;
        memcpy(bytes.data() + second_span_offset, &overlap, sizeof(overlap));
        EXPECT_TRUE(rejected(std::move(bytes)));
    }
    {
        auto bytes = valid;
        const uint32_t out_of_bounds = static_cast<uint32_t>(new_row.size + 1);
        memcpy(bytes.data() + first_span_offset + sizeof(uint32_t), &out_of_bounds, sizeof(out_of_bounds));
        EXPECT_TRUE(rejected(std::move(bytes)));
    }
    {
        auto bytes = valid;
        bytes.push_back('!');
        const uint32_t length_with_trailing_byte = static_cast<uint32_t>(bytes.size());
        memcpy(bytes.data() + OFFSET_LOG_TOT_LEN, &length_with_trailing_byte, sizeof(length_with_trailing_byte));
        EXPECT_TRUE(rejected(std::move(bytes)));
    }
}

TEST(WalReaderTest, ReadWalRecordAtMatchesTheStreamingScan) {
    ScopedWalDir dir("wal_reader_random_root");
    const auto bytes = BuildMixedWal();
    WriteWal(bytes);

    DiskManager disk;
    const auto scanned = ScanAll(&disk, static_cast<int64_t>(bytes.size()), WalReader::kDefaultBufferBytes);
    ASSERT_EQ(scanned.size(), 7u);

    std::vector<char> scratch;
    for (const auto& expected : scanned) {
        WalRecordView view;
        ASSERT_TRUE(ReadWalRecordAt(&disk, expected.offset, static_cast<int64_t>(bytes.size()), &scratch, &view))
            << "offset=" << expected.offset << " type=" << static_cast<int>(expected.log_type);
        EXPECT_EQ(view.log_type, expected.log_type);
        EXPECT_EQ(view.lsn, expected.lsn);
        EXPECT_EQ(view.prev_lsn, expected.prev_lsn);
        EXPECT_EQ(view.txn_id, expected.txn_id);
        EXPECT_EQ(view.total_len, expected.total_len);
        WalDmlView dml;
        if (ParseWalDml(view, &dml)) {
            EXPECT_EQ(std::string(dml.table_name), expected.table_name);
            EXPECT_EQ(dml.rid, expected.rid);
        }
    }

    // Past the end and inside the last record are both rejected.
    WalRecordView view;
    EXPECT_FALSE(ReadWalRecordAt(&disk, static_cast<int64_t>(bytes.size()), static_cast<int64_t>(bytes.size()),
                                 &scratch, &view));
    EXPECT_FALSE(
        ReadWalRecordAt(&disk, scanned.back().offset, static_cast<int64_t>(bytes.size()) - 1, &scratch, &view));
}
