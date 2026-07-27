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

    // Cutting the file anywhere inside the last record must yield the six
    // records before it and report their exact end as the intact prefix.
    const auto full = [&] {
        WriteWal(bytes);
        DiskManager disk;
        return ScanAll(&disk, static_cast<int64_t>(bytes.size()), WalReader::kDefaultBufferBytes);
    }();
    ASSERT_EQ(full.size(), 7u);
    const int64_t last_offset = full.back().offset;

    for (size_t cut = static_cast<size_t>(last_offset) + 1; cut < bytes.size(); ++cut) {
        std::vector<char> truncated(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(cut));
        WriteWal(truncated);
        DiskManager disk;
        WalReader reader(&disk, 0, static_cast<int64_t>(truncated.size()));
        WalRecordView view;
        size_t count = 0;
        while (reader.next(&view)) {
            ++count;
        }
        EXPECT_EQ(count, 6u) << "cut=" << cut;
        EXPECT_EQ(reader.next_offset(), last_offset) << "cut=" << cut;
    }
}

TEST(WalReaderTest, GarbageLengthAndUnknownTypeAreTreatedAsEndOfLog) {
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
    }
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
        ASSERT_TRUE(ReadWalRecordAt(&disk, expected.offset, static_cast<int64_t>(bytes.size()), &scratch, &view));
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
