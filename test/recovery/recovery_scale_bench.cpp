/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

/*
 * Scaled crash-recovery timing harness.
 *
 * The existing recovery regressions all run on a handful of rows, which says
 * nothing about a 256 MB WAL over a table and indexes several times larger
 * than the buffer pool. This harness builds that state, kills the writer, and
 * reports per-phase recovery cost plus a digest of the recovered database so
 * two recovery runs can be compared byte for byte.
 *
 * It is skipped unless RMDB_RECOVERY_SCALE=1, because a full run takes
 * minutes and writes gigabytes.
 *
 *   RMDB_RECOVERY_SCALE=1          enable
 *   RMDB_SCALE_ROWS=1500000        rows loaded before the workload
 *   RMDB_SCALE_WAL_MB=256          stop the workload at this WAL size
 *   RMDB_SCALE_BPM_PAGES=19200     buffer pool size (75 MB by default)
 *   RMDB_SCALE_DIR=...             working directory (kept on exit)
 *   RMDB_SCALE_RECOVER_ONLY=<dir>  skip build/workload, recover <dir> as it is
 *                                  (used to replay one crash state against a
 *                                  different build of the recovery code)
 */

#include "index/ix.h"
#include "record/rm.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "minilog.h"

namespace {

constexpr const char* kTableName = "stock";
constexpr int kHashLen = 48;
constexpr int kDataLen = 150;
// s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_hash, s_data
constexpr int kHashOffset = 6 * static_cast<int>(sizeof(int));
constexpr int kRowSize = kHashOffset + kHashLen + kDataLen;

int64_t EnvInt(const char* name, int64_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return fallback;
    }
    try {
        return std::stoll(value);
    } catch (const std::exception&) {
        return fallback;
    }
}

bool ScaleEnabled() {
    const char* value = std::getenv("RMDB_RECOVERY_SCALE");
    return value != nullptr && std::string(value) == "1";
}

double ElapsedMs(std::chrono::steady_clock::time_point begin) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - begin).count();
}

std::vector<ColDef> TableColumns() {
    return {{"s_i_id", TYPE_INT, static_cast<int>(sizeof(int))},
            {"s_w_id", TYPE_INT, static_cast<int>(sizeof(int))},
            {"s_quantity", TYPE_INT, static_cast<int>(sizeof(int))},
            {"s_ytd", TYPE_INT, static_cast<int>(sizeof(int))},
            {"s_order_cnt", TYPE_INT, static_cast<int>(sizeof(int))},
            {"s_remote_cnt", TYPE_INT, static_cast<int>(sizeof(int))},
            {"s_hash", TYPE_STRING, kHashLen},
            {"s_data", TYPE_STRING, kDataLen}};
}

// Two indexes, as orders and customer both have in the benchmark schema: one
// narrow and append ordered like a synthetic primary key, one wide and randomly
// distributed like the customer name index. The wide one is what makes the
// index working set exceed the buffer pool.
const std::vector<std::string>& PrimaryIndexCols() {
    static const std::vector<std::string> cols{"s_i_id", "s_w_id"};
    return cols;
}
const std::vector<std::string>& SecondaryIndexCols() {
    static const std::vector<std::string> cols{"s_hash", "s_i_id"};
    return cols;
}

struct Row {
    int i_id{0};
    int w_id{0};
    int quantity{0};
    int ytd{0};
    int order_cnt{0};
    int remote_cnt{0};
    uint64_t hash_seed{0};
};

void EncodeHash(uint64_t seed, char* out) {
    memset(out, ' ', kHashLen);
    // A scrambled, printable key so secondary index access is random.
    for (int i = 0; i < kHashLen; ++i) {
        seed = seed * 6364136223846793005ull + 1442695040888963407ull;
        out[i] = static_cast<char>('a' + static_cast<int>((seed >> 33) % 26));
    }
}

void EncodeRow(const Row& row, char* out) {
    int offset = 0;
    for (const int value : {row.i_id, row.w_id, row.quantity, row.ytd, row.order_cnt, row.remote_cnt}) {
        memcpy(out + offset, &value, sizeof(int));
        offset += static_cast<int>(sizeof(int));
    }
    EncodeHash(row.hash_seed, out + kHashOffset);
    memset(out + kHashOffset + kHashLen, 'x', kDataLen);
    // Keep some payload variation so the digest is sensitive to row content.
    snprintf(out + kHashOffset + kHashLen, kDataLen, "stock-%d-%d", row.i_id, row.w_id);
}

// FNV-1a over the recovered state, so two recoveries can be compared exactly.
class Digest {
public:
    void add(const void* data, size_t size) {
        const auto* bytes = static_cast<const unsigned char*>(data);
        for (size_t i = 0; i < size; ++i) {
            value_ ^= bytes[i];
            value_ *= 0x100000001b3ull;
        }
    }
    void count_item() {
        ++count_;
    }
    uint64_t value() const {
        return value_;
    }
    uint64_t count() const {
        return count_;
    }

private:
    uint64_t value_{0xcbf29ce484222325ull};
    uint64_t count_{0};
};

// Evicts a file's clean pages so recovery measures real reads instead of the
// page cache this process just warmed. No privileges required.
void DropFileCache(const std::filesystem::path& path) {
    const int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return;
    }
    (void)fsync(fd);
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);
}

void DropDirectoryCache(const std::filesystem::path& dir) {
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            DropFileCache(entry.path());
        }
    }
}

class ScaleDb {
public:
    explicit ScaleDb(size_t bpm_pages)
        : bpm_(static_cast<int>(bpm_pages), &disk_), rm_mgr_(&disk_, &bpm_), ix_mgr_(&disk_, &bpm_),
          sm_mgr_(&disk_, &bpm_, &rm_mgr_, &ix_mgr_), log_mgr_(&disk_) {}

    DiskManager disk_;
    BufferPoolManager bpm_;
    RmManager rm_mgr_;
    IxManager ix_mgr_;
    SmManager sm_mgr_;
    LogManager log_mgr_;

    IxIndexHandle* index(const std::vector<std::string>& cols) {
        return sm_mgr_.ihs_.at(ix_mgr_.get_index_name(kTableName, cols)).get();
    }
    RmFileHandle* table() {
        return sm_mgr_.fhs_.at(kTableName).get();
    }
};

std::vector<char> IndexKey(const IndexMeta& meta, const char* row) {
    std::vector<char> key(static_cast<size_t>(meta.col_tot_len));
    int offset = 0;
    for (const auto& col : meta.cols) {
        memcpy(key.data() + offset, row + col.offset, static_cast<size_t>(col.len));
        offset += col.len;
    }
    return key;
}

// Loads the initial data set and leaves a fully consistent database on disk,
// which is what a clean checkpoint plus shutdown would leave.
void BuildDatabase(const std::string& db_name, int64_t rows, size_t bpm_pages) {
    const auto begin = std::chrono::steady_clock::now();
    ScaleDb db(bpm_pages);
    db.sm_mgr_.create_db(db_name);
    db.sm_mgr_.open_db(db_name);
    db.sm_mgr_.create_table(kTableName, TableColumns(), nullptr);
    db.sm_mgr_.create_index(kTableName, PrimaryIndexCols(), nullptr);
    db.sm_mgr_.create_index(kTableName, SecondaryIndexCols(), nullptr);

    auto* table = db.table();
    const auto& tab_meta = db.sm_mgr_.db_.get_table(kTableName);
    std::vector<char> row_bytes(kRowSize);
    for (int64_t i = 0; i < rows; ++i) {
        Row row;
        row.i_id = static_cast<int>(i);
        row.w_id = static_cast<int>(i % 50);
        row.quantity = static_cast<int>(i % 1000);
        row.ytd = 0;
        row.hash_seed = static_cast<uint64_t>(i) * 0x9e3779b97f4a7c15ull + 12345ull;
        EncodeRow(row, row_bytes.data());
        const Rid rid = table->insert_record(row_bytes.data(), nullptr);
        for (const auto& index_meta : tab_meta.indexes) {
            auto key = IndexKey(index_meta, row_bytes.data());
            db.sm_mgr_.ihs_.at(db.ix_mgr_.get_index_name(kTableName, index_meta.cols))
                ->insert_entry(key.data(), rid, nullptr, true);
        }
    }
    db.sm_mgr_.close_db();
    std::cout << "[scale] load: " << rows << " rows in " << ElapsedMs(begin) / 1000.0 << " s\n";
}

// Runs a mixed DML workload against the loaded database, writing WAL exactly
// as the executors do, until the WAL reaches the requested size. Never closes
// the database: the caller kills this process so dirty pages and the unwritten
// index headers stay unwritten, as after kill -9.
void RunWorkloadThenDie(const std::string& db_name, int64_t rows, int64_t wal_target_bytes, size_t bpm_pages) {
    const auto begin = std::chrono::steady_clock::now();
    ScaleDb db(bpm_pages);
    db.sm_mgr_.open_db(db_name);
    db.log_mgr_.initialize_from_existing_log();
    db.bpm_.set_log_manager(&db.log_mgr_);

    auto* table = db.table();
    auto& tab_meta = db.sm_mgr_.db_.get_table(kTableName);
    const int records_per_page = table->get_file_hdr().num_records_per_page;
    const int loaded_pages = table->get_file_hdr().num_pages;

    std::mt19937_64 rng(20260726);
    std::vector<char> new_bytes(kRowSize);
    int64_t committed_txns = 0;
    int64_t dml_records = 0;
    txn_id_t txn_id = 1000;

    const auto append_update = [&](const Rid& rid, RmRecord& old_value, RmRecord& new_value, lsn_t prev_lsn) {
        Rid log_rid = rid;
        UpdateLogRecord record(txn_id, old_value, new_value, log_rid, kTableName);
        record.prev_lsn_ = prev_lsn;
        return db.log_mgr_.add_log_to_buffer(&record);
    };
    const auto append_insert = [&](const Rid& rid, RmRecord& value, lsn_t prev_lsn) {
        Rid log_rid = rid;
        InsertLogRecord record(txn_id, value, log_rid, kTableName);
        record.prev_lsn_ = prev_lsn;
        return db.log_mgr_.add_log_to_buffer(&record);
    };
    const auto append_delete = [&](const Rid& rid, RmRecord& value, lsn_t prev_lsn) {
        Rid log_rid = rid;
        DeleteLogRecord record(txn_id, value, log_rid, kTableName);
        record.prev_lsn_ = prev_lsn;
        return db.log_mgr_.add_log_to_buffer(&record);
    };

    // The last few transactions are left in flight, standing in for the
    // connections that were mid-transaction when the process died.
    constexpr int kInFlightTxns = 50;
    std::vector<bool> leave_uncommitted;

    while (db.log_mgr_.current_log_offset() < wal_target_bytes) {
        ++txn_id;
        BeginLogRecord begin_record(txn_id);
        lsn_t prev_lsn = db.log_mgr_.add_log_to_buffer(&begin_record);

        for (int operation = 0; operation < 10; ++operation) {
            const int64_t victim = static_cast<int64_t>(rng() % static_cast<uint64_t>(rows));
            Rid rid{static_cast<int>(victim / records_per_page), static_cast<int>(victim % records_per_page)};
            if (rid.page_no <= 0 || rid.page_no >= loaded_pages) {
                continue;
            }
            std::unique_ptr<RmRecord> current;
            try {
                if (!table->is_record(rid)) {
                    continue;
                }
                current = table->get_record(rid, nullptr);
            } catch (const std::exception&) {
                continue;
            }

            const uint64_t roll = rng() % 100;
            memcpy(new_bytes.data(), current->data, static_cast<size_t>(kRowSize));
            if (roll < 85) {
                // The common case: only non-indexed columns change, so both
                // index keys stay exactly where they are.
                int ytd = 0;
                memcpy(&ytd, new_bytes.data() + 3 * sizeof(int), sizeof(int));
                ytd += 7;
                memcpy(new_bytes.data() + 3 * sizeof(int), &ytd, sizeof(int));
            } else if (roll < 95) {
                // Move the wide secondary index key to a new random position.
                EncodeHash(rng(), new_bytes.data() + kHashOffset);
            } else {
                // Delete the row and its index entries.
                RmRecord old_value(kRowSize);
                memcpy(old_value.data, current->data, static_cast<size_t>(kRowSize));
                prev_lsn = append_delete(rid, old_value, prev_lsn);
                ++dml_records;
                for (const auto& index_meta : tab_meta.indexes) {
                    auto key = IndexKey(index_meta, old_value.data);
                    db.sm_mgr_.ihs_.at(db.ix_mgr_.get_index_name(kTableName, index_meta.cols))
                        ->delete_entry(key.data(), rid, nullptr);
                }
                table->delete_record(rid, nullptr, prev_lsn);
                continue;
            }

            RmRecord old_value(kRowSize);
            memcpy(old_value.data, current->data, static_cast<size_t>(kRowSize));
            RmRecord new_value(kRowSize);
            memcpy(new_value.data, new_bytes.data(), static_cast<size_t>(kRowSize));
            prev_lsn = append_update(rid, old_value, new_value, prev_lsn);
            ++dml_records;

            TupleMeta meta;
            meta.commit_ts_ = 0;
            meta.writer_txn_id_ = txn_id;
            meta.is_committed_ = false;
            meta.is_deleted_ = false;
            meta.version_chain_head_ = UndoLink{};
            table->apply_tuple_update(rid, new_value.data, meta, prev_lsn);
            for (const auto& index_meta : tab_meta.indexes) {
                auto old_key = IndexKey(index_meta, old_value.data);
                auto new_key = IndexKey(index_meta, new_value.data);
                if (old_key == new_key) {
                    continue;
                }
                auto* index = db.sm_mgr_.ihs_.at(db.ix_mgr_.get_index_name(kTableName, index_meta.cols)).get();
                index->delete_entry(old_key.data(), rid, nullptr);
                index->insert_entry(new_key.data(), rid, nullptr, true);
            }
        }

        // Insert one fresh row per transaction so RID allocation and index
        // growth are also exercised.
        {
            Row row;
            row.i_id = static_cast<int>(rows + txn_id);
            row.w_id = static_cast<int>(txn_id % 50);
            row.quantity = static_cast<int>(txn_id % 1000);
            row.hash_seed = rng();
            EncodeRow(row, new_bytes.data());
            const Rid rid = table->insert_record(new_bytes.data(), nullptr);
            RmRecord value(kRowSize);
            memcpy(value.data, new_bytes.data(), static_cast<size_t>(kRowSize));
            prev_lsn = append_insert(rid, value, prev_lsn);
            ++dml_records;
            for (const auto& index_meta : tab_meta.indexes) {
                auto key = IndexKey(index_meta, value.data);
                db.sm_mgr_.ihs_.at(db.ix_mgr_.get_index_name(kTableName, index_meta.cols))
                    ->insert_entry(key.data(), rid, nullptr, true);
            }
        }

        CommitLogRecord commit_record(txn_id);
        commit_record.prev_lsn_ = prev_lsn;
        db.log_mgr_.add_log_to_buffer(&commit_record);
        ++committed_txns;
        if (committed_txns % 64 == 0) {
            db.log_mgr_.flush_log_to_disk_with_sync();
        }
        // Stand in for the checkpoint thread's background preflush, which keeps
        // writing dirty table and index pages between checkpoints. Without it
        // the crash state would be far dirtier than production's ever is.
        if (committed_txns % 256 == 0) {
            db.sm_mgr_.flush_dirty_pages(2048);
        }
    }

    // Now the in-flight transactions: WAL records with no COMMIT.
    for (int i = 0; i < kInFlightTxns; ++i) {
        ++txn_id;
        BeginLogRecord begin_record(txn_id);
        lsn_t prev_lsn = db.log_mgr_.add_log_to_buffer(&begin_record);
        for (int operation = 0; operation < 8; ++operation) {
            const int64_t victim = static_cast<int64_t>(rng() % static_cast<uint64_t>(rows));
            Rid rid{static_cast<int>(victim / records_per_page), static_cast<int>(victim % records_per_page)};
            if (rid.page_no <= 0 || rid.page_no >= loaded_pages) {
                continue;
            }
            std::unique_ptr<RmRecord> current;
            try {
                if (!table->is_record(rid)) {
                    continue;
                }
                current = table->get_record(rid, nullptr);
            } catch (const std::exception&) {
                continue;
            }
            RmRecord old_value(kRowSize);
            memcpy(old_value.data, current->data, static_cast<size_t>(kRowSize));
            RmRecord new_value(kRowSize);
            memcpy(new_value.data, current->data, static_cast<size_t>(kRowSize));
            EncodeHash(rng(), new_value.data + kHashOffset);
            prev_lsn = append_update(rid, old_value, new_value, prev_lsn);
            ++dml_records;

            TupleMeta meta;
            meta.commit_ts_ = 0;
            meta.writer_txn_id_ = txn_id;
            meta.is_committed_ = false;
            meta.is_deleted_ = false;
            meta.version_chain_head_ = UndoLink{};
            table->apply_tuple_update(rid, new_value.data, meta, prev_lsn);
            for (const auto& index_meta : tab_meta.indexes) {
                auto old_key = IndexKey(index_meta, old_value.data);
                auto new_key = IndexKey(index_meta, new_value.data);
                if (old_key == new_key) {
                    continue;
                }
                auto* index = db.sm_mgr_.ihs_.at(db.ix_mgr_.get_index_name(kTableName, index_meta.cols)).get();
                index->delete_entry(old_key.data(), rid, nullptr);
                index->insert_entry(new_key.data(), rid, nullptr, true);
            }
        }
    }
    db.log_mgr_.flush_log_to_disk_with_sync();

    if (std::getenv("RMDB_SCALE_FLUSH_PAGES_BEFORE_CRASH") != nullptr) {
        // Models a crash right after the checkpoint's page flush finished but
        // before it truncated the WAL: the indexes are structurally intact on
        // disk, so recovery exercises the key-level index repair instead of
        // falling back to a rebuild.
        db.sm_mgr_.flush_all_table_and_index_pages(true);
    }

    std::cout << "[scale] workload: " << committed_txns << " committed txns, " << kInFlightTxns << " in flight, "
              << dml_records << " dml records, wal " << db.log_mgr_.current_log_offset() / (1024 * 1024) << " MB, "
              << ElapsedMs(begin) / 1000.0 << " s\n";
    std::cout.flush();
    // Disappear without flushing anything else.
    _exit(0);
}

struct RecoveryTiming {
    double startup_scan_ms{0};
    double analyze_ms{0};
    double redo_ms{0};
    double undo_ms{0};
    double total_ms{0};
    uint64_t scanned_records{0};
    uint64_t redo_applied{0};
    uint64_t undo_applied{0};
    uint64_t index_probes{0};
    uint64_t index_mutations{0};
    uint64_t analyze_page_reads{0};
    uint64_t redo_page_reads{0};
    uint64_t undo_page_reads{0};
    uint64_t page_reads{0};
    uint64_t page_writes{0};
    uint64_t wal_reads{0};
    uint64_t wal_read_bytes{0};
};

// Digest of everything a validation query could observe: the live heap rows and
// every entry of every index, both in a deterministic order.
uint64_t StateDigest(ScaleDb& db, uint64_t* row_count, uint64_t* entry_count) {
    Digest digest;
    auto* table = db.table();
    for (RmScan scan(table); !scan.is_end(); scan.next()) {
        const Rid rid = scan.rid();
        const auto meta = table->get_tuple_meta(rid);
        if (meta.is_deleted_) {
            continue;
        }
        auto record = table->get_record(rid, nullptr);
        digest.add(&rid.page_no, sizeof(rid.page_no));
        digest.add(&rid.slot_no, sizeof(rid.slot_no));
        digest.add(record->data, static_cast<size_t>(record->size));
        digest.count_item();
    }
    *row_count = digest.count();

    Digest index_digest;
    const auto& tab_meta = db.sm_mgr_.db_.get_table(kTableName);
    for (const auto& index_meta : tab_meta.indexes) {
        auto* index = db.sm_mgr_.ihs_.at(db.ix_mgr_.get_index_name(kTableName, index_meta.cols)).get();
        IxScan scan(index, index->leaf_begin(), index->leaf_end(), &db.bpm_, true, true);
        while (!scan.is_end()) {
            const Rid rid = scan.rid();
            index_digest.add(&rid.page_no, sizeof(rid.page_no));
            index_digest.add(&rid.slot_no, sizeof(rid.slot_no));
            index_digest.count_item();
            scan.next();
        }
    }
    *entry_count = index_digest.count();
    return digest.value() ^ (index_digest.value() * 0x9e3779b97f4a7c15ull);
}

RecoveryTiming RecoverAndMeasure(const std::string& db_name, size_t bpm_pages, uint64_t* digest, uint64_t* row_count,
                                 uint64_t* entry_count) {
    DropDirectoryCache(std::filesystem::path(db_name));
    RecoveryTiming timing;
    ScaleDb db(bpm_pages);
    const auto total_begin = std::chrono::steady_clock::now();
    db.sm_mgr_.open_db(db_name);

    auto phase_begin = std::chrono::steady_clock::now();
    db.log_mgr_.initialize_from_existing_log();
    timing.startup_scan_ms = ElapsedMs(phase_begin);
    db.bpm_.set_log_manager(&db.log_mgr_);

    RecoveryManager recovery(&db.disk_, &db.bpm_, &db.sm_mgr_, &db.log_mgr_);
    uint64_t reads_before = db.disk_.get_page_read_count();
    phase_begin = std::chrono::steady_clock::now();
    recovery.analyze();
    timing.analyze_ms = ElapsedMs(phase_begin);
    timing.analyze_page_reads = db.disk_.get_page_read_count() - reads_before;

    reads_before = db.disk_.get_page_read_count();
    phase_begin = std::chrono::steady_clock::now();
    recovery.redo();
    timing.redo_ms = ElapsedMs(phase_begin);
    timing.redo_page_reads = db.disk_.get_page_read_count() - reads_before;

    reads_before = db.disk_.get_page_read_count();
    phase_begin = std::chrono::steady_clock::now();
    recovery.undo();
    timing.undo_ms = ElapsedMs(phase_begin);
    timing.undo_page_reads = db.disk_.get_page_read_count() - reads_before;
    db.sm_mgr_.refresh_index_residency();
    timing.total_ms = ElapsedMs(total_begin);

    timing.scanned_records = recovery.get_scanned_record_count();
    timing.redo_applied = recovery.get_redo_applied_count();
    timing.undo_applied = recovery.get_undo_applied_count();
    timing.index_probes = recovery.get_index_probe_count();
    timing.index_mutations = recovery.get_index_mutation_count();
    timing.page_reads = db.disk_.get_page_read_count();
    timing.page_writes = db.disk_.get_page_write_count();
    timing.wal_reads = db.disk_.get_log_read_count();
    timing.wal_read_bytes = db.disk_.get_log_read_bytes();

    *digest = StateDigest(db, row_count, entry_count);
    db.sm_mgr_.close_db();
    return timing;
}

void PrintTiming(const char* label, const RecoveryTiming& timing) {
    std::cout << "[scale] " << label << "\n"
              << "         wal startup scan : " << timing.startup_scan_ms << " ms\n"
              << "         analyze          : " << timing.analyze_ms << " ms (" << timing.analyze_page_reads
              << " page reads)\n"
              << "         redo             : " << timing.redo_ms << " ms (" << timing.redo_page_reads
              << " page reads)\n"
              << "         undo+meta+index  : " << timing.undo_ms << " ms (" << timing.undo_page_reads
              << " page reads)\n"
              << "         total            : " << timing.total_ms << " ms\n"
              << "         records scanned  : " << timing.scanned_records << "\n"
              << "         redo applied     : " << timing.redo_applied << "\n"
              << "         undo applied     : " << timing.undo_applied << "\n"
              << "         index probes     : " << timing.index_probes << "\n"
              << "         index mutations  : " << timing.index_mutations << "\n"
              << "         page reads       : " << timing.page_reads << "\n"
              << "         page writes      : " << timing.page_writes << "\n"
              << "         wal preads       : " << timing.wal_reads << " (" << timing.wal_read_bytes << " bytes)\n";
    std::cout.flush();
}

} // namespace

TEST(RecoveryScaleBench, MeasurePhasesAndIdempotency) {
    if (!ScaleEnabled()) {
        GTEST_SKIP() << "set RMDB_RECOVERY_SCALE=1 to run the scaled recovery benchmark";
    }
    minilog::Logger::get().init(stdout, false);
    minilog::Logger::get().set_level(minilog::LogLevel::INFO);

    const auto bpm_pages = static_cast<size_t>(EnvInt("RMDB_SCALE_BPM_PAGES", 19200));
    const char* recover_only = std::getenv("RMDB_SCALE_RECOVER_ONLY");
    if (recover_only != nullptr) {
        // Replay an existing crash state, so the same input can be measured
        // against a different build of the recovery code.
        uint64_t digest = 0;
        uint64_t rows = 0;
        uint64_t entries = 0;
        const auto timing = RecoverAndMeasure(recover_only, bpm_pages, &digest, &rows, &entries);
        PrintTiming("recovery (replay of existing crash state)", timing);
        std::cout << "[scale] digest=" << digest << " rows=" << rows << " index_entries=" << entries << "\n";
        return;
    }

    const int64_t rows = EnvInt("RMDB_SCALE_ROWS", 1500000);
    const int64_t wal_target = EnvInt("RMDB_SCALE_WAL_MB", 256) * 1024 * 1024;
    const std::string root =
        std::getenv("RMDB_SCALE_DIR") != nullptr ? std::getenv("RMDB_SCALE_DIR") : std::string("recovery_scale_root");
    const std::string db_name = "scale_db";

    const auto original_path = std::filesystem::current_path();
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    std::filesystem::current_path(root);

    BuildDatabase(db_name, rows, bpm_pages);

    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
        RunWorkloadThenDie(db_name, rows, wal_target, bpm_pages);
        _exit(1);
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);

    std::cout << "[scale] crash state: db " << std::filesystem::file_size(db_name + "/" + kTableName) / (1024 * 1024)
              << " MB table, wal " << std::filesystem::file_size(db_name + "/" + LOG_FILE_NAME) / (1024 * 1024)
              << " MB\n";

    // Two copies of the crash state: one this test replays to prove
    // repeatability, and one left untouched so another build of the recovery
    // code can be measured against the identical input.
    const std::string snapshot = db_name + "_snapshot";
    std::filesystem::remove_all(snapshot);
    std::filesystem::copy(db_name, snapshot, std::filesystem::copy_options::recursive);
    const std::string pristine = db_name + "_pristine";
    std::filesystem::remove_all(pristine);
    std::filesystem::copy(db_name, pristine, std::filesystem::copy_options::recursive);

    uint64_t first_digest = 0;
    uint64_t first_rows = 0;
    uint64_t first_entries = 0;
    const auto first = RecoverAndMeasure(db_name, bpm_pages, &first_digest, &first_rows, &first_entries);
    PrintTiming("recovery pass 1 (cold cache)", first);
    std::cout << "[scale] digest=" << first_digest << " rows=" << first_rows << " index_entries=" << first_entries
              << "\n";

    // Recovering the already recovered database must change nothing.
    uint64_t second_digest = 0;
    uint64_t second_rows = 0;
    uint64_t second_entries = 0;
    const auto second = RecoverAndMeasure(db_name, bpm_pages, &second_digest, &second_rows, &second_entries);
    PrintTiming("recovery pass 2 (over the already recovered database)", second);
    std::cout << "[scale] digest=" << second_digest << " rows=" << second_rows << " index_entries=" << second_entries
              << "\n";
    EXPECT_EQ(first_digest, second_digest) << "recovery is not idempotent";
    EXPECT_EQ(first_rows, second_rows);
    EXPECT_EQ(first_entries, second_entries);

    // Recovering the pristine crash state again must reach the same result as
    // the first pass did, which is the property final.md:430 asks for.
    uint64_t replay_digest = 0;
    uint64_t replay_rows = 0;
    uint64_t replay_entries = 0;
    const auto replay = RecoverAndMeasure(snapshot, bpm_pages, &replay_digest, &replay_rows, &replay_entries);
    PrintTiming("recovery of the pristine crash state (repeatability)", replay);
    std::cout << "[scale] digest=" << replay_digest << " rows=" << replay_rows << " index_entries=" << replay_entries
              << "\n";
    EXPECT_EQ(first_digest, replay_digest) << "recovery is not repeatable from the same WAL";

    std::filesystem::current_path(original_path);
    if (std::getenv("RMDB_SCALE_KEEP") == nullptr) {
        std::filesystem::remove_all(root);
    } else {
        std::cout << "[scale] kept working directory " << root << "\n";
    }
}
