/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "log_defs.h"
#include "log_manager.h"
#include "record/rm_defs.h"
#include "storage/disk_manager.h"

// A borrowed view of one serialized WAL record. `bytes` points into the
// reader's buffer and stays valid only until the next WalReader::next() call.
struct WalRecordView {
    LogType log_type{LogType::BEGIN};
    lsn_t lsn{INVALID_LSN};
    lsn_t prev_lsn{INVALID_LSN};
    txn_id_t txn_id{INVALID_TXN_ID};
    uint32_t total_len{0};
    int64_t offset{0};          // byte offset of this record inside the WAL file
    const char* bytes{nullptr}; // total_len bytes, starting at the record header
};

struct WalUpdateDeltaSpan {
    uint32_t offset{0};
    uint32_t length{0};
    const char* before_bytes{nullptr};
    const char* after_bytes{nullptr};
};

// Allocation-free borrowed view of a bidirectional UPDATE delta. `span_bytes`
// aliases the WAL record and contains exactly `span_count` validated spans.
struct WalUpdateDeltaView {
    const char* span_bytes{nullptr};
    uint32_t span_bytes_length{0};
    uint32_t row_size{0};
    uint32_t span_count{0};
    uint32_t flags{0};

    bool present() const {
        return span_bytes != nullptr;
    }
};

// Advances one validated span without allocating. `cursor` is a byte offset
// inside delta.span_bytes and must start at zero.
bool ReadWalUpdateDeltaSpan(const WalUpdateDeltaView& delta, uint32_t* cursor, WalUpdateDeltaSpan* span);

// The payload of an INSERT/DELETE/UPDATE record. Legacy images and the sparse
// UPDATE after-anchor borrow record.bytes. A sparse UPDATE's before image is
// materialized here by copying the anchor and applying its before-byte spans.
struct WalDmlView {
    std::string_view table_name;
    Rid rid{};
    // DELETE and UPDATE carry the image recovery must restore during undo.
    const char* before_image{nullptr};
    int before_size{0};
    // INSERT and UPDATE carry the image recovery must install during redo.
    const char* after_image{nullptr};
    int after_size{0};
    std::vector<char> materialized_before;
    bool before_is_materialized{false};
    WalUpdateDeltaView update_delta;
};

// Parses the payload of a DML record. Returns false when the record is not a
// DML record or when its payload is malformed, in which case *out is untouched.
// Borrowed pointers alias record.bytes and share its lifetime. For a sparse
// UPDATE, before_image instead aliases out->materialized_before.
bool ParseWalDml(const WalRecordView& record, WalDmlView* out);

// Redo only needs an UPDATE's after image. This variant still validates every
// sparse before span and the exact payload end, but avoids materializing the
// complete before image.
bool ParseWalDmlForRedo(const WalRecordView& record, WalDmlView* out);

// Reads the single record stored at `offset`, using *scratch as its buffer.
// Undo needs random access along a transaction's prev_lsn chain, which is at
// most a few records per loser transaction. Returns false when the bytes at
// that offset are not a complete record. The view aliases *scratch.
bool ReadWalRecordAt(DiskManager* disk_manager, int64_t offset, int64_t end_offset, std::vector<char>* scratch,
                     WalRecordView* out);

/**
 * Forward streaming reader over the WAL file.
 *
 * One large pread per buffer refill replaces the two per-record reads (each
 * with its own stat) that recovery used to issue, and records are parsed in
 * place so a full pass allocates nothing. Records that straddle a buffer
 * boundary are spliced by carrying the partial tail to the front of the
 * buffer before the next refill.
 *
 * The reader stops at the first byte that does not begin a complete,
 * well-formed record. That is the normal end of a WAL that was being appended
 * to when the process died; next_offset() then reports the end of the intact
 * prefix.
 */
class WalReader {
public:
    static constexpr int kDefaultBufferBytes = 16 * 1024 * 1024;

    // Scans [begin_offset, end_offset) of the WAL. The default buffer already
    // holds any record a LogManager can emit; a smaller one still works,
    // growing once if a record does not fit.
    WalReader(DiskManager* disk_manager, int64_t begin_offset, int64_t end_offset,
              int buffer_bytes = kDefaultBufferBytes);

    WalReader(const WalReader&) = delete;
    WalReader& operator=(const WalReader&) = delete;

    // Fills *out with the next record and advances. Returns false at the end
    // of the intact prefix. Invalidates the previously returned view.
    bool next(WalRecordView* out);

    // Offset of the record next() would return, i.e. the end of everything
    // already consumed. After next() returns false this is the end of the
    // intact WAL prefix.
    int64_t next_offset() const {
        return buffer_offset_ + static_cast<int64_t>(cursor_);
    }

    // Number of pread calls issued so far, for recovery diagnostics.
    uint64_t read_count() const {
        return read_count_;
    }

private:
    int buffered() const {
        return buffer_size_ - cursor_;
    }

    // Moves the unconsumed tail to the front and reads as much as the buffer
    // and the scan bound allow. Returns false when nothing more was read.
    bool refill();

    DiskManager* disk_manager_;
    int64_t end_offset_;
    std::vector<char> buffer_;
    int64_t buffer_offset_{0}; // file offset of buffer_[0]
    int buffer_size_{0};       // valid bytes in buffer_
    int cursor_{0};            // next unconsumed byte in buffer_
    bool exhausted_{false};    // the scan bound has been reached
    uint64_t read_count_{0};
};
