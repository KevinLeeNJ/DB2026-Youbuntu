/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "wal_reader.h"

#include <algorithm>
#include <climits>
#include <cstring>

namespace {

constexpr uint32_t kMaxRecordBytes = MAX_INDEX_SMO_RECORD_BYTES;

bool IsKnownLogType(LogType type) {
    switch (type) {
    case LogType::UPDATE:
    case LogType::INSERT:
    case LogType::DELETE:
    case LogType::BEGIN:
    case LogType::COMMIT:
    case LogType::ABORT:
    case LogType::CHECKPOINT:
    case LogType::INDEX_BIND:
    case LogType::INDEX_SMO:
        return true;
    default:
        return false;
    }
}

// Header-only validation runs as soon as LOG_HEADER_SIZE bytes exist. This
// ordering is what keeps an unknown type or an impossible fixed-size control
// record from masquerading as a torn body merely by claiming a longer length.
bool HasValidWalHeader(const char* bytes, uint32_t* total_len_out) {
    const LogType type = read_unaligned<LogType>(bytes + OFFSET_LOG_TYPE);
    const lsn_t lsn = read_unaligned<lsn_t>(bytes + OFFSET_LSN);
    const uint32_t total_len = read_unaligned<uint32_t>(bytes + OFFSET_LOG_TOT_LEN);
    const txn_id_t txn_id = read_unaligned<txn_id_t>(bytes + OFFSET_LOG_TID);
    const lsn_t prev_lsn = read_unaligned<lsn_t>(bytes + OFFSET_PREV_LSN);
    if (!IsKnownLogType(type) || lsn == INVALID_LSN || total_len < static_cast<uint32_t>(LOG_HEADER_SIZE) ||
        total_len > kMaxRecordBytes) {
        return false;
    }
    switch (type) {
    case LogType::BEGIN:
        if (total_len != LOG_HEADER_SIZE || txn_id == INVALID_TXN_ID || prev_lsn != INVALID_LSN) return false;
        break;
    case LogType::ABORT:
        if (total_len != LOG_HEADER_SIZE || txn_id == INVALID_TXN_ID) return false;
        break;
    case LogType::COMMIT:
        if ((total_len != LOG_HEADER_SIZE && total_len != LOG_HEADER_SIZE + sizeof(timestamp_t)) ||
            txn_id == INVALID_TXN_ID)
            return false;
        break;
    case LogType::CHECKPOINT: {
        const uint32_t minimum = LOG_HEADER_SIZE + sizeof(size_t);
        if (total_len < minimum || txn_id != INVALID_TXN_ID || prev_lsn != INVALID_LSN)
            return false;
        const uint32_t entry_bytes = sizeof(txn_id_t) + sizeof(lsn_t);
        if ((total_len - minimum) % entry_bytes != 0) return false;
        break;
    }
    default:
        break;
    }
    *total_len_out = total_len;
    return true;
}

// Runs only after the header's legal total_len bytes are present. DML and
// index payloads remain owned by their existing semantic parsers/CRC checks.
bool HasValidCompleteWalRecord(const char* bytes, uint32_t total_len) {
    if (read_unaligned<LogType>(bytes + OFFSET_LOG_TYPE) != LogType::CHECKPOINT) return true;
    const size_t count = read_unaligned<size_t>(bytes + OFFSET_LOG_DATA);
    const size_t entry_bytes = sizeof(txn_id_t) + sizeof(lsn_t);
    const size_t payload_entries = total_len - LOG_HEADER_SIZE - sizeof(size_t);
    return count == payload_entries / entry_bytes;
}

// Reads one length-prefixed row image. Returns false if the prefix or the
// image would leave the record.
bool ReadImage(const char* bytes, uint32_t total_len, int* offset, const char** image, int* size) {
    if (!LogPayloadReadable(total_len, *offset, sizeof(int))) {
        return false;
    }
    const int image_size = read_unaligned<int>(bytes + *offset);
    *offset += static_cast<int>(sizeof(int));
    if (image_size < 0 || !LogPayloadReadable(total_len, *offset, static_cast<size_t>(image_size))) {
        return false;
    }
    *image = bytes + *offset;
    *size = image_size;
    *offset += image_size;
    return true;
}

// Reads the trailing `Rid` + length-prefixed table name shared by all three
// DML record layouts.
bool ReadRidAndTable(const char* bytes, uint32_t total_len, int* offset, WalDmlView* out) {
    if (!LogPayloadReadable(total_len, *offset, sizeof(Rid))) {
        return false;
    }
    out->rid = read_unaligned<Rid>(bytes + *offset);
    *offset += static_cast<int>(sizeof(Rid));
    if (!LogPayloadReadable(total_len, *offset, sizeof(size_t))) {
        return false;
    }
    const size_t name_size = read_unaligned<size_t>(bytes + *offset);
    *offset += static_cast<int>(sizeof(size_t));
    if (!LogPayloadReadable(total_len, *offset, name_size)) {
        return false;
    }
    out->table_name = std::string_view(bytes + *offset, name_size);
    *offset += static_cast<int>(name_size);
    return static_cast<uint32_t>(*offset) == total_len;
}

bool ReadSparseUpdate(const char* bytes, uint32_t total_len, int* offset, WalDmlView* out, bool materialize_before) {
    if (!LogPayloadReadable(total_len, *offset, sizeof(int)) ||
        read_unaligned<int>(bytes + *offset) != UpdateLogRecord::kSparseBeforeVersion) {
        return false;
    }
    *offset += sizeof(int);
    if (!ReadImage(bytes, total_len, offset, &out->after_image, &out->after_size) ||
        !LogPayloadReadable(total_len, *offset, sizeof(uint32_t))) {
        return false;
    }
    const uint32_t span_count = read_unaligned<uint32_t>(bytes + *offset);
    *offset += sizeof(uint32_t);
    // Every span consumes two uint32 fields and at least one byte. Bound the
    // loop before materializing so a corrupt count cannot amplify recovery CPU.
    const uint32_t remaining = total_len - static_cast<uint32_t>(*offset);
    if (span_count > static_cast<uint32_t>(out->after_size) || span_count > remaining / (sizeof(uint32_t) * 2 + 1)) {
        return false;
    }

    if (materialize_before) {
        out->materialized_before.assign(out->after_image, out->after_image + out->after_size);
    }
    uint32_t previous_offset = 0;
    uint32_t previous_end = 0;
    const uint32_t row_size = static_cast<uint32_t>(out->after_size);
    for (uint32_t i = 0; i < span_count; ++i) {
        if (!LogPayloadReadable(total_len, *offset, sizeof(uint32_t) * 2)) {
            return false;
        }
        const uint32_t span_offset = read_unaligned<uint32_t>(bytes + *offset);
        *offset += sizeof(uint32_t);
        const uint32_t span_length = read_unaligned<uint32_t>(bytes + *offset);
        *offset += sizeof(uint32_t);
        if (span_length == 0 || span_offset > row_size || span_length > row_size - span_offset ||
            (i > 0 && (span_offset <= previous_offset || span_offset < previous_end)) ||
            !LogPayloadReadable(total_len, *offset, span_length)) {
            return false;
        }
        if (materialize_before) {
            memcpy(out->materialized_before.data() + span_offset, bytes + *offset, span_length);
        }
        *offset += static_cast<int>(span_length);
        previous_offset = span_offset;
        previous_end = span_offset + span_length;
    }
    if (materialize_before) {
        out->before_size = out->after_size;
        out->before_image = out->materialized_before.data();
        out->before_is_materialized = true;
    }
    return true;
}

bool ReadBidirectionalUpdate(const char* bytes, uint32_t total_len, int* offset, WalDmlView* out) {
    if (!LogPayloadReadable(total_len, *offset, sizeof(int)) ||
        read_unaligned<int>(bytes + *offset) != UpdateLogRecord::kBidirectionalDeltaVersion) {
        return false;
    }
    *offset += sizeof(int);
    if (!LogPayloadReadable(total_len, *offset, sizeof(uint32_t) * 3)) {
        return false;
    }

    WalUpdateDeltaView delta;
    delta.row_size = read_unaligned<uint32_t>(bytes + *offset);
    *offset += sizeof(uint32_t);
    delta.flags = read_unaligned<uint32_t>(bytes + *offset);
    *offset += sizeof(uint32_t);
    delta.span_count = read_unaligned<uint32_t>(bytes + *offset);
    *offset += sizeof(uint32_t);
    const uint32_t remaining = total_len - static_cast<uint32_t>(*offset);
    if (delta.row_size == 0 || delta.row_size > static_cast<uint32_t>(INT_MAX) ||
        delta.flags != UpdateLogRecord::kIndexKeysUnchangedFlag || delta.span_count > delta.row_size ||
        delta.span_count > remaining / (sizeof(uint32_t) * 2 + 2)) {
        return false;
    }

    delta.span_bytes = bytes + *offset;
    const int spans_begin = *offset;
    uint32_t previous_offset = 0;
    uint32_t previous_end = 0;
    for (uint32_t i = 0; i < delta.span_count; ++i) {
        if (!LogPayloadReadable(total_len, *offset, sizeof(uint32_t) * 2)) {
            return false;
        }
        const uint32_t span_offset = read_unaligned<uint32_t>(bytes + *offset);
        *offset += sizeof(uint32_t);
        const uint32_t span_length = read_unaligned<uint32_t>(bytes + *offset);
        *offset += sizeof(uint32_t);
        if (span_length == 0 || span_offset > delta.row_size || span_length > delta.row_size - span_offset ||
            (i > 0 && (span_offset <= previous_offset || span_offset < previous_end)) ||
            !LogPayloadReadable(total_len, *offset, static_cast<size_t>(span_length) * 2)) {
            return false;
        }
        *offset += static_cast<int>(span_length) * 2;
        previous_offset = span_offset;
        previous_end = span_offset + span_length;
    }
    delta.span_bytes_length = static_cast<uint32_t>(*offset - spans_begin);
    out->update_delta = delta;
    return true;
}

bool ParseWalDmlImpl(const WalRecordView& record, WalDmlView* out, bool materialize_sparse_before) {
    if (record.bytes == nullptr) {
        return false;
    }
    WalDmlView parsed;
    int offset = OFFSET_LOG_DATA;
    switch (record.log_type) {
    case LogType::INSERT:
        if (!ReadImage(record.bytes, record.total_len, &offset, &parsed.after_image, &parsed.after_size)) {
            return false;
        }
        break;
    case LogType::DELETE:
        if (!ReadImage(record.bytes, record.total_len, &offset, &parsed.before_image, &parsed.before_size)) {
            return false;
        }
        break;
    case LogType::UPDATE:
        if (!LogPayloadReadable(record.total_len, offset, sizeof(int))) {
            return false;
        }
        if (read_unaligned<int>(record.bytes + offset) == UpdateLogRecord::kBidirectionalDeltaVersion) {
            if (!ReadBidirectionalUpdate(record.bytes, record.total_len, &offset, &parsed)) {
                return false;
            }
        } else if (read_unaligned<int>(record.bytes + offset) < 0) {
            if (!ReadSparseUpdate(record.bytes, record.total_len, &offset, &parsed, materialize_sparse_before)) {
                return false;
            }
        } else {
            if (!ReadImage(record.bytes, record.total_len, &offset, &parsed.before_image, &parsed.before_size) ||
                !ReadImage(record.bytes, record.total_len, &offset, &parsed.after_image, &parsed.after_size)) {
                return false;
            }
        }
        break;
    default:
        return false;
    }
    if (!ReadRidAndTable(record.bytes, record.total_len, &offset, &parsed)) {
        return false;
    }
    *out = std::move(parsed);
    if (out->before_is_materialized) {
        out->before_image = out->materialized_before.data();
    }
    return true;
}

} // namespace

bool ReadWalUpdateDeltaSpan(const WalUpdateDeltaView& delta, uint32_t* cursor, WalUpdateDeltaSpan* span) {
    if (cursor == nullptr || span == nullptr || !delta.present() || *cursor > delta.span_bytes_length ||
        delta.span_bytes_length - *cursor < sizeof(uint32_t) * 2) {
        return false;
    }
    const char* bytes = delta.span_bytes + *cursor;
    const uint32_t span_offset = read_unaligned<uint32_t>(bytes);
    const uint32_t span_length = read_unaligned<uint32_t>(bytes + sizeof(uint32_t));
    const uint32_t payload_offset = sizeof(uint32_t) * 2;
    if (span_length == 0 || span_offset > delta.row_size || span_length > delta.row_size - span_offset ||
        static_cast<uint64_t>(payload_offset) + static_cast<uint64_t>(span_length) * 2 >
            delta.span_bytes_length - *cursor) {
        return false;
    }
    span->offset = span_offset;
    span->length = span_length;
    span->before_bytes = bytes + payload_offset;
    span->after_bytes = span->before_bytes + span_length;
    *cursor += payload_offset + span_length * 2;
    return true;
}

bool ParseWalDml(const WalRecordView& record, WalDmlView* out) {
    return ParseWalDmlImpl(record, out, true);
}

bool ParseWalDmlForRedo(const WalRecordView& record, WalDmlView* out) {
    return ParseWalDmlImpl(record, out, false);
}

WalReader::WalReader(DiskManager* disk_manager, int64_t begin_offset, int64_t end_offset, int buffer_bytes)
    : disk_manager_(disk_manager), end_offset_(std::max<int64_t>(end_offset, 0)),
      buffer_(static_cast<size_t>(std::max(buffer_bytes, LOG_HEADER_SIZE))),
      buffer_offset_(std::min(std::max<int64_t>(begin_offset, 0), end_offset_)) {}

bool WalReader::refill() {
    const int carried = buffered();
    if (carried > 0 && cursor_ > 0) {
        std::memmove(buffer_.data(), buffer_.data() + cursor_, static_cast<size_t>(carried));
    }
    if (buffer_offset_ > end_offset_ || cursor_ < 0 || static_cast<int64_t>(cursor_) > end_offset_ - buffer_offset_) {
        exhausted_ = true;
        return false;
    }
    buffer_offset_ += static_cast<int64_t>(cursor_);
    buffer_size_ = carried;
    cursor_ = 0;

    if (exhausted_) {
        return false;
    }
    if (buffer_size_ < 0 || static_cast<int64_t>(buffer_size_) > end_offset_ - buffer_offset_) {
        exhausted_ = true;
        return false;
    }
    const int64_t read_from = buffer_offset_ + static_cast<int64_t>(buffer_size_);
    const int64_t remaining = end_offset_ - read_from;
    if (remaining <= 0) {
        exhausted_ = true;
        return false;
    }
    const int want = static_cast<int>(
        std::min<int64_t>(remaining, static_cast<int64_t>(buffer_.size()) - static_cast<int64_t>(buffer_size_)));
    if (want <= 0) {
        return false;
    }
    const int got = disk_manager_->read_log_chunk(buffer_.data() + buffer_size_, want, read_from);
    ++read_count_;
    if (got <= 0) {
        exhausted_ = true;
        return false;
    }
    buffer_size_ += got;
    if (got < want) {
        // The file ended earlier than the caller's bound.
        exhausted_ = true;
    }
    return true;
}

bool WalReader::next(WalRecordView* out) {
    if (buffered() < LOG_HEADER_SIZE && !refill()) {
        status_ = buffered() == 0 ? WalReadStatus::kCleanEnd : WalReadStatus::kTornTail;
        return false;
    }
    if (buffered() < LOG_HEADER_SIZE) {
        status_ = WalReadStatus::kTornTail;
        return false;
    }

    const char* header = buffer_.data() + cursor_;
    uint32_t total_len = 0;
    if (!HasValidWalHeader(header, &total_len)) {
        status_ = WalReadStatus::kCorruption;
        return false;
    }
    const int64_t record_offset = next_offset();
    if (record_offset < 0 || record_offset > end_offset_ ||
        static_cast<int64_t>(total_len) > end_offset_ - record_offset) {
        status_ = WalReadStatus::kTornTail;
        return false;
    }
    if (buffered() < static_cast<int>(total_len)) {
        // The record straddles a buffer boundary: carry the partial head to
        // the front and read the rest before parsing it.
        if (static_cast<size_t>(total_len) > buffer_.size()) {
            // resize keeps the bytes already buffered, so the cursor stays valid.
            buffer_.resize(total_len);
        }
        refill();
        if (buffered() < static_cast<int>(total_len)) {
            status_ = WalReadStatus::kTornTail;
            return false;
        }
        header = buffer_.data() + cursor_;
    }

    if (!HasValidCompleteWalRecord(header, total_len)) {
        status_ = WalReadStatus::kCorruption;
        return false;
    }

    out->log_type = read_unaligned<LogType>(header + OFFSET_LOG_TYPE);
    out->lsn = read_unaligned<lsn_t>(header + OFFSET_LSN);
    out->total_len = total_len;
    out->txn_id = read_unaligned<txn_id_t>(header + OFFSET_LOG_TID);
    out->prev_lsn = read_unaligned<lsn_t>(header + OFFSET_PREV_LSN);
    out->offset = record_offset;
    out->bytes = header;
    cursor_ += static_cast<int>(total_len);
    return true;
}

void WalFramer::finish_segment(int64_t end) {
    if (segment_records_ != 0) segments_.push_back(FramedSegment{segment_begin_, end, segment_records_});
    segment_begin_ = end;
    segment_records_ = 0;
}

bool WalFramer::next(WalRecordView* out) {
    if (!reader_.next(out)) {
        if (reader_.status() != WalReadStatus::kCorruption) finish_segment(reader_.next_offset());
        return false;
    }
    // WalReader has already advanced across one validated record, so its
    // frontier is the overflow-safe record end and the only boundary used by
    // this catalogue.
    const int64_t record_end = reader_.next_offset();
    if (static_cast<int64_t>(out->total_len) > kTargetBytes) {
        finish_segment(out->offset);
        segment_records_ = 1;
        finish_segment(record_end);
        return true;
    }
    ++segment_records_;
    const int64_t segment_bytes = record_end - segment_begin_;
    if (segment_records_ >= kTargetRecords || segment_bytes >= kTargetBytes) finish_segment(record_end);
    return true;
}

bool ReadWalRecordAt(DiskManager* disk_manager, int64_t offset, int64_t end_offset, std::vector<char>* scratch,
                     WalRecordView* out) {
    if (offset < 0 || end_offset < offset || end_offset - offset < LOG_HEADER_SIZE) {
        return false;
    }
    scratch->resize(LOG_HEADER_SIZE);
    if (disk_manager->read_log_chunk(scratch->data(), LOG_HEADER_SIZE, offset) != LOG_HEADER_SIZE) {
        return false;
    }
    uint32_t total_len = 0;
    if (!HasValidWalHeader(scratch->data(), &total_len) || static_cast<int64_t>(total_len) > end_offset - offset) {
        return false;
    }

    if (total_len > static_cast<uint32_t>(LOG_HEADER_SIZE)) {
        scratch->resize(total_len);
        if (disk_manager->read_log_chunk(scratch->data(), static_cast<int>(total_len), offset) !=
            static_cast<int>(total_len)) {
            return false;
        }
    }
    if (!HasValidCompleteWalRecord(scratch->data(), total_len)) return false;

    const char* header = scratch->data();
    out->log_type = read_unaligned<LogType>(header + OFFSET_LOG_TYPE);
    out->lsn = read_unaligned<lsn_t>(header + OFFSET_LSN);
    out->total_len = total_len;
    out->txn_id = read_unaligned<txn_id_t>(header + OFFSET_LOG_TID);
    out->prev_lsn = read_unaligned<lsn_t>(header + OFFSET_PREV_LSN);
    out->offset = offset;
    out->bytes = header;
    return true;
}
