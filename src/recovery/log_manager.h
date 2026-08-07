/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/wal_flush_metrics.h"
#include "log_defs.h"
#include "common/config.h"
#include "record/rm_defs.h"
#include "storage/index_smo_wal.h"

/* 日志记录对应操作的类型 */
enum LogType : int { UPDATE = 0, INSERT, DELETE, BEGIN, COMMIT, ABORT, CHECKPOINT, INDEX_BIND, INDEX_SMO };
static std::string LogTypeStr[] = {"UPDATE", "INSERT",     "DELETE",     "BEGIN",    "COMMIT",
                                   "ABORT",  "CHECKPOINT", "INDEX_BIND", "INDEX_SMO"};

static bool LogPayloadReadable(uint32_t total_len, int offset, size_t bytes) {
    return offset >= 0 && static_cast<uint32_t>(offset) <= total_len &&
           bytes <= total_len - static_cast<uint32_t>(offset);
}

static bool ReadRecordPayload(const char* src, uint32_t total_len, int* offset, RmRecord* record) {
    if (!LogPayloadReadable(total_len, *offset, sizeof(int))) {
        return false;
    }
    int record_size = read_unaligned<int>(src + *offset);
    *offset += sizeof(int);
    if (record_size < 0 || !LogPayloadReadable(total_len, *offset, static_cast<size_t>(record_size))) {
        return false;
    }
    if (record->allocated_) {
        delete[] record->data;
    }
    record->size = record_size;
    record->data = new char[record_size];
    memcpy(record->data, src + *offset, record_size);
    record->allocated_ = true;
    *offset += record_size;
    return true;
}

class LogRecord {
public:
    LogType log_type_;     /* 日志对应操作的类型 */
    lsn_t lsn_;            /* 当前日志的lsn */
    uint32_t log_tot_len_; /* 整个日志记录的长度 */
    txn_id_t log_tid_;     /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;       /* 事务创建的前一条日志记录的lsn，用于undo */

    virtual ~LogRecord() = default;

    // 把日志记录序列化到dest中
    virtual void serialize(char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        log_type_ = read_unaligned<LogType>(src);
        lsn_ = read_unaligned<lsn_t>(src + OFFSET_LSN);
        log_tot_len_ = read_unaligned<uint32_t>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = read_unaligned<txn_id_t>(src + OFFSET_LOG_TID);
        prev_lsn_ = read_unaligned<lsn_t>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
        std::cout << "Print Log Record:\n";
        std::cout << "log_type_: " << LogTypeStr[log_type_] << "\n";
        std::cout << "lsn: " << lsn_ << "\n";
        std::cout << "log_tot_len: " << log_tot_len_ << "\n";
        std::cout << "log_tid: " << log_tid_ << "\n";
        std::cout << "prev_lsn: " << prev_lsn_ << "\n";
    }
};

class BeginLogRecord : public LogRecord {
public:
    BeginLogRecord() {
        log_type_ = LogType::BEGIN;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
        log_tid_ = txn_id;
    }
    // 序列化Begin日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Begin日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    virtual void format_print() override {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
};

/**
 * commit操作的日志记录
 *
 * 载荷只有 8 字节：本事务的 MVCC 提交时间戳。它是恢复期重建时间戳计数器的两个
 * 来源之一（另一个是 checkpoint 写进 db.restart 的计数器快照），详见
 * RecoveryManager::get_recovered_next_timestamp() 上的论证。
 *
 * 向后兼容：旧 WAL 里的 COMMIT 记录只有日志头（log_tot_len_ == LOG_HEADER_SIZE）。
 * deserialize 用长度判断载荷是否存在，缺失时留 INVALID_TS，analyze 会跳过它。
 */
class CommitLogRecord : public LogRecord {
public:
    timestamp_t commit_ts_{INVALID_TS};

    CommitLogRecord() {
        log_type_ = LogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE + sizeof(timestamp_t);
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() {
        log_tid_ = txn_id;
    }
    CommitLogRecord(txn_id_t txn_id, timestamp_t commit_ts) : CommitLogRecord(txn_id) {
        commit_ts_ = commit_ts;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        memcpy(dest + OFFSET_LOG_DATA, &commit_ts_, sizeof(timestamp_t));
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        commit_ts_ = HasCommitTs(log_tot_len_) ? read_unaligned<timestamp_t>(src + OFFSET_LOG_DATA) : INVALID_TS;
    }

    /** 该长度的 COMMIT 记录是否带提交时间戳载荷。恢复期直接按字节解析，不构造对象。 */
    static bool HasCommitTs(uint32_t total_len) {
        return total_len >= static_cast<uint32_t>(LOG_HEADER_SIZE) + sizeof(timestamp_t);
    }
};

/**
 * abort操作的日志记录
 */
class AbortLogRecord : public LogRecord {
public:
    AbortLogRecord() {
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit AbortLogRecord(txn_id_t txn_id) : AbortLogRecord() {
        log_tid_ = txn_id;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
};

class InsertLogRecord : public LogRecord {
public:
    InsertLogRecord() {
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    InsertLogRecord(txn_id_t txn_id, RmRecord& insert_value, Rid& rid, std::string table_name) : InsertLogRecord() {
        log_tid_ = txn_id;
        insert_value_ = insert_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += insert_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_ = std::move(table_name);
        table_name_size_ = table_name_.size();
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }

    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &insert_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, insert_value_.data, insert_value_.size);
        offset += insert_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size_);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        if (!ReadRecordPayload(src, log_tot_len_, &offset, &insert_value_) ||
            !LogPayloadReadable(log_tot_len_, offset, sizeof(Rid))) {
            return;
        }
        rid_ = read_unaligned<Rid>(src + offset);
        offset += sizeof(Rid);
        if (!LogPayloadReadable(log_tot_len_, offset, sizeof(size_t))) {
            return;
        }
        table_name_size_ = read_unaligned<size_t>(src + offset);
        offset += sizeof(size_t);
        if (!LogPayloadReadable(log_tot_len_, offset, table_name_size_)) {
            table_name_size_ = 0;
            return;
        }
        table_name_.assign(src + offset, table_name_size_);
    }
    void format_print() override {
        std::cout << "insert record\n";
        LogRecord::format_print();
        std::cout << "insert_value: " << insert_value_.data << "\n";
        std::cout << "insert rid: " << rid_.page_no << ", " << rid_.slot_no << "\n";
        std::cout << "table name: " << table_name_ << '\n';
    }

    RmRecord insert_value_;     // 插入的记录
    Rid rid_;                   // 记录插入的位置
    std::string table_name_;    // 插入记录的表名称
    size_t table_name_size_{0}; // 表名称的大小
};

/**
 * delete操作的日志记录
 */
class DeleteLogRecord : public LogRecord {
public:
    DeleteLogRecord() {
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    DeleteLogRecord(txn_id_t txn_id, RmRecord& delete_value, Rid& rid, std::string table_name) : DeleteLogRecord() {
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        rid_ = rid;
        table_name_ = std::move(table_name);
        table_name_size_ = table_name_.size();
        log_tot_len_ += sizeof(int) + delete_value_.size + sizeof(Rid) + sizeof(size_t) + table_name_size_;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &delete_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, delete_value_.data, delete_value_.size);
        offset += delete_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size_);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        if (!ReadRecordPayload(src, log_tot_len_, &offset, &delete_value_) ||
            !LogPayloadReadable(log_tot_len_, offset, sizeof(Rid))) {
            return;
        }
        rid_ = read_unaligned<Rid>(src + offset);
        offset += sizeof(Rid);
        if (!LogPayloadReadable(log_tot_len_, offset, sizeof(size_t))) {
            return;
        }
        table_name_size_ = read_unaligned<size_t>(src + offset);
        offset += sizeof(size_t);
        if (!LogPayloadReadable(log_tot_len_, offset, table_name_size_)) {
            table_name_size_ = 0;
            return;
        }
        table_name_.assign(src + offset, table_name_size_);
    }

    RmRecord delete_value_;
    Rid rid_;
    std::string table_name_;
    size_t table_name_size_{0};
};

/**
 * update操作的日志记录
 */
class UpdateLogRecord : public LogRecord {
public:
    // Legacy UPDATE starts with a non-negative before-image size. A negative
    // value is therefore an unambiguous in-band version marker while keeping
    // LogType::UPDATE and mixed old/new WAL streams compatible.
    static constexpr int kSparseBeforeVersion = -1;
    static constexpr int kBidirectionalDeltaVersion = -2;
    static constexpr uint32_t kIndexKeysUnchangedFlag = 1U;

    struct BeforeSpan {
        uint32_t offset{0};
        uint32_t length{0};
    };

    UpdateLogRecord() {
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    UpdateLogRecord(txn_id_t txn_id, RmRecord& old_value, RmRecord& new_value, Rid& rid, std::string table_name,
                    bool index_keys_unchanged = false)
        : UpdateLogRecord() {
        if (old_value.size < 0 || new_value.size < 0) {
            throw std::length_error("UPDATE WAL row image has a negative size");
        }
        log_tid_ = txn_id;
        old_value_ = old_value;
        new_value_ = new_value;
        rid_ = rid;
        table_name_ = std::move(table_name);
        table_name_size_ = table_name_.size();
        BuildBeforeSpans();
        const uint64_t legacy_image_bytes =
            sizeof(int) + static_cast<uint64_t>(old_value_.size) + sizeof(int) + static_cast<uint64_t>(new_value_.size);
        uint64_t sparse_image_bytes =
            sizeof(int) + sizeof(int) + static_cast<uint64_t>(new_value_.size) + sizeof(uint32_t);
        uint64_t bidirectional_delta_bytes = sizeof(int) + sizeof(uint32_t) * 3;
        for (const BeforeSpan& span : before_spans_) {
            sparse_image_bytes += sizeof(uint32_t) + sizeof(uint32_t) + span.length;
            bidirectional_delta_bytes += sizeof(uint32_t) + sizeof(uint32_t) + static_cast<uint64_t>(span.length) * 2;
        }
        const bool same_sized_rows = old_value_.size == new_value_.size && old_value_.size > 0;
        bidirectional_delta_encoding_ = index_keys_unchanged && same_sized_rows &&
                                        bidirectional_delta_bytes < std::min(legacy_image_bytes, sparse_image_bytes);
        sparse_encoding_ = !bidirectional_delta_encoding_ && same_sized_rows && sparse_image_bytes < legacy_image_bytes;
        const uint64_t image_bytes = bidirectional_delta_encoding_
                                         ? bidirectional_delta_bytes
                                         : (sparse_encoding_ ? sparse_image_bytes : legacy_image_bytes);
        uint64_t total_bytes = static_cast<uint64_t>(LOG_HEADER_SIZE) + image_bytes + sizeof(Rid) + sizeof(size_t);
        if (table_name_size_ > UINT64_MAX - total_bytes) {
            throw std::length_error("UPDATE WAL record length overflows uint64_t");
        }
        total_bytes += static_cast<uint64_t>(table_name_size_);
        if (total_bytes > MAX_INDEX_SMO_RECORD_BYTES || total_bytes > UINT32_MAX) {
            throw std::length_error("UPDATE WAL record exceeds the bounded WAL record size");
        }
        log_tot_len_ = static_cast<uint32_t>(total_bytes);
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        if (bidirectional_delta_encoding_) {
            memcpy(dest + offset, &kBidirectionalDeltaVersion, sizeof(int));
            offset += sizeof(int);
            const uint32_t row_size = static_cast<uint32_t>(new_value_.size);
            memcpy(dest + offset, &row_size, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            memcpy(dest + offset, &kIndexKeysUnchangedFlag, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            const uint32_t span_count = static_cast<uint32_t>(before_spans_.size());
            memcpy(dest + offset, &span_count, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            for (const BeforeSpan& span : before_spans_) {
                memcpy(dest + offset, &span.offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(dest + offset, &span.length, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(dest + offset, old_value_.data + span.offset, span.length);
                offset += static_cast<int>(span.length);
                memcpy(dest + offset, new_value_.data + span.offset, span.length);
                offset += static_cast<int>(span.length);
            }
        } else if (sparse_encoding_) {
            memcpy(dest + offset, &kSparseBeforeVersion, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, &new_value_.size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, new_value_.data, new_value_.size);
            offset += new_value_.size;
            const uint32_t span_count = static_cast<uint32_t>(before_spans_.size());
            memcpy(dest + offset, &span_count, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            for (const BeforeSpan& span : before_spans_) {
                memcpy(dest + offset, &span.offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(dest + offset, &span.length, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(dest + offset, old_value_.data + span.offset, span.length);
                offset += static_cast<int>(span.length);
            }
        } else {
            memcpy(dest + offset, &old_value_.size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, old_value_.data, old_value_.size);
            offset += old_value_.size;
            memcpy(dest + offset, &new_value_.size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, new_value_.data, new_value_.size);
            offset += new_value_.size;
        }
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size_);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        if (!LogPayloadReadable(log_tot_len_, offset, sizeof(int))) {
            return;
        }
        const int version_or_before_size = read_unaligned<int>(src + offset);
        if (version_or_before_size >= 0) {
            if (!ReadRecordPayload(src, log_tot_len_, &offset, &old_value_) ||
                !ReadRecordPayload(src, log_tot_len_, &offset, &new_value_)) {
                return;
            }
        } else if (version_or_before_size == kSparseBeforeVersion) {
            offset += sizeof(int);
            if (!ReadRecordPayload(src, log_tot_len_, &offset, &new_value_) ||
                !LogPayloadReadable(log_tot_len_, offset, sizeof(uint32_t))) {
                return;
            }
            const uint32_t span_count = read_unaligned<uint32_t>(src + offset);
            offset += sizeof(uint32_t);
            const uint32_t remaining = log_tot_len_ - static_cast<uint32_t>(offset);
            if (span_count > static_cast<uint32_t>(new_value_.size) ||
                span_count > remaining / (sizeof(uint32_t) * 2 + 1)) {
                return;
            }
            old_value_ = new_value_;
            uint32_t previous_offset = 0;
            uint32_t previous_end = 0;
            for (uint32_t i = 0; i < span_count; ++i) {
                if (!LogPayloadReadable(log_tot_len_, offset, sizeof(uint32_t) * 2)) {
                    return;
                }
                const uint32_t span_offset = read_unaligned<uint32_t>(src + offset);
                offset += sizeof(uint32_t);
                const uint32_t span_length = read_unaligned<uint32_t>(src + offset);
                offset += sizeof(uint32_t);
                const uint32_t row_size = static_cast<uint32_t>(new_value_.size);
                if (span_length == 0 || span_offset > row_size || span_length > row_size - span_offset ||
                    (i > 0 && (span_offset <= previous_offset || span_offset < previous_end)) ||
                    !LogPayloadReadable(log_tot_len_, offset, span_length)) {
                    return;
                }
                memcpy(old_value_.data + span_offset, src + offset, span_length);
                offset += static_cast<int>(span_length);
                previous_offset = span_offset;
                previous_end = span_offset + span_length;
            }
            sparse_encoding_ = true;
        } else if (version_or_before_size == kBidirectionalDeltaVersion) {
            offset += sizeof(int);
            if (!LogPayloadReadable(log_tot_len_, offset, sizeof(uint32_t) * 3)) {
                return;
            }
            const uint32_t row_size = read_unaligned<uint32_t>(src + offset);
            offset += sizeof(uint32_t);
            const uint32_t flags = read_unaligned<uint32_t>(src + offset);
            offset += sizeof(uint32_t);
            const uint32_t span_count = read_unaligned<uint32_t>(src + offset);
            offset += sizeof(uint32_t);
            const uint32_t remaining = log_tot_len_ - static_cast<uint32_t>(offset);
            if (row_size == 0 || row_size > static_cast<uint32_t>(std::numeric_limits<int>::max()) ||
                flags != kIndexKeysUnchangedFlag || span_count > row_size ||
                span_count > remaining / (sizeof(uint32_t) * 2 + 2)) {
                return;
            }
            uint32_t previous_offset = 0;
            uint32_t previous_end = 0;
            before_spans_.clear();
            before_spans_.reserve(span_count);
            for (uint32_t i = 0; i < span_count; ++i) {
                if (!LogPayloadReadable(log_tot_len_, offset, sizeof(uint32_t) * 2)) {
                    return;
                }
                const uint32_t span_offset = read_unaligned<uint32_t>(src + offset);
                offset += sizeof(uint32_t);
                const uint32_t span_length = read_unaligned<uint32_t>(src + offset);
                offset += sizeof(uint32_t);
                if (span_length == 0 || span_offset > row_size || span_length > row_size - span_offset ||
                    (i > 0 && (span_offset <= previous_offset || span_offset < previous_end)) ||
                    !LogPayloadReadable(log_tot_len_, offset, static_cast<size_t>(span_length) * 2)) {
                    before_spans_.clear();
                    return;
                }
                offset += static_cast<int>(span_length) * 2;
                before_spans_.push_back(BeforeSpan{span_offset, span_length});
                previous_offset = span_offset;
                previous_end = span_offset + span_length;
            }
            bidirectional_delta_encoding_ = true;
        } else {
            return;
        }
        if (!LogPayloadReadable(log_tot_len_, offset, sizeof(Rid))) {
            return;
        }
        rid_ = read_unaligned<Rid>(src + offset);
        offset += sizeof(Rid);
        if (!LogPayloadReadable(log_tot_len_, offset, sizeof(size_t))) {
            return;
        }
        table_name_size_ = read_unaligned<size_t>(src + offset);
        offset += sizeof(size_t);
        if (!LogPayloadReadable(log_tot_len_, offset, table_name_size_)) {
            table_name_size_ = 0;
            return;
        }
        table_name_.assign(src + offset, table_name_size_);
        offset += static_cast<int>(table_name_size_);
        if (static_cast<uint32_t>(offset) != log_tot_len_) {
            table_name_.clear();
            table_name_size_ = 0;
        }
    }

    RmRecord old_value_;
    RmRecord new_value_;
    Rid rid_;
    std::string table_name_;
    size_t table_name_size_{0};
    bool sparse_encoding_{false};
    bool bidirectional_delta_encoding_{false};
    std::vector<BeforeSpan> before_spans_;

private:
    void BuildBeforeSpans() {
        before_spans_.clear();
        if (old_value_.size != new_value_.size || old_value_.size <= 0) {
            return;
        }
        const uint32_t row_size = static_cast<uint32_t>(old_value_.size);
        uint32_t cursor = 0;
        while (cursor < row_size) {
            while (cursor < row_size && old_value_.data[cursor] == new_value_.data[cursor]) {
                ++cursor;
            }
            if (cursor == row_size) {
                break;
            }
            const uint32_t begin = cursor;
            while (cursor < row_size && old_value_.data[cursor] != new_value_.data[cursor]) {
                ++cursor;
            }
            if (!before_spans_.empty()) {
                BeforeSpan& previous = before_spans_.back();
                const uint32_t previous_end = previous.offset + previous.length;
                if (begin - previous_end <= 2) {
                    // Paying one or two unchanged bytes is cheaper than another
                    // offset/length pair, and the before bytes still reconstruct
                    // exactly when the whole merged span is applied.
                    previous.length = cursor - previous.offset;
                    continue;
                }
            }
            before_spans_.push_back(BeforeSpan{begin, cursor - begin});
        }
    }
};

class CheckpointLogRecord : public LogRecord {
public:
    CheckpointLogRecord() {
        log_type_ = LogType::CHECKPOINT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE + sizeof(size_t);
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit CheckpointLogRecord(std::unordered_map<txn_id_t, lsn_t> active_txns) : CheckpointLogRecord() {
        active_txns_ = std::move(active_txns);
        log_tot_len_ += active_txns_.size() * (sizeof(txn_id_t) + sizeof(lsn_t));
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        size_t active_count = active_txns_.size();
        memcpy(dest + offset, &active_count, sizeof(size_t));
        offset += sizeof(size_t);
        for (const auto& [txn_id, last_lsn] : active_txns_) {
            memcpy(dest + offset, &txn_id, sizeof(txn_id_t));
            offset += sizeof(txn_id_t);
            memcpy(dest + offset, &last_lsn, sizeof(lsn_t));
            offset += sizeof(lsn_t);
        }
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        if (!LogPayloadReadable(log_tot_len_, offset, sizeof(size_t))) {
            return;
        }
        size_t active_count = read_unaligned<size_t>(src + offset);
        offset += sizeof(size_t);
        if (active_count > (log_tot_len_ - offset) / (sizeof(txn_id_t) + sizeof(lsn_t))) {
            return;
        }
        active_txns_.clear();
        for (size_t i = 0; i < active_count; ++i) {
            txn_id_t txn_id = read_unaligned<txn_id_t>(src + offset);
            offset += sizeof(txn_id_t);
            lsn_t last_lsn = read_unaligned<lsn_t>(src + offset);
            offset += sizeof(lsn_t);
            active_txns_.emplace(txn_id, last_lsn);
        }
    }

    std::unordered_map<txn_id_t, lsn_t> active_txns_;
};

std::unique_ptr<LogRecord> DeserializeLogRecord(const char* src, int size);

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer {
public:
    LogBuffer() : buffer_(LOG_BUFFER_SIZE + 1), offset_(0) {}

    bool is_full(int append_size) {
        if (offset_ + append_size > static_cast<int>(buffer_.size()))
            return true;
        return false;
    }

    void ensure_capacity(size_t bytes) {
        if (buffer_.size() < bytes) {
            buffer_.resize(bytes);
        }
    }

    void shrink_after_flush() {
        if (buffer_.size() > LOG_BUFFER_SIZE + 1) {
            buffer_.resize(LOG_BUFFER_SIZE + 1);
        }
    }

    std::vector<char> buffer_;
    int offset_; // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
enum class DurabilityMode { PROCESS_CRASH, STRICT };

/**
 * db.restart：重启清单。它承担 PostgreSQL 里 pg_control 的角色——记录“下一次恢复
 * 需要知道、但无法从数据页便宜地重算”的少量标量。
 *
 * 磁盘格式（文本，空白分隔）：第一行仍是裸的 checkpoint 偏移，后面跟若干
 * `key=value` 行。这样两个方向都兼容：旧读者 `>> offset` 照旧工作并忽略其余内容；
 * 新读者遇到没有 key 的旧文件时，把缺失字段留在“安全默认值”上。
 */
struct RestartManifest {
    // 恢复扫描的起点（今天恒为 0：clean checkpoint 会把 WAL 截为空）。
    int64_t checkpoint_offset{0};
    // checkpoint 那一刻 TransactionManager::next_timestamp_ 的快照。
    // 0 表示“文件里没有这个字段”，与“计数器确实是 0”同义，因此可以安全合并：
    // 两种情况下有效下界都由保留 WAL 里 COMMIT 记录的最大 commit_ts 补齐。
    timestamp_t next_timestamp{0};
    // 同上，但针对 next_txn_id_。
    txn_id_t next_txn_id{0};
    lsn_t next_lsn{0};
    // Version 2 makes the physical WAL layout explicit. A v2 marker is never
    // allowed to fall back to db.log: malformed layout metadata is a recovery
    // failure, not an invitation to scan a different byte stream.
    bool segmented_wal{false};
    bool malformed{false};
    uint64_t wal_generation{0};
    uint64_t wal_segment_bytes{0};
    uint64_t restart_segment{0};
    uint64_t restart_offset{0};
};

/**
 * A durable, self-contained WAL boundary for a fuzzy checkpoint.
 *
 * `checkpoint_offset` is the exact byte offset of the empty CHECKPOINT record.
 * Every binding in `index_bindings` is serialized contiguously immediately
 * after that record, in caller order. `last_lsn` covers the complete batch and
 * is appended before create_checkpoint_wal_cut() returns. Callers must make
 * last_lsn durable before publishing checkpointed data or a restart manifest.
 */
struct CheckpointWalCut {
    int64_t checkpoint_offset{0};
    lsn_t checkpoint_lsn{INVALID_LSN};
    lsn_t last_lsn{INVALID_LSN};
    std::vector<std::pair<std::string, uint64_t>> index_bindings;
};

class LogManager {
public:
    static constexpr const char* RESTART_FILE_NAME = "db.restart";
    // Test-only bounded scheduling point for group-commit ownership tests.
    // Production construction leaves it empty.
    struct GroupCommitTestOptions {
        std::function<void(std::string_view)> hook;
    };

    explicit LogManager(DiskManager* disk_manager, DurabilityMode durability_mode = DurabilityMode::STRICT,
                        WalFlushMetrics* wal_flush_metrics = nullptr, GroupCommitTestOptions test_options = {})
        : log_buffer_(std::make_unique<LogBuffer>()), flushing_buffer_(std::make_unique<LogBuffer>()) {
        disk_manager_ = disk_manager;
        durability_mode_ = durability_mode;
        wal_flush_metrics_ = wal_flush_metrics;
        group_commit_test_hook_ = std::move(test_options.hook);
        const char* leader_rotation = std::getenv("RMDB_WAL_LEADER_ROTATION");
        leader_rotation_enabled_ = leader_rotation == nullptr || WalFlushMetrics::ParseEnabled(leader_rotation);
        persist_lsn_.store(INVALID_LSN);
        durable_lsn_ = INVALID_LSN;
    }

    lsn_t add_log_to_buffer(LogRecord* log_record);
    lsn_t append_index_smo(const IndexSmoWalData& data);
    uint64_t ensure_index_binding(const std::string& index_file_name);
    uint64_t renew_index_binding(const std::string& index_file_name);
    CheckpointWalCut create_checkpoint_wal_cut(const std::vector<std::string>& index_file_names);
    void sync_checkpoint_wal_cut(const CheckpointWalCut& cut);
    void flush_log_to_disk();
    void flush_log_to_disk_with_sync();
    void flush_log_to_disk_up_to(lsn_t target_lsn);
    // Physical page/header publication always needs a stable WAL prefix,
    // independently of the transaction commit durability mode.
    void flush_log_to_disk_up_to_durable(lsn_t target_lsn);
    // Production startup is deliberately split around RecoveryManager::analyze().
    // prepare only selects the WAL layout and a safe checkpoint boundary; it
    // must not scan or truncate the retained WAL.  finalize is called only
    // after analyze has accepted every complete record.
    void prepare_existing_log();
    void finalize_existing_log(int64_t accepted_end_offset, lsn_t max_lsn,
                               const std::vector<std::pair<std::string, uint64_t>>& index_bindings);
    int64_t prepared_restart_offset() const;
    bool startup_is_prepared() const;
    void initialize_from_existing_log();

    // recovery/checkpoint 成功落盘表页与元数据后调用：先把缓冲区残留日志刷盘，
    // 再把日志文件截断为空，并把 global_lsn/persist_lsn/追加偏移重置为 next_lsn 起点。
    // 这样下一次重启只从干净日志开始扫描，已 undo 完毕的旧 loser 日志不再残留，
    // 避免跨轮 recovery 在同 RID 上重复 undo 覆盖后续 committed 数据。
    void reset_log(lsn_t next_lsn);

    lsn_t get_persist_lsn() const {
        return persist_lsn_.load(std::memory_order_acquire);
    }

    lsn_t get_durable_lsn() const {
        return durable_lsn_.load(std::memory_order_acquire);
    }
    struct FdatasyncObservation {
        uint64_t sequence{0}, elapsed_ns{0};
    };
    FdatasyncObservation recent_fdatasync_observation() const noexcept {
        const uint64_t sequence = fdatasync_observation_sequence_.load(std::memory_order_acquire);
        return {sequence, recent_fdatasync_ns_.load(std::memory_order_relaxed)};
    }
    uint64_t recent_fdatasync_ns() const noexcept {
        return recent_fdatasync_observation().elapsed_ns;
    }
    void set_fdatasync_observation_for_test(uint64_t sequence, uint64_t elapsed_ns) noexcept {
        recent_fdatasync_ns_.store(elapsed_ns, std::memory_order_relaxed);
        fdatasync_observation_sequence_.store(sequence, std::memory_order_release);
    }

    lsn_t get_global_lsn() const {
        return global_lsn_.load();
    }

    // Return the logical WAL footprint, including records still in the active
    // buffer and a buffer currently being written. Checkpoint scheduling must
    // not wait for those bytes to reach the file before starting preflush.
    int64_t current_log_offset() const {
        std::lock_guard<std::mutex> lock(latch_);
        return log_file_offset_ + static_cast<int64_t>(flushing_bytes_) + static_cast<int64_t>(log_buffer_->offset_);
    }

    DurabilityMode durability_mode() const {
        return durability_mode_;
    }
    bool leader_rotation_enabled_for_test() const noexcept {
        return leader_rotation_enabled_;
    }

    // 原子发布重启清单：tmp 文件 + fdatasync + rename + 目录 fsync。
    // checkpoint 必须在截断 WAL *之前* 调用它，见 CheckpointManager::RunCleanCheckpoint()。
    void write_restart_manifest(const RestartManifest& manifest);
    RestartManifest read_restart_manifest() const;

    // 只关心扫描起点的调用方（以及旧测试）的薄封装。写入时计数器字段留默认值 0，
    // 语义等同于“本次发布不提供计数器”。
    void write_restart_offset(int64_t checkpoint_offset);
    int64_t read_restart_offset() const;

    LogBuffer* get_log_buffer() {
        return log_buffer_.get();
    }

private:
    struct CommitWaiter {
        enum class State : uint8_t { Waiting, Promoted, Done };
        lsn_t target_lsn{INVALID_LSN};
        bool require_sync{true};
        // Kept for the default-off path so its waiter protocol stays byte-for-
        // byte equivalent to the established implementation.
        bool done{false};
        State state{State::Waiting};
        std::exception_ptr error;
        std::condition_variable cv;
    };

    void flush_buffer(bool sync);
    void flush_log_to_disk_up_to_impl(lsn_t target_lsn, bool require_sync);
    void flush_log_to_disk_up_to_legacy(lsn_t target_lsn, bool require_sync);
    void flush_log_to_disk_up_to_with_leader_rotation(lsn_t target_lsn, bool require_sync);
    void run_group_commit_test_hook(std::string_view point) const;
    uint64_t publish_index_binding_locked(const std::string& index_file_name, uint64_t epoch, bool durable = true);

    // One leader performs the durable flush for all waiters that arrive
    // before it finishes. Waiters are released only after durable_lsn_ moves
    // past their target LSN.
    std::mutex group_commit_latch_;
    std::condition_variable group_commit_cv_;
    bool group_commit_leader_active_{false};
    std::deque<std::shared_ptr<CommitWaiter>> group_commit_waiters_;

    std::atomic<lsn_t> global_lsn_{0}; // 全局lsn，递增，用于为每条记录分发lsn
    mutable std::mutex latch_;         // protects active/flushing buffers and WAL metadata
    std::condition_variable buffer_cv_;
    std::unique_ptr<LogBuffer> log_buffer_;      // active append buffer
    std::unique_ptr<LogBuffer> flushing_buffer_; // stable buffer written without latch_
    bool flushing_in_progress_{false};
    lsn_t flushing_lsn_{INVALID_LSN};
    int flushing_bytes_{0};
    std::atomic<lsn_t> persist_lsn_{INVALID_LSN}; // 最后一个已 pwrite 到 OS page cache 的日志号
    std::atomic<lsn_t> durable_lsn_{INVALID_LSN}; // 最后一个已通过 fdatasync 的日志号
    // A best-effort congestion signal for optional background I/O only. It is
    // never consulted by commit durability or WAL ordering.
    std::atomic<uint64_t> recent_fdatasync_ns_{0};
    std::atomic<uint64_t> fdatasync_observation_sequence_{0};
    int64_t log_file_offset_{0}; // 日志文件当前追加偏移
    DiskManager* disk_manager_;
    DurabilityMode durability_mode_{DurabilityMode::STRICT};
    WalFlushMetrics* wal_flush_metrics_{nullptr};
    // Production defaults to rotation. An exact "0" (or any other non-"1"
    // value) opts into the established legacy group-commit implementation.
    // Cache this once so runtime flushing never reads process configuration.
    bool leader_rotation_enabled_{true};
    std::function<void(std::string_view)> group_commit_test_hook_;
    struct IndexBinding {
        uint64_t generation{0};
        uint64_t epoch{0};
    };
    std::mutex index_binding_latch_;
    std::unordered_map<std::string, IndexBinding> index_bindings_;
    std::atomic<uint64_t> wal_epoch_{1};
    RestartManifest prepared_manifest_;
    int64_t prepared_file_size_{0};
    int64_t prepared_restart_offset_{0};
    bool prepared_restart_rejected_{false};
    bool startup_prepared_{false};
};
