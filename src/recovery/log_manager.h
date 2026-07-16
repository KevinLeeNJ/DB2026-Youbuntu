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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "log_defs.h"
#include "common/config.h"
#include "record/rm_defs.h"

/* 日志记录对应操作的类型 */
enum LogType : int { UPDATE = 0, INSERT, DELETE, BEGIN, COMMIT, ABORT, CHECKPOINT };
static std::string LogTypeStr[] = {"UPDATE", "INSERT", "DELETE", "BEGIN", "COMMIT", "ABORT", "CHECKPOINT"};

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
 */
class CommitLogRecord : public LogRecord {
public:
    CommitLogRecord() {
        log_type_ = LogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    explicit CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() {
        log_tid_ = txn_id;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
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
    UpdateLogRecord() {
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    UpdateLogRecord(txn_id_t txn_id, RmRecord& old_value, RmRecord& new_value, Rid& rid, std::string table_name)
        : UpdateLogRecord() {
        log_tid_ = txn_id;
        old_value_ = old_value;
        new_value_ = new_value;
        rid_ = rid;
        table_name_ = std::move(table_name);
        table_name_size_ = table_name_.size();
        log_tot_len_ += sizeof(int) + old_value_.size;
        log_tot_len_ += sizeof(int) + new_value_.size;
        log_tot_len_ += sizeof(Rid) + sizeof(size_t) + table_name_size_;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &old_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, old_value_.data, old_value_.size);
        offset += old_value_.size;
        memcpy(dest + offset, &new_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, new_value_.data, new_value_.size);
        offset += new_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_.data(), table_name_size_);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        if (!ReadRecordPayload(src, log_tot_len_, &offset, &old_value_) ||
            !ReadRecordPayload(src, log_tot_len_, &offset, &new_value_) ||
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

    RmRecord old_value_;
    RmRecord new_value_;
    Rid rid_;
    std::string table_name_;
    size_t table_name_size_{0};
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
    LogBuffer() : offset_(0) {}

    bool is_full(int append_size) {
        if (offset_ + append_size > LOG_BUFFER_SIZE)
            return true;
        return false;
    }

    char buffer_[LOG_BUFFER_SIZE + 1];
    int offset_; // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
enum class DurabilityMode { PROCESS_CRASH, STRICT };

class LogManager {
public:
    static constexpr const char* RESTART_FILE_NAME = "db.restart";

    explicit LogManager(DiskManager* disk_manager, DurabilityMode durability_mode = DurabilityMode::STRICT)
        : log_buffer_(std::make_unique<LogBuffer>()), flushing_buffer_(std::make_unique<LogBuffer>()) {
        disk_manager_ = disk_manager;
        durability_mode_ = durability_mode;
        persist_lsn_.store(INVALID_LSN);
        durable_lsn_ = INVALID_LSN;
    }

    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk();
    void flush_log_to_disk_with_sync();
    void flush_log_to_disk_up_to(lsn_t target_lsn);
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

    lsn_t get_global_lsn() const {
        return global_lsn_.load();
    }

    // Return the logical WAL footprint, including records still in the active
    // buffer and a buffer currently being written. Checkpoint scheduling must
    // not wait for those bytes to reach the file before starting preflush.
    int64_t current_log_offset() const {
        std::lock_guard<std::mutex> lock(latch_);
        return log_file_offset_ + static_cast<int64_t>(flushing_bytes_) +
               static_cast<int64_t>(log_buffer_->offset_);
    }

    uint64_t get_fsync_count() const {
        return fsync_count_.load(std::memory_order_acquire);
    }

    uint64_t get_group_commit_count() const {
        return group_commit_count_.load(std::memory_order_acquire);
    }

    uint64_t get_group_commit_waiter_count() const {
        return group_commit_waiter_count_.load(std::memory_order_acquire);
    }

    uint64_t get_group_commit_wait_ns() const {
        return group_commit_wait_ns_.load(std::memory_order_acquire);
    }

    uint64_t get_pwrite_count() const {
        return pwrite_count_.load(std::memory_order_acquire);
    }
    uint64_t get_pwrite_bytes() const {
        return pwrite_bytes_.load(std::memory_order_acquire);
    }
    uint64_t get_wal_write_ns() const {
        return wal_write_ns_.load(std::memory_order_acquire);
    }
    uint64_t get_wal_fsync_ns() const {
        return wal_fsync_ns_.load(std::memory_order_acquire);
    }
    uint64_t get_commit_count() const {
        return commit_count_.load(std::memory_order_acquire);
    }
    DurabilityMode durability_mode() const {
        return durability_mode_;
    }

    void write_restart_offset(int64_t checkpoint_offset);
    int64_t read_restart_offset() const;

    LogBuffer* get_log_buffer() {
        return log_buffer_.get();
    }

private:
    struct CommitWaiter {
        lsn_t target_lsn{INVALID_LSN};
        bool require_sync{true};
        bool done{false};
        std::exception_ptr error;
        uint64_t enqueue_time_ns{0};
        std::condition_variable cv;
    };

    void flush_buffer(bool sync);
    void flush_log_to_disk_up_to_impl(lsn_t target_lsn, bool require_sync);

    // One leader performs the durable flush for all waiters that arrive
    // before it finishes. Waiters are released only after durable_lsn_ moves
    // past their target LSN.
    std::mutex group_commit_latch_;
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
    std::atomic<uint64_t> fsync_count_{0};
    std::atomic<uint64_t> group_commit_count_{0};
    std::atomic<uint64_t> group_commit_waiter_count_{0};
    std::atomic<uint64_t> group_commit_wait_ns_{0};
    std::atomic<uint64_t> pwrite_count_{0};
    std::atomic<uint64_t> pwrite_bytes_{0};
    std::atomic<uint64_t> wal_write_ns_{0};
    std::atomic<uint64_t> wal_fsync_ns_{0};
    std::atomic<uint64_t> commit_count_{0};
    int64_t log_file_offset_{0}; // 日志文件当前追加偏移
    DiskManager* disk_manager_;
    DurabilityMode durability_mode_{DurabilityMode::STRICT};
};
