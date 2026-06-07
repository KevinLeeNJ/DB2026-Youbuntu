/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <unistd.h>
#include <vector>
#include "log_manager.h"

std::unique_ptr<LogRecord> DeserializeLogRecord(const char* src, int size) {
    if (src == nullptr || size < LOG_HEADER_SIZE) {
        return nullptr;
    }

    LogRecord header;
    header.deserialize(src);
    if (header.log_tot_len_ < LOG_HEADER_SIZE || static_cast<int>(header.log_tot_len_) > size) {
        return nullptr;
    }

    std::unique_ptr<LogRecord> record;
    switch (header.log_type_) {
    case LogType::UPDATE:
        record = std::make_unique<UpdateLogRecord>();
        break;
    case LogType::INSERT:
        record = std::make_unique<InsertLogRecord>();
        break;
    case LogType::DELETE:
        record = std::make_unique<DeleteLogRecord>();
        break;
    case LogType::BEGIN:
        record = std::make_unique<BeginLogRecord>();
        break;
    case LogType::COMMIT:
        record = std::make_unique<CommitLogRecord>();
        break;
    case LogType::ABORT:
        record = std::make_unique<AbortLogRecord>();
        break;
    case LogType::CHECKPOINT:
        record = std::make_unique<CheckpointLogRecord>();
        break;
    default:
        return nullptr;
    }

    record->deserialize(src);
    return record;
}

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    if (log_record == nullptr) {
        return INVALID_LSN;
    }
    if (log_record->log_tot_len_ > LOG_BUFFER_SIZE) {
        throw std::length_error("log record is larger than log buffer");
    }

    std::lock_guard<std::mutex> lock(latch_);
    if (log_buffer_.is_full(static_cast<int>(log_record->log_tot_len_))) {
        flush_log_to_disk_unlocked();
    }

    lsn_t lsn = global_lsn_.fetch_add(1);
    log_record->lsn_ = lsn;
    log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    log_buffer_.offset_ += static_cast<int>(log_record->log_tot_len_);
    return lsn;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    std::lock_guard<std::mutex> lock(latch_);
    flush_log_to_disk_unlocked();
}

void LogManager::flush_log_to_disk_with_sync() {
    std::lock_guard<std::mutex> lock(latch_);
    flush_log_to_disk_unlocked();
    disk_manager_->fsync_log();
}

void LogManager::flush_log_to_disk_unlocked() {
    if (log_buffer_.offset_ == 0) {
        return;
    }
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    log_file_offset_ += log_buffer_.offset_;
    persist_lsn_ = global_lsn_.load() - 1;
    log_buffer_.offset_ = 0;
}

void LogManager::initialize_from_existing_log() {
    std::lock_guard<std::mutex> lock(latch_);
    log_buffer_.offset_ = 0;
    memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));

    int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        log_file_offset_ = 0;
        persist_lsn_ = INVALID_LSN;
        global_lsn_.store(0);
        return;
    }

    int offset = 0;
    lsn_t max_lsn = INVALID_LSN;
    std::vector<char> header_buf(LOG_HEADER_SIZE);
    while (offset + LOG_HEADER_SIZE <= file_size) {
        int bytes_read = disk_manager_->read_log(header_buf.data(), LOG_HEADER_SIZE, offset);
        if (bytes_read != LOG_HEADER_SIZE) {
            break;
        }

        LogRecord header;
        header.deserialize(header_buf.data());
        if (header.log_tot_len_ < LOG_HEADER_SIZE || offset + static_cast<int>(header.log_tot_len_) > file_size) {
            break;
        }

        max_lsn = std::max(max_lsn, header.lsn_);
        offset += static_cast<int>(header.log_tot_len_);
    }

    log_file_offset_ = offset;
    persist_lsn_ = max_lsn;
    global_lsn_.store(max_lsn == INVALID_LSN ? 0 : max_lsn + 1);
    if (offset < file_size) {
        truncate(LOG_FILE_NAME.c_str(), offset);
    }
    disk_manager_->SetLogOffset(offset);
}

void LogManager::write_restart_offset(int checkpoint_offset) {
    std::ofstream restart_file(RESTART_FILE_NAME, std::ios::trunc);
    if (!restart_file.is_open()) {
        return;
    }
    restart_file << checkpoint_offset;
}

int LogManager::read_restart_offset() const {
    std::ifstream restart_file(RESTART_FILE_NAME);
    if (!restart_file.is_open()) {
        return 0;
    }

    int checkpoint_offset = 0;
    restart_file >> checkpoint_offset;
    if (!restart_file || checkpoint_offset < 0) {
        return 0;
    }
    return checkpoint_offset;
}
