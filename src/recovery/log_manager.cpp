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

#include <algorithm>
#include <cstdio>
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
    lsn_t target_lsn = global_lsn_.load(std::memory_order_acquire) - 1;
    if (target_lsn == INVALID_LSN) {
        std::lock_guard<std::mutex> lock(latch_);
        flush_log_to_disk_unlocked();
        disk_manager_->fsync_log();
        durable_lsn_.store(persist_lsn_, std::memory_order_release);
        return;
    }
    flush_log_to_disk_up_to(target_lsn);
}

void LogManager::flush_log_to_disk_up_to(lsn_t target_lsn) {
    if (target_lsn == INVALID_LSN) {
        return;
    }

    // A page can retain an LSN from a previous WAL epoch, or legacy callers
    // can pass bytes from the page payload as an LSN. Never wait for a record
    // that this LogManager has not allocated: WAL durability only needs to
    // cover the current log prefix.
    {
        std::lock_guard<std::mutex> lock(latch_);
        lsn_t latest_lsn = global_lsn_.load(std::memory_order_acquire) - 1;
        if (latest_lsn == INVALID_LSN) {
            return;
        }
        target_lsn = std::min(target_lsn, latest_lsn);
    }
    if (durable_lsn_.load(std::memory_order_acquire) >= target_lsn) {
        return;
    }

    auto waiter = std::make_shared<CommitWaiter>();
    waiter->target_lsn = target_lsn;
    waiter->enqueue_time_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
    bool become_leader = false;
    {
        std::unique_lock<std::mutex> group_lock(group_commit_latch_);
        if (durable_lsn_.load(std::memory_order_acquire) >= target_lsn) {
            return;
        }
        group_commit_waiters_.push_back(waiter);
        if (!group_commit_leader_active_) {
            group_commit_leader_active_ = true;
            become_leader = true;
        }

        if (!become_leader) {
            waiter->cv.wait(group_lock, [&waiter] { return waiter->done; });
            if (waiter->error != nullptr) {
                std::rethrow_exception(waiter->error);
            }
            return;
        }
    }

    // A concurrent commit may have joined while the leader was writing or
    // syncing. Keep extending the batch until every pending target is
    // covered by the durable LSN.
    for (;;) {
        try {
            {
                std::lock_guard<std::mutex> log_lock(latch_);
                flush_log_to_disk_unlocked();
                disk_manager_->fsync_log();
                fsync_count_.fetch_add(1, std::memory_order_acq_rel);
                durable_lsn_.store(persist_lsn_, std::memory_order_release);
            }
        } catch (...) {
            auto error = std::current_exception();
            std::vector<std::shared_ptr<CommitWaiter>> notify;
            {
                std::lock_guard<std::mutex> group_lock(group_commit_latch_);
                for (const auto& pending : group_commit_waiters_) {
                    pending->error = error;
                    pending->done = true;
                    notify.push_back(pending);
                }
                group_commit_waiters_.clear();
                group_commit_leader_active_ = false;
            }
            for (const auto& pending : notify) {
                pending->cv.notify_one();
            }
            std::rethrow_exception(error);
        }

        std::vector<std::shared_ptr<CommitWaiter>> notify;
        bool group_done = false;
        const lsn_t durable_lsn = durable_lsn_.load(std::memory_order_acquire);
        {
            std::lock_guard<std::mutex> group_lock(group_commit_latch_);
            for (auto it = group_commit_waiters_.begin(); it != group_commit_waiters_.end();) {
                if ((*it)->target_lsn <= durable_lsn) {
                    (*it)->done = true;
                    notify.push_back(*it);
                    it = group_commit_waiters_.erase(it);
                } else {
                    ++it;
                }
            }
            if (group_commit_waiters_.empty()) {
                group_commit_leader_active_ = false;
                group_done = true;
            }
        }
        for (const auto& pending : notify) {
            pending->cv.notify_one();
        }
        if (!notify.empty()) {
            const uint64_t now_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                    .count());
            group_commit_count_.fetch_add(1, std::memory_order_acq_rel);
            group_commit_waiter_count_.fetch_add(notify.size(), std::memory_order_acq_rel);
            for (const auto& pending : notify) {
                group_commit_wait_ns_.fetch_add(now_ns - pending->enqueue_time_ns, std::memory_order_acq_rel);
            }
        }
        if (group_done) {
            return;
        }
    }
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

    int64_t file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        log_file_offset_ = 0;
        persist_lsn_ = INVALID_LSN;
        durable_lsn_.store(INVALID_LSN, std::memory_order_release);
        global_lsn_.store(0);
        return;
    }

    int64_t offset = 0;
    lsn_t max_lsn = INVALID_LSN;
    std::vector<char> header_buf(LOG_HEADER_SIZE);
    while (offset + LOG_HEADER_SIZE <= file_size) {
        int bytes_read = disk_manager_->read_log(header_buf.data(), LOG_HEADER_SIZE, offset);
        if (bytes_read != LOG_HEADER_SIZE) {
            break;
        }

        LogRecord header;
        header.deserialize(header_buf.data());
        if (header.log_tot_len_ < LOG_HEADER_SIZE || offset + static_cast<int64_t>(header.log_tot_len_) > file_size) {
            break;
        }

        max_lsn = std::max(max_lsn, header.lsn_);
        offset += static_cast<int64_t>(header.log_tot_len_);
    }

    log_file_offset_ = offset;
    persist_lsn_ = max_lsn;
    durable_lsn_.store(max_lsn, std::memory_order_release);
    global_lsn_.store(max_lsn == INVALID_LSN ? 0 : max_lsn + 1);
    if (offset < file_size) {
        disk_manager_->truncate_log_to(offset);
    }
    disk_manager_->SetLogOffset(offset);
}

void LogManager::reset_log(lsn_t next_lsn) {
    std::lock_guard<std::mutex> lock(latch_);
    // 先把日志缓冲区残留内容落盘，避免截断丢失未刷盘的日志。
    flush_log_to_disk_unlocked();
    // 截断日志文件为空并重置追加偏移。
    disk_manager_->truncate_log();
    log_file_offset_ = 0;
    log_buffer_.offset_ = 0;
    global_lsn_.store(next_lsn);
    persist_lsn_ = INVALID_LSN;
    // Pages flushed before truncation may still carry their old page LSN.
    // Treat the truncated prefix as durable so those stale LSNs do not cause
    // a false WAL-missing failure after restart.
    durable_lsn_.store(next_lsn == 0 ? INVALID_LSN : next_lsn - 1, std::memory_order_release);
}

void LogManager::write_restart_offset(int64_t checkpoint_offset) {
    const std::string temp_name = std::string(RESTART_FILE_NAME) + ".tmp";
    std::ofstream restart_file(temp_name, std::ios::trunc);
    if (!restart_file.is_open()) {
        throw UnixError();
    }
    restart_file << checkpoint_offset;
    restart_file.flush();
    if (!restart_file) {
        throw UnixError();
    }
    restart_file.close();
    if (!restart_file) {
        throw UnixError();
    }
    disk_manager_->sync_path(temp_name);
    if (rename(temp_name.c_str(), RESTART_FILE_NAME) != 0) {
        throw UnixError();
    }
    disk_manager_->sync_directory(".");
}

int64_t LogManager::read_restart_offset() const {
    std::ifstream restart_file(RESTART_FILE_NAME);
    if (!restart_file.is_open()) {
        return 0;
    }

    int64_t checkpoint_offset = 0;
    restart_file >> checkpoint_offset;
    if (!restart_file || checkpoint_offset < 0) {
        return 0;
    }
    return checkpoint_offset;
}
