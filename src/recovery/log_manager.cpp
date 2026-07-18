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
#include <thread>
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

    for (;;) {
        {
            std::unique_lock<std::mutex> lock(latch_);
            if (!log_buffer_->is_full(static_cast<int>(log_record->log_tot_len_))) {
                lsn_t lsn = global_lsn_.fetch_add(1);
                log_record->lsn_ = lsn;
                log_record->serialize(log_buffer_->buffer_ + log_buffer_->offset_);
                log_buffer_->offset_ += static_cast<int>(log_record->log_tot_len_);
                if (log_record->log_type_ == LogType::COMMIT) {
                    commit_count_.fetch_add(1, std::memory_order_acq_rel);
                }
                return lsn;
            }
        }
        flush_buffer(false);
    }
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    flush_buffer(false);
}

void LogManager::flush_log_to_disk_with_sync() {
    lsn_t target_lsn = global_lsn_.load(std::memory_order_acquire) - 1;
    if (target_lsn == INVALID_LSN) {
        flush_buffer(true);
        return;
    }
    flush_log_to_disk_up_to_impl(target_lsn, true);
}

void LogManager::flush_log_to_disk_up_to(lsn_t target_lsn) {
    flush_log_to_disk_up_to_impl(target_lsn, durability_mode_ == DurabilityMode::STRICT);
}

void LogManager::flush_log_to_disk_up_to_impl(lsn_t target_lsn, bool require_sync) {
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
    const auto completed_lsn = [&] {
        return require_sync ? durable_lsn_.load(std::memory_order_acquire)
                            : persist_lsn_.load(std::memory_order_acquire);
    };
    if (completed_lsn() >= target_lsn) {
        return;
    }

    auto waiter = std::make_shared<CommitWaiter>();
    waiter->target_lsn = target_lsn;
    waiter->require_sync = require_sync;
    waiter->enqueue_time_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
    bool become_leader = false;
    {
        std::unique_lock<std::mutex> group_lock(group_commit_latch_);
        if (completed_lsn() >= target_lsn) {
            return;
        }
        group_commit_waiters_.push_back(waiter);
        if (!group_commit_leader_active_) {
            group_commit_leader_active_ = true;
            become_leader = true;
        }
        group_commit_cv_.notify_one();

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
            // Allow commits arriving together to append before the active
            // buffer is swapped. The swap itself is short; pwrite and
            // fdatasync happen without the append latch.
            bool sync_batch = false;
            const auto batch_wait_begin = std::chrono::steady_clock::now();
            // At high concurrency four waiters form a useful batch
            // immediately; otherwise allow a short bounded window for
            // committers to join the same WAL write. New waiters wake the
            // leader, so the common low-contention path does not poll.
            {
                std::unique_lock<std::mutex> group_lock(group_commit_latch_);
                const auto deadline = batch_wait_begin + std::chrono::milliseconds(2);
                group_commit_cv_.wait_until(group_lock, deadline, [this] { return group_commit_waiters_.size() >= 4; });
                sync_batch = false;
                for (const auto& pending : group_commit_waiters_) {
                    sync_batch = sync_batch || pending->require_sync;
                }
            }
            flush_buffer(sync_batch);
            if (sync_batch) {
                fsync_count_.fetch_add(1, std::memory_order_acq_rel);
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
        const lsn_t written_lsn = persist_lsn_.load(std::memory_order_acquire);
        const lsn_t durable_lsn = durable_lsn_.load(std::memory_order_acquire);
        {
            std::lock_guard<std::mutex> group_lock(group_commit_latch_);
            for (auto it = group_commit_waiters_.begin(); it != group_commit_waiters_.end();) {
                const lsn_t completion_lsn = (*it)->require_sync ? durable_lsn : written_lsn;
                if ((*it)->target_lsn <= completion_lsn) {
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
            const uint64_t now_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                              std::chrono::steady_clock::now().time_since_epoch())
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

void LogManager::flush_buffer(bool sync) {
    for (;;) {
        int bytes = 0;
        lsn_t target_lsn = INVALID_LSN;
        {
            std::unique_lock<std::mutex> lock(latch_);
            buffer_cv_.wait(lock, [this] { return !flushing_in_progress_; });
            if (flushing_buffer_->offset_ != 0) {
                // A previous write failed. Retry that stable buffer before
                // swapping newer active records behind it.
                bytes = flushing_buffer_->offset_;
                target_lsn = flushing_lsn_;
                flushing_bytes_ = bytes;
                flushing_in_progress_ = true;
            } else {
                if (log_buffer_->offset_ == 0) {
                    if (!sync) {
                        return;
                    }
                    // Capture the exact written prefix covered by this sync.
                    // Another writer may advance persist_lsn_ while the latch
                    // is released; that newer prefix belongs to a later sync.
                    target_lsn = persist_lsn_.load(std::memory_order_acquire);
                    lock.unlock();
                    const auto fsync_begin = std::chrono::steady_clock::now();
                    disk_manager_->fsync_log();
                    wal_fsync_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                      std::chrono::steady_clock::now() - fsync_begin)
                                                                      .count()),
                                            std::memory_order_acq_rel);
                    lsn_t durable = durable_lsn_.load(std::memory_order_acquire);
                    while (durable < target_lsn &&
                           !durable_lsn_.compare_exchange_weak(durable, target_lsn, std::memory_order_release,
                                                               std::memory_order_acquire)) {
                    }
                    return;
                }
                std::swap(log_buffer_, flushing_buffer_);
                bytes = flushing_buffer_->offset_;
                target_lsn = global_lsn_.load(std::memory_order_acquire) - 1;
                flushing_lsn_ = target_lsn;
                flushing_bytes_ = bytes;
                flushing_in_progress_ = true;
            }
        }

        // The active buffer is available to producers while this stable
        // buffer is written and synced.
        bool write_succeeded = false;
        try {
            const auto write_begin = std::chrono::steady_clock::now();
            disk_manager_->write_log(flushing_buffer_->buffer_, bytes);
            wal_write_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                              std::chrono::steady_clock::now() - write_begin)
                                                              .count()),
                                    std::memory_order_acq_rel);
            pwrite_count_.fetch_add(1, std::memory_order_acq_rel);
            pwrite_bytes_.fetch_add(static_cast<uint64_t>(bytes), std::memory_order_acq_rel);
            write_succeeded = true;
            if (sync) {
                const auto fsync_begin = std::chrono::steady_clock::now();
                disk_manager_->fsync_log();
                wal_fsync_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                  std::chrono::steady_clock::now() - fsync_begin)
                                                                  .count()),
                                        std::memory_order_acq_rel);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(latch_);
            if (write_succeeded) {
                log_file_offset_ += bytes;
                persist_lsn_.store(target_lsn, std::memory_order_release);
                flushing_buffer_->offset_ = 0;
                flushing_bytes_ = 0;
                flushing_lsn_ = INVALID_LSN;
            }
            flushing_in_progress_ = false;
            buffer_cv_.notify_all();
            throw;
        }

        {
            std::lock_guard<std::mutex> lock(latch_);
            log_file_offset_ += flushing_bytes_;
            persist_lsn_.store(flushing_lsn_, std::memory_order_release);
            flushing_buffer_->offset_ = 0;
            flushing_bytes_ = 0;
            flushing_lsn_ = INVALID_LSN;
            flushing_in_progress_ = false;
            if (sync) {
                lsn_t durable = durable_lsn_.load(std::memory_order_acquire);
                while (durable < target_lsn &&
                       !durable_lsn_.compare_exchange_weak(durable, target_lsn, std::memory_order_release,
                                                           std::memory_order_acquire)) {
                }
            }
        }
        buffer_cv_.notify_all();
        return;
    }
}

void LogManager::initialize_from_existing_log() {
    std::lock_guard<std::mutex> lock(latch_);
    log_buffer_->offset_ = 0;
    flushing_buffer_->offset_ = 0;
    flushing_in_progress_ = false;
    memset(log_buffer_->buffer_, 0, sizeof(log_buffer_->buffer_));

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
    // Ensure active WAL is written and durable before truncation.
    flush_log_to_disk_with_sync();
    std::lock_guard<std::mutex> lock(latch_);
    // 截断日志文件为空并重置追加偏移。
    disk_manager_->truncate_log();
    log_file_offset_ = 0;
    log_buffer_->offset_ = 0;
    flushing_buffer_->offset_ = 0;
    global_lsn_.store(next_lsn);
    persist_lsn_.store(INVALID_LSN, std::memory_order_release);
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
