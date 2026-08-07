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
#include <cassert>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <unistd.h>
#include <vector>
#include "index_smo_log.h"
#include "log_manager.h"
#include "wal_reader.h"
#include "minilog.h"

namespace {

// lsn_t is int32_t. Signed overflow is undefined behaviour and would also
// invalidate every page_lsn comparison, so stop the process well before the
// counter can wrap. At the observed consumption rate this margin is hours away.
constexpr lsn_t LSN_EXHAUSTION_MARGIN = 1 << 24;

[[noreturn]] void FailStopOnLsnExhaustion(lsn_t lsn) {
    std::fprintf(stderr, "FATAL: WAL LSN space nearly exhausted (lsn=%d); stopping for recovery\n",
                 static_cast<int>(lsn));
    std::fflush(stderr);
    std::_Exit(134);
}

// High-concurrency A/B shows eight waiters balancing commit latency and
// fdatasync amortization. Larger waves wait too long; smaller waves leave the
// WAL device saturated with roughly half as many commits per sync.
constexpr size_t GROUP_COMMIT_BATCH_WAITERS = 8;
// Upper bound on the coalescing window, reachable only below saturation where
// no other committer is competing for the disk. PostgreSQL's commit_delay plays
// the same role and is likewise capped in the millisecond range.
constexpr std::chrono::milliseconds GROUP_COMMIT_BATCH_WINDOW{2};
constexpr std::chrono::milliseconds kSlowWalFdatasyncThreshold{20};

void LogSlowWalFdatasync(std::chrono::steady_clock::duration elapsed, int bytes, lsn_t target_lsn,
                         lsn_t durable_before, bool mode_strict) noexcept {
    (void)durable_before;
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
    if (elapsed_ms <= kSlowWalFdatasyncThreshold) return;
    // Keep slow-storage diagnosis from becoming a competing source of tail
    // latency. The formatting/logging itself is intentionally outside the
    // measured fdatasync interval.
    struct Aggregate {
        std::mutex latch;
        std::chrono::steady_clock::time_point window{};
        uint64_t count{};
        uint64_t total_ms{};
        uint64_t max_ms{};
        int min_bytes{};
        int max_bytes{};
        lsn_t min_target{INVALID_LSN};
        lsn_t max_target{INVALID_LSN};
    };
    static Aggregate aggregate;
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(aggregate.latch);
    if (aggregate.window == std::chrono::steady_clock::time_point{} ||
        now - aggregate.window >= std::chrono::seconds(1)) {
        if (aggregate.count != 0) {
            LOG_WARN("wal-fdatasync-slow count=%llu total_ms=%llu max_ms=%llu bytes_range=%d:%d "
                     "target_lsn_range=%d:%d suppressed=%llu mode_strict=%d",
                     static_cast<unsigned long long>(aggregate.count),
                     static_cast<unsigned long long>(aggregate.total_ms),
                     static_cast<unsigned long long>(aggregate.max_ms), aggregate.min_bytes, aggregate.max_bytes,
                     static_cast<int>(aggregate.min_target), static_cast<int>(aggregate.max_target),
                     static_cast<unsigned long long>(aggregate.count - 1), mode_strict);
        }
        aggregate.window = now;
        aggregate.count = 0;
        aggregate.total_ms = 0;
        aggregate.max_ms = 0;
        aggregate.min_bytes = bytes;
        aggregate.max_bytes = bytes;
        aggregate.min_target = target_lsn;
        aggregate.max_target = target_lsn;
    }
    ++aggregate.count;
    aggregate.total_ms += static_cast<uint64_t>(elapsed_ms.count());
    aggregate.max_ms = std::max(aggregate.max_ms, static_cast<uint64_t>(elapsed_ms.count()));
    aggregate.min_bytes = std::min(aggregate.min_bytes, bytes);
    aggregate.max_bytes = std::max(aggregate.max_bytes, bytes);
    aggregate.min_target = std::min(aggregate.min_target, target_lsn);
    aggregate.max_target = std::max(aggregate.max_target, target_lsn);
}

bool IsLegalEmptyCheckpoint(const char* bytes, size_t bytes_size) {
    constexpr size_t kEmptyCheckpointBytes = LOG_HEADER_SIZE + sizeof(size_t);
    return bytes != nullptr && bytes_size == kEmptyCheckpointBytes &&
           read_unaligned<LogType>(bytes + OFFSET_LOG_TYPE) == LogType::CHECKPOINT &&
           read_unaligned<uint32_t>(bytes + OFFSET_LOG_TOT_LEN) == kEmptyCheckpointBytes &&
           read_unaligned<lsn_t>(bytes + OFFSET_LSN) != INVALID_LSN &&
           read_unaligned<txn_id_t>(bytes + OFFSET_LOG_TID) == INVALID_TXN_ID &&
           read_unaligned<lsn_t>(bytes + OFFSET_PREV_LSN) == INVALID_LSN &&
           read_unaligned<size_t>(bytes + OFFSET_LOG_DATA) == 0;
}

std::string RestartManifestV2Payload(const RestartManifest& manifest) {
    return "wal_format=segmented-v2\nwal_generation=" + std::to_string(manifest.wal_generation) +
           "\nsegment_bytes=" + std::to_string(manifest.wal_segment_bytes) +
           "\nrestart_segment=" + std::to_string(manifest.restart_segment) +
           "\nrestart_offset=" + std::to_string(manifest.restart_offset) +
           "\nnext_lsn=" + std::to_string(manifest.next_lsn) +
           "\nnext_timestamp=" + std::to_string(manifest.next_timestamp) +
           "\nnext_txn_id=" + std::to_string(manifest.next_txn_id) + "\n";
}

uint64_t RestartManifestV2Checksum(const RestartManifest& manifest) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char byte : RestartManifestV2Payload(manifest)) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

} // namespace

std::unique_ptr<LogRecord> DeserializeLogRecord(const char* src, int size) {
    if (src == nullptr || size < LOG_HEADER_SIZE) {
        return nullptr;
    }

    LogRecord header;
    header.deserialize(src);
    if (header.log_tot_len_ < LOG_HEADER_SIZE || header.log_tot_len_ > MAX_INDEX_SMO_RECORD_BYTES ||
        static_cast<int>(header.log_tot_len_) > size) {
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
    if (log_record->log_tot_len_ > MAX_INDEX_SMO_RECORD_BYTES) {
        throw std::length_error("log record exceeds the bounded WAL record size");
    }

    for (;;) {
        {
            std::unique_lock<std::mutex> lock(latch_);
            const int record_bytes = static_cast<int>(log_record->log_tot_len_);
            const bool ordinary_batch_has_room = log_buffer_->offset_ != 0 && log_buffer_->offset_ <= LOG_BUFFER_SIZE &&
                                                 log_buffer_->offset_ + record_bytes <= LOG_BUFFER_SIZE;
            if (log_buffer_->offset_ == 0 || ordinary_batch_has_room) {
                // Capacity growth can throw, so it deliberately happens before
                // allocating the LSN. Once fetch_add succeeds serialize() must
                // be a deterministic, non-throwing copy into reserved memory.
                log_buffer_->ensure_capacity(static_cast<size_t>(log_buffer_->offset_) + log_record->log_tot_len_);
                lsn_t lsn = global_lsn_.fetch_add(1);
                if (lsn > INT32_MAX - LSN_EXHAUSTION_MARGIN) {
                    FailStopOnLsnExhaustion(lsn);
                }
                log_record->lsn_ = lsn;
                log_record->serialize(log_buffer_->buffer_.data() + log_buffer_->offset_);
                log_buffer_->offset_ += static_cast<int>(log_record->log_tot_len_);
                return lsn;
            }
        }
        flush_buffer(false);
    }
}

lsn_t LogManager::append_index_smo(const IndexSmoWalData& data) {
    // Keep the binding stable until the dependent SMO record is appended.
    // A checkpoint WAL cut takes the same lock before replacing all visible
    // generations, so it cannot land between generation lookup and append.
    std::lock_guard<std::mutex> binding_lock(index_binding_latch_);
    IndexSmoWalData bound_data = data;
    const uint64_t epoch = wal_epoch_.load(std::memory_order_acquire);
    auto binding = index_bindings_.find(data.index_file_name);
    bound_data.index_generation = binding != index_bindings_.end() && binding->second.epoch == epoch
                                      ? binding->second.generation
                                      : publish_index_binding_locked(data.index_file_name, epoch);
    IndexSmoLogRecord record(bound_data);
    return add_log_to_buffer(&record);
}

uint64_t LogManager::ensure_index_binding(const std::string& index_file_name) {
    std::lock_guard<std::mutex> binding_lock(index_binding_latch_);
    const uint64_t epoch = wal_epoch_.load(std::memory_order_acquire);
    auto it = index_bindings_.find(index_file_name);
    if (it != index_bindings_.end() && it->second.epoch == epoch) {
        return it->second.generation;
    }
    return publish_index_binding_locked(index_file_name, epoch);
}

uint64_t LogManager::renew_index_binding(const std::string& index_file_name) {
    std::lock_guard<std::mutex> binding_lock(index_binding_latch_);
    return publish_index_binding_locked(index_file_name, wal_epoch_.load(std::memory_order_acquire));
}

CheckpointWalCut LogManager::create_checkpoint_wal_cut(const std::vector<std::string>& index_file_names) {
    // Validate every name and bound the eventual checkpoint batch before
    // entering either WAL mutex.
    CheckpointLogRecord checkpoint;
    size_t batch_bytes = checkpoint.log_tot_len_;
    for (const std::string& index_file_name : index_file_names) {
        IndexBindLogRecord validated(index_file_name);
        if (validated.log_tot_len_ > static_cast<size_t>(INT_MAX) - batch_bytes) {
            throw std::length_error("checkpoint WAL cut exceeds the append-buffer limit");
        }
        batch_bytes += validated.log_tot_len_;
    }

    CheckpointWalCut cut;
    cut.index_bindings.reserve(index_file_names.size());
    for (const std::string& index_file_name : index_file_names) {
        cut.index_bindings.emplace_back(index_file_name, 0);
    }

    // Global lock order for this append batch is binding_latch_ -> latch_.
    // append_index_smo() follows the same order. The later durable-prefix sync
    // holds neither latch, so it cannot prolong checkpoint admission.
    std::unique_lock<std::mutex> binding_lock(index_binding_latch_);
    const uint64_t epoch = wal_epoch_.load(std::memory_order_acquire);
    std::vector<IndexBindLogRecord> binding_records;
    binding_records.reserve(index_file_names.size());
    for (size_t i = 0; i < index_file_names.size(); ++i) {
        const std::string& index_file_name = index_file_names[i];
        auto binding = index_bindings_.find(index_file_name);
        if (binding == index_bindings_.end() || binding->second.epoch != epoch) {
            // Establish a normal V1 generation durably before the cut. If this
            // fails, no checkpoint record has been appended and the caller can
            // safely abandon the round.
            const uint64_t generation = publish_index_binding_locked(index_file_name, epoch, false);
            binding = index_bindings_.find(index_file_name);
            assert(binding != index_bindings_.end() && binding->second.generation == generation);
        }
        cut.index_bindings[i].second = binding->second.generation;
        // V2 reissues the current identity; it deliberately does not renew it.
        // Thus a crash before manifest publication cannot invalidate older SMO
        // records still reachable through the previous restart boundary.
        binding_records.emplace_back(index_file_name, binding->second.generation);
    }
    {
        std::lock_guard<std::mutex> lock(latch_);
        if (batch_bytes > static_cast<size_t>(INT_MAX - log_buffer_->offset_)) {
            throw std::length_error("checkpoint WAL cut overflows the append buffer");
        }
        const size_t record_count = binding_records.size() + 1;
        const lsn_t next_lsn = global_lsn_.load(std::memory_order_acquire);
        if (record_count > static_cast<size_t>(INT32_MAX - LSN_EXHAUSTION_MARGIN) ||
            next_lsn > INT32_MAX - LSN_EXHAUSTION_MARGIN - static_cast<lsn_t>(record_count - 1)) {
            FailStopOnLsnExhaustion(next_lsn);
        }

        // A currently flushing buffer precedes the active buffer in the WAL.
        // All three terms are protected by latch_, making this the exact byte
        // address at which the checkpoint will be serialized.
        cut.checkpoint_offset =
            log_file_offset_ + static_cast<int64_t>(flushing_bytes_) + static_cast<int64_t>(log_buffer_->offset_);
        log_buffer_->ensure_capacity(static_cast<size_t>(log_buffer_->offset_) + batch_bytes);

        cut.checkpoint_lsn = global_lsn_.fetch_add(1);
        checkpoint.lsn_ = cut.checkpoint_lsn;
        checkpoint.serialize(log_buffer_->buffer_.data() + log_buffer_->offset_);
        log_buffer_->offset_ += static_cast<int>(checkpoint.log_tot_len_);

        for (size_t i = 0; i < binding_records.size(); ++i) {
            IndexBindLogRecord& record = binding_records[i];
            record.lsn_ = global_lsn_.fetch_add(1);
            record.serialize(log_buffer_->buffer_.data() + log_buffer_->offset_);
            log_buffer_->offset_ += static_cast<int>(record.log_tot_len_);
            cut.last_lsn = record.lsn_;
        }
        if (binding_records.empty()) {
            cut.last_lsn = cut.checkpoint_lsn;
        }
    }

    return cut;
}

void LogManager::sync_checkpoint_wal_cut(const CheckpointWalCut& cut) {
    if (cut.last_lsn == INVALID_LSN) {
        throw std::invalid_argument("checkpoint WAL cut has no last LSN");
    }
    // The durable prefix contains the cut and every preceding record. Later
    // appends may be coalesced, but never make the cut appear after them.
    flush_log_to_disk_up_to_impl(cut.last_lsn, true);
}

uint64_t LogManager::publish_index_binding_locked(const std::string& index_file_name, uint64_t epoch, bool durable) {
    IndexBindLogRecord record(index_file_name);
    const lsn_t lsn = add_log_to_buffer(&record);
    if (durable) flush_log_to_disk_up_to_impl(lsn, true);
    const uint64_t generation = static_cast<uint64_t>(lsn) + 1U;
    index_bindings_[index_file_name] = IndexBinding{generation, epoch};
    return generation;
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

void LogManager::flush_log_to_disk_up_to_durable(lsn_t target_lsn) {
    flush_log_to_disk_up_to_impl(target_lsn, true);
}

void LogManager::flush_log_to_disk_up_to_impl(lsn_t target_lsn, bool require_sync) {
    if (!leader_rotation_enabled_) {
        flush_log_to_disk_up_to_legacy(target_lsn, require_sync);
        return;
    }
    flush_log_to_disk_up_to_with_leader_rotation(target_lsn, require_sync);
}

void LogManager::run_group_commit_test_hook(std::string_view point) const {
    if (group_commit_test_hook_) group_commit_test_hook_(point);
}

void LogManager::flush_log_to_disk_up_to_legacy(lsn_t target_lsn, bool require_sync) {
    WalFlushMetrics* const metrics = wal_flush_metrics_;
    const bool metrics_enabled = metrics != nullptr && metrics->enabled();
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
        if (metrics_enabled) metrics->record_already_covered_fast_path();
        return;
    }

    auto waiter = std::make_shared<CommitWaiter>();
    waiter->target_lsn = target_lsn;
    waiter->require_sync = require_sync;
    bool become_leader = false;
    {
        std::unique_lock<std::mutex> group_lock(group_commit_latch_);
        if (completed_lsn() >= target_lsn) {
            if (metrics_enabled) metrics->record_already_covered_fast_path();
            return;
        }
        group_commit_waiters_.push_back(waiter);
        if (!group_commit_leader_active_) {
            group_commit_leader_active_ = true;
            become_leader = true;
        }
        group_commit_cv_.notify_one();

        if (!become_leader) {
            if (metrics_enabled) {
                metrics->record_follower_request();
                const auto wait_begin = std::chrono::steady_clock::now();
                waiter->cv.wait(group_lock, [&waiter] { return waiter->done; });
                metrics->record_follower_wait(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - wait_begin)
                        .count()));
            } else {
                waiter->cv.wait(group_lock, [&waiter] { return waiter->done; });
            }
            if (waiter->error != nullptr) {
                std::rethrow_exception(waiter->error);
            }
            return;
        }
        if (metrics_enabled) metrics->record_leader_request();
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
            // At high concurrency the batch is already wider than the threshold
            // when the leader gets here, so this returns without waiting;
            // otherwise allow a short bounded window for committers to join the
            // same WAL write. New waiters wake the leader, so the common
            // low-contention path does not poll.
            {
                std::unique_lock<std::mutex> group_lock(group_commit_latch_);
                const auto deadline = batch_wait_begin + GROUP_COMMIT_BATCH_WINDOW;
                group_commit_cv_.wait_until(group_lock, deadline, [this] {
                    return group_commit_waiters_.size() >= GROUP_COMMIT_BATCH_WAITERS;
                });
                sync_batch = false;
                for (const auto& pending : group_commit_waiters_) {
                    sync_batch = sync_batch || pending->require_sync;
                }
            }
            if (metrics_enabled) {
                metrics->record_coalescing_wait(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - batch_wait_begin)
                        .count()));
            }
            flush_buffer(sync_batch);
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
        if (metrics_enabled) metrics->record_completed_batch(notify.size());
        for (const auto& pending : notify) {
            pending->cv.notify_one();
        }
        if (group_done) {
            return;
        }
    }
}

void LogManager::flush_log_to_disk_up_to_with_leader_rotation(lsn_t target_lsn, bool require_sync) {
    WalFlushMetrics* const metrics = wal_flush_metrics_;
    const bool metrics_enabled = metrics != nullptr && metrics->enabled();
    if (target_lsn == INVALID_LSN) return;

    {
        std::lock_guard<std::mutex> lock(latch_);
        const lsn_t latest_lsn = global_lsn_.load(std::memory_order_acquire) - 1;
        if (latest_lsn == INVALID_LSN) return;
        target_lsn = std::min(target_lsn, latest_lsn);
    }
    const auto completed_lsn = [&] {
        return require_sync ? durable_lsn_.load(std::memory_order_acquire)
                            : persist_lsn_.load(std::memory_order_acquire);
    };
    if (completed_lsn() >= target_lsn) {
        if (metrics_enabled) metrics->record_already_covered_fast_path();
        return;
    }

    auto waiter = std::make_shared<CommitWaiter>();
    waiter->target_lsn = target_lsn;
    waiter->require_sync = require_sync;
    bool leader = false;
    {
        std::unique_lock<std::mutex> group_lock(group_commit_latch_);
        if (completed_lsn() >= target_lsn) {
            if (metrics_enabled) metrics->record_already_covered_fast_path();
            return;
        }
        group_commit_waiters_.push_back(waiter);
        if (!group_commit_leader_active_) {
            group_commit_leader_active_ = true;
            waiter->state = CommitWaiter::State::Promoted;
            leader = true;
        }
        group_commit_cv_.notify_one();
        if (!leader) {
            run_group_commit_test_hook("rotation_follower_enqueued");
            if (metrics_enabled) {
                metrics->record_follower_request();
                const auto wait_begin = std::chrono::steady_clock::now();
                waiter->cv.wait(group_lock, [&waiter] {
                    return waiter->state == CommitWaiter::State::Done || waiter->state == CommitWaiter::State::Promoted;
                });
                metrics->record_follower_wait(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - wait_begin)
                        .count()));
            } else {
                waiter->cv.wait(group_lock, [&waiter] {
                    return waiter->state == CommitWaiter::State::Done || waiter->state == CommitWaiter::State::Promoted;
                });
            }
            if (waiter->state == CommitWaiter::State::Done) {
                if (waiter->error != nullptr) std::rethrow_exception(waiter->error);
                return;
            }
            leader = true;
        }
        if (metrics_enabled) metrics->record_leader_request();
    }

    uint64_t batches = 0;
    for (;;) {
        try {
            bool sync_batch = false;
            const auto batch_wait_begin = std::chrono::steady_clock::now();
            {
                std::unique_lock<std::mutex> group_lock(group_commit_latch_);
                const auto deadline = batch_wait_begin + GROUP_COMMIT_BATCH_WINDOW;
                group_commit_cv_.wait_until(group_lock, deadline, [this] {
                    return group_commit_waiters_.size() >= GROUP_COMMIT_BATCH_WAITERS;
                });
                for (const auto& pending : group_commit_waiters_) sync_batch = sync_batch || pending->require_sync;
            }
            if (metrics_enabled) {
                metrics->record_coalescing_wait(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - batch_wait_begin)
                        .count()));
            }
            run_group_commit_test_hook("rotation_before_flush");
            flush_buffer(sync_batch);
            ++batches;
        } catch (...) {
            const auto error = std::current_exception();
            std::vector<std::shared_ptr<CommitWaiter>> notify;
            {
                std::lock_guard<std::mutex> group_lock(group_commit_latch_);
                for (const auto& pending : group_commit_waiters_) {
                    pending->error = error;
                    pending->state = CommitWaiter::State::Done;
                    notify.push_back(pending);
                }
                group_commit_waiters_.clear();
                group_commit_leader_active_ = false;
            }
            for (const auto& pending : notify) pending->cv.notify_one();
            if (metrics_enabled) metrics->record_leader_tenure(batches);
            std::rethrow_exception(error);
        }

        std::vector<std::shared_ptr<CommitWaiter>> notify;
        std::shared_ptr<CommitWaiter> promoted;
        bool own_done = false;
        const lsn_t written_lsn = persist_lsn_.load(std::memory_order_acquire);
        const lsn_t durable = durable_lsn_.load(std::memory_order_acquire);
        {
            std::lock_guard<std::mutex> group_lock(group_commit_latch_);
            for (auto it = group_commit_waiters_.begin(); it != group_commit_waiters_.end();) {
                const lsn_t completion_lsn = (*it)->require_sync ? durable : written_lsn;
                if ((*it)->target_lsn <= completion_lsn) {
                    if (*it == waiter) own_done = true;
                    (*it)->state = CommitWaiter::State::Done;
                    notify.push_back(*it);
                    it = group_commit_waiters_.erase(it);
                } else {
                    ++it;
                }
            }
            // Rotate only after this leader's original target is covered. This
            // bounds a single commit call's disk tenure while FIFO chooses the
            // next owner among every still-uncovered durability obligation.
            if (own_done && !group_commit_waiters_.empty()) {
                promoted = group_commit_waiters_.front();
                promoted->state = CommitWaiter::State::Promoted;
                notify.push_back(promoted);
                if (metrics_enabled) metrics->record_leader_rotation();
            } else if (group_commit_waiters_.empty()) {
                group_commit_leader_active_ = false;
            }
        }
        if (metrics_enabled) metrics->record_completed_batch(notify.size() - (promoted ? 1 : 0));
        for (const auto& pending : notify) pending->cv.notify_one();
        if (own_done) {
            if (metrics_enabled) metrics->record_leader_tenure(batches);
            return;
        }
    }
}

void LogManager::flush_buffer(bool sync) {
    WalFlushMetrics* const metrics = wal_flush_metrics_;
    const bool metrics_enabled = metrics != nullptr && metrics->enabled();
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
                    const lsn_t durable_before = durable_lsn_.load(std::memory_order_acquire);
                    if (metrics_enabled) metrics->record_physical_flush_iteration();
                    const auto fsync_begin = std::chrono::steady_clock::now();
                    disk_manager_->fsync_log();
                    const auto fsync_elapsed = std::chrono::steady_clock::now() - fsync_begin;
                    recent_fdatasync_ns_.store(static_cast<uint64_t>(
                                                  std::chrono::duration_cast<std::chrono::nanoseconds>(fsync_elapsed)
                                                      .count()),
                                              std::memory_order_release);
                    fdatasync_observation_sequence_.fetch_add(1, std::memory_order_release);
                    LogSlowWalFdatasync(fsync_elapsed, 0, target_lsn, durable_before,
                                        durability_mode_ == DurabilityMode::STRICT);
                    if (metrics_enabled) {
                        metrics->record_fdatasync(static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(fsync_elapsed)
                                .count()));
                    }
                    lsn_t durable = durable_lsn_.load(std::memory_order_acquire);
                    while (durable < target_lsn &&
                           !durable_lsn_.compare_exchange_weak(durable, target_lsn, std::memory_order_release,
                                                               std::memory_order_acquire)) {
                    }
                    if (metrics_enabled) {
                        metrics->record_durable_lag(target_lsn, durable_before,
                                                    durable_lsn_.load(std::memory_order_acquire));
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
        run_group_commit_test_hook("flush_buffer_after_swap");
        bool write_succeeded = false;
        lsn_t durable_before = INVALID_LSN;
        try {
            durable_before = durable_lsn_.load(std::memory_order_acquire);
            if (metrics_enabled) metrics->record_physical_flush_iteration();
            // This point is inside the write failure cleanup region: a
            // test-injected pwrite failure therefore exercises the same
            // waiter fanout and retry semantics as DiskManager::write_log.
            run_group_commit_test_hook("flush_buffer_before_pwrite");
            const auto pwrite_begin = metrics_enabled ? std::chrono::steady_clock::now()
                                                       : std::chrono::steady_clock::time_point{};
            disk_manager_->write_log(flushing_buffer_->buffer_.data(), bytes);
            write_succeeded = true;
            if (metrics_enabled) {
                metrics->record_pwrite(static_cast<uint64_t>(bytes), static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - pwrite_begin)
                        .count()));
            }
            if (sync) {
                const auto fsync_begin = std::chrono::steady_clock::now();
                disk_manager_->fsync_log();
                const auto fsync_elapsed = std::chrono::steady_clock::now() - fsync_begin;
                recent_fdatasync_ns_.store(static_cast<uint64_t>(
                                              std::chrono::duration_cast<std::chrono::nanoseconds>(fsync_elapsed)
                                                  .count()),
                                          std::memory_order_release);
                fdatasync_observation_sequence_.fetch_add(1, std::memory_order_release);
                LogSlowWalFdatasync(fsync_elapsed, bytes, target_lsn, durable_before,
                                    durability_mode_ == DurabilityMode::STRICT);
                if (metrics_enabled) {
                    metrics->record_fdatasync(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(fsync_elapsed)
                            .count()));
                }
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(latch_);
            if (write_succeeded) {
                log_file_offset_ += bytes;
                persist_lsn_.store(target_lsn, std::memory_order_release);
                flushing_buffer_->offset_ = 0;
                flushing_buffer_->shrink_after_flush();
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
            flushing_buffer_->shrink_after_flush();
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
            if (metrics_enabled) {
                metrics->record_durable_lag(target_lsn, durable_before,
                                            durable_lsn_.load(std::memory_order_acquire));
            }
        }
        buffer_cv_.notify_all();
        return;
    }
}

void LogManager::prepare_existing_log() {
    std::unique_lock<std::mutex> binding_lock(index_binding_latch_);
    std::lock_guard<std::mutex> lock(latch_);
    log_buffer_->offset_ = 0;
    flushing_buffer_->offset_ = 0;
    flushing_in_progress_ = false;
    log_buffer_->shrink_after_flush();
    flushing_buffer_->shrink_after_flush();
    std::fill(log_buffer_->buffer_.begin(), log_buffer_->buffer_.end(), 0);
    index_bindings_.clear();

    prepared_manifest_ = read_restart_manifest();
    if (prepared_manifest_.malformed) {
        throw InternalError("malformed segmented WAL restart manifest");
    }
    if (prepared_manifest_.segmented_wal) {
        disk_manager_->configure_segmented_wal(prepared_manifest_.wal_generation, prepared_manifest_.restart_segment,
                                               static_cast<int64_t>(prepared_manifest_.wal_segment_bytes));
        if (!disk_manager_->is_file(disk_manager_->wal_segment_name(prepared_manifest_.restart_segment))) {
            throw InternalError("segmented WAL manifest root segment is missing");
        }
        disk_manager_->sync_segmented_wal_directory();
    } else {
        disk_manager_->configure_legacy_wal();
    }
    prepared_file_size_ = disk_manager_->get_log_file_size();
    const char* segmented_override = std::getenv("RMDB_WAL_SEGMENTED");
    const bool default_segmented = segmented_override != nullptr && std::strcmp(segmented_override, "1") == 0;
    if (!prepared_manifest_.segmented_wal && prepared_file_size_ == 0 && prepared_manifest_.checkpoint_offset == 0 &&
        default_segmented) {
        disk_manager_->configure_segmented_wal(0, 0);
        disk_manager_->ensure_segmented_wal_root();
        prepared_manifest_.segmented_wal = true;
        prepared_manifest_.wal_generation = 0;
        prepared_manifest_.wal_segment_bytes = DiskManager::kWalSegmentBytes;
        prepared_manifest_.restart_segment = 0;
        prepared_manifest_.restart_offset = 0;
        prepared_manifest_.next_lsn = 0;
        write_restart_manifest(prepared_manifest_);
        prepared_file_size_ = disk_manager_->get_log_file_size();
    }
    prepared_restart_offset_ = 0;
    prepared_restart_rejected_ = false;
    if (prepared_manifest_.checkpoint_offset > 0) {
        constexpr size_t kEmptyCheckpointBytes = LOG_HEADER_SIZE + sizeof(size_t);
        std::vector<char> bytes(kEmptyCheckpointBytes);
        const int64_t offset = prepared_manifest_.checkpoint_offset;
        if (offset <= prepared_file_size_ - static_cast<int64_t>(kEmptyCheckpointBytes) &&
            disk_manager_->read_log_chunk(bytes.data(), static_cast<int>(bytes.size()), offset) ==
                static_cast<int>(bytes.size()) &&
            IsLegalEmptyCheckpoint(bytes.data(), bytes.size())) {
            prepared_restart_offset_ = offset;
        } else {
            prepared_restart_rejected_ = true;
        }
    }
    startup_prepared_ = true;
}

int64_t LogManager::prepared_restart_offset() const {
    std::lock_guard<std::mutex> lock(latch_);
    return startup_prepared_ ? prepared_restart_offset_ : 0;
}

bool LogManager::startup_is_prepared() const {
    std::lock_guard<std::mutex> lock(latch_);
    return startup_prepared_;
}

void LogManager::finalize_existing_log(int64_t accepted_end_offset, lsn_t max_lsn,
                                       const std::vector<std::pair<std::string, uint64_t>>& index_bindings) {
    std::unique_lock<std::mutex> binding_lock(index_binding_latch_);
    std::lock_guard<std::mutex> lock(latch_);
    if (!startup_prepared_ || accepted_end_offset < 0 || accepted_end_offset > prepared_file_size_) {
        throw InternalError("invalid WAL startup finalization; WAL retained");
    }
    // This is the sole truncation point: analyze has already semantically
    // validated every complete record up to accepted_end_offset.
    if (accepted_end_offset < prepared_file_size_) disk_manager_->truncate_log_to(accepted_end_offset);
    log_file_offset_ = accepted_end_offset;
    persist_lsn_ = max_lsn;
    durable_lsn_.store(max_lsn, std::memory_order_release);
    global_lsn_.store(std::max(max_lsn == INVALID_LSN ? 0 : max_lsn + 1, prepared_manifest_.next_lsn));
    index_bindings_.clear();
    const uint64_t epoch = wal_epoch_.load(std::memory_order_acquire);
    for (const auto& [name, generation] : index_bindings) index_bindings_[name] = IndexBinding{generation, epoch};
    disk_manager_->SetLogOffset(accepted_end_offset);
    if (prepared_restart_rejected_) {
        prepared_manifest_.checkpoint_offset = 0;
        write_restart_manifest(prepared_manifest_);
    }
    startup_prepared_ = false;
}

void LogManager::initialize_from_existing_log() {
    // Match the binding-sensitive append order. Startup is normally
    // single-threaded, but keeping one order makes later reuse safe too.
    std::unique_lock<std::mutex> binding_lock(index_binding_latch_);
    std::lock_guard<std::mutex> lock(latch_);
    log_buffer_->offset_ = 0;
    flushing_buffer_->offset_ = 0;
    flushing_in_progress_ = false;
    log_buffer_->shrink_after_flush();
    flushing_buffer_->shrink_after_flush();
    std::fill(log_buffer_->buffer_.begin(), log_buffer_->buffer_.end(), 0);
    index_bindings_.clear();
    // The legacy compatibility wrapper performs its own complete startup
    // scan. Do not let a prior prepare_existing_log() leak its checkpoint
    // decision into a later direct RecoveryManager user of this same object.
    startup_prepared_ = false;
    prepared_restart_offset_ = 0;
    prepared_restart_rejected_ = false;
    prepared_file_size_ = 0;

    RestartManifest manifest = read_restart_manifest();
    if (manifest.malformed) {
        throw InternalError("malformed segmented WAL restart manifest");
    }
    if (manifest.segmented_wal) {
        disk_manager_->configure_segmented_wal(manifest.wal_generation, manifest.restart_segment,
                                               static_cast<int64_t>(manifest.wal_segment_bytes));
        if (!disk_manager_->is_file(disk_manager_->wal_segment_name(manifest.restart_segment))) {
            throw InternalError("segmented WAL manifest root segment is missing");
        }
        disk_manager_->sync_segmented_wal_directory();
    } else {
        disk_manager_->configure_legacy_wal();
    }

    int64_t file_size = disk_manager_->get_log_file_size();
    // A newly created, empty legacy db.log may opt into v2 at first open. A
    // non-empty legacy WAL is never migrated in this batch.
    const char* segmented_override = std::getenv("RMDB_WAL_SEGMENTED");
    // Batch 1 has no clean-checkpoint generation switch yet, so production
    // remains legacy until the complete manifest/reclaim protocol lands.
    const bool default_segmented = segmented_override != nullptr && std::strcmp(segmented_override, "1") == 0;
    if (!manifest.segmented_wal && file_size == 0 && manifest.checkpoint_offset == 0 && default_segmented) {
        disk_manager_->configure_segmented_wal(0, 0);
        disk_manager_->ensure_segmented_wal_root();
        manifest.segmented_wal = true;
        manifest.wal_generation = 0;
        manifest.wal_segment_bytes = DiskManager::kWalSegmentBytes;
        manifest.restart_segment = 0;
        manifest.restart_offset = 0;
        manifest.checkpoint_offset = 0;
        manifest.next_lsn = 0;
        write_restart_manifest(manifest);
        file_size = disk_manager_->get_log_file_size();
    }
    if (file_size <= 0) {
        log_file_offset_ = 0;
        persist_lsn_ = INVALID_LSN;
        durable_lsn_.store(INVALID_LSN, std::memory_order_release);
        global_lsn_.store(manifest.segmented_wal ? manifest.next_lsn : 0);
        if (manifest.checkpoint_offset != 0) {
            manifest.checkpoint_offset = 0;
            write_restart_manifest(manifest);
        }
        return;
    }

    int64_t scan_start_offset = 0;
    if (manifest.checkpoint_offset > 0) {
        constexpr size_t kEmptyCheckpointBytes = LOG_HEADER_SIZE + sizeof(size_t);
        const int64_t restart_offset = manifest.checkpoint_offset;
        std::vector<char> checkpoint_bytes(kEmptyCheckpointBytes);
        if (restart_offset <= file_size - static_cast<int64_t>(kEmptyCheckpointBytes) &&
            disk_manager_->read_log_chunk(checkpoint_bytes.data(), static_cast<int>(checkpoint_bytes.size()),
                                          restart_offset) == static_cast<int>(checkpoint_bytes.size()) &&
            IsLegalEmptyCheckpoint(checkpoint_bytes.data(), checkpoint_bytes.size())) {
            scan_start_offset = restart_offset;
        }
    }
    const bool rejected_restart_offset = manifest.checkpoint_offset > 0 && scan_start_offset == 0;

    // Stream the file once instead of issuing a stat plus a pread per record:
    // this runs before recovery on the full retained WAL, so it used to pay the
    // same read amplification analyze did.
    const auto scan_begin = std::chrono::steady_clock::now();
    lsn_t max_lsn = INVALID_LSN;
    uint64_t record_count = 0;
    std::unordered_map<std::string, IndexBinding> scanned_bindings;
    const uint64_t epoch = wal_epoch_.load(std::memory_order_acquire);
    WalReader reader(disk_manager_, scan_start_offset, file_size);
    WalRecordView record;
    while (reader.next(&record)) {
        if (record.log_type == LogType::INDEX_SMO) {
            IndexSmoWalView smo;
            if (!ParseIndexSmoWal(record, &smo)) {
                throw InternalError("corrupt complete INDEX_SMO WAL record at offset " + std::to_string(record.offset) +
                                    "; WAL retained");
            }
        } else if (record.log_type == LogType::INDEX_BIND) {
            std::string_view name;
            uint64_t generation = 0;
            if (!ParseIndexBindWal(record, &name, &generation)) {
                throw InternalError("corrupt complete INDEX_BIND WAL record at offset " +
                                    std::to_string(record.offset) + "; WAL retained");
            }
            scanned_bindings[std::string(name)] = IndexBinding{generation, epoch};
        }
        max_lsn = std::max(max_lsn, record.lsn);
        ++record_count;
    }
    const int64_t offset = reader.next_offset();
    LOG_INFO("wal startup scan: %llu records, [%lld,%lld), %llu preads, %lld ms",
             static_cast<unsigned long long>(record_count), static_cast<long long>(scan_start_offset),
             static_cast<long long>(offset), static_cast<unsigned long long>(reader.read_count()),
             static_cast<long long>(
                 std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - scan_begin)
                     .count()));

    log_file_offset_ = offset;
    persist_lsn_ = max_lsn;
    durable_lsn_.store(max_lsn, std::memory_order_release);
    const lsn_t scanned_next_lsn = max_lsn == INVALID_LSN ? 0 : max_lsn + 1;
    global_lsn_.store(std::max(scanned_next_lsn, manifest.next_lsn));
    index_bindings_.swap(scanned_bindings);
    if (offset < file_size) {
        disk_manager_->truncate_log_to(offset);
    }
    disk_manager_->SetLogOffset(offset);
    if (rejected_restart_offset) {
        // RecoveryManager reads the same manifest after this method. Publish
        // the fail-safe decision so it cannot independently accept a boundary
        // that startup rejected (for example a non-empty CHECKPOINT).
        manifest.checkpoint_offset = 0;
        write_restart_manifest(manifest);
    }
}

void LogManager::reset_log(lsn_t next_lsn) {
    // Ensure active WAL is written and durable before truncation.
    flush_log_to_disk_with_sync();
    std::unique_lock<std::mutex> lock(latch_);
    // flush_buffer releases latch_ before its pwrite/fsync, so holding latch_
    // is not enough. Truncating while a write is in flight would let that
    // writer append previous-epoch bytes after the truncation point and move
    // log_file_offset_/persist_lsn_ backwards; recovery would then replay them.
    buffer_cv_.wait(lock, [this] { return !flushing_in_progress_; });
    assert(log_buffer_->offset_ == 0 && flushing_buffer_->offset_ == 0);
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
    wal_epoch_.fetch_add(1, std::memory_order_acq_rel);
}

void LogManager::write_restart_manifest(const RestartManifest& manifest) {
    RestartManifest effective = manifest;
    if (disk_manager_ != nullptr && disk_manager_->wal_is_segmented()) {
        effective.segmented_wal = true;
        effective.wal_generation = disk_manager_->wal_generation();
        effective.wal_segment_bytes = static_cast<uint64_t>(disk_manager_->wal_segment_bytes());
        if (effective.checkpoint_offset < 0) throw InternalError("negative segmented WAL restart offset");
        effective.restart_segment = static_cast<uint64_t>(effective.checkpoint_offset / disk_manager_->wal_segment_bytes());
        effective.restart_offset = static_cast<uint64_t>(effective.checkpoint_offset % disk_manager_->wal_segment_bytes());
        effective.next_lsn = std::max(effective.next_lsn, global_lsn_.load(std::memory_order_acquire));
    }
    const std::string temp_name = std::string(RESTART_FILE_NAME) + ".tmp";
    std::ofstream restart_file(temp_name, std::ios::trunc);
    if (!restart_file.is_open()) {
        throw UnixError();
    }
    // 第一行保持裸偏移，只为让旧读者（`>> offset`）继续可读。
    restart_file << effective.checkpoint_offset << '\n';
    if (effective.segmented_wal) {
        RestartManifest v2 = effective;
        if (v2.wal_segment_bytes != static_cast<uint64_t>(DiskManager::kWalSegmentBytes)) {
            throw InternalError("segmented WAL manifest has unsupported segment size");
        }
        restart_file << RestartManifestV2Payload(v2) << "checksum=" << RestartManifestV2Checksum(v2) << '\n';
    } else {
        restart_file << "next_timestamp=" << effective.next_timestamp << '\n'
                     << "next_txn_id=" << effective.next_txn_id << '\n';
    }
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

RestartManifest LogManager::read_restart_manifest() const {
    RestartManifest manifest;
    std::ifstream restart_file(RESTART_FILE_NAME);
    if (!restart_file.is_open()) {
        return manifest;
    }

    int64_t checkpoint_offset = 0;
    if (!(restart_file >> checkpoint_offset) || checkpoint_offset < 0) {
        // 无法解析出偏移的清单整体不可信：偏移错了会让恢复从错误的位置开始扫描，
        // 所以此时连计数器也不采纳，全部退回“字段缺失”的安全默认值。
        manifest.malformed = true;
        return manifest;
    }
    manifest.checkpoint_offset = checkpoint_offset;

    // 每一项都独立解析：将来新增的 legacy 键不会让旧字段读不出来，无法识别的键被忽略。
    // 负值一律当作缺失——计数器只可能单调增长，负数只能来自损坏的文件。
    std::unordered_map<std::string, std::string> entries;
    std::string entry;
    while (restart_file >> entry) {
        const size_t separator = entry.find('=');
        if (separator == std::string::npos) {
            continue;
        }
        const std::string key = entry.substr(0, separator);
        if (!entries.emplace(key, entry.substr(separator + 1)).second) {
            manifest.malformed = true;
            return manifest;
        }
    }
    const auto format = entries.find("wal_format");
    static const std::unordered_set<std::string> kV2OnlyKeys = {
        "wal_generation", "segment_bytes", "restart_segment", "restart_offset", "next_lsn", "checksum"};
    if (format == entries.end()) {
        for (const auto& [key, value] : entries) {
            (void)value;
            if (kV2OnlyKeys.count(key) != 0) {
                manifest.malformed = true;
                return manifest;
            }
        }
    }
    if (format != entries.end()) {
        manifest.segmented_wal = true;
        if (format->second != "segmented-v2") {
            manifest.malformed = true;
            return manifest;
        }
        const auto parse_unsigned = [&](const char* key, uint64_t* out) {
            const auto found = entries.find(key);
            if (found == entries.end() || found->second.empty() ||
                found->second.find_first_not_of("0123456789") != std::string::npos) {
                return false;
            }
            try {
                *out = std::stoull(found->second);
                return true;
            } catch (const std::exception&) {
                return false;
            }
        };
        uint64_t checksum = 0;
        uint64_t next_lsn = 0;
        uint64_t next_timestamp = 0;
        uint64_t next_txn_id = 0;
        if (!parse_unsigned("wal_generation", &manifest.wal_generation) ||
            !parse_unsigned("segment_bytes", &manifest.wal_segment_bytes) ||
            !parse_unsigned("restart_segment", &manifest.restart_segment) ||
            !parse_unsigned("restart_offset", &manifest.restart_offset) || !parse_unsigned("next_timestamp", &next_timestamp) ||
            !parse_unsigned("next_txn_id", &next_txn_id) || !parse_unsigned("next_lsn", &next_lsn) ||
            !parse_unsigned("checksum", &checksum) ||
            manifest.wal_segment_bytes != static_cast<uint64_t>(DiskManager::kWalSegmentBytes) ||
            next_timestamp > static_cast<uint64_t>(std::numeric_limits<timestamp_t>::max()) ||
            next_txn_id > static_cast<uint64_t>(std::numeric_limits<txn_id_t>::max()) ||
            next_lsn > static_cast<uint64_t>(std::numeric_limits<lsn_t>::max()) ||
            manifest.restart_offset >= manifest.wal_segment_bytes) {
            manifest.malformed = true;
            return manifest;
        }
        manifest.next_timestamp = static_cast<timestamp_t>(next_timestamp);
        manifest.next_txn_id = static_cast<txn_id_t>(next_txn_id);
        manifest.next_lsn = static_cast<lsn_t>(next_lsn);
        if (manifest.restart_segment > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) /
                                           manifest.wal_segment_bytes ||
            manifest.checkpoint_offset != static_cast<int64_t>(manifest.restart_segment * manifest.wal_segment_bytes +
                                                                manifest.restart_offset) ||
            checksum != RestartManifestV2Checksum(manifest)) {
            manifest.malformed = true;
            return manifest;
        }
        // V2 is deliberately closed: unknown fields make the versioned format
        // invalid rather than letting a typo select a different recovery root.
        static const std::unordered_set<std::string> kV2Keys = {
            "wal_format", "wal_generation", "segment_bytes", "restart_segment", "restart_offset", "next_lsn", "next_timestamp",
            "next_txn_id", "checksum"};
        for (const auto& [key, value] : entries) {
            (void)value;
            if (kV2Keys.count(key) == 0) {
                manifest.malformed = true;
                return manifest;
            }
        }
    }
    for (const auto& [key, text] : entries) {
        int64_t value = 0;
        try { value = std::stoll(text); } catch (const std::exception&) { continue; }
        if (value < 0) continue;
        if (key == "next_timestamp") {
            manifest.next_timestamp = static_cast<timestamp_t>(value);
        } else if (key == "next_txn_id") {
            manifest.next_txn_id = static_cast<txn_id_t>(value);
        }
    }
    return manifest;
}

void LogManager::write_restart_offset(int64_t checkpoint_offset) {
    RestartManifest manifest;
    manifest.checkpoint_offset = checkpoint_offset;
    write_restart_manifest(manifest);
}

int64_t LogManager::read_restart_offset() const {
    return read_restart_manifest().checkpoint_offset;
}
