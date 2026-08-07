/* Default-off, allocation-free WAL flush observability. */
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

class WalFlushMetrics {
public:
    static constexpr size_t kCompletedBatchBuckets = 13;

    struct TimingSnapshot {
        uint64_t count{};
        uint64_t elapsed_ns{};
        uint64_t max_ns{};
    };
    struct Snapshot {
        uint64_t already_covered_fast_paths{};
        uint64_t leader_requests{};
        uint64_t leader_rotations{};
        uint64_t max_batches_per_leader{};
        uint64_t follower_requests{};
        TimingSnapshot follower_wait{};
        TimingSnapshot coalescing_wait{};
        uint64_t physical_flush_iterations{};
        TimingSnapshot pwrite{};
        uint64_t pwrite_bytes{};
        TimingSnapshot fdatasync{};
        std::array<uint64_t, kCompletedBatchBuckets> completed_batch_histogram{};
        uint64_t durable_lag_samples{};
        uint64_t durable_lag_before_sum{};
        uint64_t durable_lag_before_max{};
        uint64_t durable_lag_after_sum{};
        uint64_t durable_lag_after_max{};
    };

    static bool ParseEnabled(const char* value) noexcept {
        return value != nullptr && std::strcmp(value, "1") == 0;
    }
    static bool EnabledFromEnvironment() noexcept { return ParseEnabled(std::getenv("RMDB_WAL_METRICS")); }

    explicit WalFlushMetrics(bool enabled = EnabledFromEnvironment()) noexcept : enabled_(enabled) {}
    bool enabled() const noexcept { return enabled_; }

    Snapshot snapshot() const noexcept {
        Snapshot result;
        result.already_covered_fast_paths = already_covered_fast_paths_.load(std::memory_order_relaxed);
        result.leader_requests = leader_requests_.load(std::memory_order_relaxed);
        result.leader_rotations = leader_rotations_.load(std::memory_order_relaxed);
        result.max_batches_per_leader = max_batches_per_leader_.load(std::memory_order_relaxed);
        result.follower_requests = follower_requests_.load(std::memory_order_relaxed);
        result.follower_wait = snapshot_timing(follower_wait_);
        result.coalescing_wait = snapshot_timing(coalescing_wait_);
        result.physical_flush_iterations = physical_flush_iterations_.load(std::memory_order_relaxed);
        result.pwrite = snapshot_timing(pwrite_);
        result.pwrite_bytes = pwrite_bytes_.load(std::memory_order_relaxed);
        result.fdatasync = snapshot_timing(fdatasync_);
        for (size_t index = 0; index < kCompletedBatchBuckets; ++index) {
            result.completed_batch_histogram[index] = completed_batch_histogram_[index].load(std::memory_order_relaxed);
        }
        result.durable_lag_samples = durable_lag_samples_.load(std::memory_order_relaxed);
        result.durable_lag_before_sum = durable_lag_before_sum_.load(std::memory_order_relaxed);
        result.durable_lag_before_max = durable_lag_before_max_.load(std::memory_order_relaxed);
        result.durable_lag_after_sum = durable_lag_after_sum_.load(std::memory_order_relaxed);
        result.durable_lag_after_max = durable_lag_after_max_.load(std::memory_order_relaxed);
        return result;
    }

    void record_already_covered_fast_path() noexcept { increment(already_covered_fast_paths_); }
    void record_leader_request() noexcept { increment(leader_requests_); }
    void record_leader_rotation() noexcept { increment(leader_rotations_); }
    void record_leader_tenure(uint64_t batches) noexcept { update_max(max_batches_per_leader_, batches); }
    void record_follower_request() noexcept { increment(follower_requests_); }
    void record_follower_wait(uint64_t elapsed_ns) noexcept { record_timing(follower_wait_, elapsed_ns); }
    void record_coalescing_wait(uint64_t elapsed_ns) noexcept { record_timing(coalescing_wait_, elapsed_ns); }
    void record_physical_flush_iteration() noexcept { increment(physical_flush_iterations_); }
    void record_pwrite(uint64_t bytes, uint64_t elapsed_ns) noexcept {
        increment(pwrite_bytes_, bytes);
        record_timing(pwrite_, elapsed_ns);
    }
    void record_fdatasync(uint64_t elapsed_ns) noexcept { record_timing(fdatasync_, elapsed_ns); }
    void record_completed_batch(size_t completed) noexcept {
        increment(completed_batch_histogram_[completed_batch_bucket(completed)]);
    }
    void record_durable_lag(int64_t target_lsn, int64_t before_lsn, int64_t after_lsn) noexcept {
        if (target_lsn < 0 || before_lsn < 0 || after_lsn < 0) {
            return;
        }
        const uint64_t before = target_lsn > before_lsn ? static_cast<uint64_t>(target_lsn - before_lsn) : 0;
        const uint64_t after = target_lsn > after_lsn ? static_cast<uint64_t>(target_lsn - after_lsn) : 0;
        increment(durable_lag_samples_);
        increment(durable_lag_before_sum_, before);
        increment(durable_lag_after_sum_, after);
        update_max(durable_lag_before_max_, before);
        update_max(durable_lag_after_max_, after);
    }

private:
    struct alignas(64) Timing {
        std::atomic<uint64_t> count{0};
        std::atomic<uint64_t> elapsed_ns{0};
        std::atomic<uint64_t> max_ns{0};
    };
    static void increment(std::atomic<uint64_t>& counter, uint64_t value = 1) noexcept {
        counter.fetch_add(value, std::memory_order_relaxed);
    }
    static void update_max(std::atomic<uint64_t>& maximum, uint64_t value) noexcept {
        uint64_t observed = maximum.load(std::memory_order_relaxed);
        while (observed < value &&
               !maximum.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }
    static void record_timing(Timing& timing, uint64_t elapsed_ns) noexcept {
        increment(timing.count);
        increment(timing.elapsed_ns, elapsed_ns);
        update_max(timing.max_ns, elapsed_ns);
    }
    static TimingSnapshot snapshot_timing(const Timing& timing) noexcept {
        return {timing.count.load(std::memory_order_relaxed), timing.elapsed_ns.load(std::memory_order_relaxed),
                timing.max_ns.load(std::memory_order_relaxed)};
    }
    static size_t completed_batch_bucket(size_t completed) noexcept {
        if (completed == 0) return 0;
        if (completed <= 8) return completed;
        if (completed <= 16) return 9;
        if (completed <= 32) return 10;
        if (completed <= 64) return 11;
        return 12;
    }

    bool enabled_{false};
    std::atomic<uint64_t> already_covered_fast_paths_{0};
    std::atomic<uint64_t> leader_requests_{0};
    std::atomic<uint64_t> leader_rotations_{0};
    std::atomic<uint64_t> max_batches_per_leader_{0};
    std::atomic<uint64_t> follower_requests_{0};
    Timing follower_wait_;
    Timing coalescing_wait_;
    std::atomic<uint64_t> physical_flush_iterations_{0};
    Timing pwrite_;
    std::atomic<uint64_t> pwrite_bytes_{0};
    Timing fdatasync_;
    std::array<std::atomic<uint64_t>, kCompletedBatchBuckets> completed_batch_histogram_{};
    std::atomic<uint64_t> durable_lag_samples_{0};
    std::atomic<uint64_t> durable_lag_before_sum_{0};
    std::atomic<uint64_t> durable_lag_before_max_{0};
    std::atomic<uint64_t> durable_lag_after_sum_{0};
    std::atomic<uint64_t> durable_lag_after_max_{0};
};
