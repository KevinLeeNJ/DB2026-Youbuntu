#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "common/common.h"

// Process-wide, cumulative transaction timing and contention observations.
// The disabled path is a single predictable branch. Enabled observations use
// fixed storage and relaxed atomics; they never allocate or retain Transaction
// pointers across transaction lifetimes.
class TransactionPhaseMetrics {
public:
    static constexpr size_t kHistogramBuckets = 12;

    enum class Phase : uint8_t {
        ExecBatchWall,
        RecordLockWait,
        CommitPrepareSortValidate,
        CommitWalEnqueue,
        CommitWalCoverageWait,
        CommitTupleFinalize,
        CommitFrontierWait,
        CommitRelease,
        CommitOwnerCleanup,
        AbortWal,
        AbortHeapUndo,
        AbortIndexUndo,
        AbortSsiCleanup,
        AbortRelease,
        WaitForGraphBuild,
        Count,

        // Source-compatible names for focused tests and embedded callers that
        // predate the Phase-0 split.
        CommitWalWait = CommitWalCoverageWait,
        TuplePublicationWork = CommitTupleFinalize,
        FrontierWait = CommitFrontierWait,
        LockReleaseWork = CommitRelease,
    };

    struct Snapshot {
        uint64_t count{};
        uint64_t elapsed_ns{};
        uint64_t max_ns{};
        std::array<uint64_t, kHistogramBuckets> histogram{};
    };

    struct ValueSnapshot {
        uint64_t sum{};
        uint64_t max{};
    };

    struct WaitForGraphSnapshot {
        Snapshot build{};
        ValueSnapshot shards{};
        ValueSnapshot queues{};
        ValueSnapshot edges{};
    };

    struct OwnerConflictSnapshot {
        uint64_t commit_cleanup_terminals{};
        uint64_t abort_cleanup_terminals{};
        uint64_t observer_count{};
        Snapshot observation_to_cleanup_terminal{};
    };

    struct ReadOnlyWalSnapshot {
        uint64_t inferred_successful_begin_commit_pairs{};
    };

    static bool ParseEnabled(const char* value) noexcept {
        return value != nullptr && value[0] == '1' && value[1] == '\0';
    }
    static bool EnabledFromEnvironment() noexcept {
        return ParseEnabled(std::getenv("RMDB_TXN_PHASE_METRICS"));
    }

    explicit TransactionPhaseMetrics(bool enabled = EnabledFromEnvironment()) noexcept : enabled_(enabled) {}

    bool enabled() const noexcept {
        return enabled_;
    }

    Snapshot snapshot(Phase phase) const noexcept {
        return snapshot_counter(counters_[static_cast<size_t>(phase)]);
    }

    WaitForGraphSnapshot wait_for_graph_snapshot() const noexcept {
        return {snapshot(Phase::WaitForGraphBuild), snapshot_value(wait_for_graph_shards_),
                snapshot_value(wait_for_graph_queues_), snapshot_value(wait_for_graph_edges_)};
    }

    OwnerConflictSnapshot owner_conflict_snapshot() const noexcept {
        return {owner_commit_cleanup_terminals_.load(std::memory_order_relaxed),
                owner_abort_cleanup_terminals_.load(std::memory_order_relaxed),
                owner_observer_count_.load(std::memory_order_relaxed),
                snapshot_counter(owner_observation_to_cleanup_terminal_)};
    }

    ReadOnlyWalSnapshot read_only_wal_snapshot() const noexcept {
        return {inferred_successful_begin_commit_pairs_.load(std::memory_order_relaxed)};
    }

    void record(Phase phase, uint64_t elapsed_ns) noexcept {
        if (!enabled_) {
            return;
        }
        record_counter(counters_[static_cast<size_t>(phase)], elapsed_ns);
    }

    void record_wait_for_graph(uint64_t elapsed_ns, uint64_t shards, uint64_t queues, uint64_t edges) noexcept {
        if (!enabled_) {
            return;
        }
        record_counter(counters_[static_cast<size_t>(Phase::WaitForGraphBuild)], elapsed_ns);
        record_value(wait_for_graph_shards_, shards);
        record_value(wait_for_graph_queues_, queues);
        record_value(wait_for_graph_edges_, edges);
    }

    void record_owner_cleanup_terminal(bool committed, uint64_t observer_count,
                                       uint64_t first_observation_ns) noexcept {
        if (!enabled_ || observer_count == 0) {
            return;
        }
        const uint64_t terminal_ns = monotonic_ns();
        const uint64_t latency_ns = terminal_ns >= first_observation_ns ? terminal_ns - first_observation_ns : 0;
        owner_observer_count_.fetch_add(observer_count, std::memory_order_relaxed);
        if (committed) {
            owner_commit_cleanup_terminals_.fetch_add(1, std::memory_order_relaxed);
        } else {
            owner_abort_cleanup_terminals_.fetch_add(1, std::memory_order_relaxed);
        }
        record_counter(owner_observation_to_cleanup_terminal_, latency_ns);
    }

    void record_inferred_successful_begin_commit_pair() noexcept {
        if (!enabled_) {
            return;
        }
        inferred_successful_begin_commit_pairs_.fetch_add(1, std::memory_order_relaxed);
    }

    class Scope {
    public:
        Scope() noexcept = default;
        Scope(TransactionPhaseMetrics* metrics, Phase phase) noexcept : metrics_(metrics), phase_(phase) {
            if (metrics_ != nullptr && metrics_->enabled_) {
                begin_ = Clock::now();
                active_ = true;
            }
        }
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;
        Scope(Scope&&) = delete;
        Scope& operator=(Scope&&) = delete;
        void Stop() noexcept {
            if (active_ && !stopped_) {
                elapsed_ = elapsed_ns(begin_);
                stopped_ = true;
            }
        }
        void Finish() noexcept {
            Stop();
            if (metrics_ != nullptr && stopped_ && !finished_) {
                metrics_->record(phase_, elapsed_);
                finished_ = true;
            }
        }
        uint64_t elapsed() const noexcept {
            return elapsed_;
        }
        ~Scope() noexcept {
            Finish();
        }

    private:
        using Clock = std::chrono::steady_clock;
        static uint64_t elapsed_ns(Clock::time_point begin) noexcept {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin).count());
        }
        TransactionPhaseMetrics* metrics_{nullptr};
        Phase phase_{Phase::ExecBatchWall};
        Clock::time_point begin_{};
        uint64_t elapsed_{};
        bool active_{false};
        bool stopped_{false};
        bool finished_{false};
    };

    Scope scope(Phase phase) noexcept {
        return Scope(this, phase);
    }

private:
    struct alignas(64) Counter {
        std::atomic<uint64_t> count{0};
        std::atomic<uint64_t> elapsed_ns{0};
        std::atomic<uint64_t> max_ns{0};
        std::array<std::atomic<uint64_t>, kHistogramBuckets> histogram{};
    };

    struct alignas(64) ValueCounter {
        std::atomic<uint64_t> sum{0};
        std::atomic<uint64_t> max{0};
    };

    static uint64_t monotonic_ns() noexcept {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }

    static size_t histogram_bucket(uint64_t elapsed_ns) noexcept {
        constexpr std::array<uint64_t, kHistogramBuckets - 1> upper_bounds = {
            1000, 4000, 16000, 64000, 256000, 1000000, 4000000, 16000000, 64000000, 256000000, 1000000000,
        };
        for (size_t index = 0; index < upper_bounds.size(); ++index) {
            if (elapsed_ns <= upper_bounds[index]) {
                return index;
            }
        }
        return kHistogramBuckets - 1;
    }

    static void update_max(std::atomic<uint64_t>& maximum, uint64_t value) noexcept {
        uint64_t observed = maximum.load(std::memory_order_relaxed);
        while (observed < value &&
               !maximum.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }

    static void record_counter(Counter& counter, uint64_t elapsed_ns) noexcept {
        counter.count.fetch_add(1, std::memory_order_relaxed);
        counter.elapsed_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        update_max(counter.max_ns, elapsed_ns);
        counter.histogram[histogram_bucket(elapsed_ns)].fetch_add(1, std::memory_order_relaxed);
    }

    static Snapshot snapshot_counter(const Counter& counter) noexcept {
        Snapshot result;
        result.count = counter.count.load(std::memory_order_relaxed);
        result.elapsed_ns = counter.elapsed_ns.load(std::memory_order_relaxed);
        result.max_ns = counter.max_ns.load(std::memory_order_relaxed);
        for (size_t index = 0; index < kHistogramBuckets; ++index) {
            result.histogram[index] = counter.histogram[index].load(std::memory_order_relaxed);
        }
        return result;
    }

    static void record_value(ValueCounter& counter, uint64_t value) noexcept {
        counter.sum.fetch_add(value, std::memory_order_relaxed);
        update_max(counter.max, value);
    }

    static ValueSnapshot snapshot_value(const ValueCounter& counter) noexcept {
        return {counter.sum.load(std::memory_order_relaxed), counter.max.load(std::memory_order_relaxed)};
    }

    bool enabled_{false};
    std::array<Counter, static_cast<size_t>(Phase::Count)> counters_{};
    ValueCounter wait_for_graph_shards_;
    ValueCounter wait_for_graph_queues_;
    ValueCounter wait_for_graph_edges_;
    Counter owner_observation_to_cleanup_terminal_;
    std::atomic<uint64_t> owner_commit_cleanup_terminals_{0};
    std::atomic<uint64_t> owner_abort_cleanup_terminals_{0};
    std::atomic<uint64_t> owner_observer_count_{0};
    std::atomic<uint64_t> inferred_successful_begin_commit_pairs_{0};
};
