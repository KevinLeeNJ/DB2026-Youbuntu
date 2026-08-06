#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

// Process-wide, cumulative phase timing. Timings are observed wall-clock
// durations and are intentionally inclusive/non-additive where documented.
class TransactionPhaseMetrics {
public:
    enum class Phase : uint8_t {
        ExecBatchWall,
        RecordLockWait,
        CommitWalWait,
        TuplePublicationWork,
        FrontierWait,
        LockReleaseWork,
        Count,
    };
    struct Snapshot {
        uint64_t count{};
        uint64_t elapsed_ns{};
        uint64_t max_ns{};
    };

    static bool ParseEnabled(const char* value) noexcept {
        return value != nullptr && value[0] == '1' && value[1] == '\0';
    }
    static bool EnabledFromEnvironment() noexcept {
        return ParseEnabled(std::getenv("RMDB_TXN_PHASE_METRICS"));
    }
    explicit TransactionPhaseMetrics(bool enabled = EnabledFromEnvironment()) noexcept : enabled_(enabled) {}
    bool enabled() const noexcept { return enabled_; }
    Snapshot snapshot(Phase phase) const noexcept {
        const auto& counter = counters_[static_cast<size_t>(phase)];
        return {counter.count.load(std::memory_order_relaxed), counter.elapsed_ns.load(std::memory_order_relaxed),
                counter.max_ns.load(std::memory_order_relaxed)};
    }
    void record(Phase phase, uint64_t elapsed_ns) noexcept {
        if (!enabled_) {
            return;
        }
        auto& counter = counters_[static_cast<size_t>(phase)];
        counter.count.fetch_add(1, std::memory_order_relaxed);
        counter.elapsed_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        uint64_t maximum = counter.max_ns.load(std::memory_order_relaxed);
        while (maximum < elapsed_ns &&
               !counter.max_ns.compare_exchange_weak(maximum, elapsed_ns, std::memory_order_relaxed)) {
        }
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
                elapsed_ = elapsed_ns(begin_); stopped_ = true;
            }
        }
        void Finish() noexcept {
            Stop();
            if (metrics_ != nullptr && stopped_ && !finished_) {
                metrics_->record(phase_, elapsed_); finished_ = true;
            }
        }
        ~Scope() noexcept { Finish(); }
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
    Scope scope(Phase phase) noexcept { return Scope(this, phase); }
private:
    struct alignas(64) Counter {
        std::atomic<uint64_t> count{0};
        std::atomic<uint64_t> elapsed_ns{0};
        std::atomic<uint64_t> max_ns{0};
    };
    bool enabled_{false};
    std::array<Counter, static_cast<size_t>(Phase::Count)> counters_{};
};
