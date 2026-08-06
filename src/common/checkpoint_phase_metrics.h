/* Default-off, allocation-free cumulative checkpoint attribution. */
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

class CheckpointPhaseMetrics {
public:
    enum class Timing : uint8_t { DrainAdmissionWait, CutCapture, PageWrite, CleanDataSync, CleanMetaFlush,
                                  FuzzyFinalPublish, ManifestPublish, WalResetReclaim, FuzzyLifetime, Count };
    struct TimingSnapshot { uint64_t count{}, elapsed_ns{}, max_ns{}; };
    struct Snapshot {
        uint64_t clean_attempts{}, clean_successes{}, clean_failures{};
        uint64_t fuzzy_attempts{}, fuzzy_successes{}, fuzzy_failures{}, fuzzy_cancels{};
        uint64_t pages_marked{}, page_write_calls{}, pages_written{}, pages_remaining_max{};
        uint64_t retry_deferrals{}, deadline_deferrals{}, budget_yields_io{}, budget_yields_time{}, zero_progress_yields{};
        std::array<TimingSnapshot, static_cast<size_t>(Timing::Count)> timing{};
    };
    static bool ParseEnabled(const char* v) noexcept { return v != nullptr && v[0] == '1' && v[1] == '\0'; }
    static bool EnabledFromEnvironment() noexcept { return ParseEnabled(std::getenv("RMDB_CHECKPOINT_METRICS")); }
    explicit CheckpointPhaseMetrics(bool enabled = EnabledFromEnvironment()) noexcept : enabled_(enabled) {}
    bool enabled() const noexcept { return enabled_; }
    Snapshot snapshot() const noexcept {
        Snapshot s;
        s.clean_attempts = clean_attempts_.load(std::memory_order_relaxed); s.clean_successes = clean_successes_.load(std::memory_order_relaxed); s.clean_failures = clean_failures_.load(std::memory_order_relaxed);
        s.fuzzy_attempts = fuzzy_attempts_.load(std::memory_order_relaxed); s.fuzzy_successes = fuzzy_successes_.load(std::memory_order_relaxed); s.fuzzy_failures = fuzzy_failures_.load(std::memory_order_relaxed); s.fuzzy_cancels = fuzzy_cancels_.load(std::memory_order_relaxed);
        s.pages_marked = pages_marked_.load(std::memory_order_relaxed); s.page_write_calls = page_write_calls_.load(std::memory_order_relaxed); s.pages_written = pages_written_.load(std::memory_order_relaxed); s.pages_remaining_max = pages_remaining_max_.load(std::memory_order_relaxed);
        s.retry_deferrals = retry_deferrals_.load(std::memory_order_relaxed); s.deadline_deferrals = deadline_deferrals_.load(std::memory_order_relaxed); s.budget_yields_io = budget_yields_io_.load(std::memory_order_relaxed); s.budget_yields_time = budget_yields_time_.load(std::memory_order_relaxed); s.zero_progress_yields = zero_progress_yields_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < s.timing.size(); ++i) {
            s.timing[i] = read(timing_[i]);
        }
        return s;
    }
    void clean_attempt() noexcept { if (enabled_) add(clean_attempts_); } void clean_success() noexcept { if (enabled_) add(clean_successes_); } void clean_failure() noexcept { if (enabled_) add(clean_failures_); }
    void fuzzy_attempt() noexcept { if (enabled_) add(fuzzy_attempts_); } void fuzzy_success() noexcept { if (enabled_) add(fuzzy_successes_); } void fuzzy_failure() noexcept { if (enabled_) add(fuzzy_failures_); } void fuzzy_cancel() noexcept { if (enabled_) add(fuzzy_cancels_); }
    void pages_marked(uint64_t n) noexcept { if (enabled_) add(pages_marked_, n); }
    void page_write(uint64_t written, uint64_t remaining) noexcept { if (!enabled_) return; add(page_write_calls_); add(pages_written_, written); maximum(pages_remaining_max_, remaining); }
    void retry_deferral(bool deadline) noexcept { if (!enabled_) return; add(retry_deferrals_); if (deadline) add(deadline_deferrals_); }
    void budget_yield_io() noexcept { if (enabled_) add(budget_yields_io_); }
    void budget_yield_time() noexcept { if (enabled_) add(budget_yields_time_); }
    void zero_progress_yield() noexcept { if (enabled_) add(zero_progress_yields_); }
    void record(Timing kind, uint64_t elapsed) noexcept { if (!enabled_) return; auto& c = timing_[static_cast<size_t>(kind)]; add(c.count); add(c.elapsed_ns, elapsed); maximum(c.max_ns, elapsed); }
    class Scope {
    public:
        Scope(CheckpointPhaseMetrics* m, Timing k) noexcept : metrics_(m), kind_(k) { if (m != nullptr && m->enabled_) { begin_ = Clock::now(); active_ = true; } }
        ~Scope() noexcept { Finish(); }
        Scope(const Scope&) = delete; Scope& operator=(const Scope&) = delete;
        void Stop() noexcept { if (active_ && !stopped_) { elapsed_ns_ = elapsed(); stopped_ = true; } }
        void Finish() noexcept { Stop(); if (stopped_ && !finished_) { metrics_->record(kind_, elapsed_ns_); finished_ = true; } }
    private:
        using Clock = std::chrono::steady_clock;
        uint64_t elapsed() const noexcept { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin_).count()); }
        CheckpointPhaseMetrics* metrics_; Timing kind_; Clock::time_point begin_{}; uint64_t elapsed_ns_{};
        bool active_{false}, stopped_{false}, finished_{false};
    };
    Scope scope(Timing kind) noexcept { return Scope(this, kind); }
private:
    struct alignas(64) Counter { std::atomic<uint64_t> count{0}, elapsed_ns{0}, max_ns{0}; };
    static void add(std::atomic<uint64_t>& c, uint64_t n = 1) noexcept { c.fetch_add(n, std::memory_order_relaxed); }
    static void maximum(std::atomic<uint64_t>& c, uint64_t n) noexcept { uint64_t old = c.load(std::memory_order_relaxed); while (old < n && !c.compare_exchange_weak(old, n, std::memory_order_relaxed, std::memory_order_relaxed)) {} }
    static TimingSnapshot read(const Counter& c) noexcept { return {c.count.load(std::memory_order_relaxed), c.elapsed_ns.load(std::memory_order_relaxed), c.max_ns.load(std::memory_order_relaxed)}; }
    bool enabled_{false};
    std::atomic<uint64_t> clean_attempts_{0}, clean_successes_{0}, clean_failures_{0};
    std::atomic<uint64_t> fuzzy_attempts_{0}, fuzzy_successes_{0}, fuzzy_failures_{0}, fuzzy_cancels_{0};
    std::atomic<uint64_t> pages_marked_{0}, page_write_calls_{0}, pages_written_{0}, pages_remaining_max_{0};
    std::atomic<uint64_t> retry_deferrals_{0}, deadline_deferrals_{0}, budget_yields_io_{0}, budget_yields_time_{0}, zero_progress_yields_{0};
    std::array<Counter, static_cast<size_t>(Timing::Count)> timing_{};
};
