/* Default-off, allocation-free background-preclean attribution. */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>

class BackgroundPrecleanMetrics {
public:
    struct TimingSnapshot {
        uint64_t count{}, elapsed_ns{}, max_ns{};
    };
    struct Snapshot {
        TimingSnapshot foreground_dirty_eviction{}, foreground_dependency_wait{}, foreground_pwrite{},
            foreground_read{};
        uint64_t background_flush_calls{}, background_pages{}, congestion_pauses{}, congestion_throttles{},
            congestion_ramps{};
        uint64_t clean_victims{}, dirty_victim_fallbacks{}, victim_search_scanned{};
        TimingSnapshot background_flush{};
    };
    static bool ParseEnabled(const char* value) noexcept {
        return value != nullptr && std::strcmp(value, "1") == 0;
    }
    static bool EnabledFromEnvironment() noexcept {
        return ParseEnabled(std::getenv("RMDB_BACKGROUND_PRECLEAN_METRICS"));
    }
    explicit BackgroundPrecleanMetrics(bool enabled = EnabledFromEnvironment()) noexcept : enabled_(enabled) {}
    bool enabled() const noexcept {
        return enabled_;
    }
    Snapshot snapshot() const noexcept {
        return {read(foreground_dirty_eviction_),
                read(foreground_dependency_wait_),
                read(foreground_pwrite_),
                read(foreground_read_),
                background_flush_calls_.load(std::memory_order_relaxed),
                background_pages_.load(std::memory_order_relaxed),
                congestion_pauses_.load(std::memory_order_relaxed),
                congestion_throttles_.load(std::memory_order_relaxed),
                congestion_ramps_.load(std::memory_order_relaxed),
                clean_victims_.load(std::memory_order_relaxed),
                dirty_victim_fallbacks_.load(std::memory_order_relaxed),
                victim_search_scanned_.load(std::memory_order_relaxed),
                read(background_flush_)};
    }
    void foreground_dirty_eviction(uint64_t ns) noexcept {
        record(foreground_dirty_eviction_, ns);
    }
    void foreground_dependency_wait(uint64_t ns) noexcept {
        record(foreground_dependency_wait_, ns);
    }
    void foreground_pwrite(uint64_t ns) noexcept {
        record(foreground_pwrite_, ns);
    }
    void foreground_read(uint64_t ns) noexcept {
        record(foreground_read_, ns);
    }
    void background_flush(uint64_t pages, uint64_t ns) noexcept {
        if (!enabled_)
            return;
        background_flush_calls_.fetch_add(1, std::memory_order_relaxed);
        background_pages_.fetch_add(pages, std::memory_order_relaxed);
        record(background_flush_, ns);
    }
    void congestion_pause() noexcept {
        if (enabled_)
            congestion_pauses_.fetch_add(1, std::memory_order_relaxed);
    }
    void congestion_throttle() noexcept {
        if (enabled_)
            congestion_throttles_.fetch_add(1, std::memory_order_relaxed);
    }
    void congestion_ramp() noexcept {
        if (enabled_)
            congestion_ramps_.fetch_add(1, std::memory_order_relaxed);
    }
    void clean_victim() noexcept {
        if (enabled_)
            clean_victims_.fetch_add(1, std::memory_order_relaxed);
    }
    void dirty_victim_fallback() noexcept {
        if (enabled_)
            dirty_victim_fallbacks_.fetch_add(1, std::memory_order_relaxed);
    }
    void victim_search_scanned(uint64_t count) noexcept {
        if (enabled_)
            victim_search_scanned_.fetch_add(count, std::memory_order_relaxed);
    }

private:
    struct alignas(64) Timing {
        std::atomic<uint64_t> count{0}, elapsed_ns{0}, max_ns{0};
    };
    static TimingSnapshot read(const Timing& timing) noexcept {
        return {timing.count.load(std::memory_order_relaxed), timing.elapsed_ns.load(std::memory_order_relaxed),
                timing.max_ns.load(std::memory_order_relaxed)};
    }
    void record(Timing& timing, uint64_t ns) noexcept {
        if (!enabled_)
            return;
        timing.count.fetch_add(1, std::memory_order_relaxed);
        timing.elapsed_ns.fetch_add(ns, std::memory_order_relaxed);
        uint64_t old = timing.max_ns.load(std::memory_order_relaxed);
        while (old < ns &&
               !timing.max_ns.compare_exchange_weak(old, ns, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    }
    bool enabled_{false};
    Timing foreground_dirty_eviction_, foreground_dependency_wait_, foreground_pwrite_, foreground_read_;
    std::atomic<uint64_t> background_flush_calls_{0}, background_pages_{0}, congestion_pauses_{0},
        congestion_throttles_{0}, congestion_ramps_{0};
    std::atomic<uint64_t> clean_victims_{0}, dirty_victim_fallbacks_{0}, victim_search_scanned_{0};
    Timing background_flush_;
};
