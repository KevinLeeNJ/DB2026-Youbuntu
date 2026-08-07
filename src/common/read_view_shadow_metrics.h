#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

class ReadViewShadowMetrics {
public:
    enum class Classification : uint8_t {
        Match,
        UndoMissing,
        VersionMismatch,
        DeleteMismatch,
        PayloadMismatch,
        Count,
    };
    struct Snapshot { uint64_t captures{}, rc_replacements{}; std::array<uint64_t, static_cast<size_t>(Classification::Count)> classification{}; };
    Snapshot snapshot() const noexcept {
        Snapshot s; s.captures = captures_.load(std::memory_order_relaxed); s.rc_replacements = rc_replacements_.load(std::memory_order_relaxed);
        for (size_t i = 0; i < s.classification.size(); ++i) s.classification[i] = classification_[i].load(std::memory_order_relaxed);
        return s;
    }
    void capture() noexcept { captures_.fetch_add(1, std::memory_order_relaxed); }
    void rc_replacement() noexcept { rc_replacements_.fetch_add(1, std::memory_order_relaxed); }
    void classify(Classification c) noexcept { classification_[static_cast<size_t>(c)].fetch_add(1, std::memory_order_relaxed); }
private:
    std::atomic<uint64_t> captures_{0}, rc_replacements_{0};
    std::array<std::atomic<uint64_t>, static_cast<size_t>(Classification::Count)> classification_{};
};
