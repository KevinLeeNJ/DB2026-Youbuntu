/* Default-off, allocation-free abort observability. */
#pragma once

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>

#include "transaction/txn_defs.h"

class TransactionAbortMetrics {
public:
    static constexpr size_t kTableSlots = 64;
    static constexpr size_t kUnknownSlot = kTableSlots;
    static constexpr size_t kOverflowSlot = kTableSlots + 1;
    static constexpr size_t kReasons = 7;
    static constexpr size_t kDetails = 4;
    static constexpr size_t kOrigins = 2;
    static constexpr size_t kModes = 2;
    static constexpr size_t kIsolations = 5;
    static constexpr size_t kOperations = 6;
    struct Config {
        bool enabled{false};
        static bool ParseEnabled(const char* value) noexcept {
            return value != nullptr && std::strcmp(value, "1") == 0;
        }
        static Config FromEnvironment() noexcept {
            return {ParseEnabled(std::getenv("RMDB_ABORT_METRICS"))};
        }
    };
    struct Cell { uint64_t count; };
    struct TableCell { uint64_t runtime_id; uint64_t count; };
    explicit TransactionAbortMetrics(Config config = Config::FromEnvironment()) noexcept : enabled_(config.enabled) {}
    bool enabled() const noexcept { return enabled_; }
    void record(const TransactionAbortException& exception) noexcept {
        if (!enabled_) return;
        const size_t reason = static_cast<size_t>(exception.GetAbortReason());
        const size_t detail = static_cast<size_t>(exception.GetAbortDetail());
        const size_t origin = static_cast<size_t>(exception.GetAbortOrigin());
        const size_t mode = static_cast<size_t>(exception.GetAbortTxnMode());
        const size_t isolation = static_cast<size_t>(exception.GetAbortIsolation());
        const size_t operation = static_cast<size_t>(exception.GetAbortOperation());
        if (reason >= kReasons || detail >= kDetails || origin >= kOrigins || mode >= kModes ||
            isolation >= kIsolations || operation >= kOperations) return;
        const size_t index = (((((origin * kModes + mode) * kIsolations + isolation) * kOperations + operation) *
                               kReasons + reason) * kDetails + detail);
        global_[index].fetch_add(1, std::memory_order_relaxed);
        const uint64_t id = exception.GetTriggeringTableRuntimeId();
        size_t slot = id == 0 ? kUnknownSlot : kOverflowSlot;
        if (id != 0) for (size_t i = 0; i < kTableSlots; ++i) {
            uint64_t current = table_ids_[i].load(std::memory_order_acquire);
            for (;;) {
                if (current == id) { slot = i; break; }
                if (current != 0) break;
                if (table_ids_[i].compare_exchange_weak(current, id, std::memory_order_release,
                                                        std::memory_order_acquire)) { slot = i; break; }
                // `current` has been refreshed by the failed CAS.  Re-check it
                // before considering another slot so same-id racers converge.
            }
            if (slot != kOverflowSlot) break;
        }
        // id is release-published before this release count increment; readers
        // acquire the count before consuming id, preventing id=0 for a real cell.
        table_counts_[slot * kReasons * kDetails + reason * kDetails + detail].fetch_add(1, std::memory_order_release);
    }
    Cell snapshot(AbortOrigin origin, AbortTxnMode mode, IsolationLevel isolation, AbortOperation operation,
                  AbortReason reason, AbortDetail detail) const noexcept {
        if (static_cast<size_t>(origin) >= kOrigins || static_cast<size_t>(mode) >= kModes ||
            static_cast<size_t>(isolation) >= kIsolations || static_cast<size_t>(operation) >= kOperations ||
            static_cast<size_t>(reason) >= kReasons || static_cast<size_t>(detail) >= kDetails) return {};
        const size_t index = (((((static_cast<size_t>(origin) * kModes + static_cast<size_t>(mode)) * kIsolations +
                                 static_cast<size_t>(isolation)) * kOperations + static_cast<size_t>(operation)) *
                               kReasons + static_cast<size_t>(reason)) * kDetails + static_cast<size_t>(detail));
        return {global_[index].load(std::memory_order_relaxed)};
    }
    TableCell table_snapshot(size_t slot, AbortReason reason, AbortDetail detail) const noexcept {
        if (slot > kOverflowSlot || static_cast<size_t>(reason) >= kReasons || static_cast<size_t>(detail) >= kDetails)
            return {};
        const uint64_t count = table_counts_[slot * kReasons * kDetails + static_cast<size_t>(reason) * kDetails +
                                              static_cast<size_t>(detail)].load(std::memory_order_acquire);
        return {slot < kTableSlots && count != 0 ? table_ids_[slot].load(std::memory_order_acquire) : 0, count};
    }
private:
    bool enabled_;
    std::array<std::atomic<uint64_t>, kOrigins * kModes * kIsolations * kOperations * kReasons * kDetails> global_{};
    std::array<std::atomic<uint64_t>, kTableSlots> table_ids_{};
    std::array<std::atomic<uint64_t>, (kTableSlots + 2) * kReasons * kDetails> table_counts_{};
};
