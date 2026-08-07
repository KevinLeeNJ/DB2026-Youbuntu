/* Shadow-only active transaction read view and status table. */
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <vector>

#include "common/config.h"

enum class ReadViewMode : uint8_t { Off, Shadow };

struct ActiveWriterReadView {
    txn_id_t self_id{INVALID_TXN_ID};
    txn_id_t upper_limit{INVALID_TXN_ID}; // first transaction id not allocated at capture
    std::vector<txn_id_t> active_txn_ids;

    bool contains(txn_id_t txn_id) const noexcept {
        return std::binary_search(active_txn_ids.begin(), active_txn_ids.end(), txn_id);
    }
};

inline ReadViewMode ReadViewModeFromEnvironment() {
    const char* value = std::getenv("RMDB_SI_READVIEW_MODE");
    if (value == nullptr || value[0] == '\0' || (value[0] == 'o' && value[1] == 'f' && value[2] == 'f' && value[3] == '\0')) {
        return ReadViewMode::Off;
    }
    if (value[0] == 's' && value[1] == 'h' && value[2] == 'a' && value[3] == 'd' && value[4] == 'o' &&
        value[5] == 'w' && value[6] == '\0') {
        return ReadViewMode::Shadow;
    }
    throw std::invalid_argument("RMDB_SI_READVIEW_MODE must be off or shadow");
}

// This table is deliberately process-local and shadow-only. Its lock is used
// only when RMDB_SI_READVIEW_MODE=shadow; the legacy transaction path neither
// creates it nor touches it.
class TransactionStatusTable {
public:
    std::shared_ptr<const ActiveWriterReadView> AllocateRegisterAndCapture(std::atomic<txn_id_t>& next_txn_id) {
        std::lock_guard<std::mutex> lock(latch_);
        const txn_id_t id = next_txn_id.fetch_add(1, std::memory_order_relaxed);
        active_.insert(id);
        return CaptureLocked(id, next_txn_id.load(std::memory_order_relaxed));
    }

    std::shared_ptr<const ActiveWriterReadView> RegisterAndCapture(txn_id_t id, txn_id_t upper_limit) {
        std::lock_guard<std::mutex> lock(latch_);
        active_.insert(id);
        return CaptureLocked(id, upper_limit);
    }

    std::shared_ptr<const ActiveWriterReadView> Capture(txn_id_t self_id, txn_id_t upper_limit) const {
        std::lock_guard<std::mutex> lock(latch_);
        return CaptureLocked(self_id, upper_limit);
    }

    void SetCommitting(txn_id_t) noexcept {}
    void SetCommitted(txn_id_t id, timestamp_t) noexcept { RemoveActive(id); }
    void SetAborted(txn_id_t id) noexcept { RemoveActive(id); }
    void RemoveActive(txn_id_t id) noexcept {
        std::lock_guard<std::mutex> lock(latch_);
        active_.erase(id);
    }

private:
    std::shared_ptr<const ActiveWriterReadView> CaptureLocked(txn_id_t self_id, txn_id_t upper_limit) const {
        auto result = std::make_shared<ActiveWriterReadView>();
        result->self_id = self_id;
        result->upper_limit = upper_limit;
        result->active_txn_ids.assign(active_.begin(), active_.end());
        return result;
    }
    mutable std::mutex latch_;
    std::set<txn_id_t> active_;
};
