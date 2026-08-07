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

#include "buffer_pool_manager.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <exception>
#include <limits>
#include <thread>
#include <unordered_set>

#include "minilog.h"
#include "recovery/log_manager.h"

namespace {
constexpr size_t kShardLogLineShards = 4;
constexpr size_t kBase62Uint64Chars = 11;
constexpr size_t kShardEntryMaxChars = 1 + 3 + 4 * kBase62Uint64Chars;
// The 128-byte fixed body allowance plus a <60-byte minilog header stays
// below minilog's 510-byte line buffer even with four maximum-size entries.
static_assert(kShardLogLineShards * kShardEntryMaxChars + 128 + 60 < 510);
static_assert(sizeof("shard-acq bpm schema=base62 entries=a/s/e/m a=sampled_acquisitions "
                     "s=slow_acquisitions e=observed_elapsed_ns m=max_ns "
                     "slow=threshold_proxy_not_exact_contention") -
                  1 + 60 <
              510);

std::string CompactUnsigned(uint64_t value) {
    constexpr char digits[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    char buffer[11];
    size_t size = 0;
    do {
        buffer[size++] = digits[value % 62];
        value /= 62;
    } while (value != 0);
    std::string result;
    result.reserve(size);
    while (size != 0) {
        result += buffer[--size];
    }
    return result;
}

bool IsValidPageId(const PageId& page_id) {
    return page_id.fd >= 0 && page_id.page_no != INVALID_PAGE_ID;
}

constexpr size_t kMinimumDirtyScanFrames = 4096;

uint64_t SplitMix64(uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

size_t DirtyScanFrameBudget(size_t batch_pages) {
    constexpr size_t kMultiplier = 8;
    const size_t max_size = std::numeric_limits<size_t>::max();
    if (batch_pages > max_size / kMultiplier) {
        return max_size;
    }
    return std::max(kMinimumDirtyScanFrames, batch_pages * kMultiplier);
}

} // namespace

std::mutex BufferPoolManager::flush_page_test_hook_latch_;
BufferPoolManager::FlushPageTestHook BufferPoolManager::flush_page_test_hook_;
BufferPoolManager::FlushPageTestHook BufferPoolManager::flush_page_after_write_test_hook_;
BufferPoolManager::FlushPageTestHook BufferPoolManager::flush_batch_before_write_test_hook_;

bool BufferPoolManager::shard_metrics_enabled() const noexcept {
    return shard_read_metrics_.enabled() || shard_write_metrics_.enabled();
}

void BufferPoolManager::log_shard_metrics(uint64_t sequence) const {
    const auto log_direction = [sequence](const char* direction, const ShardAcquisitionMetrics& metrics) {
        if (!metrics.enabled()) {
            return;
        }
        const auto& config = metrics.config();
        LOG_WARN("shard-acq bpm schema=base62 entries=a/s/e/m a=sampled_acquisitions "
                 "s=slow_acquisitions e=observed_elapsed_ns m=max_ns "
                 "slow=threshold_proxy_not_exact_contention");
        for (size_t begin = 0; begin < ShardAcquisitionMetrics::kShardCount; begin += kShardLogLineShards) {
            // Each range has four base-62 a/s/e/m entries in shard order.
            std::string entries;
            for (size_t shard = begin; shard < begin + kShardLogLineShards; ++shard) {
                const auto snapshot = metrics.snapshot(shard);
                entries += " " + CompactUnsigned(snapshot.sampled_acquisitions) + "/" +
                           CompactUnsigned(snapshot.slow_acquisitions) + "/" +
                           CompactUnsigned(snapshot.sampled_elapsed_ns) + "/" +
                           CompactUnsigned(snapshot.sampled_max_ns);
            }
            LOG_WARN("shard-acq bpm seq=%s sample_log2=%u slow_ns=%s direction=%s shard=%zu-%zu%s",
                     CompactUnsigned(sequence).c_str(), static_cast<unsigned>(config.sample_log2),
                     CompactUnsigned(config.slow_ns).c_str(), direction, begin, begin + kShardLogLineShards - 1,
                     entries.c_str());
        }
    };
    log_direction("shared", shard_read_metrics_);
    log_direction("exclusive", shard_write_metrics_);
}

void BufferPoolManager::set_flush_page_test_hook(FlushPageTestHook hook) {
    std::scoped_lock lock{flush_page_test_hook_latch_};
    flush_page_test_hook_ = std::move(hook);
}

void BufferPoolManager::set_flush_page_after_write_test_hook(FlushPageTestHook hook) {
    std::scoped_lock lock{flush_page_test_hook_latch_};
    flush_page_after_write_test_hook_ = std::move(hook);
}

void BufferPoolManager::set_flush_batch_before_write_test_hook(FlushPageTestHook hook) {
    std::scoped_lock lock{flush_page_test_hook_latch_};
    flush_batch_before_write_test_hook_ = std::move(hook);
}

void BufferPoolManager::run_flush_page_test_hook(PageId page_id, Page* page) {
    FlushPageTestHook hook;
    {
        std::scoped_lock lock{flush_page_test_hook_latch_};
        hook = flush_page_test_hook_;
    }
    if (hook) {
        hook(page_id, page);
    }
}

void BufferPoolManager::run_flush_page_after_write_test_hook(PageId page_id, Page* page) {
    FlushPageTestHook hook;
    {
        std::scoped_lock lock{flush_page_test_hook_latch_};
        hook = flush_page_after_write_test_hook_;
    }
    if (hook) {
        hook(page_id, page);
    }
}

void BufferPoolManager::run_flush_batch_before_write_test_hook(PageId page_id, Page* page) {
    FlushPageTestHook hook;
    {
        std::scoped_lock lock{flush_page_test_hook_latch_};
        hook = flush_batch_before_write_test_hook_;
    }
    if (hook) {
        hook(page_id, page);
    }
}

BufferPoolManager::FrameOperationToken::~FrameOperationToken() {
    release();
}

BufferPoolManager::FrameOperationToken::FrameOperationToken(FrameOperationToken&& other) noexcept
    : manager_(other.manager_), generation_(other.generation_) {
    other.manager_ = nullptr;
}

BufferPoolManager::FrameOperationToken&
BufferPoolManager::FrameOperationToken::operator=(FrameOperationToken&& other) noexcept {
    if (this != &other) {
        release();
        manager_ = other.manager_;
        generation_ = other.generation_;
        other.manager_ = nullptr;
    }
    return *this;
}

Page* BufferPoolManager::FrameOperationToken::fetch_page(PageId page_id) const {
    return manager_ == nullptr ? nullptr : manager_->fetch_page_impl(page_id, this);
}

Page* BufferPoolManager::FrameOperationToken::new_page(PageId* page_id) const {
    return manager_ == nullptr ? nullptr : manager_->new_page_impl(page_id, this);
}

void BufferPoolManager::FrameOperationToken::release() noexcept {
    if (manager_ != nullptr) {
        manager_->release_frame_operation(generation_);
        manager_ = nullptr;
    }
}

frame_id_t BufferPoolManager::take_free_frame() {
    if (!recycled_frames_.empty()) {
        const frame_id_t frame_id = recycled_frames_.back();
        recycled_frames_.pop_back();
        return frame_id;
    }
    if (next_unused_frame_ < static_cast<frame_id_t>(pool_size_)) {
        return next_unused_frame_++;
    }
    return INVALID_FRAME_ID;
}

void BufferPoolManager::recycle_frame(frame_id_t frame_id) {
    recycled_frames_.push_back(frame_id);
}

size_t BufferPoolManager::available_frames_locked() const {
    const size_t unused =
        pool_size_ - std::min(pool_size_, static_cast<size_t>(std::max<frame_id_t>(next_unused_frame_, 0)));
    return unused + recycled_frames_.size() + replacer_->Size();
}

bool BufferPoolManager::operation_authorized(const FrameOperationToken* operation, uint64_t generation) const {
    return operation != nullptr && operation->manager_ == this && operation->generation_ == generation;
}

BufferPoolManager::FrameOperationToken BufferPoolManager::acquire_frame_operation(size_t minimum_available_frames) {
    if (minimum_available_frames > pool_size_) {
        throw InternalError("buffer pool frame operation reservation exceeds pool size");
    }
    std::unique_lock lock{latch_};
    frame_operation_cv_.wait(lock, [&] { return !frame_operation_active_.load(std::memory_order_acquire); });
    const uint64_t previous_generation = frame_operation_generation_.load(std::memory_order_acquire);
    if (previous_generation == std::numeric_limits<uint64_t>::max()) {
        throw InternalError("buffer pool frame operation generation exhausted");
    }
    const uint64_t generation = previous_generation + 1;
    frame_operation_generation_.store(generation, std::memory_order_release);
    frame_operation_active_.store(true, std::memory_order_release);

    // Publishing the gate prevents new unauthorized fast hits. Taking every
    // shard exclusively drains hits that observed the previous open gate but
    // had not completed their pin yet. Release each shard immediately so an
    // unpin can make a frame available while the reservation waits below.
    for (size_t shard_index = 0; shard_index < resident_directory_.size(); ++shard_index) {
        ResidentDirectoryShard& shard = resident_directory_[shard_index];
        auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    }
    frame_operation_cv_.wait(lock, [&] { return available_frames_locked() >= minimum_available_frames; });
    return FrameOperationToken(this, generation);
}

void BufferPoolManager::release_frame_operation(uint64_t generation) noexcept {
    {
        std::unique_lock lock{latch_};
        if (!frame_operation_active_.load(std::memory_order_acquire) ||
            generation != frame_operation_generation_.load(std::memory_order_acquire)) {
            return;
        }
        frame_operation_active_.store(false, std::memory_order_release);
    }
    frame_operation_cv_.notify_all();
}

size_t BufferPoolManager::resident_directory_shard_index(PageId page_id) const noexcept {
    static_assert((RESIDENT_DIRECTORY_SHARD_COUNT & (RESIDENT_DIRECTORY_SHARD_COUNT - 1)) == 0,
                  "resident directory shard count must be a power of two");
    const uint64_t fd = static_cast<uint32_t>(page_id.fd);
    const uint64_t page_no = static_cast<uint32_t>(page_id.page_no);
    const uint64_t packed = (fd << 32) | page_no;
    return static_cast<size_t>(SplitMix64(packed)) & (RESIDENT_DIRECTORY_SHARD_COUNT - 1);
}

void BufferPoolManager::install_page_mapping_locked(PageId page_id, frame_id_t frame_id, FrameState state) {
    assert(IsValidPageId(page_id));
    assert(frame_id >= 0 && static_cast<size_t>(frame_id) < pool_size_);
    assert(state == FrameState::LOADING || state == FrameState::VALID);
    Page* page = &pages_[frame_id];
    assert(page->id_ == page_id);

    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    auto resident = shard.entries.find(page_id);
    assert(resident == shard.entries.end() || resident->second == frame_id);
    page_table_.insert_or_assign(page_id, frame_id);
    if (state == FrameState::VALID) {
        std::scoped_lock pin_lock{page->pin_latch_};
        if (page->pin_count_ == 0 && residency_classes_[frame_id] == ResidencyClass::Normal) {
            replacer_->unpin(frame_id);
        }
        page->state_.store(FrameState::VALID, std::memory_order_release);
        shard.entries.insert_or_assign(page_id, frame_id);
    } else {
        shard.entries.erase(page_id);
        page->state_.store(FrameState::LOADING, std::memory_order_release);
    }
}

void BufferPoolManager::set_mapped_frame_state_locked(PageId page_id, frame_id_t frame_id, FrameState state) {
    assert(IsValidPageId(page_id));
    assert(frame_id >= 0 && static_cast<size_t>(frame_id) < pool_size_);
    assert(state == FrameState::VALID || state == FrameState::FLUSHING || state == FrameState::EVICTING);
    auto authoritative = page_table_.find(page_id);
    assert(authoritative != page_table_.end() && authoritative->second == frame_id);
    Page* page = &pages_[frame_id];
    assert(page->id_ == page_id);

    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    auto resident = shard.entries.find(page_id);
    assert(resident == shard.entries.end() || resident->second == frame_id);
    if (state == FrameState::VALID) {
        std::scoped_lock pin_lock{page->pin_latch_};
        if (page->pin_count_ == 0 && residency_classes_[frame_id] == ResidencyClass::Normal) {
            replacer_->unpin(frame_id);
        }
        page->state_.store(FrameState::VALID, std::memory_order_release);
        shard.entries.insert_or_assign(page_id, frame_id);
    } else {
        shard.entries.erase(page_id);
        if (state == FrameState::FLUSHING) {
            // A clean last-unpin may have raced with the caller's first
            // replacer pin while this transition waited for the shard. Pin
            // again under shard -> pin_latch before publishing FLUSHING so
            // the frame cannot become a victim during the write.
            std::scoped_lock pin_lock{page->pin_latch_};
            replacer_->pin(frame_id);
        }
        page->state_.store(state, std::memory_order_release);
    }
}

void BufferPoolManager::erase_page_mapping_locked(PageId page_id, frame_id_t frame_id, FrameState state) {
    assert(IsValidPageId(page_id));
    assert(frame_id >= 0 && static_cast<size_t>(frame_id) < pool_size_);
    assert(state == FrameState::EVICTING || state == FrameState::FREE);

    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    auto resident = shard.entries.find(page_id);
    assert(resident == shard.entries.end() || resident->second == frame_id);
    if (resident != shard.entries.end()) {
        shard.entries.erase(resident);
    }
    auto authoritative = page_table_.find(page_id);
    assert(authoritative != page_table_.end() && authoritative->second == frame_id);
    page_table_.erase(authoritative);
    pages_[frame_id].state_.store(state, std::memory_order_release);
}

bool BufferPoolManager::claim_page_for_eviction_locked(PageId page_id, frame_id_t frame_id, bool removed_from_replacer,
                                                       bool require_normal_residency) {
    if (!IsValidPageId(page_id) || frame_id < 0 || static_cast<size_t>(frame_id) >= pool_size_) {
        return false;
    }
    auto authoritative = page_table_.find(page_id);
    if (authoritative == page_table_.end() || authoritative->second != frame_id) {
        return false;
    }

    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    Page* page = &pages_[frame_id];
    auto resident = shard.entries.find(page_id);
    if (resident == shard.entries.end() || resident->second != frame_id || !(page->id_ == page_id) ||
        page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
        return false;
    }

    std::scoped_lock pin_lock{page->pin_latch_};
    if (resident->second != frame_id || !(page->id_ == page_id) ||
        page->state_.load(std::memory_order_acquire) != FrameState::VALID || page->pin_count_ != 0 ||
        (require_normal_residency && residency_classes_[frame_id] != ResidencyClass::Normal)) {
        return false;
    }
    if (!removed_from_replacer) {
        replacer_->pin(frame_id);
    }
    shard.entries.erase(resident);
    page->state_.store(FrameState::EVICTING, std::memory_order_release);
    return true;
}

void BufferPoolManager::restore_blocked_victims_locked(const std::vector<frame_id_t>& blocked) {
    for (const frame_id_t frame_id : blocked) {
        if (frame_id < 0 || static_cast<size_t>(frame_id) >= pool_size_) {
            continue;
        }
        Page* page = &pages_[frame_id];
        const PageId page_id = page->id_;
        if (!IsValidPageId(page_id)) {
            continue;
        }

        const size_t shard_index = resident_directory_shard_index(page_id);
        ResidentDirectoryShard& shard = resident_directory_[shard_index];
        auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
        auto authoritative = page_table_.find(page_id);
        auto resident = shard.entries.find(page_id);
        std::scoped_lock pin_lock{page->pin_latch_};
        if (authoritative != page_table_.end() && authoritative->second == frame_id &&
            resident != shard.entries.end() && resident->second == frame_id && page->id_ == page_id &&
            page->state_.load(std::memory_order_acquire) == FrameState::VALID && page->pin_count_ == 0 &&
            residency_classes_[frame_id] == ResidencyClass::Normal) {
            replacer_->unpin(frame_id);
        }
    }
}

Page* BufferPoolManager::fetch_resident_page_fast(PageId page_id, const FrameOperationToken* operation) {
    const uint64_t generation_before = frame_operation_generation_.load(std::memory_order_acquire);
    const bool active_before = frame_operation_active_.load(std::memory_order_acquire);
    if (active_before && !operation_authorized(operation, generation_before)) {
        return nullptr;
    }

    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_read_metrics_.acquire_shared(shard.latch, shard_index);
    const uint64_t generation_after = frame_operation_generation_.load(std::memory_order_acquire);
    const bool active_after = frame_operation_active_.load(std::memory_order_acquire);
    if (generation_before != generation_after || active_before != active_after ||
        (active_after && !operation_authorized(operation, generation_after))) {
        return nullptr;
    }

    auto resident = shard.entries.find(page_id);
    if (resident == shard.entries.end() || resident->second < 0 ||
        static_cast<size_t>(resident->second) >= pool_size_) {
        return nullptr;
    }
    const frame_id_t frame_id = resident->second;
    Page* page = &pages_[frame_id];
    if (!(page->id_ == page_id) || page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
        return nullptr;
    }

    std::scoped_lock pin_lock{page->pin_latch_};
    if (!(page->id_ == page_id) || page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
        return nullptr;
    }
    if (page->pin_count_ == 0) {
        replacer_->pin(frame_id);
    }
    ++page->pin_count_;
    return page;
}

BufferPoolManager::FastUnpinResult BufferPoolManager::unpin_clean_page_fast(PageId page_id) {
    bool became_available = false;
    {
        const size_t shard_index = resident_directory_shard_index(page_id);
        ResidentDirectoryShard& shard = resident_directory_[shard_index];
        auto shard_lock = shard_read_metrics_.acquire_shared(shard.latch, shard_index);
        auto resident = shard.entries.find(page_id);
        if (resident == shard.entries.end() || resident->second < 0 ||
            static_cast<size_t>(resident->second) >= pool_size_) {
            return FastUnpinResult::Miss;
        }

        const frame_id_t frame_id = resident->second;
        Page* page = &pages_[frame_id];
        if (!(page->id_ == page_id) || page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
            return FastUnpinResult::Miss;
        }

        std::scoped_lock pin_lock{page->pin_latch_};
        if (!(page->id_ == page_id) || page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
            return FastUnpinResult::Miss;
        }
        if (page->pin_count_ == 0) {
            return FastUnpinResult::InvalidPin;
        }
        --page->pin_count_;
        became_available = page->pin_count_ == 0 && residency_classes_[frame_id] == ResidencyClass::Normal;
        if (became_available) {
            replacer_->unpin(frame_id);
        }
    }

    if (became_available && frame_operation_active_.load(std::memory_order_acquire)) {
        // If a frame reservation is active, synchronize with its BPM-latch
        // wait boundary after releasing shard/pin locks. This closes the
        // check-then-sleep window without introducing shard -> global order.
        std::shared_lock operation_handshake{latch_};
        frame_operation_cv_.notify_all();
    }
    return FastUnpinResult::Success;
}

bool BufferPoolManager::resident_directory_is_consistent_for_test() {
    std::shared_lock global_lock{latch_};
    for (const auto& [page_id, frame_id] : page_table_) {
        if (frame_id < 0 || static_cast<size_t>(frame_id) >= pool_size_) {
            return false;
        }
        const Page* page = &pages_[frame_id];
        if (!(page->id_ == page_id)) {
            return false;
        }
        const ResidentDirectoryShard& shard = resident_directory_[resident_directory_shard_index(page_id)];
        std::shared_lock shard_lock{shard.latch};
        auto resident = shard.entries.find(page_id);
        if (page->state_.load(std::memory_order_acquire) == FrameState::VALID) {
            if (resident == shard.entries.end() || resident->second != frame_id) {
                return false;
            }
        } else if (resident != shard.entries.end()) {
            return false;
        }
    }

    for (size_t shard_index = 0; shard_index < resident_directory_.size(); ++shard_index) {
        const ResidentDirectoryShard& shard = resident_directory_[shard_index];
        std::shared_lock shard_lock{shard.latch};
        for (const auto& [page_id, frame_id] : shard.entries) {
            if (resident_directory_shard_index(page_id) != shard_index || frame_id < 0 ||
                static_cast<size_t>(frame_id) >= pool_size_) {
                return false;
            }
            auto authoritative = page_table_.find(page_id);
            if (authoritative == page_table_.end() || authoritative->second != frame_id) {
                return false;
            }
            const Page* page = &pages_[frame_id];
            if (!(page->id_ == page_id) || page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                return false;
            }
        }
    }
    return true;
}

// Reaching this with a non-Normal class means a frame was reused while its
// owner still classified it, i.e. the owner's pin bookkeeping has a hole. The
// class cannot survive the reuse, so drop it - but say so, because the holder
// may also be keeping a raw Page* for the page that just left this frame.
void BufferPoolManager::clear_residency(frame_id_t frame_id) {
    if (residency_classes_[frame_id] == ResidencyClass::Normal) {
        return;
    }
    LOG_WARN("buffer pool reused frame %d while it was still classified as resident (page %s)",
             static_cast<int>(frame_id), pages_[frame_id].id_.toString().c_str());
    if (residency_classes_[frame_id] == ResidencyClass::IndexInternal) {
        assert(index_internal_resident_count_ > 0);
        --index_internal_resident_count_;
    }
    residency_classes_[frame_id] = ResidencyClass::Normal;
}

void BufferPoolManager::clear_checkpoint_cohort_marker_locked(Page* page) {
    const uint64_t page_epoch = page->checkpoint_cohort_epoch_;
    if (page_epoch == 0) {
        return;
    }
    page->checkpoint_cohort_epoch_ = 0;
    if (page_epoch == active_checkpoint_cohort_epoch_) {
        assert(checkpoint_cohort_pages_remaining_ > 0);
        --checkpoint_cohort_pages_remaining_;
    }
}

void BufferPoolManager::finish_checkpoint_cohort_if_complete_locked() {
    if (active_checkpoint_cohort_epoch_ == 0 || checkpoint_cohort_pages_remaining_ != 0) {
        return;
    }
    checkpoint_cohort_pending_frames_.clear();
    active_checkpoint_cohort_epoch_ = 0;
}

void BufferPoolManager::ensure_write_dependency(const PageWriteDependency& dependency) {
    if (dependency.kind() == PageWriteDependency::Kind::WalLsn && log_manager_ == nullptr) {
        throw InternalError("WAL-dependent page write has no LogManager");
    }
    if (dependency.kind() == PageWriteDependency::Kind::WalLsn) {
        log_manager_->flush_log_to_disk_up_to_durable(dependency.wal_lsn());
    }
}

void BufferPoolManager::begin_index_smo(int fd) {
    std::unique_lock lock{latch_};
    ++index_smo_barriers_[fd];
    index_smo_cv_.wait(lock, [&] {
        auto inflight = index_writes_inflight_.find(fd);
        return inflight == index_writes_inflight_.end() || inflight->second == 0;
    });
}

void BufferPoolManager::end_index_smo(int fd) noexcept {
    {
        std::unique_lock lock{latch_};
        auto it = index_smo_barriers_.find(fd);
        if (it == index_smo_barriers_.end()) {
            return;
        }
        if (--it->second == 0) {
            index_smo_barriers_.erase(it);
        }
    }
    index_smo_cv_.notify_all();
}

void BufferPoolManager::begin_index_file_write(int fd) {
    std::unique_lock lock{latch_};
    index_smo_cv_.wait(lock, [&] { return !index_smo_blocked_locked(fd); });
    claim_index_file_write_locked(fd);
}

void BufferPoolManager::end_index_file_write(int fd) noexcept {
    {
        std::unique_lock lock{latch_};
        release_index_file_write_locked(fd);
    }
    index_smo_cv_.notify_all();
}

lsn_t BufferPoolManager::append_index_smo(const IndexSmoWalData& data) {
    if (log_manager_ == nullptr) {
        throw InternalError("INDEX_SMO requires a LogManager");
    }
    return log_manager_->append_index_smo(data);
}

void BufferPoolManager::ensure_index_binding(const std::string& index_file_name) {
    if (log_manager_ != nullptr) {
        (void)log_manager_->ensure_index_binding(index_file_name);
    }
}

void BufferPoolManager::renew_index_binding(const std::string& index_file_name) {
    if (log_manager_ != nullptr) {
        (void)log_manager_->renew_index_binding(index_file_name);
    }
}

bool BufferPoolManager::index_smo_blocked_locked(int fd) const {
    auto it = index_smo_barriers_.find(fd);
    return it != index_smo_barriers_.end() && it->second != 0;
}

void BufferPoolManager::claim_index_file_write_locked(int fd) {
    ++index_writes_inflight_[fd];
}

void BufferPoolManager::release_index_file_write_locked(int fd) {
    auto it = index_writes_inflight_.find(fd);
    assert(it != index_writes_inflight_.end() && it->second != 0);
    if (--it->second == 0) {
        index_writes_inflight_.erase(it);
    }
}

frame_id_t BufferPoolManager::take_unblocked_victim_locked() {
    auto& blocked = blocked_victims_scratch_;
    blocked.clear();
    frame_id_t candidate = INVALID_FRAME_ID;
    frame_id_t claimed = INVALID_FRAME_ID;
    try {
        size_t examined = 0;
        while (examined++ < pool_size_ && replacer_->victim(&candidate)) {
            if (candidate < 0 || static_cast<size_t>(candidate) >= pool_size_) {
                continue;
            }
            const Page& page = pages_[candidate];
            const PageId page_id = page.id_;
            if (IsValidPageId(page_id) && page.is_dirty_.load(std::memory_order_acquire) &&
                index_smo_blocked_locked(page_id.fd)) {
                // The candidate remains VALID. Reinsert it only after rechecking
                // that a fast hit did not pin it while it was outside CLOCK.
                blocked.push_back(candidate);
                continue;
            }
            if (claim_page_for_eviction_locked(page_id, candidate, true, true)) {
                claimed = candidate;
                break;
            }
        }
    } catch (...) {
        restore_blocked_victims_locked(blocked);
        blocked.clear();
        throw;
    }
    restore_blocked_victims_locked(blocked);
    blocked.clear();
    return claimed;
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim
 * page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    return fetch_page_impl(page_id, nullptr);
}

Page* BufferPoolManager::fetch_page_impl(PageId page_id, const FrameOperationToken* operation) {
    while (true) {
        if (Page* resident = fetch_resident_page_fast(page_id, operation); resident != nullptr) {
            return resident;
        }

        Page* target_page = nullptr;
        Page* wait_page = nullptr;
        PageId old_page_id;
        bool old_page_dirty = false;
        uint64_t old_dirty_epoch = 0;
        PageWriteDependency old_dependency = PageWriteDependency::None();
        frame_id_t fid = INVALID_FRAME_ID;

        {
            std::shared_lock lock{latch_};
            const bool operation_active = frame_operation_active_.load(std::memory_order_acquire);
            const uint64_t operation_generation = frame_operation_generation_.load(std::memory_order_acquire);
            if (operation_active && !operation_authorized(operation, operation_generation)) {
                lock.unlock();
                std::unique_lock operation_lock{latch_};
                frame_operation_cv_.wait(operation_lock,
                                         [&] { return !frame_operation_active_.load(std::memory_order_acquire); });
                continue;
            }
            auto hit = page_table_.find(page_id);
            if (hit != page_table_.end()) {
                target_page = &pages_[hit->second];
                if (target_page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                    wait_page = target_page;
                } else {
                    std::scoped_lock pin_lock{target_page->pin_latch_};
                    if (target_page->pin_count_ == 0) {
                        replacer_->pin(hit->second);
                    }
                    ++target_page->pin_count_;
                    return target_page;
                }
            }
        }

        if (wait_page != nullptr) {
            std::unique_lock<std::mutex> wait_lock(wait_page->io_latch_);
            wait_page->io_cv_.wait_for(wait_lock, std::chrono::milliseconds(1), [wait_page] {
                FrameState state = wait_page->state_.load(std::memory_order_acquire);
                return state == FrameState::FREE || state == FrameState::VALID;
            });
            continue;
        }

        {
            std::unique_lock lock{latch_};
            frame_operation_cv_.wait(lock, [&] {
                const bool operation_active = frame_operation_active_.load(std::memory_order_acquire);
                const uint64_t operation_generation = frame_operation_generation_.load(std::memory_order_acquire);
                return !operation_active || operation_authorized(operation, operation_generation);
            });
            auto hit = page_table_.find(page_id);
            if (hit != page_table_.end()) {
                target_page = &pages_[hit->second];
                if (target_page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                    wait_page = target_page;
                } else {
                    std::scoped_lock pin_lock{target_page->pin_latch_};
                    if (target_page->pin_count_ == 0) {
                        replacer_->pin(hit->second);
                    }
                    ++target_page->pin_count_;
                    return target_page;
                }
            } else {
                fid = take_free_frame();
                if (fid == INVALID_FRAME_ID) {
                    fid = take_unblocked_victim_locked();
                    if (fid == INVALID_FRAME_ID) {
                        return nullptr;
                    }
                }

                target_page = &pages_[fid];
                old_page_id = target_page->id_;
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    old_page_dirty = target_page->is_dirty_.load(std::memory_order_acquire);
                    old_dirty_epoch = target_page->dirty_epoch_.load(std::memory_order_acquire);
                    old_dependency = target_page->write_dependency_;
                    target_page->is_dirty_ = false;
                    target_page->dirty_epoch_ = 0;
                    target_page->write_dependency_ = PageWriteDependency::None();
                    if (!old_page_dirty || !IsValidPageId(old_page_id)) {
                        clear_checkpoint_cohort_marker_locked(target_page);
                    }
                }
                if (IsValidPageId(old_page_id)) {
                    erase_page_mapping_locked(old_page_id, fid, FrameState::EVICTING);
                }
                clear_residency(fid);
                if (old_page_dirty && IsValidPageId(old_page_id)) {
                    claim_index_file_write_locked(old_page_id.fd);
                }
                target_page->id_ = page_id;
                target_page->pin_count_ = 1;
                install_page_mapping_locked(page_id, fid, FrameState::LOADING);
            }
        }

        if (wait_page != nullptr) {
            std::unique_lock<std::mutex> wait_lock(wait_page->io_latch_);
            wait_page->io_cv_.wait_for(wait_lock, std::chrono::milliseconds(1), [wait_page] {
                FrameState state = wait_page->state_.load(std::memory_order_acquire);
                return state == FrameState::FREE || state == FrameState::VALID;
            });
            continue;
        }

        bool loaded = false;
        bool old_page_write_succeeded = !old_page_dirty || !IsValidPageId(old_page_id);
        const bool record_eviction = background_preclean_metrics_.enabled();
        const auto eviction_started =
            record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
        try {
            std::unique_lock<std::shared_mutex> page_lock(target_page->latch_);
            if (old_page_dirty && IsValidPageId(old_page_id)) {
                const auto dependency_started =
                    record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                ensure_write_dependency(old_dependency);
                if (record_eviction)
                    background_preclean_metrics_.foreground_dependency_wait(
                        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - dependency_started)
                                                  .count()));
                const auto pwrite_started =
                    record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
                disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, target_page->data_, PAGE_SIZE);
                if (record_eviction)
                    background_preclean_metrics_.foreground_pwrite(
                        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - pwrite_started)
                                                  .count()));
                old_page_write_succeeded = true;
            }
            target_page->reset_memory();
            const auto read_started =
                record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            disk_manager_->read_page(page_id.fd, page_id.page_no, target_page->data_, PAGE_SIZE);
            if (record_eviction)
                background_preclean_metrics_.foreground_read(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - read_started)
                                              .count()));
            if (record_eviction && old_page_dirty && IsValidPageId(old_page_id))
                background_preclean_metrics_.foreground_dirty_eviction(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - eviction_started)
                                              .count()));
            loaded = true;
        } catch (...) {
            loaded = false;
        }

        {
            std::scoped_lock lock{latch_};
            auto hit = page_table_.find(page_id);
            if (loaded && hit != page_table_.end() && hit->second == fid && target_page->id_ == page_id) {
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    clear_checkpoint_cohort_marker_locked(target_page);
                }
                set_mapped_frame_state_locked(page_id, fid, FrameState::VALID);
            } else if (!old_page_write_succeeded && IsValidPageId(old_page_id)) {
                // The frame still contains the old page image. Restore its
                // ownership instead of discarding the only dirty copy.
                if (hit != page_table_.end() && hit->second == fid) {
                    erase_page_mapping_locked(page_id, fid, FrameState::EVICTING);
                }
                target_page->id_ = old_page_id;
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    target_page->is_dirty_ = old_page_dirty;
                    target_page->dirty_epoch_ = old_dirty_epoch;
                    target_page->write_dependency_ = old_dependency;
                }
                target_page->pin_count_ = 0;
                install_page_mapping_locked(old_page_id, fid, FrameState::VALID);
            } else {
                assert(hit != page_table_.end() && hit->second == fid);
                target_page->reset_memory();
                target_page->id_ = PageId{};
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    target_page->is_dirty_ = false;
                    target_page->dirty_epoch_ = 0;
                    clear_checkpoint_cohort_marker_locked(target_page);
                    target_page->write_dependency_ = PageWriteDependency::None();
                }
                target_page->pin_count_ = 0;
                erase_page_mapping_locked(page_id, fid, FrameState::FREE);
                clear_residency(fid);
                recycle_frame(fid);
            }
            if (old_page_dirty && IsValidPageId(old_page_id)) {
                release_index_file_write_locked(old_page_id.fd);
            }
        }
        target_page->io_cv_.notify_all();
        frame_operation_cv_.notify_all();
        index_smo_cv_.notify_all();
        return loaded ? target_page : nullptr;
    }
}

bool BufferPoolManager::is_page_resident(PageId page_id) {
    std::shared_lock lock{latch_};
    auto hit = page_table_.find(page_id);
    return hit != page_table_.end() && pages_[hit->second].state_.load(std::memory_order_acquire) == FrameState::VALID;
}

std::optional<ResidencyClass> BufferPoolManager::get_residency_class(PageId page_id) {
    std::shared_lock lock{latch_};
    auto hit = page_table_.find(page_id);
    if (hit == page_table_.end() || pages_[hit->second].state_.load(std::memory_order_acquire) != FrameState::VALID) {
        return std::nullopt;
    }
    return residency_classes_[hit->second];
}

bool BufferPoolManager::try_mark_resident(PageId page_id, ResidencyClass residency_class) {
    std::unique_lock lock{latch_};
    auto hit = page_table_.find(page_id);
    if (hit == page_table_.end()) {
        return false;
    }
    Page* page = &pages_[hit->second];
    if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
        return false;
    }
    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    ResidencyClass& current = residency_classes_[hit->second];
    if (current == residency_class) {
        return true;
    }
    if (residency_class == ResidencyClass::IndexInternal) {
        if (index_internal_resident_count_ >= index_internal_residency_budget()) {
            return false;
        }
        ++index_internal_resident_count_;
    } else if (current == ResidencyClass::IndexInternal) {
        assert(index_internal_resident_count_ > 0);
        --index_internal_resident_count_;
    }
    current = residency_class;
    if (residency_class == ResidencyClass::IndexInternal) {
        replacer_->pin(hit->second);
    } else {
        std::scoped_lock pin_lock{page->pin_latch_};
        if (page->pin_count_ == 0) {
            replacer_->unpin(hit->second);
        }
    }
    return true;
}

void BufferPoolManager::unmark_resident(PageId page_id) {
    std::unique_lock lock{latch_};
    auto hit = page_table_.find(page_id);
    if (hit == page_table_.end()) {
        return;
    }
    Page* page = &pages_[hit->second];
    const size_t shard_index = resident_directory_shard_index(page_id);
    ResidentDirectoryShard& shard = resident_directory_[shard_index];
    auto shard_lock = shard_write_metrics_.acquire_exclusive(shard.latch, shard_index);
    ResidencyClass& current = residency_classes_[hit->second];
    if (current != ResidencyClass::IndexInternal) {
        return;
    }
    assert(index_internal_resident_count_ > 0);
    --index_internal_resident_count_;
    current = ResidencyClass::Normal;
    if (page->state_.load(std::memory_order_acquire) == FrameState::VALID) {
        std::scoped_lock pin_lock{page->pin_latch_};
        if (page->pin_count_ == 0) {
            replacer_->unpin(hit->second);
        }
    }
    lock.unlock();
    frame_operation_cv_.notify_all();
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    if (!is_dirty) {
        switch (unpin_clean_page_fast(page_id)) {
        case FastUnpinResult::Success:
            return true;
        case FastUnpinResult::InvalidPin:
            return false;
        case FastUnpinResult::Miss:
            break;
        }
    }

    PageWriteDependency dependency = PageWriteDependency::None();
    if (is_dirty) {
        std::shared_lock lock{latch_};
        auto hit = page_table_.find(page_id);
        if (hit == page_table_.end()) {
            return false;
        }
        Page* page = &pages_[hit->second];
        // The bool overload predates explicit write dependencies and is also
        // used for raw/index pages whose first four bytes are not an LSN. Only
        // derive a table-page WAL dependency when a LogManager is installed;
        // callers that know an image is WAL-dependent use the explicit
        // PageWriteDependency overload, which remains fail-closed.
        if (log_manager_ != nullptr) {
            dependency = PageWriteDependency::Wal(page->get_page_lsn());
        }
    }
    return unpin_page_impl(page_id, is_dirty, dependency);
}

bool BufferPoolManager::unpin_page(PageId page_id, const PageWriteDependency& dependency) {
    return unpin_page_impl(page_id, true, dependency);
}

bool BufferPoolManager::unpin_page_impl(PageId page_id, bool is_dirty, const PageWriteDependency& dependency) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    std::shared_lock lock{latch_};
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    auto hit = page_table_.find(page_id);
    if (hit == page_table_.end())
        return false;

    // 1.2 P在页表中存在,获取其pin_count_
    frame_id_t fid = hit->second;
    Page* targetPage = &(pages_[fid]);

    FrameState state = targetPage->state_.load(std::memory_order_acquire);
    if (state != FrameState::VALID && state != FrameState::FLUSHING) {
        return false;
    }

    std::scoped_lock pin_lock{targetPage->pin_latch_};
    // 3 根据参数is_dirty，更改P的is_dirty_
    // Marking a modified page dirty is independent of releasing a pin (compare
    // PostgreSQL's MarkBufferDirty vs ReleaseBuffer). Doing it before the
    // pin-count check matters: two owners can hold the same page - the index
    // root cache pins the root and hands the same raw page to a writer - and
    // whoever releases second must not lose the modification just because the
    // pin is already gone.
    if (is_dirty == true) {
        mark_dirty(targetPage, dependency);
    }
    // 2.1 若pin_count_已经等于0,则返回false
    if (targetPage->pin_count_ == 0)
        return false;

    // 2.2 若pin_count_大于0，则pin_count_自减一
    --targetPage->pin_count_;
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    const bool became_available =
        targetPage->pin_count_ == 0 && state == FrameState::VALID && residency_classes_[fid] == ResidencyClass::Normal;
    if (became_available) {
        replacer_->unpin(fid);
        frame_operation_cv_.notify_all();
    }
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    return flush_page_impl(page_id, false);
}

bool BufferPoolManager::flush_page_impl(PageId page_id, bool dirty_only) {
    Page* page = nullptr;
    frame_id_t fid = INVALID_FRAME_ID;
    while (true) {
        Page* wait_page = nullptr;
        {
            std::unique_lock lock{latch_};
            index_smo_cv_.wait(lock, [&] { return !index_smo_blocked_locked(page_id.fd); });
            auto hit = page_table_.find(page_id);
            if (hit == page_table_.end()) {
                return false;
            }
            page = &pages_[hit->second];
            fid = hit->second;
            if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                wait_page = page;
            } else {
                if (dirty_only && !page->is_dirty_.load(std::memory_order_acquire)) {
                    return true;
                }
                // Keep an unpinned page out of the replacer while its stable
                // image is being written.
                replacer_->pin(hit->second);
                set_mapped_frame_state_locked(page_id, fid, FrameState::FLUSHING);
                claim_index_file_write_locked(page_id.fd);
            }
        }

        if (wait_page != nullptr) {
            std::unique_lock<std::mutex> wait_lock(wait_page->io_latch_);
            wait_page->io_cv_.wait_for(wait_lock, std::chrono::milliseconds(1), [wait_page] {
                FrameState state = wait_page->state_.load(std::memory_order_acquire);
                return state == FrameState::FREE || state == FrameState::VALID;
            });
            continue;
        }
        break;
    }

    bool flushed = false;
    PageWriteDependency flushed_dependency = PageWriteDependency::None();
    uint64_t flushed_dirty_epoch = 0;
    try {
        run_flush_page_test_hook(page_id, page);
        {
            std::shared_lock<std::shared_mutex> page_lock(page->latch_);
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                flushed_dependency = page->write_dependency_;
                flushed_dirty_epoch = page->dirty_epoch_.load(std::memory_order_acquire);
            }
            ensure_write_dependency(flushed_dependency);
            disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
        }
        run_flush_page_after_write_test_hook(page_id, page);
        flushed = true;
    } catch (...) {
        flushed = false;
    }

    {
        std::scoped_lock lock{latch_};
        auto hit = page_table_.find(page_id);
        if (hit != page_table_.end() && hit->second == fid && page->id_ == page_id &&
            page->state_.load(std::memory_order_acquire) == FrameState::FLUSHING) {
            if (flushed) {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                clear_checkpoint_cohort_marker_locked(page);
                if (page->dirty_epoch_.load(std::memory_order_acquire) == flushed_dirty_epoch) {
                    page->is_dirty_ = false;
                    page->write_dependency_ = PageWriteDependency::None();
                }
            }
            set_mapped_frame_state_locked(page_id, fid, FrameState::VALID);
        }
        release_index_file_write_locked(page_id.fd);
    }
    page->io_cv_.notify_all();
    frame_operation_cv_.notify_all();
    index_smo_cv_.notify_all();
    return flushed;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    return new_page_impl(page_id, nullptr);
}

Page* BufferPoolManager::new_page_impl(PageId* page_id, const FrameOperationToken* operation) {
    Page* page = nullptr;
    PageId old_page_id;
    bool old_page_dirty = false;
    uint64_t old_dirty_epoch = 0;
    PageWriteDependency old_dependency = PageWriteDependency::None();
    frame_id_t fid = INVALID_FRAME_ID;
    {
        std::unique_lock lock{latch_};
        frame_operation_cv_.wait(lock, [&] {
            const bool operation_active = frame_operation_active_.load(std::memory_order_acquire);
            const uint64_t operation_generation = frame_operation_generation_.load(std::memory_order_acquire);
            return !operation_active || operation_authorized(operation, operation_generation);
        });
        fid = take_free_frame();
        if (fid == INVALID_FRAME_ID) {
            fid = take_unblocked_victim_locked();
            if (fid == INVALID_FRAME_ID) {
                return nullptr;
            }
        }

        page_id->page_no = disk_manager_->allocate_page(page_id->fd);
        page = &pages_[fid];
        old_page_id = page->id_;
        {
            std::scoped_lock dirty_lock{page->dirty_latch_};
            old_page_dirty = page->is_dirty_.load(std::memory_order_acquire);
            old_dirty_epoch = page->dirty_epoch_.load(std::memory_order_acquire);
            old_dependency = page->write_dependency_;
            page->is_dirty_ = false;
            page->dirty_epoch_ = 0;
            page->write_dependency_ = PageWriteDependency::None();
            if (!old_page_dirty || !IsValidPageId(old_page_id)) {
                clear_checkpoint_cohort_marker_locked(page);
            }
        }
        if (IsValidPageId(old_page_id)) {
            erase_page_mapping_locked(old_page_id, fid, FrameState::EVICTING);
        }
        clear_residency(fid);
        if (old_page_dirty && IsValidPageId(old_page_id)) {
            claim_index_file_write_locked(old_page_id.fd);
        }
        page->id_ = *page_id;
        page->pin_count_ = 1;
        install_page_mapping_locked(*page_id, fid, FrameState::LOADING);
    }

    bool initialized = false;
    bool old_page_write_succeeded = !old_page_dirty || !IsValidPageId(old_page_id);
    const bool record_eviction = background_preclean_metrics_.enabled();
    const auto eviction_started =
        record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    try {
        std::unique_lock<std::shared_mutex> page_lock(page->latch_);
        if (old_page_dirty && IsValidPageId(old_page_id)) {
            const auto dependency_started =
                record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            ensure_write_dependency(old_dependency);
            if (record_eviction)
                background_preclean_metrics_.foreground_dependency_wait(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - dependency_started)
                                              .count()));
            const auto pwrite_started =
                record_eviction ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, page->data_, PAGE_SIZE);
            if (record_eviction)
                background_preclean_metrics_.foreground_pwrite(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - pwrite_started)
                                              .count()));
            if (record_eviction)
                background_preclean_metrics_.foreground_dirty_eviction(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - eviction_started)
                                              .count()));
            old_page_write_succeeded = true;
        }
        page->reset_memory();
        initialized = true;
    } catch (...) {
        initialized = false;
    }

    {
        std::scoped_lock lock{latch_};
        auto hit = page_table_.find(*page_id);
        if (initialized && hit != page_table_.end() && hit->second == fid) {
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                clear_checkpoint_cohort_marker_locked(page);
            }
            set_mapped_frame_state_locked(*page_id, fid, FrameState::VALID);
        } else if (!old_page_write_succeeded && IsValidPageId(old_page_id)) {
            if (hit != page_table_.end() && hit->second == fid) {
                erase_page_mapping_locked(*page_id, fid, FrameState::EVICTING);
            }
            page->id_ = old_page_id;
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                page->is_dirty_ = old_page_dirty;
                page->dirty_epoch_ = old_dirty_epoch;
                page->write_dependency_ = old_dependency;
            }
            page->pin_count_ = 0;
            install_page_mapping_locked(old_page_id, fid, FrameState::VALID);
        } else {
            assert(hit != page_table_.end() && hit->second == fid);
            page->reset_memory();
            page->id_ = PageId{};
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                page->is_dirty_ = false;
                page->dirty_epoch_ = 0;
                clear_checkpoint_cohort_marker_locked(page);
                page->write_dependency_ = PageWriteDependency::None();
            }
            page->pin_count_ = 0;
            erase_page_mapping_locked(*page_id, fid, FrameState::FREE);
            clear_residency(fid);
            recycle_frame(fid);
        }
        if (old_page_dirty && IsValidPageId(old_page_id)) {
            release_index_file_write_locked(old_page_id.fd);
        }
    }
    page->io_cv_.notify_all();
    frame_operation_cv_.notify_all();
    index_smo_cv_.notify_all();
    return initialized ? page : nullptr;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入回收帧集合，返回true

    while (true) {
        Page* page = nullptr;
        Page* wait_page = nullptr;
        frame_id_t fid = INVALID_FRAME_ID;
        {
            std::unique_lock lock{latch_};
            index_smo_cv_.wait(lock, [&] { return !index_smo_blocked_locked(page_id.fd); });
            auto hit = page_table_.find(page_id);
            if (hit == page_table_.end()) {
                return true;
            }
            fid = hit->second;
            page = &pages_[fid];
            if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                wait_page = page;
            } else {
                if (!claim_page_for_eviction_locked(page_id, fid, false, false)) {
                    return false;
                }
                claim_index_file_write_locked(page_id.fd);
            }
        }

        if (wait_page != nullptr) {
            std::unique_lock<std::mutex> wait_lock(wait_page->io_latch_);
            wait_page->io_cv_.wait_for(wait_lock, std::chrono::milliseconds(1), [wait_page] {
                FrameState state = wait_page->state_.load(std::memory_order_acquire);
                return state == FrameState::FREE || state == FrameState::VALID;
            });
            continue;
        }

        bool deleted = false;
        PageWriteDependency dependency = PageWriteDependency::None();
        try {
            std::unique_lock<std::shared_mutex> page_lock(page->latch_);
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                dependency = page->write_dependency_;
            }
            ensure_write_dependency(dependency);
            disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
            deleted = true;
        } catch (...) {
            deleted = false;
        }

        {
            std::scoped_lock lock{latch_};
            auto hit = page_table_.find(page_id);
            if (deleted && hit != page_table_.end() && hit->second == fid) {
                page->reset_memory();
                page->id_ = PageId{};
                page->pin_count_ = 0;
                {
                    std::scoped_lock dirty_lock{page->dirty_latch_};
                    page->is_dirty_ = false;
                    page->dirty_epoch_ = 0;
                    clear_checkpoint_cohort_marker_locked(page);
                    page->write_dependency_ = PageWriteDependency::None();
                }
                erase_page_mapping_locked(page_id, fid, FrameState::FREE);
                clear_residency(fid);
                recycle_frame(fid);
            } else if (!deleted && hit != page_table_.end() && hit->second == fid) {
                set_mapped_frame_state_locked(page_id, fid, FrameState::VALID);
            }
            release_index_file_write_locked(page_id.fd);
        }
        page->io_cv_.notify_all();
        frame_operation_cv_.notify_all();
        index_smo_cv_.notify_all();
        return deleted;
    }
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
bool BufferPoolManager::flush_all_pages(int fd) {
    bool success = true;
    std::vector<PageId> pages_to_flush;
    {
        std::shared_lock lock{latch_};
        pages_to_flush.reserve(page_table_.size());
        for (const auto& [page_id, frame_id] : page_table_) {
            if (page_id.fd != fd) {
                continue;
            }
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID && page->is_dirty_) {
                pages_to_flush.push_back(page_id);
            }
        }
    }
    for (const PageId& page_id_to_flush : pages_to_flush) {
        success = flush_page_impl(page_id_to_flush, true) && success;
    }
    return success;
}

bool BufferPoolManager::flush_all_pages(const std::vector<int>& fds) {
    return flush_all_pages(fds, FlushDependencyPolicy::Enforce());
}

bool BufferPoolManager::flush_all_pages(const std::vector<int>& fds, FlushDependencyPolicy policy) {
    if (fds.empty()) {
        return true;
    }

    const std::unordered_set<int> fd_set(fds.begin(), fds.end());
    std::vector<PageId> candidates;
    {
        std::shared_lock lock{latch_};
        candidates.reserve(page_table_.size());
        for (const auto& [page_id, frame_id] : page_table_) {
            if (fd_set.find(page_id.fd) == fd_set.end()) {
                continue;
            }
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID && page->is_dirty_) {
                candidates.push_back(page_id);
            }
        }
    }
    return flush_pages(candidates, policy).success;
}

bool BufferPoolManager::flush_all_pages_for_recovery(const std::vector<int>& fds) {
    if (fds.empty()) {
        return true;
    }

    const std::unordered_set<int> fd_set(fds.begin(), fds.end());
    std::vector<PageId> candidates;
    {
        std::shared_lock lock{latch_};
        candidates.reserve(page_table_.size());
        for (const auto& [page_id, frame_id] : page_table_) {
            if (fd_set.find(page_id.fd) == fd_set.end()) {
                continue;
            }
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID && page->is_dirty_) {
                candidates.push_back(page_id);
            }
        }
    }
    if (candidates.empty()) {
        return true;
    }

    std::sort(candidates.begin(), candidates.end(), [](const PageId& lhs, const PageId& rhs) {
        if (lhs.fd != rhs.fd) {
            return lhs.fd < rhs.fd;
        }
        return lhs.page_no < rhs.page_no;
    });

    constexpr size_t kRecoveryTaskPages = 256;
    constexpr size_t kMaxRecoveryWorkers = 4;
    struct RecoveryTask {
        size_t begin;
        size_t end;
    };
    std::vector<RecoveryTask> tasks;
    for (size_t run_begin = 0; run_begin < candidates.size();) {
        size_t run_end = run_begin + 1;
        while (run_end < candidates.size() && candidates[run_end].fd == candidates[run_begin].fd &&
               candidates[run_end].page_no == candidates[run_end - 1].page_no + 1) {
            ++run_end;
        }
        for (size_t task_begin = run_begin; task_begin < run_end; task_begin += kRecoveryTaskPages) {
            tasks.push_back(RecoveryTask{task_begin, std::min(task_begin + kRecoveryTaskPages, run_end)});
        }
        run_begin = run_end;
    }

    const size_t worker_count = std::min(kMaxRecoveryWorkers, tasks.size());
    constexpr size_t kClaimPages = 64;
    std::vector<std::vector<char>> worker_images;
    worker_images.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        // Allocate every worker's image before it can claim a frame. This keeps
        // allocation failure from leaving a claimed frame in FLUSHING state.
        worker_images.emplace_back();
        worker_images.back().reserve(kClaimPages * PAGE_SIZE);
    }

    std::atomic<size_t> next_task{0};
    std::atomic<bool> failed{false};
    std::mutex exception_latch;
    std::exception_ptr worker_exception;
    auto record_exception = [&](std::exception_ptr exception) {
        failed.store(true, std::memory_order_release);
        std::scoped_lock lock{exception_latch};
        if (worker_exception == nullptr) {
            worker_exception = exception;
        }
    };
    auto worker = [&](size_t worker_index) {
        try {
            while (!failed.load(std::memory_order_acquire)) {
                const size_t task_index = next_task.fetch_add(1, std::memory_order_relaxed);
                if (task_index >= tasks.size()) {
                    return;
                }
                const RecoveryTask task = tasks[task_index];
                const FlushBatchResult result = flush_sorted_pages(
                    candidates, task.begin, task.end, FlushDependencyPolicy::Enforce(), worker_images[worker_index]);
                if (!result.success) {
                    failed.store(true, std::memory_order_release);
                    return;
                }
            }
        } catch (...) {
            record_exception(std::current_exception());
        }
    };

    std::vector<std::thread> workers;
    workers.reserve(worker_count);
    try {
        for (size_t i = 0; i < worker_count; ++i) {
            workers.emplace_back(worker, i);
        }
    } catch (...) {
        record_exception(std::current_exception());
    }
    for (std::thread& thread : workers) {
        thread.join();
    }
    if (worker_exception != nullptr) {
        std::rethrow_exception(worker_exception);
    }
    return !failed.load(std::memory_order_acquire);
}

BufferPoolManager::FlushBatchResult BufferPoolManager::flush_pages(std::vector<PageId>& page_ids,
                                                                   FlushDependencyPolicy policy) {
    if (page_ids.empty()) {
        return FlushBatchResult{};
    }

    // Sorted so that runs of adjacent page numbers become single pwrites.
    std::sort(page_ids.begin(), page_ids.end(), [](const PageId& lhs, const PageId& rhs) {
        if (lhs.fd != rhs.fd) {
            return lhs.fd < rhs.fd;
        }
        return lhs.page_no < rhs.page_no;
    });
    constexpr size_t kClaimPages = 64;
    // Reserve before the first claim. flush_sorted_pages only resizes within
    // this capacity after a claim, so it cannot allocate or throw there.
    std::vector<char> image;
    image.reserve(std::min(kClaimPages, page_ids.size()) * PAGE_SIZE);
    return flush_sorted_pages(page_ids, 0, page_ids.size(), policy, image);
}

BufferPoolManager::FlushBatchResult BufferPoolManager::flush_sorted_pages(const std::vector<PageId>& candidates,
                                                                          size_t candidate_begin, size_t candidate_end,
                                                                          FlushDependencyPolicy policy,
                                                                          std::vector<char>& image) {
    FlushBatchResult result;
    if (candidate_begin >= candidate_end) {
        return result;
    }

    constexpr size_t kClaimPages = 64;
    struct ClaimedPage {
        PageId page_id;
        frame_id_t frame_id;
        uint64_t dirty_epoch{0};
        PageWriteDependency dependency{PageWriteDependency::None()};
    };

    const size_t required_image_size = std::min(kClaimPages, candidate_end - candidate_begin) * PAGE_SIZE;
    if (image.capacity() < required_image_size) {
        throw InternalError("flush image must be allocated before claiming pages");
    }
    std::vector<ClaimedPage> claimed;
    claimed.reserve(kClaimPages);
    bool success = true;
    for (; candidate_begin < candidate_end;) {
        claimed.clear();
        {
            std::unique_lock lock{latch_};
            while (candidate_begin < candidate_end && claimed.size() < kClaimPages) {
                const PageId page_id = candidates[candidate_begin++];
                index_smo_cv_.wait(lock, [&] { return !index_smo_blocked_locked(page_id.fd); });
                auto hit = page_table_.find(page_id);
                if (hit == page_table_.end()) {
                    continue;
                }
                Page* page = &pages_[hit->second];
                if (page->state_.load(std::memory_order_acquire) != FrameState::VALID ||
                    !page->is_dirty_.load(std::memory_order_acquire)) {
                    continue;
                }
                replacer_->pin(hit->second);
                set_mapped_frame_state_locked(page_id, hit->second, FrameState::FLUSHING);
                claim_index_file_write_locked(page_id.fd);
                claimed.push_back(ClaimedPage{page_id, hit->second});
            }
        }

        size_t claimed_begin = 0;
        // The caller reserved the largest possible claim for this range before
        // any frame transition.
        // char default construction within existing capacity cannot allocate or
        // throw, while preserving the old small-batch initialization cost.
        image.resize(claimed.size() * PAGE_SIZE);
        while (claimed_begin < claimed.size()) {
            size_t claimed_end = claimed_begin + 1;
            while (claimed_end < claimed.size() &&
                   claimed[claimed_end].page_id.fd == claimed[claimed_begin].page_id.fd &&
                   claimed[claimed_end].page_id.page_no == claimed[claimed_end - 1].page_id.page_no + 1) {
                ++claimed_end;
            }

            const size_t page_count = claimed_end - claimed_begin;
            bool copied = true;
            PageWriteDependency dependency = PageWriteDependency::None();
            try {
                run_flush_batch_before_write_test_hook(claimed[claimed_begin].page_id,
                                                       &pages_[claimed[claimed_begin].frame_id]);
            } catch (...) {
                copied = false;
            }
            for (size_t i = claimed_begin; copied && i < claimed_end; ++i) {
                Page* page = &pages_[claimed[i].frame_id];
                try {
                    std::shared_lock page_lock(page->latch_);
                    std::memcpy(image.data() + (i - claimed_begin) * PAGE_SIZE, page->data_, PAGE_SIZE);
                    {
                        std::scoped_lock dirty_lock{page->dirty_latch_};
                        claimed[i].dirty_epoch = page->dirty_epoch_.load(std::memory_order_acquire);
                        claimed[i].dependency = page->write_dependency_;
                    }
                    dependency.merge(claimed[i].dependency);
                } catch (...) {
                    copied = false;
                    break;
                }
            }
            if (copied) {
                try {
                    if (policy.kind() == FlushDependencyPolicy::Kind::Enforce) {
                        ensure_write_dependency(dependency);
                    }
                    disk_manager_->write_page(claimed[claimed_begin].page_id.fd, claimed[claimed_begin].page_id.page_no,
                                              image.data(), static_cast<int>(page_count * PAGE_SIZE));
                    for (size_t i = claimed_begin; i < claimed_end; ++i) {
                        run_flush_page_after_write_test_hook(claimed[i].page_id, &pages_[claimed[i].frame_id]);
                    }
                } catch (...) {
                    copied = false;
                }
            }
            success = copied && success;
            if (copied) {
                result.pages_written += page_count;
            }

            {
                std::unique_lock lock{latch_};
                for (size_t i = claimed_begin; i < claimed_end; ++i) {
                    const auto& claimed_page = claimed[i];
                    Page* page = &pages_[claimed_page.frame_id];
                    auto hit = page_table_.find(claimed_page.page_id);
                    if (hit == page_table_.end() || hit->second != claimed_page.frame_id ||
                        page->state_.load(std::memory_order_acquire) != FrameState::FLUSHING) {
                        release_index_file_write_locked(claimed_page.page_id.fd);
                        continue;
                    }
                    if (copied) {
                        {
                            std::scoped_lock dirty_lock{page->dirty_latch_};
                            clear_checkpoint_cohort_marker_locked(page);
                            if (page->dirty_epoch_.load(std::memory_order_acquire) == claimed_page.dirty_epoch) {
                                page->is_dirty_ = false;
                                page->write_dependency_ = PageWriteDependency::None();
                            }
                        }
                    }
                    set_mapped_frame_state_locked(claimed_page.page_id, claimed_page.frame_id, FrameState::VALID);
                    page->io_cv_.notify_all();
                    release_index_file_write_locked(claimed_page.page_id.fd);
                }
            }
            index_smo_cv_.notify_all();
            claimed_begin = claimed_end;
        }
    }
    result.success = success;
    return result;
}

BufferPoolManager::FlushBatchResult BufferPoolManager::flush_dirty_pages(const std::vector<int>& allowed_fds,
                                                                         size_t max_pages) {
    FlushBatchResult result;
    if (max_pages == 0 || allowed_fds.empty() || pool_size_ == 0) {
        return result;
    }

    const std::unordered_set<int> fd_set(allowed_fds.begin(), allowed_fds.end());
    std::vector<PageId> pages_to_flush;
    {
        std::unique_lock lock{latch_};
        pages_to_flush.reserve(std::min(max_pages, pool_size_));
        const size_t frame_budget = std::min(pool_size_, DirtyScanFrameBudget(max_pages));
        size_t frames_examined = 0;
        while (frames_examined < frame_budget && pages_to_flush.size() < max_pages) {
            const frame_id_t frame_id = next_dirty_flush_frame_;
            next_dirty_flush_frame_ =
                static_cast<frame_id_t>((static_cast<size_t>(next_dirty_flush_frame_) + 1) % pool_size_);
            ++frames_examined;
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID &&
                fd_set.find(page->id_.fd) != fd_set.end() && page->is_dirty_.load(std::memory_order_acquire)) {
                pages_to_flush.push_back(page->id_);
            }
        }
    }

    // Every frame carries a typed WAL dependency. Enforce it per batch while
    // foreground writers remain active, including for index pages whose payload
    // does not contain a table-page LSN.
    return flush_pages(pages_to_flush, FlushDependencyPolicy::Enforce());
}

BufferPoolManager::CheckpointCohort
BufferPoolManager::begin_checkpoint_cohort(const std::vector<int>& allowed_fds,
                                           std::chrono::milliseconds settle_timeout) {
    const std::unordered_set<int> fd_set(allowed_fds.begin(), allowed_fds.end());
    std::unique_lock lock{latch_};
    finish_checkpoint_cohort_if_complete_locked();
    if (active_checkpoint_cohort_epoch_ != 0) {
        throw InternalError("checkpoint cohort already active");
    }

    // A flush or dirty-victim eviction may already have copied an older image
    // while a writer re-dirties the frame. Waiting for every allowed file's
    // claimed write to settle ensures that begin never skips that FLUSHING or
    // EVICTING frame: success restores it as VALID+dirty or discharges it by a
    // successful write. wait_for releases latch_, so completions and ordinary
    // buffer-pool admission remain live while the cut is waiting.
    const auto allowed_writes_settled = [&] {
        for (const int fd : fd_set) {
            auto inflight = index_writes_inflight_.find(fd);
            if (inflight != index_writes_inflight_.end() && inflight->second != 0) {
                return false;
            }
        }
        return true;
    };
    if (!index_smo_cv_.wait_for(lock, settle_timeout, allowed_writes_settled)) {
        return CheckpointCohort{};
    }

    // Another coordinator may have installed a cohort while this caller was
    // asleep. The active epoch is the authoritative ownership record, so no
    // second page-table scan is needed merely to detect it.
    finish_checkpoint_cohort_if_complete_locked();
    if (active_checkpoint_cohort_epoch_ != 0) {
        throw InternalError("checkpoint cohort already active");
    }
    if (next_checkpoint_cohort_epoch_ == std::numeric_limits<uint64_t>::max()) {
        throw InternalError("checkpoint cohort epoch exhausted");
    }

    CheckpointCohort cohort;
    cohort.epoch = next_checkpoint_cohort_epoch_++;
    cohort.success = true;
    active_checkpoint_cohort_epoch_ = cohort.epoch;
    checkpoint_cohort_pages_remaining_ = 0;
    checkpoint_cohort_pending_frames_.clear();

    // This is the cohort's only full resident-page scan. The fixed frame-id
    // queue drives every later pacing tick.
    for (const auto& [page_id, frame_id] : page_table_) {
        if (fd_set.find(page_id.fd) == fd_set.end()) {
            continue;
        }
        Page* page = &pages_[frame_id];
        if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
            continue;
        }
        std::scoped_lock dirty_lock{page->dirty_latch_};
        if (!page->is_dirty_.load(std::memory_order_acquire)) {
            continue;
        }
        page->checkpoint_cohort_epoch_ = cohort.epoch;
        checkpoint_cohort_pending_frames_.push_back(frame_id);
        ++checkpoint_cohort_pages_remaining_;
        ++cohort.pages_marked;
    }
    return cohort;
}

BufferPoolManager::CheckpointCohortFlushResult
BufferPoolManager::flush_checkpoint_cohort(uint64_t epoch, size_t max_io_pages, size_t max_frames_to_visit) {
    CheckpointCohortFlushResult result;
    if (epoch == 0) {
        result.success = false;
        return result;
    }

    std::vector<PageId> pages_to_flush;
    {
        std::unique_lock lock{latch_};
        checkpoint_cohort_frames_visited_for_test_ = 0;
        finish_checkpoint_cohort_if_complete_locked();
        if (active_checkpoint_cohort_epoch_ == 0) {
            return result;
        }
        if (active_checkpoint_cohort_epoch_ != epoch) {
            result.success = false;
            return result;
        }
        if (max_io_pages == 0 || max_frames_to_visit == 0) {
            result.pages_remaining = checkpoint_cohort_pages_remaining_;
            return result;
        }

        const size_t frames_to_visit = std::min(checkpoint_cohort_pending_frames_.size(), max_frames_to_visit);
        const size_t pages_to_select = std::min(max_io_pages, max_frames_to_visit);
        pages_to_flush.reserve(std::min(pages_to_select, frames_to_visit));
        for (size_t visited = 0; visited < frames_to_visit && pages_to_flush.size() < pages_to_select; ++visited) {
            ++checkpoint_cohort_frames_visited_for_test_;
            const frame_id_t frame_id = checkpoint_cohort_pending_frames_.front();
            checkpoint_cohort_pending_frames_.pop_front();
            if (frame_id < 0 || static_cast<size_t>(frame_id) >= pool_size_) {
                continue;
            }

            Page* page = &pages_[frame_id];
            std::scoped_lock dirty_lock{page->dirty_latch_};
            if (page->checkpoint_cohort_epoch_ != epoch) {
                // A foreground flush, successful victim write, delete, or
                // cancel already discharged this fixed-frame obligation.
                continue;
            }
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID) {
                if (page->is_dirty_.load(std::memory_order_acquire)) {
                    pages_to_flush.push_back(page->id_);
                } else {
                    // A clean marked page can only be an already-persisted
                    // image. Settle it defensively rather than rotating it
                    // forever if a future write path misses marker cleanup.
                    clear_checkpoint_cohort_marker_locked(page);
                    continue;
                }
            }
            // Keep both selected pages and transient FLUSHING/EVICTING/LOADING
            // frames in the queue. Success clears their marker; failure keeps
            // it, so the next bounded visit retries without a global scan.
            checkpoint_cohort_pending_frames_.push_back(frame_id);
        }
        finish_checkpoint_cohort_if_complete_locked();
    }

    FlushBatchResult flushed = flush_pages(pages_to_flush, FlushDependencyPolicy::Enforce());
    result.pages_written = flushed.pages_written;
    result.success = flushed.success;

    std::unique_lock lock{latch_};
    if (active_checkpoint_cohort_epoch_ == epoch) {
        result.pages_remaining = checkpoint_cohort_pages_remaining_;
        finish_checkpoint_cohort_if_complete_locked();
    }
    return result;
}

size_t BufferPoolManager::cancel_checkpoint_cohort(uint64_t epoch) {
    if (epoch == 0) {
        return 0;
    }

    size_t pages_cancelled = 0;
    std::unique_lock lock{latch_};
    finish_checkpoint_cohort_if_complete_locked();
    if (active_checkpoint_cohort_epoch_ != epoch) {
        return 0;
    }
    for (const frame_id_t frame_id : checkpoint_cohort_pending_frames_) {
        if (frame_id < 0 || static_cast<size_t>(frame_id) >= pool_size_) {
            continue;
        }
        Page* page = &pages_[frame_id];
        std::scoped_lock dirty_lock{page->dirty_latch_};
        if (page->checkpoint_cohort_epoch_ != epoch) {
            continue;
        }
        page->checkpoint_cohort_epoch_ = 0;
        ++pages_cancelled;
    }
    checkpoint_cohort_pending_frames_.clear();
    checkpoint_cohort_pages_remaining_ = 0;
    active_checkpoint_cohort_epoch_ = 0;
    return pages_cancelled;
}

size_t BufferPoolManager::index_internal_resident_count() {
    std::shared_lock lock{latch_};
    return index_internal_resident_count_;
}

size_t BufferPoolManager::count_dirty_pages(const std::vector<int>& allowed_fds) {
    if (allowed_fds.empty()) {
        return 0;
    }

    const std::unordered_set<int> fd_set(allowed_fds.begin(), allowed_fds.end());
    size_t dirty_pages = 0;
    std::shared_lock lock{latch_};
    for (const auto& [page_id, frame_id] : page_table_) {
        const Page& page = pages_[frame_id];
        if (page.state_.load(std::memory_order_acquire) == FrameState::VALID &&
            fd_set.find(page_id.fd) != fd_set.end() && page.is_dirty_.load(std::memory_order_acquire)) {
            ++dirty_pages;
        }
    }
    return dirty_pages;
}

void BufferPoolManager::delete_all_pages(int fd) {
    std::unique_lock lock{latch_};
    for (auto it = page_table_.begin(); it != page_table_.end();) {
        if (it->first.fd != fd) {
            ++it;
            continue;
        }

        const PageId page_id = it->first;
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
            ++it;
            continue;
        }
        if (!claim_page_for_eviction_locked(page_id, frame_id, false, false)) {
            ++it;
            continue;
        }
        ++it;

        clear_residency(frame_id);
        recycle_frame(frame_id);
        page->reset_memory();
        page->pin_count_ = 0;
        {
            std::scoped_lock dirty_lock{page->dirty_latch_};
            page->is_dirty_ = false;
            page->dirty_epoch_ = 0;
            clear_checkpoint_cohort_marker_locked(page);
            page->write_dependency_ = PageWriteDependency::None();
        }
        page->id_ = PageId{};
        erase_page_mapping_locked(page_id, frame_id, FrameState::FREE);
    }
    lock.unlock();
    frame_operation_cv_.notify_all();
}
