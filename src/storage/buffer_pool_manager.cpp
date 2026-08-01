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
#include <unordered_set>

#include "minilog.h"
#include "recovery/log_manager.h"

namespace {

bool IsValidPageId(const PageId& page_id) {
    return page_id.fd >= 0 && page_id.page_no != INVALID_PAGE_ID;
}

} // namespace

std::mutex BufferPoolManager::flush_page_test_hook_latch_;
BufferPoolManager::FlushPageTestHook BufferPoolManager::flush_page_test_hook_;
BufferPoolManager::FlushPageTestHook BufferPoolManager::flush_page_after_write_test_hook_;
BufferPoolManager::FlushPageTestHook BufferPoolManager::flush_batch_before_write_test_hook_;

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

bool BufferPoolManager::operation_authorized_locked(const FrameOperationToken* operation) const {
    return operation != nullptr && operation->manager_ == this && operation->generation_ == frame_operation_generation_;
}

BufferPoolManager::FrameOperationToken BufferPoolManager::acquire_frame_operation(size_t minimum_available_frames) {
    if (minimum_available_frames > pool_size_) {
        throw InternalError("buffer pool frame operation reservation exceeds pool size");
    }
    std::unique_lock lock{latch_};
    frame_operation_cv_.wait(
        lock, [&] { return !frame_operation_active_ && available_frames_locked() >= minimum_available_frames; });
    frame_operation_active_ = true;
    ++frame_operation_generation_;
    return FrameOperationToken(this, frame_operation_generation_);
}

void BufferPoolManager::release_frame_operation(uint64_t generation) noexcept {
    {
        std::unique_lock lock{latch_};
        if (!frame_operation_active_ || generation != frame_operation_generation_) {
            return;
        }
        frame_operation_active_ = false;
    }
    frame_operation_cv_.notify_all();
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
    std::vector<frame_id_t> blocked;
    blocked.reserve(pool_size_);
    frame_id_t candidate = INVALID_FRAME_ID;
    while (blocked.size() < pool_size_ && replacer_->victim(&candidate)) {
        const Page& page = pages_[candidate];
        if (!page.is_dirty_.load(std::memory_order_acquire) || !IsValidPageId(page.id_) ||
            !index_smo_blocked_locked(page.id_.fd)) {
            for (frame_id_t fid : blocked) {
                replacer_->unpin(fid);
            }
            return candidate;
        }
        blocked.push_back(candidate);
    }
    for (frame_id_t fid : blocked) {
        replacer_->unpin(fid);
    }
    return INVALID_FRAME_ID;
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
        Page* target_page = nullptr;
        Page* wait_page = nullptr;
        PageId old_page_id;
        bool old_page_dirty = false;
        uint64_t old_dirty_epoch = 0;
        PageWriteDependency old_dependency = PageWriteDependency::None();
        frame_id_t fid = INVALID_FRAME_ID;

        {
            std::shared_lock lock{latch_};
            if (frame_operation_active_ && !operation_authorized_locked(operation)) {
                lock.unlock();
                std::unique_lock operation_lock{latch_};
                frame_operation_cv_.wait(operation_lock, [&] { return !frame_operation_active_; });
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
            frame_operation_cv_.wait(
                lock, [&] { return !frame_operation_active_ || operation_authorized_locked(operation); });
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
                clear_residency(fid);
                old_page_id = target_page->id_;
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    old_page_dirty = target_page->is_dirty_.load(std::memory_order_acquire);
                    old_dirty_epoch = target_page->dirty_epoch_.load(std::memory_order_acquire);
                    old_dependency = target_page->write_dependency_;
                    target_page->is_dirty_ = false;
                    target_page->dirty_epoch_ = 0;
                    target_page->write_dependency_ = PageWriteDependency::None();
                }
                if (IsValidPageId(old_page_id)) {
                    page_table_.erase(old_page_id);
                }
                if (old_page_dirty && IsValidPageId(old_page_id)) {
                    claim_index_file_write_locked(old_page_id.fd);
                }
                target_page->state_.store(FrameState::EVICTING, std::memory_order_release);
                target_page->id_ = page_id;
                target_page->pin_count_ = 1;
                target_page->state_.store(FrameState::LOADING, std::memory_order_release);
                page_table_.insert_or_assign(page_id, fid);
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
        try {
            std::unique_lock<std::shared_mutex> page_lock(target_page->latch_);
            if (old_page_dirty && IsValidPageId(old_page_id)) {
                ensure_write_dependency(old_dependency);
                disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, target_page->data_, PAGE_SIZE);
                old_page_write_succeeded = true;
            }
            target_page->reset_memory();
            disk_manager_->read_page(page_id.fd, page_id.page_no, target_page->data_, PAGE_SIZE);
            loaded = true;
        } catch (...) {
            loaded = false;
        }

        {
            std::scoped_lock lock{latch_};
            auto hit = page_table_.find(page_id);
            if (loaded && hit != page_table_.end() && hit->second == fid && target_page->id_ == page_id) {
                target_page->state_.store(FrameState::VALID, std::memory_order_release);
            } else if (!old_page_write_succeeded && IsValidPageId(old_page_id)) {
                // The frame still contains the old page image. Restore its
                // ownership instead of discarding the only dirty copy.
                if (hit != page_table_.end() && hit->second == fid) {
                    page_table_.erase(hit);
                }
                target_page->id_ = old_page_id;
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    target_page->is_dirty_ = old_page_dirty;
                    target_page->dirty_epoch_ = old_dirty_epoch;
                    target_page->write_dependency_ = old_dependency;
                }
                target_page->pin_count_ = 0;
                target_page->state_.store(FrameState::VALID, std::memory_order_release);
                page_table_.insert_or_assign(old_page_id, fid);
                if (residency_classes_[fid] == ResidencyClass::Normal) {
                    replacer_->unpin(fid);
                }
            } else {
                if (hit != page_table_.end() && hit->second == fid) {
                    page_table_.erase(hit);
                }
                target_page->reset_memory();
                target_page->id_ = PageId{};
                {
                    std::scoped_lock dirty_lock{target_page->dirty_latch_};
                    target_page->is_dirty_ = false;
                    target_page->dirty_epoch_ = 0;
                    target_page->write_dependency_ = PageWriteDependency::None();
                }
                target_page->pin_count_ = 0;
                target_page->state_.store(FrameState::FREE, std::memory_order_release);
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
                page->state_.store(FrameState::FLUSHING, std::memory_order_release);
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
                if (page->dirty_epoch_.load(std::memory_order_acquire) == flushed_dirty_epoch) {
                    page->is_dirty_ = false;
                    page->write_dependency_ = PageWriteDependency::None();
                }
            }
            page->state_.store(FrameState::VALID, std::memory_order_release);
            std::scoped_lock pin_lock{page->pin_latch_};
            if (page->pin_count_ == 0 && residency_classes_[fid] == ResidencyClass::Normal) {
                replacer_->unpin(fid);
            }
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
        frame_operation_cv_.wait(lock,
                                 [&] { return !frame_operation_active_ || operation_authorized_locked(operation); });
        fid = take_free_frame();
        if (fid == INVALID_FRAME_ID) {
            fid = take_unblocked_victim_locked();
            if (fid == INVALID_FRAME_ID) {
                return nullptr;
            }
        }

        page_id->page_no = disk_manager_->allocate_page(page_id->fd);
        page = &pages_[fid];
        clear_residency(fid);
        old_page_id = page->id_;
        {
            std::scoped_lock dirty_lock{page->dirty_latch_};
            old_page_dirty = page->is_dirty_.load(std::memory_order_acquire);
            old_dirty_epoch = page->dirty_epoch_.load(std::memory_order_acquire);
            old_dependency = page->write_dependency_;
            page->is_dirty_ = false;
            page->dirty_epoch_ = 0;
            page->write_dependency_ = PageWriteDependency::None();
        }
        if (IsValidPageId(old_page_id)) {
            page_table_.erase(old_page_id);
        }
        if (old_page_dirty && IsValidPageId(old_page_id)) {
            claim_index_file_write_locked(old_page_id.fd);
        }
        page->state_.store(FrameState::EVICTING, std::memory_order_release);
        page->id_ = *page_id;
        page->pin_count_ = 1;
        page->state_.store(FrameState::LOADING, std::memory_order_release);
        page_table_.insert_or_assign(*page_id, fid);
    }

    bool initialized = false;
    bool old_page_write_succeeded = !old_page_dirty || !IsValidPageId(old_page_id);
    try {
        std::unique_lock<std::shared_mutex> page_lock(page->latch_);
        if (old_page_dirty && IsValidPageId(old_page_id)) {
            ensure_write_dependency(old_dependency);
            disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, page->data_, PAGE_SIZE);
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
            page->state_.store(FrameState::VALID, std::memory_order_release);
        } else if (!old_page_write_succeeded && IsValidPageId(old_page_id)) {
            if (hit != page_table_.end() && hit->second == fid) {
                page_table_.erase(hit);
            }
            page->id_ = old_page_id;
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                page->is_dirty_ = old_page_dirty;
                page->dirty_epoch_ = old_dirty_epoch;
                page->write_dependency_ = old_dependency;
            }
            page->pin_count_ = 0;
            page->state_.store(FrameState::VALID, std::memory_order_release);
            page_table_.insert_or_assign(old_page_id, fid);
            if (residency_classes_[fid] == ResidencyClass::Normal) {
                replacer_->unpin(fid);
            }
        } else {
            if (hit != page_table_.end() && hit->second == fid) {
                page_table_.erase(hit);
            }
            page->reset_memory();
            page->id_ = PageId{};
            {
                std::scoped_lock dirty_lock{page->dirty_latch_};
                page->is_dirty_ = false;
                page->dirty_epoch_ = 0;
                page->write_dependency_ = PageWriteDependency::None();
            }
            page->pin_count_ = 0;
            page->state_.store(FrameState::FREE, std::memory_order_release);
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
                replacer_->pin(fid);
                std::scoped_lock pin_lock{page->pin_latch_};
                if (page->pin_count_ != 0) {
                    if (residency_classes_[fid] == ResidencyClass::Normal) {
                        replacer_->unpin(fid);
                    }
                    return false;
                }
                page->state_.store(FrameState::EVICTING, std::memory_order_release);
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
                page_table_.erase(hit);
                page->reset_memory();
                page->id_ = PageId{};
                page->pin_count_ = 0;
                {
                    std::scoped_lock dirty_lock{page->dirty_latch_};
                    page->is_dirty_ = false;
                    page->dirty_epoch_ = 0;
                    page->write_dependency_ = PageWriteDependency::None();
                }
                page->state_.store(FrameState::FREE, std::memory_order_release);
                clear_residency(fid);
                recycle_frame(fid);
            } else if (!deleted && hit != page_table_.end() && hit->second == fid) {
                page->state_.store(FrameState::VALID, std::memory_order_release);
                if (residency_classes_[fid] == ResidencyClass::Normal) {
                    replacer_->unpin(fid);
                }
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

BufferPoolManager::FlushBatchResult BufferPoolManager::flush_pages(std::vector<PageId>& page_ids,
                                                                   FlushDependencyPolicy policy) {
    FlushBatchResult result;
    if (page_ids.empty()) {
        return result;
    }

    constexpr size_t kClaimPages = 64;
    struct ClaimedPage {
        PageId page_id;
        frame_id_t frame_id;
        uint64_t dirty_epoch{0};
        PageWriteDependency dependency{PageWriteDependency::None()};
    };

    // Sorted so that runs of adjacent page numbers become single pwrites.
    std::sort(page_ids.begin(), page_ids.end(), [](const PageId& lhs, const PageId& rhs) {
        if (lhs.fd != rhs.fd) {
            return lhs.fd < rhs.fd;
        }
        return lhs.page_no < rhs.page_no;
    });

    const std::vector<PageId>& candidates = page_ids;
    // Grown, never shrunk, and never larger than the batch actually claimed. A
    // checkpoint reaches the full kClaimPages once and keeps it; an index
    // structure change publishes a handful of pages and must not pay for
    // zero-filling 256 KiB it will not use.
    std::vector<char> image;
    std::vector<ClaimedPage> claimed;
    claimed.reserve(kClaimPages);
    bool success = true;
    for (size_t candidate_begin = 0; candidate_begin < candidates.size();) {
        claimed.clear();
        {
            std::unique_lock lock{latch_};
            while (candidate_begin < candidates.size() && claimed.size() < kClaimPages) {
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
                page->state_.store(FrameState::FLUSHING, std::memory_order_release);
                claim_index_file_write_locked(page_id.fd);
                claimed.push_back(ClaimedPage{page_id, hit->second});
            }
        }

        size_t claimed_begin = 0;
        if (image.size() < claimed.size() * PAGE_SIZE) {
            image.resize(claimed.size() * PAGE_SIZE);
        }
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
                        std::scoped_lock dirty_lock{page->dirty_latch_};
                        if (page->dirty_epoch_.load(std::memory_order_acquire) == claimed_page.dirty_epoch) {
                            page->is_dirty_ = false;
                            page->write_dependency_ = PageWriteDependency::None();
                        }
                    }
                    page->state_.store(FrameState::VALID, std::memory_order_release);
                    std::scoped_lock pin_lock{page->pin_latch_};
                    if (page->pin_count_ == 0 && residency_classes_[claimed_page.frame_id] == ResidencyClass::Normal) {
                        replacer_->unpin(claimed_page.frame_id);
                    }
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
        size_t frames_examined = 0;
        while (frames_examined < pool_size_ && pages_to_flush.size() < max_pages) {
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
    for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
        const Page& page = pages_[frame_id];
        if (page.state_.load(std::memory_order_acquire) == FrameState::VALID &&
            fd_set.find(page.id_.fd) != fd_set.end() && page.is_dirty_.load(std::memory_order_acquire)) {
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

        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
            ++it;
            continue;
        }

        replacer_->pin(frame_id);
        clear_residency(frame_id);
        recycle_frame(frame_id);
        page->reset_memory();
        page->pin_count_ = 0;
        {
            std::scoped_lock dirty_lock{page->dirty_latch_};
            page->is_dirty_ = false;
            page->dirty_epoch_ = 0;
            page->write_dependency_ = PageWriteDependency::None();
        }
        page->id_ = PageId{};
        page->state_.store(FrameState::FREE, std::memory_order_release);
        it = page_table_.erase(it);
    }
    lock.unlock();
    frame_operation_cv_.notify_all();
}
