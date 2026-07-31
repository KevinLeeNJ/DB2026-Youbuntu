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

BufferPoolObservabilitySnapshot BufferPoolManager::observability_snapshot() const {
    return {fetch_miss_.load(std::memory_order_relaxed),       inflight_wait_.load(std::memory_order_relaxed),
            inflight_wait_ns_.load(std::memory_order_relaxed), no_victim_.load(std::memory_order_relaxed),
            eviction_clean_.load(std::memory_order_relaxed),   eviction_dirty_.load(std::memory_order_relaxed)};
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
    residency_classes_[frame_id] = ResidencyClass::Normal;
}

void BufferPoolManager::reset_preflush_state(Page* page) {
    page->preflush_generation_ = 0;
    page->preflush_attempted_ = false;
}

void BufferPoolManager::flush_log_before_page_write(lsn_t page_lsn) {
    if (log_manager_ != nullptr) {
        log_manager_->flush_log_to_disk_up_to(page_lsn);
    }
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
    while (true) {
        Page* target_page = nullptr;
        Page* wait_page = nullptr;
        PageId old_page_id;
        bool old_page_dirty = false;
        frame_id_t fid = INVALID_FRAME_ID;

        {
            std::shared_lock lock{latch_};
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
            const auto wait_begin = std::chrono::steady_clock::now();
            inflight_wait_.fetch_add(1, std::memory_order_relaxed);
            std::unique_lock<std::mutex> wait_lock(wait_page->io_latch_);
            wait_page->io_cv_.wait_for(wait_lock, std::chrono::milliseconds(1), [wait_page] {
                FrameState state = wait_page->state_.load(std::memory_order_acquire);
                return state == FrameState::FREE || state == FrameState::VALID;
            });
            inflight_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                  std::chrono::steady_clock::now() - wait_begin)
                                                                  .count()),
                                        std::memory_order_relaxed);
            continue;
        }

        {
            std::unique_lock lock{latch_};
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
                fetch_miss_.fetch_add(1, std::memory_order_relaxed);
                fid = take_free_frame();
                if (fid == INVALID_FRAME_ID) {
                    if (!replacer_->victim(&fid)) {
                        no_victim_.fetch_add(1, std::memory_order_relaxed);
                        return nullptr;
                    }
                    if (pages_[fid].is_dirty_.load(std::memory_order_acquire)) {
                        eviction_dirty_.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        eviction_clean_.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                target_page = &pages_[fid];
                clear_residency(fid);
                old_page_id = target_page->id_;
                old_page_dirty = target_page->is_dirty_;
                if (IsValidPageId(old_page_id)) {
                    page_table_.erase(old_page_id);
                }
                target_page->state_.store(FrameState::EVICTING, std::memory_order_release);
                target_page->id_ = page_id;
                target_page->is_dirty_ = false;
                target_page->dirty_epoch_ = 0;
                reset_preflush_state(target_page);
                target_page->pin_count_ = 1;
                target_page->state_.store(FrameState::LOADING, std::memory_order_release);
                page_table_.insert_or_assign(page_id, fid);
            }
        }

        if (wait_page != nullptr) {
            const auto wait_begin = std::chrono::steady_clock::now();
            inflight_wait_.fetch_add(1, std::memory_order_relaxed);
            std::unique_lock<std::mutex> wait_lock(wait_page->io_latch_);
            wait_page->io_cv_.wait_for(wait_lock, std::chrono::milliseconds(1), [wait_page] {
                FrameState state = wait_page->state_.load(std::memory_order_acquire);
                return state == FrameState::FREE || state == FrameState::VALID;
            });
            inflight_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                  std::chrono::steady_clock::now() - wait_begin)
                                                                  .count()),
                                        std::memory_order_relaxed);
            continue;
        }

        bool loaded = false;
        bool old_page_write_succeeded = !old_page_dirty || !IsValidPageId(old_page_id);
        try {
            std::unique_lock<std::shared_mutex> page_lock(target_page->latch_);
            if (old_page_dirty && IsValidPageId(old_page_id)) {
                flush_log_before_page_write(target_page->get_page_lsn());
                disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, target_page->data_, PAGE_SIZE);
                old_page_write_succeeded = true;
            }
            target_page->reset_memory();
            disk_manager_->read_page(page_id.fd, page_id.page_no, target_page->data_, PAGE_SIZE);
            target_page->is_dirty_ = false;
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
                mark_dirty(target_page);
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
                target_page->is_dirty_ = false;
                target_page->pin_count_ = 0;
                target_page->state_.store(FrameState::FREE, std::memory_order_release);
                clear_residency(fid);
                recycle_frame(fid);
            }
        }
        target_page->io_cv_.notify_all();
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

void BufferPoolManager::mark_resident(PageId page_id, ResidencyClass residency_class) {
    std::unique_lock lock{latch_};
    auto hit = page_table_.find(page_id);
    if (hit == page_table_.end()) {
        return;
    }
    Page* page = &pages_[hit->second];
    if (page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
        return;
    }
    ResidencyClass& current = residency_classes_[hit->second];
    if (current == residency_class) {
        return;
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
    current = ResidencyClass::Normal;
    if (page->state_.load(std::memory_order_acquire) == FrameState::VALID) {
        std::scoped_lock pin_lock{page->pin_latch_};
        if (page->pin_count_ == 0) {
            replacer_->unpin(hit->second);
        }
    }
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
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
        mark_dirty(targetPage);
    }
    // 2.1 若pin_count_已经等于0,则返回false
    if (targetPage->pin_count_ == 0)
        return false;

    // 2.2 若pin_count_大于0，则pin_count_自减一
    --targetPage->pin_count_;
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    if (targetPage->pin_count_ == 0 && state == FrameState::VALID &&
        residency_classes_[fid] == ResidencyClass::Normal) {
        replacer_->unpin(fid);
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
            std::scoped_lock lock{latch_};
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
    lsn_t flushed_lsn = INVALID_LSN;
    uint64_t flushed_dirty_epoch = 0;
    try {
        std::shared_lock<std::shared_mutex> page_lock(page->latch_);
        flushed_lsn = page->get_page_lsn();
        flushed_dirty_epoch = page->dirty_epoch_.load(std::memory_order_acquire);
        flush_log_before_page_write(flushed_lsn);
        disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
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
                }
            }
            page->state_.store(FrameState::VALID, std::memory_order_release);
            std::scoped_lock pin_lock{page->pin_latch_};
            if (page->pin_count_ == 0 && residency_classes_[fid] == ResidencyClass::Normal) {
                replacer_->unpin(fid);
            }
        }
    }
    page->io_cv_.notify_all();
    return flushed;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    Page* page = nullptr;
    PageId old_page_id;
    bool old_page_dirty = false;
    frame_id_t fid = INVALID_FRAME_ID;
    {
        std::scoped_lock lock{latch_};
        fid = take_free_frame();
        if (fid == INVALID_FRAME_ID) {
            if (!replacer_->victim(&fid)) {
                no_victim_.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }
            if (pages_[fid].is_dirty_.load(std::memory_order_acquire)) {
                eviction_dirty_.fetch_add(1, std::memory_order_relaxed);
            } else {
                eviction_clean_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        page_id->page_no = disk_manager_->allocate_page(page_id->fd);
        page = &pages_[fid];
        clear_residency(fid);
        old_page_id = page->id_;
        old_page_dirty = page->is_dirty_;
        if (IsValidPageId(old_page_id)) {
            page_table_.erase(old_page_id);
        }
        page->state_.store(FrameState::EVICTING, std::memory_order_release);
        page->id_ = *page_id;
        page->is_dirty_ = false;
        page->dirty_epoch_ = 0;
        reset_preflush_state(page);
        page->pin_count_ = 1;
        page->state_.store(FrameState::LOADING, std::memory_order_release);
        page_table_.insert_or_assign(*page_id, fid);
    }

    bool initialized = false;
    bool old_page_write_succeeded = !old_page_dirty || !IsValidPageId(old_page_id);
    try {
        std::unique_lock<std::shared_mutex> page_lock(page->latch_);
        if (old_page_dirty && IsValidPageId(old_page_id)) {
            flush_log_before_page_write(page->get_page_lsn());
            disk_manager_->write_page(old_page_id.fd, old_page_id.page_no, page->data_, PAGE_SIZE);
            old_page_write_succeeded = true;
        }
        page->reset_memory();
        page->is_dirty_ = false;
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
            mark_dirty(page);
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
            page->is_dirty_ = false;
            page->pin_count_ = 0;
            page->state_.store(FrameState::FREE, std::memory_order_release);
            clear_residency(fid);
            recycle_frame(fid);
        }
    }
    page->io_cv_.notify_all();
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
            std::scoped_lock lock{latch_};
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
        try {
            std::unique_lock<std::shared_mutex> page_lock(page->latch_);
            flush_log_before_page_write(page->get_page_lsn());
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
                page->is_dirty_ = false;
                page->state_.store(FrameState::FREE, std::memory_order_release);
                clear_residency(fid);
                recycle_frame(fid);
            } else if (!deleted && hit != page_table_.end() && hit->second == fid) {
                page->state_.store(FrameState::VALID, std::memory_order_release);
                if (residency_classes_[fid] == ResidencyClass::Normal) {
                    replacer_->unpin(fid);
                }
            }
        }
        page->io_cv_.notify_all();
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
    return flush_all_pages(fds, false);
}

bool BufferPoolManager::flush_all_pages(const std::vector<int>& fds, bool wal_preflushed) {
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
    return flush_pages(candidates, wal_preflushed).success;
}

BufferPoolManager::FlushBatchResult BufferPoolManager::flush_pages(std::vector<PageId>& page_ids, bool wal_preflushed,
                                                                   bool skip_pinned) {
    FlushBatchResult result;
    if (page_ids.empty()) {
        return result;
    }

    constexpr size_t kClaimPages = 64;
    struct ClaimedPage {
        PageId page_id;
        frame_id_t frame_id;
        uint64_t dirty_epoch;
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
                auto hit = page_table_.find(page_id);
                if (hit == page_table_.end()) {
                    continue;
                }
                Page* page = &pages_[hit->second];
                if (page->state_.load(std::memory_order_acquire) != FrameState::VALID ||
                    !page->is_dirty_.load(std::memory_order_acquire)) {
                    continue;
                }
                std::scoped_lock pin_lock{page->pin_latch_};
                if (skip_pinned && page->pin_count_ != 0) {
                    continue;
                }
                replacer_->pin(hit->second);
                page->state_.store(FrameState::FLUSHING, std::memory_order_release);
                claimed.push_back(
                    ClaimedPage{page_id, hit->second, page->dirty_epoch_.load(std::memory_order_acquire)});
            }
        }

        if (image.size() < claimed.size() * PAGE_SIZE) {
            image.resize(claimed.size() * PAGE_SIZE);
        }

        // Capture the complete batch before doing any write. Apart from making
        // the write image independent from foreground mutations, this gives us
        // one WAL high-water mark for the whole batch. The old implementation
        // computed that mark once per contiguous run, so a sparse batch could
        // force the same WAL buffer repeatedly (and, in STRICT mode, repeat
        // fdatasync) before writing its next page.
        bool copied = true;
        lsn_t max_page_lsn = INVALID_LSN;
        for (size_t i = 0; i < claimed.size(); ++i) {
            Page* page = &pages_[claimed[i].frame_id];
            try {
                std::shared_lock page_lock(page->latch_);
                std::memcpy(image.data() + i * PAGE_SIZE, page->data_, PAGE_SIZE);
                max_page_lsn = std::max(max_page_lsn, page->get_page_lsn());
            } catch (...) {
                copied = false;
                break;
            }
        }
        if (copied) {
            try {
                if (!wal_preflushed) {
                    flush_log_before_page_write(max_page_lsn);
                }
            } catch (...) {
                copied = false;
            }
        }

        const auto finish_claimed = [&](size_t begin, size_t end, bool written) {
            std::unique_lock lock{latch_};
            for (size_t i = begin; i < end; ++i) {
                const auto& claimed_page = claimed[i];
                Page* page = &pages_[claimed_page.frame_id];
                auto hit = page_table_.find(claimed_page.page_id);
                if (hit == page_table_.end() || hit->second != claimed_page.frame_id ||
                    page->state_.load(std::memory_order_acquire) != FrameState::FLUSHING) {
                    continue;
                }
                if (written) {
                    std::scoped_lock dirty_lock{page->dirty_latch_};
                    if (page->dirty_epoch_.load(std::memory_order_acquire) == claimed_page.dirty_epoch) {
                        page->is_dirty_ = false;
                    }
                }
                page->state_.store(FrameState::VALID, std::memory_order_release);
                std::scoped_lock pin_lock{page->pin_latch_};
                if (page->pin_count_ == 0 && residency_classes_[claimed_page.frame_id] == ResidencyClass::Normal) {
                    replacer_->unpin(claimed_page.frame_id);
                }
                page->io_cv_.notify_all();
            }
        };

        if (!copied) {
            finish_claimed(0, claimed.size(), false);
            success = false;
            continue;
        }

        size_t claimed_begin = 0;
        while (claimed_begin < claimed.size()) {
            size_t claimed_end = claimed_begin + 1;
            while (claimed_end < claimed.size() &&
                   claimed[claimed_end].page_id.fd == claimed[claimed_begin].page_id.fd &&
                   claimed[claimed_end].page_id.page_no == claimed[claimed_end - 1].page_id.page_no + 1) {
                ++claimed_end;
            }

            const size_t page_count = claimed_end - claimed_begin;
            bool written = true;
            try {
                disk_manager_->write_page(claimed[claimed_begin].page_id.fd, claimed[claimed_begin].page_id.page_no,
                                          image.data() + claimed_begin * PAGE_SIZE,
                                          static_cast<int>(page_count * PAGE_SIZE));
            } catch (...) {
                written = false;
            }
            finish_claimed(claimed_begin, claimed_end, written);
            success = written && success;
            if (written) {
                result.pages_written += page_count;
            } else {
                // Keep the remaining claims dirty and immediately make them
                // available again; a later flush can retry without losing the
                // stable images that were already written above.
                finish_claimed(claimed_end, claimed.size(), false);
                break;
            }
            claimed_begin = claimed_end;
        }
    }
    result.success = success;
    return result;
}

void BufferPoolManager::log_pool_stats() {
    static const bool enabled = [] {
        const char* value = std::getenv("RMDB_LOG_BPM_STATS");
        return value != nullptr && std::string(value) == "1";
    }();
    if (!enabled) {
        return;
    }
    // The preflush loop calls in bursts; one line per second is what makes the
    // resident/pinned counts readable as a time series.
    static std::atomic<int64_t> next_log_ns{0};
    const auto now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    int64_t expected_ns = next_log_ns.load(std::memory_order_relaxed);
    if (now_ns < expected_ns ||
        !next_log_ns.compare_exchange_strong(expected_ns, now_ns + 1'000'000'000, std::memory_order_relaxed)) {
        return;
    }

    size_t resident_pages = 0;
    size_t index_internal_frames = 0;
    size_t pinned_frames = 0;
    size_t dirty_frames = 0;
    {
        std::shared_lock lock{latch_};
        resident_pages = page_table_.size();
        for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
            if (residency_classes_[frame_id] == ResidencyClass::IndexInternal) {
                ++index_internal_frames;
            }
            const Page& page = pages_[frame_id];
            if (page.state_.load(std::memory_order_acquire) != FrameState::VALID) {
                continue;
            }
            // pin_count_ is read without its latch: this is a diagnostic, and a
            // torn read costs one unit of accuracy in a number that is only ever
            // compared by order of magnitude.
            if (page.pin_count_ > 0) {
                ++pinned_frames;
            }
            if (page.is_dirty_.load(std::memory_order_acquire)) {
                ++dirty_frames;
            }
        }
    }
    // WARN, not INFO: the server runs at WARN outside recovery (rmdb.cpp sets the
    // level), so an INFO line here would be silently discarded. The env gate is
    // what keeps this quiet by default.
    LOG_WARN("bpm stats: %zu/%zu frames resident, %zu evictable, %zu pinned, %zu index-internal, %zu dirty, "
             "%lu page reads, %lu page writes",
             resident_pages, pool_size_, replacer_->Size(), pinned_frames, index_internal_frames, dirty_frames,
             static_cast<unsigned long>(disk_manager_->get_page_read_count()),
             static_cast<unsigned long>(disk_manager_->get_page_write_count()));
    // A benchmark run ends in SIGKILL, which throws away the logger's 1 MiB
    // buffer. Without this the whole time series is lost.
    minilog::Logger::get().flush();
}

size_t BufferPoolManager::flush_dirty_pages(size_t max_pages) {
    const std::unordered_set<int> no_index_fds;
    return flush_dirty_pages(max_pages, no_index_fds);
}

size_t BufferPoolManager::flush_selected_pages(std::vector<PageId>& pages_to_flush,
                                               const std::unordered_set<int>& index_fds, bool skip_pinned) {
    std::vector<PageId> table_pages;
    std::vector<PageId> index_pages;
    table_pages.reserve(pages_to_flush.size());
    index_pages.reserve(pages_to_flush.size());
    for (const PageId& page_id : pages_to_flush) {
        if (index_fds.count(page_id.fd) > 0) {
            index_pages.push_back(page_id);
        } else {
            table_pages.push_back(page_id);
        }
    }

    const auto table_result = flush_pages(table_pages, /*wal_preflushed=*/false, skip_pinned);
    const auto index_result = flush_pages(index_pages, /*wal_preflushed=*/true, skip_pinned);
    return table_result.pages_written + index_result.pages_written;
}

size_t BufferPoolManager::flush_dirty_pages(size_t max_pages, const std::unordered_set<int>& index_fds) {
    log_pool_stats();
    if (max_pages == 0 || pool_size_ == 0) {
        return 0;
    }

    // Continue from the prior batch instead of restarting at the beginning of
    // page_table_. A hot page can be dirtied again immediately after writeback;
    // restarting would repeatedly select that prefix and starve older dirty
    // frames, causing write amplification without shrinking the dirty set.
    std::lock_guard<std::mutex> flush_lock(dirty_flush_latch_);
    std::vector<PageId> pages_to_flush;
    {
        std::shared_lock lock{latch_};
        pages_to_flush.reserve(std::min(max_pages, page_table_.size()));
        size_t frames_scanned = 0;
        while (frames_scanned < pool_size_ && pages_to_flush.size() < max_pages) {
            const size_t frame_id = (dirty_flush_cursor_ + frames_scanned) % pool_size_;
            ++frames_scanned;
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID &&
                page->is_dirty_.load(std::memory_order_acquire)) {
                pages_to_flush.push_back(page->id_);
            }
        }
        dirty_flush_cursor_ = (dirty_flush_cursor_ + frames_scanned) % pool_size_;
    }

    return flush_selected_pages(pages_to_flush, index_fds, /*skip_pinned=*/false);
}

PreflushBatchResult BufferPoolManager::preflush_dirty_pages(size_t max_pages, const std::unordered_set<int>& index_fds,
                                                            uint64_t generation) {
    log_pool_stats();
    if (max_pages == 0 || pool_size_ == 0) {
        return {};
    }

    std::lock_guard<std::mutex> flush_lock(dirty_flush_latch_);
    if (preflush_scan_generation_ != generation || preflush_scan_remaining_ == 0) {
        preflush_scan_generation_ = generation;
        preflush_scan_remaining_ = pool_size_;
    }

    // A bounded scan keeps a sparse dirty set from taking the buffer-pool latch
    // for a full multi-gigabyte pool on every 100 ms scheduler tick.
    constexpr size_t kFramesPerBatchPage = 4;
    constexpr size_t kExhaustiveBatchPages = 64;
    const size_t scan_budget =
        max_pages < kExhaustiveBatchPages
            ? pool_size_
            : (max_pages > pool_size_ / kFramesPerBatchPage ? pool_size_
                                                            : std::min(pool_size_, max_pages * kFramesPerBatchPage));
    const size_t frames_to_scan = std::min(scan_budget, preflush_scan_remaining_);
    const bool exhaustive_scan = scan_budget == pool_size_;
    std::vector<PageId> pages_to_flush;
    pages_to_flush.reserve(std::min(max_pages, page_table_.size()));
    size_t frames_scanned = 0;
    {
        std::shared_lock lock{latch_};
        while (frames_scanned < frames_to_scan && (exhaustive_scan || pages_to_flush.size() < max_pages)) {
            const size_t frame_id = (dirty_flush_cursor_ + frames_scanned) % pool_size_;
            ++frames_scanned;
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) != FrameState::VALID ||
                !page->is_dirty_.load(std::memory_order_acquire)) {
                continue;
            }

            if (page->preflush_generation_ != generation) {
                page->preflush_generation_ = generation;
                page->preflush_attempted_ = false;
            }
            if (page->preflush_attempted_) {
                continue;
            }

            // Do not turn a page currently used by a foreground operation into
            // an I/O wait point. It remains eligible for the next scan.
            std::scoped_lock pin_lock{page->pin_latch_};
            if (page->pin_count_ != 0) {
                continue;
            }
            if (pages_to_flush.size() < max_pages) {
                page->preflush_attempted_ = true;
                pages_to_flush.push_back(page->id_);
            }
        }
    }
    dirty_flush_cursor_ = (dirty_flush_cursor_ + frames_scanned) % pool_size_;
    preflush_scan_remaining_ -= frames_scanned;

    const bool scan_complete = preflush_scan_remaining_ == 0;
    if (scan_complete) {
        preflush_scan_remaining_ = pool_size_;
    }
    return {flush_selected_pages(pages_to_flush, index_fds, /*skip_pinned=*/true), scan_complete};
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
        page->is_dirty_ = false;
        page->id_ = PageId{};
        page->state_.store(FrameState::FREE, std::memory_order_release);
        it = page_table_.erase(it);
    }
}
