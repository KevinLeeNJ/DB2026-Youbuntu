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

#include "recovery/log_manager.h"

namespace {

bool IsValidPageId(const PageId& page_id) {
    return page_id.fd >= 0 && page_id.page_no != INVALID_PAGE_ID;
}

} // namespace

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
            const bool collect_metrics = metrics_enabled();
            const auto wait_begin =
                collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            std::shared_lock lock{latch_};
            if (collect_metrics) {
                page_table_shared_wait_ns_.fetch_add(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - wait_begin)
                                              .count()),
                    std::memory_order_relaxed);
            }
            auto hit = page_table_.find(page_id);
            if (hit != page_table_.end()) {
                if (collect_metrics) {
                    fetch_hits_.fetch_add(1, std::memory_order_relaxed);
                }
                target_page = &pages_[hit->second];
                if (target_page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                    wait_page = target_page;
                } else {
                    std::scoped_lock pin_lock{target_page->pin_latch_};
                    if (target_page->pin_count_ == 0) {
                        replacer_->pin(hit->second);
                        if (collect_metrics) {
                            pin_0_to_1_.fetch_add(1, std::memory_order_relaxed);
                        }
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
            const bool collect_metrics = metrics_enabled();
            const auto wait_begin =
                collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
            std::unique_lock lock{latch_};
            if (collect_metrics) {
                page_table_exclusive_wait_ns_.fetch_add(
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - wait_begin)
                                              .count()),
                    std::memory_order_relaxed);
            }
            auto hit = page_table_.find(page_id);
            if (hit != page_table_.end()) {
                if (collect_metrics) {
                    fetch_hits_.fetch_add(1, std::memory_order_relaxed);
                }
                target_page = &pages_[hit->second];
                if (target_page->state_.load(std::memory_order_acquire) != FrameState::VALID) {
                    wait_page = target_page;
                } else {
                    std::scoped_lock pin_lock{target_page->pin_latch_};
                    if (target_page->pin_count_ == 0) {
                        replacer_->pin(hit->second);
                        if (collect_metrics) {
                            pin_0_to_1_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    ++target_page->pin_count_;
                    return target_page;
                }
            } else {
                if (collect_metrics) {
                    fetch_misses_.fetch_add(1, std::memory_order_relaxed);
                }
                if (!free_list_.empty()) {
                    fid = free_list_.front();
                    free_list_.pop_front();
                } else if (!replacer_->victim(&fid)) {
                    return nullptr;
                }

                target_page = &pages_[fid];
                old_page_id = target_page->id_;
                old_page_dirty = target_page->is_dirty_;
                if (IsValidPageId(old_page_id)) {
                    page_table_.erase(old_page_id);
                }
                target_page->state_.store(FrameState::EVICTING, std::memory_order_release);
                target_page->id_ = page_id;
                target_page->is_dirty_ = false;
                target_page->dirty_epoch_ = 0;
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
                replacer_->unpin(fid);
            } else {
                if (hit != page_table_.end() && hit->second == fid) {
                    page_table_.erase(hit);
                }
                target_page->reset_memory();
                target_page->id_ = PageId{};
                target_page->is_dirty_ = false;
                target_page->pin_count_ = 0;
                target_page->state_.store(FrameState::FREE, std::memory_order_release);
                free_list_.push_back(fid);
            }
        }
        target_page->io_cv_.notify_all();
        return loaded ? target_page : nullptr;
    }
}

bool BufferPoolManager::is_page_resident(PageId page_id) {
    const bool collect_metrics = metrics_enabled();
    const auto wait_begin =
        collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    std::shared_lock lock{latch_};
    if (collect_metrics) {
        page_table_shared_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                       std::chrono::steady_clock::now() - wait_begin)
                                                                       .count()),
                                             std::memory_order_relaxed);
    }
    auto hit = page_table_.find(page_id);
    return hit != page_table_.end() && pages_[hit->second].state_.load(std::memory_order_acquire) == FrameState::VALID;
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
    const bool collect_metrics = metrics_enabled();
    const auto wait_begin =
        collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    std::shared_lock lock{latch_};
    if (collect_metrics) {
        page_table_shared_wait_ns_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                       std::chrono::steady_clock::now() - wait_begin)
                                                                       .count()),
                                             std::memory_order_relaxed);
    }
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
    // 2.1 若pin_count_已经等于0,则返回false
    if (targetPage->pin_count_ == 0)
        return false;

    // 2.2 若pin_count_大于0，则pin_count_自减一
    --targetPage->pin_count_;
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    if (targetPage->pin_count_ == 0 && state == FrameState::VALID) {
        replacer_->unpin(fid);
        if (collect_metrics) {
            unpin_1_to_0_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    // 3 根据参数is_dirty，更改P的is_dirty_
    if (is_dirty == true) {
        mark_dirty(targetPage);
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
            if (page->pin_count_ == 0) {
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
        if (!free_list_.empty()) {
            fid = free_list_.front();
            free_list_.pop_front();
        } else if (!replacer_->victim(&fid)) {
            return nullptr;
        }

        page_id->page_no = disk_manager_->allocate_page(page_id->fd);
        page = &pages_[fid];
        old_page_id = page->id_;
        old_page_dirty = page->is_dirty_;
        if (IsValidPageId(old_page_id)) {
            page_table_.erase(old_page_id);
        }
        page->state_.store(FrameState::EVICTING, std::memory_order_release);
        page->id_ = *page_id;
        page->is_dirty_ = false;
        page->dirty_epoch_ = 0;
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
            replacer_->unpin(fid);
        } else {
            if (hit != page_table_.end() && hit->second == fid) {
                page_table_.erase(hit);
            }
            page->reset_memory();
            page->id_ = PageId{};
            page->is_dirty_ = false;
            page->pin_count_ = 0;
            page->state_.store(FrameState::FREE, std::memory_order_release);
            free_list_.push_back(fid);
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
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true

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
                    replacer_->unpin(fid);
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
                free_list_.push_back(fid);
            } else if (!deleted && hit != page_table_.end() && hit->second == fid) {
                page->state_.store(FrameState::VALID, std::memory_order_release);
                replacer_->unpin(fid);
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

    constexpr size_t kClaimPages = 64;
    struct CandidatePage {
        PageId page_id;
    };
    struct ClaimedPage {
        PageId page_id;
        frame_id_t frame_id;
        uint64_t dirty_epoch;
    };

    const std::unordered_set<int> fd_set(fds.begin(), fds.end());
    std::vector<CandidatePage> candidates;
    {
        std::shared_lock lock{latch_};
        candidates.reserve(page_table_.size());
        for (const auto& [page_id, frame_id] : page_table_) {
            (void)frame_id;
            if (fd_set.find(page_id.fd) == fd_set.end()) {
                continue;
            }
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID && page->is_dirty_) {
                candidates.push_back(CandidatePage{page_id});
            }
        }
    }

    std::sort(candidates.begin(), candidates.end(), [](const CandidatePage& lhs, const CandidatePage& rhs) {
        if (lhs.page_id.fd != rhs.page_id.fd) {
            return lhs.page_id.fd < rhs.page_id.fd;
        }
        return lhs.page_id.page_no < rhs.page_id.page_no;
    });

    bool success = true;
    for (size_t candidate_begin = 0; candidate_begin < candidates.size();) {
        std::vector<ClaimedPage> claimed;
        claimed.reserve(kClaimPages);
        {
            std::unique_lock lock{latch_};
            while (candidate_begin < candidates.size() && claimed.size() < kClaimPages) {
                const PageId page_id = candidates[candidate_begin++].page_id;
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
                claimed.push_back(
                    ClaimedPage{page_id, hit->second, page->dirty_epoch_.load(std::memory_order_acquire)});
            }
        }

        size_t claimed_begin = 0;
        std::vector<char> image(kClaimPages * PAGE_SIZE);
        while (claimed_begin < claimed.size()) {
            size_t claimed_end = claimed_begin + 1;
            while (claimed_end < claimed.size() && claimed[claimed_end].page_id.fd == claimed[claimed_begin].page_id.fd &&
                   claimed[claimed_end].page_id.page_no == claimed[claimed_end - 1].page_id.page_no + 1) {
                ++claimed_end;
            }

            const size_t page_count = claimed_end - claimed_begin;
            bool copied = true;
            lsn_t max_page_lsn = INVALID_LSN;
            for (size_t i = claimed_begin; i < claimed_end; ++i) {
                Page* page = &pages_[claimed[i].frame_id];
                try {
                    std::shared_lock page_lock(page->latch_);
                    std::memcpy(image.data() + (i - claimed_begin) * PAGE_SIZE, page->data_, PAGE_SIZE);
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
                    disk_manager_->write_page(claimed[claimed_begin].page_id.fd,
                                               claimed[claimed_begin].page_id.page_no, image.data(),
                                               static_cast<int>(page_count * PAGE_SIZE));
                } catch (...) {
                    copied = false;
                }
            }
            success = copied && success;

            {
                std::unique_lock lock{latch_};
                for (size_t i = claimed_begin; i < claimed_end; ++i) {
                    const auto& claimed_page = claimed[i];
                    Page* page = &pages_[claimed_page.frame_id];
                    auto hit = page_table_.find(claimed_page.page_id);
                    if (hit == page_table_.end() || hit->second != claimed_page.frame_id ||
                        page->state_.load(std::memory_order_acquire) != FrameState::FLUSHING) {
                        continue;
                    }
                    if (copied) {
                        std::scoped_lock dirty_lock{page->dirty_latch_};
                        if (page->dirty_epoch_.load(std::memory_order_acquire) == claimed_page.dirty_epoch) {
                            page->is_dirty_ = false;
                        }
                    }
                    page->state_.store(FrameState::VALID, std::memory_order_release);
                    std::scoped_lock pin_lock{page->pin_latch_};
                    if (page->pin_count_ == 0) {
                        replacer_->unpin(claimed_page.frame_id);
                    }
                    page->io_cv_.notify_all();
                }
            }
            claimed_begin = claimed_end;
        }
    }
    return success;
}

size_t BufferPoolManager::flush_dirty_pages(size_t max_pages) {
    if (max_pages == 0) {
        return 0;
    }

    std::vector<PageId> pages_to_flush;
    {
        std::shared_lock lock{latch_};
        pages_to_flush.reserve(std::min(max_pages, page_table_.size()));
        for (const auto& [page_id, frame_id] : page_table_) {
            if (pages_to_flush.size() >= max_pages) {
                break;
            }
            Page* page = &pages_[frame_id];
            if (page->state_.load(std::memory_order_acquire) == FrameState::VALID &&
                page->is_dirty_.load(std::memory_order_acquire)) {
                pages_to_flush.push_back(page_id);
            }
        }
    }

    size_t flushed_pages = 0;
    for (const PageId& page_id : pages_to_flush) {
        if (flush_page_impl(page_id, true)) {
            ++flushed_pages;
        }
    }
    return flushed_pages;
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
        free_list_.emplace_back(frame_id);
        page->reset_memory();
        page->pin_count_ = 0;
        page->is_dirty_ = false;
        page->id_ = PageId{};
        page->state_.store(FrameState::FREE, std::memory_order_release);
        it = page_table_.erase(it);
    }
}
