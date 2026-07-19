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

#pragma once
#include <cstdlib>
#include <memory>
#include <optional>
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "disk_manager.h"
#include "errors.h"
#include "page.h"
#include "replacer/clock_replacer.h"
#include "replacer/lru_replacer.h"
#include "replacer/replacer.h"

class LogManager;

enum class ResidencyClass : uint8_t {
    Normal,
    IndexInternal,
};

class BufferPoolManager {
private:
    static constexpr size_t kPageTableShardCount = 16;
    struct PageTableShard {
        mutable std::mutex latch;
        std::unordered_map<PageId, frame_id_t, PageIdHash> entries;
        // Reserves an old PageId while its frame writes back and changes
        // identity. Readers wait instead of loading a stale second copy.
        std::unordered_map<PageId, frame_id_t, PageIdHash> evicting_entries;
    };

    size_t pool_size_; // buffer_pool中可容纳页面的个数，即帧的个数
    std::unique_ptr<Page[]>
        pages_; // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    // Compatibility snapshot for existing diagnostics/tests. Runtime lookups
    // use page_table_shards_; this map is updated only while latch_ is held.
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
    std::array<PageTableShard, kPageTableShardCount> page_table_shards_;
    frame_id_t next_unused_frame_{0};
    std::vector<frame_id_t> recycled_frames_; // 已回收的空闲帧编号，按栈使用
    std::unique_ptr<std::atomic<ResidencyClass>[]> residency_classes_;
    DiskManager* disk_manager_;
    LogManager* log_manager_{nullptr};
    std::unique_ptr<Replacer> replacer_; // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::shared_mutex latch_;            // 用于共享数据结构的并发控制
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = std::make_unique<Page[]>(pool_size_);
        residency_classes_ = std::make_unique<std::atomic<ResidencyClass>[]>(pool_size_);
        for (size_t i = 0; i < pool_size_; ++i) {
            residency_classes_[i].store(ResidencyClass::Normal, std::memory_order_relaxed);
        }
        if (REPLACER_TYPE == "CLOCK") {
            replacer_ = std::make_unique<ClockReplacer>(pool_size_);
        } else {
            replacer_ = std::make_unique<LRUReplacer>(pool_size_);
        }
        recycled_frames_.reserve(pool_size_);
        // Set the load factor before reserve so the reserved capacity remains
        // sufficient for the complete pool without an intermediate rehash.
        page_table_.max_load_factor(0.7F);
        const size_t expected_resident_pages = pool_size_ + (pool_size_ + 4) / 5;
        page_table_.reserve(expected_resident_pages);
        const size_t expected_per_shard = (expected_resident_pages + kPageTableShardCount - 1) / kPageTableShardCount;
        for (auto& shard : page_table_shards_) {
            shard.entries.max_load_factor(0.7F);
            shard.entries.reserve(expected_per_shard);
            shard.evicting_entries.max_load_factor(0.7F);
        }
    }

    ~BufferPoolManager() = default;

    /**
     * @description: 将目标页面标记为脏页
     * @param {Page*} page 脏页
     */
    static void mark_dirty(Page* page) {
        std::scoped_lock dirty_lock{page->dirty_latch_};
        page->dirty_epoch_.fetch_add(1, std::memory_order_release);
        page->is_dirty_.store(true, std::memory_order_release);
    }

public:
    Page* fetch_page(PageId page_id);

    bool is_page_resident(PageId page_id);

    // Returns the replacement classification of a valid resident page. A
    // missing or in-flight page has no observable residency classification.
    std::optional<ResidencyClass> get_residency_class(PageId page_id);

    // Resident pages remain eligible for normal pin/unpin access but are kept
    // out of the replacer. The caller owns the lifetime of this classification
    // and must unmark pages before dropping an index.
    void mark_resident(PageId page_id, ResidencyClass residency_class);
    void unmark_resident(PageId page_id);

    bool unpin_page(PageId page_id, bool is_dirty);

    bool flush_page(PageId page_id);

    Page* new_page(PageId* page_id);

    bool delete_page(PageId page_id);

    bool flush_all_pages(int fd);

    bool flush_all_pages(const std::vector<int>& fds);

    // Flush a stable checkpoint image after the caller has made the WAL
    // durable. Adjacent pages in one file may be written with one pwrite.
    bool flush_all_pages(const std::vector<int>& fds, bool wal_preflushed);

    // Write at most max_pages dirty resident pages without requiring a
    // checkpoint barrier. The page dirty epoch prevents a concurrent update
    // from being lost when the write completes.
    size_t flush_dirty_pages(size_t max_pages);

    void delete_all_pages(int fd);

    void set_log_manager(LogManager* log_manager) {
        log_manager_ = log_manager;
    }

private:
    size_t page_table_shard(PageId page_id) const {
        return PageIdHash{}(page_id) % kPageTableShardCount;
    }

    template <typename Fn> void with_shards_locked(size_t first_index, size_t second_index, Fn&& fn) {
        if (first_index == second_index) {
            std::unique_lock first_lock(page_table_shards_[first_index].latch);
            fn();
            return;
        }
        const size_t low_index = std::min(first_index, second_index);
        const size_t high_index = std::max(first_index, second_index);
        std::unique_lock low_lock(page_table_shards_[low_index].latch);
        std::unique_lock high_lock(page_table_shards_[high_index].latch);
        fn();
    }

    std::vector<std::pair<PageId, frame_id_t>> snapshot_page_table() const;
    bool try_pin_page(PageId page_id, Page** page, Page** wait_page);
    frame_id_t take_free_frame();
    // Called with latch_ held exclusively.  Replacer entries can be stale
    // after a concurrent pin/unpin transition, so every victim is validated
    // against the frame's current state before it is claimed.
    frame_id_t claim_frame();
    void recycle_frame(frame_id_t frame_id);
    void clear_residency(frame_id_t frame_id);
    void flush_log_before_page_write(lsn_t page_lsn);
    bool flush_page_impl(PageId page_id, bool dirty_only);
};
