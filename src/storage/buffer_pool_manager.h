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
#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <optional>
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
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

struct BufferPoolObservabilitySnapshot {
    uint64_t fetch_miss{0};
    uint64_t inflight_wait{0};
    uint64_t inflight_wait_ns{0};
    uint64_t no_victim{0};
    uint64_t eviction_clean{0};
    uint64_t eviction_dirty{0};
};

class BufferPoolManager {
private:
    size_t pool_size_; // buffer_pool中可容纳页面的个数，即帧的个数
    std::unique_ptr<Page[]>
        pages_; // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash>
        page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    frame_id_t next_unused_frame_{0};
    std::vector<frame_id_t> recycled_frames_; // 已回收的空闲帧编号，按栈使用
    std::vector<ResidencyClass> residency_classes_;
    DiskManager* disk_manager_;
    LogManager* log_manager_{nullptr};
    std::unique_ptr<Replacer> replacer_; // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::shared_mutex latch_;            // 用于共享数据结构的并发控制
    std::atomic<uint64_t> fetch_miss_{0};
    std::atomic<uint64_t> inflight_wait_{0};
    std::atomic<uint64_t> inflight_wait_ns_{0};
    std::atomic<uint64_t> no_victim_{0};
    std::atomic<uint64_t> eviction_clean_{0};
    std::atomic<uint64_t> eviction_dirty_{0};

public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = std::make_unique<Page[]>(pool_size_);
        residency_classes_.assign(pool_size_, ResidencyClass::Normal);
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

    struct FlushBatchResult {
        // Pages whose image actually reached the file. A named page that is no
        // longer resident, or resident but clean, contributes nothing: both mean
        // its current image is already on disk.
        size_t pages_written = 0;
        bool success = true;
    };

    // Write the current image of every named page that is still resident and
    // dirty, coalescing runs of adjacent page numbers in one file into single
    // pwrites. page_ids is sorted in place.
    //
    // wal_preflushed declares that the caller has already satisfied WAL-before-
    // page ordering for this batch. Index pages set it because they carry no
    // page LSN at all - byte 0 of an index page is IxPageHdr, not Page::OFFSET_LSN
    // - so there is no WAL record to wait for and treating those bytes as an LSN
    // would make a split block on the log.
    FlushBatchResult flush_pages(std::vector<PageId>& page_ids, bool wal_preflushed);

    // Write at most max_pages dirty resident pages without requiring a
    // checkpoint barrier. The page dirty epoch prevents a concurrent update
    // from being lost when the write completes.
    size_t flush_dirty_pages(size_t max_pages);

    void delete_all_pages(int fd);

    void set_log_manager(LogManager* log_manager) {
        log_manager_ = log_manager;
    }

    BufferPoolObservabilitySnapshot observability_snapshot() const;

private:
    // Env-gated (RMDB_LOG_BPM_STATS=1) snapshot of how much of the pool is
    // actually available for replacement. Emitted from the background preflush
    // tick - the only periodic call the buffer pool receives - so nothing is
    // added to any request path. Written to answer a specific question: whether
    // the index's resident/pinned internal pages measurably shrink the effective
    // pool.
    void log_pool_stats();

    frame_id_t take_free_frame();
    void recycle_frame(frame_id_t frame_id);
    void clear_residency(frame_id_t frame_id);
    void flush_log_before_page_write(lsn_t page_lsn);
    bool flush_page_impl(PageId page_id, bool dirty_only);
};
