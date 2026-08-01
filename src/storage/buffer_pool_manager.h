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
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <cstdint>
#include <functional>
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
#include "index_smo_wal.h"
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

class FlushDependencyPolicy {
public:
    enum class Kind : uint8_t { Enforce, AlreadyDurable };

    static FlushDependencyPolicy Enforce() {
        return FlushDependencyPolicy(Kind::Enforce);
    }

    static FlushDependencyPolicy AlreadyDurable() {
        return FlushDependencyPolicy(Kind::AlreadyDurable);
    }

    Kind kind() const {
        return kind_;
    }

private:
    explicit FlushDependencyPolicy(Kind kind) : kind_(kind) {}

    Kind kind_;
};

class BufferPoolManager {
private:
    using FlushPageTestHook = std::function<void(PageId, Page*)>;
    static std::mutex flush_page_test_hook_latch_;
    static FlushPageTestHook flush_page_test_hook_;
    static FlushPageTestHook flush_page_after_write_test_hook_;
    static FlushPageTestHook flush_batch_before_write_test_hook_;

    size_t pool_size_; // buffer_pool中可容纳页面的个数，即帧的个数
    std::unique_ptr<Page[]>
        pages_; // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash>
        page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    frame_id_t next_unused_frame_{0};
    std::vector<frame_id_t> recycled_frames_; // 已回收的空闲帧编号，按栈使用
    std::vector<ResidencyClass> residency_classes_;
    size_t index_internal_resident_count_{0};
    // Background preflush walks frames instead of the unordered page table.
    // Advancing this cursor after every bounded scan prevents a perpetually
    // dirty low-numbered frame from starving later table pages.
    frame_id_t next_dirty_flush_frame_{0};
    DiskManager* disk_manager_;
    LogManager* log_manager_{nullptr};
    std::unique_ptr<Replacer> replacer_; // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::shared_mutex latch_;            // 用于共享数据结构的并发控制
    std::condition_variable_any frame_operation_cv_;
    std::condition_variable_any index_smo_cv_;
    std::unordered_map<int, size_t> index_smo_barriers_;
    std::unordered_map<int, size_t> index_writes_inflight_;
    bool frame_operation_active_{false};
    uint64_t frame_operation_generation_{0};
    std::atomic<uint64_t> fetch_miss_{0};
    std::atomic<uint64_t> inflight_wait_{0};
    std::atomic<uint64_t> inflight_wait_ns_{0};
    std::atomic<uint64_t> no_victim_{0};
    std::atomic<uint64_t> eviction_clean_{0};
    std::atomic<uint64_t> eviction_dirty_{0};

public:
    static void set_flush_page_test_hook(FlushPageTestHook hook);
    static void set_flush_page_after_write_test_hook(FlushPageTestHook hook);
    static void set_flush_batch_before_write_test_hook(FlushPageTestHook hook);

    class FrameOperationToken {
        friend class BufferPoolManager;

    public:
        FrameOperationToken() = default;
        ~FrameOperationToken();
        FrameOperationToken(FrameOperationToken&& other) noexcept;
        FrameOperationToken& operator=(FrameOperationToken&& other) noexcept;
        FrameOperationToken(const FrameOperationToken&) = delete;
        FrameOperationToken& operator=(const FrameOperationToken&) = delete;

        Page* fetch_page(PageId page_id) const;
        Page* new_page(PageId* page_id) const;

    private:
        FrameOperationToken(BufferPoolManager* manager, uint64_t generation)
            : manager_(manager), generation_(generation) {}

        void release() noexcept;

        BufferPoolManager* manager_{nullptr};
        uint64_t generation_{0};
    };

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
    static void mark_dirty(Page* page, const PageWriteDependency& dependency) {
        std::scoped_lock dirty_lock{page->dirty_latch_};
        page->write_dependency_.merge(dependency);
        page->dirty_epoch_.fetch_add(1, std::memory_order_release);
        page->is_dirty_.store(true, std::memory_order_release);
    }

    // Transitional table-page adapter. Index code must use the typed overload
    // because byte zero of an index page is IxPageHdr, not an LSN.
    static void mark_dirty(Page* page) {
        mark_dirty(page, PageWriteDependency::Wal(page->get_page_lsn()));
    }

public:
    Page* fetch_page(PageId page_id);
    FrameOperationToken acquire_frame_operation(size_t minimum_available_frames = 2);

    bool is_page_resident(PageId page_id);

    // Returns the replacement classification of a valid resident page. A
    // missing or in-flight page has no observable residency classification.
    std::optional<ResidencyClass> get_residency_class(PageId page_id);

    // Index residency is a global, buffer-pool-owned resource. In addition to
    // the half-pool cap, keep eight ordinary frames for a B+tree SMO's leaf,
    // sibling, parent/new-parent chain, and transient child fetch. Tiny pools
    // therefore decline residency admission and use the ordinary fetch path.
    // Admission and the class transition are atomic under latch_; repeated
    // admission is idempotent.
    bool try_mark_resident(PageId page_id, ResidencyClass residency_class);
    void unmark_resident(PageId page_id);
    size_t index_internal_residency_budget() const noexcept {
        const size_t half_pool = pool_size_ / 2;
        const size_t smo_limited = pool_size_ > 8 ? pool_size_ - 8 : 0;
        return std::min(half_pool, smo_limited);
    }
    size_t index_internal_resident_count();

    bool unpin_page(PageId page_id, bool is_dirty);
    bool unpin_page(PageId page_id, const PageWriteDependency& dependency);

    bool flush_page(PageId page_id);

    Page* new_page(PageId* page_id);

    bool delete_page(PageId page_id);

    bool flush_all_pages(int fd);

    bool flush_all_pages(const std::vector<int>& fds);

    // Flush a stable checkpoint image under an explicit dependency policy.
    bool flush_all_pages(const std::vector<int>& fds, FlushDependencyPolicy policy);

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
    // The policy is deliberately strongly typed: callers either enforce the
    // frame dependencies or prove that the relevant WAL boundary is already
    // durable. No flush path reads an LSN from the page payload.
    FlushBatchResult flush_pages(std::vector<PageId>& page_ids, FlushDependencyPolicy policy);

    // Used by index-header writes, which do not pass through a buffer frame.
    void ensure_write_dependency(const PageWriteDependency& dependency);
    void begin_index_smo(int fd);
    void end_index_smo(int fd) noexcept;
    void begin_index_file_write(int fd);
    void end_index_file_write(int fd) noexcept;
    lsn_t append_index_smo(const IndexSmoWalData& data);
    void ensure_index_binding(const std::string& index_file_name);
    void renew_index_binding(const std::string& index_file_name);

    // Write at most max_pages dirty resident pages whose file descriptor is in
    // allowed_fds, without requiring a checkpoint barrier. An empty whitelist
    // intentionally flushes nothing: background callers must explicitly prove
    // that every file is a table file, so index payload can never be mistaken
    // for a page LSN. Selection advances round-robin over buffer frames.
    FlushBatchResult flush_dirty_pages(const std::vector<int>& allowed_fds, size_t max_pages);

    // Count dirty, valid resident pages in the explicit file whitelist. This is
    // used by focused storage tests.
    size_t count_dirty_pages(const std::vector<int>& allowed_fds);

    void delete_all_pages(int fd);

    void set_log_manager(LogManager* log_manager) {
        log_manager_ = log_manager;
    }

    BufferPoolObservabilitySnapshot observability_snapshot() const;

    // The frame array is fixed for this manager's lifetime, so this is a
    // lock-free read-only capacity query for checkpoint pacing.
    size_t frame_capacity() const noexcept {
        return pool_size_;
    }

private:
    // Env-gated (RMDB_LOG_BPM_STATS=1) snapshot of how much of the pool is
    // actually available for replacement. Emitted from the background preflush
    // tick - the only periodic call the buffer pool receives - so nothing is
    // added to any request path. Written to answer a specific question: whether
    // the index's resident/pinned internal pages measurably shrink the effective
    // pool.
    void log_pool_stats();

    frame_id_t take_free_frame();
    frame_id_t take_unblocked_victim_locked();
    bool index_smo_blocked_locked(int fd) const;
    void claim_index_file_write_locked(int fd);
    void release_index_file_write_locked(int fd);
    void recycle_frame(frame_id_t frame_id);
    size_t available_frames_locked() const;
    bool operation_authorized_locked(const FrameOperationToken* operation) const;
    void release_frame_operation(uint64_t generation) noexcept;
    Page* fetch_page_impl(PageId page_id, const FrameOperationToken* operation);
    Page* new_page_impl(PageId* page_id, const FrameOperationToken* operation);
    void clear_residency(frame_id_t frame_id);
    bool unpin_page_impl(PageId page_id, bool is_dirty, const PageWriteDependency& dependency);
    static void run_flush_page_test_hook(PageId page_id, Page* page);
    static void run_flush_page_after_write_test_hook(PageId page_id, Page* page);
    static void run_flush_batch_before_write_test_hook(PageId page_id, Page* page);
    bool flush_page_impl(PageId page_id, bool dirty_only);
};
