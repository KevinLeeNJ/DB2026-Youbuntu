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
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"

/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    int fd = -1; //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;

    friend bool operator==(const PageId& x, const PageId& y) {
        return x.fd == y.fd && x.page_no == y.page_no;
    }
    bool operator<(const PageId& x) const {
        if (fd < x.fd)
            return true;
        return page_no < x.page_no;
    }

    std::string toString() {
        return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}";
    }

    inline int64_t Get() const {
        uint64_t fd_part = static_cast<uint32_t>(fd);
        uint64_t page_part = static_cast<uint32_t>(page_no);
        return static_cast<int64_t>((fd_part << 32) | page_part);
    }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
    size_t operator()(const PageId& x) const {
        return std::hash<int64_t>()(x.Get());
    }
};

template <> struct std::hash<PageId> {
    size_t operator()(const PageId& obj) const {
        return std::hash<int64_t>()(obj.Get());
    }
};

enum class FrameState : uint8_t {
    FREE,
    LOADING,
    VALID,
    EVICTING,
    FLUSHING,
};

/**
 * @description: Page类声明, Page是RMDB数据块的单位、是负责数据操作Record模块的操作对象，
 * Page对象在磁盘上有文件存储, 若在Buffer中则有帧偏移, 并非特指Buffer或Disk上的数据
 */
class Page {
    friend class BufferPoolManager;

public:
    // Free frames are not observable until BufferPoolManager initializes them
    // on new_page/fetch_page. Avoid touching the full frame payload while a
    // large buffer pool is being constructed.
    Page() = default;

    ~Page() = default;

    PageId get_page_id() const {
        return id_;
    }

    inline char* get_data() {
        return data_;
    }

    bool is_dirty() const {
        return is_dirty_.load(std::memory_order_acquire);
    }

    std::shared_mutex& latch() {
        return latch_;
    }

    static constexpr size_t OFFSET_PAGE_START = 0;
    static constexpr size_t OFFSET_LSN = 0;
    static constexpr size_t OFFSET_PAGE_HDR = 4;

    inline lsn_t get_page_lsn() {
        lsn_t page_lsn;
        memcpy(&page_lsn, get_data() + OFFSET_LSN, sizeof(page_lsn));
        return page_lsn;
    }

    inline void set_page_lsn(lsn_t page_lsn) {
        memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t));
    }

private:
    void reset_memory() {
        memset(data_, OFFSET_PAGE_START, PAGE_SIZE);
    } // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId id_;

    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE];

    /** 脏页判断 */
    std::atomic<bool> is_dirty_{false};

    // Distinguishes the page image captured by a flush from writes that arrive
    // after that image has reached disk.
    std::atomic<uint64_t> dirty_epoch_{0};
    // Background checkpoint writeback attempts a page at most once per WAL
    // generation. If it is dirtied again, the final quiescent checkpoint writes
    // the latest image instead of repeatedly rewriting it in the background.
    uint64_t preflush_generation_{0};
    bool preflush_attempted_{false};
    std::mutex dirty_latch_;

    // Buffer-pool pinning protects residency; this protects the page payload.
    std::shared_mutex latch_;

    // I/O state is separate from the page payload latch. Fetch waiters sleep
    // here while an owner performs disk I/O without the BPM latch.
    std::atomic<FrameState> state_{FrameState::FREE};
    std::mutex io_latch_;
    std::condition_variable io_cv_;

    /** The pin count of this page. */
    int pin_count_{0};
    std::mutex pin_latch_;
};
