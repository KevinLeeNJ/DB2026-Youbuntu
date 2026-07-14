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
#include <memory>
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "disk_manager.h"
#include "errors.h"
#include "page.h"
#include "replacer/clock_replacer.h"
#include "replacer/lru_replacer.h"
#include "replacer/replacer.h"

class LogManager;

class BufferPoolManager {
private:
    size_t pool_size_; // buffer_pool中可容纳页面的个数，即帧的个数
    std::unique_ptr<Page[]>
        pages_; // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash>
        page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    std::list<frame_id_t> free_list_; // 空闲帧编号的链表
    DiskManager* disk_manager_;
    LogManager* log_manager_{nullptr};
    std::unique_ptr<Replacer> replacer_; // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::shared_mutex latch_;            // 用于共享数据结构的并发控制

public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = std::make_unique<Page[]>(pool_size_);
        if (REPLACER_TYPE == "CLOCK") {
            replacer_ = std::make_unique<ClockReplacer>(pool_size_);
        } else {
            replacer_ = std::make_unique<LRUReplacer>(pool_size_);
        }
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<frame_id_t>(i)); // static_cast转换数据类型
        }
    }

    ~BufferPoolManager() = default;

    /**
     * @description: 将目标页面标记为脏页
     * @param {Page*} page 脏页
     */
    static void mark_dirty(Page* page) {
        page->is_dirty_ = true;
    }

public:
    Page* fetch_page(PageId page_id);

    bool is_page_resident(PageId page_id);

    bool unpin_page(PageId page_id, bool is_dirty);

    bool flush_page(PageId page_id);

    Page* new_page(PageId* page_id);

    bool delete_page(PageId page_id);

    bool flush_all_pages(int fd);

    void delete_all_pages(int fd);

    void set_log_manager(LogManager* log_manager) {
        log_manager_ = log_manager;
    }

private:
    void flush_log_before_page_write(lsn_t page_lsn);
    bool flush_page_impl(PageId page_id, bool dirty_only);
};
