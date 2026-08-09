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

#include <mutex>
#include <vector>

#include "common/config.h"
#include "replacer/replacer.h"

/*
LRUReplacer实现了LRU替换策略
*/
class LRUReplacer : public Replacer {
public:
    /**
     * @description: 创建一个新的LRUReplacer
     * @param {size_t} num_pages LRUReplacer最多需要存储的page数量
     */
    explicit LRUReplacer(size_t num_pages);

    ~LRUReplacer();

    bool victim(frame_id_t* frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    void restore(frame_id_t frame_id) override;

    bool restore_claimed_noexcept(frame_id_t frame_id) noexcept override;

    size_t Size();

private:
    bool valid(frame_id_t frame_id) const noexcept;
    void remove_locked(frame_id_t frame_id) noexcept;
    void push_front_locked(frame_id_t frame_id) noexcept;
    void push_back_locked(frame_id_t frame_id) noexcept;

    std::mutex latch_;
    std::vector<frame_id_t> previous_;
    std::vector<frame_id_t> next_;
    std::vector<uint8_t> present_;
    frame_id_t head_{INVALID_FRAME_ID}; // most recently unpinned
    frame_id_t tail_{INVALID_FRAME_ID}; // oldest, next victim
    size_t size_{0};
    size_t max_size_;
};
