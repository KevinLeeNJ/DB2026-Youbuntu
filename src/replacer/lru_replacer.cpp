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

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages)
    : previous_(num_pages, INVALID_FRAME_ID), next_(num_pages, INVALID_FRAME_ID), present_(num_pages, 0),
      max_size_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::scoped_lock lock{latch_}; //  如果编译报错可以替换成其他lock

    // Todo:
    //  利用lru_replacer中的LRUlist_,LRUHash_实现LRU策略
    //  选择合适的frame指定为淘汰页面,赋值给*frame_id
    if (tail_ == INVALID_FRAME_ID) {
        *frame_id = INVALID_FRAME_ID;
        return false;
    }
    *frame_id = tail_;
    remove_locked(*frame_id);

    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    if (!valid(frame_id) || !present_[frame_id]) return;
    remove_locked(frame_id);
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    // Todo:
    //  支持并发锁
    //  选择一个frame取消固定
    std::scoped_lock lock{latch_};
    if (!valid(frame_id) || present_[frame_id] || size_ >= max_size_) return;
    push_front_locked(frame_id);
}

void LRUReplacer::restore(frame_id_t frame_id) {
    (void)restore_claimed_noexcept(frame_id);
}

bool LRUReplacer::restore_claimed_noexcept(frame_id_t frame_id) noexcept {
    std::scoped_lock lock{latch_};
    if (!valid(frame_id)) return false;
    if (!present_[frame_id]) push_back_locked(frame_id);
    return true;
}

bool LRUReplacer::valid(frame_id_t frame_id) const noexcept {
    return frame_id >= 0 && static_cast<size_t>(frame_id) < max_size_;
}

void LRUReplacer::remove_locked(frame_id_t frame_id) noexcept {
    const frame_id_t previous = previous_[frame_id];
    const frame_id_t next = next_[frame_id];
    if (previous == INVALID_FRAME_ID) head_ = next;
    else next_[previous] = next;
    if (next == INVALID_FRAME_ID) tail_ = previous;
    else previous_[next] = previous;
    previous_[frame_id] = INVALID_FRAME_ID;
    next_[frame_id] = INVALID_FRAME_ID;
    present_[frame_id] = 0;
    --size_;
}

void LRUReplacer::push_front_locked(frame_id_t frame_id) noexcept {
    previous_[frame_id] = INVALID_FRAME_ID;
    next_[frame_id] = head_;
    if (head_ != INVALID_FRAME_ID) previous_[head_] = frame_id;
    else tail_ = frame_id;
    head_ = frame_id;
    present_[frame_id] = 1;
    ++size_;
}

void LRUReplacer::push_back_locked(frame_id_t frame_id) noexcept {
    next_[frame_id] = INVALID_FRAME_ID;
    previous_[frame_id] = tail_;
    if (tail_ != INVALID_FRAME_ID) next_[tail_] = frame_id;
    else head_ = frame_id;
    tail_ = frame_id;
    present_[frame_id] = 1;
    ++size_;
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() {
    std::scoped_lock lock{latch_};
    return size_;
}
