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

#include "ix_defs.h"
#include "ix_index_handle.h"

#include <utility>

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点。
// 默认在整个扫描生命周期内持有索引共享锁，避免叶链遍历与并发结构修改交错。
class IxScan : public RecScan {
    const IxIndexHandle* ih_;
    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    Iid iid_; // 初始为lower（用于遍历的指针）
    Iid end_; // 初始为upper
    BufferPoolManager* bpm_;

    // Pinned current leaf page. Stays in sync with iid_.page_no while the scan
    // has not reached the end. Released on destruction / reaching end.
    Page* pinned_leaf_page_ = nullptr;
    IxNodeHandle leaf_;

    void pin_current_leaf() {
        if (pinned_leaf_page_ != nullptr) {
            return;
        }
        if (iid_ == end_) {
            return;
        }
        pinned_leaf_page_ = bpm_->fetch_page(PageId{ih_->fd_, iid_.page_no});
        assert(pinned_leaf_page_ != nullptr);
        leaf_ = IxNodeHandle(ih_->file_hdr_.get(), pinned_leaf_page_);
    }

    void unpin_current_leaf() {
        if (pinned_leaf_page_ != nullptr) {
            bpm_->unpin_page(pinned_leaf_page_->get_page_id(), false);
            pinned_leaf_page_ = nullptr;
        }
    }

    void release_index_latch_if_held() {
        if (index_latch_guard_.owns_lock()) {
            index_latch_guard_.unlock();
        }
    }

    void move_to_end() {
        unpin_current_leaf();
        iid_ = end_;
        release_index_latch_if_held();
    }

    void normalize_position() {
        while (iid_ != end_) {
            pin_current_leaf();
            assert(leaf_.is_leaf_page());

            if (iid_.page_no == end_.page_no && iid_.slot_no >= end_.slot_no) {
                move_to_end();
                return;
            }
            if (iid_.slot_no < leaf_.get_size()) {
                return;
            }

            page_id_t next_leaf = leaf_.get_next_leaf();
            if (iid_.page_no == ih_->file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE) {
                move_to_end();
                return;
            }
            unpin_current_leaf();
            iid_.page_no = next_leaf;
            iid_.slot_no = 0;
        }
        move_to_end();
    }

public:
    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm,
           bool acquire_index_latch = true)
        : IxScan(ih, lower, upper, bpm, acquire_index_latch ? ih->lock_shared() : IxIndexHandle::SharedIndexLatch{}) {}

    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm,
           IxIndexHandle::SharedIndexLatch index_latch_guard)
        : ih_(ih), index_latch_guard_(std::move(index_latch_guard)), iid_(lower), end_(upper), bpm_(bpm) {
        normalize_position();
        if (is_end()) {
            release_index_latch_if_held();
        }
    }

    ~IxScan() override {
        unpin_current_leaf();
    }

    IxScan(const IxScan&) = delete;
    IxScan& operator=(const IxScan&) = delete;

    void next() override;

    bool is_end() const override {
        return iid_ == end_;
    }

    Rid rid() const override {
        // Caller must ensure !is_end(). Leaf is pinned, so read directly.
        return *leaf_.get_rid(iid_.slot_no);
    }

    const char* key() const {
        // Caller must ensure !is_end(). Leaf is pinned, so read directly.
        return leaf_.get_key(iid_.slot_no);
    }

    const Iid& iid() const {
        return iid_;
    }
};
