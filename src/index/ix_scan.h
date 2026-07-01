/* Copyright (c) 2023 Renmin University of China
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

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan {
    const IxIndexHandle* ih_;
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
        Page* page = bpm_->fetch_page(PageId{ih_->fd_, iid_.page_no});
        pinned_leaf_page_ = page;
        ih_->fetch_node_into(iid_.page_no, leaf_);
    }

    void unpin_current_leaf() {
        if (pinned_leaf_page_ != nullptr) {
            bpm_->unpin_page(pinned_leaf_page_->get_page_id(), false);
            pinned_leaf_page_ = nullptr;
        }
    }

public:
    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm)
        : ih_(ih), iid_(lower), end_(upper), bpm_(bpm) {
        pin_current_leaf();
    }

    ~IxScan() override { unpin_current_leaf(); }

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

    const Iid& iid() const {
        return iid_;
    }
};
