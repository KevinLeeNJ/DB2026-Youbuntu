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

#include <algorithm>
#include <cstring>
#include <shared_mutex>
#include <utility>
#include <vector>

// Iterates index entries without retaining the structure latch for the whole
// consumer lifetime. The normal mode copies at most one stable leaf while
// holding structure-S + leaf-S, releases both, and re-seeks from the root before
// loading the next leaf. Callers already holding a structure latch can request
// legacy_mode by passing acquire_index_latch=false.
class IxScan : public RecScan {
    struct BufferedEntry {
        Rid rid;
        Iid iid;
    };

    const IxIndexHandle* ih_;
    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    Iid iid_;
    Iid end_;
    BufferPoolManager* bpm_;
    bool coupled_mode_{false};

    // Legacy cursor used only by internal callers that already hold a
    // structure latch around the complete scan.
    Page* pinned_leaf_page_{nullptr};
    IxNodeHandle leaf_;
    std::shared_lock<std::shared_mutex> leaf_latch_guard_;

    // Coupled/re-seek cursor. Entries contain copies, so no page or structure
    // latch is retained while the executor evaluates heap visibility.
    std::vector<BufferedEntry> batch_;
    std::vector<char> batch_keys_;
    size_t batch_pos_{0};
    bool first_batch_{true};
    bool coupled_end_{false};
    bool unbounded_end_{false};
    std::vector<char> end_key_;
    std::vector<char> last_key_;
    std::vector<uint64_t> emitted_last_key_rids_;

    static uint64_t rid_key(const Rid& rid) {
        return (static_cast<uint64_t>(static_cast<uint32_t>(rid.page_no)) << 32) | static_cast<uint32_t>(rid.slot_no);
    }

    const char* batch_key(size_t position) const {
        return batch_keys_.data() + position * ih_->file_hdr_->col_tot_len_;
    }

    bool last_key_rid_was_emitted(const Rid& rid) const {
        const uint64_t target = rid_key(rid);
        return std::find(emitted_last_key_rids_.begin(), emitted_last_key_rids_.end(), target) !=
               emitted_last_key_rids_.end();
    }

    void remember_completed_batch_tail() {
        assert(!batch_.empty());
        const size_t tail = batch_.size() - 1;
        const char* tail_key = batch_key(tail);
        const bool continues_previous =
            !last_key_.empty() &&
            ix_compare(tail_key, last_key_.data(), ih_->file_hdr_->col_types_, ih_->file_hdr_->col_lens_) == 0;
        if (!continues_previous) {
            last_key_.assign(tail_key, tail_key + ih_->file_hdr_->col_tot_len_);
            emitted_last_key_rids_.clear();
        }
        for (size_t position = batch_.size(); position > 0; --position) {
            const size_t index = position - 1;
            if (ix_compare(batch_key(index), last_key_.data(), ih_->file_hdr_->col_types_, ih_->file_hdr_->col_lens_) !=
                0) {
                break;
            }
            emitted_last_key_rids_.push_back(rid_key(batch_[index].rid));
        }
    }

    void pin_current_leaf() {
        if (pinned_leaf_page_ != nullptr || iid_ == end_) {
            return;
        }
        pinned_leaf_page_ = bpm_->fetch_page(PageId{ih_->fd_, iid_.page_no});
        assert(pinned_leaf_page_ != nullptr);
        leaf_latch_guard_ = std::shared_lock<std::shared_mutex>(pinned_leaf_page_->latch());
        leaf_ = IxNodeHandle(ih_->file_hdr_.get(), pinned_leaf_page_);
    }

    void unpin_current_leaf() {
        if (pinned_leaf_page_ != nullptr) {
            leaf_latch_guard_.unlock();
            bpm_->unpin_page(pinned_leaf_page_->get_page_id(), false);
            pinned_leaf_page_ = nullptr;
        }
    }

    void release_index_latch_if_held() {
        if (index_latch_guard_.owns_lock()) {
            index_latch_guard_.unlock();
        }
    }

    void move_legacy_to_end() {
        unpin_current_leaf();
        iid_ = end_;
        release_index_latch_if_held();
    }

    void normalize_legacy_position() {
        while (iid_ != end_) {
            pin_current_leaf();
            assert(leaf_.is_leaf_page());
            if (iid_.page_no == end_.page_no && iid_.slot_no >= end_.slot_no) {
                move_legacy_to_end();
                return;
            }
            if (iid_.slot_no < leaf_.get_size()) {
                return;
            }
            page_id_t next_leaf = leaf_.get_next_leaf();
            if (iid_.page_no == ih_->file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE) {
                move_legacy_to_end();
                return;
            }
            unpin_current_leaf();
            iid_.page_no = next_leaf;
            iid_.slot_no = 0;
        }
        move_legacy_to_end();
    }

    void capture_end_key() {
        if (iid_ == end_) {
            coupled_end_ = true;
            return;
        }
        Page* page = bpm_->fetch_page(PageId{ih_->fd_, end_.page_no});
        assert(page != nullptr);
        std::shared_lock<std::shared_mutex> leaf_lock(page->latch());
        IxNodeHandle end_leaf(ih_->file_hdr_.get(), page);
        if (end_.slot_no < end_leaf.get_size()) {
            end_key_.assign(end_leaf.get_key(end_.slot_no),
                            end_leaf.get_key(end_.slot_no) + ih_->file_hdr_->col_tot_len_);
        } else {
            unbounded_end_ = end_.page_no == ih_->file_hdr_->last_leaf_;
        }
        leaf_lock.unlock();
        bpm_->unpin_page(page->get_page_id(), false);
    }

    bool at_or_after_end(const char* key) const {
        return !unbounded_end_ && !end_key_.empty() &&
               ix_compare(key, end_key_.data(), ih_->file_hdr_->col_types_, ih_->file_hdr_->col_lens_) >= 0;
    }

    Iid find_resume_iid() {
        Iid cursor = ih_->lower_bound(last_key_.data());
        while (true) {
            Page* page = bpm_->fetch_page(PageId{ih_->fd_, cursor.page_no});
            assert(page != nullptr);
            std::shared_lock<std::shared_mutex> leaf_lock(page->latch());
            IxNodeHandle leaf(ih_->file_hdr_.get(), page);
            int slot = cursor.slot_no;
            while (slot < leaf.get_size()) {
                const int cmp = ix_compare(leaf.get_key(slot), last_key_.data(), ih_->file_hdr_->col_types_,
                                           ih_->file_hdr_->col_lens_);
                if (cmp > 0 || (cmp == 0 && !last_key_rid_was_emitted(*leaf.get_rid(slot)))) {
                    Iid result{leaf.get_page_no(), slot};
                    leaf_lock.unlock();
                    bpm_->unpin_page(page->get_page_id(), false);
                    return result;
                }
                ++slot;
            }
            const page_id_t next_leaf = leaf.get_next_leaf();
            const bool at_last = leaf.get_page_no() == ih_->file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE;
            leaf_lock.unlock();
            bpm_->unpin_page(page->get_page_id(), false);
            if (at_last) {
                return ih_->leaf_end();
            }
            cursor = Iid{next_leaf, 0};
        }
    }

    void load_coupled_batch() {
        batch_.clear();
        batch_keys_.clear();
        batch_pos_ = 0;
        if (coupled_end_) {
            return;
        }
        if (!index_latch_guard_.owns_lock()) {
            index_latch_guard_ = ih_->lock_shared();
        }
        Iid cursor = first_batch_ ? iid_ : find_resume_iid();
        first_batch_ = false;

        while (batch_.empty()) {
            Page* page = bpm_->fetch_page(PageId{ih_->fd_, cursor.page_no});
            assert(page != nullptr);
            std::shared_lock<std::shared_mutex> leaf_lock(page->latch());
            IxNodeHandle leaf(ih_->file_hdr_.get(), page);
            const size_t remaining = static_cast<size_t>(std::max(0, leaf.get_size() - cursor.slot_no));
            batch_.reserve(remaining);
            batch_keys_.reserve(remaining * ih_->file_hdr_->col_tot_len_);
            for (int slot = cursor.slot_no; slot < leaf.get_size(); ++slot) {
                if (at_or_after_end(leaf.get_key(slot))) {
                    coupled_end_ = true;
                    break;
                }
                BufferedEntry entry;
                entry.rid = *leaf.get_rid(slot);
                entry.iid = Iid{leaf.get_page_no(), slot};
                batch_.push_back(std::move(entry));
                batch_keys_.insert(batch_keys_.end(), leaf.get_key(slot),
                                   leaf.get_key(slot) + ih_->file_hdr_->col_tot_len_);
            }
            const page_id_t next_leaf = leaf.get_next_leaf();
            const bool at_last = leaf.get_page_no() == ih_->file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE;
            leaf_lock.unlock();
            bpm_->unpin_page(page->get_page_id(), false);
            if (!batch_.empty() || coupled_end_ || at_last) {
                if (batch_.empty()) {
                    coupled_end_ = true;
                }
                break;
            }
            cursor = Iid{next_leaf, 0};
        }
        release_index_latch_if_held();
        if (!batch_.empty()) {
            iid_ = batch_[0].iid;
        }
    }

public:
    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm,
           bool acquire_index_latch = true)
        : IxScan(ih, lower, upper, bpm, acquire_index_latch ? ih->lock_shared() : IxIndexHandle::SharedIndexLatch{}) {}

    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm,
           IxIndexHandle::SharedIndexLatch index_latch_guard)
        : ih_(ih), index_latch_guard_(std::move(index_latch_guard)), iid_(lower), end_(upper), bpm_(bpm),
          coupled_mode_(index_latch_guard_.owns_lock()) {
        if (coupled_mode_) {
            capture_end_key();
            load_coupled_batch();
        } else {
            normalize_legacy_position();
        }
    }

    ~IxScan() override {
        unpin_current_leaf();
    }

    IxScan(const IxScan&) = delete;
    IxScan& operator=(const IxScan&) = delete;

    void next() override;

    bool is_end() const override {
        return coupled_mode_ ? (coupled_end_ && batch_pos_ >= batch_.size()) : iid_ == end_;
    }

    Rid rid() const override {
        return coupled_mode_ ? batch_[batch_pos_].rid : *leaf_.get_rid(iid_.slot_no);
    }

    const char* key() const {
        return coupled_mode_ ? batch_key(batch_pos_) : leaf_.get_key(iid_.slot_no);
    }

    const Iid& iid() const {
        return iid_;
    }
};
