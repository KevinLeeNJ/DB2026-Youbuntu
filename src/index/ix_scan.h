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
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

// IxScan has two cursor implementations.  The legacy cursor keeps a shared
// structure latch and a leaf pinned for its lifetime.  The coupled cursor
// copies a bounded batch while holding the structure latch, then releases all
// latches before the executor evaluates heap visibility.
enum class ScanDirection { Forward, Backward };

class IxScan : public RecScan {
public:
    enum class Mode { LEGACY, HYBRID, COUPLED };

    static Mode configured_mode() {
        static const Mode mode = [] {
            const char* value = std::getenv("IX_SCAN_MODE");
            if (value == nullptr || std::string(value) == "hybrid") {
                return Mode::HYBRID;
            }
            if (std::string(value) == "legacy") {
                return Mode::LEGACY;
            }
            if (std::string(value) == "coupled") {
                return Mode::COUPLED;
            }
            return Mode::HYBRID;
        }();
        return mode;
    }

    // Prefetching is deliberately limited to pages already resident in the
    // index cache.  Calling fetch_page speculatively would change pin counts
    // and eviction behavior, while a cache-only prefetch is a pure CPU hint.
    static bool prefetch_enabled() {
        static const bool enabled = [] {
            const char* value = std::getenv("IX_SCAN_PREFETCH");
            return value == nullptr || std::string(value) != "0";
        }();
        return enabled;
    }

private:
    const IxIndexHandle* ih_;
    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    Iid iid_;
    Iid end_;
    BufferPoolManager* bpm_;
    Mode mode_{Mode::HYBRID};
    ScanDirection direction_{ScanDirection::Forward};
    bool coupled_mode_{false};
    bool backward_end_{false};
    bool copy_keys_{false};

    // Legacy cursor used for single-leaf hybrid scans and for internal
    // callers that already hold a structure latch.
    Page* pinned_leaf_page_{nullptr};
    bool pinned_leaf_has_bpm_pin_{false};
    IxNodeHandle leaf_;
    std::shared_lock<std::shared_mutex> leaf_latch_guard_;

    // Coupled cursor.  The batch retains only RID/IID by default.  key() is
    // opt-in for callers that need it; normal IndexScan paths only consume RID.
    static constexpr size_t kMaxBatchLeaves = 8;
    static constexpr size_t kMaxBatchEntries = 1024;
    static constexpr size_t kDuplicateRidVectorLimit = 8;
    std::vector<Rid> batch_;
    std::vector<char> batch_keys_;
    std::vector<char> batch_tail_key_;
    std::vector<Rid> batch_tail_rids_;
    size_t batch_pos_{0};
    bool first_batch_{true};
    bool coupled_end_{false};
    bool unbounded_end_{false};
    std::vector<char> end_key_;
    std::vector<char> last_key_;
    std::vector<uint64_t> emitted_last_key_rids_;
    std::unordered_set<uint64_t> emitted_last_key_rid_set_;
    bool emitted_rid_set_active_{false};
    page_id_t resume_page_no_{IX_NO_PAGE};
    int resume_slot_no_{0};
    uint64_t resume_topology_epoch_{0};
    bool resume_cursor_valid_{false};

    static uint64_t rid_key(const Rid& rid) {
        return (static_cast<uint64_t>(static_cast<uint32_t>(rid.page_no)) << 32) | static_cast<uint32_t>(rid.slot_no);
    }

    const char* batch_key(size_t position) const {
        assert(copy_keys_);
        return batch_keys_.data() + position * ih_->file_hdr_->col_tot_len_;
    }

    Page* fetch_scan_page(page_id_t page_no) {
        if (Page* cached = ih_->cached_page(page_no); cached != nullptr) {
            return cached;
        }
        Page* page = bpm_->fetch_page(PageId{ih_->fd_, page_no});
        assert(page != nullptr);
        return page;
    }

    void prefetch_cached_page(page_id_t page_no) const {
#if defined(__GNUC__) || defined(__clang__)
        if (!prefetch_enabled()) {
            return;
        }
        if (Page* cached = ih_->cached_page(page_no); cached != nullptr) {
            __builtin_prefetch(cached->get_data(), 0, 3);
        }
#else
        (void)page_no;
#endif
    }

    void unpin_scan_page(Page* page) {
        ih_->unpin_if_not_cached(page->get_page_id());
    }

    void clear_emitted_rids() {
        emitted_last_key_rids_.clear();
        emitted_last_key_rid_set_.clear();
        emitted_rid_set_active_ = false;
    }

    void remember_emitted_rid(const Rid& rid) {
        const uint64_t value = rid_key(rid);
        if (!emitted_rid_set_active_ && emitted_last_key_rids_.size() < kDuplicateRidVectorLimit) {
            emitted_last_key_rids_.push_back(value);
            return;
        }
        if (!emitted_rid_set_active_) {
            emitted_last_key_rid_set_.reserve(emitted_last_key_rids_.size() * 2 + 1);
            for (uint64_t old_value : emitted_last_key_rids_) {
                emitted_last_key_rid_set_.insert(old_value);
            }
            emitted_last_key_rids_.clear();
            emitted_rid_set_active_ = true;
        }
        emitted_last_key_rid_set_.insert(value);
    }

    bool last_key_rid_was_emitted(const Rid& rid) const {
        const uint64_t value = rid_key(rid);
        if (emitted_rid_set_active_) {
            return emitted_last_key_rid_set_.find(value) != emitted_last_key_rid_set_.end();
        }
        return std::find(emitted_last_key_rids_.begin(), emitted_last_key_rids_.end(), value) !=
               emitted_last_key_rids_.end();
    }

    void remember_completed_batch_tail() {
        assert(!batch_.empty());
        const char* tail_key = batch_tail_key_.data();
        const bool continues_previous =
            !last_key_.empty() &&
            ix_compare(tail_key, last_key_.data(), ih_->file_hdr_->col_types_, ih_->file_hdr_->col_lens_) == 0;
        if (!continues_previous) {
            last_key_.assign(tail_key, tail_key + ih_->file_hdr_->col_tot_len_);
            clear_emitted_rids();
        }
        for (const Rid& rid : batch_tail_rids_) {
            remember_emitted_rid(rid);
        }
    }

    void pin_current_leaf() {
        if (pinned_leaf_page_ != nullptr || (direction_ == ScanDirection::Forward && iid_ == end_) || backward_end_) {
            return;
        }
        if (Page* cached = ih_->cached_page(iid_.page_no); cached != nullptr) {
            pinned_leaf_page_ = cached;
            pinned_leaf_has_bpm_pin_ = false;
        } else {
            pinned_leaf_page_ = bpm_->fetch_page(PageId{ih_->fd_, iid_.page_no});
            assert(pinned_leaf_page_ != nullptr);
            pinned_leaf_has_bpm_pin_ = true;
        }
        leaf_latch_guard_ = std::shared_lock<std::shared_mutex>(pinned_leaf_page_->latch());
        leaf_ = IxNodeHandle(ih_->file_hdr_.get(), pinned_leaf_page_);
    }

    void unpin_current_leaf() {
        if (pinned_leaf_page_ != nullptr) {
            leaf_latch_guard_.unlock();
            if (pinned_leaf_has_bpm_pin_) {
                bpm_->unpin_page(pinned_leaf_page_->get_page_id(), false);
            }
            // Cache-owned pages carry no scan-owned BPM pin to release.
            pinned_leaf_page_ = nullptr;
            pinned_leaf_has_bpm_pin_ = false;
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
        backward_end_ = direction_ == ScanDirection::Backward;
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
            iid_ = Iid{next_leaf, 0};
        }
        move_legacy_to_end();
    }

    void normalize_backward_position() {
        while (!backward_end_) {
            pin_current_leaf();
            assert(leaf_.is_leaf_page());
            int slot = std::min(iid_.slot_no, leaf_.get_size());
            if (iid_.page_no == end_.page_no && slot <= end_.slot_no) {
                move_legacy_to_end();
                return;
            }
            if (slot > 0) {
                iid_.slot_no = slot - 1;
                return;
            }
            const page_id_t prev_leaf = leaf_.get_prev_leaf();
            if (prev_leaf == IX_LEAF_HEADER_PAGE || prev_leaf == IX_NO_PAGE) {
                move_legacy_to_end();
                return;
            }
            unpin_current_leaf();
            iid_ = Iid{prev_leaf, std::numeric_limits<int>::max()};
        }
    }

    void capture_end_bound() {
        if (iid_ == end_) {
            coupled_end_ = true;
            return;
        }
        Page* page = fetch_scan_page(end_.page_no);
        std::shared_lock<std::shared_mutex> leaf_lock(page->latch());
        IxNodeHandle end_leaf(ih_->file_hdr_.get(), page);
        if (end_.slot_no < end_leaf.get_size()) {
            end_key_.assign(end_leaf.get_key(end_.slot_no),
                            end_leaf.get_key(end_.slot_no) + ih_->file_hdr_->col_tot_len_);
        } else {
            unbounded_end_ = end_.page_no == ih_->file_hdr_->last_leaf_;
        }
        leaf_lock.unlock();
        unpin_scan_page(page);
    }

    // Locate the lower bound while holding the coupled structure latch and
    // return the final leaf still pinned.  This avoids lower_bound()'s final
    // unpin followed by a second fetch of the same leaf.
    Page* fetch_lower_bound_leaf(const char* key, Iid* cursor) {
        Page* page = fetch_scan_page(ih_->file_hdr_->root_page_);
        IxNodeHandle node(ih_->file_hdr_.get(), page);
        while (!node.is_leaf_page()) {
            int child_idx = node.lower_bound(key);
            if (child_idx >= node.get_size()) {
                child_idx = node.get_size() - 1;
            } else if (child_idx > 0 && ix_compare(node.get_key(child_idx), key, ih_->file_hdr_->col_types_,
                                                   ih_->file_hdr_->col_lens_) > 0) {
                --child_idx;
            }
            page_id_t child_page_no = node.value_at(child_idx);
            prefetch_cached_page(child_page_no);
            unpin_scan_page(page);
            page = fetch_scan_page(child_page_no);
            node = IxNodeHandle(ih_->file_hdr_.get(), page);
        }

        std::shared_lock<std::shared_mutex> leaf_guard(page->latch());
        int slot = node.lower_bound(key);
        if (slot == node.get_size() && node.get_page_no() != ih_->file_hdr_->last_leaf_) {
            page_id_t next_leaf = node.get_next_leaf();
            leaf_guard.unlock();
            unpin_scan_page(page);
            page = fetch_scan_page(next_leaf);
            slot = 0;
        } else {
            leaf_guard.unlock();
        }
        *cursor = Iid{page->get_page_id().page_no, slot};
        return page;
    }

    void append_batch_entries(IxNodeHandle& leaf, int begin, int end) {
        assert(begin >= 0 && begin <= end && end <= leaf.get_size());
        if (begin == end) {
            return;
        }

        const size_t count = static_cast<size_t>(end - begin);
        const size_t old_size = batch_.size();
        batch_.resize(old_size + count);
        std::memcpy(batch_.data() + old_size, leaf.get_rid(begin), count * sizeof(Rid));
        if (copy_keys_) {
            const char* first_key = leaf.get_key(begin);
            batch_keys_.insert(batch_keys_.end(), first_key, first_key + count * ih_->file_hdr_->col_tot_len_);
        }

        const int tail_slot = end - 1;
        const char* tail_key = leaf.get_key(tail_slot);
        batch_tail_key_.assign(tail_key, tail_key + ih_->file_hdr_->col_tot_len_);
        batch_tail_rids_.clear();
        for (int slot = tail_slot; slot >= begin; --slot) {
            if (ix_compare(leaf.get_key(slot), batch_tail_key_.data(), ih_->file_hdr_->col_types_,
                           ih_->file_hdr_->col_lens_) != 0) {
                break;
            }
            batch_tail_rids_.push_back(*leaf.get_rid(slot));
        }
    }

    void load_coupled_batch() {
        batch_.clear();
        batch_keys_.clear();
        batch_tail_key_.clear();
        batch_tail_rids_.clear();
        batch_pos_ = 0;
        const size_t initial_batch_capacity = std::min(kMaxBatchEntries, static_cast<size_t>(512));
        if (batch_.capacity() < initial_batch_capacity) {
            batch_.reserve(initial_batch_capacity);
        }
        if (copy_keys_ && batch_keys_.capacity() < initial_batch_capacity * ih_->file_hdr_->col_tot_len_) {
            batch_keys_.reserve(initial_batch_capacity * ih_->file_hdr_->col_tot_len_);
        }
        if (coupled_end_) {
            release_index_latch_if_held();
            return;
        }
        if (!index_latch_guard_.owns_lock()) {
            index_latch_guard_ = ih_->lock_shared();
        }

        Iid cursor = iid_;
        Page* page = nullptr;
        if (first_batch_) {
            page = fetch_scan_page(cursor.page_no);
        } else {
            const bool can_use_fast_resume =
                resume_cursor_valid_ && resume_topology_epoch_ == ih_->topology_epoch_.load(std::memory_order_relaxed);
            if (can_use_fast_resume) {
                cursor = Iid{resume_page_no_, resume_slot_no_};
                page = fetch_scan_page(resume_page_no_);
            } else {
                page = fetch_lower_bound_leaf(last_key_.data(), &cursor);
            }
            while (page != nullptr) {
                std::shared_lock<std::shared_mutex> leaf_lock(page->latch());
                IxNodeHandle leaf(ih_->file_hdr_.get(), page);
                int slot = cursor.slot_no;
                if (can_use_fast_resume && page->get_page_id().page_no == resume_page_no_) {
                    // Leaf-local inserts and deletes do not advance the
                    // topology epoch. Re-seek within the same leaf so slot
                    // shifts cannot skip an entry; emitted-RID filtering below
                    // still handles duplicates equal to the batch tail.
                    slot = leaf.lower_bound(last_key_.data());
                }
                while (slot < leaf.get_size()) {
                    const int cmp = ix_compare(leaf.get_key(slot), last_key_.data(), ih_->file_hdr_->col_types_,
                                               ih_->file_hdr_->col_lens_);
                    if (cmp > 0 || (cmp == 0 && !last_key_rid_was_emitted(*leaf.get_rid(slot)))) {
                        cursor.slot_no = slot;
                        break;
                    }
                    ++slot;
                }
                const bool found = slot < leaf.get_size();
                const page_id_t next_leaf = leaf.get_next_leaf();
                const bool at_last =
                    leaf.get_page_no() == ih_->file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE;
                leaf_lock.unlock();
                if (found) {
                    cursor.slot_no = slot;
                    break;
                }
                prefetch_cached_page(next_leaf);
                unpin_scan_page(page);
                if (at_last) {
                    page = nullptr;
                    break;
                }
                cursor = Iid{next_leaf, 0};
                page = fetch_scan_page(next_leaf);
            }
        }
        first_batch_ = false;

        size_t leaf_count = 0;
        page_id_t resume_page_no = IX_NO_PAGE;
        int resume_slot_no = 0;
        while (page != nullptr && !coupled_end_ && (leaf_count < kMaxBatchLeaves || batch_.empty()) &&
               batch_.size() < kMaxBatchEntries) {
            ++leaf_count;
            std::shared_lock<std::shared_mutex> leaf_lock(page->latch());
            IxNodeHandle leaf(ih_->file_hdr_.get(), page);
            const page_id_t next_leaf = leaf.get_next_leaf();
            const bool at_last = leaf.get_page_no() == ih_->file_hdr_->last_leaf_ || next_leaf == IX_LEAF_HEADER_PAGE;
            const int begin = cursor.slot_no;
            int stop = leaf.get_size();
            int logical_end_slot = leaf.get_size();
            bool end_key_reached_in_leaf = false;
            if (!unbounded_end_) {
                if (!end_key_.empty()) {
                    logical_end_slot = leaf.lower_bound(end_key_.data());
                    end_key_reached_in_leaf = logical_end_slot < leaf.get_size();
                    if (end_key_reached_in_leaf) {
                        stop = logical_end_slot <= begin ? begin : std::min(stop, logical_end_slot);
                    }
                } else if (leaf.get_page_no() == end_.page_no) {
                    stop = std::min(stop, end_.slot_no);
                }
            }
            stop = std::min(stop, begin + static_cast<int>(kMaxBatchEntries - batch_.size()));
            const bool reached_end_key = end_key_reached_in_leaf && logical_end_slot <= stop;
            append_batch_entries(leaf, begin, stop);
            if (!unbounded_end_ &&
                (reached_end_key || (end_key_.empty() && leaf.get_page_no() == end_.page_no && stop >= end_.slot_no))) {
                coupled_end_ = true;
            }
            const bool reached_batch_limit = batch_.size() >= kMaxBatchEntries;
            const bool reached_leaf_limit = leaf_count >= kMaxBatchLeaves && !batch_.empty();
            if (reached_batch_limit) {
                resume_page_no = leaf.get_page_no();
                resume_slot_no = stop;
            } else if (reached_leaf_limit) {
                // Resume from the leaf containing the batch tail. Starting
                // directly at next_leaf would miss a concurrent leaf-local
                // insert whose key sorts after the emitted tail key.
                resume_page_no = leaf.get_page_no();
                resume_slot_no = stop;
            }
            if (!at_last && !coupled_end_ && !reached_batch_limit && !reached_leaf_limit) {
                prefetch_cached_page(next_leaf);
            }
            leaf_lock.unlock();
            unpin_scan_page(page);
            if (coupled_end_ || at_last || reached_batch_limit || reached_leaf_limit) {
                break;
            }
            cursor = Iid{next_leaf, 0};
            page = fetch_scan_page(next_leaf);
        }

        if (page != nullptr) {
            // The loop always releases the current page. This guard handles
            // the case where no entry was available in an empty trailing leaf.
            page = nullptr;
        }
        if (batch_.empty()) {
            coupled_end_ = true;
        }
        if (!batch_.empty() && !coupled_end_ && resume_page_no != IX_NO_PAGE && resume_page_no != IX_LEAF_HEADER_PAGE) {
            resume_page_no_ = resume_page_no;
            resume_slot_no_ = resume_slot_no;
            resume_topology_epoch_ = ih_->topology_epoch_.load(std::memory_order_relaxed);
            resume_cursor_valid_ = true;
        } else {
            resume_slot_no_ = 0;
            resume_cursor_valid_ = false;
        }
        release_index_latch_if_held();
    }

public:
    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm,
           bool acquire_index_latch = true, bool copy_keys = false, ScanDirection direction = ScanDirection::Forward)
        : IxScan(ih, lower, upper, bpm, acquire_index_latch ? ih->lock_shared() : IxIndexHandle::SharedIndexLatch{},
                 copy_keys, direction) {}

    IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm,
           IxIndexHandle::SharedIndexLatch index_latch_guard, bool copy_keys = false,
           ScanDirection direction = ScanDirection::Forward)
        : ih_(ih), index_latch_guard_(std::move(index_latch_guard)),
          iid_(direction == ScanDirection::Forward ? lower : upper),
          end_(direction == ScanDirection::Forward ? upper : lower), bpm_(bpm),
          mode_(index_latch_guard_.owns_lock() && direction == ScanDirection::Forward ? configured_mode()
                                                                                      : Mode::LEGACY),
          direction_(direction), copy_keys_(copy_keys) {
        if (direction_ == ScanDirection::Backward) {
            normalize_backward_position();
            return;
        }
        const bool use_single_leaf =
            index_latch_guard_.owns_lock() &&
            (mode_ == Mode::LEGACY || (mode_ == Mode::HYBRID && lower.page_no == upper.page_no));
        if (use_single_leaf) {
            normalize_legacy_position();
            const bool pinned_root =
                pinned_leaf_page_ != nullptr && pinned_leaf_page_->get_page_id().page_no == ih_->file_hdr_->root_page_;
            if (mode_ == Mode::HYBRID && !pinned_root) {
                // The leaf S latch is acquired while the tree S latch is still
                // held. Structural writers take tree X before leaf X, so a
                // single-leaf scan no longer needs to retain tree S while its
                // consumer processes tuples.
                release_index_latch_if_held();
            }
            return;
        }

        if (index_latch_guard_.owns_lock()) {
            coupled_mode_ = true;
            capture_end_bound();
            load_coupled_batch();
        } else {
            normalize_legacy_position();
        }
    }

    ~IxScan() override {
        unpin_current_leaf();
        release_index_latch_if_held();
    }

    IxScan(const IxScan&) = delete;
    IxScan& operator=(const IxScan&) = delete;

    void next() override;

    bool is_end() const override {
        return coupled_mode_ ? (coupled_end_ && batch_pos_ >= batch_.size())
                             : (direction_ == ScanDirection::Backward ? backward_end_ : iid_ == end_);
    }

    Rid rid() const override {
        if (!coupled_mode_) {
            return *leaf_.get_rid(iid_.slot_no);
        }
        return batch_[batch_pos_];
    }

    const char* key() const {
        if (!coupled_mode_) {
            return leaf_.get_key(iid_.slot_no);
        }
        return batch_key(batch_pos_);
    }

    const Iid& iid() const {
        return iid_;
    }
};
