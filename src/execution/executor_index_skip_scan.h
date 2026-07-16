/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "executor_index_scan.h"

class IndexSkipScanExecutor : public IndexScanExecutor {
    struct IndexRange {
        Iid lower;
        Iid upper;
    };

    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    std::vector<IndexRange> ranges_;
    size_t next_range_pos_ = 0;
    std::vector<char> prefix_key_;
    std::vector<char> range_lower_key_;
    std::vector<char> range_upper_key_;
    std::vector<char> next_prefix_key_;

    std::optional<size_t> first_suffix_equality_pos() const {
        bool saw_missing_prefix = false;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            bool has_eq = constraints_[i].eq_present;
            if (has_eq && saw_missing_prefix) {
                return i;
            }
            if (!has_eq) {
                saw_missing_prefix = true;
            }
        }
        return std::nullopt;
    }

    void copy_prefix_from_key(std::vector<char>& key, const char* source_key, size_t prefix_col_count) const {
        int offset = 0;
        for (size_t i = 0; i < prefix_col_count; ++i) {
            const auto& col = index_meta_.cols[i];
            memcpy(key.data() + offset, source_key + offset, col.len);
            offset += col.len;
        }
    }

    void apply_suffix_equalities(std::vector<char>& lower_key, std::vector<char>& upper_key, size_t suffix_pos) const {
        int offset = 0;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            const auto& col = index_meta_.cols[i];
            if (i >= suffix_pos) {
                if (!constraints_[i].eq_present) {
                    break;
                }
                memcpy(lower_key.data() + offset, constraints_[i].eq.data(), col.len);
                memcpy(upper_key.data() + offset, constraints_[i].eq.data(), col.len);
            }
            offset += col.len;
        }
    }

    void build_ranges(size_t suffix_pos) {
        ranges_.clear();
        next_range_pos_ = 0;

        reset_key_bounds();
        const auto& min_key = lower_key_;
        const auto& max_key = upper_key_;
        Iid cursor = ih_->lower_bound(min_key.data());
        Iid end = ih_->upper_bound(max_key.data());

        while (cursor != end) {
            {
                IxScan probe(ih_, cursor, end, sm_manager_->get_bpm(), false);
                if (probe.is_end()) {
                    break;
                }
                memcpy(prefix_key_.data(), probe.key(), index_meta_.col_tot_len);
            }

            memcpy(range_lower_key_.data(), min_key.data(), index_meta_.col_tot_len);
            memcpy(range_upper_key_.data(), max_key.data(), index_meta_.col_tot_len);
            memcpy(next_prefix_key_.data(), max_key.data(), index_meta_.col_tot_len);
            copy_prefix_from_key(range_lower_key_, prefix_key_.data(), suffix_pos);
            copy_prefix_from_key(range_upper_key_, prefix_key_.data(), suffix_pos);
            copy_prefix_from_key(next_prefix_key_, prefix_key_.data(), suffix_pos);
            apply_suffix_equalities(range_lower_key_, range_upper_key_, suffix_pos);

            Iid lower = ih_->lower_bound(range_lower_key_.data());
            Iid upper = ih_->upper_bound(range_upper_key_.data());
            if (lower != upper) {
                ranges_.push_back(IndexRange{lower, upper});
            }

            Iid next_cursor = ih_->upper_bound(next_prefix_key_.data());
            if (next_cursor == cursor) {
                break;
            }
            cursor = next_cursor;
        }
    }

    void open_next_range() {
        scan_.reset();
        while (next_range_pos_ < ranges_.size()) {
            const auto range = ranges_[next_range_pos_++];
            scan_ = std::make_unique<IxScan>(ih_, range.lower, range.upper, sm_manager_->get_bpm());
            if (!scan_->is_end()) {
                return;
            }
        }
        scan_.reset();
        if (index_latch_guard_.owns_lock()) {
            index_latch_guard_.unlock();
        }
    }

    void advance_to_match() {
        buffered_record_.reset();
        while (scan_ != nullptr) {
            IndexScanExecutor::advance_to_match();
            if (buffered_record_ != nullptr) {
                return;
            }
            open_next_range();
        }
    }

public:
    IndexSkipScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds,
                          std::vector<std::string> index_col_names, Context* context)
        : IndexScanExecutor(sm_manager, std::move(tab_name), std::move(conds), std::move(index_col_names), context) {
        prefix_key_.resize(index_meta_.col_tot_len);
        range_lower_key_.resize(index_meta_.col_tot_len);
        range_upper_key_.resize(index_meta_.col_tot_len);
        next_prefix_key_.resize(index_meta_.col_tot_len);
    }

    void beginTuple() override {
        scan_.reset();
        ranges_.clear();
        next_range_pos_ = 0;
        if (index_latch_guard_.owns_lock()) {
            index_latch_guard_.unlock();
        }
        historical_candidates_merged_ = false;
        record_predicate_read();

        if (needs_historical_index_candidates()) {
            // The historical fallback is a heap scan, not index order.  Do not
            // let MIN/MAX planning use the ordered-index shortcut here.
            historical_candidates_merged_ = true;
            scan_ = std::make_unique<RmScan>(fh_);
            IndexScanExecutor::advance_to_match();
            return;
        }

        index_latch_guard_ = ih_->lock_shared();
        auto suffix_pos = first_suffix_equality_pos();
        if (!suffix_pos.has_value()) {
            scan_.reset();
            index_latch_guard_.unlock();
            return;
        }

        build_ranges(*suffix_pos);
        index_latch_guard_.unlock();
        open_next_range();
        advance_to_match();
    }

    void nextTuple() override {
        if (scan_ == nullptr) {
            return;
        }
        scan_->next();
        advance_to_match();
    }

    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    std::string getType() override {
        return "IndexSkipScanExecutor";
    }
};
