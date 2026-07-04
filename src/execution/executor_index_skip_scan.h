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

    IxIndexHandle* ih_ = nullptr;
    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    std::vector<IndexRange> ranges_;
    size_t next_range_pos_ = 0;

    std::vector<char> make_min_key() const {
        std::vector<char> key(index_meta_.col_tot_len);
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_min(key.data() + offset, col);
            offset += col.len;
        }
        return key;
    }

    std::vector<char> make_max_key() const {
        std::vector<char> key(index_meta_.col_tot_len);
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_max(key.data() + offset, col);
            offset += col.len;
        }
        return key;
    }

    std::optional<size_t> first_suffix_equality_pos(const std::map<std::string, ColumnConstraint>& constraints) const {
        bool saw_missing_prefix = false;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            const auto& col = index_meta_.cols[i];
            auto constraint_it = constraints.find(col.name);
            bool has_eq = constraint_it != constraints.end() && constraint_it->second.eq.has_value();
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

    void apply_suffix_equalities(std::vector<char>& lower_key, std::vector<char>& upper_key,
                                 const std::map<std::string, ColumnConstraint>& constraints, size_t suffix_pos) const {
        int offset = 0;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            const auto& col = index_meta_.cols[i];
            if (i >= suffix_pos) {
                auto constraint_it = constraints.find(col.name);
                if (constraint_it == constraints.end() || !constraint_it->second.eq.has_value()) {
                    break;
                }
                memcpy(lower_key.data() + offset, constraint_it->second.eq->data(), col.len);
                memcpy(upper_key.data() + offset, constraint_it->second.eq->data(), col.len);
            }
            offset += col.len;
        }
    }

    void build_ranges(const std::map<std::string, ColumnConstraint>& constraints, size_t suffix_pos) {
        ranges_.clear();
        next_range_pos_ = 0;

        auto min_key = make_min_key();
        auto max_key = make_max_key();
        Iid cursor = ih_->lower_bound(min_key.data());
        Iid end = ih_->upper_bound(max_key.data());

        while (cursor != end) {
            std::vector<char> prefix_key;
            {
                IxScan probe(ih_, cursor, end, schema_manager_->get_bpm(), false);
                if (probe.is_end()) {
                    break;
                }
                prefix_key.assign(probe.key(), probe.key() + index_meta_.col_tot_len);
            }

            auto lower_key = min_key;
            auto upper_key = max_key;
            auto next_prefix_key = max_key;
            copy_prefix_from_key(lower_key, prefix_key.data(), suffix_pos);
            copy_prefix_from_key(upper_key, prefix_key.data(), suffix_pos);
            copy_prefix_from_key(next_prefix_key, prefix_key.data(), suffix_pos);
            apply_suffix_equalities(lower_key, upper_key, constraints, suffix_pos);

            Iid lower = ih_->lower_bound(lower_key.data());
            Iid upper = ih_->upper_bound(upper_key.data());
            if (lower != upper) {
                ranges_.push_back(IndexRange{lower, upper});
            }

            Iid next_cursor = ih_->upper_bound(next_prefix_key.data());
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
            scan_ = std::make_unique<IxScan>(ih_, range.lower, range.upper, schema_manager_->get_bpm(), false);
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
    IndexSkipScanExecutor(SchemaManager* schema_manager, std::string tab_name, std::vector<Condition> conds,
                          std::vector<std::string> index_col_names, Context* context)
        : IndexScanExecutor(schema_manager, std::move(tab_name), std::move(conds), std::move(index_col_names),
                            context) {}

    void beginTuple() override {
        record_predicate_read();

        use_heap_scan_for_mvcc_ = needs_historical_heap_scan();
        if (use_heap_scan_for_mvcc_) {
            scan_ = std::make_unique<RmScan>(fh_);
            IndexScanExecutor::advance_to_match();
            return;
        }

        ih_ = schema_manager_->get_index_handle(tab_name_, index_meta_.cols);
        index_latch_guard_ = ih_->lock_shared();
        auto constraints = build_constraints();
        auto suffix_pos = first_suffix_equality_pos(constraints);
        if (!suffix_pos.has_value()) {
            scan_.reset();
            index_latch_guard_.unlock();
            return;
        }

        build_ranges(constraints, *suffix_pos);
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
