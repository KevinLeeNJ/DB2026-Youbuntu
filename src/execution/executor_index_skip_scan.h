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

namespace rmdb::exec {
class IndexSkipScanExecutor : public IndexScanExecutor {
    struct IndexRange {
        Iid lower;
        Iid upper;
    };

    std::unique_ptr<rmdb::access::IndexCursor> index_cursor_; // 持有索引锁 + 提供范围查询 + 当前子范围扫描
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
        Iid cursor = index_cursor_->lower_bound(min_key.data());
        Iid end = index_cursor_->upper_bound(max_key.data());

        while (cursor != end) {
            auto probe = index_cursor_->probe_first_key(cursor, end, index_meta_.col_tot_len);
            if (!probe.has_value()) {
                break;
            }
            const auto& prefix_key = *probe;

            auto lower_key = min_key;
            auto upper_key = max_key;
            auto next_prefix_key = max_key;
            copy_prefix_from_key(lower_key, prefix_key.data(), suffix_pos);
            copy_prefix_from_key(upper_key, prefix_key.data(), suffix_pos);
            copy_prefix_from_key(next_prefix_key, prefix_key.data(), suffix_pos);
            apply_suffix_equalities(lower_key, upper_key, constraints, suffix_pos);

            Iid lower = index_cursor_->lower_bound(lower_key.data());
            Iid upper = index_cursor_->upper_bound(upper_key.data());
            if (lower != upper) {
                ranges_.push_back(IndexRange{lower, upper});
            }

            Iid next_cursor = index_cursor_->upper_bound(next_prefix_key.data());
            if (next_cursor == cursor) {
                break;
            }
            cursor = next_cursor;
        }
    }

    /// 打开下一个子范围；返回 true 表示成功打开一个非空范围。
    bool open_next_range() {
        while (next_range_pos_ < ranges_.size()) {
            const auto range = ranges_[next_range_pos_++];
            index_cursor_->open_range_no_lock(range.lower, range.upper);
            if (!index_cursor_->is_end()) {
                return true;
            }
        }
        return false;
    }

    /// skip scan 专用 advance：直接用 index_cursor_ 迭代，不走基类 scan_。
    void advance_to_match_skip() {
        buffered_record_.reset();
        while (index_cursor_ != nullptr && !index_cursor_->is_end()) {
            rid_ = index_cursor_->rid();
            auto rec = index_cursor_->get_visible_record(context_);
            if (rec == nullptr) {
                index_cursor_->next();
                continue;
            }
            bool match = true;
            for (const auto& cond : fed_conds_) {
                if (!compare(cond, *rec)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                record_tuple_read(rid_);
                buffered_record_ = std::move(rec);
                return;
            }
            index_cursor_->next();
        }
        // 当前子范围耗尽，尝试打开下一个
        while (index_cursor_ != nullptr && open_next_range()) {
            advance_to_match_skip();
            return;
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
            auto table_cursor = table_access_.open_table_scan(tab_name_);
            table_cursor->open();
            scan_ = std::move(table_cursor);
            IndexScanExecutor::advance_to_match();
            return;
        }

        index_cursor_ = table_access_.open_index_scan(tab_name_, index_meta_.cols);
        index_cursor_->acquire_shared_lock();
        auto constraints = build_constraints();
        auto suffix_pos = first_suffix_equality_pos(constraints);
        if (!suffix_pos.has_value()) {
            index_cursor_.reset();
            return;
        }

        build_ranges(constraints, *suffix_pos);
        if (open_next_range()) {
            advance_to_match_skip();
        } else {
            index_cursor_.reset();
        }
    }

    void nextTuple() override {
        if (use_heap_scan_for_mvcc_) {
            scan_->next();
            IndexScanExecutor::advance_to_match();
            return;
        }
        if (index_cursor_ == nullptr) {
            return;
        }
        index_cursor_->next();
        advance_to_match_skip();
    }

    bool is_end() const override {
        if (use_heap_scan_for_mvcc_) {
            return scan_ == nullptr || scan_->is_end();
        }
        return index_cursor_ == nullptr || index_cursor_->is_end();
    }

    std::string getType() override {
        return "IndexSkipScanExecutor";
    }
};

} // namespace rmdb::exec

namespace rmdb {
using exec::IndexSkipScanExecutor;
} // namespace rmdb
