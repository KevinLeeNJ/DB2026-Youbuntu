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

/**
 * @brief 针对复合索引后缀等值条件执行多段范围扫描。
 *
 * 当索引前缀列缺少条件、但后缀列存在等值条件时，先枚举不同前缀，再为每个
 * 前缀构造一个连续的索引范围；若历史索引候选不可安全利用，则回退到父类的
 * 全表候选匹配逻辑。
 */
class IndexSkipScanExecutor : public IndexScanExecutor {
    /**
     * @brief 保存一个待打开的半开索引 RID 区间。
     */
    struct IndexRange {
        Iid lower;
        Iid upper;
    };

    IxIndexHandle* ih_ = nullptr;
    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    std::vector<IndexRange> ranges_;
    size_t next_range_pos_ = 0;

    /**
     * @brief 构造由所有索引列最小值组成的复合键。
     * @return 索引字典序中的最小复合键。
     */
    std::vector<char> make_min_key() const {
        std::vector<char> key(index_meta_.col_tot_len);
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_min(key.data() + offset, col);
            offset += col.len;
        }
        return key;
    }

    /**
     * @brief 构造由所有索引列最大值组成的复合键。
     * @return 索引字典序中的最大复合键。
     */
    std::vector<char> make_max_key() const {
        std::vector<char> key(index_meta_.col_tot_len);
        int offset = 0;
        for (const auto& col : index_meta_.cols) {
            write_max(key.data() + offset, col);
            offset += col.len;
        }
        return key;
    }

    /**
     * @brief 查找第一个位于缺失前缀之后的等值约束列。
     * @param constraints 已从查询条件提取的索引列约束。
     * @return 后缀等值列下标；不存在可用于跳跃扫描的后缀条件时返回 nullopt。
     */
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

    /**
     * @brief 将已探测索引键的前缀复制到新的复合边界键中。
     * @param key 待修改的目标键。
     * @param source_key 提供前缀字节的索引键。
     * @param prefix_col_count 要复制的前缀列数量。
     */
    void copy_prefix_from_key(std::vector<char>& key, const char* source_key, size_t prefix_col_count) const {
        int offset = 0;
        for (size_t i = 0; i < prefix_col_count; ++i) {
            const auto& col = index_meta_.cols[i];
            memcpy(key.data() + offset, source_key + offset, col.len);
            offset += col.len;
        }
    }

    /**
     * @brief 将从 suffix_pos 开始连续出现的等值条件写入范围上下界。
     * @param lower_key 待修改的下界键。
     * @param upper_key 待修改的上界键。
     * @param constraints 索引列约束。
     * @param suffix_pos 后缀等值条件的起始列下标。
     */
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

    /**
     * @brief 枚举不同前缀并为每个前缀构造后缀等值扫描区间。
     * @param constraints 当前索引列约束。
     * @param suffix_pos 第一个后缀等值列的位置。
     *
     * 每次先读取当前前缀的代表键，再把后缀等值条件写入上下界，并把游标跳到
     * 下一个前缀的起点；这样不会为同一前缀重复生成范围。
     */
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
                IxScan probe(ih_, cursor, end, sm_manager_->get_bpm(), false);
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

    /**
     * @brief 打开下一个非空索引范围，所有范围耗尽后释放索引共享锁。
     */
    void open_next_range() {
        scan_.reset();
        while (next_range_pos_ < ranges_.size()) {
            const auto range = ranges_[next_range_pos_++];
            scan_ = std::make_unique<IxScan>(ih_, range.lower, range.upper, sm_manager_->get_bpm(), false);
            if (!scan_->is_end()) {
                return;
            }
        }
        scan_.reset();
        if (index_latch_guard_.owns_lock()) {
            index_latch_guard_.unlock();
        }
    }

    /**
     * @brief 在当前范围及后续范围中寻找下一条有效记录。
     *
     * 当前范围没有匹配记录时切换到下一个范围；具体的 MVCC 和谓词判断复用
     * IndexScanExecutor::advance_to_match()，保证两种索引扫描的结果语义一致。
     */
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
    /**
     * @brief 创建复合索引跳跃扫描执行器。
     * @param sm_manager 系统管理器。
     * @param tab_name 要扫描的表名。
     * @param conds 初始扫描条件。
     * @param index_col_names 复合索引列顺序。
     * @param context 当前执行上下文。
     */
    IndexSkipScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds,
                          std::vector<std::string> index_col_names, Context* context)
        : IndexScanExecutor(sm_manager, std::move(tab_name), std::move(conds), std::move(index_col_names), context) {}

    /**
     * @brief 初始化跳跃扫描范围并定位到第一条匹配记录。
     *
     * 有历史候选需求时直接回退为记录文件扫描；否则获取索引锁、寻找后缀等值
     * 起点、构造多个范围并打开第一个非空范围。
     */
    void beginTuple() override {
        record_predicate_read();

        // 历史索引候选没有稳定的前缀顺序，直接复用父类匹配逻辑保证快照正确性。
        if (needs_historical_index_candidates()) {
            scan_ = std::make_unique<RmScan>(fh_);
            IndexScanExecutor::advance_to_match();
            return;
        }

        ih_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
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

    /**
     * @brief 推进当前范围并在必要时切换到后续范围。
     */
    void nextTuple() override {
        if (scan_ == nullptr) {
            return;
        }
        scan_->next();
        advance_to_match();
    }

    /**
     * @brief 判断当前范围或全部跳跃范围是否已经耗尽。
     * @return 没有活动扫描器或当前扫描器结束时返回 true。
     */
    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    /**
     * @brief 返回执行器类型名称。
     * @return "IndexSkipScanExecutor"。
     */
    std::string getType() override {
        return "IndexSkipScanExecutor";
    }
};
