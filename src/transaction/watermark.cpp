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

#include "transaction/watermark.h"

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
    (void)AddTxnSlot(read_ts);
}

size_t Watermark::AddTxnSlot(timestamp_t read_ts) {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = std::find(active_read_ts_.begin(), active_read_ts_.end(), inactive_read_ts());
    size_t slot;
    if (it == active_read_ts_.end()) {
        slot = active_read_ts_.size();
        active_read_ts_.push_back(read_ts);
    } else {
        slot = static_cast<size_t>(it - active_read_ts_.begin());
        *it = read_ts;
    }
    recompute_watermark_locked();
    return slot;
}

void Watermark::RemoveTxnSlot(size_t slot) {
    std::lock_guard<std::mutex> lock(latch_);
    if (slot >= active_read_ts_.size() || active_read_ts_[slot] == inactive_read_ts()) {
        return;
    }
    active_read_ts_[slot] = inactive_read_ts();
    recompute_watermark_locked();
}

void Watermark::UpdateTxnReadTsSlot(size_t slot, timestamp_t new_read_ts) {
    std::lock_guard<std::mutex> lock(latch_);
    if (slot >= active_read_ts_.size() || active_read_ts_[slot] == inactive_read_ts()) {
        return;
    }
    active_read_ts_[slot] = new_read_ts;
    recompute_watermark_locked();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = std::find(active_read_ts_.begin(), active_read_ts_.end(), read_ts);
    if (it == active_read_ts_.end()) {
        return;
    }
    *it = inactive_read_ts();
    recompute_watermark_locked();
}

void Watermark::UpdateTxnReadTs(timestamp_t old_read_ts, timestamp_t new_read_ts) {
    if (old_read_ts == new_read_ts) {
        return;
    }

    std::lock_guard<std::mutex> lock(latch_);
    auto old_it = std::find(active_read_ts_.begin(), active_read_ts_.end(), old_read_ts);
    if (old_it != active_read_ts_.end()) {
        *old_it = new_read_ts;
    } else {
        auto free_it = std::find(active_read_ts_.begin(), active_read_ts_.end(), inactive_read_ts());
        if (free_it == active_read_ts_.end()) {
            active_read_ts_.push_back(new_read_ts);
        } else {
            *free_it = new_read_ts;
        }
    }
    recompute_watermark_locked();
}

void Watermark::UpdateCommitTs(timestamp_t commit_ts) {
    std::lock_guard<std::mutex> lock(latch_);
    if (commit_ts > commit_ts_) {
        commit_ts_ = commit_ts;
    }
    // 无活跃事务时，水位线跟随 commit_ts_ 上移
    recompute_watermark_locked();
}

timestamp_t Watermark::GetWatermark() {
    std::lock_guard<std::mutex> lock(latch_);
    return watermark_;
}

void Watermark::recompute_watermark_locked() {
    timestamp_t minimum = commit_ts_;
    for (timestamp_t read_ts : active_read_ts_) {
        if (read_ts != inactive_read_ts()) {
            minimum = std::min(minimum, read_ts);
        }
    }
    watermark_ = minimum;
}
