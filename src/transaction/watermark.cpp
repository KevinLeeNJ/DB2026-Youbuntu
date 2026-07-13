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
    std::lock_guard<std::mutex> lock(latch_);
    auto it = current_reads_.find(read_ts);
    if (it != current_reads_.end()) {
        ++it->second;
    } else {
        current_reads_.emplace(read_ts, 1);
        // 新的最小读时间戳会推低水位线
        if (read_ts < watermark_) {
            watermark_ = read_ts;
        }
    }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = current_reads_.find(read_ts);
    if (it == current_reads_.end()) {
        return;
    }
    if (--it->second == 0) {
        current_reads_.erase(it);
    }
    // 重算水位线：无活跃事务时回退到 commit_ts_，否则取最小读时间戳
    if (current_reads_.empty()) {
        watermark_ = commit_ts_;
    } else {
        watermark_ = current_reads_.begin()->first;
    }
}

void Watermark::UpdateTxnReadTs(timestamp_t old_read_ts, timestamp_t new_read_ts) {
    if (old_read_ts == new_read_ts) {
        return;
    }

    std::lock_guard<std::mutex> lock(latch_);
    auto old_it = current_reads_.find(old_read_ts);
    if (old_it != current_reads_.end()) {
        if (--old_it->second == 0) {
            current_reads_.erase(old_it);
        }
    }
    ++current_reads_[new_read_ts];
    watermark_ = current_reads_.begin()->first;
}

void Watermark::UpdateCommitTs(timestamp_t commit_ts) {
    std::lock_guard<std::mutex> lock(latch_);
    if (commit_ts > commit_ts_) {
        commit_ts_ = commit_ts;
    }
    // 无活跃事务时，水位线跟随 commit_ts_ 上移
    if (current_reads_.empty()) {
        watermark_ = commit_ts_;
    }
}

timestamp_t Watermark::GetWatermark() {
    std::lock_guard<std::mutex> lock(latch_);
    return watermark_;
}
