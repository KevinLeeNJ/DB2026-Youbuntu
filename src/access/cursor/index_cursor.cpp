/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "access/cursor/index_cursor.h"

#include "access/mvcc_access.h"

namespace rmdb::access {

IndexCursor::IndexCursor(IxIndexHandle* ih, RmFileHandle* fh, BufferPoolManager* bpm) : ih_(ih), fh_(fh), bpm_(bpm) {}

IndexCursor::~IndexCursor() = default;

void IndexCursor::open_range(const std::vector<char>& lower_key, const std::vector<char>& upper_key,
                             bool lower_exclusive, bool upper_inclusive) {
    Iid lower, upper;
    if (!lower_exclusive && upper_inclusive && lower_key == upper_key) {
        auto [lo, hi] = ih_->equal_range(lower_key.data());
        lower = lo;
        upper = hi;
    } else {
        lower = lower_exclusive ? ih_->upper_bound(lower_key.data()) : ih_->lower_bound(lower_key.data());
        upper = upper_inclusive ? ih_->upper_bound(upper_key.data()) : ih_->lower_bound(upper_key.data());
    }
    scan_ = std::make_unique<IxScan>(ih_, lower, upper, bpm_, std::move(index_latch_guard_));
}

void IndexCursor::open_equal_range(const std::vector<char>& key) {
    auto [lo, hi] = ih_->equal_range(key.data());
    scan_ = std::make_unique<IxScan>(ih_, lo, hi, bpm_, std::move(index_latch_guard_));
}

void IndexCursor::open_full_scan(int col_tot_len) {
    std::vector<char> min_key(col_tot_len, 0);
    std::vector<char> max_key(col_tot_len, static_cast<char>(0xFF));
    Iid lower = ih_->lower_bound(min_key.data());
    Iid upper = ih_->upper_bound(max_key.data());
    scan_ = std::make_unique<IxScan>(ih_, lower, upper, bpm_, std::move(index_latch_guard_));
}

void IndexCursor::open_range_no_lock(const Iid& lower, const Iid& upper) {
    // skip scan 多范围模式：latch 由本 cursor 持有，子范围扫描不再获取锁
    scan_ = std::make_unique<IxScan>(ih_, lower, upper, bpm_, false);
}

void IndexCursor::next() {
    scan_->next();
}

bool IndexCursor::is_end() const {
    return scan_ == nullptr || scan_->is_end();
}

Rid IndexCursor::rid() const {
    return scan_->rid();
}

const char* IndexCursor::key() const {
    return scan_->key();
}

std::unique_ptr<RmRecord> IndexCursor::get_visible_record(rmdb::statement::StatementContext* context) {
    return GetVisibleRecord(fh_, scan_->rid(), context);
}

TupleMeta IndexCursor::get_tuple_meta(const Rid& rid) const {
    return fh_->get_tuple_meta(rid);
}

bool IndexCursor::is_record(const Rid& rid) const {
    return fh_->is_record(rid);
}

std::unique_ptr<RmRecord> IndexCursor::get_record(const Rid& rid, rmdb::statement::StatementContext* context) const {
    return fh_->get_record(rid, context);
}

Iid IndexCursor::lower_bound(const char* key) const {
    return ih_->lower_bound(key);
}

Iid IndexCursor::upper_bound(const char* key) const {
    return ih_->upper_bound(key);
}

std::optional<std::vector<char>> IndexCursor::probe_first_key(const Iid& cursor, const Iid& end, int key_len) {
    IxScan probe(ih_, cursor, end, bpm_, false);
    if (probe.is_end()) {
        return std::nullopt;
    }
    return std::vector<char>(probe.key(), probe.key() + key_len);
}

void IndexCursor::acquire_shared_lock() {
    index_latch_guard_ = ih_->lock_shared();
}

} // namespace rmdb::access
