/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "access/cursor/table_cursor.h"

#include "access/mvcc_access.h"

namespace rmdb::access {

TableCursor::TableCursor(RmFileHandle* fh, BufferPoolManager* bpm) : fh_(fh), bpm_(bpm) {}

TableCursor::~TableCursor() = default;

void TableCursor::open() {
    scan_ = std::make_unique<RmScan>(fh_);
}

void TableCursor::next() {
    scan_->next();
}

bool TableCursor::is_end() const {
    return scan_->is_end();
}

Rid TableCursor::rid() const {
    return scan_->rid();
}

std::unique_ptr<RmRecord> TableCursor::get_visible_record(rmdb::Context* context) {
    return GetVisibleRecord(fh_, scan_->rid(), context);
}

TupleMeta TableCursor::get_tuple_meta(const Rid& rid) const {
    return fh_->get_tuple_meta(rid);
}

bool TableCursor::is_record(const Rid& rid) const {
    return fh_->is_record(rid);
}

std::unique_ptr<RmRecord> TableCursor::get_record(const Rid& rid, rmdb::Context* context) const {
    return fh_->get_record(rid, context);
}

RmRecordWithMeta TableCursor::get_record_with_meta(const Rid& rid, rmdb::Context* context) const {
    return fh_->get_record_with_meta(rid, context);
}

int TableCursor::record_size() const {
    return fh_->get_file_hdr().record_size;
}

} // namespace rmdb::access
