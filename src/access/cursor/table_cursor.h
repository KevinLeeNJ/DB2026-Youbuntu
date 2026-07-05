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

#include <memory>

#include "access/cursor/scan_cursor.h"
#include "record/rm_defs.h"
#include "record/rm_file_handle.h"
#include "record/rm_scan.h"
#include "storage/buffer_pool_manager.h"

namespace rmdb::access {

/// 封装 RmScan + heap 访问。
/// 持有 RmFileHandle* 和 RmScan，对外不暴露这些存储细节。
class TableCursor : public ScanCursor {
public:
    TableCursor(RmFileHandle* fh, BufferPoolManager* bpm);
    ~TableCursor() override;

    TableCursor(const TableCursor&) = delete;
    TableCursor& operator=(const TableCursor&) = delete;

    void open();
    void next() override;
    bool is_end() const override;
    Rid rid() const override;

    std::unique_ptr<RmRecord> get_visible_record(rmdb::statement::StatementContext* context) override;
    TupleMeta get_tuple_meta(const Rid& rid) const override;
    bool is_record(const Rid& rid) const override;
    std::unique_ptr<RmRecord> get_record(const Rid& rid, rmdb::statement::StatementContext* context) const override;
    RmRecordWithMeta get_record_with_meta(const Rid& rid, rmdb::statement::StatementContext* context) const;

    int record_size() const;

private:
    RmFileHandle* fh_;
    BufferPoolManager* bpm_;
    std::unique_ptr<RmScan> scan_;
};

} // namespace rmdb::access
