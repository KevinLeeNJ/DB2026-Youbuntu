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
#include <optional>
#include <vector>

#include "access/cursor/scan_cursor.h"
#include "index/ix_defs.h"
#include "index/ix_index_handle.h"
#include "index/ix_scan.h"
#include "record/rm_file_handle.h"
#include "storage/buffer_pool_manager.h"

namespace rmdb::access {

/// 封装 IxScan + heap 回表 + 索引范围查询构造（Phase 6）。
/// 持有 IxIndexHandle* 和 RmFileHandle*，对外不暴露这些存储细节。
class IndexCursor : public ScanCursor {
public:
    IndexCursor(IxIndexHandle* ih, RmFileHandle* fh, BufferPoolManager* bpm);
    ~IndexCursor() override;

    IndexCursor(const IndexCursor&) = delete;
    IndexCursor& operator=(const IndexCursor&) = delete;

    // --- 范围打开（替代执行器中 ih->lower_bound/upper_bound/equal_range 调用）---
    /// 通用范围扫描。lower_exclusive=true 表示下界开区间，upper_inclusive=false 表示上界开区间。
    /// 首次调用会移交 acquire_shared_lock() 获取的 latch；后续若需多范围扫描，用 open_range_no_lock。
    void open_range(const std::vector<char>& lower_key, const std::vector<char>& upper_key, bool lower_exclusive,
                    bool upper_inclusive);
    /// 等值范围扫描（lower_key == upper_key）。
    void open_equal_range(const std::vector<char>& key);
    /// 全索引扫描（lower_bound(min_key) ~ upper_bound(max_key)）。
    void open_full_scan(int col_tot_len);
    /// 多范围扫描专用：不移动 latch（skip scan 持有 latch 后逐范围打开）。
    void open_range_no_lock(const Iid& lower, const Iid& upper);

    // --- 扫描迭代 ---
    void next() override;
    bool is_end() const override;
    Rid rid() const override;
    const char* key() const;

    // --- heap 回表 ---
    std::unique_ptr<RmRecord> get_visible_record(rmdb::Context* context) override;
    TupleMeta get_tuple_meta(const Rid& rid) const override;
    bool is_record(const Rid& rid) const override;
    std::unique_ptr<RmRecord> get_record(const Rid& rid, rmdb::Context* context) const override;

    // --- 索引级查询（供 IndexSkipScan 构造子范围用）---
    /// 返回 lower_bound(key) 的 Iid（不移动当前 scan_）。
    Iid lower_bound(const char* key) const;
    /// 返回 upper_bound(key) 的 Iid。
    Iid upper_bound(const char* key) const;
    /// 在 [cursor, end) 上做临时探针扫描，返回首个 key 的拷贝（用于 skip scan 的 prefix 探测）。
    /// 若范围为空返回 nullopt。
    std::optional<std::vector<char>> probe_first_key(const Iid& cursor, const Iid& end, int key_len);

    /// 持有索引共享锁（生命周期与 cursor 绑定）。
    void acquire_shared_lock();

private:
    IxIndexHandle* ih_;
    RmFileHandle* fh_;
    BufferPoolManager* bpm_;
    IxIndexHandle::SharedIndexLatch index_latch_guard_;
    std::unique_ptr<IxScan> scan_;
};

} // namespace rmdb::access
