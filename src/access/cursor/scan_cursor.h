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

#include "common/common.h"
#include "statement/statement_context.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

namespace rmdb::access {

/// 扫描游标抽象基类。
/// TableCursor / IndexCursor 共同继承，让执行器以统一接口持有 scan_。
/// 不暴露 RmScan/IxScan/RmFileHandle/IxIndexHandle，execution/ 经此门面访问数据。
class ScanCursor {
public:
    virtual ~ScanCursor() = default;

    virtual void next() = 0;
    virtual bool is_end() const = 0;
    virtual Rid rid() const = 0;

    /// 读取当前 rid 对应的 MVCC 可见记录（不可见返回 nullptr）。
    virtual std::unique_ptr<RmRecord> get_visible_record(rmdb::statement::StatementContext* context) = 0;

    /// 读取指定 rid 的 TupleMeta（SSI 追踪用）。
    virtual TupleMeta get_tuple_meta(const Rid& rid) const = 0;

    /// 判断指定 rid 的 slot 是否存在记录（SSI 用）。
    virtual bool is_record(const Rid& rid) const = 0;

    /// 读取指定 rid 的物理记录（不经 MVCC 过滤）。
    virtual std::unique_ptr<RmRecord> get_record(const Rid& rid, rmdb::statement::StatementContext* context) const = 0;
};

} // namespace rmdb::access
