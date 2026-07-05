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
#include <string>

#include "record/rm_file_handle.h"
#include "recovery/log_manager.h"
#include "system/schema_manager.h"

namespace rmdb::access {

/// Recovery 路径的存储访问桥接。RecoveryManager 不再直接调 fh->insert_record /
/// set_tuple_meta，统一经此桥接。详见 write-protocol-contract.md 第 7 节：
/// redo/undo 只执行 heap/index mutation + TupleMeta 重置，不加锁、不写 WAL。
class RecoveryAccess {
public:
    explicit RecoveryAccess(SchemaManager* schema_mgr);

    // ---- redo：物理写入 + 设置 TupleMeta（committed, writer=log_tid）----
    void redo_insert(const InsertLogRecord& log);
    void redo_delete(const DeleteLogRecord& log);
    void redo_update(const UpdateLogRecord& log);

    // ---- undo：幂等守卫 + 物理回滚 + 重置 TupleMeta ----
    void undo_insert(const InsertLogRecord& log);
    void undo_delete(const DeleteLogRecord& log);
    void undo_update(const UpdateLogRecord& log);

    // ---- recovery 结束后的清理 ----
    void reset_all_tuple_meta();
    void rebuild_indexes();
    void flush_all_table_and_index_pages();

    // ---- 幂等守卫辅助 ----
    bool record_exists(const std::string& table_name, const Rid& rid) const;
    std::unique_ptr<RmRecord> get_record_if_exists(const std::string& table_name, const Rid& rid) const;
    bool record_equals(const std::string& table_name, const Rid& rid, const RmRecord& expected) const;

private:
    void reset_tuple_meta(const std::string& table_name, const Rid& rid);

    SchemaManager* schema_mgr_;
};

} // namespace rmdb::access
