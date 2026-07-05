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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include "access/recovery_access.h"
#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "system/schema_manager.h"

namespace rmdb::recovery {
class RedoLogsInPage {
public:
    RedoLogsInPage() {
        table_file_ = nullptr;
    }
    RmFileHandle* table_file_;
    std::vector<lsn_t> redo_logs_; // 在该page上需要redo的操作的lsn
};

class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SchemaManager* schema_manager,
                    LogManager* log_manager = nullptr, rmdb::access::RecoveryAccess* recovery_access = nullptr) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        schema_manager_ = schema_manager;
        log_manager_ = log_manager;
        // 调用方未提供 RecoveryAccess 时，内部默认持有一个（保持向后兼容）。
        if (recovery_access != nullptr) {
            recovery_access_ = recovery_access;
        } else {
            owned_recovery_access_ = std::make_unique<rmdb::access::RecoveryAccess>(schema_manager);
            recovery_access_ = owned_recovery_access_.get();
        }
    }

    void analyze();
    void redo();
    void undo();

private:
    void redo_insert(const InsertLogRecord& log);
    void redo_delete(const DeleteLogRecord& log);
    void redo_update(const UpdateLogRecord& log);
    void undo_insert(const InsertLogRecord& log);
    void undo_delete(const DeleteLogRecord& log);
    void undo_update(const UpdateLogRecord& log);

    std::unordered_map<txn_id_t, lsn_t> active_txn_last_lsn_;
    std::unordered_set<txn_id_t> committed_txns_;
    std::unordered_map<lsn_t, std::unique_ptr<LogRecord>> log_records_;
    std::vector<lsn_t> log_order_;
    lsn_t max_lsn_{INVALID_LSN}; // analyze 扫描到的最大 lsn，用于 recovery 后推进 global_lsn
    int64_t checkpoint_offset_{0};

    LogBuffer buffer_;                                                    // 读入日志
    DiskManager* disk_manager_;                                           // 用来读写文件
    BufferPoolManager* buffer_pool_manager_;                              // 对页面进行读写
    SchemaManager* schema_manager_;                                       // 访问数据库元数据
    LogManager* log_manager_;                                             // recovery 完成后截断日志
    std::unique_ptr<rmdb::access::RecoveryAccess> owned_recovery_access_; // 默认持有的桥接
    rmdb::access::RecoveryAccess* recovery_access_;                       // 存储访问桥接
};

} // namespace rmdb::recovery

namespace rmdb {
using recovery::RecoveryManager;
using recovery::RedoLogsInPage;
} // namespace rmdb
