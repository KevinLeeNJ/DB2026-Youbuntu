/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <map>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

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
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
    }

    void analyze();
    void redo();
    void undo();

private:
    bool record_exists(const std::string& table_name, const Rid& rid) const;
    std::unique_ptr<RmRecord> get_record_if_exists(const std::string& table_name, const Rid& rid) const;
    void reset_tuple_meta(const std::string& table_name, const Rid& rid);

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
    int checkpoint_offset_{0};

    LogBuffer buffer_;                       // 读入日志
    DiskManager* disk_manager_;              // 用来读写文件
    BufferPoolManager* buffer_pool_manager_; // 对页面进行读写
    SmManager* sm_manager_;                  // 访问数据库元数据
};
