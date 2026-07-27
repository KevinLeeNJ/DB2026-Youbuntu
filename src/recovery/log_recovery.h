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
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager,
                    LogManager* log_manager = nullptr) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        log_manager_ = log_manager;
    }

    void analyze();
    void redo();
    void undo();

private:
    bool record_exists(const std::string& table_name, const Rid& rid) const;
    // 当前 rid 处的记录是否与 expected 内容一致（rid 不存在视为不等）。
    // 用于 undo 幂等守卫：仅当页面仍反映该 loser 事务自身的效果时才回滚，
    // 避免跨轮 recovery 重复 undo 覆盖同 RID 上的后续 committed 数据。
    bool record_equals(const std::string& table_name, const Rid& rid, const RmRecord& expected) const;
    std::unique_ptr<RmRecord> get_record_if_exists(const std::string& table_name, const Rid& rid) const;
    void reset_tuple_meta(const std::string& table_name, const Rid& rid);

    void redo_insert(const InsertLogRecord& log);
    void redo_delete(const DeleteLogRecord& log);
    void redo_update(const UpdateLogRecord& log);
    void undo_insert(const InsertLogRecord& log);
    void undo_delete(const DeleteLogRecord& log);
    void undo_update(const UpdateLogRecord& log);
    void repair_touched_file_headers();
    void reset_touched_tuple_meta();
    void repair_touched_indexes();
    void reset_wal_if_needed();

    std::unordered_map<txn_id_t, lsn_t> active_txn_last_lsn_;
    std::unordered_set<txn_id_t> committed_txns_;
    std::unordered_map<lsn_t, std::unique_ptr<LogRecord>> log_records_;
    std::vector<lsn_t> log_order_;
    std::unordered_map<std::string, std::vector<Rid>> touched_rids_;
    std::unordered_set<std::string> touched_tables_;
    bool has_dml_records_{false};
    lsn_t max_lsn_{INVALID_LSN}; // analyze 扫描到的最大 lsn，用于 recovery 后推进 global_lsn
    int64_t checkpoint_offset_{0};

    LogBuffer buffer_;                       // 读入日志
    DiskManager* disk_manager_;              // 用来读写文件
    BufferPoolManager* buffer_pool_manager_; // 对页面进行读写
    SmManager* sm_manager_;                  // 访问数据库元数据
    LogManager* log_manager_;                // recovery 完成后截断日志
};
