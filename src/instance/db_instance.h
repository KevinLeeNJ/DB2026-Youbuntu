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

#include "access/load_data_service.h"
#include "access/recovery_access.h"
#include "access/table_write_service.h"
#include "access/tuple_meta_writer.h"
#include "analyze/analyze.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "record/rm_manager.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/schema_manager.h"
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

namespace instance {

/// 一个数据库实例的生命周期主人。持有所有 manager 的 unique_ptr，
/// 构造顺序即依赖顺序，析构按成员声明逆序自动释放。
/// 取代 rmdb.cpp 中文件级全局 unique_ptr（Phase 1 目标）。
class DBInstance {
public:
    DBInstance();
    ~DBInstance();

    DBInstance(const DBInstance&) = delete;
    DBInstance& operator=(const DBInstance&) = delete;

    /// 打开（必要时创建）数据库目录，初始化 log，注入 BPM。
    /// 保留 BPM::set_log_manager 调用，Phase 5 由 Pager 接管后删除。
    void open_database(const std::string& db_name);

    /// 运行 ARIES 三阶段恢复。须在 open_database 之后调用。
    void run_recovery();

    /// 关闭数据库（flush 句柄、写回元数据）。
    void close_database();

    // 访问器（返回引用，不转让所有权）
    DiskManager& disk_manager() {
        return *disk_manager_;
    }
    BufferPoolManager& buffer_pool_manager() {
        return *buffer_pool_manager_;
    }
    RmManager& rm_manager() {
        return *rm_manager_;
    }
    IxManager& ix_manager() {
        return *ix_manager_;
    }
    SmManager& sm_manager() {
        return *sm_manager_;
    }
    SchemaManager& schema_manager() {
        return *schema_manager_;
    }
    LockManager& lock_manager() {
        return *lock_manager_;
    }
    TransactionManager& transaction_manager() {
        return *txn_manager_;
    }
    Planner& planner() {
        return *planner_;
    }
    Optimizer& optimizer() {
        return *optimizer_;
    }
    QlManager& ql_manager() {
        return *ql_manager_;
    }
    LogManager& log_manager() {
        return *log_manager_;
    }
    RecoveryManager& recovery() {
        return *recovery_;
    }
    Portal& portal() {
        return *portal_;
    }
    Analyze& analyze() {
        return *analyze_;
    }
    dbaccess::TableWriteService& write_service() {
        return *write_service_;
    }
    dbaccess::RecoveryAccess& recovery_access() {
        return *recovery_access_;
    }
    dbaccess::TupleMetaWriter& tuple_meta_writer() {
        return *tuple_meta_writer_;
    }
    dbaccess::LoadDataService& load_data_service() {
        return *load_data_service_;
    }

private:
    // 声明顺序即构造顺序，逆序析构。切勿随意调整顺序。
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<SchemaManager> schema_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Optimizer> optimizer_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<dbaccess::RecoveryAccess> recovery_access_;
    std::unique_ptr<RecoveryManager> recovery_;
    std::unique_ptr<dbaccess::TableWriteService> write_service_;
    std::unique_ptr<dbaccess::TupleMetaWriter> tuple_meta_writer_;
    std::unique_ptr<dbaccess::LoadDataService> load_data_service_;
    std::unique_ptr<QlManager> ql_manager_;
    std::unique_ptr<Portal> portal_;
    std::unique_ptr<Analyze> analyze_;
};

} // namespace instance
