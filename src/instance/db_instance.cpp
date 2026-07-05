/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "instance/db_instance.h"

namespace rmdb::instance {

DBInstance::DBInstance() {
    // 构造顺序与成员声明一致：底层存储先行，上层 manager 依赖前者
    disk_manager_ = std::make_unique<DiskManager>();
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
    ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
    sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                              ix_manager_.get());
    schema_manager_ = std::make_unique<SchemaManager>(sm_manager_.get());
    lock_manager_ = std::make_unique<LockManager>();
    txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), schema_manager_.get());
    planner_ = std::make_unique<Planner>(schema_manager_.get());
    optimizer_ = std::make_unique<Optimizer>(schema_manager_.get(), planner_.get());
    log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
    recovery_access_ = std::make_unique<rmdb::access::RecoveryAccess>(schema_manager_.get());
    recovery_ = std::make_unique<RecoveryManager>(disk_manager_.get(), buffer_pool_manager_.get(),
                                                  schema_manager_.get(), log_manager_.get(), recovery_access_.get());
    write_service_ = std::make_unique<rmdb::access::TableWriteService>(schema_manager_.get(), lock_manager_.get(),
                                                                       log_manager_.get(), txn_manager_.get());
    tuple_meta_writer_ = std::make_unique<rmdb::access::TupleMetaWriter>(schema_manager_.get());
    load_data_service_ = std::make_unique<rmdb::access::LoadDataService>(schema_manager_.get(), write_service_.get());
    ql_manager_ = std::make_unique<QlManager>(schema_manager_.get(), txn_manager_.get(), planner_.get(),
                                              load_data_service_.get());
    portal_ = std::make_unique<Portal>(schema_manager_.get(), write_service_.get());
    analyze_ = std::make_unique<Analyze>(schema_manager_.get());
}

DBInstance::~DBInstance() = default;

void DBInstance::open_database(const std::string& db_name) {
    if (!sm_manager_->is_dir(db_name)) {
        sm_manager_->create_db(db_name);
    }
    sm_manager_->open_db(db_name);
    log_manager_->initialize_from_existing_log();
    // WAL 注入 BPM：Phase 5 由 Pager 接管后删除此调用
    buffer_pool_manager_->set_log_manager(log_manager_.get());
}

void DBInstance::run_recovery() {
    recovery_->analyze();
    recovery_->redo();
    recovery_->undo();
}

void DBInstance::close_database() {
    sm_manager_->close_db();
}

} // namespace rmdb::instance
