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
    // log_manager_ 和 pager_ 先于 rm/ix/sm_manager_（后者依赖 pager_）
    disk_manager_ = std::make_unique<DiskManager>();
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
    log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
    pager_ = std::make_unique<rmdb::pager::Pager>(buffer_pool_manager_.get(), log_manager_.get());
    rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
    ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get(), pager_.get());
    // 构造顺序：schema_manager 先于依赖它的 analyzer/planner。
    schema_manager_ = std::make_unique<SchemaManager>(disk_manager_.get(), buffer_pool_manager_.get(),
                                                      rm_manager_.get(), ix_manager_.get(), pager_.get());
    lock_manager_ = std::make_unique<LockManager>();
    txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), schema_manager_.get());
    planner_ = std::make_unique<Planner>(&schema_manager_->catalog());
    optimizer_ = std::make_unique<Optimizer>(planner_.get());
    recovery_access_ = std::make_unique<rmdb::access::RecoveryAccess>(schema_manager_.get());
    recovery_ = std::make_unique<RecoveryManager>(disk_manager_.get(), buffer_pool_manager_.get(),
                                                  schema_manager_.get(), log_manager_.get(), recovery_access_.get());
    write_service_ = std::make_unique<rmdb::access::TableWriteService>(schema_manager_.get(), lock_manager_.get(),
                                                                       log_manager_.get(), txn_manager_.get());
    tuple_meta_writer_ = std::make_unique<rmdb::access::TupleMetaWriter>(schema_manager_.get());
    load_data_service_ = std::make_unique<rmdb::access::LoadDataService>(schema_manager_.get(), write_service_.get());
    statement_runner_ = std::make_unique<StatementRunner>(schema_manager_.get(), write_service_.get(), planner_.get(),
                                                          load_data_service_.get(), txn_manager_.get());
    analyze_ = std::make_unique<Analyze>(&schema_manager_->catalog());
}

DBInstance::~DBInstance() = default;

void DBInstance::open_database(const std::string& db_name) {
    if (!schema_manager_->is_dir(db_name)) {
        schema_manager_->create_db(db_name);
    }
    schema_manager_->open_db(db_name);
    log_manager_->initialize_from_existing_log();
    // 向 BPM 注入 Pager 作为 WAL guard：eviction 路径写盘前触发 log flush
    buffer_pool_manager_->set_wal_guard(pager_.get());
}

void DBInstance::run_recovery() {
    recovery_->analyze();
    recovery_->redo();
    recovery_->undo();
}

void DBInstance::close_database() {
    schema_manager_->close_db();
}

} // namespace rmdb::instance
