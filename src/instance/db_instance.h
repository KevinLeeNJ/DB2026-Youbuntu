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

#include "analyze/analyze.h"
#include "execution/execution_manager.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "record/rm_manager.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

namespace rmdb::instance {

/**
 * Owns the runtime components of one RMDB database instance.
 *
 * This class is the composition root only: it establishes construction and
 * destruction order, but does not implement query, transaction, or storage
 * behavior. Callers should pass narrow component references to lower layers
 * instead of passing DBInstance itself.
 */
class DBInstance {
public:
    DBInstance() {
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ =
            std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(), ix_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get(), sm_manager_.get());
        planner_ = std::make_unique<Planner>(sm_manager_.get());
        optimizer_ = std::make_unique<Optimizer>(sm_manager_.get(), planner_.get());
        ql_manager_ = std::make_unique<QlManager>(sm_manager_.get(), txn_manager_.get(), planner_.get());
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        recovery_ = std::make_unique<RecoveryManager>(disk_manager_.get(), buffer_pool_manager_.get(), sm_manager_.get(),
                                                      log_manager_.get());
        portal_ = std::make_unique<Portal>(sm_manager_.get());
        analyze_ = std::make_unique<Analyze>(sm_manager_.get());
    }

    ~DBInstance() = default;

    DBInstance(const DBInstance&) = delete;
    DBInstance& operator=(const DBInstance&) = delete;
    DBInstance(DBInstance&&) = delete;
    DBInstance& operator=(DBInstance&&) = delete;

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

    QlManager& query_manager() {
        return *ql_manager_;
    }

    LogManager& log_manager() {
        return *log_manager_;
    }

    RecoveryManager& recovery_manager() {
        return *recovery_;
    }

    Portal& portal() {
        return *portal_;
    }

    Analyze& analyzer() {
        return *analyze_;
    }

private:
    // Declaration order is construction order; destruction happens in reverse.
    // Keep dependencies below the components they depend on.
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Planner> planner_;
    std::unique_ptr<Optimizer> optimizer_;
    std::unique_ptr<QlManager> ql_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<RecoveryManager> recovery_;
    std::unique_ptr<Portal> portal_;
    std::unique_ptr<Analyze> analyze_;
};

} // namespace rmdb::instance
