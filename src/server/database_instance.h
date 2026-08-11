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

#include <cstddef>
#include <string>

#include "analyze/analyze.h"
#include "execution/execution_manager.h"
#include "index/ix_manager.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "record/rm_manager.h"
#include "recovery/log_manager.h"
#include "recovery/log_recovery.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction_manager.h"

inline constexpr size_t SERVER_BUFFER_POOL_PAGES = (size_t{3} << 30) / PAGE_SIZE;

// The concrete process composition root. Declaration order is dependency
// order; C++ destroys it in reverse, so TransactionManager joins GC before
// the catalog, buffer pool, and disk objects it uses are destroyed.
struct DatabaseInstance {
    DiskManager disk_manager;
    BufferPoolManager buffer_pool_manager{SERVER_BUFFER_POOL_PAGES, &disk_manager};
    RmManager rm_manager{&disk_manager, &buffer_pool_manager};
    IxManager ix_manager{&disk_manager, &buffer_pool_manager};
    SmManager sm_manager{&disk_manager, &buffer_pool_manager, &rm_manager, &ix_manager};
    LockManager lock_manager;
    LogManager log_manager{&disk_manager, DurabilityMode::STRICT};
    TransactionManager txn_manager{&lock_manager, &sm_manager};
    Planner planner{&sm_manager};
    Optimizer optimizer{&sm_manager, &planner};
    QlManager ql_manager{&sm_manager, &txn_manager};
    Analyze analyze{&sm_manager};
    RecoveryManager recovery{&disk_manager, &buffer_pool_manager, &sm_manager, &log_manager};

    void open_and_recover(const std::string& db_name);
    ~DatabaseInstance() {
        try {
            close();
        } catch (...) {
        }
    }
    void close();

private:
    bool open_{false};
};
