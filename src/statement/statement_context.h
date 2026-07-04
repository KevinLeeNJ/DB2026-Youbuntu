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

#include "diagnostics/trace.h"
#include "recovery/log_manager.h"
#include "transaction/concurrency/lock_manager.h"
#include "transaction/transaction.h"
#include "transaction/txn_defs.h"

class TransactionManager;

namespace statement {

/// 一条 SQL 语句执行期间的内核资源引用集合。
/// 不拥有任何对象，生命周期短于 server::Session。
/// Phase 1 仅定义，被执行器采用自 Phase 4 起（见 migration-ledger）。
struct StatementContext {
    Transaction* txn{nullptr};
    LockManager* lock_mgr{nullptr};
    LogManager* log_mgr{nullptr};
    TransactionManager* txn_mgr{nullptr};
    IsolationLevel isolation_level{DEFAULT_ISOLATION_LEVEL};
    bool enable_ssi_read_tracking{false};
    diagnostics::TraceHook trace_hook{nullptr};
};

} // namespace statement
