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

#include <string>

#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "recovery/log_manager.h"

class TransactionManager;

// used for data_send
static int const_offset = -1;

class Context {
public:
    Context(LockManager* lock_mgr, LogManager* log_mgr, Transaction* txn, char* data_send = nullptr,
            int* offset = &const_offset, TransactionManager* txn_mgr = nullptr)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), txn_mgr_(txn_mgr), data_send_(data_send), offset_(offset),
          isolation_level_(DEFAULT_ISOLATION_LEVEL) {
        ellipsis_ = false;
    }

    // TransactionManager *txn_mgr_;
    LockManager* lock_mgr_;
    LogManager* log_mgr_;
    Transaction* txn_;
    TransactionManager* txn_mgr_;
    char* data_send_;
    int* offset_;
    bool ellipsis_;
    IsolationLevel isolation_level_;
    bool enable_ssi_read_tracking_{false};
    bool has_statement_template_identity_{false};
    uint64_t statement_shape_high_{0};
    uint64_t statement_shape_low_{0};
    uint32_t statement_shape_size_{0};
    uint64_t statement_template_generation_{0};
    uint64_t planner_generation_{0};
    uint32_t jit_predicate_ordinal_{0};
};
