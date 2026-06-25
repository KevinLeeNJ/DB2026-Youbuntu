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
          isolation_level_(IsolationLevel::SERIALIZABLE) {
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
    bool output_file_enabled_{true};
};
