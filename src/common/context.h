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
#include <vector>

#include "common/common.h"
#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "recovery/log_manager.h"

class TransactionManager;
struct ColMeta;

class QueryResultSink {
public:
    virtual ~QueryResultSink() = default;
    virtual void begin_query(const std::vector<ColMeta>& columns, const std::vector<std::string>& names) = 0;
    virtual void append_row(const std::vector<ColMeta>& columns, const char* data, std::size_t size) = 0;
};

struct ExecutionOutput {
    char* data_send = nullptr;
    int* offset = nullptr;
    bool ellipsis = false;
    QueryResultSink* result_sink = nullptr;
    bool* output_file_enabled = nullptr;
};

class Context {
public:
    Context(LockManager* lock_mgr, LogManager* log_mgr, Transaction* txn, TransactionManager* txn_mgr = nullptr)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), txn_mgr_(txn_mgr),
          isolation_level_(DEFAULT_ISOLATION_LEVEL) {}

    // TransactionManager *txn_mgr_;
    LockManager* lock_mgr_;
    LogManager* log_mgr_;
    Transaction* txn_;
    TransactionManager* txn_mgr_;
    IsolationLevel isolation_level_;
    bool enable_ssi_read_tracking_{false};
};
