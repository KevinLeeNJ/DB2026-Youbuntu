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

#include <cstring>

#include "common/config.h"
#include "output_sink.h"
#include "transaction/txn_defs.h"

namespace rmdb::server {

/// 单个客户端连接的 per-session 状态。
/// 持有当前事务 id、隔离级别、输出缓冲区；不拥有任何内核对象。
/// 生命周期与连接线程绑定。Phase 1 从 rmdb.cpp 的 client_handler 局部变量迁入。
class Session {
public:
    Session() {
        reset_buffer();
        txn_id_ = INVALID_TXN_ID;
        isolation_level_ = DEFAULT_ISOLATION_LEVEL;
    }

    /// 每条语句开始前清空输出缓冲区。
    void reset_buffer() {
        memset(data_send_, '\0', BUFFER_LENGTH);
        offset_ = 0;
        output_sink_.ellipsis = false;
    }

    txn_id_t txn_id() const {
        return txn_id_;
    }
    void set_txn_id(txn_id_t id) {
        txn_id_ = id;
    }
    txn_id_t* txn_id_ptr() {
        return &txn_id_;
    }

    IsolationLevel isolation_level() const {
        return isolation_level_;
    }
    void set_isolation_level(IsolationLevel lvl) {
        isolation_level_ = lvl;
    }

    char* data_send() {
        return data_send_;
    }
    int* offset_ptr() {
        return &offset_;
    }
    int offset() const {
        return offset_;
    }

    /// 输出缓冲视图，供 RecordPrinter / show / desc 等格式化路径使用。
    OutputSink& output_sink() {
        return output_sink_;
    }

private:
    char data_send_[BUFFER_LENGTH];
    int offset_;
    txn_id_t txn_id_;
    IsolationLevel isolation_level_;
    OutputSink output_sink_{data_send_, &offset_, false};
};

} // namespace rmdb::server
