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

#include <map>
#include <mutex>

#include "transaction/transaction.h"

/**
 * @brief 追踪所有的读时间戳
 *
 * 用于 MVCC/RC 版本链的垃圾回收：水位线是当前所有活跃事务读时间戳的最小值，
 * 只有 commit_ts 严格小于水位线的已提交事务，其 undo log 才不会被任何活跃
 * 事务的版本链遍历访问到，因而可安全回收。
 */
class Watermark {
public:
    explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

    void AddTxn(timestamp_t read_ts);

    void RemoveTxn(timestamp_t read_ts);

    /** Atomically replace one active transaction's read timestamp.
     *
     * READ COMMITTED advances its statement snapshot while remaining active.
     * Removing and re-adding it in separate critical sections incorrectly
     * exposes an empty reader set to concurrent garbage collection. */
    void UpdateTxnReadTs(timestamp_t old_read_ts, timestamp_t new_read_ts);

    /** 调用者应在从水印中移除事务之前更新提交时间戳，以便我们能够正确跟踪水印。 */
    void UpdateCommitTs(timestamp_t commit_ts);

    timestamp_t GetWatermark();

    mutable timestamp_t commit_ts_;

    timestamp_t watermark_;

    std::map<timestamp_t, int> current_reads_;

private:
    std::mutex latch_;
};
