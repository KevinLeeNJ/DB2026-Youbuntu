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

#include "pager/wal_guard.h"
#include "storage/buffer_pool_manager.h"

namespace rmdb::recovery {
class LogManager;
}

namespace rmdb::pager {

/// WAL-before-page-write 规则的唯一执行者（Phase 5 目标）。
///
/// 持有 BufferPoolManager* 和 LogManager*，对外提供 flush_page / flush_all_pages /
/// discard_pages 接口。每次写盘前均先执行全量 log flush（Route A 保守策略）。
///
/// 同时实现 IWalGuard，供 BPM 的 eviction 路径调用，避免循环头文件依赖：
///   BPM ──> IWalGuard（最小接口）<── Pager ──> BPM
class Pager : public IWalGuard {
public:
    /// log_manager 可为 nullptr（测试环境无 WAL 时跳过 log flush）。
    Pager(rmdb::BufferPoolManager* bpm, rmdb::recovery::LogManager* log_manager)
        : bpm_(bpm), log_manager_(log_manager) {}

    ~Pager() override = default;

    Pager(const Pager&) = delete;
    Pager& operator=(const Pager&) = delete;

    /// 将指定页写回磁盘，写前先 flush WAL。
    bool flush_page(rmdb::storage::PageId page_id);

    /// 将指定 fd 的所有脏页写回磁盘，写前先 flush WAL。
    void flush_all_pages(int fd);

    /// 丢弃 BPM 中指定 fd 的所有帧（不写盘）。
    void discard_pages(int fd);

    // --- IWalGuard ---
    /// BPM eviction 路径调用：写脏页前 flush WAL。
    void flush_before_write() noexcept override;

private:
    rmdb::BufferPoolManager* bpm_;
    rmdb::recovery::LogManager* log_manager_; // 可为 nullptr

    /// 调用 log_manager_ 全量 flush（若为 nullptr 则跳过）。
    void flush_wal_if_needed();
};

} // namespace rmdb::pager

namespace rmdb {
using pager::Pager;
} // namespace rmdb
