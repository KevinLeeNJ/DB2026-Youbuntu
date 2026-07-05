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

namespace rmdb::pager {

/// WAL-before-page-write 保证的最小接口。
/// BPM eviction 通过此接口触发 log flush，不直接依赖 LogManager 或 Pager，
/// 从而避免 BPM ↔ Pager 的循环头文件依赖。
class IWalGuard {
public:
    virtual ~IWalGuard() = default;
    /// 在将脏页写盘前调用，保证对应 WAL 记录已持久化。
    virtual void flush_before_write() noexcept = 0;
};

} // namespace rmdb::pager
