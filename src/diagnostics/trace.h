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

#include <functional>
#include <string>
#include <string_view>

namespace rmdb::diagnostics {

/// 事件类型，仅枚举重构期间有实际消费者的类别。
enum class TraceEventType {
    WriteBegin, // 写操作开始（payload: 表名）
    WriteEnd,   // 写操作结束（payload: 表名）
    TxnAbort,   // 事务回滚（payload: abort 原因）
};

/// 最小追踪事件。默认无消费者，no-op。
struct TraceEvent {
    TraceEventType type;
    std::string payload;
};

/// TraceHook 默认 nullptr；设置后由调用方在关键路径 emit 事件。
/// 不建框架：无 sink、无 scope、无 SQL 命令。
using TraceHook = std::function<void(const TraceEvent&)>;

} // namespace rmdb::diagnostics
