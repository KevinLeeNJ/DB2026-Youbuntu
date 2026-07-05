/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You may use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

namespace rmdb::statement {

/// 语句大类。StatementRunner 按 StatementKind 分派命令处理路径，
/// 替代旧 Portal 的 portalTag 内部分发。
enum class StatementKind { Select, Insert, Update, Delete, DDL, Transaction, Utility, Load, ExplainAnalyze };

} // namespace rmdb::statement
