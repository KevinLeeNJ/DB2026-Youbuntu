/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You may use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/config.h"

namespace rmdb::server {

/// 语句输出缓冲区：承载 data_send/offset/ellipsis 三个输出字段。
/// 由 server::Session 拥有，传给 RecordPrinter / show / desc / explain 等
/// 仅做结果格式化的路径。内核执行路径只持有 StatementContext，不再接触输出缓冲。
struct OutputSink {
    char* data_send{nullptr};
    int* offset{nullptr};
    bool ellipsis{false};
};

} // namespace rmdb::server

namespace rmdb {
using server::OutputSink;
} // namespace rmdb
