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

// Phase 6: MVCC 可见性 / SSI 冲突检测工具已迁移至 access/mvcc_access.h，
// 供 access/cursor 与 execution/ 共享，打破 cursor ↔ execution 循环依赖。
// 此头文件保留 include 转发，便于现有执行器逐步迁移。
#include "access/mvcc_access.h"
