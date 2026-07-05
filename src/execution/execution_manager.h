/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You may use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

// Phase 7: QlManager 已删除，命令分发由 statement::StatementRunner 接管。
// 此头文件保留为执行器依赖的聚合包含点（record/schema/plan/executor_abstract 等），
// 便于现有执行器逐步迁移到更精确的包含。
#include "execution_defs.h"
#include "record/rm.h"
#include "system/sm_meta.h"
#include "system/schema_manager.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"
#include "access/load_data_service.h"
