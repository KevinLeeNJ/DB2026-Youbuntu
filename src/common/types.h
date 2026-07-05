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

#include "common/common.h"
#include "common/config.h"
#include "defs.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

namespace rmdb {

// Transitional aliases for high-traffic value and metadata types.
using common::Condition;
using common::lsn_t;
using common::timestamp_t;
using common::txn_id_t;
using common::Value;
using record::Rid;
using record::RmRecord;
using system::ColMeta;
using system::TabMeta;

} // namespace rmdb
