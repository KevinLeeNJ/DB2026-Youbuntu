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

#include "execution/executor_abstract.h"
#include "optimizer/plan.h"
#include "execution/executor_delete.h"
#include "execution/executor_insert.h"
#include "execution/executor_update.h"

inline std::unique_ptr<RmRecord> CopyCurrentTuple(AbstractExecutor& executor) {
    const TupleView tuple = executor.current();
    if (!tuple) {
        return nullptr;
    }
    auto copy = std::make_unique<RmRecord>(static_cast<int>(tuple.size));
    std::memcpy(copy->data, tuple.data, tuple.size);
    return copy;
}

inline std::unique_ptr<RmRecord> CopyCurrentTuple(InsertExecutor& executor) {
    executor.Execute();
    return nullptr;
}

inline std::unique_ptr<RmRecord> CopyCurrentTuple(UpdateExecutor& executor) {
    executor.Execute();
    return nullptr;
}

inline std::unique_ptr<RmRecord> CopyCurrentTuple(DeleteExecutor& executor) {
    executor.Execute();
    return nullptr;
}
