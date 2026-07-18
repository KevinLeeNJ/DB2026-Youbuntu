/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#include "delete_runtime.h"

bool DeleteRuntime::DeleteOne(const Rid& rid, const DeleteRuntimeInfo& info, Context* context) {
    auto visible_record = GetVisibleRecord(info.fh, rid, context);
    if (visible_record == nullptr) {
        return false;
    }
    return RowMutationEngine::DeleteOne(rid, *visible_record, info, context);
}
