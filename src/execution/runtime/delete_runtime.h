/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#pragma once

#include "execution/row_mutation.h"

class DeleteRuntime {
public:
    static bool DeleteOne(const Rid& rid, const DeleteRuntimeInfo& info, Context* context);
};
