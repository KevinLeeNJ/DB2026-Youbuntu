/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
*/

#pragma once

#include "execution/row_mutation.h"

struct InsertRuntimeInfo {
    SmManager* sm_manager;
    const std::string* tab_name;
    const TabMeta* tab;
    RmFileHandle* fh;
    const std::vector<RowMutationIndex>* indexes;
};

class InsertRuntime {
public:
    static Rid InsertOne(RmRecord& record, const InsertRuntimeInfo& info, Context* context);
};
