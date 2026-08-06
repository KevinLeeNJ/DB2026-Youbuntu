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

#include <optional>
#include <vector>

#include "defs.h"
#include "errors.h"

// A point DML target is intentionally distinct from the legacy vector-RID
// constructor so braced vector arguments in existing callers remain unambiguous.
struct PointMutationWorkspaceAlias {
    int index_fd = -1;
    std::vector<char> key;
};

struct PointMutationTarget {
    std::optional<Rid> rid;
    std::optional<PointMutationWorkspaceAlias> workspace_alias;
};
