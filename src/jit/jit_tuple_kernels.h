/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstddef>
#include <vector>

#include "execution/execution_defs.h"
#include "jit/jit_types.h"
#include "system/sm.h"

namespace jit {

bool tuple_jit_enabled();

struct CopySpan {
    size_t source_offset{0};
    size_t destination_offset{0};
    size_t length{0};
};

class ProjectionKernel {
public:
    ProjectionKernel(const std::vector<ColMeta>& output_columns, const std::vector<size_t>& selected_columns,
                     const std::vector<ColMeta>& input_columns);

    bool valid() const {
        return valid_;
    }
    void project(const char* input, char* output) const;

private:
    bool valid_{false};
    std::vector<CopySpan> spans_;
};

class UpdateKernel {
public:
    UpdateKernel(const TabMeta& table, const std::vector<SetClause>& clauses);

    bool valid() const {
        return valid_;
    }
    // Performs pure tuple computation only. Callers retain all storage and transaction side effects.
    JitStatus update(char* destination, const char* source) const;

private:
    struct Assignment {
        ColMeta target;
        ColMeta source;
        SetClause clause;
    };

    bool valid_{false};
    std::vector<Assignment> assignments_;
};

} // namespace jit
