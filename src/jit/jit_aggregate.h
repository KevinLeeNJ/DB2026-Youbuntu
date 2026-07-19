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
#include <cstdint>
#include <vector>

#include "execution/execution_defs.h"
#include "jit/jit_types.h"

namespace jit {

enum class AggregateOp : uint8_t { COUNT, SUM, MIN, MAX, AVG };

struct AggregateDescriptor {
    AggregateOp op{AggregateOp::COUNT};
    ColType input_type{TYPE_INT};
    uint32_t offset{0};
    uint32_t length{0};
    bool star{false};
};

struct AggregateSlot {
    int64_t count{0};
    double sum{0.0};
    double value{0.0};
    bool has_value{false};
};

class AggregateKernel {
public:
    explicit AggregateKernel(std::vector<AggregateDescriptor> descriptors);

    bool valid() const {
        return valid_;
    }
    JitStatus update(const char* tuple, uint32_t tuple_len);
    void reset();
    const std::vector<AggregateSlot>& slots() const {
        return slots_;
    }

private:
    std::vector<AggregateDescriptor> descriptors_;
    std::vector<AggregateSlot> slots_;
    bool valid_{false};
};

bool aggregate_jit_enabled();

} // namespace jit
