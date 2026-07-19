/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "jit/jit_aggregate.h"

#include <algorithm>
#include <cstring>

#include "common/config.h"

namespace jit {
namespace {

bool numeric(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

} // namespace

AggregateKernel::AggregateKernel(std::vector<AggregateDescriptor> descriptors)
    : descriptors_(std::move(descriptors)), slots_(descriptors_.size()) {
    for (const auto& descriptor : descriptors_) {
        if (descriptor.op != AggregateOp::COUNT && (!numeric(descriptor.input_type) || descriptor.length == 0)) {
            return;
        }
        if (descriptor.op == AggregateOp::COUNT && !descriptor.star && !numeric(descriptor.input_type)) {
            return;
        }
        const uint64_t width = descriptor.input_type == TYPE_INT ? sizeof(int) : sizeof(double);
        if (descriptor.op != AggregateOp::COUNT && descriptor.length != width) {
            return;
        }
    }
    valid_ = true;
}

JitStatus AggregateKernel::update(const char* tuple, uint32_t tuple_len) {
    if (!valid_ || tuple == nullptr) {
        return JitStatus::INVALID_INPUT;
    }
    for (size_t index = 0; index < descriptors_.size(); ++index) {
        const auto& descriptor = descriptors_[index];
        auto& slot = slots_[index];
        if (descriptor.op != AggregateOp::COUNT &&
            static_cast<uint64_t>(descriptor.offset) + descriptor.length > tuple_len) {
            return JitStatus::INVALID_INPUT;
        }
        double value = 0.0;
        if (descriptor.op != AggregateOp::COUNT || !descriptor.star) {
            if (descriptor.input_type == TYPE_INT) {
                int integer = 0;
                std::memcpy(&integer, tuple + descriptor.offset, sizeof(integer));
                value = static_cast<double>(integer);
            } else if (descriptor.input_type == TYPE_FLOAT) {
                std::memcpy(&value, tuple + descriptor.offset, sizeof(value));
            }
        }
        switch (descriptor.op) {
        case AggregateOp::COUNT:
            ++slot.count;
            break;
        case AggregateOp::SUM:
            slot.sum += value;
            break;
        case AggregateOp::AVG:
            slot.sum += value;
            ++slot.count;
            break;
        case AggregateOp::MIN:
            if (!slot.has_value || value < slot.value) {
                slot.value = value;
                slot.has_value = true;
            }
            break;
        case AggregateOp::MAX:
            if (!slot.has_value || value > slot.value) {
                slot.value = value;
                slot.has_value = true;
            }
            break;
        }
    }
    return JitStatus::OK;
}

void AggregateKernel::reset() {
    std::fill(slots_.begin(), slots_.end(), AggregateSlot{});
}

bool aggregate_jit_enabled() {
    // The generic descriptor loop is retained for force-mode correctness tests only;
    // its measured dispatch overhead is higher than the existing scalar transition.
    return rmdb_config::jit_mode == rmdb_config::JitMode::FORCE;
}

} // namespace jit
