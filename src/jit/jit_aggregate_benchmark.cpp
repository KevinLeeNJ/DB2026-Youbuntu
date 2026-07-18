/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO ANY WARRANTY OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>

#include <x86intrin.h>

#include "jit/jit_aggregate.h"

namespace {

uint64_t read_cycles() {
    unsigned int auxiliary = 0;
    return __rdtscp(&auxiliary);
}

} // namespace

int main() {
    constexpr int rounds = 3;
    constexpr int iterations = 1000000;
    const std::vector<jit::AggregateDescriptor> descriptors{
        {jit::AggregateOp::COUNT, TYPE_INT, 0, sizeof(int), true},
        {jit::AggregateOp::SUM, TYPE_INT, 0, sizeof(int), false},
        {jit::AggregateOp::AVG, TYPE_INT, 0, sizeof(int), false},
        {jit::AggregateOp::MIN, TYPE_INT, 0, sizeof(int), false},
        {jit::AggregateOp::MAX, TYPE_INT, 0, sizeof(int), false},
    };
    std::array<uint64_t, rounds> scalar{};
    std::array<uint64_t, rounds> kernel_cycles{};
    volatile double sink = 0.0;
    for (int round = 0; round < rounds; ++round) {
        std::array<int, 1> tuple{};
        int64_t count = 0;
        double sum = 0.0;
        double min_value = 0.0;
        double max_value = 0.0;
        uint64_t start = read_cycles();
        for (int value = 0; value < iterations; ++value) {
            tuple[0] = value;
            const double current = value;
            ++count;
            sum += current;
            if (value == 0 || current < min_value)
                min_value = current;
            if (value == 0 || current > max_value)
                max_value = current;
        }
        sink += static_cast<double>(count) + sum + min_value + max_value;
        scalar[round] = read_cycles() - start;

        jit::AggregateKernel kernel(descriptors);
        start = read_cycles();
        for (int value = 0; value < iterations; ++value) {
            tuple[0] = value;
            if (kernel.update(reinterpret_cast<const char*>(tuple.data()), sizeof(tuple)) != jit::JitStatus::OK) {
                return 1;
            }
        }
        const auto& slots = kernel.slots();
        sink += static_cast<double>(slots[0].count) + slots[1].sum + slots[2].sum + slots[3].value + slots[4].value;
        kernel_cycles[round] = read_cycles() - start;
    }
    std::sort(scalar.begin(), scalar.end());
    std::sort(kernel_cycles.begin(), kernel_cycles.end());
    const double reduction = 100.0 * (1.0 - static_cast<double>(kernel_cycles[1]) / static_cast<double>(scalar[1]));
    std::cout << "{\"rounds\":3,\"iterations\":" << iterations << ",\"scalar_cycles\":[" << scalar[0] << ','
              << scalar[1] << ',' << scalar[2] << "],\"kernel_cycles\":[" << kernel_cycles[0] << ',' << kernel_cycles[1]
              << ',' << kernel_cycles[2] << "],\"median_reduction_pct\":" << reduction << ",\"sink\":" << sink << "}\n";
    return sink == 0.0 ? 2 : 0;
}
