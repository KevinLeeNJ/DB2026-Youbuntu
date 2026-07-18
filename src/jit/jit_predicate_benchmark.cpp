/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of the Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

#include <x86intrin.h>

#include "execution/executor_abstract.h"
#include "jit/jit_ir.h"
#include "jit/jit_types.h"

namespace {

class PredicateOracle final : public AbstractExecutor {
public:
    explicit PredicateOracle(std::vector<ColMeta> columns) : columns_(std::move(columns)) {}

    bool matches(const Condition& condition, const RmRecord& record) {
        return compare(condition, record);
    }

    Rid& rid() override {
        return rid_;
    }

    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    ColMeta get_col_offset(const TabCol& target) override {
        return *get_col(columns_, target);
    }

private:
    std::vector<ColMeta> columns_;
    Rid rid_{};
};

uint64_t read_cycles() {
    unsigned int auxiliary = 0;
    return __rdtscp(&auxiliary);
}

} // namespace

int main() {
    constexpr int rounds = 3;
    constexpr int iterations = 1000000;
    ColMeta column{"t", "v", TYPE_INT, static_cast<int>(sizeof(int)), 0, false};
    Condition condition;
    condition.lhs_col = {"t", "v"};
    condition.op = OP_GE;
    condition.is_rhs_val = true;
    condition.rhs_val.set_int(500000);
    auto program = jit::build_predicate_program(T_SeqScan, {condition}, {sizeof(int), {column}}, std::nullopt, 1);
    if (!program) {
        return 1;
    }
    jit::JitRuntime runtime;
    auto compiled = runtime.compile_predicate(*program.program);
    if (!compiled) {
        return 1;
    }
    PredicateOracle oracle({column});
    RmRecord record(sizeof(double));
    std::array<uint64_t, rounds> interpreted{};
    std::array<uint64_t, rounds> generated{};
    volatile uint64_t sink = 0;
    for (int round = 0; round < rounds; ++round) {
        uint64_t start = read_cycles();
        for (int value = 0; value < iterations; ++value) {
            std::memcpy(record.data, &value, sizeof(value));
            sink += oracle.matches(condition, record);
        }
        interpreted[round] = read_cycles() - start;

        start = read_cycles();
        for (int value = 0; value < iterations; ++value) {
            jit::JitCallFrame frame{reinterpret_cast<const char*>(&value),
                                    sizeof(value),
                                    nullptr,
                                    0,
                                    program.params.values.data(),
                                    static_cast<uint32_t>(program.params.values.size()),
                                    false};
            sink += compiled.code.invoke_predicate(&frame) == jit::JitStatus::OK && frame.match;
        }
        generated[round] = read_cycles() - start;
    }
    std::sort(interpreted.begin(), interpreted.end());
    std::sort(generated.begin(), generated.end());
    const double reduction = 100.0 * (1.0 - static_cast<double>(generated[1]) / static_cast<double>(interpreted[1]));
    std::cout << "{\"rounds\":3,\"iterations\":" << iterations << ",\"interpreter_cycles\":[" << interpreted[0] << ','
              << interpreted[1] << ',' << interpreted[2] << "],\"jit_cycles\":[" << generated[0] << ',' << generated[1]
              << ',' << generated[2] << "],\"median_reduction_pct\":" << reduction << ",\"sink\":" << sink << "}\n";
    return sink == 0 ? 2 : 0;
}
