/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "compiled/parameter_frame.h"
#include "compiled/program_verifier.h"
#include "jit/native_abi.h"

namespace {

compiled::CompiledProgram make_point_key_program(std::uint32_t key_capacity) {
    compiled::TupleLayout layout;
    layout.byte_size = sizeof(std::int32_t);
    layout.columns.push_back({compiled::ValueType::INT32, 0, static_cast<std::uint32_t>(sizeof(std::int32_t))});
    std::vector<compiled::RegisterDesc> registers = {
        {compiled::ValueType::TUPLE, 0, 0},
        {compiled::ValueType::POINT_KEY, compiled::kNoOperand, key_capacity},
    };
    std::vector<compiled::Instruction> instructions = {
        {compiled::Opcode::MAKE_POINT_KEY, 1, 0, compiled::kNoOperand, 0},
        {compiled::Opcode::HALT},
    };
    return compiled::CompiledProgram(compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION,
                                     compiled::ProgramKind::POINT_SELECT, 1, {}, std::move(registers),
                                     {std::move(layout)}, std::move(instructions));
}

} // namespace

TEST(NativeFrameTest, PointKeyUsesCompiledIndexWidth) {
    constexpr std::uint32_t key_width = 12;
    auto program = make_point_key_program(key_width);
    ASSERT_TRUE(compiled::VerifyProgram(program));
    std::string error;
    auto parameters = compiled::ParameterFrame::Bind({}, {}, &error);
    ASSERT_TRUE(parameters.has_value()) << error;
    compiled::ProgramRuntime runtime;

    jit::native::ExecutionFrame* frame = jit::native::CreateFrame(&program, &*parameters, &runtime);

    ASSERT_NE(frame, nullptr);
    EXPECT_EQ(frame->registers[1].capacity, key_width);
    jit::native::DestroyFrame(frame);
}

TEST(NativeFrameTest, PointKeyRequiresAValidBoundedWidth) {
    EXPECT_FALSE(compiled::VerifyProgram(make_point_key_program(0)));
    EXPECT_FALSE(compiled::VerifyProgram(make_point_key_program(compiled::MAX_PROGRAM_VALUE_BYTES + 1)));
}
