/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include "compiled/compiled_program.h"
#include "compiled/parameter_frame.h"

#include <type_traits>

namespace jit::native {

struct NativeSlot {
    compiled::ValueType type{compiled::ValueType::INT32};
    uint8_t initialized{0};
    uint8_t bool_value{0};
    uint8_t reserved{0};
    int32_t int_value{0};
    double float_value{0.0};
    uint8_t* data{nullptr};
    uint32_t length{0};
    uint32_t capacity{0};
    uint64_t opaque{0};
};
static_assert(std::is_standard_layout_v<NativeSlot> && std::is_trivially_copyable_v<NativeSlot>);

struct ExecutionFrame {
    NativeSlot* registers;
    void* owner;
};

extern "C" ExecutionFrame* CreateFrame(const compiled::CompiledProgram* program,
                                       const compiled::ParameterFrame* parameters,
                                       compiled::ProgramRuntime* runtime) noexcept;
extern "C" void DestroyFrame(ExecutionFrame* frame) noexcept;
extern "C" const compiled::ParameterSlot* ParameterData(const compiled::ParameterFrame* parameters) noexcept;
extern "C" compiled::ExecStatus LoadBytesParameter(NativeSlot* destination, const compiled::ParameterSlot* source,
                                                   compiled::ProgramRuntime* runtime) noexcept;
extern "C" compiled::ExecStatus LoadBytesColumn(NativeSlot* destination, const uint8_t* source, uint32_t width,
                                                compiled::ProgramRuntime* runtime) noexcept;
extern "C" compiled::ExecStatus StoreBytesColumn(uint8_t* destination, uint32_t width, const NativeSlot* source,
                                                 compiled::ProgramRuntime* runtime) noexcept;
extern "C" uint8_t CompareBytes(const NativeSlot* lhs, const NativeSlot* rhs, compiled::CompareOp op) noexcept;
extern "C" compiled::ExecStatus Fail(compiled::ProgramRuntime* runtime, const char* message) noexcept;
extern "C" compiled::ExecStatus MakePointKey(compiled::ProgramRuntime* runtime, uint32_t index_id,
                                             const NativeSlot* tuple, NativeSlot* key) noexcept;
extern "C" compiled::ExecStatus PointLookup(compiled::ProgramRuntime* runtime, const NativeSlot* key, NativeSlot* row,
                                            NativeSlot* tuple, uint32_t tuple_size) noexcept;
extern "C" compiled::ExecStatus PrepareUpdate(compiled::ProgramRuntime* runtime, const NativeSlot* row,
                                              NativeSlot* tuple, NativeSlot* prepared, uint32_t tuple_size) noexcept;
extern "C" compiled::ExecStatus CommitUpdate(compiled::ProgramRuntime* runtime, const NativeSlot* prepared,
                                             const NativeSlot* tuple) noexcept;
extern "C" compiled::ExecStatus DeleteRow(compiled::ProgramRuntime* runtime, const NativeSlot* row) noexcept;
extern "C" compiled::ExecStatus InsertRow(compiled::ProgramRuntime* runtime, const NativeSlot* tuple,
                                          NativeSlot* row) noexcept;
extern "C" compiled::ExecStatus EmitRow(compiled::ProgramRuntime* runtime, const NativeSlot* tuple) noexcept;

} // namespace jit::native
