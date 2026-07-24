/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <vector>

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

// Borrowed views used by the opt-in v2 ABI. The pointed-to storage is owned by
// the caller and must remain alive for the duration of the callback.
struct NativeKeyView {
    const uint8_t* data;
    uint32_t size;
};

struct NativeTupleRef {
    const uint8_t* data;
    uint32_t size;
    uint64_t rid;
};

struct NativeMutableTuple {
    uint8_t* data;
    uint32_t size;
    uint32_t capacity;
};

static_assert(std::is_standard_layout_v<NativeKeyView> && std::is_trivially_copyable_v<NativeKeyView>);
static_assert(std::is_standard_layout_v<NativeTupleRef> && std::is_trivially_copyable_v<NativeTupleRef>);
static_assert(std::is_standard_layout_v<NativeMutableTuple> && std::is_trivially_copyable_v<NativeMutableTuple>);

constexpr uint32_t kNativeRuntimeV2AbiVersion = 1;

using NativeMakePointKeyFn = compiled::ExecStatus (*)(void* context, uint32_t index_id, const NativeSlot* tuple,
                                                      NativeSlot* key) noexcept;
using NativePointLookupFn = compiled::ExecStatus (*)(void* context, const NativeSlot* key, NativeSlot* row,
                                                     NativeSlot* tuple, uint32_t tuple_size) noexcept;
using NativePrepareUpdateFn = compiled::ExecStatus (*)(void* context, const NativeSlot* row, NativeSlot* tuple,
                                                       NativeSlot* prepared, uint32_t tuple_size) noexcept;
using NativeCommitUpdateFn = compiled::ExecStatus (*)(void* context, const NativeSlot* prepared,
                                                      const NativeSlot* tuple) noexcept;
using NativeDeleteRowFn = compiled::ExecStatus (*)(void* context, const NativeSlot* row) noexcept;
using NativeInsertRowFn = compiled::ExecStatus (*)(void* context, const NativeSlot* tuple, NativeSlot* row) noexcept;
using NativeEmitRowFn = compiled::ExecStatus (*)(void* context, const NativeSlot* tuple) noexcept;

// Versioned POD callback table. NativeSlot pointers are borrowed for the
// callback duration; callbacks must not retain them or throw exceptions.
struct NativeRuntimeV2 {
    uint32_t abi_version;
    uint32_t struct_size;
    void* context;
    NativeMakePointKeyFn make_point_key;
    NativePointLookupFn point_lookup;
    NativePrepareUpdateFn prepare_update;
    NativeCommitUpdateFn commit_update;
    NativeDeleteRowFn delete_row;
    NativeInsertRowFn insert_row;
    NativeEmitRowFn emit_row;
};

// A view is borrowed by PushRuntimeV2; neither the view nor its table is
// copied or owned by the ABI layer.
struct NativeRuntimeV2View {
    const NativeRuntimeV2* table;
    uint32_t abi_version;
    uint32_t view_size;
};

// Caller-owned binding node. Push/Pop are thread-local and allocation-free,
// which permits nested opt-in execution without changing ProgramRuntime.
struct NativeRuntimeV2Binding {
    compiled::ProgramRuntime* runtime;
    const NativeRuntimeV2View* view;
    NativeRuntimeV2Binding* previous;
};

static_assert(std::is_standard_layout_v<NativeRuntimeV2> && std::is_trivially_copyable_v<NativeRuntimeV2>);
static_assert(std::is_standard_layout_v<NativeRuntimeV2View> && std::is_trivially_copyable_v<NativeRuntimeV2View>);
static_assert(std::is_standard_layout_v<NativeRuntimeV2Binding> &&
              std::is_trivially_copyable_v<NativeRuntimeV2Binding>);

// Immutable register/storage geometry for one compiled program shape. The
// vectors are owned by the frame owner, while NativeSlot::data points into the
// single arena described by offsets/capacities.
struct FrameLayout {
    uint32_t register_count{0};
    uint32_t storage_size{0};
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> capacities;
    std::vector<compiled::ValueType> types;

    bool Matches(const compiled::CompiledProgram& program) const noexcept;
};

struct ExecutionFrame {
    NativeSlot* registers{nullptr};
    void* owner{nullptr};
};

extern "C" ExecutionFrame* CreateFrame(const compiled::CompiledProgram* program,
                                       const compiled::ParameterFrame* parameters,
                                       compiled::ProgramRuntime* runtime) noexcept;
extern "C" void DestroyFrame(ExecutionFrame* frame) noexcept;
// The returned layout is valid only while frame is alive. It is intended for
// diagnostics/tests and does not change the generated program ABI.
extern "C" const FrameLayout* FrameLayoutData(const ExecutionFrame* frame) noexcept;
extern "C" const compiled::ParameterSlot* ParameterData(const compiled::ParameterFrame* parameters) noexcept;
extern "C" void PushRuntimeV2(NativeRuntimeV2Binding* binding, compiled::ProgramRuntime* runtime,
                              const NativeRuntimeV2View* view) noexcept;
extern "C" void PopRuntimeV2(NativeRuntimeV2Binding* binding) noexcept;
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
