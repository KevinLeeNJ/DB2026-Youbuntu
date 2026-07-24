/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <array>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "compiled/bytecode_interpreter.h"
#include "compiled/program_verifier.h"
#include "jit/native_abi.h"
#include "jit/jit_types.h"

namespace {

using compiled::CompareOp;
using compiled::CompiledProgram;
using compiled::ExecStatus;
using compiled::Instruction;
using compiled::Opcode;
using compiled::ParameterDesc;
using compiled::ParameterFrame;
using compiled::ParameterValue;
using compiled::ProgramKind;
using compiled::RegisterDesc;
using compiled::RuntimeValue;
using compiled::TupleLayout;
using compiled::ValueType;

Instruction Op(Opcode opcode, uint32_t dst = compiled::kNoOperand, uint32_t lhs = compiled::kNoOperand,
               uint32_t rhs = compiled::kNoOperand, uint32_t aux = compiled::kNoOperand,
               CompareOp comparison = CompareOp::EQ) {
    return {opcode, dst, lhs, rhs, aux, comparison};
}

CompiledProgram Program(std::vector<ParameterDesc> parameters, std::vector<RegisterDesc> registers,
                        std::vector<TupleLayout> layouts, std::vector<Instruction> instructions,
                        ProgramKind kind = ProgramKind::POINT_SELECT) {
    return {compiled::COMPILED_IR_VERSION,
            compiled::COMPILED_ABI_VERSION,
            kind,
            1,
            std::move(parameters),
            std::move(registers),
            std::move(layouts),
            std::move(instructions)};
}

ParameterFrame Bind(const CompiledProgram& program, const std::vector<ParameterValue>& values) {
    std::string error;
    auto frame = ParameterFrame::Bind(program.parameters(), values, &error);
    EXPECT_TRUE(frame.has_value()) << error;
    return std::move(*frame);
}

class DifferentialRuntime : public compiled::ProgramRuntime {
public:
    ExecStatus MakePointKey(uint32_t index, const RuntimeValue& tuple, RuntimeValue* key) noexcept override {
        calls.push_back("key");
        if (sticky_key_error) {
            SetError(ExecStatus::ERROR, "sticky key error");
            return ExecStatus::OK;
        }
        key->type = wrong_key_type ? ValueType::BYTES : ValueType::POINT_KEY;
        key->bytes.assign(reinterpret_cast<const char*>(tuple.tuple.data()), tuple.tuple.size());
        key->opaque = index;
        key->initialized = initialize_key;
        return key_status;
    }
    ExecStatus PointLookup(const RuntimeValue&, RuntimeValue* row, RuntimeValue* tuple) noexcept override {
        calls.push_back("lookup");
        row->type = ValueType::ROW_HANDLE;
        row->opaque = 19;
        row->initialized = true;
        tuple->type = ValueType::TUPLE;
        tuple->tuple.resize(lookup_tuple_size != 0 ? lookup_tuple_size : short_lookup_tuple ? 3 : 4);
        std::memcpy(tuple->tuple.data(), &lookup_value, tuple->tuple.size());
        tuple->initialized = true;
        return ExecStatus::OK;
    }
    ExecStatus PrepareUpdate(const RuntimeValue& row, RuntimeValue* tuple, RuntimeValue* prepared) noexcept override {
        calls.push_back("prepare");
        seen_opaque = row.opaque;
        tuple->type = ValueType::TUPLE;
        tuple->tuple.resize(short_prepare_tuple ? 3 : 4);
        std::memcpy(tuple->tuple.data(), &refresh_value, tuple->tuple.size());
        tuple->initialized = true;
        prepared->type = ValueType::PREPARED_UPDATE;
        prepared->opaque = 23;
        prepared->initialized = true;
        return ExecStatus::OK;
    }
    ExecStatus CommitUpdate(const RuntimeValue& prepared, const RuntimeValue& tuple) noexcept override {
        calls.push_back("commit");
        seen_opaque = prepared.opaque;
        committed = tuple.tuple;
        return ExecStatus::OK;
    }
    ExecStatus DeleteRow(const RuntimeValue& row) noexcept override {
        calls.push_back("delete");
        seen_opaque = row.opaque;
        return ExecStatus::OK;
    }
    ExecStatus InsertRow(const RuntimeValue& tuple, RuntimeValue* row) noexcept override {
        calls.push_back("insert");
        inserted = tuple.tuple;
        row->type = ValueType::ROW_HANDLE;
        row->opaque = 29;
        row->initialized = true;
        return ExecStatus::OK;
    }
    ExecStatus EmitRow(const RuntimeValue& tuple) noexcept override {
        calls.push_back("emit");
        emitted.push_back(tuple.tuple);
        return ExecStatus::OK;
    }

    int32_t lookup_value{10};
    int32_t refresh_value{40};
    ExecStatus key_status{ExecStatus::OK};
    bool sticky_key_error{false};
    bool initialize_key{true};
    bool wrong_key_type{false};
    size_t lookup_tuple_size{0};
    bool short_lookup_tuple{false};
    bool short_prepare_tuple{false};
    uint64_t seen_opaque{0};
    std::vector<std::string> calls;
    std::vector<uint8_t> committed;
    std::vector<uint8_t> inserted;
    std::vector<std::vector<uint8_t>> emitted;
};

struct NativeV2Context {
    std::array<const char*, 8> calls{};
    size_t call_count{0};
    ExecStatus status{ExecStatus::OK};

    void Record(const char* call) noexcept {
        if (call_count < calls.size())
            calls[call_count++] = call;
    }
};

ExecStatus V2Status(NativeV2Context* context) noexcept {
    return context->status;
}

ExecStatus V2MakePointKey(void* opaque, uint32_t index, const jit::native::NativeSlot* tuple,
                          jit::native::NativeSlot* key) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("key");
    if (V2Status(context) != ExecStatus::OK)
        return context->status;
    key->type = ValueType::POINT_KEY;
    key->opaque = index;
    key->length = tuple->length;
    key->initialized = 1;
    if (key->length != 0)
        std::memcpy(key->data, tuple->data, key->length);
    return ExecStatus::OK;
}

ExecStatus V2PointLookup(void* opaque, const jit::native::NativeSlot*, jit::native::NativeSlot* row,
                         jit::native::NativeSlot* tuple, uint32_t tuple_size) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("lookup");
    if (V2Status(context) != ExecStatus::OK)
        return context->status;
    row->type = ValueType::ROW_HANDLE;
    row->opaque = 19;
    row->initialized = 1;
    tuple->type = ValueType::TUPLE;
    tuple->length = tuple_size;
    tuple->initialized = 1;
    for (uint32_t i = 0; i < tuple_size; ++i)
        tuple->data[i] = static_cast<uint8_t>(i + 1);
    return ExecStatus::OK;
}

ExecStatus V2PrepareUpdate(void* opaque, const jit::native::NativeSlot* row, jit::native::NativeSlot* tuple,
                           jit::native::NativeSlot* prepared, uint32_t tuple_size) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("prepare");
    if (V2Status(context) != ExecStatus::OK)
        return context->status;
    EXPECT_EQ(row->opaque, 19U);
    tuple->type = ValueType::TUPLE;
    tuple->length = tuple_size;
    tuple->initialized = 1;
    for (uint32_t i = 0; i < tuple_size; ++i)
        tuple->data[i] = static_cast<uint8_t>(10 + i);
    prepared->type = ValueType::PREPARED_UPDATE;
    prepared->opaque = 23;
    prepared->initialized = 1;
    return ExecStatus::OK;
}

ExecStatus V2CommitUpdate(void* opaque, const jit::native::NativeSlot* prepared,
                          const jit::native::NativeSlot*) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("commit");
    if (V2Status(context) != ExecStatus::OK)
        return context->status;
    return prepared->opaque == 23 ? ExecStatus::OK : ExecStatus::ERROR;
}

ExecStatus V2DeleteRow(void* opaque, const jit::native::NativeSlot* row) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("delete");
    if (V2Status(context) != ExecStatus::OK)
        return context->status;
    return row->opaque == 19 ? ExecStatus::OK : ExecStatus::ERROR;
}

ExecStatus V2InsertRow(void* opaque, const jit::native::NativeSlot* tuple, jit::native::NativeSlot* row) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("insert");
    if (V2Status(context) != ExecStatus::OK)
        return context->status;
    row->type = ValueType::ROW_HANDLE;
    row->opaque = tuple->length;
    row->initialized = 1;
    return ExecStatus::OK;
}

ExecStatus V2EmitRow(void* opaque, const jit::native::NativeSlot*) noexcept {
    auto* context = static_cast<NativeV2Context*>(opaque);
    context->Record("emit");
    return V2Status(context);
}

jit::native::NativeRuntimeV2 V2Table(NativeV2Context* context) {
    return {jit::native::kNativeRuntimeV2AbiVersion,
            sizeof(jit::native::NativeRuntimeV2),
            context,
            V2MakePointKey,
            V2PointLookup,
            V2PrepareUpdate,
            V2CommitUpdate,
            V2DeleteRow,
            V2InsertRow,
            V2EmitRow};
}

TEST(NativeRuntimeV2Test, OptInCallbacksDispatchWithoutChangingLegacyFallback) {
    DifferentialRuntime runtime;
    NativeV2Context v2_context;
    auto table = V2Table(&v2_context);
    const jit::native::NativeRuntimeV2View view{&table, jit::native::kNativeRuntimeV2AbiVersion,
                                                sizeof(jit::native::NativeRuntimeV2View)};
    jit::native::NativeRuntimeV2Binding binding{};

    std::array<uint8_t, 8> tuple_bytes{1, 2, 3, 4, 0, 0, 0, 0};
    std::array<uint8_t, 8> key_bytes{};
    std::array<uint8_t, 8> output_bytes{};
    jit::native::NativeSlot tuple{ValueType::TUPLE, 1, 0, 0, 0, 0, tuple_bytes.data(), 4, 8, 0};
    jit::native::NativeSlot key{ValueType::POINT_KEY, 0, 0, 0, 0, 0, key_bytes.data(), 0, 8, 0};
    jit::native::NativeSlot row{ValueType::ROW_HANDLE, 0, 0, 0, 0, 0, nullptr, 0, 0, 0};
    jit::native::NativeSlot prepared{ValueType::PREPARED_UPDATE, 0, 0, 0, 0, 0, nullptr, 0, 0, 0};
    jit::native::NativeSlot output{ValueType::ROW_HANDLE, 0, 0, 0, 0, 0, output_bytes.data(), 0, 8, 0};

    jit::native::PushRuntimeV2(&binding, &runtime, &view);
    EXPECT_EQ(jit::native::MakePointKey(&runtime, 7, &tuple, &key), ExecStatus::OK);
    EXPECT_EQ(jit::native::PointLookup(&runtime, &key, &row, &tuple, 4), ExecStatus::OK);
    EXPECT_EQ(jit::native::PrepareUpdate(&runtime, &row, &tuple, &prepared, 4), ExecStatus::OK);
    EXPECT_EQ(jit::native::CommitUpdate(&runtime, &prepared, &tuple), ExecStatus::OK);
    EXPECT_EQ(jit::native::DeleteRow(&runtime, &row), ExecStatus::OK);
    EXPECT_EQ(jit::native::InsertRow(&runtime, &tuple, &output), ExecStatus::OK);
    EXPECT_EQ(jit::native::EmitRow(&runtime, &tuple), ExecStatus::OK);
    jit::native::PopRuntimeV2(&binding);

    ASSERT_EQ(v2_context.call_count, 7U);
    EXPECT_EQ(v2_context.calls[0], std::string("key"));
    EXPECT_EQ(v2_context.calls[1], std::string("lookup"));
    EXPECT_EQ(v2_context.calls[2], std::string("prepare"));
    EXPECT_EQ(v2_context.calls[3], std::string("commit"));
    EXPECT_EQ(v2_context.calls[4], std::string("delete"));
    EXPECT_EQ(v2_context.calls[5], std::string("insert"));
    EXPECT_EQ(v2_context.calls[6], std::string("emit"));

    // Pop restores the legacy RuntimeValue-based path for the same runtime.
    EXPECT_EQ(jit::native::MakePointKey(&runtime, 3, &tuple, &key), ExecStatus::OK);
    EXPECT_EQ(runtime.calls.back(), "key");
}

void ExpectSame(const DifferentialRuntime& interpreted, const DifferentialRuntime& native) {
    EXPECT_EQ(native.calls, interpreted.calls);
    EXPECT_EQ(native.committed, interpreted.committed);
    EXPECT_EQ(native.inserted, interpreted.inserted);
    EXPECT_EQ(native.emitted, interpreted.emitted);
    EXPECT_EQ(native.seen_opaque, interpreted.seen_opaque);
    EXPECT_EQ(native.error_message(), interpreted.error_message());
}

CompiledProgram FrameProgram() {
    TupleLayout layout{8, {{ValueType::INT32, 0, 4}, {ValueType::INT32, 4, 4}}};
    return Program({},
                   {{ValueType::INT32},
                    {ValueType::BYTES, compiled::kNoOperand, 5},
                    {ValueType::TUPLE, 0},
                    {ValueType::POINT_KEY}},
                   {layout}, {Op(Opcode::HALT)});
}

TEST(NativeProgramFrameTest, UsesSingleArenaAndReusesThreadLocalFrame) {
    auto program = FrameProgram();
    auto parameters = Bind(program, {});
    DifferentialRuntime runtime;

    auto* first = jit::native::CreateFrame(&program, &parameters, &runtime);
    ASSERT_NE(first, nullptr);
    const auto* first_layout = jit::native::FrameLayoutData(first);
    ASSERT_NE(first_layout, nullptr);
    ASSERT_EQ(first_layout->register_count, 4U);
    ASSERT_EQ(first_layout->offsets, (std::vector<uint32_t>{0, 0, 5, 13}));
    ASSERT_EQ(first_layout->capacities, (std::vector<uint32_t>{0, 5, 8, compiled::MAX_PROGRAM_VALUE_BYTES}));
    EXPECT_EQ(first_layout->storage_size, 13U + compiled::MAX_PROGRAM_VALUE_BYTES);
    EXPECT_EQ(first_layout->types,
              (std::vector<ValueType>{ValueType::INT32, ValueType::BYTES, ValueType::TUPLE, ValueType::POINT_KEY}));

    auto* first_registers = first->registers;
    EXPECT_EQ(first_registers[0].data, nullptr);
    ASSERT_NE(first_registers[1].data, nullptr);
    ASSERT_NE(first_registers[2].data, nullptr);
    ASSERT_NE(first_registers[3].data, nullptr);
    EXPECT_EQ(first_registers[2].data, first_registers[1].data + first_registers[1].capacity);
    EXPECT_EQ(first_registers[3].data, first_registers[2].data + first_registers[2].capacity);

    // Reuse must reset request-local register state, including the tuple's
    // fixed initialized state and non-tuple registers' cleared state.
    first_registers[1].initialized = 1;
    first_registers[1].length = 4;
    first_registers[2].initialized = 0;
    first_registers[2].length = 0;
    auto* first_data = first_registers[1].data;
    jit::native::DestroyFrame(first);
    EXPECT_EQ(jit::native::FrameLayoutData(first), nullptr);

    auto* second = jit::native::CreateFrame(&program, &parameters, &runtime);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second, first);
    EXPECT_EQ(second->registers, first_registers);
    EXPECT_EQ(second->registers[1].data, first_data);
    EXPECT_EQ(second->registers[1].initialized, 0);
    EXPECT_EQ(second->registers[1].length, 0U);
    EXPECT_EQ(second->registers[2].initialized, 1);
    EXPECT_EQ(second->registers[2].length, second->registers[2].capacity);
    jit::native::DestroyFrame(second);
}

TEST(NativeProgramFrameTest, ActiveFramesAreNotSharedAcrossThreads) {
    auto program = FrameProgram();
    auto parameters = Bind(program, {});
    std::mutex mutex;
    std::condition_variable condition;
    bool ready = false;
    bool release = false;
    jit::native::ExecutionFrame* worker_frame = nullptr;

    std::thread worker([&] {
        DifferentialRuntime runtime;
        auto* frame = jit::native::CreateFrame(&program, &parameters, &runtime);
        {
            std::lock_guard lock(mutex);
            worker_frame = frame;
            ready = true;
        }
        condition.notify_one();
        {
            std::unique_lock lock(mutex);
            condition.wait(lock, [&] { return release; });
        }
        jit::native::DestroyFrame(frame);
    });

    bool worker_ready = false;
    {
        std::unique_lock lock(mutex);
        worker_ready = condition.wait_for(lock, std::chrono::seconds(2), [&] { return ready; });
    }
    EXPECT_TRUE(worker_ready);
    auto* active_worker_frame = worker_frame;
    DifferentialRuntime main_runtime;
    auto* main_frame = jit::native::CreateFrame(&program, &parameters, &main_runtime);
    EXPECT_NE(active_worker_frame, nullptr);
    EXPECT_NE(main_frame, nullptr);
    EXPECT_NE(main_frame, active_worker_frame);
    if (main_frame != nullptr)
        jit::native::DestroyFrame(main_frame);

    {
        std::lock_guard lock(mutex);
        release = true;
    }
    condition.notify_one();
    worker.join();
}

TEST(NativeProgramAbiTest, V2TableAndBorrowedViewsHaveStablePODLayout) {
    static_assert(std::is_standard_layout_v<jit::native::NativeRuntimeV2>);
    static_assert(std::is_trivially_copyable_v<jit::native::NativeRuntimeV2>);
    static_assert(std::is_standard_layout_v<jit::native::NativeRuntimeV2View>);
    static_assert(std::is_trivially_copyable_v<jit::native::NativeRuntimeV2View>);
    static_assert(std::is_standard_layout_v<jit::native::NativeRuntimeV2Binding>);
    static_assert(std::is_trivially_copyable_v<jit::native::NativeRuntimeV2Binding>);

    EXPECT_EQ(sizeof(jit::native::NativeRuntimeV2View),
              sizeof(const jit::native::NativeRuntimeV2*) + 2 * sizeof(uint32_t));
    EXPECT_EQ(offsetof(jit::native::NativeRuntimeV2, abi_version), 0U);
    EXPECT_EQ(offsetof(jit::native::NativeRuntimeV2, struct_size), sizeof(uint32_t));
    EXPECT_EQ(jit::native::kNativeRuntimeV2AbiVersion, 1U);
}

TEST(NativeProgramAbiTest, ExplicitV2BindingUsesCallbacksAndUnboundRuntimeUsesLegacyFallback) {
    DifferentialRuntime runtime;
    NativeV2Context context;
    const auto table = V2Table(&context);
    const jit::native::NativeRuntimeV2View view{&table, jit::native::kNativeRuntimeV2AbiVersion,
                                                sizeof(jit::native::NativeRuntimeV2View)};
    jit::native::NativeRuntimeV2Binding binding{};
    jit::native::PushRuntimeV2(&binding, &runtime, &view);

    std::array<uint8_t, 8> tuple_data{1, 2, 3, 4, 0, 0, 0, 0};
    std::array<uint8_t, 8> key_data{};
    jit::native::NativeSlot tuple{};
    tuple.type = ValueType::TUPLE;
    tuple.initialized = 1;
    tuple.data = tuple_data.data();
    tuple.length = 4;
    tuple.capacity = tuple_data.size();
    jit::native::NativeSlot key{};
    key.type = ValueType::POINT_KEY;
    key.data = key_data.data();
    key.capacity = key_data.size();
    jit::native::NativeSlot row{};
    row.type = ValueType::ROW_HANDLE;
    jit::native::NativeSlot prepared{};
    prepared.type = ValueType::PREPARED_UPDATE;

    EXPECT_EQ(jit::native::MakePointKey(&runtime, 7, &tuple, &key), ExecStatus::OK);
    EXPECT_EQ(key.opaque, 7U);
    EXPECT_EQ(std::memcmp(key.data, tuple.data, tuple.length), 0);
    EXPECT_EQ(jit::native::PointLookup(&runtime, &key, &row, &tuple, 4), ExecStatus::OK);
    EXPECT_EQ(row.opaque, 19U);
    EXPECT_EQ(jit::native::PrepareUpdate(&runtime, &row, &tuple, &prepared, 4), ExecStatus::OK);
    EXPECT_EQ(prepared.opaque, 23U);
    EXPECT_EQ(jit::native::CommitUpdate(&runtime, &prepared, &tuple), ExecStatus::OK);
    EXPECT_EQ(jit::native::DeleteRow(&runtime, &row), ExecStatus::OK);
    EXPECT_EQ(jit::native::InsertRow(&runtime, &tuple, &row), ExecStatus::OK);
    EXPECT_EQ(row.opaque, 4U);
    EXPECT_EQ(jit::native::EmitRow(&runtime, &tuple), ExecStatus::OK);
    EXPECT_TRUE(runtime.calls.empty());
    ASSERT_EQ(context.call_count, 7U);
    EXPECT_EQ(std::vector<const char*>(context.calls.begin(), context.calls.begin() + context.call_count),
              (std::vector<const char*>{"key", "lookup", "prepare", "commit", "delete", "insert", "emit"}));

    runtime.ClearError();
    context.status = ExecStatus::NO_MATCH_RESULT;
    const auto old_key_length = key.length;
    EXPECT_EQ(jit::native::MakePointKey(&runtime, 7, &tuple, &key), ExecStatus::NO_MATCH_RESULT);
    EXPECT_EQ(runtime.last_status(), ExecStatus::NO_MATCH_RESULT);
    EXPECT_EQ(key.length, old_key_length);

    jit::native::PopRuntimeV2(&binding);
    runtime.ClearError();
    context.status = ExecStatus::OK;
    EXPECT_EQ(jit::native::MakePointKey(&runtime, 9, &tuple, &key), ExecStatus::OK);
    ASSERT_FALSE(runtime.calls.empty());
    EXPECT_EQ(runtime.calls.back(), "key");
    EXPECT_EQ(context.call_count, 8U);
}

TEST(NativeProgramTest, ScalarControlFlowBytesAndFixedTupleMatchInterpreter) {
    TupleLayout layout{
        21,
        {{ValueType::INT32, 0, 4}, {ValueType::FLOAT64, 4, 8}, {ValueType::BOOL, 12, 1}, {ValueType::BYTES, 13, 8}}};
    auto program = Program({{ValueType::INT32, 0},
                            {ValueType::INT32, 0},
                            {ValueType::FLOAT64, 0},
                            {ValueType::BOOL, 0},
                            {ValueType::BYTES, 8}},
                           {{ValueType::INT32},
                            {ValueType::INT32},
                            {ValueType::FLOAT64},
                            {ValueType::BOOL},
                            {ValueType::BYTES, compiled::kNoOperand, 8},
                            {ValueType::INT32},
                            {ValueType::BOOL},
                            {ValueType::TUPLE, 0},
                            {ValueType::TUPLE, 0},
                            {ValueType::INT32},
                            {ValueType::FLOAT64},
                            {ValueType::BYTES, compiled::kNoOperand, 8},
                            {ValueType::BOOL}},
                           {layout},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                            Op(Opcode::LOAD_PARAM, 2, compiled::kNoOperand, compiled::kNoOperand, 2),
                            Op(Opcode::LOAD_PARAM, 3, compiled::kNoOperand, compiled::kNoOperand, 3),
                            Op(Opcode::LOAD_PARAM, 4, compiled::kNoOperand, compiled::kNoOperand, 4),
                            Op(Opcode::MUL, 5, 0, 1),
                            Op(Opcode::COMPARE, 6, 0, 1, compiled::kNoOperand, CompareOp::GT),
                            Op(Opcode::JUMP_IF_FALSE, compiled::kNoOperand, 6, compiled::kNoOperand, 13),
                            Op(Opcode::STORE_COLUMN, 7, 5, compiled::kNoOperand, 0),
                            Op(Opcode::STORE_COLUMN, 7, 2, compiled::kNoOperand, 1),
                            Op(Opcode::STORE_COLUMN, 7, 3, compiled::kNoOperand, 2),
                            Op(Opcode::STORE_COLUMN, 7, 4, compiled::kNoOperand, 3),
                            Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 14),
                            Op(Opcode::STORE_COLUMN, 7, 0, compiled::kNoOperand, 0),
                            Op(Opcode::COPY_TUPLE, 8, 7),
                            Op(Opcode::LOAD_COLUMN, 9, 8, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_COLUMN, 10, 8, compiled::kNoOperand, 1),
                            Op(Opcode::LOAD_COLUMN, 11, 8, compiled::kNoOperand, 3),
                            Op(Opcode::COMPARE, 12, 11, 4, compiled::kNoOperand, CompareOp::EQ),
                            Op(Opcode::EMIT_ROW, compiled::kNoOperand, 8),
                            Op(Opcode::HALT)});
    auto frame = Bind(program, {ParameterValue::Int(7), ParameterValue::Int(3), ParameterValue::Float(2.5),
                                ParameterValue::Bool(true), ParameterValue::Bytes("abc")});
    DifferentialRuntime interpreted, native;
    ASSERT_EQ(compiled::Interpret(program, frame, &interpreted), ExecStatus::OK);
    jit::JitRuntime jit_runtime;
    auto compiled = jit_runtime.compile_program(program);
    ASSERT_TRUE(compiled) << compiled.error;
    EXPECT_GT(compiled.code.code_size(), 0U);
    EXPECT_EQ(compiled.code.invoke_program(&native, &frame), ExecStatus::OK);
    ExpectSame(interpreted, native);

    auto empty_frame = Bind(program, {ParameterValue::Int(7), ParameterValue::Int(3), ParameterValue::Float(2.5),
                                      ParameterValue::Bool(true), ParameterValue::Bytes("")});
    DifferentialRuntime empty_interpreted, empty_native;
    ASSERT_EQ(compiled::Interpret(program, empty_frame, &empty_interpreted), ExecStatus::OK);
    EXPECT_EQ(compiled.code.invoke_program(&empty_native, &empty_frame), ExecStatus::OK);
    ExpectSame(empty_interpreted, empty_native);
}

CompiledProgram UpdateProgram() {
    TupleLayout layout{4, {{ValueType::INT32, 0, 4}}};
    return Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                   {{ValueType::INT32},
                    {ValueType::INT32},
                    {ValueType::TUPLE, 0},
                    {ValueType::POINT_KEY},
                    {ValueType::ROW_HANDLE},
                    {ValueType::TUPLE, 0},
                    {ValueType::PREPARED_UPDATE},
                    {ValueType::INT32},
                    {ValueType::INT32},
                    {ValueType::TUPLE, 0}},
                   {layout},
                   {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                    Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                    Op(Opcode::STORE_COLUMN, 2, 0, compiled::kNoOperand, 0),
                    Op(Opcode::MAKE_POINT_KEY, 3, 2, compiled::kNoOperand, 7), Op(Opcode::POINT_LOOKUP, 5, 3, 4),
                    Op(Opcode::PREPARE_UPDATE, 6, 4, 5), Op(Opcode::LOAD_COLUMN, 7, 5, compiled::kNoOperand, 0),
                    Op(Opcode::ADD, 8, 7, 1), Op(Opcode::COPY_TUPLE, 9, 5),
                    Op(Opcode::STORE_COLUMN, 9, 8, compiled::kNoOperand, 0),
                    Op(Opcode::COMMIT_UPDATE, compiled::kNoOperand, 6, 9),
                    Op(Opcode::EMIT_ROW, compiled::kNoOperand, 9), Op(Opcode::HALT)},
                   ProgramKind::POINT_UPDATE);
}

TEST(NativeProgramTest, DatabaseRuntimeBoundaryAndProgramLifetimeMatchInterpreter) {
    auto program = UpdateProgram();
    auto frame = Bind(program, {ParameterValue::Int(1), ParameterValue::Int(2)});
    DifferentialRuntime interpreted, native;
    ASSERT_EQ(compiled::Interpret(program, frame, &interpreted), ExecStatus::OK);
    jit::JitRuntime jit_runtime;
    jit::JitCompileResult result;
    {
        auto temporary = program;
        result = jit_runtime.compile_program(temporary);
    }
    ASSERT_TRUE(result) << result.error;
    EXPECT_EQ(result.code.invoke_program(&native, &frame), ExecStatus::OK);
    ExpectSame(interpreted, native);
    ASSERT_EQ(native.committed.size(), 4U);
    int32_t value = 0;
    std::memcpy(&value, native.committed.data(), 4);
    EXPECT_EQ(value, 42);
}

TEST(NativeProgramTest, InsertDeleteAndFloatingArithmeticMatchInterpreter) {
    TupleLayout layout{8, {{ValueType::FLOAT64, 0, 8}}};
    auto program = Program({{ValueType::FLOAT64, 0}, {ValueType::INT32, 0}},
                           {{ValueType::FLOAT64},
                            {ValueType::INT32},
                            {ValueType::FLOAT64},
                            {ValueType::FLOAT64},
                            {ValueType::FLOAT64},
                            {ValueType::TUPLE, 0},
                            {ValueType::ROW_HANDLE},
                            {ValueType::BOOL}},
                           {layout},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                            Op(Opcode::ADD, 2, 0, 1), Op(Opcode::SUB, 3, 2, 1), Op(Opcode::DIV, 4, 3, 1),
                            Op(Opcode::COMPARE, 7, 4, 0, compiled::kNoOperand, CompareOp::LT),
                            Op(Opcode::STORE_COLUMN, 5, 4, compiled::kNoOperand, 0), Op(Opcode::INSERT_ROW, 6, 5),
                            Op(Opcode::DELETE_ROW, compiled::kNoOperand, 6),
                            Op(Opcode::EMIT_ROW, compiled::kNoOperand, 5), Op(Opcode::HALT)},
                           ProgramKind::POINT_INSERT);
    auto frame = Bind(program, {ParameterValue::Float(9.0), ParameterValue::Int(3)});
    DifferentialRuntime interpreted, native;
    ASSERT_EQ(compiled::Interpret(program, frame, &interpreted), ExecStatus::OK);
    jit::JitRuntime runtime;
    auto code = runtime.compile_program(program);
    ASSERT_TRUE(code) << code.error;
    EXPECT_EQ(code.code.invoke_program(&native, &frame), ExecStatus::OK);
    ExpectSame(interpreted, native);
}

TEST(NativeProgramTest, ErrorAndCompilationFallbackBehaviorMatch) {
    auto divide = Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                          {{ValueType::INT32}, {ValueType::INT32}, {ValueType::INT32}}, {},
                          {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                           Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                           Op(Opcode::DIV, 2, 0, 1), Op(Opcode::HALT)});
    auto frame = Bind(divide, {ParameterValue::Int(8), ParameterValue::Int(0)});
    DifferentialRuntime interpreted, native;
    ASSERT_EQ(compiled::Interpret(divide, frame, &interpreted), ExecStatus::ERROR);
    jit::JitRuntime runtime;
    auto code = runtime.compile_program(divide);
    ASSERT_TRUE(code) << code.error;
    EXPECT_EQ(code.code.invoke_program(&native, &frame), ExecStatus::ERROR);
    EXPECT_EQ(native.error_message(), interpreted.error_message());
    auto valid_frame = Bind(divide, {ParameterValue::Int(8), ParameterValue::Int(2)});
    native.ClearError();
    EXPECT_EQ(code.code.invoke_program(&native, &valid_frame), ExecStatus::OK);
    EXPECT_FALSE(runtime.compile_program(divide, {.force_compile_failure = true}));
    auto malformed = Program(
        {}, {}, {},
        {Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 9), Op(Opcode::HALT)});
    EXPECT_FALSE(runtime.compile_program(malformed));
}

TEST(NativeProgramTest, MalformedHelperOutputsAndInvalidStatusMatchInterpreter) {
    auto program = UpdateProgram();
    auto frame = Bind(program, {ParameterValue::Int(1), ParameterValue::Int(2)});
    jit::JitRuntime runtime;
    auto code = runtime.compile_program(program);
    ASSERT_TRUE(code) << code.error;

    for (int malformed_stage = 0; malformed_stage < 2; ++malformed_stage) {
        DifferentialRuntime interpreted, native;
        interpreted.short_lookup_tuple = native.short_lookup_tuple = malformed_stage == 0;
        interpreted.short_prepare_tuple = native.short_prepare_tuple = malformed_stage == 1;
        const auto interpreted_status = compiled::Interpret(program, frame, &interpreted);
        const auto native_status = code.code.invoke_program(&native, &frame);
        EXPECT_EQ(native_status, interpreted_status);
        EXPECT_EQ(native.error_message(), interpreted.error_message());
        EXPECT_EQ(native.calls, interpreted.calls);
        EXPECT_NE(native.error_message().find("wrong size"), std::string::npos);
    }

    DifferentialRuntime interpreted, native;
    interpreted.key_status = native.key_status = static_cast<ExecStatus>(255);
    const auto interpreted_status = compiled::Interpret(program, frame, &interpreted);
    const auto native_status = code.code.invoke_program(&native, &frame);
    EXPECT_EQ(native_status, ExecStatus::ERROR);
    EXPECT_EQ(native_status, interpreted_status);
    EXPECT_EQ(native.error_message(), interpreted.error_message());
    EXPECT_EQ(native.calls, interpreted.calls);
    EXPECT_NE(native.error_message().find("invalid status"), std::string::npos);
}

TEST(NativeProgramTest, Int32OverflowBoundariesMatchInterpreter) {
    const std::array<std::tuple<Opcode, int32_t, int32_t>, 7> cases{
        std::tuple{Opcode::ADD, std::numeric_limits<int32_t>::max(), int32_t{1}},
        std::tuple{Opcode::ADD, std::numeric_limits<int32_t>::min(), int32_t{-1}},
        std::tuple{Opcode::SUB, std::numeric_limits<int32_t>::max(), int32_t{-1}},
        std::tuple{Opcode::SUB, std::numeric_limits<int32_t>::min(), int32_t{1}},
        std::tuple{Opcode::MUL, std::numeric_limits<int32_t>::max(), int32_t{2}},
        std::tuple{Opcode::MUL, std::numeric_limits<int32_t>::min(), int32_t{2}},
        std::tuple{Opcode::DIV, std::numeric_limits<int32_t>::min(), int32_t{-1}},
    };
    jit::JitRuntime runtime;
    for (const auto& [opcode, lhs, rhs] : cases) {
        auto program = Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                               {{ValueType::INT32}, {ValueType::INT32}, {ValueType::INT32}}, {},
                               {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                                Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                                Op(opcode, 2, 0, 1), Op(Opcode::HALT)});
        auto frame = Bind(program, {ParameterValue::Int(lhs), ParameterValue::Int(rhs)});
        DifferentialRuntime interpreted, native;
        auto code = runtime.compile_program(program);
        ASSERT_TRUE(code) << code.error;
        const auto interpreted_status = compiled::Interpret(program, frame, &interpreted);
        const auto native_status = code.code.invoke_program(&native, &frame);
        EXPECT_EQ(native_status, interpreted_status) << static_cast<int>(opcode);
        EXPECT_EQ(native.error_message(), interpreted.error_message()) << static_cast<int>(opcode);
        EXPECT_EQ(native_status, ExecStatus::ERROR);
        EXPECT_NE(native.error_message().find("overflow"), std::string::npos);
    }
}

TEST(NativeProgramTest, FloatingPointExceptionalCasesMatchInterpreter) {
    auto divide = Program({{ValueType::FLOAT64, 0}, {ValueType::FLOAT64, 0}},
                          {{ValueType::FLOAT64}, {ValueType::FLOAT64}, {ValueType::FLOAT64}}, {},
                          {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                           Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                           Op(Opcode::DIV, 2, 0, 1), Op(Opcode::HALT)});
    jit::JitRuntime runtime;
    auto divide_code = runtime.compile_program(divide);
    ASSERT_TRUE(divide_code) << divide_code.error;
    for (double zero : {0.0, -0.0}) {
        auto frame = Bind(divide, {ParameterValue::Float(1.0), ParameterValue::Float(zero)});
        DifferentialRuntime interpreted, native;
        const auto interpreted_status = compiled::Interpret(divide, frame, &interpreted);
        const auto native_status = divide_code.code.invoke_program(&native, &frame);
        EXPECT_EQ(native_status, interpreted_status);
        EXPECT_EQ(native.error_message(), interpreted.error_message());
        EXPECT_NE(native.error_message().find("division by zero"), std::string::npos);
    }

    TupleLayout int_layout{4, {{ValueType::INT32, 0, 4}}};
    auto store = Program({{ValueType::FLOAT64, 0}}, {{ValueType::FLOAT64}, {ValueType::TUPLE, 0}}, {int_layout},
                         {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                          Op(Opcode::STORE_COLUMN, 1, 0, compiled::kNoOperand, 0), Op(Opcode::HALT)});
    auto store_code = runtime.compile_program(store);
    ASSERT_TRUE(store_code) << store_code.error;
    const std::array<double, 3> unsafe{std::numeric_limits<double>::quiet_NaN(),
                                       static_cast<double>(std::numeric_limits<int32_t>::max()) + 1.0,
                                       static_cast<double>(std::numeric_limits<int32_t>::min()) - 1.0};
    for (double value : unsafe) {
        auto frame = Bind(store, {ParameterValue::Float(value)});
        DifferentialRuntime interpreted, native;
        const auto interpreted_status = compiled::Interpret(store, frame, &interpreted);
        const auto native_status = store_code.code.invoke_program(&native, &frame);
        EXPECT_EQ(native_status, interpreted_status);
        EXPECT_EQ(native.error_message(), interpreted.error_message());
        EXPECT_NE(native.error_message().find("INT32"), std::string::npos);
    }
}

TEST(NativeProgramTest, NanComparisonsMatchInterpreterForEveryOperator) {
    const std::array<CompareOp, 6> comparisons{CompareOp::EQ, CompareOp::NE, CompareOp::LT,
                                               CompareOp::GT, CompareOp::LE, CompareOp::GE};
    TupleLayout layout{1, {{ValueType::BOOL, 0, 1}}};
    jit::JitRuntime runtime;
    for (CompareOp comparison : comparisons) {
        auto program =
            Program({{ValueType::FLOAT64, 0}, {ValueType::FLOAT64, 0}},
                    {{ValueType::FLOAT64}, {ValueType::FLOAT64}, {ValueType::BOOL}, {ValueType::TUPLE, 0}}, {layout},
                    {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                     Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                     Op(Opcode::COMPARE, 2, 0, 1, compiled::kNoOperand, comparison),
                     Op(Opcode::STORE_COLUMN, 3, 2, compiled::kNoOperand, 0),
                     Op(Opcode::EMIT_ROW, compiled::kNoOperand, 3), Op(Opcode::HALT)});
        auto frame = Bind(
            program, {ParameterValue::Float(std::numeric_limits<double>::quiet_NaN()), ParameterValue::Float(1.0)});
        DifferentialRuntime interpreted, native;
        auto code = runtime.compile_program(program);
        ASSERT_TRUE(code) << code.error;
        ASSERT_EQ(compiled::Interpret(program, frame, &interpreted), ExecStatus::OK);
        ASSERT_EQ(code.code.invoke_program(&native, &frame), ExecStatus::OK);
        ExpectSame(interpreted, native);
        ASSERT_EQ(native.emitted.size(), 1U);
        EXPECT_EQ(native.emitted[0][0] != 0, comparison == CompareOp::NE);
    }
}

TEST(NativeProgramTest, ByteLengthBoundariesAndBackwardJumpMatchInterpreter) {
    constexpr uint32_t width = compiled::MAX_PROGRAM_VALUE_BYTES;
    TupleLayout layout{width, {{ValueType::BYTES, 0, width}}};
    auto bytes = Program({{ValueType::BYTES, width}},
                         {{ValueType::BYTES, compiled::kNoOperand, width}, {ValueType::TUPLE, 0}}, {layout},
                         {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                          Op(Opcode::STORE_COLUMN, 1, 0, compiled::kNoOperand, 0),
                          Op(Opcode::EMIT_ROW, compiled::kNoOperand, 1), Op(Opcode::HALT)});
    jit::JitRuntime runtime;
    auto code = runtime.compile_program(bytes);
    ASSERT_TRUE(code) << code.error;
    const std::array<std::string, 2> byte_values{std::string{}, std::string(width, 'x')};
    for (const std::string& value : byte_values) {
        auto frame = Bind(bytes, {ParameterValue::Bytes(value)});
        DifferentialRuntime interpreted, native;
        ASSERT_EQ(compiled::Interpret(bytes, frame, &interpreted), ExecStatus::OK);
        ASSERT_EQ(code.code.invoke_program(&native, &frame), ExecStatus::OK);
        ExpectSame(interpreted, native);
    }

    auto loop = Program(
        {}, {}, {},
        {Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 0), Op(Opcode::HALT)});
    ParameterFrame empty = Bind(loop, {});
    auto loop_code = runtime.compile_program(loop);
    ASSERT_TRUE(loop_code) << loop_code.error;
    DifferentialRuntime interpreted, native;
    EXPECT_EQ(compiled::Interpret(loop, empty, &interpreted), ExecStatus::ERROR);
    EXPECT_EQ(loop_code.code.invoke_program(&native, &empty), ExecStatus::ERROR);
    EXPECT_NE(interpreted.error_message().find("step limit"), std::string::npos);
    EXPECT_NE(native.error_message().find("step limit"), std::string::npos);
}

TEST(NativeProgramTest, RuntimeHelperStatusesAndMalformedOutputsMatchInterpreter) {
    auto program = UpdateProgram();
    auto frame = Bind(program, {ParameterValue::Int(1), ParameterValue::Int(2)});
    jit::JitRuntime runtime;
    auto code = runtime.compile_program(program);
    ASSERT_TRUE(code) << code.error;

    const std::array<ExecStatus, 4> statuses{ExecStatus::NO_MATCH_RESULT, ExecStatus::FALLBACK, ExecStatus::TXN_ABORT,
                                             ExecStatus::ERROR};
    for (ExecStatus status : statuses) {
        DifferentialRuntime interpreted, native;
        interpreted.key_status = native.key_status = status;
        const auto interpreted_status = compiled::Interpret(program, frame, &interpreted);
        const auto native_status = code.code.invoke_program(&native, &frame);
        EXPECT_EQ(native_status, interpreted_status);
        EXPECT_EQ(native.error_message(), interpreted.error_message());
        EXPECT_EQ(native.calls, interpreted.calls);
    }

    for (int malformed = 0; malformed < 4; ++malformed) {
        DifferentialRuntime interpreted, native;
        interpreted.sticky_key_error = native.sticky_key_error = malformed == 0;
        interpreted.initialize_key = native.initialize_key = malformed != 1;
        interpreted.wrong_key_type = native.wrong_key_type = malformed == 2;
        interpreted.lookup_tuple_size = native.lookup_tuple_size = malformed == 3 ? 5 : 0;
        const auto interpreted_status = compiled::Interpret(program, frame, &interpreted);
        const auto native_status = code.code.invoke_program(&native, &frame);
        EXPECT_EQ(native_status, interpreted_status) << malformed;
        EXPECT_EQ(native.error_message(), interpreted.error_message()) << malformed;
        EXPECT_EQ(native.calls, interpreted.calls) << malformed;
        EXPECT_EQ(native_status, ExecStatus::ERROR);
    }
}

} // namespace
