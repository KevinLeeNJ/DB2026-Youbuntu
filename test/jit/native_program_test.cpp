/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include <cstring>
#include <array>
#include <limits>
#include <string>
#include <tuple>
#include <vector>

#include "compiled/bytecode_interpreter.h"
#include "compiled/program_verifier.h"
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

void ExpectSame(const DifferentialRuntime& interpreted, const DifferentialRuntime& native) {
    EXPECT_EQ(native.calls, interpreted.calls);
    EXPECT_EQ(native.committed, interpreted.committed);
    EXPECT_EQ(native.inserted, interpreted.inserted);
    EXPECT_EQ(native.emitted, interpreted.emitted);
    EXPECT_EQ(native.seen_opaque, interpreted.seen_opaque);
    EXPECT_EQ(native.error_message(), interpreted.error_message());
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
