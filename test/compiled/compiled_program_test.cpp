/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "compiled/bytecode_interpreter.h"
#include "compiled/program_verifier.h"

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
               CompareOp compare_op = CompareOp::EQ) {
    return {opcode, dst, lhs, rhs, aux, compare_op};
}

CompiledProgram Program(std::vector<ParameterDesc> parameters, std::vector<RegisterDesc> registers,
                        std::vector<TupleLayout> layouts, std::vector<Instruction> instructions,
                        uint32_t ir_version = compiled::COMPILED_IR_VERSION,
                        uint32_t abi_version = compiled::COMPILED_ABI_VERSION) {
    return {ir_version,           abi_version,        ProgramKind::POINT_SELECT, 17, std::move(parameters),
            std::move(registers), std::move(layouts), std::move(instructions)};
}

ParameterFrame Bind(const CompiledProgram& program, std::vector<ParameterValue> values) {
    std::string error;
    auto frame = ParameterFrame::Bind(program.parameters(), values, &error);
    EXPECT_TRUE(frame.has_value()) << error;
    return std::move(*frame);
}

class FakeRuntime : public compiled::ProgramRuntime {
public:
    ExecStatus MakePointKey(uint32_t index_id, const RuntimeValue& value, RuntimeValue* key) noexcept override {
        calls.push_back("key");
        if (sticky_key_error) {
            SetError(ExecStatus::ERROR, "sticky key error");
            return ExecStatus::OK;
        }
        seen_index_id = index_id;
        EXPECT_EQ(value.type, ValueType::TUPLE);
        seen_key_tuple = value.tuple;
        key->opaque = 1000 + index_id;
        key->initialized = !skip_key_write;
        return ExecStatus::OK;
    }

    ExecStatus PointLookup(const RuntimeValue& key, RuntimeValue* row, RuntimeValue* tuple) noexcept override {
        calls.push_back("lookup");
        if (lookup_status != ExecStatus::OK) {
            return SetError(lookup_status, lookup_error);
        }
        EXPECT_EQ(key.type, ValueType::POINT_KEY);
        row->opaque = 77;
        tuple->tuple = lookup_tuple;
        row->initialized = true;
        tuple->initialized = true;
        return ExecStatus::OK;
    }

    ExecStatus PrepareUpdate(const RuntimeValue& row, RuntimeValue* current_tuple,
                             RuntimeValue* prepared) noexcept override {
        calls.push_back("prepare");
        EXPECT_EQ(row.opaque, 77U);
        EXPECT_EQ(current_tuple->type, ValueType::TUPLE);
        prepare_input_tuple = current_tuple->tuple;
        if (!skip_prepare_tuple_write) {
            if (!prepare_refresh_tuple.empty()) {
                current_tuple->tuple = prepare_refresh_tuple;
            }
            current_tuple->initialized = true;
        }
        if (!skip_prepare_handle_write) {
            prepared->opaque = 88;
            prepared->initialized = true;
        }
        return ExecStatus::OK;
    }

    ExecStatus CommitUpdate(const RuntimeValue& prepared, const RuntimeValue& proposed_tuple) noexcept override {
        calls.push_back("commit");
        EXPECT_EQ(prepared.opaque, 88U);
        committed_tuple = proposed_tuple.tuple;
        return ExecStatus::OK;
    }

    ExecStatus DeleteRow(const RuntimeValue& row) noexcept override {
        calls.push_back("delete");
        EXPECT_EQ(row.type, ValueType::ROW_HANDLE);
        return ExecStatus::OK;
    }

    ExecStatus InsertRow(const RuntimeValue& tuple, RuntimeValue* row) noexcept override {
        calls.push_back("insert");
        EXPECT_EQ(tuple.type, ValueType::TUPLE);
        row->opaque = 99;
        row->initialized = true;
        return ExecStatus::OK;
    }

    ExecStatus EmitRow(const RuntimeValue& tuple) noexcept override {
        calls.push_back("emit");
        emitted.push_back(tuple.tuple);
        return ExecStatus::OK;
    }

    std::vector<std::string> calls;
    std::vector<uint8_t> lookup_tuple;
    std::vector<std::vector<uint8_t>> emitted;
    std::vector<uint8_t> seen_key_tuple;
    std::vector<uint8_t> prepare_input_tuple;
    std::vector<uint8_t> prepare_refresh_tuple;
    std::vector<uint8_t> committed_tuple;
    uint32_t seen_index_id{0};
    ExecStatus lookup_status{ExecStatus::OK};
    std::string lookup_error;
    bool sticky_key_error{false};
    bool skip_key_write{false};
    bool skip_prepare_tuple_write{false};
    bool skip_prepare_handle_write{false};
};

TupleLayout IntTuple() {
    return {sizeof(int32_t), {{ValueType::INT32, 0, sizeof(int32_t)}}};
}

int32_t ReadInt(const std::vector<uint8_t>& bytes, size_t offset = 0) {
    int32_t value = 0;
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
}

TEST(CompiledProgramTest, ExecutesArithmeticCompareBranchAndHalt) {
    auto program =
        Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}, {ValueType::INT32, 0}},
                {{ValueType::INT32},
                 {ValueType::INT32},
                 {ValueType::INT32},
                 {ValueType::INT32},
                 {ValueType::BOOL},
                 {ValueType::TUPLE, 0}},
                {IntTuple()},
                {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                 Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1), Op(Opcode::ADD, 2, 0, 1),
                 Op(Opcode::LOAD_PARAM, 3, compiled::kNoOperand, compiled::kNoOperand, 2),
                 Op(Opcode::COMPARE, 4, 2, 3, compiled::kNoOperand, CompareOp::GT),
                 Op(Opcode::JUMP_IF_FALSE, compiled::kNoOperand, 4, compiled::kNoOperand, 8),
                 Op(Opcode::STORE_COLUMN, 5, 2, compiled::kNoOperand, 0), Op(Opcode::EMIT_ROW, compiled::kNoOperand, 5),
                 Op(Opcode::HALT)});
    ASSERT_TRUE(compiled::VerifyProgram(program)) << compiled::VerifyProgram(program).error;

    FakeRuntime runtime;
    EXPECT_EQ(compiled::Interpret(
                  program, Bind(program, {ParameterValue::Int(7), ParameterValue::Int(5), ParameterValue::Int(10)}),
                  &runtime),
              ExecStatus::OK);
    ASSERT_EQ(runtime.emitted.size(), 1U);
    EXPECT_EQ(ReadInt(runtime.emitted.front()), 12);

    runtime.calls.clear();
    runtime.emitted.clear();
    EXPECT_EQ(compiled::Interpret(
                  program, Bind(program, {ParameterValue::Int(2), ParameterValue::Int(3), ParameterValue::Int(10)}),
                  &runtime),
              ExecStatus::OK);
    EXPECT_TRUE(runtime.emitted.empty());
}

TEST(ParameterFrameTest, OwnsStringBytesAndRejectsTypeOrLengthMismatch) {
    const std::vector<ParameterDesc> descriptors{{ValueType::BYTES, 8}, {ValueType::INT32, 0}};
    std::string source = "owned";
    std::vector<ParameterValue> values{ParameterValue::Bytes(source), ParameterValue::Int(4)};
    std::string error;
    auto frame = ParameterFrame::Bind(descriptors, values, &error);
    ASSERT_TRUE(frame.has_value()) << error;
    ParameterFrame copied = *frame;
    ParameterFrame moved = std::move(copied);
    EXPECT_EQ(copied.size(), 0U);
    EXPECT_THROW(copied.slot(0), std::out_of_range);
    ParameterFrame copy_assigned = moved;
    copy_assigned = *frame;
    ParameterFrame move_assigned = *frame;
    move_assigned = std::move(copy_assigned);
    EXPECT_EQ(copy_assigned.size(), 0U);
    EXPECT_THROW(copy_assigned.slot(0), std::out_of_range);
    source.assign("xxxxx");
    values[0] = ParameterValue::Bytes("yyyyy");
    EXPECT_EQ(frame->value(0).bytes, "owned");
    EXPECT_EQ(moved.value(0).bytes, "owned");
    EXPECT_EQ(std::string(moved.slot(0).bytes, moved.slot(0).bytes_length), "owned");
    EXPECT_EQ(moved.slot(1).int_value, 4);
    EXPECT_EQ(std::string(move_assigned.slot(0).bytes, move_assigned.slot(0).bytes_length), "owned");

    EXPECT_FALSE(ParameterFrame::Bind(descriptors, {ParameterValue::Int(1), ParameterValue::Int(4)}, &error));
    EXPECT_NE(error.find("type"), std::string::npos);
    EXPECT_FALSE(
        ParameterFrame::Bind(descriptors, {ParameterValue::Bytes("too-long!"), ParameterValue::Int(4)}, &error));
    EXPECT_NE(error.find("length"), std::string::npos);
}

TEST(ParameterFrameTest, MoveBindTransfersStringStorageAndPreservesScalarValues) {
    const std::vector<ParameterDesc> descriptors{{ValueType::BYTES, 64}, {ValueType::INT32, 0}};
    std::vector<ParameterValue> values{ParameterValue::Bytes(std::string(48, 'x')), ParameterValue::Int(37)};
    const char* source_bytes = values[0].value().bytes.data();
    std::string error = "stale";

    auto frame = ParameterFrame::Bind(descriptors, std::move(values), &error);

    ASSERT_TRUE(frame.has_value()) << error;
    EXPECT_TRUE(error.empty());
    EXPECT_EQ(frame->value(0).bytes, std::string(48, 'x'));
    EXPECT_EQ(frame->value(0).bytes.data(), source_bytes);
    EXPECT_EQ(frame->slot(0).bytes, frame->value(0).bytes.data());
    EXPECT_EQ(frame->slot(0).bytes_length, 48U);
    EXPECT_EQ(frame->value(1).int_value, 37);
    EXPECT_EQ(frame->slot(1).int_value, 37);
}

TEST(CompiledProgramTest, CopiesTupleAndPreservesFixedWidthStringSemantics) {
    TupleLayout layout{16, {{ValueType::INT32, 0, 4}, {ValueType::BYTES, 4, 8}}};
    TupleLayout key_layout{8, {{ValueType::BYTES, 0, 8}}};
    auto program = Program({{ValueType::INT32, 0}, {ValueType::BYTES, 16}},
                           {{ValueType::INT32},
                            {ValueType::BYTES, compiled::kNoOperand, 16},
                            {ValueType::TUPLE, 0},
                            {ValueType::TUPLE, 0},
                            {ValueType::BYTES, compiled::kNoOperand, 8},
                            {ValueType::TUPLE, 1},
                            {ValueType::POINT_KEY}},
                           {layout, key_layout},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                            Op(Opcode::STORE_COLUMN, 2, 0, compiled::kNoOperand, 0),
                            Op(Opcode::STORE_COLUMN, 2, 1, compiled::kNoOperand, 1), Op(Opcode::COPY_TUPLE, 3, 2),
                            Op(Opcode::LOAD_COLUMN, 4, 3, compiled::kNoOperand, 1),
                            Op(Opcode::STORE_COLUMN, 5, 4, compiled::kNoOperand, 0),
                            Op(Opcode::MAKE_POINT_KEY, 6, 5, compiled::kNoOperand, 3),
                            Op(Opcode::EMIT_ROW, compiled::kNoOperand, 3), Op(Opcode::HALT)});
    FakeRuntime runtime;
    std::string with_nul("ab\0cd", 5);
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(9), ParameterValue::Bytes(with_nul)}),
                                  &runtime),
              ExecStatus::OK);
    ASSERT_EQ(runtime.emitted.size(), 1U);
    EXPECT_EQ(ReadInt(runtime.emitted.front()), 9);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(runtime.emitted.front().data() + 4), 8),
              std::string("ab\0cd\0\0\0", 8));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(runtime.seen_key_tuple.data()), 2), "ab");

    runtime.emitted.clear();
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(9), ParameterValue::Bytes("abcdefgh")}),
                                  &runtime),
              ExecStatus::OK);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(runtime.seen_key_tuple.data()), 8), "abcdefgh");

    runtime.emitted.clear();
    EXPECT_EQ(compiled::Interpret(
                  program, Bind(program, {ParameterValue::Int(9), ParameterValue::Bytes("toolong-value")}), &runtime),
              ExecStatus::OK);
    ASSERT_EQ(runtime.emitted.size(), 1U);
    EXPECT_EQ(ReadInt(runtime.emitted.front()), 9);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(runtime.emitted.front().data() + 4), 8), "toolong-");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(runtime.seen_key_tuple.data()), 8), "toolong-");
    EXPECT_EQ(runtime.emitted.front()[12], 0U);
    EXPECT_EQ(runtime.emitted.front()[15], 0U);
}

TEST(CompiledProgramTest, UsesMemcpyForUnalignedColumnsWithoutTouchingCanaries) {
    TupleLayout layout{16, {{ValueType::INT32, 6, sizeof(int32_t)}}};
    auto program = Program({{ValueType::INT32, 0}}, {{ValueType::INT32}, {ValueType::TUPLE, 0}}, {layout},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::STORE_COLUMN, 1, 0, compiled::kNoOperand, 0),
                            Op(Opcode::EMIT_ROW, compiled::kNoOperand, 1), Op(Opcode::HALT)});
    FakeRuntime runtime;
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(0x12345678)}), &runtime), ExecStatus::OK);
    ASSERT_EQ(runtime.emitted.size(), 1U);
    EXPECT_EQ(ReadInt(runtime.emitted.front(), 6), 0x12345678);
    for (size_t index = 0; index < runtime.emitted.front().size(); ++index) {
        if (index < 6 || index >= 10) {
            EXPECT_EQ(runtime.emitted.front()[index], 0U) << index;
        }
    }
}

TEST(CompiledProgramTest, DispatchesAllDatabaseRuntimeHelpers) {
    auto program =
        Program({{ValueType::INT32, 0}},
                {{ValueType::INT32},
                 {ValueType::TUPLE, 0},
                 {ValueType::POINT_KEY},
                 {ValueType::ROW_HANDLE},
                 {ValueType::TUPLE, 0},
                 {ValueType::TUPLE, 0},
                 {ValueType::PREPARED_UPDATE}},
                {IntTuple()},
                {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                 Op(Opcode::STORE_COLUMN, 1, 0, compiled::kNoOperand, 0),
                 Op(Opcode::MAKE_POINT_KEY, 2, 1, compiled::kNoOperand, 12), Op(Opcode::POINT_LOOKUP, 4, 2, 3),
                 Op(Opcode::PREPARE_UPDATE, 6, 3, 4), Op(Opcode::COPY_TUPLE, 5, 4),
                 Op(Opcode::COMMIT_UPDATE, compiled::kNoOperand, 6, 5), Op(Opcode::DELETE_ROW, compiled::kNoOperand, 3),
                 Op(Opcode::INSERT_ROW, 3, 5), Op(Opcode::EMIT_ROW, compiled::kNoOperand, 5), Op(Opcode::HALT)});
    FakeRuntime runtime;
    runtime.lookup_tuple.resize(sizeof(int32_t));
    const int32_t value = 43;
    std::memcpy(runtime.lookup_tuple.data(), &value, sizeof(value));
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(5)}), &runtime), ExecStatus::OK);
    EXPECT_EQ(runtime.calls,
              (std::vector<std::string>{"key", "lookup", "prepare", "commit", "delete", "insert", "emit"}));
    EXPECT_EQ(runtime.seen_index_id, 12U);
    EXPECT_EQ(ReadInt(runtime.seen_key_tuple), 5);
    ASSERT_EQ(runtime.emitted.size(), 1U);
    EXPECT_EQ(ReadInt(runtime.emitted.front()), value);
}

TEST(CompiledProgramTest, DispatchesCompositePointKeyTuple) {
    TupleLayout key_layout{12, {{ValueType::INT32, 0, 4}, {ValueType::BYTES, 4, 8}}};
    auto program = Program({{ValueType::INT32, 0}, {ValueType::BYTES, 8}},
                           {{ValueType::INT32},
                            {ValueType::BYTES, compiled::kNoOperand, 8},
                            {ValueType::TUPLE, 0},
                            {ValueType::POINT_KEY}},
                           {key_layout},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                            Op(Opcode::STORE_COLUMN, 2, 0, compiled::kNoOperand, 0),
                            Op(Opcode::STORE_COLUMN, 2, 1, compiled::kNoOperand, 1),
                            Op(Opcode::MAKE_POINT_KEY, 3, 2, compiled::kNoOperand, 7), Op(Opcode::HALT)});
    FakeRuntime runtime;
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(17), ParameterValue::Bytes("district")}),
                                  &runtime),
              ExecStatus::OK);
    ASSERT_EQ(runtime.seen_key_tuple.size(), 12U);
    EXPECT_EQ(ReadInt(runtime.seen_key_tuple), 17);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(runtime.seen_key_tuple.data() + 4), 8), "district");
    EXPECT_EQ(runtime.seen_index_id, 7U);
}

TEST(CompiledProgramTest, PrepareRefreshesTupleBeforeArithmeticAndCommit) {
    auto program = Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
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
                           {IntTuple()},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                            Op(Opcode::STORE_COLUMN, 2, 0, compiled::kNoOperand, 0),
                            Op(Opcode::MAKE_POINT_KEY, 3, 2, compiled::kNoOperand, 0),
                            Op(Opcode::POINT_LOOKUP, 5, 3, 4), Op(Opcode::PREPARE_UPDATE, 6, 4, 5),
                            Op(Opcode::LOAD_COLUMN, 7, 5, compiled::kNoOperand, 0), Op(Opcode::ADD, 8, 7, 1),
                            Op(Opcode::COPY_TUPLE, 9, 5), Op(Opcode::STORE_COLUMN, 9, 8, compiled::kNoOperand, 0),
                            Op(Opcode::COMMIT_UPDATE, compiled::kNoOperand, 6, 9), Op(Opcode::HALT)});
    FakeRuntime runtime;
    runtime.lookup_tuple.resize(sizeof(int32_t));
    runtime.prepare_refresh_tuple.resize(sizeof(int32_t));
    const int32_t stale = 10;
    const int32_t refreshed = 40;
    std::memcpy(runtime.lookup_tuple.data(), &stale, sizeof(stale));
    std::memcpy(runtime.prepare_refresh_tuple.data(), &refreshed, sizeof(refreshed));
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(1), ParameterValue::Int(2)}), &runtime),
              ExecStatus::OK);
    EXPECT_EQ(ReadInt(runtime.prepare_input_tuple), stale);
    EXPECT_EQ(ReadInt(runtime.committed_tuple), 42);
    EXPECT_EQ(runtime.calls, (std::vector<std::string>{"key", "lookup", "prepare", "commit"}));

    runtime.calls.clear();
    runtime.skip_prepare_tuple_write = true;
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(1), ParameterValue::Int(2)}), &runtime),
              ExecStatus::ERROR);
    EXPECT_NE(runtime.error_message().find("initialize"), std::string::npos);
    EXPECT_EQ(runtime.calls, (std::vector<std::string>{"key", "lookup", "prepare"}));

    runtime.calls.clear();
    runtime.skip_prepare_tuple_write = false;
    runtime.skip_prepare_handle_write = true;
    EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(1), ParameterValue::Int(2)}), &runtime),
              ExecStatus::ERROR);
    EXPECT_NE(runtime.error_message().find("initialize"), std::string::npos);
    EXPECT_EQ(runtime.calls, (std::vector<std::string>{"key", "lookup", "prepare"}));
}

TEST(CompiledProgramTest, PropagatesHelperErrorsAndDivisionByZero) {
    auto lookup = Program({{ValueType::INT32, 0}},
                          {{ValueType::INT32},
                           {ValueType::TUPLE, 0},
                           {ValueType::POINT_KEY},
                           {ValueType::ROW_HANDLE},
                           {ValueType::TUPLE, 0}},
                          {IntTuple()},
                          {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                           Op(Opcode::STORE_COLUMN, 1, 0, compiled::kNoOperand, 0),
                           Op(Opcode::MAKE_POINT_KEY, 2, 1, compiled::kNoOperand, 1), Op(Opcode::POINT_LOOKUP, 4, 2, 3),
                           Op(Opcode::HALT)});
    FakeRuntime runtime;
    runtime.lookup_status = ExecStatus::TXN_ABORT;
    runtime.lookup_error = "write conflict";
    EXPECT_EQ(compiled::Interpret(lookup, Bind(lookup, {ParameterValue::Int(1)}), &runtime), ExecStatus::TXN_ABORT);
    EXPECT_EQ(runtime.error_message(), "write conflict");
    EXPECT_EQ(runtime.calls, (std::vector<std::string>{"key", "lookup"}));
    runtime.sticky_key_error = true;
    runtime.calls.clear();
    EXPECT_EQ(compiled::Interpret(lookup, Bind(lookup, {ParameterValue::Int(1)}), &runtime), ExecStatus::ERROR);
    EXPECT_EQ(runtime.error_message(), "sticky key error");
    EXPECT_EQ(runtime.calls, (std::vector<std::string>{"key"}));
    runtime.sticky_key_error = false;
    runtime.skip_key_write = true;
    runtime.calls.clear();
    EXPECT_EQ(compiled::Interpret(lookup, Bind(lookup, {ParameterValue::Int(1)}), &runtime), ExecStatus::ERROR);
    EXPECT_NE(runtime.error_message().find("initialize"), std::string::npos);
    EXPECT_EQ(runtime.calls, (std::vector<std::string>{"key"}));
    runtime.skip_key_write = false;
    runtime.lookup_status = ExecStatus::OK;
    runtime.lookup_tuple.resize(sizeof(int32_t));
    runtime.calls.clear();
    EXPECT_EQ(compiled::Interpret(lookup, Bind(lookup, {ParameterValue::Int(1)}), &runtime), ExecStatus::OK);
    EXPECT_TRUE(runtime.error_message().empty());

    auto divide = Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                          {{ValueType::INT32}, {ValueType::INT32}, {ValueType::INT32}}, {},
                          {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                           Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                           Op(Opcode::DIV, 2, 0, 1), Op(Opcode::HALT)});
    EXPECT_EQ(compiled::Interpret(divide, Bind(divide, {ParameterValue::Int(8), ParameterValue::Int(0)}), &runtime),
              ExecStatus::ERROR);
    EXPECT_NE(runtime.error_message().find("division by zero"), std::string::npos);
}

TEST(CompiledProgramTest, RejectsMalformedProgramsBeforeExecution) {
    FakeRuntime runtime;
    auto good = Program({{ValueType::INT32, 0}}, {{ValueType::INT32}}, {},
                        {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0), Op(Opcode::HALT)});
    EXPECT_TRUE(compiled::VerifyProgram(good));

    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {}, {}, {Op(Opcode::HALT)}, 99)));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {}, {}, {Op(Opcode::HALT)}, compiled::COMPILED_IR_VERSION, 99)));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {{ValueType::INT32}}, {}, {Op(Opcode::HALT, 0)})));
    EXPECT_FALSE(
        compiled::VerifyProgram(Program({{ValueType::INT32, 0}}, {{ValueType::INT32}}, {},
                                        {Op(Opcode::LOAD_PARAM, 0, 0, compiled::kNoOperand, 0), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, {{ValueType::INT32}}, {},
                {Op(Opcode::JUMP, 0, compiled::kNoOperand, compiled::kNoOperand, 1), Op(Opcode::HALT)})));
    EXPECT_FALSE(
        compiled::VerifyProgram(CompiledProgram(compiled::COMPILED_IR_VERSION, compiled::COMPILED_ABI_VERSION,
                                                static_cast<ProgramKind>(255), 0, {}, {}, {}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, {{ValueType::INT32}}, {},
                {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({{ValueType::FLOAT64, 0}}, {{ValueType::INT32}}, {},
                {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0), Op(Opcode::HALT)})));
    EXPECT_FALSE(
        compiled::VerifyProgram(Program({}, {{ValueType::INT32}}, {}, {Op(Opcode::ADD, 0, 0, 4), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program(
        {}, {}, {},
        {Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 9), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, {}, {}, {Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 0)})));
    EXPECT_FALSE(
        compiled::VerifyProgram(Program({{ValueType::INT32, 0}}, {{ValueType::INT32}}, {},
                                        {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {}, {}, {Op(Opcode::HALT), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, {{ValueType::TUPLE, 0}}, {{4, {{ValueType::INT32, 2, 4}}}}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, {{ValueType::TUPLE, 1}}, {{4, {{ValueType::INT32, 0, 4}}}}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {{ValueType::TUPLE, 0}},
                                                 {{4, {{ValueType::INT32, std::numeric_limits<uint32_t>::max(), 4}}}},
                                                 {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {{ValueType::INT32}, {ValueType::INT32}, {ValueType::INT32}}, {},
                                                 {Op(Opcode::ADD, 2, 0, 1), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program(
        {{ValueType::INT32, 0}, {ValueType::INT32, 0}}, {{ValueType::INT32}, {ValueType::INT32}, {ValueType::BOOL}}, {},
        {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
         Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
         Op(Opcode::COMPARE, 2, 0, 1, compiled::kNoOperand, static_cast<CompareOp>(255)), Op(Opcode::HALT)})));
    EXPECT_FALSE(
        compiled::VerifyProgram(Program({}, {{ValueType::PREPARED_UPDATE}}, {},
                                        {Op(Opcode::COMMIT_UPDATE, compiled::kNoOperand, 0), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({{ValueType::INT32, 0}}, {{ValueType::INT32}, {ValueType::POINT_KEY}}, {},
                {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                 Op(Opcode::MAKE_POINT_KEY, 1, 0, compiled::kNoOperand, 0), Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program(
        {{ValueType::BOOL, 0}, {ValueType::INT32, 0}}, {{ValueType::BOOL}, {ValueType::INT32}, {ValueType::INT32}}, {},
        {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
         Op(Opcode::JUMP_IF_FALSE, compiled::kNoOperand, 0, compiled::kNoOperand, 3),
         Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1), Op(Opcode::ADD, 2, 1, 1),
         Op(Opcode::HALT)})));

    std::vector<Instruction> too_many(compiled::MAX_PROGRAM_INSTRUCTIONS + 1, Op(Opcode::HALT));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, {}, {}, std::move(too_many))));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program(std::vector<ParameterDesc>(compiled::MAX_PROGRAM_PARAMETERS + 1), {}, {}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, std::vector<RegisterDesc>(compiled::MAX_PROGRAM_REGISTERS + 1), {}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({}, {}, std::vector<TupleLayout>(compiled::MAX_PROGRAM_TUPLE_LAYOUTS + 1), {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(Program({}, std::vector<RegisterDesc>(17, {ValueType::TUPLE, 0}),
                                                 {{compiled::MAX_PROGRAM_VALUE_BYTES, {}}}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program({},
                std::vector<RegisterDesc>(
                    17, RegisterDesc{ValueType::BYTES, compiled::kNoOperand, compiled::MAX_PROGRAM_VALUE_BYTES}),
                {}, {Op(Opcode::HALT)})));
    EXPECT_FALSE(compiled::VerifyProgram(
        Program(std::vector<ParameterDesc>(17, {ValueType::BYTES, compiled::MAX_PROGRAM_VALUE_BYTES}), {}, {},
                {Op(Opcode::HALT)})));
    std::string error;
    auto frame = ParameterFrame::Bind(good.parameters(), {ParameterValue::Int(1)}, &error);
    ASSERT_TRUE(frame.has_value());
    EXPECT_EQ(compiled::Interpret(
                  Program({}, {}, {},
                          {Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 9),
                           Op(Opcode::HALT)}),
                  *frame, &runtime),
              ExecStatus::ERROR);
    EXPECT_FALSE(runtime.error_message().empty());
}

TEST(CompiledProgramTest, StopsJumpZeroAtTheStepBound) {
    auto loop = Program(
        {}, {}, {},
        {Op(Opcode::JUMP, compiled::kNoOperand, compiled::kNoOperand, compiled::kNoOperand, 0), Op(Opcode::HALT)});
    ASSERT_TRUE(compiled::VerifyProgram(loop)) << compiled::VerifyProgram(loop).error;
    FakeRuntime runtime;
    EXPECT_EQ(compiled::Interpret(loop, Bind(loop, {}), &runtime, 20), ExecStatus::ERROR);
    EXPECT_NE(runtime.error_message().find("step limit"), std::string::npos);
}

bool OracleCompare(int32_t lhs, int32_t rhs, CompareOp op) {
    switch (op) {
    case CompareOp::EQ:
        return lhs == rhs;
    case CompareOp::NE:
        return lhs != rhs;
    case CompareOp::LT:
        return lhs < rhs;
    case CompareOp::GT:
        return lhs > rhs;
    case CompareOp::LE:
        return lhs <= rhs;
    case CompareOp::GE:
        return lhs >= rhs;
    }
    return false;
}

TEST(CompiledProgramTest, RandomizedArithmeticAndCompareMatchScalarOracle) {
    std::mt19937 random(20260718);
    std::uniform_int_distribution<int32_t> values(-1000, 1000);
    const std::array<Opcode, 4> arithmetic{Opcode::ADD, Opcode::SUB, Opcode::MUL, Opcode::DIV};
    const std::array<CompareOp, 6> comparisons{CompareOp::EQ, CompareOp::NE, CompareOp::LT,
                                               CompareOp::GT, CompareOp::LE, CompareOp::GE};
    FakeRuntime runtime;

    for (Opcode opcode : arithmetic) {
        auto program =
            Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                    {{ValueType::INT32}, {ValueType::INT32}, {ValueType::INT32}, {ValueType::TUPLE, 0}}, {IntTuple()},
                    {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                     Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1), Op(opcode, 2, 0, 1),
                     Op(Opcode::STORE_COLUMN, 3, 2, compiled::kNoOperand, 0),
                     Op(Opcode::EMIT_ROW, compiled::kNoOperand, 3), Op(Opcode::HALT)});
        for (int iteration = 0; iteration < 1000; ++iteration) {
            const int32_t lhs = values(random);
            int32_t rhs = values(random);
            if (opcode == Opcode::DIV && rhs == 0)
                rhs = 1;
            int32_t expected = 0;
            if (opcode == Opcode::ADD)
                expected = lhs + rhs;
            if (opcode == Opcode::SUB)
                expected = lhs - rhs;
            if (opcode == Opcode::MUL)
                expected = lhs * rhs;
            if (opcode == Opcode::DIV)
                expected = lhs / rhs;
            runtime.emitted.clear();
            ASSERT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(lhs), ParameterValue::Int(rhs)}),
                                          &runtime),
                      ExecStatus::OK);
            ASSERT_EQ(runtime.emitted.size(), 1U);
            EXPECT_EQ(ReadInt(runtime.emitted.front()), expected) << iteration;
        }
    }

    TupleLayout bool_tuple{1, {{ValueType::BOOL, 0, 1}}};
    for (CompareOp op : comparisons) {
        auto program =
            Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                    {{ValueType::INT32}, {ValueType::INT32}, {ValueType::BOOL}, {ValueType::TUPLE, 0}}, {bool_tuple},
                    {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                     Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                     Op(Opcode::COMPARE, 2, 0, 1, compiled::kNoOperand, op),
                     Op(Opcode::STORE_COLUMN, 3, 2, compiled::kNoOperand, 0),
                     Op(Opcode::EMIT_ROW, compiled::kNoOperand, 3), Op(Opcode::HALT)});
        for (int iteration = 0; iteration < 1000; ++iteration) {
            const int32_t lhs = values(random);
            const int32_t rhs = values(random);
            runtime.emitted.clear();
            ASSERT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Int(lhs), ParameterValue::Int(rhs)}),
                                          &runtime),
                      ExecStatus::OK);
            ASSERT_EQ(runtime.emitted.size(), 1U);
            EXPECT_EQ(runtime.emitted.front()[0] != 0, OracleCompare(lhs, rhs, op)) << iteration;
        }
    }
}

TEST(CompiledProgramTest, NanComparisonsMatchPredicateSemantics) {
    TupleLayout bool_tuple{1, {{ValueType::BOOL, 0, 1}}};
    const std::array<CompareOp, 6> comparisons{CompareOp::EQ, CompareOp::NE, CompareOp::LT,
                                               CompareOp::GT, CompareOp::LE, CompareOp::GE};
    FakeRuntime runtime;
    for (CompareOp op : comparisons) {
        auto program = Program({{ValueType::FLOAT64, 0}, {ValueType::FLOAT64, 0}},
                               {{ValueType::FLOAT64}, {ValueType::FLOAT64}, {ValueType::BOOL}, {ValueType::TUPLE, 0}},
                               {bool_tuple},
                               {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                                Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                                Op(Opcode::COMPARE, 2, 0, 1, compiled::kNoOperand, op),
                                Op(Opcode::STORE_COLUMN, 3, 2, compiled::kNoOperand, 0),
                                Op(Opcode::EMIT_ROW, compiled::kNoOperand, 3), Op(Opcode::HALT)});
        runtime.emitted.clear();
        EXPECT_EQ(compiled::Interpret(program,
                                      Bind(program, {ParameterValue::Float(std::numeric_limits<double>::quiet_NaN()),
                                                     ParameterValue::Float(1.0)}),
                                      &runtime),
                  ExecStatus::OK);
        ASSERT_EQ(runtime.emitted.size(), 1U);
        EXPECT_EQ(runtime.emitted.front()[0] != 0, op == CompareOp::NE);
    }
}

TEST(CompiledProgramTest, RejectsUnsafeFloatToIntTupleStores) {
    auto program = Program({{ValueType::FLOAT64, 0}}, {{ValueType::FLOAT64}, {ValueType::TUPLE, 0}}, {IntTuple()},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::STORE_COLUMN, 1, 0, compiled::kNoOperand, 0), Op(Opcode::HALT)});
    FakeRuntime runtime;
    const std::array<double, 4> unsafe{
        std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(), static_cast<double>(std::numeric_limits<int32_t>::max()) * 2.0};
    for (double value : unsafe) {
        EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Float(value)}), &runtime),
                  ExecStatus::ERROR);
        EXPECT_NE(runtime.error_message().find("INT32"), std::string::npos);
    }
}

TEST(CompiledProgramTest, RejectsPositiveAndNegativeFloatingPointZeroDivisors) {
    auto program = Program({{ValueType::FLOAT64, 0}, {ValueType::FLOAT64, 0}},
                           {{ValueType::FLOAT64}, {ValueType::FLOAT64}, {ValueType::FLOAT64}}, {},
                           {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                            Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                            Op(Opcode::DIV, 2, 0, 1), Op(Opcode::HALT)});
    FakeRuntime runtime;
    for (double zero : {0.0, -0.0}) {
        EXPECT_EQ(compiled::Interpret(program, Bind(program, {ParameterValue::Float(1.0), ParameterValue::Float(zero)}),
                                      &runtime),
                  ExecStatus::ERROR);
        EXPECT_NE(runtime.error_message().find("division by zero"), std::string::npos);
    }
}

TEST(CompiledProgramTest, RejectsAllInt32ArithmeticOverflowCases) {
    FakeRuntime runtime;
    const std::array<std::tuple<Opcode, int32_t, int32_t>, 4> cases{
        std::tuple{Opcode::ADD, std::numeric_limits<int32_t>::max(), int32_t{1}},
        std::tuple{Opcode::SUB, std::numeric_limits<int32_t>::min(), int32_t{1}},
        std::tuple{Opcode::MUL, std::numeric_limits<int32_t>::max(), int32_t{2}},
        std::tuple{Opcode::DIV, std::numeric_limits<int32_t>::min(), int32_t{-1}},
    };
    for (const auto& [opcode, lhs, rhs] : cases) {
        auto program = Program({{ValueType::INT32, 0}, {ValueType::INT32, 0}},
                               {{ValueType::INT32}, {ValueType::INT32}, {ValueType::INT32}}, {},
                               {Op(Opcode::LOAD_PARAM, 0, compiled::kNoOperand, compiled::kNoOperand, 0),
                                Op(Opcode::LOAD_PARAM, 1, compiled::kNoOperand, compiled::kNoOperand, 1),
                                Op(opcode, 2, 0, 1), Op(Opcode::HALT)});
        EXPECT_EQ(
            compiled::Interpret(program, Bind(program, {ParameterValue::Int(lhs), ParameterValue::Int(rhs)}), &runtime),
            ExecStatus::ERROR);
        EXPECT_NE(runtime.error_message().find("overflow"), std::string::npos);
    }
}

} // namespace
