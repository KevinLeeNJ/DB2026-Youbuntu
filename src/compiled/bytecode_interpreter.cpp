/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "compiled/bytecode_interpreter.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "compiled/program_verifier.h"

namespace compiled {
namespace {

bool IsNumeric(ValueType type) {
    return type == ValueType::INT32 || type == ValueType::FLOAT64;
}

double AsDouble(const RuntimeValue& value) {
    return value.type == ValueType::INT32 ? static_cast<double>(value.int_value) : value.float_value;
}

bool CompareResult(CompareOp op, int comparison) {
    switch (op) {
    case CompareOp::EQ:
        return comparison == 0;
    case CompareOp::NE:
        return comparison != 0;
    case CompareOp::LT:
        return comparison < 0;
    case CompareOp::GT:
        return comparison > 0;
    case CompareOp::LE:
        return comparison <= 0;
    case CompareOp::GE:
        return comparison >= 0;
    }
    return false;
}

ExecStatus CheckHelperResult(ExecStatus status, ProgramRuntime* runtime, const char* helper) {
    if (runtime->last_status() != ExecStatus::OK) {
        return runtime->last_status();
    }
    if (status == ExecStatus::OK || status == ExecStatus::NO_MATCH_RESULT || status == ExecStatus::FALLBACK ||
        status == ExecStatus::TXN_ABORT || status == ExecStatus::ERROR) {
        if (status == ExecStatus::OK) {
            return status;
        }
        const std::string message =
            status == ExecStatus::NO_MATCH_RESULT ? std::string() : std::string(helper) + " failed";
        return runtime->SetError(status, message);
    }
    return runtime->SetError(ExecStatus::ERROR, std::string(helper) + " returned an invalid status");
}

ExecStatus ValidateHelperOutput(const RuntimeValue& value, const RegisterDesc& descriptor,
                                const CompiledProgram& program, ProgramRuntime* runtime, const char* helper) {
    if (!value.initialized) {
        return runtime->SetError(ExecStatus::ERROR, std::string(helper) + " did not initialize its output");
    }
    if (value.type != descriptor.type) {
        return runtime->SetError(ExecStatus::ERROR, std::string(helper) + " returned a value with the wrong type");
    }
    if (descriptor.type == ValueType::TUPLE) {
        const uint32_t expected = program.tuple_layouts()[descriptor.tuple_layout].byte_size;
        if (value.tuple.size() != expected) {
            return runtime->SetError(ExecStatus::ERROR, std::string(helper) + " returned a tuple with the wrong size");
        }
    }
    if (descriptor.type == ValueType::BYTES && value.bytes.size() > descriptor.max_length) {
        return runtime->SetError(ExecStatus::ERROR, std::string(helper) + " returned an oversized byte value");
    }
    return ExecStatus::OK;
}

ExecStatus Arithmetic(const Instruction& instruction, std::vector<RuntimeValue>* registers, ProgramRuntime* runtime) {
    RuntimeValue& dst = (*registers)[instruction.dst];
    const RuntimeValue& lhs = (*registers)[instruction.lhs];
    const RuntimeValue& rhs = (*registers)[instruction.rhs];
    if (dst.type == ValueType::INT32) {
        const int64_t left = lhs.int_value;
        const int64_t right = rhs.int_value;
        int64_t result = 0;
        switch (instruction.opcode) {
        case Opcode::ADD:
            result = left + right;
            break;
        case Opcode::SUB:
            result = left - right;
            break;
        case Opcode::MUL:
            result = left * right;
            break;
        case Opcode::DIV:
            if (right == 0) {
                return runtime->SetError(ExecStatus::ERROR, "integer division by zero");
            }
            result = left / right;
            break;
        default:
            return runtime->SetError(ExecStatus::ERROR, "invalid arithmetic opcode");
        }
        if (result < std::numeric_limits<int32_t>::min() || result > std::numeric_limits<int32_t>::max()) {
            return runtime->SetError(ExecStatus::ERROR, "integer arithmetic overflow");
        }
        dst.int_value = static_cast<int32_t>(result);
        return ExecStatus::OK;
    }

    const double left = AsDouble(lhs);
    const double right = AsDouble(rhs);
    if (instruction.opcode == Opcode::DIV && right == 0.0) {
        return runtime->SetError(ExecStatus::ERROR, "floating-point division by zero");
    }
    switch (instruction.opcode) {
    case Opcode::ADD:
        dst.float_value = left + right;
        break;
    case Opcode::SUB:
        dst.float_value = left - right;
        break;
    case Opcode::MUL:
        dst.float_value = left * right;
        break;
    case Opcode::DIV:
        dst.float_value = left / right;
        break;
    default:
        return runtime->SetError(ExecStatus::ERROR, "invalid arithmetic opcode");
    }
    return ExecStatus::OK;
}

void LoadColumnValue(const ColumnDesc& column, const RuntimeValue& tuple, RuntimeValue* result) {
    const uint8_t* data = tuple.tuple.data() + column.offset;
    switch (column.type) {
    case ValueType::INT32:
        std::memcpy(&result->int_value, data, sizeof(result->int_value));
        break;
    case ValueType::FLOAT64:
        std::memcpy(&result->float_value, data, sizeof(result->float_value));
        break;
    case ValueType::BOOL:
        result->bool_value = *data != 0;
        break;
    case ValueType::BYTES: {
        const auto* end = static_cast<const uint8_t*>(std::memchr(data, 0, column.width));
        const size_t length = end == nullptr ? column.width : static_cast<size_t>(end - data);
        result->bytes.assign(reinterpret_cast<const char*>(data), length);
        break;
    }
    default:
        break;
    }
}

ExecStatus StoreColumnValue(const ColumnDesc& column, const RuntimeValue& value, RuntimeValue* tuple,
                            ProgramRuntime* runtime) {
    uint8_t* data = tuple->tuple.data() + column.offset;
    switch (column.type) {
    case ValueType::INT32: {
        if (value.type == ValueType::FLOAT64 &&
            (value.float_value < std::numeric_limits<int32_t>::min() ||
             value.float_value > std::numeric_limits<int32_t>::max() || std::isnan(value.float_value))) {
            return runtime->SetError(ExecStatus::ERROR, "numeric value cannot be stored as INT32");
        }
        const int32_t converted =
            value.type == ValueType::INT32 ? value.int_value : static_cast<int32_t>(value.float_value);
        std::memcpy(data, &converted, sizeof(converted));
        break;
    }
    case ValueType::FLOAT64: {
        const double converted =
            value.type == ValueType::FLOAT64 ? value.float_value : static_cast<double>(value.int_value);
        std::memcpy(data, &converted, sizeof(converted));
        break;
    }
    case ValueType::BOOL: {
        const uint8_t converted = value.bool_value ? 1 : 0;
        std::memcpy(data, &converted, sizeof(converted));
        break;
    }
    case ValueType::BYTES:
        std::memset(data, 0, column.width);
        std::memcpy(data, value.bytes.data(), std::min<size_t>(column.width, value.bytes.size()));
        break;
    default:
        return runtime->SetError(ExecStatus::ERROR, "invalid tuple column type");
    }
    return ExecStatus::OK;
}

void CompareValues(const Instruction& instruction, std::vector<RuntimeValue>* registers) {
    RuntimeValue& result = (*registers)[instruction.dst];
    const RuntimeValue& lhs = (*registers)[instruction.lhs];
    const RuntimeValue& rhs = (*registers)[instruction.rhs];
    if (IsNumeric(lhs.type) && IsNumeric(rhs.type)) {
        const double left = AsDouble(lhs);
        const double right = AsDouble(rhs);
        if (std::isnan(left) || std::isnan(right)) {
            result.bool_value = instruction.compare_op == CompareOp::NE;
            return;
        }
        result.bool_value = CompareResult(instruction.compare_op, left < right ? -1 : (left > right ? 1 : 0));
        return;
    }
    if (lhs.type == ValueType::BYTES) {
        result.bool_value = CompareResult(instruction.compare_op, lhs.bytes.compare(rhs.bytes));
        return;
    }
    result.bool_value =
        instruction.compare_op == CompareOp::EQ ? lhs.bool_value == rhs.bool_value : lhs.bool_value != rhs.bool_value;
}

} // namespace

ExecStatus Interpret(const CompiledProgram& program, const ParameterFrame& parameters, ProgramRuntime* runtime,
                     uint64_t step_limit) noexcept {
    if (runtime == nullptr) {
        return ExecStatus::ERROR;
    }
    try {
        runtime->ClearError();
        const VerifyResult verification = VerifyProgram(program);
        if (!verification) {
            return runtime->SetError(ExecStatus::ERROR, verification.error);
        }
        if (step_limit == 0 || parameters.size() != program.parameters().size()) {
            return runtime->SetError(ExecStatus::ERROR, "invalid interpreter frame or step limit");
        }
        for (size_t index = 0; index < parameters.size(); ++index) {
            const RuntimeValue& value = parameters.value(index);
            const ParameterDesc& descriptor = program.parameters()[index];
            if (value.type != descriptor.type ||
                (value.type == ValueType::BYTES && value.bytes.size() > descriptor.max_length)) {
                return runtime->SetError(ExecStatus::ERROR, "parameter frame does not match the program");
            }
        }

        std::vector<RuntimeValue> registers(program.registers().size());
        for (size_t index = 0; index < registers.size(); ++index) {
            const RegisterDesc& descriptor = program.registers()[index];
            registers[index].type = descriptor.type;
            if (descriptor.type == ValueType::TUPLE) {
                registers[index].tuple.resize(program.tuple_layouts()[descriptor.tuple_layout].byte_size);
                registers[index].initialized = true;
            }
        }

        size_t pc = 0;
        uint64_t steps = 0;
        while (true) {
            if (steps++ >= step_limit) {
                return runtime->SetError(ExecStatus::ERROR, "interpreter step limit exceeded");
            }
            const Instruction& instruction = program.instructions()[pc];
            ExecStatus status = ExecStatus::OK;
            switch (instruction.opcode) {
            case Opcode::LOAD_PARAM:
                registers[instruction.dst] = parameters.value(instruction.aux);
                break;
            case Opcode::MAKE_POINT_KEY:
                registers[instruction.dst].initialized = false;
                status = CheckHelperResult(
                    runtime->MakePointKey(instruction.aux, registers[instruction.lhs], &registers[instruction.dst]),
                    runtime, "MakePointKey");
                if (status != ExecStatus::OK)
                    return status;
                status = ValidateHelperOutput(registers[instruction.dst], program.registers()[instruction.dst], program,
                                              runtime, "MakePointKey");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::POINT_LOOKUP:
                registers[instruction.rhs].initialized = false;
                registers[instruction.dst].initialized = false;
                status = CheckHelperResult(runtime->PointLookup(registers[instruction.lhs], &registers[instruction.rhs],
                                                                &registers[instruction.dst]),
                                           runtime, "PointLookup");
                if (status != ExecStatus::OK)
                    return status;
                status = ValidateHelperOutput(registers[instruction.rhs], program.registers()[instruction.rhs], program,
                                              runtime, "PointLookup");
                if (status != ExecStatus::OK)
                    return status;
                status = ValidateHelperOutput(registers[instruction.dst], program.registers()[instruction.dst], program,
                                              runtime, "PointLookup");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::COPY_TUPLE:
                registers[instruction.dst].tuple = registers[instruction.lhs].tuple;
                break;
            case Opcode::LOAD_COLUMN: {
                const RegisterDesc& tuple = program.registers()[instruction.lhs];
                const ColumnDesc& column = program.tuple_layouts()[tuple.tuple_layout].columns[instruction.aux];
                LoadColumnValue(column, registers[instruction.lhs], &registers[instruction.dst]);
                registers[instruction.dst].initialized = true;
                break;
            }
            case Opcode::STORE_COLUMN: {
                const RegisterDesc& tuple = program.registers()[instruction.dst];
                const ColumnDesc& column = program.tuple_layouts()[tuple.tuple_layout].columns[instruction.aux];
                status = StoreColumnValue(column, registers[instruction.lhs], &registers[instruction.dst], runtime);
                if (status != ExecStatus::OK)
                    return status;
                break;
            }
            case Opcode::ADD:
            case Opcode::SUB:
            case Opcode::MUL:
            case Opcode::DIV:
                status = Arithmetic(instruction, &registers, runtime);
                if (status != ExecStatus::OK)
                    return status;
                registers[instruction.dst].initialized = true;
                break;
            case Opcode::COMPARE:
                CompareValues(instruction, &registers);
                registers[instruction.dst].initialized = true;
                break;
            case Opcode::JUMP:
                pc = instruction.aux;
                continue;
            case Opcode::JUMP_IF_FALSE:
                if (!registers[instruction.lhs].bool_value) {
                    pc = instruction.aux;
                    continue;
                }
                break;
            case Opcode::PREPARE_UPDATE:
                registers[instruction.dst].initialized = false;
                // Preserve the point-lookup bytes for the helper's input while
                // clearing the output-valid bit. A successful helper must
                // explicitly confirm or replace the post-lock tuple.
                registers[instruction.rhs].initialized = false;
                status =
                    CheckHelperResult(runtime->PrepareUpdate(registers[instruction.lhs], &registers[instruction.rhs],
                                                             &registers[instruction.dst]),
                                      runtime, "PrepareUpdate");
                if (status != ExecStatus::OK)
                    return status;
                status = ValidateHelperOutput(registers[instruction.rhs], program.registers()[instruction.rhs], program,
                                              runtime, "PrepareUpdate");
                if (status != ExecStatus::OK)
                    return status;
                status = ValidateHelperOutput(registers[instruction.dst], program.registers()[instruction.dst], program,
                                              runtime, "PrepareUpdate");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::COMMIT_UPDATE:
                status =
                    CheckHelperResult(runtime->CommitUpdate(registers[instruction.lhs], registers[instruction.rhs]),
                                      runtime, "CommitUpdate");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::DELETE_ROW:
                status = CheckHelperResult(runtime->DeleteRow(registers[instruction.lhs]), runtime, "DeleteRow");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::INSERT_ROW:
                registers[instruction.dst].initialized = false;
                status = CheckHelperResult(runtime->InsertRow(registers[instruction.lhs], &registers[instruction.dst]),
                                           runtime, "InsertRow");
                if (status != ExecStatus::OK)
                    return status;
                status = ValidateHelperOutput(registers[instruction.dst], program.registers()[instruction.dst], program,
                                              runtime, "InsertRow");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::EMIT_ROW:
                status = CheckHelperResult(runtime->EmitRow(registers[instruction.lhs]), runtime, "EmitRow");
                if (status != ExecStatus::OK)
                    return status;
                break;
            case Opcode::HALT:
                return ExecStatus::OK;
            }
            ++pc;
        }
    } catch (const std::exception& error) {
        return runtime->SetError(ExecStatus::ERROR, std::string("interpreter exception: ") + error.what());
    } catch (...) {
        return runtime->SetError(ExecStatus::ERROR, "unknown interpreter exception");
    }
}

} // namespace compiled
