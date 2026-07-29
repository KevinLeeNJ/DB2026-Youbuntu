/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "compiled/program_verifier.h"

#include <cstdint>
#include <queue>
#include <vector>

namespace compiled {
namespace {

VerifyResult Fail(std::string message) {
    return {false, std::move(message)};
}

bool IsValueType(ValueType type) {
    switch (type) {
    case ValueType::INT32:
    case ValueType::FLOAT64:
    case ValueType::BOOL:
    case ValueType::BYTES:
    case ValueType::TUPLE:
    case ValueType::POINT_KEY:
    case ValueType::ROW_HANDLE:
    case ValueType::PREPARED_UPDATE:
        return true;
    }
    return false;
}

bool IsProgramKind(ProgramKind kind) {
    switch (kind) {
    case ProgramKind::POINT_SELECT:
    case ProgramKind::POINT_UPDATE:
    case ProgramKind::POINT_DELETE:
    case ProgramKind::POINT_INSERT:
        return true;
    }
    return false;
}

bool IsCompareOp(CompareOp op) {
    switch (op) {
    case CompareOp::EQ:
    case CompareOp::NE:
    case CompareOp::LT:
    case CompareOp::GT:
    case CompareOp::LE:
    case CompareOp::GE:
        return true;
    }
    return false;
}

bool IsScalar(ValueType type) {
    return type == ValueType::INT32 || type == ValueType::FLOAT64 || type == ValueType::BOOL ||
           type == ValueType::BYTES;
}

bool IsNumeric(ValueType type) {
    return type == ValueType::INT32 || type == ValueType::FLOAT64;
}

bool CanCompare(ValueType lhs, ValueType rhs, CompareOp op) {
    if (IsNumeric(lhs) && IsNumeric(rhs)) {
        return true;
    }
    if (lhs != rhs || (lhs != ValueType::BYTES && lhs != ValueType::BOOL)) {
        return false;
    }
    return lhs != ValueType::BOOL || op == CompareOp::EQ || op == CompareOp::NE;
}

bool CanStore(ValueType column, ValueType value) {
    return column == value || (IsNumeric(column) && IsNumeric(value));
}

bool ValidColumn(const ColumnDesc& column, uint32_t tuple_size) {
    if (!IsScalar(column.type) || column.width == 0 ||
        static_cast<uint64_t>(column.offset) + column.width > tuple_size) {
        return false;
    }
    if (column.type == ValueType::INT32) {
        return column.width == sizeof(int32_t);
    }
    if (column.type == ValueType::FLOAT64) {
        return column.width == sizeof(double);
    }
    if (column.type == ValueType::BOOL) {
        return column.width == sizeof(uint8_t);
    }
    return true;
}

bool HasCanonicalOperands(const Instruction& instruction) {
    const auto none = [](uint32_t operand) { return operand == kNoOperand; };
    const bool default_compare = instruction.compare_op == CompareOp::EQ;
    switch (instruction.opcode) {
    case Opcode::LOAD_PARAM:
        return !none(instruction.dst) && none(instruction.lhs) && none(instruction.rhs) && !none(instruction.aux) &&
               default_compare;
    case Opcode::MAKE_POINT_KEY:
        return !none(instruction.dst) && !none(instruction.lhs) && none(instruction.rhs) && !none(instruction.aux) &&
               default_compare;
    case Opcode::POINT_LOOKUP:
    case Opcode::PREPARE_UPDATE:
        return !none(instruction.dst) && !none(instruction.lhs) && !none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    case Opcode::COPY_TUPLE:
        return !none(instruction.dst) && !none(instruction.lhs) && none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    case Opcode::LOAD_COLUMN:
    case Opcode::STORE_COLUMN:
        return !none(instruction.dst) && !none(instruction.lhs) && none(instruction.rhs) && !none(instruction.aux) &&
               default_compare;
    case Opcode::ADD:
    case Opcode::SUB:
    case Opcode::MUL:
    case Opcode::DIV:
        return !none(instruction.dst) && !none(instruction.lhs) && !none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    case Opcode::COMPARE:
        return !none(instruction.dst) && !none(instruction.lhs) && !none(instruction.rhs) && none(instruction.aux);
    case Opcode::JUMP:
        return none(instruction.dst) && none(instruction.lhs) && none(instruction.rhs) && !none(instruction.aux) &&
               default_compare;
    case Opcode::JUMP_IF_FALSE:
        return none(instruction.dst) && !none(instruction.lhs) && none(instruction.rhs) && !none(instruction.aux) &&
               default_compare;
    case Opcode::COMMIT_UPDATE:
        return none(instruction.dst) && !none(instruction.lhs) && !none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    case Opcode::DELETE_ROW:
    case Opcode::EMIT_ROW:
        return none(instruction.dst) && !none(instruction.lhs) && none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    case Opcode::INSERT_ROW:
        return !none(instruction.dst) && !none(instruction.lhs) && none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    case Opcode::HALT:
        return none(instruction.dst) && none(instruction.lhs) && none(instruction.rhs) && none(instruction.aux) &&
               default_compare;
    }
    return false;
}

} // namespace

VerifyResult VerifyProgram(const CompiledProgram& program) {
    if (program.ir_version() != COMPILED_IR_VERSION || program.abi_version() != COMPILED_ABI_VERSION) {
        return Fail("unsupported compiled IR or ABI version");
    }
    if (!IsProgramKind(program.kind())) {
        return Fail("invalid compiled program kind");
    }
    if (program.parameters().size() > MAX_PROGRAM_PARAMETERS || program.registers().size() > MAX_PROGRAM_REGISTERS ||
        program.tuple_layouts().size() > MAX_PROGRAM_TUPLE_LAYOUTS || program.instructions().empty() ||
        program.instructions().size() > MAX_PROGRAM_INSTRUCTIONS) {
        return Fail("compiled program exceeds resource limits");
    }

    uint64_t frame_bytes = 0;
    for (const ParameterDesc& parameter : program.parameters()) {
        if (!IsScalar(parameter.type) ||
            (parameter.type == ValueType::BYTES &&
             (parameter.max_length == 0 || parameter.max_length > MAX_PROGRAM_VALUE_BYTES)) ||
            (parameter.type != ValueType::BYTES && parameter.max_length != 0)) {
            return Fail("invalid parameter descriptor");
        }
        if (parameter.type == ValueType::BYTES) {
            frame_bytes += parameter.max_length;
        }
    }
    for (const TupleLayout& layout : program.tuple_layouts()) {
        if (layout.byte_size == 0 || layout.byte_size > MAX_PROGRAM_VALUE_BYTES ||
            layout.columns.size() > MAX_PROGRAM_COLUMNS) {
            return Fail("invalid tuple layout resource size");
        }
        for (const ColumnDesc& column : layout.columns) {
            if (!ValidColumn(column, layout.byte_size)) {
                return Fail("tuple column is outside its fixed-width layout");
            }
        }
    }
    for (const RegisterDesc& reg : program.registers()) {
        if (!IsValueType(reg.type)) {
            return Fail("invalid register type");
        }
        if (reg.type == ValueType::TUPLE) {
            if (reg.tuple_layout >= program.tuple_layouts().size() || reg.max_length != 0) {
                return Fail("tuple register has an invalid layout");
            }
            frame_bytes += program.tuple_layouts()[reg.tuple_layout].byte_size;
        } else if (reg.tuple_layout != kNoOperand) {
            return Fail("non-tuple register references a tuple layout");
        } else if (reg.type == ValueType::BYTES) {
            if (reg.max_length == 0 || reg.max_length > MAX_PROGRAM_VALUE_BYTES) {
                return Fail("byte register has an invalid length");
            }
            frame_bytes += reg.max_length;
        } else if (reg.max_length != 0) {
            return Fail("non-byte register has a byte length");
        }
    }
    if (frame_bytes > MAX_PROGRAM_FRAME_BYTES) {
        return Fail("compiled program frame exceeds its aggregate byte limit");
    }
    if (program.output().columns.size() > MAX_PROGRAM_COLUMNS) {
        return Fail("output description exceeds resource limits");
    }
    for (const OutputColumnDesc& column : program.output().columns) {
        if (!IsScalar(column.type) || column.width == 0 || column.width > MAX_PROGRAM_VALUE_BYTES ||
            (column.type == ValueType::INT32 && column.width != sizeof(int32_t)) ||
            (column.type == ValueType::FLOAT64 && column.width != sizeof(double)) ||
            (column.type == ValueType::BOOL && column.width != sizeof(uint8_t))) {
            return Fail("invalid output column descriptor");
        }
    }

    auto type_of = [&](uint32_t index, ValueType expected, const char* message) -> VerifyResult {
        if (index >= program.registers().size() || program.registers()[index].type != expected) {
            return Fail(message);
        }
        return {true, {}};
    };
    size_t halt_count = 0;
    for (size_t pc = 0; pc < program.instructions().size(); ++pc) {
        const Instruction& instruction = program.instructions()[pc];
        if (!HasCanonicalOperands(instruction)) {
            return Fail("instruction does not use its canonical operand shape");
        }
        VerifyResult result{true, {}};
        switch (instruction.opcode) {
        case Opcode::LOAD_PARAM:
            if (instruction.aux >= program.parameters().size() || instruction.dst >= program.registers().size() ||
                program.registers()[instruction.dst].type != program.parameters()[instruction.aux].type ||
                (program.parameters()[instruction.aux].type == ValueType::BYTES &&
                 program.registers()[instruction.dst].max_length < program.parameters()[instruction.aux].max_length)) {
                return Fail("LoadParam has an invalid parameter or destination register");
            }
            break;
        case Opcode::MAKE_POINT_KEY:
            result = type_of(instruction.dst, ValueType::POINT_KEY, "MakePointKey requires a point-key destination");
            if (!result)
                return result;
            result = type_of(instruction.lhs, ValueType::TUPLE, "MakePointKey requires a key-tuple source");
            if (!result) {
                return Fail("MakePointKey requires a key-tuple source");
            }
            break;
        case Opcode::POINT_LOOKUP:
            if (!type_of(instruction.dst, ValueType::TUPLE, "PointLookup requires a tuple destination") ||
                !type_of(instruction.lhs, ValueType::POINT_KEY, "PointLookup requires a point key") ||
                !type_of(instruction.rhs, ValueType::ROW_HANDLE, "PointLookup requires a row-handle destination")) {
                return Fail("PointLookup has invalid register types");
            }
            break;
        case Opcode::COPY_TUPLE:
            if (!type_of(instruction.dst, ValueType::TUPLE, "CopyTuple requires tuple registers") ||
                !type_of(instruction.lhs, ValueType::TUPLE, "CopyTuple requires tuple registers") ||
                program.registers()[instruction.dst].tuple_layout !=
                    program.registers()[instruction.lhs].tuple_layout) {
                return Fail("CopyTuple requires matching tuple layouts");
            }
            break;
        case Opcode::LOAD_COLUMN: {
            if (!type_of(instruction.lhs, ValueType::TUPLE, "LoadColumn requires a tuple source")) {
                return Fail("LoadColumn requires a tuple source");
            }
            const RegisterDesc& tuple = program.registers()[instruction.lhs];
            const TupleLayout& layout = program.tuple_layouts()[tuple.tuple_layout];
            if (instruction.aux >= layout.columns.size() || instruction.dst >= program.registers().size() ||
                program.registers()[instruction.dst].type != layout.columns[instruction.aux].type ||
                (layout.columns[instruction.aux].type == ValueType::BYTES &&
                 program.registers()[instruction.dst].max_length < layout.columns[instruction.aux].width)) {
                return Fail("LoadColumn has an invalid column or destination register");
            }
            break;
        }
        case Opcode::STORE_COLUMN: {
            if (!type_of(instruction.dst, ValueType::TUPLE, "StoreColumn requires a tuple destination") ||
                instruction.lhs >= program.registers().size()) {
                return Fail("StoreColumn has an invalid register");
            }
            const TupleLayout& layout = program.tuple_layouts()[program.registers()[instruction.dst].tuple_layout];
            if (instruction.aux >= layout.columns.size() ||
                !CanStore(layout.columns[instruction.aux].type, program.registers()[instruction.lhs].type)) {
                return Fail("StoreColumn has an incompatible column or source type");
            }
            break;
        }
        case Opcode::ADD:
        case Opcode::SUB:
        case Opcode::MUL:
        case Opcode::DIV:
            if (instruction.dst >= program.registers().size() || instruction.lhs >= program.registers().size() ||
                instruction.rhs >= program.registers().size() ||
                !IsNumeric(program.registers()[instruction.dst].type) ||
                !IsNumeric(program.registers()[instruction.lhs].type) ||
                !IsNumeric(program.registers()[instruction.rhs].type) ||
                (program.registers()[instruction.dst].type == ValueType::INT32 &&
                 (program.registers()[instruction.lhs].type != ValueType::INT32 ||
                  program.registers()[instruction.rhs].type != ValueType::INT32))) {
                return Fail("arithmetic instruction has incompatible register types");
            }
            break;
        case Opcode::COMPARE:
            if (!type_of(instruction.dst, ValueType::BOOL, "Compare requires a boolean destination") ||
                instruction.lhs >= program.registers().size() || instruction.rhs >= program.registers().size() ||
                !IsCompareOp(instruction.compare_op) ||
                !CanCompare(program.registers()[instruction.lhs].type, program.registers()[instruction.rhs].type,
                            instruction.compare_op)) {
                return Fail("Compare has incompatible register types");
            }
            break;
        case Opcode::JUMP:
            if (instruction.aux >= program.instructions().size()) {
                return Fail("Jump target is outside the instruction stream");
            }
            break;
        case Opcode::JUMP_IF_FALSE:
            if (!type_of(instruction.lhs, ValueType::BOOL, "JumpIfFalse requires a boolean condition") ||
                instruction.aux >= program.instructions().size()) {
                return Fail("JumpIfFalse has an invalid condition or target");
            }
            break;
        case Opcode::PREPARE_UPDATE:
            if (!type_of(instruction.dst, ValueType::PREPARED_UPDATE,
                         "PrepareUpdate requires a prepared-update destination") ||
                !type_of(instruction.lhs, ValueType::ROW_HANDLE, "PrepareUpdate requires a row handle") ||
                !type_of(instruction.rhs, ValueType::TUPLE, "PrepareUpdate requires a tuple")) {
                return Fail("PrepareUpdate has invalid register types");
            }
            break;
        case Opcode::COMMIT_UPDATE:
            if (!type_of(instruction.lhs, ValueType::PREPARED_UPDATE,
                         "CommitUpdate requires a prepared-update value") ||
                !type_of(instruction.rhs, ValueType::TUPLE, "CommitUpdate requires a proposed tuple")) {
                return Fail("CommitUpdate has invalid register types");
            }
            break;
        case Opcode::DELETE_ROW:
            result = type_of(instruction.lhs, ValueType::ROW_HANDLE, "DeleteRow requires a row handle");
            if (!result)
                return result;
            break;
        case Opcode::INSERT_ROW:
            if (!type_of(instruction.dst, ValueType::ROW_HANDLE, "InsertRow requires a row-handle destination") ||
                !type_of(instruction.lhs, ValueType::TUPLE, "InsertRow requires a tuple")) {
                return Fail("InsertRow has invalid register types");
            }
            break;
        case Opcode::EMIT_ROW:
            result = type_of(instruction.lhs, ValueType::TUPLE, "EmitRow requires a tuple");
            if (!result)
                return result;
            break;
        case Opcode::HALT:
            ++halt_count;
            if (pc + 1 != program.instructions().size()) {
                return Fail("Halt must be the final instruction");
            }
            break;
        default:
            return Fail("program contains an unknown opcode");
        }
    }
    if (halt_count != 1) {
        return Fail("program must contain exactly one final Halt");
    }

    const size_t instruction_count = program.instructions().size();
    const size_t register_count = program.registers().size();
    std::vector<bool> reachable(instruction_count, false);
    std::vector<std::vector<bool>> definitely_assigned(instruction_count, std::vector<bool>(register_count, false));
    for (size_t reg = 0; reg < register_count; ++reg) {
        // Tuple registers are fixed-size zero-initialized storage allocated with the frame.
        definitely_assigned[0][reg] = program.registers()[reg].type == ValueType::TUPLE;
    }
    std::queue<size_t> work;
    reachable[0] = true;
    work.push(0);
    while (!work.empty()) {
        const size_t pc = work.front();
        work.pop();
        const Instruction& instruction = program.instructions()[pc];
        std::vector<bool> output = definitely_assigned[pc];
        switch (instruction.opcode) {
        case Opcode::LOAD_PARAM:
        case Opcode::MAKE_POINT_KEY:
        case Opcode::COPY_TUPLE:
        case Opcode::LOAD_COLUMN:
        case Opcode::ADD:
        case Opcode::SUB:
        case Opcode::MUL:
        case Opcode::DIV:
        case Opcode::COMPARE:
        case Opcode::INSERT_ROW:
            output[instruction.dst] = true;
            break;
        case Opcode::PREPARE_UPDATE:
            output[instruction.dst] = true;
            output[instruction.rhs] = true;
            break;
        case Opcode::POINT_LOOKUP:
            output[instruction.dst] = true;
            output[instruction.rhs] = true;
            break;
        default:
            break;
        }
        auto add = [&](size_t target) {
            if (!reachable[target]) {
                reachable[target] = true;
                definitely_assigned[target] = output;
                work.push(target);
                return;
            }
            std::vector<bool> merged = definitely_assigned[target];
            bool changed = false;
            for (size_t reg = 0; reg < register_count; ++reg) {
                const bool value = merged[reg] && output[reg];
                changed = changed || value != merged[reg];
                merged[reg] = value;
            }
            if (changed) {
                definitely_assigned[target] = std::move(merged);
                work.push(target);
            }
        };
        if (instruction.opcode == Opcode::HALT) {
            continue;
        }
        if (instruction.opcode == Opcode::JUMP) {
            add(instruction.aux);
            continue;
        }
        if (instruction.opcode == Opcode::JUMP_IF_FALSE) {
            add(instruction.aux);
        }
        add(pc + 1);
    }

    auto require_assigned = [&](size_t pc, uint32_t reg, const char* message) -> VerifyResult {
        return definitely_assigned[pc][reg] ? VerifyResult{true, {}} : Fail(message);
    };
    for (size_t pc = 0; pc < instruction_count; ++pc) {
        if (!reachable[pc]) {
            continue;
        }
        const Instruction& instruction = program.instructions()[pc];
        switch (instruction.opcode) {
        case Opcode::MAKE_POINT_KEY:
        case Opcode::COPY_TUPLE:
        case Opcode::LOAD_COLUMN:
        case Opcode::DELETE_ROW:
        case Opcode::INSERT_ROW:
        case Opcode::EMIT_ROW:
            if (!require_assigned(pc, instruction.lhs, "instruction reads an uninitialized register")) {
                return Fail("instruction reads an uninitialized register");
            }
            break;
        case Opcode::STORE_COLUMN:
            if (!require_assigned(pc, instruction.dst, "StoreColumn writes an uninitialized tuple") ||
                !require_assigned(pc, instruction.lhs, "StoreColumn reads an uninitialized value")) {
                return Fail("StoreColumn uses an uninitialized register");
            }
            break;
        case Opcode::POINT_LOOKUP:
            if (!require_assigned(pc, instruction.lhs, "PointLookup reads an uninitialized key")) {
                return Fail("PointLookup reads an uninitialized key");
            }
            break;
        case Opcode::ADD:
        case Opcode::SUB:
        case Opcode::MUL:
        case Opcode::DIV:
        case Opcode::COMPARE:
        case Opcode::PREPARE_UPDATE:
        case Opcode::COMMIT_UPDATE:
            if (!require_assigned(pc, instruction.lhs, "instruction reads an uninitialized left operand") ||
                !require_assigned(pc, instruction.rhs, "instruction reads an uninitialized right operand")) {
                return Fail("instruction reads an uninitialized operand");
            }
            break;
        case Opcode::JUMP_IF_FALSE:
            if (!require_assigned(pc, instruction.lhs, "JumpIfFalse reads an uninitialized condition")) {
                return Fail("JumpIfFalse reads an uninitialized condition");
            }
            break;
        default:
            break;
        }
    }
    return {true, {}};
}

} // namespace compiled
