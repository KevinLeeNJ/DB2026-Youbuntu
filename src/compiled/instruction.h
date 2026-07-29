/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <limits>

namespace compiled {

inline constexpr uint32_t kNoOperand = std::numeric_limits<uint32_t>::max();

enum class ValueType : uint8_t {
    INT32,
    FLOAT64,
    BOOL,
    BYTES,
    TUPLE,
    POINT_KEY,
    ROW_HANDLE,
    PREPARED_UPDATE,
};

enum class CompareOp : uint8_t { EQ, NE, LT, GT, LE, GE };

enum class Opcode : uint8_t {
    LOAD_PARAM,
    MAKE_POINT_KEY,
    POINT_LOOKUP,
    COPY_TUPLE,
    LOAD_COLUMN,
    STORE_COLUMN,
    ADD,
    SUB,
    MUL,
    DIV,
    COMPARE,
    JUMP,
    JUMP_IF_FALSE,
    PREPARE_UPDATE,
    COMMIT_UPDATE,
    DELETE_ROW,
    INSERT_ROW,
    EMIT_ROW,
    HALT,
};

// Operands are register indices unless the opcode documents them as immediates.
// aux is a parameter/column/index id or jump target, depending on the opcode.
struct Instruction {
    Opcode opcode{Opcode::HALT};
    uint32_t dst{kNoOperand};
    uint32_t lhs{kNoOperand};
    uint32_t rhs{kNoOperand};
    uint32_t aux{kNoOperand};
    CompareOp compare_op{CompareOp::EQ};
};

} // namespace compiled
