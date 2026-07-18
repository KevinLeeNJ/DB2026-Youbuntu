/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstddef>
#include <string>

#include "compiled/compiled_program.h"

namespace compiled {

inline constexpr size_t MAX_PROGRAM_PARAMETERS = 256;
inline constexpr size_t MAX_PROGRAM_REGISTERS = 1024;
inline constexpr size_t MAX_PROGRAM_TUPLE_LAYOUTS = 64;
inline constexpr size_t MAX_PROGRAM_COLUMNS = 256;
inline constexpr size_t MAX_PROGRAM_INSTRUCTIONS = 4096;
inline constexpr uint32_t MAX_PROGRAM_VALUE_BYTES = 1024 * 1024;
inline constexpr uint64_t MAX_PROGRAM_FRAME_BYTES = 16ULL * 1024 * 1024;

struct VerifyResult {
    bool valid{false};
    std::string error;

    explicit operator bool() const {
        return valid;
    }
};

VerifyResult VerifyProgram(const CompiledProgram& program);

} // namespace compiled
