/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>

#include "compiled/compiled_program.h"
#include "compiled/parameter_frame.h"
#include "compiled/program_runtime.h"

namespace compiled {

inline constexpr uint64_t DEFAULT_INTERPRETER_STEP_LIMIT = 1000000;

ExecStatus Interpret(const CompiledProgram& program, const ParameterFrame& parameters, ProgramRuntime* runtime,
                     uint64_t step_limit = DEFAULT_INTERPRETER_STEP_LIMIT) noexcept;

} // namespace compiled
