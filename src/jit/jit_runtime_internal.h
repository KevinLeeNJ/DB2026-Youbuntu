/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <atomic>

#include <asmjit/core.h>

namespace jit {

struct JitRuntimeImpl {
    asmjit::JitRuntime runtime;
    std::atomic<size_t> active_code_count{0};
};

} // namespace jit
