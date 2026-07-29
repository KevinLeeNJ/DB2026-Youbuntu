/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "jit/jit_types.h"

#include <utility>

#include "jit/jit_runtime_internal.h"

namespace jit {

namespace {

bool host_supports_x86_64() {
#if defined(__x86_64__) || defined(_M_X64)
    return true;
#else
    return false;
#endif
}

} // namespace

JitCode::JitCode(std::shared_ptr<JitRuntimeImpl> runtime, void* function, std::size_t code_size, Kind kind,
                 std::shared_ptr<const compiled::CompiledProgram> program_owner)
    : runtime_(std::move(runtime)), function_(function), code_size_(code_size), kind_(kind),
      program_owner_(std::move(program_owner)) {}

JitCode::~JitCode() {
    reset();
}

JitCode::JitCode(JitCode&& other) noexcept
    : runtime_(std::move(other.runtime_)), function_(other.function_), code_size_(other.code_size_),
      kind_(other.kind_), program_owner_(std::move(other.program_owner_)) {
    other.function_ = nullptr;
    other.code_size_ = 0;
}

JitCode& JitCode::operator=(JitCode&& other) noexcept {
    if (this != &other) {
        reset();
        runtime_ = std::move(other.runtime_);
        function_ = other.function_;
        code_size_ = other.code_size_;
        kind_ = other.kind_;
        program_owner_ = std::move(other.program_owner_);
        other.function_ = nullptr;
        other.code_size_ = 0;
    }
    return *this;
}

int32_t JitCode::test_add_i32(int32_t, int32_t) const {
    return 0;
}

JitStatus JitCode::invoke_predicate(JitCallFrame*) const {
    return JitStatus::INVALID_INPUT;
}

compiled::ExecStatus JitCode::invoke_program(compiled::ProgramRuntime* runtime,
                                             const compiled::ParameterFrame* parameters) const noexcept {
    if (kind_ != Kind::PROGRAM || function_ == nullptr) {
        return compiled::ExecStatus::ERROR;
    }
    return reinterpret_cast<NativeEntry>(function_)(runtime, parameters);
}

void JitCode::reset() {
    if (function_ != nullptr) {
        runtime_->runtime.release(function_);
        runtime_->active_code_count.fetch_sub(1, std::memory_order_relaxed);
        function_ = nullptr;
        code_size_ = 0;
    }
    program_owner_.reset();
    runtime_.reset();
}

JitRuntime::JitRuntime(JitRuntimeOptions options) : runtime_(std::make_shared<JitRuntimeImpl>()) {
    supported_ = !options.force_unsupported_architecture && host_supports_x86_64();
}

bool JitRuntime::is_supported() const {
    return supported_;
}

std::size_t JitRuntime::active_code_count() const {
    return runtime_->active_code_count.load(std::memory_order_relaxed);
}

JitCompileResult JitRuntime::compile_test_add_i32(JitCompileOptions) {
    return {JitStatus::COMPILE_ERROR, {}, "test JIT entry is not built"};
}

JitCompileResult JitRuntime::compile_predicate(const JitProgram&, JitCompileOptions) {
    return {JitStatus::COMPILE_ERROR, {}, "legacy predicate JIT entry is not built"};
}

} // namespace jit
