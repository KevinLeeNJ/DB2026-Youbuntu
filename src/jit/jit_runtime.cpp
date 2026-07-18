/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "jit/jit_types.h"

#include <atomic>
#include <utility>

#include <asmjit/core.h>
#include <asmjit/x86.h>

namespace jit {

struct JitRuntimeImpl {
    asmjit::JitRuntime runtime;
    std::atomic<size_t> active_code_count{0};
};

namespace {

class CapturingErrorHandler final : public asmjit::ErrorHandler {
public:
    void handle_error(asmjit::Error error, const char* message, asmjit::BaseEmitter*) override {
        error_ = error;
        if (message != nullptr) {
            message_ = message;
        }
    }

    std::string description(asmjit::Error fallback) const {
        if (!message_.empty()) {
            return message_;
        }
        const asmjit::Error error = error_ == asmjit::Error::kOk ? fallback : error_;
        return asmjit::DebugUtils::error_as_string(error);
    }

private:
    asmjit::Error error_{asmjit::Error::kOk};
    std::string message_;
};

bool host_supports_x86_64() {
#if defined(__x86_64__) || defined(_M_X64)
    return true;
#else
    return false;
#endif
}

} // namespace

JitCode::JitCode(std::shared_ptr<JitRuntimeImpl> runtime, TestAddI32Fn function, size_t code_size)
    : runtime_(std::move(runtime)), function_(function), code_size_(code_size) {}

JitCode::~JitCode() {
    reset();
}

JitCode::JitCode(JitCode&& other) noexcept
    : runtime_(std::move(other.runtime_)), function_(other.function_), code_size_(other.code_size_) {
    other.function_ = nullptr;
    other.code_size_ = 0;
}

JitCode& JitCode::operator=(JitCode&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    reset();
    runtime_ = std::move(other.runtime_);
    function_ = other.function_;
    code_size_ = other.code_size_;
    other.function_ = nullptr;
    other.code_size_ = 0;
    return *this;
}

int32_t JitCode::test_add_i32(int32_t lhs, int32_t rhs) const {
    return function_(lhs, rhs);
}

void JitCode::reset() {
    if (function_ != nullptr) {
        runtime_->runtime.release(function_);
        runtime_->active_code_count.fetch_sub(1, std::memory_order_relaxed);
        function_ = nullptr;
        code_size_ = 0;
    }
    runtime_.reset();
}

JitRuntime::JitRuntime(JitRuntimeOptions options) : runtime_(std::make_shared<JitRuntimeImpl>()) {
    supported_ = !options.force_unsupported_architecture && host_supports_x86_64();
}

bool JitRuntime::is_supported() const {
    return supported_;
}

size_t JitRuntime::active_code_count() const {
    return runtime_->active_code_count.load(std::memory_order_relaxed);
}

JitCompileResult JitRuntime::compile_test_add_i32(JitCompileOptions options) {
    if (!supported_) {
        return {JitStatus::UNSUPPORTED_ARCHITECTURE, {}, "AsmJit x86-64 backend is unavailable on this host"};
    }
    if (options.force_compile_failure) {
        return {JitStatus::COMPILE_ERROR, {}, "forced compile failure"};
    }
    if (options.force_allocation_failure) {
        return {JitStatus::ALLOCATION_ERROR, {}, "forced executable memory allocation failure"};
    }

    asmjit::CodeHolder code;
    CapturingErrorHandler error_handler;
    code.init(runtime_->runtime.environment(), runtime_->runtime.cpu_features());
    code.set_error_handler(&error_handler);

    asmjit::x86::Compiler compiler(&code);
    asmjit::FuncNode* function = compiler.add_func(asmjit::FuncSignature::build<int32_t, int32_t, int32_t>());
    asmjit::x86::Gp lhs = compiler.new_gp32("lhs");
    asmjit::x86::Gp rhs = compiler.new_gp32("rhs");
    function->set_arg(0, lhs);
    function->set_arg(1, rhs);
    compiler.add(lhs, rhs);
    compiler.ret(lhs);
    compiler.end_func();

    asmjit::Error error = compiler.finalize();
    if (error != asmjit::Error::kOk) {
        return {JitStatus::COMPILE_ERROR, {}, error_handler.description(error)};
    }

    JitCode::TestAddI32Fn generated = nullptr;
    error = runtime_->runtime.add(&generated, &code);
    if (error != asmjit::Error::kOk) {
        return {JitStatus::ALLOCATION_ERROR, {}, error_handler.description(error)};
    }

    runtime_->active_code_count.fetch_add(1, std::memory_order_relaxed);
    return {JitStatus::OK, JitCode(runtime_, generated, code.code_size()), {}};
}

} // namespace jit
