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
#include <cstring>
#include <string_view>
#include <utility>

#include <asmjit/core.h>
#include <asmjit/x86.h>

#include "jit/jit_ir.h"

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

extern "C" int jit_compare_bytes(uint32_t operation, const char* lhs, uint32_t lhs_len, uint32_t lhs_trimmed,
                                 const char* rhs, uint32_t rhs_len, uint32_t rhs_trimmed) {
    if ((lhs == nullptr && lhs_len != 0) || (rhs == nullptr && rhs_len != 0)) {
        return 0;
    }
    if (lhs_trimmed != 0) {
        const void* terminator = std::memchr(lhs, '\0', lhs_len);
        lhs_len = terminator == nullptr ? lhs_len : static_cast<uint32_t>(static_cast<const char*>(terminator) - lhs);
    }
    if (rhs_trimmed != 0) {
        const void* terminator = std::memchr(rhs, '\0', rhs_len);
        rhs_len = terminator == nullptr ? rhs_len : static_cast<uint32_t>(static_cast<const char*>(terminator) - rhs);
    }
    const uint32_t common = std::min(lhs_len, rhs_len);
    int comparison = common == 0 ? 0 : std::memcmp(lhs, rhs, common);
    if (comparison == 0) {
        comparison = lhs_len < rhs_len ? -1 : lhs_len > rhs_len ? 1 : 0;
    }
    switch (static_cast<CompOp>(operation)) {
    case OP_EQ:
        return comparison == 0;
    case OP_NE:
        return comparison != 0;
    case OP_LT:
        return comparison < 0;
    case OP_GT:
        return comparison > 0;
    case OP_LE:
        return comparison <= 0;
    case OP_GE:
        return comparison >= 0;
    }
    return 0;
}

bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

void emit_numeric_operand(asmjit::x86::Compiler& compiler, const JitOperand& operand, const asmjit::x86::Gp& frame,
                          const asmjit::x86::Vec& target) {
    using namespace asmjit;
    using namespace asmjit::x86;
    Gp base = compiler.new_gp_ptr();
    if (operand.source == JitOperandSource::PARAMETER) {
        compiler.mov(base, ptr(frame, static_cast<int32_t>(offsetof(JitCallFrame, params))));
        const int32_t offset = static_cast<int32_t>(operand.parameter_index * sizeof(JitValue));
        if (operand.type == TYPE_INT) {
            Gp value = compiler.new_gp32();
            compiler.mov(value, dword_ptr(base, offset + static_cast<int32_t>(offsetof(JitValue, int_value))));
            compiler.cvtsi2sd(target, value);
        } else {
            compiler.movsd(target, ptr(base, offset + static_cast<int32_t>(offsetof(JitValue, float_value))));
        }
        return;
    }
    const int32_t tuple_offset = operand.source == JitOperandSource::TUPLE0
                                     ? static_cast<int32_t>(offsetof(JitCallFrame, tuple0))
                                     : static_cast<int32_t>(offsetof(JitCallFrame, tuple1));
    compiler.mov(base, ptr(frame, tuple_offset));
    if (operand.type == TYPE_INT) {
        Gp value = compiler.new_gp32();
        compiler.mov(value, dword_ptr(base, static_cast<int32_t>(operand.offset)));
        compiler.cvtsi2sd(target, value);
    } else {
        compiler.movsd(target, ptr(base, static_cast<int32_t>(operand.offset)));
    }
}

void emit_bytes_operand(asmjit::x86::Compiler& compiler, const JitOperand& operand, const asmjit::x86::Gp& frame,
                        const asmjit::x86::Gp& data, const asmjit::x86::Gp& len, uint32_t* trimmed) {
    using namespace asmjit;
    using namespace asmjit::x86;
    if (operand.source == JitOperandSource::PARAMETER) {
        Gp base = compiler.new_gp_ptr();
        compiler.mov(base, ptr(frame, static_cast<int32_t>(offsetof(JitCallFrame, params))));
        const int32_t offset = static_cast<int32_t>(operand.parameter_index * sizeof(JitValue));
        compiler.mov(data, ptr(base, offset + static_cast<int32_t>(offsetof(JitValue, bytes))));
        compiler.mov(len, dword_ptr(base, offset + static_cast<int32_t>(offsetof(JitValue, bytes_len))));
        *trimmed = 0;
        return;
    }
    const int32_t tuple_offset = operand.source == JitOperandSource::TUPLE0
                                     ? static_cast<int32_t>(offsetof(JitCallFrame, tuple0))
                                     : static_cast<int32_t>(offsetof(JitCallFrame, tuple1));
    compiler.mov(data, ptr(frame, tuple_offset));
    compiler.add(data, imm(operand.offset));
    compiler.mov(len, imm(operand.len));
    *trimmed = 1;
}

void emit_numeric_false_branch(asmjit::x86::Compiler& compiler, CompOp operation, const asmjit::Label& no_match) {
    using asmjit::x86::CondCode;
    if (operation == OP_NE) {
        const asmjit::Label unordered_match = compiler.new_label();
        compiler.j(CondCode::kPE, unordered_match);
        compiler.j(CondCode::kE, no_match);
        compiler.bind(unordered_match);
        return;
    }
    compiler.j(CondCode::kPE, no_match);
    switch (operation) {
    case OP_EQ:
        compiler.j(CondCode::kNE, no_match);
        return;
    case OP_NE:
        return;
    case OP_LT:
        compiler.j(CondCode::kAE, no_match);
        return;
    case OP_GT:
        compiler.j(CondCode::kBE, no_match);
        return;
    case OP_LE:
        compiler.j(CondCode::kA, no_match);
        return;
    case OP_GE:
        compiler.j(CondCode::kB, no_match);
        return;
    }
}

} // namespace

JitCode::JitCode(std::shared_ptr<JitRuntimeImpl> runtime, void* function, size_t code_size, Kind kind)
    : runtime_(std::move(runtime)), function_(function), code_size_(code_size), kind_(kind) {}

JitCode::~JitCode() {
    reset();
}

JitCode::JitCode(JitCode&& other) noexcept
    : runtime_(std::move(other.runtime_)), function_(other.function_), code_size_(other.code_size_),
      kind_(other.kind_) {
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
    kind_ = other.kind_;
    other.function_ = nullptr;
    other.code_size_ = 0;
    return *this;
}

int32_t JitCode::test_add_i32(int32_t lhs, int32_t rhs) const {
    return kind_ == Kind::TEST_ADD_I32 ? reinterpret_cast<TestAddI32Fn>(function_)(lhs, rhs) : 0;
}

JitStatus JitCode::invoke_predicate(JitCallFrame* frame) const {
    return kind_ == Kind::PREDICATE ? reinterpret_cast<PredicateFn>(function_)(frame) : JitStatus::INVALID_INPUT;
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
    return {JitStatus::OK,
            JitCode(runtime_, reinterpret_cast<void*>(generated), code.code_size(), JitCode::Kind::TEST_ADD_I32),
            {}};
}

JitCompileResult JitRuntime::compile_predicate(const JitProgram& program, JitCompileOptions options) {
    if (!supported_) {
        return {JitStatus::UNSUPPORTED_ARCHITECTURE, {}, "AsmJit x86-64 backend is unavailable on this host"};
    }
    if (options.force_compile_failure) {
        return {JitStatus::COMPILE_ERROR, {}, "forced compile failure"};
    }
    if (options.force_allocation_failure) {
        return {JitStatus::ALLOCATION_ERROR, {}, "forced executable memory allocation failure"};
    }
    if (!verify_program(program)) {
        return {JitStatus::COMPILE_ERROR, {}, "predicate IR verification failed"};
    }

    asmjit::CodeHolder code;
    CapturingErrorHandler error_handler;
    code.init(runtime_->runtime.environment(), runtime_->runtime.cpu_features());
    code.set_error_handler(&error_handler);
    asmjit::x86::Compiler compiler(&code);
    auto* function = compiler.add_func(asmjit::FuncSignature::build<int, JitCallFrame*>());
    asmjit::x86::Gp frame = compiler.new_gp_ptr("frame");
    asmjit::x86::Gp result = compiler.new_gp32("result");
    function->set_arg(0, frame);
    const asmjit::Label no_match = compiler.new_label();
    const asmjit::Label complete = compiler.new_label();

    for (const auto& predicate : program.predicates) {
        if (is_numeric_type(predicate.lhs.type) && is_numeric_type(predicate.rhs.type)) {
            asmjit::x86::Vec lhs = compiler.new_xmm_sd();
            asmjit::x86::Vec rhs = compiler.new_xmm_sd();
            emit_numeric_operand(compiler, predicate.lhs, frame, lhs);
            emit_numeric_operand(compiler, predicate.rhs, frame, rhs);
            compiler.ucomisd(lhs, rhs);
            emit_numeric_false_branch(compiler, predicate.op, no_match);
            continue;
        }

        asmjit::x86::Gp lhs_data = compiler.new_gp_ptr();
        asmjit::x86::Gp lhs_len = compiler.new_gp32();
        asmjit::x86::Gp rhs_data = compiler.new_gp_ptr();
        asmjit::x86::Gp rhs_len = compiler.new_gp32();
        uint32_t lhs_trimmed = 0;
        uint32_t rhs_trimmed = 0;
        emit_bytes_operand(compiler, predicate.lhs, frame, lhs_data, lhs_len, &lhs_trimmed);
        emit_bytes_operand(compiler, predicate.rhs, frame, rhs_data, rhs_len, &rhs_trimmed);
        asmjit::InvokeNode* invoke = nullptr;
        compiler.invoke(asmjit::Out(invoke), asmjit::imm(reinterpret_cast<void*>(jit_compare_bytes)),
                        asmjit::FuncSignature::build<int, uint32_t, const char*, uint32_t, uint32_t, const char*,
                                                     uint32_t, uint32_t>());
        invoke->set_arg(0, asmjit::imm(static_cast<uint32_t>(predicate.op)));
        invoke->set_arg(1, lhs_data);
        invoke->set_arg(2, lhs_len);
        invoke->set_arg(3, asmjit::imm(lhs_trimmed));
        invoke->set_arg(4, rhs_data);
        invoke->set_arg(5, rhs_len);
        invoke->set_arg(6, asmjit::imm(rhs_trimmed));
        invoke->set_ret(0, result);
        compiler.test(result, result);
        compiler.jz(no_match);
    }
    compiler.mov(asmjit::x86::byte_ptr(frame, static_cast<int32_t>(offsetof(JitCallFrame, match))), 1);
    compiler.mov(result, static_cast<int>(JitStatus::OK));
    compiler.jmp(complete);
    compiler.bind(no_match);
    compiler.mov(asmjit::x86::byte_ptr(frame, static_cast<int32_t>(offsetof(JitCallFrame, match))), 0);
    compiler.mov(result, static_cast<int>(JitStatus::OK));
    compiler.bind(complete);
    compiler.ret(result);
    compiler.end_func();

    asmjit::Error error = compiler.finalize();
    if (error != asmjit::Error::kOk) {
        return {JitStatus::COMPILE_ERROR, {}, error_handler.description(error)};
    }
    using PredicateFn = JitStatus (*)(JitCallFrame*);
    PredicateFn generated = nullptr;
    error = runtime_->runtime.add(&generated, &code);
    if (error != asmjit::Error::kOk) {
        return {JitStatus::ALLOCATION_ERROR, {}, error_handler.description(error)};
    }
    runtime_->active_code_count.fetch_add(1, std::memory_order_relaxed);
    return {JitStatus::OK,
            JitCode(runtime_, reinterpret_cast<void*>(generated), code.code_size(), JitCode::Kind::PREDICATE),
            {}};
}

} // namespace jit
