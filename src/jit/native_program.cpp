/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "jit/jit_types.h"

#include <cstddef>
#include <limits>
#include <type_traits>

#include <asmjit/core.h>
#include <asmjit/x86.h>

#include "compiled/program_verifier.h"
#include "jit/native_abi.h"
#include "jit/jit_runtime_internal.h"

namespace jit {
namespace {

class NativeErrorHandler final : public asmjit::ErrorHandler {
public:
    void handle_error(asmjit::Error error, const char* message, asmjit::BaseEmitter*) override {
        error_ = error;
        if (message != nullptr)
            message_ = message;
    }
    std::string description(asmjit::Error fallback) const {
        return message_.empty() ? asmjit::DebugUtils::error_as_string(error_ == asmjit::Error::kOk ? fallback : error_)
                                : message_;
    }

private:
    asmjit::Error error_{asmjit::Error::kOk};
    std::string message_;
};

bool HostSupportsNativeProgram() {
#if defined(__x86_64__) || defined(_M_X64)
    return true;
#else
    return false;
#endif
}

constexpr int32_t RegOffset(uint32_t index, size_t member) {
    return static_cast<int32_t>(index * sizeof(native::NativeSlot) + member);
}

constexpr int32_t SlotOffset(uint32_t index, size_t member) {
    return static_cast<int32_t>(index * sizeof(compiled::ParameterSlot) + member);
}

template <typename Fn, typename... Args> asmjit::InvokeNode* Invoke(asmjit::x86::Compiler& cc, Fn fn, Args&&... args) {
    asmjit::InvokeNode* invoke = nullptr;
    cc.invoke(asmjit::Out(invoke), asmjit::imm(reinterpret_cast<void*>(fn)), asmjit::FuncSignature::build<Args...>());
    return invoke;
}

} // namespace

JitCompileResult JitRuntime::compile_program(const compiled::CompiledProgram& program, JitCompileOptions options) {
    using namespace asmjit;
    using namespace asmjit::x86;
    static_assert(std::is_standard_layout_v<native::NativeSlot>);
    static_assert(std::is_standard_layout_v<compiled::ParameterSlot>);

    if (!supported_ || !HostSupportsNativeProgram()) {
        return {JitStatus::UNSUPPORTED_ARCHITECTURE, {}, "AsmJit compiled-program backend is unavailable on this host"};
    }
    if (options.force_compile_failure)
        return {JitStatus::COMPILE_ERROR, {}, "forced compile failure"};
    if (options.force_allocation_failure)
        return {JitStatus::ALLOCATION_ERROR, {}, "forced executable memory allocation failure"};
    const auto verified = compiled::VerifyProgram(program);
    if (!verified)
        return {JitStatus::COMPILE_ERROR, {}, verified.error};

    CodeHolder code;
    NativeErrorHandler errors;
    code.init(runtime_->runtime.environment(), runtime_->runtime.cpu_features());
    code.set_error_handler(&errors);
    Compiler cc(&code);
    auto* function = cc.add_func(
        FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const compiled::ParameterFrame*>());
    Gp runtime = cc.new_gp_ptr("runtime");
    Gp parameters = cc.new_gp_ptr("parameters");
    function->set_arg(0, runtime);
    function->set_arg(1, parameters);
    Gp frame = cc.new_gp_ptr("frame");
    Gp regs = cc.new_gp_ptr("registers");
    Gp slots = cc.new_gp_ptr("parameters_data");
    Gp status = cc.new_gp32("status");
    Gp steps = cc.new_gp64("steps");
    const Label cleanup = cc.new_label();
    const Label frame_failed = cc.new_label();
    const Label step_failed = cc.new_label();
    const Label arithmetic_failed = cc.new_label();
    auto program_owner = std::make_shared<const compiled::CompiledProgram>(program);
    std::vector<Label> labels;
    labels.reserve(program.instructions().size());
    for (size_t i = 0; i < program.instructions().size(); ++i)
        labels.push_back(cc.new_label());

    auto invoke_create = [&] {
        InvokeNode* call = nullptr;
        cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::CreateFrame)),
                  FuncSignature::build<native::ExecutionFrame*, const compiled::CompiledProgram*,
                                       const compiled::ParameterFrame*, compiled::ProgramRuntime*>());
        call->set_arg(0, imm(reinterpret_cast<uint64_t>(program_owner.get())));
        call->set_arg(1, parameters);
        call->set_arg(2, runtime);
        call->set_ret(0, frame);
    };
    invoke_create();
    cc.test(frame, frame);
    cc.jz(frame_failed);
    cc.mov(regs, ptr(frame, static_cast<int32_t>(offsetof(native::ExecutionFrame, registers))));
    std::vector<Gp> register_addresses;
    register_addresses.reserve(program.registers().size());
    for (size_t index = 0; index < program.registers().size(); ++index) {
        Gp address = cc.new_gp_ptr();
        cc.lea(address, ptr(regs, RegOffset(static_cast<uint32_t>(index), 0)));
        register_addresses.push_back(address);
    }
    {
        InvokeNode* call = nullptr;
        cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::ParameterData)),
                  FuncSignature::build<const compiled::ParameterSlot*, const compiled::ParameterFrame*>());
        call->set_arg(0, parameters);
        call->set_ret(0, slots);
    }
    cc.mov(steps, compiled::DEFAULT_INTERPRETER_STEP_LIMIT);
    cc.jmp(labels[0]);

    auto reg_ptr = [&](uint32_t index) { return register_addresses[index]; };
    auto finish_helper = [&](InvokeNode* call) {
        call->set_ret(0, status);
        cc.cmp(status, static_cast<uint32_t>(compiled::ExecStatus::OK));
        cc.jne(cleanup);
    };
    auto fail = [&](const char* message) {
        InvokeNode* call = nullptr;
        cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::Fail)),
                  FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const char*>());
        call->set_arg(0, runtime);
        call->set_arg(1, imm(reinterpret_cast<uint64_t>(message)));
        call->set_ret(0, status);
        cc.jmp(cleanup);
    };
    auto tuple_data = [&](uint32_t index) {
        Gp result = cc.new_gp_ptr();
        cc.mov(result, ptr(regs, RegOffset(index, offsetof(native::NativeSlot, data))));
        return result;
    };
    auto mark_initialized = [&](uint32_t index) {
        cc.mov(byte_ptr(regs, RegOffset(index, offsetof(native::NativeSlot, initialized))), 1);
    };
    auto copy_fixed = [&](const Gp& dst, const Gp& src, uint32_t bytes) {
        uint32_t offset = 0;
        while (bytes - offset >= 8) {
            Gp temporary = cc.new_gp64();
            cc.mov(temporary, qword_ptr(src, offset));
            cc.mov(qword_ptr(dst, offset), temporary);
            offset += 8;
        }
        if (bytes - offset >= 4) {
            Gp temporary = cc.new_gp32();
            cc.mov(temporary, dword_ptr(src, offset));
            cc.mov(dword_ptr(dst, offset), temporary);
            offset += 4;
        }
        if (bytes - offset >= 2) {
            Gp temporary = cc.new_gp32();
            cc.movzx(temporary, word_ptr(src, offset));
            cc.mov(word_ptr(dst, offset), temporary.r16());
            offset += 2;
        }
        if (bytes != offset) {
            Gp temporary = cc.new_gp32();
            cc.movzx(temporary, byte_ptr(src, offset));
            cc.mov(byte_ptr(dst, offset), temporary.r8());
        }
    };

    for (size_t pc = 0; pc < program.instructions().size(); ++pc) {
        const auto& ins = program.instructions()[pc];
        cc.bind(labels[pc]);
        cc.test(steps, steps);
        cc.jz(step_failed);
        cc.dec(steps);
        switch (ins.opcode) {
        case compiled::Opcode::LOAD_PARAM: {
            const auto type = program.registers()[ins.dst].type;
            if (type == compiled::ValueType::BYTES) {
                Gp source = cc.new_gp_ptr();
                cc.lea(source, ptr(slots, SlotOffset(ins.aux, 0)));
                InvokeNode* call = nullptr;
                cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::LoadBytesParameter)),
                          FuncSignature::build<compiled::ExecStatus, native::NativeSlot*,
                                               const compiled::ParameterSlot*, compiled::ProgramRuntime*>());
                call->set_arg(0, reg_ptr(ins.dst));
                call->set_arg(1, source);
                call->set_arg(2, runtime);
                finish_helper(call);
            } else if (type == compiled::ValueType::INT32) {
                Gp value = cc.new_gp32();
                cc.mov(value, dword_ptr(slots, SlotOffset(ins.aux, offsetof(compiled::ParameterSlot, int_value))));
                cc.mov(dword_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, int_value))), value);
                mark_initialized(ins.dst);
            } else if (type == compiled::ValueType::FLOAT64) {
                Vec value = cc.new_xmm_sd();
                cc.movsd(value, ptr(slots, SlotOffset(ins.aux, offsetof(compiled::ParameterSlot, float_value))));
                cc.movsd(ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, float_value))), value);
                mark_initialized(ins.dst);
            } else {
                Gp value = cc.new_gp32();
                cc.movzx(value, byte_ptr(slots, SlotOffset(ins.aux, offsetof(compiled::ParameterSlot, bool_value))));
                cc.mov(byte_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, bool_value))), value.r8());
                mark_initialized(ins.dst);
            }
            break;
        }
        case compiled::Opcode::MAKE_POINT_KEY: {
            InvokeNode* call = nullptr;
            cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::MakePointKey)),
                      FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, uint32_t,
                                           const native::NativeSlot*, native::NativeSlot*>());
            call->set_arg(0, runtime);
            call->set_arg(1, imm(ins.aux));
            call->set_arg(2, reg_ptr(ins.lhs));
            call->set_arg(3, reg_ptr(ins.dst));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::POINT_LOOKUP: {
            const auto& desc = program.registers()[ins.dst];
            const uint32_t size = program.tuple_layouts()[desc.tuple_layout].byte_size;
            InvokeNode* call = nullptr;
            cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::PointLookup)),
                      FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const native::NativeSlot*,
                                           native::NativeSlot*, native::NativeSlot*, uint32_t>());
            call->set_arg(0, runtime);
            call->set_arg(1, reg_ptr(ins.lhs));
            call->set_arg(2, reg_ptr(ins.rhs));
            call->set_arg(3, reg_ptr(ins.dst));
            call->set_arg(4, imm(size));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::COPY_TUPLE: {
            Gp dst = tuple_data(ins.dst), src = tuple_data(ins.lhs);
            const auto& desc = program.registers()[ins.dst];
            copy_fixed(dst, src, program.tuple_layouts()[desc.tuple_layout].byte_size);
            break;
        }
        case compiled::Opcode::LOAD_COLUMN: {
            const auto& tuple_desc = program.registers()[ins.lhs];
            const auto& column = program.tuple_layouts()[tuple_desc.tuple_layout].columns[ins.aux];
            Gp source = tuple_data(ins.lhs);
            cc.add(source, column.offset);
            if (column.type == compiled::ValueType::BYTES) {
                InvokeNode* call = nullptr;
                cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::LoadBytesColumn)),
                          FuncSignature::build<compiled::ExecStatus, native::NativeSlot*, const uint8_t*, uint32_t,
                                               compiled::ProgramRuntime*>());
                call->set_arg(0, reg_ptr(ins.dst));
                call->set_arg(1, source);
                call->set_arg(2, imm(column.width));
                call->set_arg(3, runtime);
                finish_helper(call);
            } else if (column.type == compiled::ValueType::INT32) {
                Gp value = cc.new_gp32();
                cc.mov(value, dword_ptr(source));
                cc.mov(dword_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, int_value))), value);
                mark_initialized(ins.dst);
            } else if (column.type == compiled::ValueType::FLOAT64) {
                Vec value = cc.new_xmm_sd();
                cc.movsd(value, ptr(source));
                cc.movsd(ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, float_value))), value);
                mark_initialized(ins.dst);
            } else {
                Gp value = cc.new_gp32();
                cc.movzx(value, byte_ptr(source));
                cc.test(value, value);
                cc.setne(value.r8());
                cc.mov(byte_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, bool_value))), value.r8());
                mark_initialized(ins.dst);
            }
            break;
        }
        case compiled::Opcode::STORE_COLUMN: {
            const auto& tuple_desc = program.registers()[ins.dst];
            const auto& column = program.tuple_layouts()[tuple_desc.tuple_layout].columns[ins.aux];
            const auto source_type = program.registers()[ins.lhs].type;
            Gp destination = tuple_data(ins.dst);
            cc.add(destination, column.offset);
            if (column.type == compiled::ValueType::BYTES) {
                InvokeNode* call = nullptr;
                cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::StoreBytesColumn)),
                          FuncSignature::build<compiled::ExecStatus, uint8_t*, uint32_t, const native::NativeSlot*,
                                               compiled::ProgramRuntime*>());
                call->set_arg(0, destination);
                call->set_arg(1, imm(column.width));
                call->set_arg(2, reg_ptr(ins.lhs));
                call->set_arg(3, runtime);
                finish_helper(call);
            } else if (column.type == compiled::ValueType::INT32 && source_type == compiled::ValueType::INT32) {
                Gp value = cc.new_gp32();
                cc.mov(value, dword_ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, int_value))));
                cc.mov(dword_ptr(destination), value);
            } else if (column.type == compiled::ValueType::INT32) {
                Vec value = cc.new_xmm_sd();
                Gp converted = cc.new_gp32();
                cc.movsd(value, ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, float_value))));
                const Label invalid = cc.new_label(), done = cc.new_label();
                Vec min = cc.new_xmm_sd(), max = cc.new_xmm_sd();
                Gp min_int = cc.new_gp32(), max_int = cc.new_gp32();
                cc.mov(min_int, std::numeric_limits<int32_t>::min());
                cc.cvtsi2sd(min, min_int);
                cc.mov(max_int, std::numeric_limits<int32_t>::max());
                cc.cvtsi2sd(max, max_int);
                cc.ucomisd(value, min);
                cc.jb(invalid);
                cc.ucomisd(value, max);
                cc.ja(invalid);
                cc.cvttsd2si(converted, value);
                cc.mov(dword_ptr(destination), converted);
                cc.jmp(done);
                cc.bind(invalid);
                fail("numeric value cannot be stored as INT32");
                cc.bind(done);
            } else if (column.type == compiled::ValueType::FLOAT64 && source_type == compiled::ValueType::INT32) {
                Gp value = cc.new_gp32();
                Vec converted = cc.new_xmm_sd();
                cc.mov(value, dword_ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, int_value))));
                cc.cvtsi2sd(converted, value);
                cc.movsd(ptr(destination), converted);
            } else if (column.type == compiled::ValueType::FLOAT64) {
                Vec value = cc.new_xmm_sd();
                cc.movsd(value, ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, float_value))));
                cc.movsd(ptr(destination), value);
            } else {
                Gp value = cc.new_gp32();
                cc.movzx(value, byte_ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, bool_value))));
                cc.mov(byte_ptr(destination), value.r8());
            }
            break;
        }
        case compiled::Opcode::ADD:
        case compiled::Opcode::SUB:
        case compiled::Opcode::MUL:
        case compiled::Opcode::DIV: {
            const auto dst_type = program.registers()[ins.dst].type;
            if (dst_type == compiled::ValueType::INT32) {
                Gp left = cc.new_gp64(), right = cc.new_gp64(), result = cc.new_gp64();
                cc.movsxd(left, dword_ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, int_value))));
                cc.movsxd(right, dword_ptr(regs, RegOffset(ins.rhs, offsetof(native::NativeSlot, int_value))));
                cc.mov(result, left);
                if (ins.opcode == compiled::Opcode::ADD)
                    cc.add(result, right);
                if (ins.opcode == compiled::Opcode::SUB)
                    cc.sub(result, right);
                if (ins.opcode == compiled::Opcode::MUL)
                    cc.imul(result, right);
                if (ins.opcode == compiled::Opcode::DIV) {
                    const Label nonzero = cc.new_label();
                    cc.test(right, right);
                    cc.jnz(nonzero);
                    fail("integer division by zero");
                    cc.bind(nonzero);
                    Gp remainder = cc.new_gp64();
                    cc.cqo(remainder, result);
                    cc.idiv(remainder, result, right);
                }
                const Label valid = cc.new_label();
                cc.cmp(result, std::numeric_limits<int32_t>::min());
                cc.jl(arithmetic_failed);
                cc.cmp(result, std::numeric_limits<int32_t>::max());
                cc.jle(valid);
                fail("integer arithmetic overflow");
                cc.bind(valid);
                cc.mov(dword_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, int_value))), result.r32());
                mark_initialized(ins.dst);
            } else {
                auto load_numeric = [&](uint32_t index) {
                    Vec value = cc.new_xmm_sd();
                    if (program.registers()[index].type == compiled::ValueType::INT32) {
                        Gp integer = cc.new_gp32();
                        cc.mov(integer, dword_ptr(regs, RegOffset(index, offsetof(native::NativeSlot, int_value))));
                        cc.cvtsi2sd(value, integer);
                    } else
                        cc.movsd(value, ptr(regs, RegOffset(index, offsetof(native::NativeSlot, float_value))));
                    return value;
                };
                Vec left = load_numeric(ins.lhs), right = load_numeric(ins.rhs);
                if (ins.opcode == compiled::Opcode::DIV) {
                    Vec zero = cc.new_xmm_sd();
                    cc.xorpd(zero, zero);
                    const Label nonzero = cc.new_label();
                    cc.ucomisd(right, zero);
                    cc.jp(nonzero);
                    cc.jne(nonzero);
                    fail("floating-point division by zero");
                    cc.bind(nonzero);
                }
                if (ins.opcode == compiled::Opcode::ADD)
                    cc.addsd(left, right);
                if (ins.opcode == compiled::Opcode::SUB)
                    cc.subsd(left, right);
                if (ins.opcode == compiled::Opcode::MUL)
                    cc.mulsd(left, right);
                if (ins.opcode == compiled::Opcode::DIV)
                    cc.divsd(left, right);
                cc.movsd(ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, float_value))), left);
                mark_initialized(ins.dst);
            }
            break;
        }
        case compiled::Opcode::COMPARE: {
            const auto lhs_type = program.registers()[ins.lhs].type;
            if (lhs_type == compiled::ValueType::BYTES) {
                InvokeNode* call = nullptr;
                Gp result = cc.new_gp32();
                cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::CompareBytes)),
                          FuncSignature::build<uint8_t, const native::NativeSlot*, const native::NativeSlot*,
                                               compiled::CompareOp>());
                call->set_arg(0, reg_ptr(ins.lhs));
                call->set_arg(1, reg_ptr(ins.rhs));
                call->set_arg(2, imm(static_cast<uint32_t>(ins.compare_op)));
                call->set_ret(0, result);
                cc.mov(byte_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, bool_value))), result.r8());
            } else if (lhs_type == compiled::ValueType::BOOL) {
                Gp left = cc.new_gp32(), right = cc.new_gp32();
                cc.movzx(left, byte_ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, bool_value))));
                cc.movzx(right, byte_ptr(regs, RegOffset(ins.rhs, offsetof(native::NativeSlot, bool_value))));
                cc.cmp(left, right);
                if (ins.compare_op == compiled::CompareOp::EQ)
                    cc.sete(left.r8());
                else
                    cc.setne(left.r8());
                cc.mov(byte_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, bool_value))), left.r8());
            } else {
                auto load_numeric = [&](uint32_t index) {
                    Vec v = cc.new_xmm_sd();
                    if (program.registers()[index].type == compiled::ValueType::INT32) {
                        Gp i = cc.new_gp32();
                        cc.mov(i, dword_ptr(regs, RegOffset(index, offsetof(native::NativeSlot, int_value))));
                        cc.cvtsi2sd(v, i);
                    } else
                        cc.movsd(v, ptr(regs, RegOffset(index, offsetof(native::NativeSlot, float_value))));
                    return v;
                };
                Vec left = load_numeric(ins.lhs), right = load_numeric(ins.rhs);
                Gp result = cc.new_gp32();
                const Label unordered = cc.new_label(), done = cc.new_label();
                cc.ucomisd(left, right);
                cc.jp(unordered);
                switch (ins.compare_op) {
                case compiled::CompareOp::EQ:
                    cc.sete(result.r8());
                    break;
                case compiled::CompareOp::NE:
                    cc.setne(result.r8());
                    break;
                case compiled::CompareOp::LT:
                    cc.setb(result.r8());
                    break;
                case compiled::CompareOp::GT:
                    cc.seta(result.r8());
                    break;
                case compiled::CompareOp::LE:
                    cc.setbe(result.r8());
                    break;
                case compiled::CompareOp::GE:
                    cc.setae(result.r8());
                    break;
                }
                cc.jmp(done);
                cc.bind(unordered);
                cc.mov(result, ins.compare_op == compiled::CompareOp::NE ? 1 : 0);
                cc.bind(done);
                cc.mov(byte_ptr(regs, RegOffset(ins.dst, offsetof(native::NativeSlot, bool_value))), result.r8());
            }
            mark_initialized(ins.dst);
            break;
        }
        case compiled::Opcode::JUMP:
            cc.jmp(labels[ins.aux]);
            continue;
        case compiled::Opcode::JUMP_IF_FALSE: {
            Gp condition = cc.new_gp32();
            cc.movzx(condition, byte_ptr(regs, RegOffset(ins.lhs, offsetof(native::NativeSlot, bool_value))));
            cc.test(condition, condition);
            cc.jz(labels[ins.aux]);
            break;
        }
        case compiled::Opcode::PREPARE_UPDATE: {
            const auto& desc = program.registers()[ins.rhs];
            uint32_t size = program.tuple_layouts()[desc.tuple_layout].byte_size;
            InvokeNode* call = nullptr;
            cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::PrepareUpdate)),
                      FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const native::NativeSlot*,
                                           native::NativeSlot*, native::NativeSlot*, uint32_t>());
            call->set_arg(0, runtime);
            call->set_arg(1, reg_ptr(ins.lhs));
            call->set_arg(2, reg_ptr(ins.rhs));
            call->set_arg(3, reg_ptr(ins.dst));
            call->set_arg(4, imm(size));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::COMMIT_UPDATE: {
            InvokeNode* call = nullptr;
            cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::CommitUpdate)),
                      FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const native::NativeSlot*,
                                           const native::NativeSlot*>());
            call->set_arg(0, runtime);
            call->set_arg(1, reg_ptr(ins.lhs));
            call->set_arg(2, reg_ptr(ins.rhs));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::DELETE_ROW: {
            InvokeNode* call = nullptr;
            cc.invoke(
                Out(call), imm(reinterpret_cast<void*>(native::DeleteRow)),
                FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const native::NativeSlot*>());
            call->set_arg(0, runtime);
            call->set_arg(1, reg_ptr(ins.lhs));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::INSERT_ROW: {
            InvokeNode* call = nullptr;
            cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::InsertRow)),
                      FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const native::NativeSlot*,
                                           native::NativeSlot*>());
            call->set_arg(0, runtime);
            call->set_arg(1, reg_ptr(ins.lhs));
            call->set_arg(2, reg_ptr(ins.dst));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::EMIT_ROW: {
            InvokeNode* call = nullptr;
            cc.invoke(
                Out(call), imm(reinterpret_cast<void*>(native::EmitRow)),
                FuncSignature::build<compiled::ExecStatus, compiled::ProgramRuntime*, const native::NativeSlot*>());
            call->set_arg(0, runtime);
            call->set_arg(1, reg_ptr(ins.lhs));
            finish_helper(call);
            break;
        }
        case compiled::Opcode::HALT:
            cc.mov(status, static_cast<uint32_t>(compiled::ExecStatus::OK));
            cc.jmp(cleanup);
            continue;
        }
        if (pc + 1 < labels.size())
            cc.jmp(labels[pc + 1]);
    }

    cc.bind(arithmetic_failed);
    fail("integer arithmetic overflow");
    cc.bind(step_failed);
    fail("native program step limit exceeded");
    cc.bind(cleanup);
    {
        InvokeNode* call = nullptr;
        cc.invoke(Out(call), imm(reinterpret_cast<void*>(native::DestroyFrame)),
                  FuncSignature::build<void, native::ExecutionFrame*>());
        call->set_arg(0, frame);
    }
    cc.ret(status);
    cc.bind(frame_failed);
    cc.mov(status, static_cast<uint32_t>(compiled::ExecStatus::ERROR));
    cc.ret(status);
    cc.end_func();

    Error error = cc.finalize();
    if (error != Error::kOk)
        return {JitStatus::COMPILE_ERROR, {}, errors.description(error)};
    NativeEntry generated = nullptr;
    error = runtime_->runtime.add(&generated, &code);
    if (error != Error::kOk)
        return {JitStatus::ALLOCATION_ERROR, {}, errors.description(error)};
    runtime_->active_code_count.fetch_add(1, std::memory_order_relaxed);
    return {JitStatus::OK,
            JitCode(runtime_, reinterpret_cast<void*>(generated), code.code_size(), JitCode::Kind::PROGRAM,
                    std::move(program_owner)),
            {}};
}

} // namespace jit
