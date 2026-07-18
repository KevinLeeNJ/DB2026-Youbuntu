/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "jit/native_abi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include "compiled/program_verifier.h"

namespace jit::native {
namespace {

struct FrameOwner {
    ExecutionFrame frame{};
    std::vector<NativeSlot> registers;
    std::vector<std::vector<uint8_t>> storage;
};

compiled::RuntimeValue ToRuntime(const NativeSlot& slot) {
    compiled::RuntimeValue value;
    value.type = slot.type;
    value.initialized = slot.initialized != 0;
    value.int_value = slot.int_value;
    value.float_value = slot.float_value;
    value.bool_value = slot.bool_value != 0;
    value.opaque = slot.opaque;
    if (slot.type == compiled::ValueType::BYTES || slot.type == compiled::ValueType::POINT_KEY)
        value.bytes.assign(reinterpret_cast<const char*>(slot.data), slot.length);
    if (slot.type == compiled::ValueType::TUPLE)
        value.tuple.assign(slot.data, slot.data + slot.length);
    return value;
}

bool FromRuntime(const compiled::RuntimeValue& value, NativeSlot* slot) {
    if (!value.initialized || value.type != slot->type)
        return false;
    slot->int_value = value.int_value;
    slot->float_value = value.float_value;
    slot->bool_value = value.bool_value ? 1 : 0;
    slot->opaque = value.opaque;
    const uint8_t* source = nullptr;
    size_t size = 0;
    if (slot->type == compiled::ValueType::BYTES || slot->type == compiled::ValueType::POINT_KEY) {
        source = reinterpret_cast<const uint8_t*>(value.bytes.data());
        size = value.bytes.size();
    } else if (slot->type == compiled::ValueType::TUPLE) {
        source = value.tuple.data();
        size = value.tuple.size();
    }
    if (size > slot->capacity)
        return false;
    if (size != 0)
        std::memcpy(slot->data, source, size);
    slot->length = static_cast<uint32_t>(size);
    slot->initialized = 1;
    return true;
}

compiled::ExecStatus Check(compiled::ExecStatus status, compiled::ProgramRuntime* runtime, const char* helper) {
    if (runtime->last_status() != compiled::ExecStatus::OK)
        return runtime->last_status();
    switch (status) {
    case compiled::ExecStatus::OK:
        return status;
    case compiled::ExecStatus::NO_MATCH_RESULT:
        return runtime->SetError(status, {});
    case compiled::ExecStatus::FALLBACK:
    case compiled::ExecStatus::TXN_ABORT:
    case compiled::ExecStatus::ERROR:
        return runtime->SetError(status, std::string(helper) + " failed");
    }
    return runtime->SetError(compiled::ExecStatus::ERROR, std::string(helper) + " returned an invalid status");
}

compiled::ExecStatus InvalidOutput(compiled::ProgramRuntime* runtime, const char* helper) {
    return runtime->SetError(compiled::ExecStatus::ERROR, std::string(helper) + " returned an invalid output");
}

compiled::ExecStatus CopyOutput(const compiled::RuntimeValue& value, NativeSlot* slot, uint32_t tuple_size,
                                compiled::ProgramRuntime* runtime, const char* helper) {
    if (!value.initialized)
        return runtime->SetError(compiled::ExecStatus::ERROR, std::string(helper) + " did not initialize its output");
    if (value.type != slot->type)
        return runtime->SetError(compiled::ExecStatus::ERROR,
                                 std::string(helper) + " returned a value with the wrong type");
    if (slot->type == compiled::ValueType::TUPLE && value.tuple.size() != tuple_size)
        return runtime->SetError(compiled::ExecStatus::ERROR,
                                 std::string(helper) + " returned a tuple with the wrong size");
    if (slot->type == compiled::ValueType::BYTES && value.bytes.size() > slot->capacity)
        return runtime->SetError(compiled::ExecStatus::ERROR,
                                 std::string(helper) + " returned an oversized byte value");
    return FromRuntime(value, slot) ? compiled::ExecStatus::OK : InvalidOutput(runtime, helper);
}

bool Result(compiled::CompareOp op, int comparison) {
    switch (op) {
    case compiled::CompareOp::EQ:
        return comparison == 0;
    case compiled::CompareOp::NE:
        return comparison != 0;
    case compiled::CompareOp::LT:
        return comparison < 0;
    case compiled::CompareOp::GT:
        return comparison > 0;
    case compiled::CompareOp::LE:
        return comparison <= 0;
    case compiled::CompareOp::GE:
        return comparison >= 0;
    }
    return false;
}

template <typename Function>
compiled::ExecStatus Guard(compiled::ProgramRuntime* runtime, Function&& function) noexcept {
    try {
        return function();
    } catch (const std::exception& e) {
        return runtime->SetError(compiled::ExecStatus::ERROR, std::string("native helper exception: ") + e.what());
    } catch (...) {
        return runtime->SetError(compiled::ExecStatus::ERROR, "unknown native helper exception");
    }
}

} // namespace

extern "C" ExecutionFrame* CreateFrame(const compiled::CompiledProgram* program,
                                       const compiled::ParameterFrame* parameters,
                                       compiled::ProgramRuntime* runtime) noexcept {
    if (!program || !parameters || !runtime)
        return nullptr;
    try {
        runtime->ClearError();
        auto verified = compiled::VerifyProgram(*program);
        if (!verified) {
            runtime->SetError(compiled::ExecStatus::ERROR, verified.error);
            return nullptr;
        }
        if (parameters->size() != program->parameters().size()) {
            runtime->SetError(compiled::ExecStatus::ERROR, "parameter frame does not match the program");
            return nullptr;
        }
        for (size_t i = 0; i < parameters->size(); ++i) {
            const auto& slot = parameters->slot(i);
            const auto& desc = program->parameters()[i];
            if (slot.type != desc.type ||
                (slot.type == compiled::ValueType::BYTES && slot.bytes_length > desc.max_length)) {
                runtime->SetError(compiled::ExecStatus::ERROR, "parameter frame does not match the program");
                return nullptr;
            }
        }
        auto owner = std::make_unique<FrameOwner>();
        owner->registers.resize(program->registers().size());
        owner->storage.resize(program->registers().size());
        for (size_t i = 0; i < owner->registers.size(); ++i) {
            const auto& desc = program->registers()[i];
            auto& slot = owner->registers[i];
            slot.type = desc.type;
            uint32_t capacity = 0;
            if (desc.type == compiled::ValueType::TUPLE)
                capacity = program->tuple_layouts()[desc.tuple_layout].byte_size;
            else if (desc.type == compiled::ValueType::BYTES)
                capacity = desc.max_length;
            else if (desc.type == compiled::ValueType::POINT_KEY)
                capacity = compiled::MAX_PROGRAM_VALUE_BYTES;
            if (capacity) {
                owner->storage[i].resize(capacity);
                slot.data = owner->storage[i].data();
                slot.capacity = capacity;
            }
            if (desc.type == compiled::ValueType::TUPLE) {
                slot.length = capacity;
                slot.initialized = 1;
            }
        }
        owner->frame.registers = owner->registers.data();
        owner->frame.owner = owner.get();
        return &owner.release()->frame;
    } catch (...) {
        runtime->SetError(compiled::ExecStatus::ERROR, "native frame allocation failed");
        return nullptr;
    }
}

extern "C" void DestroyFrame(ExecutionFrame* frame) noexcept {
    if (frame)
        delete static_cast<FrameOwner*>(frame->owner);
}
extern "C" const compiled::ParameterSlot* ParameterData(const compiled::ParameterFrame* p) noexcept {
    return p ? p->data() : nullptr;
}
extern "C" compiled::ExecStatus LoadBytesParameter(NativeSlot* dst, const compiled::ParameterSlot* src,
                                                   compiled::ProgramRuntime* runtime) noexcept {
    if (src->bytes_length > dst->capacity)
        return InvalidOutput(runtime, "LoadParam");
    if (src->bytes_length != 0)
        std::memcpy(dst->data, src->bytes, src->bytes_length);
    dst->length = src->bytes_length;
    dst->initialized = 1;
    return compiled::ExecStatus::OK;
}
extern "C" compiled::ExecStatus LoadBytesColumn(NativeSlot* dst, const uint8_t* src, uint32_t width,
                                                compiled::ProgramRuntime* runtime) noexcept {
    const auto* end = static_cast<const uint8_t*>(std::memchr(src, 0, width));
    uint32_t size = end ? static_cast<uint32_t>(end - src) : width;
    if (size > dst->capacity)
        return InvalidOutput(runtime, "LoadColumn");
    if (size != 0)
        std::memcpy(dst->data, src, size);
    dst->length = size;
    dst->initialized = 1;
    return compiled::ExecStatus::OK;
}
extern "C" compiled::ExecStatus StoreBytesColumn(uint8_t* dst, uint32_t width, const NativeSlot* src,
                                                 compiled::ProgramRuntime*) noexcept {
    std::memset(dst, 0, width);
    const uint32_t size = std::min(width, src->length);
    if (size != 0)
        std::memcpy(dst, src->data, size);
    return compiled::ExecStatus::OK;
}
extern "C" uint8_t CompareBytes(const NativeSlot* lhs, const NativeSlot* rhs, compiled::CompareOp op) noexcept {
    uint32_t common = std::min(lhs->length, rhs->length);
    int comparison = common ? std::memcmp(lhs->data, rhs->data, common) : 0;
    if (!comparison)
        comparison = lhs->length < rhs->length ? -1 : lhs->length > rhs->length ? 1 : 0;
    return Result(op, comparison);
}
extern "C" compiled::ExecStatus Fail(compiled::ProgramRuntime* runtime, const char* message) noexcept {
    return runtime ? runtime->SetError(compiled::ExecStatus::ERROR, message ? message : "native error")
                   : compiled::ExecStatus::ERROR;
}

extern "C" compiled::ExecStatus MakePointKey(compiled::ProgramRuntime* runtime, uint32_t index, const NativeSlot* tuple,
                                             NativeSlot* key) noexcept {
    return Guard(runtime, [&] {
        auto in = ToRuntime(*tuple);
        compiled::RuntimeValue out;
        out.type = key->type;
        auto s = Check(runtime->MakePointKey(index, in, &out), runtime, "MakePointKey");
        if (s != compiled::ExecStatus::OK)
            return s;
        return CopyOutput(out, key, 0, runtime, "MakePointKey");
    });
}
extern "C" compiled::ExecStatus PointLookup(compiled::ProgramRuntime* runtime, const NativeSlot* key, NativeSlot* row,
                                            NativeSlot* tuple, uint32_t tuple_size) noexcept {
    return Guard(runtime, [&] {
        auto k = ToRuntime(*key);
        compiled::RuntimeValue r, t;
        r.type = row->type;
        t.type = tuple->type;
        auto s = Check(runtime->PointLookup(k, &r, &t), runtime, "PointLookup");
        if (s != compiled::ExecStatus::OK)
            return s;
        s = CopyOutput(r, row, 0, runtime, "PointLookup");
        return s == compiled::ExecStatus::OK ? CopyOutput(t, tuple, tuple_size, runtime, "PointLookup") : s;
    });
}
extern "C" compiled::ExecStatus PrepareUpdate(compiled::ProgramRuntime* runtime, const NativeSlot* row,
                                              NativeSlot* tuple, NativeSlot* prepared, uint32_t tuple_size) noexcept {
    return Guard(runtime, [&] {
        auto r = ToRuntime(*row), t = ToRuntime(*tuple);
        t.initialized = false;
        compiled::RuntimeValue p;
        p.type = prepared->type;
        auto s = Check(runtime->PrepareUpdate(r, &t, &p), runtime, "PrepareUpdate");
        if (s != compiled::ExecStatus::OK)
            return s;
        s = CopyOutput(t, tuple, tuple_size, runtime, "PrepareUpdate");
        return s == compiled::ExecStatus::OK ? CopyOutput(p, prepared, 0, runtime, "PrepareUpdate") : s;
    });
}
extern "C" compiled::ExecStatus CommitUpdate(compiled::ProgramRuntime* runtime, const NativeSlot* prepared,
                                             const NativeSlot* tuple) noexcept {
    return Guard(runtime, [&] {
        auto p = ToRuntime(*prepared), t = ToRuntime(*tuple);
        return Check(runtime->CommitUpdate(p, t), runtime, "CommitUpdate");
    });
}
extern "C" compiled::ExecStatus DeleteRow(compiled::ProgramRuntime* runtime, const NativeSlot* row) noexcept {
    return Guard(runtime, [&] {
        auto r = ToRuntime(*row);
        return Check(runtime->DeleteRow(r), runtime, "DeleteRow");
    });
}
extern "C" compiled::ExecStatus InsertRow(compiled::ProgramRuntime* runtime, const NativeSlot* tuple,
                                          NativeSlot* row) noexcept {
    return Guard(runtime, [&] {
        auto t = ToRuntime(*tuple);
        compiled::RuntimeValue r;
        r.type = row->type;
        auto s = Check(runtime->InsertRow(t, &r), runtime, "InsertRow");
        return s == compiled::ExecStatus::OK ? CopyOutput(r, row, 0, runtime, "InsertRow") : s;
    });
}
extern "C" compiled::ExecStatus EmitRow(compiled::ProgramRuntime* runtime, const NativeSlot* tuple) noexcept {
    return Guard(runtime, [&] {
        auto t = ToRuntime(*tuple);
        return Check(runtime->EmitRow(t), runtime, "EmitRow");
    });
}

} // namespace jit::native
