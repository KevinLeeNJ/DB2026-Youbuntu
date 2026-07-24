/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "jit/native_abi.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

#include "compiled/program_verifier.h"
#include "common/phase_metrics.h"

namespace jit::native {
namespace {

struct FrameOwner {
    ExecutionFrame frame{};
    FrameLayout layout;
    std::vector<NativeSlot> registers;
    std::vector<uint8_t> storage;

    void Initialize(const compiled::CompiledProgram& program);
    void ResetSlots() noexcept;
};

thread_local std::unique_ptr<FrameOwner> reusable_frame;
thread_local NativeRuntimeV2Binding* runtime_v2_binding = nullptr;

bool ValidRuntimeV2(const NativeRuntimeV2View* view) noexcept {
    if (!view || view->abi_version != kNativeRuntimeV2AbiVersion || view->view_size < sizeof(NativeRuntimeV2View) ||
        !view->table)
        return false;
    const auto& table = *view->table;
    return table.abi_version == kNativeRuntimeV2AbiVersion && table.struct_size >= sizeof(NativeRuntimeV2) &&
           table.make_point_key && table.point_lookup && table.prepare_update && table.commit_update &&
           table.delete_row && table.insert_row && table.emit_row;
}

const NativeRuntimeV2* RuntimeV2For(compiled::ProgramRuntime* runtime) noexcept {
    for (auto* binding = runtime_v2_binding; binding; binding = binding->previous) {
        if (binding->runtime == runtime && ValidRuntimeV2(binding->view))
            return binding->view->table;
    }
    return nullptr;
}

uint32_t RegisterCapacity(const compiled::CompiledProgram& program, const compiled::RegisterDesc& desc) {
    if (desc.type == compiled::ValueType::TUPLE) {
        if (desc.tuple_layout >= program.tuple_layouts().size())
            throw std::runtime_error("invalid tuple layout");
        return program.tuple_layouts()[desc.tuple_layout].byte_size;
    }
    if (desc.type == compiled::ValueType::BYTES)
        return desc.max_length;
    if (desc.type == compiled::ValueType::POINT_KEY)
        return compiled::MAX_PROGRAM_VALUE_BYTES;
    return 0;
}

void FrameOwner::Initialize(const compiled::CompiledProgram& program) {
    layout.register_count = static_cast<uint32_t>(program.registers().size());
    layout.offsets.resize(layout.register_count);
    layout.capacities.resize(layout.register_count);
    layout.types.resize(layout.register_count);

    uint64_t storage_size = 0;
    for (uint32_t i = 0; i < layout.register_count; ++i) {
        const auto& desc = program.registers()[i];
        const uint32_t capacity = RegisterCapacity(program, desc);
        if (storage_size > std::numeric_limits<uint32_t>::max() - capacity)
            throw std::runtime_error("native frame storage is too large");
        layout.offsets[i] = static_cast<uint32_t>(storage_size);
        layout.capacities[i] = capacity;
        layout.types[i] = desc.type;
        storage_size += capacity;
    }
    layout.storage_size = static_cast<uint32_t>(storage_size);
    registers.resize(layout.register_count);
    storage.resize(layout.storage_size);
    ResetSlots();
}

void FrameOwner::ResetSlots() noexcept {
    std::fill(registers.begin(), registers.end(), NativeSlot{});
    for (uint32_t i = 0; i < layout.register_count; ++i) {
        auto& slot = registers[i];
        slot.type = layout.types[i];
        slot.capacity = layout.capacities[i];
        if (slot.capacity != 0)
            slot.data = storage.data() + layout.offsets[i];
        if (slot.type == compiled::ValueType::TUPLE) {
            slot.length = slot.capacity;
            slot.initialized = 1;
        }
    }
    frame.registers = registers.data();
    frame.owner = this;
}

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

compiled::ExecStatus CheckV2(compiled::ExecStatus status, compiled::ProgramRuntime* runtime, const char* helper) {
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

compiled::ExecStatus ValidateNativeOutput(const NativeSlot& value, compiled::ValueType expected_type,
                                          uint32_t tuple_size, compiled::ProgramRuntime* runtime, const char* helper) {
    if (!value.initialized)
        return runtime->SetError(compiled::ExecStatus::ERROR, std::string(helper) + " did not initialize its output");
    if (value.type != expected_type)
        return runtime->SetError(compiled::ExecStatus::ERROR,
                                 std::string(helper) + " returned a value with the wrong type");
    if (value.type == compiled::ValueType::TUPLE && value.length != tuple_size)
        return runtime->SetError(compiled::ExecStatus::ERROR,
                                 std::string(helper) + " returned a tuple with the wrong size");
    if ((value.type == compiled::ValueType::BYTES || value.type == compiled::ValueType::POINT_KEY) &&
        value.length > value.capacity)
        return runtime->SetError(compiled::ExecStatus::ERROR,
                                 std::string(helper) + " returned an oversized byte value");
    return compiled::ExecStatus::OK;
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
    phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::NATIVE_HELPER,
                                               phase_metrics::sample_rate(phase_metrics::Phase::NATIVE_HELPER));
    try {
        return function();
    } catch (const std::exception& e) {
        return runtime->SetError(compiled::ExecStatus::ERROR, std::string("native helper exception: ") + e.what());
    } catch (...) {
        return runtime->SetError(compiled::ExecStatus::ERROR, "unknown native helper exception");
    }
}

} // namespace

extern "C" void PushRuntimeV2(NativeRuntimeV2Binding* binding, compiled::ProgramRuntime* runtime,
                              const NativeRuntimeV2View* view) noexcept {
    if (!binding)
        return;
    binding->runtime = runtime;
    binding->view = view;
    binding->previous = runtime_v2_binding;
    runtime_v2_binding = binding;
}

extern "C" void PopRuntimeV2(NativeRuntimeV2Binding* binding) noexcept {
    if (!binding || runtime_v2_binding != binding)
        return;
    runtime_v2_binding = binding->previous;
    binding->runtime = nullptr;
    binding->view = nullptr;
    binding->previous = nullptr;
}

bool FrameLayout::Matches(const compiled::CompiledProgram& program) const noexcept {
    if (register_count != program.registers().size() || offsets.size() != register_count ||
        capacities.size() != register_count || types.size() != register_count)
        return false;
    uint64_t storage_size = 0;
    for (uint32_t i = 0; i < register_count; ++i) {
        const auto& desc = program.registers()[i];
        uint32_t capacity = 0;
        if (desc.type == compiled::ValueType::TUPLE) {
            if (desc.tuple_layout >= program.tuple_layouts().size())
                return false;
            capacity = program.tuple_layouts()[desc.tuple_layout].byte_size;
        } else if (desc.type == compiled::ValueType::BYTES) {
            capacity = desc.max_length;
        } else if (desc.type == compiled::ValueType::POINT_KEY) {
            capacity = compiled::MAX_PROGRAM_VALUE_BYTES;
        }
        if (types[i] != desc.type || capacities[i] != capacity || offsets[i] != storage_size)
            return false;
        storage_size += capacity;
    }
    return storage_size == this->storage_size && storage_size <= std::numeric_limits<uint32_t>::max();
}

extern "C" ExecutionFrame* CreateFrame(const compiled::CompiledProgram* program,
                                       const compiled::ParameterFrame* parameters,
                                       compiled::ProgramRuntime* runtime) noexcept {
    phase_metrics::ScopedSample metrics_sample(phase_metrics::Phase::FRAME_CREATE,
                                               phase_metrics::sample_rate(phase_metrics::Phase::FRAME_CREATE));
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
        if (reusable_frame && reusable_frame->layout.Matches(*program)) {
            auto owner = reusable_frame.release();
            owner->ResetSlots();
            return &owner->frame;
        }

        // Keep one free frame per thread. A mismatched cached frame is
        // discarded rather than retaining an unbounded per-thread pool.
        reusable_frame.reset();
        auto owner = std::make_unique<FrameOwner>();
        owner->Initialize(*program);
        return &owner.release()->frame;
    } catch (...) {
        runtime->SetError(compiled::ExecStatus::ERROR, "native frame allocation failed");
        return nullptr;
    }
}

extern "C" void DestroyFrame(ExecutionFrame* frame) noexcept {
    if (!frame || !frame->owner)
        return;
    auto* owner = static_cast<FrameOwner*>(frame->owner);
    frame->registers = nullptr;
    frame->owner = nullptr;
    if (!reusable_frame)
        reusable_frame.reset(owner);
    else
        delete owner;
}

extern "C" const FrameLayout* FrameLayoutData(const ExecutionFrame* frame) noexcept {
    if (!frame || !frame->owner)
        return nullptr;
    return &static_cast<const FrameOwner*>(frame->owner)->layout;
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
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] {
            NativeSlot key_out = *key;
            auto s = CheckV2(table->make_point_key(table->context, index, tuple, &key_out), runtime, "MakePointKey");
            if (s != compiled::ExecStatus::OK)
                return s;
            s = ValidateNativeOutput(key_out, key->type, 0, runtime, "MakePointKey");
            if (s == compiled::ExecStatus::OK)
                *key = key_out;
            return s;
        });
    }
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
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] {
            NativeSlot row_out = *row;
            NativeSlot tuple_out = *tuple;
            auto s = CheckV2(table->point_lookup(table->context, key, &row_out, &tuple_out, tuple_size), runtime,
                             "PointLookup");
            if (s != compiled::ExecStatus::OK)
                return s;
            s = ValidateNativeOutput(row_out, row->type, 0, runtime, "PointLookup");
            if (s == compiled::ExecStatus::OK)
                s = ValidateNativeOutput(tuple_out, tuple->type, tuple_size, runtime, "PointLookup");
            if (s == compiled::ExecStatus::OK) {
                *row = row_out;
                *tuple = tuple_out;
            }
            return s;
        });
    }
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
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] {
            NativeSlot tuple_out = *tuple;
            tuple_out.initialized = 0;
            NativeSlot prepared_out = *prepared;
            auto s = CheckV2(table->prepare_update(table->context, row, &tuple_out, &prepared_out, tuple_size), runtime,
                             "PrepareUpdate");
            if (s != compiled::ExecStatus::OK)
                return s;
            s = ValidateNativeOutput(tuple_out, tuple->type, tuple_size, runtime, "PrepareUpdate");
            if (s == compiled::ExecStatus::OK)
                s = ValidateNativeOutput(prepared_out, prepared->type, 0, runtime, "PrepareUpdate");
            if (s == compiled::ExecStatus::OK) {
                *tuple = tuple_out;
                *prepared = prepared_out;
            }
            return s;
        });
    }
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
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] {
            return CheckV2(table->commit_update(table->context, prepared, tuple), runtime, "CommitUpdate");
        });
    }
    return Guard(runtime, [&] {
        auto p = ToRuntime(*prepared), t = ToRuntime(*tuple);
        return Check(runtime->CommitUpdate(p, t), runtime, "CommitUpdate");
    });
}
extern "C" compiled::ExecStatus DeleteRow(compiled::ProgramRuntime* runtime, const NativeSlot* row) noexcept {
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] { return CheckV2(table->delete_row(table->context, row), runtime, "DeleteRow"); });
    }
    return Guard(runtime, [&] {
        auto r = ToRuntime(*row);
        return Check(runtime->DeleteRow(r), runtime, "DeleteRow");
    });
}
extern "C" compiled::ExecStatus InsertRow(compiled::ProgramRuntime* runtime, const NativeSlot* tuple,
                                          NativeSlot* row) noexcept {
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] {
            NativeSlot row_out = *row;
            auto s = CheckV2(table->insert_row(table->context, tuple, &row_out), runtime, "InsertRow");
            if (s != compiled::ExecStatus::OK)
                return s;
            s = ValidateNativeOutput(row_out, row->type, 0, runtime, "InsertRow");
            if (s == compiled::ExecStatus::OK)
                *row = row_out;
            return s;
        });
    }
    return Guard(runtime, [&] {
        auto t = ToRuntime(*tuple);
        compiled::RuntimeValue r;
        r.type = row->type;
        auto s = Check(runtime->InsertRow(t, &r), runtime, "InsertRow");
        return s == compiled::ExecStatus::OK ? CopyOutput(r, row, 0, runtime, "InsertRow") : s;
    });
}
extern "C" compiled::ExecStatus EmitRow(compiled::ProgramRuntime* runtime, const NativeSlot* tuple) noexcept {
    if (const auto* table = RuntimeV2For(runtime)) {
        return Guard(runtime, [&] { return CheckV2(table->emit_row(table->context, tuple), runtime, "EmitRow"); });
    }
    return Guard(runtime, [&] {
        auto t = ToRuntime(*tuple);
        return Check(runtime->EmitRow(t), runtime, "EmitRow");
    });
}

} // namespace jit::native
