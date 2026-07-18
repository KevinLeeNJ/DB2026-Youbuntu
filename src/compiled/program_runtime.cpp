/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "compiled/program_runtime.h"

namespace compiled {

ExecStatus ProgramRuntime::Unsupported(const char* helper) noexcept {
    return SetError(ExecStatus::FALLBACK, std::string(helper) + " is not available");
}

ExecStatus ProgramRuntime::MakePointKey(uint32_t, const RuntimeValue&, RuntimeValue*) noexcept {
    return Unsupported("MakePointKey");
}

ExecStatus ProgramRuntime::PointLookup(const RuntimeValue&, RuntimeValue*, RuntimeValue*) noexcept {
    return Unsupported("PointLookup");
}

ExecStatus ProgramRuntime::PrepareUpdate(const RuntimeValue&, RuntimeValue*, RuntimeValue*) noexcept {
    return Unsupported("PrepareUpdate");
}

ExecStatus ProgramRuntime::CommitUpdate(const RuntimeValue&, const RuntimeValue&) noexcept {
    return Unsupported("CommitUpdate");
}

ExecStatus ProgramRuntime::DeleteRow(const RuntimeValue&) noexcept {
    return Unsupported("DeleteRow");
}

ExecStatus ProgramRuntime::InsertRow(const RuntimeValue&, RuntimeValue*) noexcept {
    return Unsupported("InsertRow");
}

ExecStatus ProgramRuntime::EmitRow(const RuntimeValue&) noexcept {
    return Unsupported("EmitRow");
}

ExecStatus ProgramRuntime::SetError(ExecStatus status, std::string message) noexcept {
    last_status_ = status;
    error_message_ = std::move(message);
    return status;
}

void ProgramRuntime::ClearError() noexcept {
    last_status_ = ExecStatus::OK;
    error_message_.clear();
}

} // namespace compiled
