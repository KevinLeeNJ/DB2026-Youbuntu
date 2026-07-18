/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "compiled/instruction.h"

namespace compiled {

enum class ExecStatus : uint8_t { OK, NO_MATCH_RESULT, FALLBACK, TXN_ABORT, ERROR };

struct RuntimeValue {
    ValueType type{ValueType::INT32};
    bool initialized{false};
    int32_t int_value{0};
    double float_value{0.0};
    bool bool_value{false};
    std::string bytes;
    std::vector<uint8_t> tuple;
    uint64_t opaque{0};
};

class ProgramRuntime {
public:
    virtual ~ProgramRuntime() = default;

    virtual ExecStatus MakePointKey(uint32_t index_id, const RuntimeValue& value, RuntimeValue* key) noexcept;
    virtual ExecStatus PointLookup(const RuntimeValue& key, RuntimeValue* row, RuntimeValue* tuple) noexcept;
    virtual ExecStatus PrepareUpdate(const RuntimeValue& row, RuntimeValue* current_tuple,
                                     RuntimeValue* prepared) noexcept;
    virtual ExecStatus CommitUpdate(const RuntimeValue& prepared, const RuntimeValue& proposed_tuple) noexcept;
    virtual ExecStatus DeleteRow(const RuntimeValue& row) noexcept;
    virtual ExecStatus InsertRow(const RuntimeValue& tuple, RuntimeValue* row) noexcept;
    virtual ExecStatus EmitRow(const RuntimeValue& tuple) noexcept;

    ExecStatus SetError(ExecStatus status, std::string message) noexcept;
    void ClearError() noexcept;
    ExecStatus last_status() const {
        return last_status_;
    }
    const std::string& error_message() const {
        return error_message_;
    }

private:
    ExecStatus Unsupported(const char* helper) noexcept;

    ExecStatus last_status_{ExecStatus::OK};
    std::string error_message_;
};

} // namespace compiled
