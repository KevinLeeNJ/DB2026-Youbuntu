/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "compiled/compiled_program.h"
#include "compiled/program_runtime.h"

namespace compiled {

struct ParameterSlot {
    ValueType type{ValueType::INT32};
    int32_t int_value{0};
    double float_value{0.0};
    const char* bytes{nullptr};
    uint32_t bytes_length{0};
    uint8_t bool_value{0};
};

static_assert(std::is_standard_layout_v<ParameterSlot>);
static_assert(std::is_trivially_copyable_v<ParameterSlot>);

class ParameterValue {
public:
    static ParameterValue Int(int32_t value);
    static ParameterValue Float(double value);
    static ParameterValue Bool(bool value);
    static ParameterValue Bytes(std::string value);

    ValueType type() const {
        return value_.type;
    }
    const RuntimeValue& value() const {
        return value_;
    }

private:
    explicit ParameterValue(RuntimeValue value) : value_(std::move(value)) {}
    RuntimeValue value_;
};

class ParameterFrame {
public:
    ParameterFrame(const ParameterFrame& other);
    ParameterFrame(ParameterFrame&& other);
    ParameterFrame& operator=(const ParameterFrame& other);
    ParameterFrame& operator=(ParameterFrame&& other);

    static std::optional<ParameterFrame> Bind(const std::vector<ParameterDesc>& descriptors,
                                              const std::vector<ParameterValue>& values, std::string* error);

    size_t size() const {
        return values_.size();
    }
    const RuntimeValue& value(size_t index) const {
        return values_.at(index);
    }
    const ParameterSlot& slot(size_t index) const {
        return slots_.at(index);
    }
    const ParameterSlot* data() const {
        return slots_.data();
    }

private:
    explicit ParameterFrame(std::vector<RuntimeValue> values);
    void RebuildSlots();

    std::vector<RuntimeValue> values_;
    std::vector<ParameterSlot> slots_;
};

} // namespace compiled
