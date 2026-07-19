/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "compiled/parameter_frame.h"

namespace compiled {

ParameterValue ParameterValue::Int(int32_t value) {
    RuntimeValue result;
    result.type = ValueType::INT32;
    result.initialized = true;
    result.int_value = value;
    return ParameterValue(std::move(result));
}

ParameterValue ParameterValue::Float(double value) {
    RuntimeValue result;
    result.type = ValueType::FLOAT64;
    result.initialized = true;
    result.float_value = value;
    return ParameterValue(std::move(result));
}

ParameterValue ParameterValue::Bool(bool value) {
    RuntimeValue result;
    result.type = ValueType::BOOL;
    result.initialized = true;
    result.bool_value = value;
    return ParameterValue(std::move(result));
}

ParameterValue ParameterValue::Bytes(std::string value) {
    RuntimeValue result;
    result.type = ValueType::BYTES;
    result.initialized = true;
    result.bytes = std::move(value);
    return ParameterValue(std::move(result));
}

ParameterFrame::ParameterFrame(std::vector<RuntimeValue> values) : values_(std::move(values)) {
    RebuildSlots();
}

ParameterFrame::ParameterFrame(const ParameterFrame& other) : values_(other.values_) {
    RebuildSlots();
}

ParameterFrame::ParameterFrame(ParameterFrame&& other) : values_(std::move(other.values_)) {
    RebuildSlots();
    other.RebuildSlots();
}

ParameterFrame& ParameterFrame::operator=(const ParameterFrame& other) {
    if (this != &other) {
        values_ = other.values_;
        RebuildSlots();
    }
    return *this;
}

ParameterFrame& ParameterFrame::operator=(ParameterFrame&& other) {
    if (this != &other) {
        values_ = std::move(other.values_);
        RebuildSlots();
        other.RebuildSlots();
    }
    return *this;
}

void ParameterFrame::RebuildSlots() {
    slots_.clear();
    slots_.reserve(values_.size());
    for (const RuntimeValue& value : values_) {
        ParameterSlot slot;
        slot.type = value.type;
        slot.int_value = value.int_value;
        slot.float_value = value.float_value;
        slot.bytes = value.type == ValueType::BYTES ? value.bytes.data() : nullptr;
        slot.bytes_length = value.type == ValueType::BYTES ? static_cast<uint32_t>(value.bytes.size()) : 0;
        slot.bool_value = value.bool_value ? 1 : 0;
        slots_.push_back(slot);
    }
}

std::optional<ParameterFrame> ParameterFrame::Bind(const std::vector<ParameterDesc>& descriptors,
                                                   const std::vector<ParameterValue>& values, std::string* error) {
    auto fail = [&](std::string message) -> std::optional<ParameterFrame> {
        if (error != nullptr) {
            *error = std::move(message);
        }
        return std::nullopt;
    };
    if (descriptors.size() != values.size()) {
        return fail("parameter count does not match the program");
    }

    std::vector<RuntimeValue> bound;
    bound.reserve(values.size());
    for (size_t index = 0; index < values.size(); ++index) {
        if (descriptors[index].type != values[index].type()) {
            return fail("parameter type does not match its descriptor");
        }
        if (values[index].type() == ValueType::BYTES &&
            values[index].value().bytes.size() > descriptors[index].max_length) {
            return fail("byte parameter exceeds its descriptor length");
        }
        bound.push_back(values[index].value());
    }
    if (error != nullptr) {
        error->clear();
    }
    return ParameterFrame(std::move(bound));
}

std::optional<ParameterFrame> ParameterFrame::Bind(const std::vector<ParameterDesc>& descriptors,
                                                   std::vector<ParameterValue>&& values, std::string* error) {
    auto fail = [&](std::string message) -> std::optional<ParameterFrame> {
        if (error != nullptr) {
            *error = std::move(message);
        }
        return std::nullopt;
    };
    if (descriptors.size() != values.size()) {
        return fail("parameter count does not match the program");
    }
    for (size_t index = 0; index < values.size(); ++index) {
        if (descriptors[index].type != values[index].type()) {
            return fail("parameter type does not match its descriptor");
        }
        if (values[index].type() == ValueType::BYTES &&
            values[index].value().bytes.size() > descriptors[index].max_length) {
            return fail("byte parameter exceeds its descriptor length");
        }
    }

    std::vector<RuntimeValue> bound;
    bound.reserve(values.size());
    for (ParameterValue& value : values) {
        bound.push_back(std::move(value).value());
    }
    if (error != nullptr) {
        error->clear();
    }
    return ParameterFrame(std::move(bound));
}

} // namespace compiled
