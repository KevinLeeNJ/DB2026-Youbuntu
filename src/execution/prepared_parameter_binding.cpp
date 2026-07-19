/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2. */

#include "execution/prepared_parameter_binding.h"

#include <algorithm>
#include <limits>

namespace {

std::optional<compiled::ValueType> compiled_type(ColType type) {
    switch (type) {
    case TYPE_INT:
        return compiled::ValueType::INT32;
    case TYPE_FLOAT:
        return compiled::ValueType::FLOAT64;
    case TYPE_STRING:
    case TYPE_DATETIME:
        return compiled::ValueType::BYTES;
    }
    return std::nullopt;
}

std::optional<compiled::ParameterValue> parameter_value(const parser::LexicalParam& parameter,
                                                        compiled::ValueType expected) {
    switch (expected) {
    case compiled::ValueType::INT32:
        if (parameter.type == parser::TokenType::VALUE_INT) {
            return compiled::ParameterValue::Int(static_cast<int32_t>(parameter.int_value));
        }
        if (parameter.type == parser::TokenType::VALUE_FLOAT) {
            return compiled::ParameterValue::Int(static_cast<int32_t>(parameter.float_value));
        }
        return std::nullopt;
    case compiled::ValueType::FLOAT64:
        if (parameter.type == parser::TokenType::VALUE_INT) {
            return compiled::ParameterValue::Float(static_cast<double>(parameter.int_value));
        }
        if (parameter.type == parser::TokenType::VALUE_FLOAT) {
            return compiled::ParameterValue::Float(parameter.float_value);
        }
        return std::nullopt;
    case compiled::ValueType::BYTES:
        if (parameter.type != parser::TokenType::VALUE_STRING) {
            return std::nullopt;
        }
        return compiled::ParameterValue::Bytes(parameter.text);
    case compiled::ValueType::BOOL:
        if (parameter.type != parser::TokenType::VALUE_BOOL) {
            return std::nullopt;
        }
        return compiled::ParameterValue::Bool(parameter.bool_value);
    default:
        return std::nullopt;
    }
    return std::nullopt;
}

} // namespace

bool PreparedParameterLayout::Register(const Value& value, uint32_t max_length) {
    if (value.lexical_slot < 0) {
        return true;
    }
    auto type = compiled_type(value.type);
    if (!type.has_value()) {
        return false;
    }
    const size_t slot = static_cast<size_t>(value.lexical_slot);
    if (slot >= descriptors_.size()) {
        descriptors_.resize(slot + 1);
        registered_.resize(slot + 1, false);
    }
    const uint32_t effective_max =
        *type == compiled::ValueType::BYTES ? (max_length == 0 ? std::numeric_limits<uint32_t>::max() : max_length) : 0;
    if (registered_[slot]) {
        if (descriptors_[slot].type != *type) {
            return false;
        }
        if (*type == compiled::ValueType::BYTES) {
            descriptors_[slot].max_length = std::min(descriptors_[slot].max_length, effective_max);
        }
        return true;
    }
    descriptors_[slot] = compiled::ParameterDesc{*type, effective_max, value.lexical_slot};
    registered_[slot] = true;
    return true;
}

std::optional<compiled::ParameterFrame> PreparedParameterLayout::Bind(const parser::OwnedTokenStream& lexical) const {
    if (lexical.parameters.size() != descriptors_.size() ||
        !std::all_of(registered_.begin(), registered_.end(), [](bool registered) { return registered; })) {
        return std::nullopt;
    }
    std::vector<compiled::ParameterValue> values;
    values.reserve(descriptors_.size());
    for (size_t slot = 0; slot < descriptors_.size(); ++slot) {
        auto value = parameter_value(lexical.parameters[slot], descriptors_[slot].type);
        if (!value.has_value()) {
            return std::nullopt;
        }
        values.push_back(std::move(*value));
    }
    return compiled::ParameterFrame::Bind(descriptors_, std::move(values), nullptr);
}

bool PreparedParameterLayout::Apply(const compiled::ParameterFrame& frame, Value* value) const {
    if (value == nullptr || value->lexical_slot < 0) {
        return value != nullptr;
    }
    const size_t slot = static_cast<size_t>(value->lexical_slot);
    if (slot >= descriptors_.size() || slot >= frame.size()) {
        return false;
    }
    const auto& runtime = frame.value(slot);
    switch (value->type) {
    case TYPE_INT:
        value->int_val = runtime.int_value;
        break;
    case TYPE_FLOAT:
        value->float_val = runtime.float_value;
        break;
    case TYPE_STRING:
    case TYPE_DATETIME:
        value->str_val = runtime.bytes;
        break;
    default:
        return false;
    }
    value->raw.reset();
    return true;
}
