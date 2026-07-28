/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cmath>
#include <cstddef>
#include <limits>
#include <utility>
#include <vector>

#include "common/common.h"

// Immutable, request-local prepared parameter storage. Values own their CHAR
// bytes and never point into a wire frame, descriptor, or executor runtime.
class ParameterFrame final {
public:
    explicit ParameterFrame(std::vector<Value> values) : values_(normalize(std::move(values))) {}

    std::size_t size() const noexcept {
        return values_.size();
    }

    const Value& at(std::size_t ordinal) const {
        if (ordinal == 0 || ordinal > values_.size()) {
            throw RMDBError("prepared parameter ordinal is out of range");
        }
        return values_[ordinal - 1];
    }

    Value bind(std::size_t ordinal, ColType expected_type) const {
        const Value& source = at(ordinal);
        Value result;
        result.type = expected_type;
        if (source.is_null) {
            result.set_null();
            result.type = expected_type;
            return result;
        }
        if (source.type == expected_type) {
            result = source;
            result.parameter_ordinal = 0;
            result.raw.reset();
            return result;
        }
        if (source.type == TYPE_INT && expected_type == TYPE_FLOAT) {
            result.set_float(static_cast<float>(source.int_val));
            return result;
        }
        if (source.type == TYPE_FLOAT && expected_type == TYPE_INT) {
            const double exact_value = static_cast<double>(source.float_val);
            if (exact_value < static_cast<double>(std::numeric_limits<int>::min()) ||
                exact_value > static_cast<double>(std::numeric_limits<int>::max())) {
                throw RMDBError("prepared FLOAT parameter is outside INT32 range");
            }
            result.set_int(static_cast<int>(source.float_val));
            return result;
        }
        if ((source.type == TYPE_STRING && expected_type == TYPE_DATETIME) ||
            (source.type == TYPE_DATETIME && expected_type == TYPE_STRING)) {
            result.set_str(source.str_val);
            result.type = expected_type;
            return result;
        }
        throw IncompatibleTypeError(coltype2str(expected_type), coltype2str(source.type));
    }

private:
    static std::vector<Value> normalize(std::vector<Value> values) {
        for (auto& value : values) {
            value.parameter_ordinal = 0;
            value.raw.reset();
            if (!value.is_null && value.type == TYPE_FLOAT && !std::isfinite(value.float_val)) {
                throw RMDBError("prepared FLOAT parameter must be finite");
            }
        }
        return values;
    }

    const std::vector<Value> values_;
};
