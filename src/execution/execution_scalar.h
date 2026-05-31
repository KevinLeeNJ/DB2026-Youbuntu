#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>

#include "defs.h"
#include "errors.h"

namespace execution_scalar {

inline bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

inline std::size_t trimmed_string_length(const char* data, int len) {
    const void* terminator = std::memchr(data, '\0', static_cast<std::size_t>(len));
    if (terminator == nullptr) {
        return static_cast<std::size_t>(len);
    }
    return static_cast<const char*>(terminator) - data;
}

inline std::string_view trim_string_view(const char* data, int len) {
    return std::string_view(data, trimmed_string_length(data, len));
}

inline std::string trim_string(const char* data, int len) {
    std::string_view view = trim_string_view(data, len);
    return std::string(view.data(), view.size());
}

struct CellValue {
    ColType type = TYPE_INT;
    int int_val = 0;
    float float_val = 0.0f;
    std::string str_val;

    bool operator==(const CellValue& other) const;
};

inline float promote_numeric_value(const CellValue& value) {
    return value.type == TYPE_INT ? static_cast<float>(value.int_val) : value.float_val;
}

inline int compare_cells(const CellValue& lhs, const CellValue& rhs) {
    if (is_numeric_type(lhs.type) && is_numeric_type(rhs.type)) {
        float lhs_val = promote_numeric_value(lhs);
        float rhs_val = promote_numeric_value(rhs);
        if (lhs_val < rhs_val) {
            return -1;
        }
        if (lhs_val > rhs_val) {
            return 1;
        }
        return 0;
    }
    if (lhs.type != rhs.type) {
        throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
    }
    if (lhs.type == TYPE_STRING) {
        if (lhs.str_val < rhs.str_val) {
            return -1;
        }
        if (lhs.str_val > rhs.str_val) {
            return 1;
        }
        return 0;
    }
    if (lhs.int_val < rhs.int_val) {
        return -1;
    }
    if (lhs.int_val > rhs.int_val) {
        return 1;
    }
    return 0;
}

inline bool CellValue::operator==(const CellValue& other) const {
    try {
        return compare_cells(*this, other) == 0;
    } catch (const IncompatibleTypeError&) {
        return false;
    }
}

inline void hash_combine(std::size_t& seed, std::size_t value) {
    seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

inline std::size_t hash_numeric_value(const CellValue& value) {
    float normalized = promote_numeric_value(value);
    if (normalized == 0.0f) {
        normalized = 0.0f;
    }
    std::uint32_t bits = 0;
    std::memcpy(&bits, &normalized, sizeof(bits));
    std::size_t seed = std::hash<int>()(static_cast<int>(TYPE_FLOAT));
    hash_combine(seed, std::hash<std::uint32_t>()(bits));
    return seed;
}

struct CellValueHash {
    std::size_t operator()(const CellValue& value) const {
        if (is_numeric_type(value.type)) {
            return hash_numeric_value(value);
        }

        std::size_t seed = std::hash<int>()(static_cast<int>(value.type));
        hash_combine(seed, std::hash<std::string>()(value.str_val));
        return seed;
    }
};

inline CellValue zero_value(ColType type) {
    CellValue value;
    value.type = type;
    if (type == TYPE_FLOAT) {
        value.float_val = 0.0f;
    } else if (type == TYPE_STRING) {
        value.str_val.clear();
    } else {
        value.int_val = 0;
    }
    return value;
}

} // namespace execution_scalar
