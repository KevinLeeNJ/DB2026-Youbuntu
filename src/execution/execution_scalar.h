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
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "defs.h"
#include "errors.h"

namespace execution_scalar {

/**
 * @brief 判断列类型是否属于可进行数值提升的数值类型。
 * @param type 待判断的列类型。
 * @return type 为 INT 或 FLOAT 时返回 true。
 */
inline bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

/**
 * @brief 计算定长字段中第一个字符串结束符之前的有效长度。
 * @param data 字段数据起始地址。
 * @param len 字段分配的最大字节数。
 * @return 有效字符串长度；没有结束符时返回 len。
 */
inline std::size_t trimmed_string_length(const char* data, int len) {
    const void* terminator = std::memchr(data, '\0', static_cast<std::size_t>(len));
    if (terminator == nullptr) {
        return static_cast<std::size_t>(len);
    }
    return static_cast<const char*>(terminator) - data;
}

/**
 * @brief 以 string_view 形式读取定长字符串的有效部分。
 * @param data 字段数据起始地址。
 * @param len 字段分配的最大字节数。
 * @return 不包含定长填充的字符串视图；视图不拥有底层内存。
 */
inline std::string_view trim_string_view(const char* data, int len) {
    return std::string_view(data, trimmed_string_length(data, len));
}

/**
 * @brief 将定长字段中的有效字符串复制为 std::string。
 * @param data 字段数据起始地址。
 * @param len 字段分配的最大字节数。
 * @return 去除尾部填充后的字符串副本。
 */
inline std::string trim_string(const char* data, int len) {
    std::string_view view = trim_string_view(data, len);
    return std::string(view.data(), view.size());
}

/**
 * @brief 使用动态规划判断字符串是否匹配 SQL LIKE 模式。
 * @param value 待匹配字符串。
 * @param pattern LIKE 模式，其中 '%' 匹配任意长度字符串，'_' 匹配单个字符。
 * @return 模式匹配时返回 true。
 */
inline bool like_match(std::string_view value, std::string_view pattern) {
    std::vector<bool> previous(pattern.size() + 1, false);
    std::vector<bool> current(pattern.size() + 1, false);
    previous[0] = true;
    for (size_t pattern_pos = 1; pattern_pos <= pattern.size(); ++pattern_pos) {
        previous[pattern_pos] = previous[pattern_pos - 1] && pattern[pattern_pos - 1] == '%';
    }

    for (size_t value_pos = 1; value_pos <= value.size(); ++value_pos) {
        current.assign(pattern.size() + 1, false);
        for (size_t pattern_pos = 1; pattern_pos <= pattern.size(); ++pattern_pos) {
            char pattern_char = pattern[pattern_pos - 1];
            if (pattern_char == '%') {
                current[pattern_pos] = current[pattern_pos - 1] || previous[pattern_pos];
            } else if (pattern_char == '_' || pattern_char == value[value_pos - 1]) {
                current[pattern_pos] = previous[pattern_pos - 1];
            }
        }
        previous.swap(current);
    }
    return previous[pattern.size()];
}

struct CellValue {
    ColType type = TYPE_INT;
    int int_val = 0;
    double float_val = 0.0;
    std::string str_val;

    /**
     * @brief 判断两个类型化标量是否具有相同的值。
     * @param other 参与比较的另一个标量。
     * @return 两个值可比较且相等时返回 true；类型不兼容时返回 false。
     */
    bool operator==(const CellValue& other) const;
};

/**
 * @brief 将 INT/FLOAT 标量提升为 double 供统一数值运算使用。
 * @param value 待提升的数值标量。
 * @return INT 转换后的 double，FLOAT 原值返回。
 */
inline double promote_numeric_value(const CellValue& value) {
    return value.type == TYPE_INT ? static_cast<double>(value.int_val) : value.float_val;
}

/**
 * @brief 对两个标量执行统一的三路比较。
 * @param lhs 左操作数。
 * @param rhs 右操作数。
 * @return lhs 小于、等于、大于 rhs 时分别返回负数、0、正数。
 * @throws IncompatibleTypeError 两个值既不是数值类型也不是字符串/日期时间类型时抛出。
 */
inline int compare_cells(const CellValue& lhs, const CellValue& rhs) {
    if (is_numeric_type(lhs.type) && is_numeric_type(rhs.type)) {
        double lhs_val = promote_numeric_value(lhs);
        double rhs_val = promote_numeric_value(rhs);
        if (lhs_val < rhs_val) {
            return -1;
        }
        if (lhs_val > rhs_val) {
            return 1;
        }
        return 0;
    }
    if ((lhs.type == TYPE_STRING || lhs.type == TYPE_DATETIME) &&
        (rhs.type == TYPE_STRING || rhs.type == TYPE_DATETIME)) {
        if (lhs.str_val < rhs.str_val) {
            return -1;
        }
        if (lhs.str_val > rhs.str_val) {
            return 1;
        }
        return 0;
    }
    if (lhs.type != rhs.type) {
        throw IncompatibleTypeError(coltype2str(lhs.type), coltype2str(rhs.type));
    }
    if (lhs.type == TYPE_STRING || lhs.type == TYPE_DATETIME) {
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

/**
 * @brief 将一个哈希值合并进已有的复合哈希种子。
 * @param seed 待更新的哈希种子。
 * @param value 新加入的哈希值。
 */
inline void hash_combine(std::size_t& seed, std::size_t value) {
    seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

/**
 * @brief 生成归一化数值标量的哈希值。
 * @param value INT 或 FLOAT 类型的数值。
 * @return 与数值比较语义一致的哈希值，使 1 和 1.0 可以得到相同结果。
 */
inline std::size_t hash_numeric_value(const CellValue& value) {
    double normalized = promote_numeric_value(value);
    if (normalized == 0.0) {
        normalized = 0.0;
    }
    std::uint64_t bits = 0;
    std::memcpy(&bits, &normalized, sizeof(bits));
    std::size_t seed = std::hash<int>()(static_cast<int>(TYPE_FLOAT));
    hash_combine(seed, std::hash<std::uint64_t>()(bits));
    return seed;
}

struct CellValueHash {
    /**
     * @brief 为标量生成可用于无序容器的哈希值。
     * @param value 待哈希的标量。
     * @return 数值或字符串/日期时间值的哈希结果。
     */
    std::size_t operator()(const CellValue& value) const {
        if (is_numeric_type(value.type)) {
            return hash_numeric_value(value);
        }

        std::size_t seed = std::hash<int>()(static_cast<int>(value.type));
        hash_combine(seed, std::hash<std::string>()(value.str_val));
        return seed;
    }
};

/**
 * @brief 构造指定类型的零值标量。
 * @param type 目标列类型。
 * @return INT/FLOAT 为数值零，字符串/日期时间为空字符串的标量。
 */
inline CellValue zero_value(ColType type) {
    CellValue value;
    value.type = type;
    if (type == TYPE_FLOAT) {
        value.float_val = 0.0;
    } else if (type == TYPE_STRING || type == TYPE_DATETIME) {
        value.str_val.clear();
    } else {
        value.int_val = 0;
    }
    return value;
}

} // namespace execution_scalar
