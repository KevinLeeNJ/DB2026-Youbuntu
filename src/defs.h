/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <iostream>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>

template <typename T> inline T read_unaligned(const void* data) {
    static_assert(std::is_trivially_copyable_v<T>);
    T value;
    std::memcpy(&value, data, sizeof(T));
    return value;
}

template <typename T> inline void write_unaligned(void* data, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::memcpy(data, &value, sizeof(T));
}

inline float read_float(const void* data) {
    return read_unaligned<float>(data);
}

inline void write_float(void* data, float value) {
    write_unaligned(data, value);
}

// 此处重载了<<操作符，在ColMeta中进行了调用
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream& operator<<(std::ostream& os, const T& enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream& operator>>(std::istream& is, T& enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid {
    int page_no;
    int slot_no;

    friend bool operator==(const Rid& x, const Rid& y) {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid& x, const Rid& y) {
        return !(x == y);
    }
};

enum ColType { TYPE_INT, TYPE_FLOAT, TYPE_STRING, TYPE_DATETIME };

inline std::string coltype2str(ColType type) {
    switch (type) {
    case TYPE_INT:
        return "INT";
    case TYPE_FLOAT:
        return "FLOAT";
    case TYPE_STRING:
        return "STRING";
    case TYPE_DATETIME:
        return "DATETIME";
    }
    throw std::out_of_range("map::at");
}

class RecScan {
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};
