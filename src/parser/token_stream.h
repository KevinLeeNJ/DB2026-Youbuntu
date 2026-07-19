/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "parser/lexer.h"

namespace parser {

inline constexpr uint32_t TOKEN_STREAM_VERSION = 1;

struct LexicalParam {
    TokenType type{TokenType::T_ERROR};
    std::string text;
    int64_t int_value{0};
    double float_value{0.0};
    bool bool_value{false};
};

struct OwnedToken {
    TokenType type{TokenType::T_ERROR};
    std::string text;
    int line{0};
    int column{0};
    int64_t int_value{0};
    double float_value{0.0};
    bool bool_value{false};
};

struct TokenShapeKey {
    uint64_t high{0};
    uint64_t low{0};
    uint32_t canonical_size{0};

    friend bool operator==(const TokenShapeKey& lhs, const TokenShapeKey& rhs) {
        return lhs.high == rhs.high && lhs.low == rhs.low && lhs.canonical_size == rhs.canonical_size;
    }

    friend bool operator!=(const TokenShapeKey& lhs, const TokenShapeKey& rhs) {
        return !(lhs == rhs);
    }
};

struct TokenShapeKeyHash {
    size_t operator()(const TokenShapeKey& key) const noexcept {
        return static_cast<size_t>(key.low ^ (key.high + 0x9e3779b97f4a7c15ULL + (key.low << 6U) + (key.low >> 2U)));
    }
};

struct OwnedTokenStream {
    std::vector<OwnedToken> tokens;
    std::vector<LexicalParam> parameters;
    TokenShapeKey key;
    std::string error;
    bool template_unsupported{false};

    explicit operator bool() const {
        return error.empty();
    }
};

OwnedTokenStream normalize_sql(std::string_view sql, bool retain_tokens = true);

} // namespace parser
