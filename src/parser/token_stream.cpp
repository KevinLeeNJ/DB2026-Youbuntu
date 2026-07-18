/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "parser/token_stream.h"

#include <cstring>

namespace parser {
namespace {

void append_u32(std::string& bytes, uint32_t value) {
    for (unsigned shift = 0; shift < 32; shift += 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xffU));
    }
}

void append_string(std::string& bytes, std::string_view value) {
    append_u32(bytes, static_cast<uint32_t>(value.size()));
    bytes.append(value.data(), value.size());
}

void append_token_type(std::string& bytes, TokenType type) {
    append_u32(bytes, static_cast<uint32_t>(type));
}

void append_param_marker(std::string& bytes, TokenType type) {
    bytes.push_back(static_cast<char>(0xff));
    append_token_type(bytes, type);
}

TokenShapeKey digest(std::string canonical) {
    uint64_t low = 1469598103934665603ULL;
    uint64_t high = 1099511628211ULL;
    for (unsigned char byte : canonical) {
        low ^= byte;
        low *= 1099511628211ULL;
        high ^= byte + 0x9dU;
        high *= 14029467366897019727ULL;
        high ^= high >> 29;
    }
    return {high, low, std::move(canonical)};
}

} // namespace

OwnedTokenStream normalize_sql(std::string_view sql) {
    OwnedTokenStream result;
    std::string canonical;
    append_string(canonical, "RMDB-TOKEN-SHAPE");
    append_u32(canonical, TOKEN_STREAM_VERSION);
    try {
        Lexer lexer(sql);
        while (true) {
            Token token = lexer.next_token();
            OwnedToken owned;
            owned.type = token.type;
            owned.text.assign(token.text.data(), token.text.size());
            owned.line = token.line;
            owned.column = token.column;
            owned.int_value = token.int_value;
            owned.float_value = token.float_value;
            owned.bool_value = token.bool_value;
            result.tokens.push_back(std::move(owned));

            append_token_type(canonical, token.type);
            if (token.type == TokenType::IDENTIFIER) {
                append_string(canonical, token.text);
            } else if (token.type == TokenType::VALUE_INT || token.type == TokenType::VALUE_FLOAT ||
                       token.type == TokenType::VALUE_STRING || token.type == TokenType::VALUE_BOOL) {
                append_param_marker(canonical, token.type);
                result.parameters.push_back(
                    {token.type, std::string(token.text), token.int_value, token.float_value, token.bool_value});
            } else if (token.type == TokenType::T_ERROR) {
                result.error = "unexpected token";
                break;
            }
            if (token.type == TokenType::T_EOF) {
                break;
            }
        }
    } catch (const LexerError& error) {
        result.error = error.what();
    }
    if (result.error.empty()) {
        result.key = digest(std::move(canonical));
    }
    return result;
}

} // namespace parser
