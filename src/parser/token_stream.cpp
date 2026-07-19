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

class ShapeHasher {
public:
    void append_byte(unsigned char byte) {
        low_ ^= byte;
        low_ *= 1099511628211ULL;
        high_ ^= byte + 0x9dU;
        high_ *= 14029467366897019727ULL;
        high_ ^= high_ >> 29;
        ++size_;
    }

    void append_u32(uint32_t value) {
        for (unsigned shift = 0; shift < 32; shift += 8) {
            append_byte(static_cast<unsigned char>((value >> shift) & 0xffU));
        }
    }

    void append_string(std::string_view value) {
        append_u32(static_cast<uint32_t>(value.size()));
        for (unsigned char byte : value) {
            append_byte(byte);
        }
    }

    void append_token_type(TokenType type) {
        append_u32(static_cast<uint32_t>(type));
    }

    void append_param_marker(TokenType type) {
        append_byte(0xffU);
        append_token_type(type);
    }

    TokenShapeKey finish() const {
        return {high_, low_, size_};
    }

private:
    uint64_t low_{1469598103934665603ULL};
    uint64_t high_{1099511628211ULL};
    uint32_t size_{0};
};

} // namespace

OwnedTokenStream normalize_sql(std::string_view sql, bool retain_tokens) {
    OwnedTokenStream result;
    ShapeHasher shape;
    result.parameters.reserve(16);
    if (retain_tokens) {
        result.tokens.reserve(sql.size() / 4 + 1);
    }
    shape.append_string("RMDB-TOKEN-SHAPE");
    shape.append_u32(TOKEN_STREAM_VERSION);
    try {
        Lexer lexer(sql);
        while (true) {
            Token token = lexer.next_token();
            if (retain_tokens) {
                OwnedToken owned;
                owned.type = token.type;
                owned.text.assign(token.text.data(), token.text.size());
                owned.line = token.line;
                owned.column = token.column;
                owned.int_value = token.int_value;
                owned.float_value = token.float_value;
                owned.bool_value = token.bool_value;
                result.tokens.push_back(std::move(owned));
            }

            result.template_unsupported =
                result.template_unsupported || token.type == TokenType::MINUS || token.type == TokenType::LIMIT;

            shape.append_token_type(token.type);
            if (token.type == TokenType::IDENTIFIER) {
                shape.append_string(token.text);
            } else if (token.type == TokenType::VALUE_INT || token.type == TokenType::VALUE_FLOAT ||
                       token.type == TokenType::VALUE_STRING || token.type == TokenType::VALUE_BOOL) {
                shape.append_param_marker(token.type);
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
        result.key = shape.finish();
    }
    return result;
}

} // namespace parser
