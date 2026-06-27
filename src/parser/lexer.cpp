/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lexer.h"

#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <limits>

namespace parser {

const std::unordered_map<std::string_view, TokenType, CIHash, CIEqual> Lexer::keywords_ = {
    {"SHOW", TokenType::SHOW},
    {"TABLES", TokenType::TABLES},
    {"CREATE", TokenType::CREATE},
    {"TABLE", TokenType::TABLE},
    {"DROP", TokenType::DROP},
    {"DESC", TokenType::DESC},
    {"INSERT", TokenType::INSERT},
    {"INTO", TokenType::INTO},
    {"VALUES", TokenType::VALUES},
    {"DELETE", TokenType::DELETE},
    {"FROM", TokenType::FROM},
    {"WHERE", TokenType::WHERE},
    {"UPDATE", TokenType::UPDATE},
    {"SET", TokenType::SET},
    {"SELECT", TokenType::SELECT},
    {"EXPLAIN", TokenType::EXPLAIN},
    {"ANALYZE", TokenType::ANALYZE},
    {"AS", TokenType::AS},
    {"INT", TokenType::INT},
    {"CHAR", TokenType::CHAR},
    {"FLOAT", TokenType::FLOAT},
    {"DATETIME", TokenType::DATETIME},
    {"INDEX", TokenType::INDEX},
    {"AND", TokenType::AND},
    {"ORDER", TokenType::ORDER},
    {"BY", TokenType::BY},
    {"ASC", TokenType::ASC},
    {"LIMIT", TokenType::LIMIT},
    {"JOIN", TokenType::JOIN},
    {"ON", TokenType::ON},
    {"GROUP", TokenType::GROUP},
    {"HAVING", TokenType::HAVING},
    {"UNION", TokenType::UNION},
    {"COUNT", TokenType::COUNT},
    {"SUM", TokenType::SUM},
    {"AVG", TokenType::AVG},
    {"MIN", TokenType::MIN},
    {"MAX", TokenType::MAX},
    {"BEGIN", TokenType::BEGIN_KW},
    {"COMMIT", TokenType::COMMIT},
    {"ABORT", TokenType::ABORT},
    {"ROLLBACK", TokenType::ROLLBACK},
    {"TRANSACTION", TokenType::TRANSACTION},
    {"ISOLATION", TokenType::ISOLATION},
    {"LEVEL", TokenType::LEVEL},
    {"SNAPSHOT", TokenType::SNAPSHOT},
    {"SERIALIZABLE", TokenType::SERIALIZABLE},
    {"ENABLE_NESTLOOP", TokenType::ENABLE_NESTLOOP},
    {"ENABLE_SORTMERGE", TokenType::ENABLE_SORTMERGE},
    {"STATIC_CHECKPOINT", TokenType::STATIC_CHECKPOINT},
    {"OUTPUT_FILE", TokenType::OUTPUT_FILE},
    {"OFF", TokenType::OFF},
    {"LOAD", TokenType::LOAD},
    {"EXIT", TokenType::EXIT},
    {"HELP", TokenType::HELP},
    {"TRUE", TokenType::VALUE_BOOL},
    {"FALSE", TokenType::VALUE_BOOL},
};

Lexer::Lexer(std::string_view input) : input_(input), pos_(0), line_(1), column_(1), has_peeked_(false) {}

char Lexer::current_char() const {
    return pos_ < input_.size() ? input_[pos_] : '\0';
}

char Lexer::peek_char(int offset) const {
    size_t idx = pos_ + offset;
    return idx < input_.size() ? input_[idx] : '\0';
}

void Lexer::advance(int count) {
    for (int i = 0; i < count && pos_ < input_.size(); ++i) {
        if (input_[pos_] == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        pos_++;
    }
}

void Lexer::skip_whitespace_and_comments() {
    while (pos_ < input_.size()) {
        char c = current_char();

        if (std::isspace(static_cast<unsigned char>(c))) {
            advance();
        } else if (c == '-' && peek_char() == '-') {
            skip_line_comment();
        } else if (c == '/' && peek_char() == '*') {
            skip_block_comment();
        } else {
            break;
        }
    }
}

void Lexer::skip_line_comment() {
    while (pos_ < input_.size() && current_char() != '\n') {
        advance();
    }
}

void Lexer::skip_block_comment() {
    int start_line = line_;
    int start_col = column_;
    advance(2); // skip /*
    while (pos_ < input_.size()) {
        if (current_char() == '*' && peek_char() == '/') {
            advance(2);
            return;
        }
        advance();
    }
    throw LexerError("Lexer Error at line " + std::to_string(start_line) + " column " + std::to_string(start_col) +
                     ": unterminated block comment");
}

Token Lexer::next_token() {
    if (has_peeked_) {
        has_peeked_ = false;
        return peeked_;
    }

    skip_whitespace_and_comments();

    if (pos_ >= input_.size()) {
        return Token(TokenType::T_EOF, "", line_, column_);
    }

    char c = current_char();

    // File path (for LOAD): starts with '/', "./" or "../"
    if ((c == '/' &&
         !(std::isspace(static_cast<unsigned char>(peek_char(1))) ||
           std::isdigit(static_cast<unsigned char>(peek_char(1))) || peek_char(1) == '-' || peek_char(1) == '\'')) ||
        (c == '.' && (peek_char(1) == '/' || (peek_char(1) == '.' && peek_char(2) == '/')))) {
        return scan_path();
    }

    // Identifier or keyword
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
        return scan_identifier_or_keyword();
    }

    // Number
    if (std::isdigit(static_cast<unsigned char>(c))) {
        return scan_number();
    }

    // String
    if (c == '\'') {
        return scan_string();
    }

    // Operator or punctuation
    return scan_operator();
}

Token Lexer::peek_token() {
    if (!has_peeked_) {
        peeked_ = next_token();
        has_peeked_ = true;
    }
    return peeked_;
}

Token Lexer::scan_identifier_or_keyword() {
    int start_line = line_;
    int start_col = column_;
    size_t start_pos = pos_;

    while (pos_ < input_.size()) {
        char c = current_char();
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
            advance();
        } else {
            break;
        }
    }

    std::string_view text = input_.substr(start_pos, pos_ - start_pos);

    auto it = keywords_.find(text);
    if (it != keywords_.end()) {
        Token tok(it->second, text, start_line, start_col);
        if (it->second == TokenType::VALUE_BOOL) {
            tok.bool_value = CIEqual{}(text, "TRUE");
        }
        return tok;
    }

    return Token(TokenType::IDENTIFIER, text, start_line, start_col);
}

Token Lexer::scan_number() {
    int start_line = line_;
    int start_col = column_;
    size_t start_pos = pos_;
    bool has_dot = false;

    while (pos_ < input_.size()) {
        char c = current_char();
        if (std::isdigit(static_cast<unsigned char>(c))) {
            advance();
        } else if (c == '.' && !has_dot) {
            has_dot = true;
            advance();
        } else {
            break;
        }
    }

    std::string_view text = input_.substr(start_pos, pos_ - start_pos);
    Token tok(has_dot ? TokenType::VALUE_FLOAT : TokenType::VALUE_INT, text, start_line, start_col);

    if (has_dot) {
        tok.float_value = parse_float(text);
    } else {
        tok.int_value = parse_integer(text);
    }

    return tok;
}

Token Lexer::scan_string() {
    int start_line = line_;
    int start_col = column_;
    advance(); // skip opening quote

    size_t start_pos = pos_;
    while (pos_ < input_.size() && current_char() != '\'') {
        advance();
    }

    if (pos_ >= input_.size()) {
        throw LexerError("Lexer Error at line " + std::to_string(start_line) + " column " + std::to_string(start_col) +
                         ": unterminated string literal");
    }

    std::string_view text = input_.substr(start_pos, pos_ - start_pos);
    advance(); // skip closing quote

    return Token(TokenType::VALUE_STRING, text, start_line, start_col);
}

Token Lexer::scan_path() {
    int start_line = line_;
    int start_col = column_;
    size_t start_pos = pos_;
    // Scan until whitespace or ';' (file paths contain no such characters).
    while (pos_ < input_.size() && current_char() != ' ' && current_char() != '\t' && current_char() != '\n' &&
           current_char() != '\r' && current_char() != ';') {
        advance();
    }
    std::string_view text = input_.substr(start_pos, pos_ - start_pos);
    return Token(TokenType::VALUE_PATH, text, start_line, start_col);
}

Token Lexer::scan_operator() {
    int start_line = line_;
    int start_col = column_;
    char c = current_char();
    char next = peek_char();

    // Two-character operators
    if (c == '<' && next == '=') {
        advance(2);
        return Token(TokenType::LEQ, "<=", start_line, start_col);
    }
    if (c == '>' && next == '=') {
        advance(2);
        return Token(TokenType::GEQ, ">=", start_line, start_col);
    }
    if (c == '<' && next == '>') {
        advance(2);
        return Token(TokenType::NEQ, "<>", start_line, start_col);
    }
    if (c == '!' && next == '=') {
        advance(2);
        return Token(TokenType::NEQ, "!=", start_line, start_col);
    }
    if (c == '+' && next == '=') {
        advance(2);
        return Token(TokenType::PLUS_ASSIGN, "+=", start_line, start_col);
    }
    if (c == '-' && next == '=') {
        advance(2);
        return Token(TokenType::MINUS_ASSIGN, "-=", start_line, start_col);
    }

    // Single-character operators
    advance();
    switch (c) {
    case '=':
        return Token(TokenType::EQ, "=", start_line, start_col);
    case '<':
        return Token(TokenType::LT, "<", start_line, start_col);
    case '>':
        return Token(TokenType::GT, ">", start_line, start_col);
    case '+':
        return Token(TokenType::PLUS, "+", start_line, start_col);
    case '-':
        return Token(TokenType::MINUS, "-", start_line, start_col);
    case '(':
        return Token(TokenType::LPAREN, "(", start_line, start_col);
    case ')':
        return Token(TokenType::RPAREN, ")", start_line, start_col);
    case ',':
        return Token(TokenType::COMMA, ",", start_line, start_col);
    case ';':
        return Token(TokenType::SEMICOLON, ";", start_line, start_col);
    case '.':
        return Token(TokenType::DOT, ".", start_line, start_col);
    case '*':
        return Token(TokenType::STAR, "*", start_line, start_col);
    case '/':
        return Token(TokenType::SLASH, "/", start_line, start_col);
    default:
        return Token(TokenType::T_ERROR, std::string_view(&input_[pos_ - 1], 1), start_line, start_col);
    }
}

int64_t Lexer::parse_integer(std::string_view text) {
    int64_t value = 0;
    for (char c : text) {
        if (!std::isdigit(static_cast<unsigned char>(c))) {
            throw LexerError("Lexer Error: integer literal malformed");
        }
        int digit = c - '0';
        if (value > (std::numeric_limits<int64_t>::max() - digit) / 10) {
            throw LexerError("Lexer Error: integer literal out of range");
        }
        value = value * 10 + digit;
    }
    return value;
}

double Lexer::parse_float(std::string_view text) {
    std::string owned(text);
    char* end = nullptr;
    errno = 0;
    double value = std::strtod(owned.c_str(), &end);
    if (errno == ERANGE || end != owned.c_str() + owned.size()) {
        throw LexerError("Lexer Error: float literal out of range or malformed");
    }
    return value;
}

} // namespace parser
