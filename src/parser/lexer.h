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

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

namespace parser {

struct LexerError : public std::runtime_error {
    explicit LexerError(const std::string& message) : std::runtime_error(message) {}
};

struct CIHash {
    size_t operator()(std::string_view s) const {
        size_t h = 0;
        for (char c : s) {
            h = h * 31 + std::toupper(static_cast<unsigned char>(c));
        }
        return h;
    }
};

struct CIEqual {
    bool operator()(std::string_view a, std::string_view b) const {
        if (a.size() != b.size())
            return false;
        for (size_t i = 0; i < a.size(); i++) {
            if (std::toupper(static_cast<unsigned char>(a[i])) != std::toupper(static_cast<unsigned char>(b[i])))
                return false;
        }
        return true;
    }
};

enum class TokenType {
    // Keywords (50个)
    SHOW,
    TABLES,
    CREATE,
    TABLE,
    DROP,
    DESC,
    INSERT,
    INTO,
    VALUES,
    DELETE,
    FROM,
    ASC,
    ORDER,
    BY,
    GROUP,
    HAVING,
    LIMIT,
    AS,
    UNION,
    BEGIN_KW,
    COMMIT,
    ABORT,
    ROLLBACK,
    ENABLE_NESTLOOP,
    ENABLE_SORTMERGE,
    TRANSACTION,
    ISOLATION,
    LEVEL,
    SNAPSHOT,
    SERIALIZABLE,
    STATIC_CHECKPOINT,
    OUTPUT_FILE,
    OFF,
    LOAD,
    VALUE_PATH,
    WHERE,
    UPDATE,
    SET,
    SELECT,
    DISTINCT,
    CASE,
    WHEN,
    THEN,
    ELSE,
    END,
    IS,
    NULL_KW,
    EXISTS,
    ANY,
    INTERSECT,
    EXCEPT,
    CROSS,
    NATURAL,
    NULLS,
    FIRST,
    LAST,
    OR,
    EXPLAIN,
    ANALYZE,
    INT,
    CHAR,
    FLOAT,
    DATETIME,
    INDEX,
    AND,
    LIKE,
    IN,
    NOT,
    BETWEEN,
    JOIN,
    ON,
    LEFT,
    RIGHT,
    FULL,
    OUTER,
    ALL,
    OFFSET,
    COUNT,
    MAX,
    MIN,
    SUM,
    AVG,
    ROW_NUMBER,
    RANK,
    DENSE_RANK,
    LAG,
    LEAD,
    OVER,
    PARTITION,
    EXIT,
    HELP,

    // Operators
    LEQ,
    NEQ,
    GEQ,
    EQ,
    LT,
    GT,
    PLUS,
    MINUS,
    STAR,
    SLASH,
    PLUS_ASSIGN,
    MINUS_ASSIGN,

    // Punctuation
    LPAREN,
    RPAREN,
    COMMA,
    SEMICOLON,
    DOT,

    // Literals
    IDENTIFIER,
    VALUE_INT,
    VALUE_FLOAT,
    VALUE_STRING,
    VALUE_BOOL,

    // Special
    T_EOF,
    T_ERROR
};

struct Token {
    TokenType type;
    std::string_view text; // 零拷贝：指向输入缓冲区
    int line;
    int column;

    // 对于数值类型，直接存储解析后的值
    union {
        int64_t int_value;
        double float_value;
        bool bool_value;
    };

    Token() : type(TokenType::T_ERROR), line(0), column(0), int_value(0) {}

    Token(TokenType t, std::string_view txt, int l, int c) : type(t), text(txt), line(l), column(c), int_value(0) {}
};

class Lexer {
public:
    explicit Lexer(std::string_view input);

    Token next_token();
    Token peek_token();

    int current_line() const {
        return line_;
    }
    int current_column() const {
        return column_;
    }

private:
    std::string_view input_;
    size_t pos_;
    int line_;
    int column_;

    Token peeked_;
    bool has_peeked_;

    // 关键字查找表（大小写不敏感）
    static const std::unordered_map<std::string_view, TokenType, CIHash, CIEqual> keywords_;

    // 辅助函数
    char current_char() const;
    char peek_char(int offset = 1) const;
    void advance(int count = 1);
    void skip_whitespace_and_comments();
    void skip_line_comment();
    void skip_block_comment();

    Token scan_identifier_or_keyword();
    Token scan_number();
    Token scan_string();
    Token scan_path();
    Token scan_operator();

    // 数字解析
    int64_t parse_integer(std::string_view text);
    double parse_float(std::string_view text);
};

} // namespace parser
