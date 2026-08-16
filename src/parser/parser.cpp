/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "parser.h"

#include "lexer.h"

#include <algorithm>
#include <climits>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <utility>

namespace ast {
namespace {

using parser::Lexer;
using parser::Token;
using parser::TokenType;

struct FromClause {
    std::vector<TableRef> tables;
    std::vector<std::unique_ptr<BinaryExpr>> conds;
    std::vector<std::unique_ptr<JoinExpr>> jointree;
};

struct PaginationClause {
    bool has_limit = false;
    int limit = 0;
    bool has_offset = false;
    int offset = 0;
};

class SqlParser {
public:
    explicit SqlParser(std::string_view sql) : lexer_(sql) {
        advance();
    }

    std::unique_ptr<TreeNode> parse() {
        if (check(TokenType::T_EOF)) {
            return nullptr;
        }
        if (match(TokenType::HELP)) {
            consume_optional_semicolon();
            expect_end();
            return std::make_unique<Help>();
        }
        if (match(TokenType::EXIT)) {
            consume_optional_semicolon();
            expect_end();
            return nullptr;
        }

        // "set output_file off" has no semicolon; handle before the semicolon-enforcing path.
        if (check(TokenType::SET) && peek(1).type == TokenType::OUTPUT_FILE) {
            advance(); // consume SET
            advance(); // consume OUTPUT_FILE
            bool enable;
            if (match(TokenType::ON)) {
                enable = true;
            } else if (match(TokenType::OFF)) {
                enable = false;
            } else {
                error("expected ON or OFF after OUTPUT_FILE");
            }
            consume_optional_semicolon();
            expect_end();
            return std::make_unique<SetOutputFile>(enable);
        }

        auto result = parse_stmt();
        expect(TokenType::SEMICOLON, "expected ';' after statement");
        expect_end();
        return result;
    }

private:
    Lexer lexer_;
    Token current_;
    // The most recently consumed token. Token.text is a zero-copy string_view into
    // the lexer input buffer, so prev_token_.text.data() + size() marks where that
    // token ended in the original source. Used to capture raw expression text for
    // constant-folded literals (e.g. display_text of "(100-20)").
    Token prev_token_;
    // Tokens scanned ahead of current_ but not yet consumed; filled by peek() and
    // drained by advance(). Lets the parser look ahead without re-lexing on backtrack.
    std::vector<Token> lookahead_;

    void advance() {
        prev_token_ = current_;
        if (!lookahead_.empty()) {
            current_ = lookahead_.front();
            lookahead_.erase(lookahead_.begin());
        } else {
            current_ = lexer_.next_token();
        }
        if (current_.type == TokenType::T_ERROR) {
            error("unexpected token");
        }
    }

    // Return the k-th token ahead of current_ (k >= 1) without consuming it.
    Token peek(size_t k) {
        while (lookahead_.size() < k) {
            Token tok = lexer_.next_token();
            if (tok.type == TokenType::T_ERROR) {
                error("unexpected token");
            }
            lookahead_.push_back(tok);
        }
        return lookahead_[k - 1];
    }

    bool check(TokenType type) const {
        return current_.type == type;
    }

    bool match(TokenType type) {
        if (!check(type)) {
            return false;
        }
        advance();
        return true;
    }

    Token expect(TokenType type, const char* message) {
        if (!check(type)) {
            error(message);
        }
        Token token = current_;
        advance();
        return token;
    }

    [[noreturn]] void error(const std::string& message) const {
        throw ParseError("Parser Error at line " + std::to_string(current_.line) + " column " +
                         std::to_string(current_.column) + ": " + message + " near '" + token_text(current_) + "'");
    }

    void expect_end() {
        if (!check(TokenType::T_EOF)) {
            error("unexpected token after statement");
        }
    }

    void consume_optional_semicolon() {
        match(TokenType::SEMICOLON);
    }

    std::string token_text(const Token& token) const {
        return std::string(token.text);
    }

    std::unique_ptr<TreeNode> parse_stmt() {
        if (check(TokenType::SHOW)) {
            return parse_db_stmt();
        }
        if (check(TokenType::CREATE) || check(TokenType::DROP) || check(TokenType::DESC)) {
            return parse_ddl();
        }
        if (check(TokenType::INSERT) || check(TokenType::DELETE) || check(TokenType::UPDATE)) {
            return parse_dml();
        }
        if (check(TokenType::SELECT) || check(TokenType::EXPLAIN)) {
            return parse_dql();
        }
        if (check(TokenType::BEGIN_KW) || check(TokenType::COMMIT) || check(TokenType::ABORT) ||
            check(TokenType::ROLLBACK)) {
            return parse_txn_stmt();
        }
        if (check(TokenType::SET)) {
            return parse_set_stmt();
        }
        if (check(TokenType::LOAD)) {
            return parse_load_stmt();
        }
        error("unexpected start of statement");
    }

    std::unique_ptr<TreeNode> parse_load_stmt() {
        expect(TokenType::LOAD, "expected LOAD");
        Token path = expect(TokenType::VALUE_PATH, "expected file path after LOAD");
        expect(TokenType::INTO, "expected INTO after file path");
        std::string table = parse_identifier();
        return std::make_unique<LoadStmt>(std::string(path.text), std::move(table));
    }

    std::unique_ptr<TreeNode> parse_db_stmt() {
        expect(TokenType::SHOW, "expected SHOW");
        if (match(TokenType::TABLES)) {
            return std::make_unique<ShowTables>();
        }
        expect(TokenType::INDEX, "expected TABLES or INDEX after SHOW");
        expect(TokenType::FROM, "expected FROM after SHOW INDEX");
        return std::make_unique<ShowIndex>(parse_identifier());
    }

    std::unique_ptr<TreeNode> parse_txn_stmt() {
        if (match(TokenType::BEGIN_KW)) {
            return std::make_unique<TxnBegin>();
        }
        if (match(TokenType::COMMIT)) {
            return std::make_unique<TxnCommit>();
        }
        if (match(TokenType::ABORT)) {
            return std::make_unique<TxnAbort>();
        }
        expect(TokenType::ROLLBACK, "expected transaction statement");
        return std::make_unique<TxnRollback>();
    }

    std::unique_ptr<TreeNode> parse_set_stmt() {
        expect(TokenType::SET, "expected SET");
        if (match(TokenType::TRANSACTION)) {
            expect(TokenType::ISOLATION, "expected ISOLATION");
            expect(TokenType::LEVEL, "expected LEVEL");
            if (match(TokenType::SNAPSHOT)) {
                expect(TokenType::ISOLATION, "expected ISOLATION after SNAPSHOT");
                return std::make_unique<SetTransaction>(IsolationLevelType::SNAPSHOT_ISOLATION);
            }
            expect(TokenType::SERIALIZABLE, "expected SNAPSHOT ISOLATION or SERIALIZABLE");
            return std::make_unique<SetTransaction>(IsolationLevelType::SERIALIZABLE);
        }

        SetKnobType knob = EnableNestLoop;
        if (match(TokenType::ENABLE_NESTLOOP)) {
            knob = EnableNestLoop;
        } else if (match(TokenType::ENABLE_SORTMERGE)) {
            knob = EnableSortMerge;
        } else {
            error("expected set knob name");
        }
        expect(TokenType::EQ, "expected '=' after set knob");
        if (!check(TokenType::VALUE_BOOL)) {
            error("expected boolean value");
        }
        auto bool_lit = parse_bool_literal();
        return std::make_unique<SetStmt>(knob, bool_lit->val);
    }

    std::unique_ptr<TreeNode> parse_ddl() {
        if (match(TokenType::CREATE)) {
            if (match(TokenType::TABLE)) {
                std::string table = parse_identifier();
                expect(TokenType::LPAREN, "expected '(' after table name");
                auto fields = parse_field_list();
                expect(TokenType::RPAREN, "expected ')' after field list");
                return std::make_unique<CreateTable>(std::move(table), std::move(fields));
            }
            if (match(TokenType::INDEX)) {
                std::string table = parse_identifier();
                expect(TokenType::LPAREN, "expected '(' after table name");
                auto columns = parse_col_name_list();
                expect(TokenType::RPAREN, "expected ')' after column list");
                return std::make_unique<CreateIndex>(std::move(table), std::move(columns));
            }
            expect(TokenType::STATIC_CHECKPOINT, "expected TABLE, INDEX, or STATIC_CHECKPOINT after CREATE");
            return std::make_unique<StaticCheckpoint>();
        }

        if (match(TokenType::DROP)) {
            if (match(TokenType::TABLE)) {
                return std::make_unique<DropTable>(parse_identifier());
            }
            expect(TokenType::INDEX, "expected TABLE or INDEX after DROP");
            std::string table = parse_identifier();
            expect(TokenType::LPAREN, "expected '(' after table name");
            auto columns = parse_col_name_list();
            expect(TokenType::RPAREN, "expected ')' after column list");
            return std::make_unique<DropIndex>(std::move(table), std::move(columns));
        }

        expect(TokenType::DESC, "expected DDL statement");
        return std::make_unique<DescTable>(parse_identifier());
    }

    std::unique_ptr<TreeNode> parse_dml() {
        if (match(TokenType::INSERT)) {
            expect(TokenType::INTO, "expected INTO after INSERT");
            std::string table = parse_identifier();
            std::vector<std::string> column_names;
            if (match(TokenType::LPAREN)) {
                column_names = parse_col_name_list();
                expect(TokenType::RPAREN, "expected ')' after INSERT column list");
            }
            if (check(TokenType::SELECT)) {
                auto select = parse_query_chain();
                return std::make_unique<InsertStmt>(std::move(table), std::move(column_names), std::move(select));
            }
            expect(TokenType::VALUES, "expected VALUES after table name");
            expect(TokenType::LPAREN, "expected '(' before values");
            auto values = parse_value_list();
            expect(TokenType::RPAREN, "expected ')' after values");
            return std::make_unique<InsertStmt>(std::move(table), std::move(column_names), std::move(values));
        }
        if (match(TokenType::DELETE)) {
            expect(TokenType::FROM, "expected FROM after DELETE");
            std::string table = parse_identifier();
            auto where_expr = parse_optional_boolean_expr(TokenType::WHERE);
            auto conds = flatten_legacy_conditions(where_expr.get());
            auto result = std::make_unique<DeleteStmt>(std::move(table), std::move(conds));
            result->where_expr = std::move(where_expr);
            return result;
        }
        // UPDATE
        expect(TokenType::UPDATE, "expected DML statement");
        std::string table = parse_identifier();
        expect(TokenType::SET, "expected SET after table name");
        auto clauses = parse_set_clause_list();
        auto where_expr = parse_optional_boolean_expr(TokenType::WHERE);
        auto conds = flatten_legacy_conditions(where_expr.get());
        auto result = std::make_unique<UpdateStmt>(std::move(table), std::move(clauses), std::move(conds));
        result->where_expr = std::move(where_expr);
        return result;
    }

    std::unique_ptr<TreeNode> parse_dql() {
        if (match(TokenType::EXPLAIN)) {
            expect(TokenType::ANALYZE, "expected ANALYZE after EXPLAIN");
            return std::make_unique<ExplainAnalyze>(parse_select_stmt());
        }
        if (is_select_from_union_wrapper()) {
            return parse_select_from_union();
        }
        return parse_query_chain();
    }

    bool is_select_from_union_wrapper() {
        // "SELECT * FROM (" distinguishes the union wrapper from a plain SELECT.
        // peek() buffers the scanned tokens so the subsequent parse consumes them
        // without re-lexing (the previous save/restore re-scanned them).
        return check(TokenType::SELECT) && peek(1).type == TokenType::STAR && peek(2).type == TokenType::FROM &&
               peek(3).type == TokenType::LPAREN;
    }

    std::unique_ptr<TreeNode> parse_select_from_union() {
        expect(TokenType::SELECT, "expected SELECT");
        expect(TokenType::STAR, "expected '*' before union wrapper");
        expect(TokenType::FROM, "expected FROM before union wrapper");
        expect(TokenType::LPAREN, "expected '(' before union query");
        auto union_stmt = parse_union_query();
        expect(TokenType::RPAREN, "expected ')' after union query");
        expect(TokenType::AS, "expected AS after union query");
        std::string alias = parse_identifier();
        auto order = parse_opt_order_clause();
        auto pagination = parse_opt_pagination_clause();
        return std::make_unique<SelectFromUnionStmt>(std::move(union_stmt), std::move(alias), std::move(order),
                                                     pagination.has_limit, pagination.limit, pagination.has_offset,
                                                     pagination.offset);
    }

    std::unique_ptr<SelectStmt> parse_select_stmt() {
        expect(TokenType::SELECT, "expected SELECT");
        bool has_distinct = match(TokenType::DISTINCT);
        bool has_star = match(TokenType::STAR);
        std::vector<std::unique_ptr<SelectItem>> items;
        if (!has_star) {
            items = parse_select_item_list();
        }
        expect(TokenType::FROM, "expected FROM in SELECT");
        return parse_select_tail(has_star, has_distinct, std::move(items));
    }

    std::unique_ptr<SelectStmt> parse_select_tail(bool has_star, bool has_distinct,
                                                  std::vector<std::unique_ptr<SelectItem>> items) {
        auto from = parse_from_clause();
        auto where_expr = parse_optional_boolean_expr(TokenType::WHERE);
        auto conds = std::move(from->conds);
        auto where_conds = flatten_legacy_conditions(where_expr.get());
        conds.insert(conds.end(), std::make_move_iterator(where_conds.begin()),
                     std::make_move_iterator(where_conds.end()));
        auto group_by = parse_opt_group_clause();
        auto having = parse_opt_having_clause();
        auto order = parse_opt_order_clause();
        auto pagination = parse_opt_pagination_clause();
        return std::make_unique<SelectStmt>(std::move(items), from->tables, std::move(conds), std::move(group_by),
                                            std::move(having), std::move(order), pagination.has_limit, pagination.limit,
                                            has_star, std::move(from->jointree), has_distinct, pagination.has_offset,
                                            pagination.offset, std::move(where_expr));
    }

    std::unique_ptr<TreeNode> parse_query_chain() {
        auto first = parse_select_stmt();
        if (!check(TokenType::UNION) && !check(TokenType::INTERSECT) && !check(TokenType::EXCEPT)) {
            return first;
        }

        return parse_set_operation(std::move(first));
    }

    std::unique_ptr<UnionStmt> parse_union_query() {
        auto first = parse_select_stmt();
        return parse_set_operation(std::move(first));
    }

    std::unique_ptr<UnionStmt> parse_set_operation(std::unique_ptr<SelectStmt> first) {
        std::vector<std::unique_ptr<SelectStmt>> branches;
        std::vector<bool> union_all;
        std::vector<SetOperator> operators;
        branches.push_back(std::move(first));
        while (check(TokenType::UNION) || check(TokenType::INTERSECT) || check(TokenType::EXCEPT)) {
            SetOperator op;
            if (match(TokenType::UNION)) {
                op = SetOperator::UNION;
            } else if (match(TokenType::INTERSECT)) {
                op = SetOperator::INTERSECT;
            } else {
                expect(TokenType::EXCEPT, "expected set operator");
                op = SetOperator::EXCEPT;
            }
            operators.push_back(op);
            union_all.push_back(op == SetOperator::UNION && match(TokenType::ALL));
            branches.push_back(parse_select_stmt());
        }
        if (branches.size() < 2) {
            error("expected SELECT after set operator");
        }
        return std::make_unique<UnionStmt>(std::move(branches), std::move(union_all), std::move(operators));
    }

    std::vector<std::unique_ptr<Field>> parse_field_list() {
        std::vector<std::unique_ptr<Field>> fields;
        fields.push_back(parse_field());
        while (match(TokenType::COMMA)) {
            fields.push_back(parse_field());
        }
        return fields;
    }

    std::unique_ptr<Field> parse_field() {
        std::string column = parse_identifier();
        auto type = parse_type();
        return std::make_unique<ColDef>(std::move(column), std::move(type));
    }

    std::unique_ptr<TypeLen> parse_type() {
        if (match(TokenType::INT)) {
            return std::make_unique<TypeLen>(SV_TYPE_INT, sizeof(int));
        }
        if (match(TokenType::FLOAT)) {
            return std::make_unique<TypeLen>(SV_TYPE_FLOAT, sizeof(double));
        }
        if (match(TokenType::DATETIME)) {
            return std::make_unique<TypeLen>(SV_TYPE_DATETIME, 19);
        }
        expect(TokenType::CHAR, "expected type");
        expect(TokenType::LPAREN, "expected '(' after CHAR");
        auto length = parse_int_literal();
        expect(TokenType::RPAREN, "expected ')' after CHAR length");
        return std::make_unique<TypeLen>(SV_TYPE_STRING, length->val);
    }

    std::vector<std::string> parse_col_name_list() {
        std::vector<std::string> columns{parse_identifier()};
        while (match(TokenType::COMMA)) {
            columns.push_back(parse_identifier());
        }
        return columns;
    }

    std::vector<std::unique_ptr<Value>> parse_value_list() {
        std::vector<std::unique_ptr<Value>> values;
        values.push_back(parse_value());
        while (match(TokenType::COMMA)) {
            values.push_back(parse_value());
        }
        return values;
    }

    std::unique_ptr<Value> parse_value() {
        bool is_negative = match(TokenType::MINUS);

        if (check(TokenType::VALUE_INT)) {
            return parse_int_literal(is_negative);
        }
        if (check(TokenType::VALUE_FLOAT)) {
            return parse_float_literal(is_negative);
        }
        if (is_negative) {
            error("expected numeric value after '-'");
        }
        if (check(TokenType::VALUE_STRING)) {
            return parse_string_literal();
        }
        if (check(TokenType::VALUE_BOOL)) {
            return parse_bool_literal();
        }
        if (match(TokenType::NULL_KW)) {
            return std::make_unique<NullLit>();
        }
        error("expected value");
    }

    std::unique_ptr<Value> parse_numeric_delta_after(TokenType op) {
        auto delta = parse_value();
        if (op == TokenType::PLUS || op == TokenType::MINUS || op == TokenType::STAR || op == TokenType::SLASH) {
            if (delta->type != AstType::IntLit && delta->type != AstType::FloatLit) {
                error("expected numeric value after arithmetic operator");
            }
            return delta;
        }
        error("expected numeric value after arithmetic operator");
    }

    std::unique_ptr<IntLit> parse_int_literal(bool is_negative = false) {
        Token token = expect(TokenType::VALUE_INT, "expected integer");
        const int64_t lower =
            is_negative ? static_cast<int64_t>(std::numeric_limits<int>::max()) + 1 : std::numeric_limits<int>::min();
        const int64_t upper = std::numeric_limits<int>::max();
        if ((!is_negative && (token.int_value < lower || token.int_value > upper)) ||
            (is_negative && (token.int_value < 0 || token.int_value > lower))) {
            error("integer literal out of range");
        }

        int val = 0;
        if (is_negative && token.int_value == lower) {
            val = std::numeric_limits<int>::min();
        } else {
            val = static_cast<int>(token.int_value);
            if (is_negative) {
                val = -val;
            }
        }

        std::string display = token_text(token);
        if (is_negative) {
            display = "-" + display;
        }
        return std::make_unique<IntLit>(val, std::move(display));
    }

    std::unique_ptr<FloatLit> parse_float_literal(bool is_negative = false) {
        Token token = expect(TokenType::VALUE_FLOAT, "expected float");
        double val = token.float_value;
        std::string display = token_text(token);
        if (is_negative) {
            val = -val;
            display = "-" + display;
        }
        return std::make_unique<FloatLit>(val, std::move(display));
    }

    std::unique_ptr<StringLit> parse_string_literal() {
        Token token = expect(TokenType::VALUE_STRING, "expected string");
        return std::make_unique<StringLit>(token_text(token), "'" + token_text(token) + "'");
    }

    std::unique_ptr<BoolLit> parse_bool_literal() {
        Token token = expect(TokenType::VALUE_BOOL, "expected boolean");
        return std::make_unique<BoolLit>(token.bool_value, token_text(token));
    }

    std::unique_ptr<BinaryExpr> parse_condition() {
        auto expression = parse_boolean_primary();
        auto* condition = dynamic_cast<BinaryExpr*>(expression.get());
        if (condition == nullptr) {
            error("expected comparison condition");
        }
        expression.release();
        return std::unique_ptr<BinaryExpr>(condition);
    }

    std::unique_ptr<BinaryExpr> parse_general_condition() {
        return parse_condition();
    }

    std::unique_ptr<Expr> parse_optional_boolean_expr(TokenType marker) {
        if (!match(marker)) {
            return nullptr;
        }
        return parse_boolean_expr();
    }

    std::unique_ptr<Expr> parse_boolean_expr() {
        return parse_or_expr();
    }

    std::unique_ptr<Expr> parse_or_expr() {
        auto lhs = parse_and_expr();
        while (match(TokenType::OR)) {
            auto rhs = parse_and_expr();
            std::vector<std::unique_ptr<Expr>> operands;
            operands.push_back(std::move(lhs));
            operands.push_back(std::move(rhs));
            lhs = std::make_unique<LogicalExpr>(LogicalOp::OR, std::move(operands));
        }
        return lhs;
    }

    std::unique_ptr<Expr> parse_and_expr() {
        auto lhs = parse_not_expr();
        while (match(TokenType::AND)) {
            auto rhs = parse_not_expr();
            std::vector<std::unique_ptr<Expr>> operands;
            operands.push_back(std::move(lhs));
            operands.push_back(std::move(rhs));
            lhs = std::make_unique<LogicalExpr>(LogicalOp::AND, std::move(operands));
        }
        return lhs;
    }

    std::unique_ptr<Expr> parse_not_expr() {
        if (match(TokenType::NOT)) {
            std::vector<std::unique_ptr<Expr>> operands;
            operands.push_back(parse_not_expr());
            return std::make_unique<LogicalExpr>(LogicalOp::NOT, std::move(operands));
        }
        return parse_boolean_primary();
    }

    std::unique_ptr<Expr> parse_boolean_primary() {
        if (match(TokenType::LPAREN)) {
            auto expression = parse_boolean_expr();
            expect(TokenType::RPAREN, "expected ')' after boolean expression");
            return expression;
        }

        if (match(TokenType::EXISTS)) {
            return std::make_unique<BinaryExpr>(nullptr, SV_OP_EXISTS, parse_subquery_expr());
        }

        auto lhs = parse_value_expr();
        bool negated = match(TokenType::NOT);
        if (match(TokenType::IS)) {
            if (negated) {
                error("expected LIKE, IN, or BETWEEN after NOT");
            }
            bool is_not = match(TokenType::NOT);
            expect(TokenType::NULL_KW, "expected NULL after IS");
            return std::make_unique<BinaryExpr>(std::move(lhs), is_not ? SV_OP_IS_NOT_NULL : SV_OP_IS_NULL, nullptr);
        }
        if (match(TokenType::LIKE)) {
            return std::make_unique<BinaryExpr>(std::move(lhs), SV_OP_LIKE, parse_value_expr(), negated);
        }
        if (match(TokenType::IN)) {
            auto result = std::make_unique<BinaryExpr>(std::move(lhs), SV_OP_IN, nullptr, negated);
            expect(TokenType::LPAREN, "expected '(' after IN");
            if (check(TokenType::RPAREN)) {
                error("expected value in IN list");
            }
            if (check(TokenType::SELECT)) {
                auto query = parse_query_chain();
                expect(TokenType::RPAREN, "expected ')' after IN subquery");
                result->rhs = std::make_unique<SubqueryExpr>(std::move(query));
                return result;
            }
            result->rhs_list.push_back(parse_value_expr());
            while (match(TokenType::COMMA)) {
                result->rhs_list.push_back(parse_value_expr());
            }
            expect(TokenType::RPAREN, "expected ')' after IN list");
            return result;
        }
        if (match(TokenType::BETWEEN)) {
            auto result = std::make_unique<BinaryExpr>(std::move(lhs), SV_OP_BETWEEN, parse_value_expr(), negated);
            expect(TokenType::AND, "expected AND in BETWEEN predicate");
            result->rhs_upper = parse_value_expr();
            return result;
        }
        SvCompOp op;
        if (try_parse_op(op)) {
            if (negated) {
                error("expected LIKE, IN, or BETWEEN after NOT");
            }
            auto result = std::make_unique<BinaryExpr>(std::move(lhs), op, nullptr);
            if (match(TokenType::ANY)) {
                result->quantifier = Quantifier::ANY;
                result->rhs = parse_subquery_expr();
            } else if (match(TokenType::ALL)) {
                result->quantifier = Quantifier::ALL;
                result->rhs = parse_subquery_expr();
            } else {
                result->rhs = parse_value_expr();
            }
            return result;
        }
        if (negated) {
            error("expected LIKE, IN, or BETWEEN after NOT");
        }
        return lhs;
    }

    std::vector<std::unique_ptr<BinaryExpr>> flatten_legacy_conditions(const Expr* expression) {
        std::vector<std::unique_ptr<BinaryExpr>> result;
        if (expression == nullptr) {
            return result;
        }
        if (auto condition = dynamic_cast<const BinaryExpr*>(expression); condition != nullptr) {
            if (condition->lhs == nullptr ||
                (condition->rhs != nullptr && condition->rhs->type == AstType::SubqueryExpr)) {
                return result;
            }
            result.push_back(clone_binary_expr(*condition));
            return result;
        }
        auto logical = dynamic_cast<const LogicalExpr*>(expression);
        if (logical == nullptr || logical->op != LogicalOp::AND) {
            return result;
        }
        for (const auto& operand : logical->operands) {
            auto nested = flatten_legacy_conditions(operand.get());
            result.insert(result.end(), std::make_move_iterator(nested.begin()), std::make_move_iterator(nested.end()));
        }
        return result;
    }

    bool try_parse_op(SvCompOp& op) {
        if (match(TokenType::EQ)) {
            op = SV_OP_EQ;
            return true;
        }
        if (match(TokenType::LT)) {
            op = SV_OP_LT;
            return true;
        }
        if (match(TokenType::GT)) {
            op = SV_OP_GT;
            return true;
        }
        if (match(TokenType::NEQ)) {
            op = SV_OP_NE;
            return true;
        }
        if (match(TokenType::LEQ)) {
            op = SV_OP_LE;
            return true;
        }
        if (match(TokenType::GEQ)) {
            op = SV_OP_GE;
            return true;
        }
        return false;
    }

    std::unique_ptr<Expr> parse_subquery_expr() {
        expect(TokenType::LPAREN, "expected '(' before subquery");
        if (!check(TokenType::SELECT)) {
            error("expected SELECT in subquery");
        }
        auto query = parse_query_chain();
        expect(TokenType::RPAREN, "expected ')' after subquery");
        return std::make_unique<SubqueryExpr>(std::move(query));
    }

    std::unique_ptr<Expr> parse_value_expr() {
        const char* start = current_.text.data();
        auto expression = parse_arithmetic_additive();
        if (auto value = dynamic_cast<Value*>(expression.get()); value != nullptr) {
            const char* end = prev_token_.text.data() + prev_token_.text.size();
            value->display_text = std::string(start, static_cast<size_t>(end - start));
        }
        return expression;
    }

    std::unique_ptr<Expr> parse_arithmetic_additive(std::unique_ptr<Expr> expression = nullptr) {
        expression = parse_arithmetic_multiplicative(std::move(expression));
        while (check(TokenType::PLUS) || check(TokenType::MINUS)) {
            TokenType op = current_.type;
            advance();
            expression = make_arithmetic_expr(op, std::move(expression), parse_arithmetic_multiplicative());
        }
        return expression;
    }

    std::unique_ptr<Expr> parse_arithmetic_multiplicative(std::unique_ptr<Expr> expression = nullptr) {
        if (expression == nullptr) {
            expression = parse_arithmetic_atom();
        }
        while (check(TokenType::STAR) || check(TokenType::SLASH)) {
            TokenType op = current_.type;
            advance();
            expression = make_arithmetic_expr(op, std::move(expression), parse_arithmetic_atom());
        }
        return expression;
    }

    std::unique_ptr<Expr> parse_arithmetic_atom() {
        if (match(TokenType::MINUS)) {
            if (check(TokenType::VALUE_INT)) {
                return parse_int_literal(true);
            }
            if (check(TokenType::VALUE_FLOAT)) {
                return parse_float_literal(true);
            }
            auto zero = std::make_unique<IntLit>(0);
            return make_arithmetic_expr(TokenType::MINUS, std::move(zero), parse_arithmetic_atom());
        }
        if (is_window_function_start(current_.type)) {
            return parse_window_function_expr();
        }
        if (current_.type == TokenType::SUM || current_.type == TokenType::AVG) {
            return parse_sum_avg_expr();
        }
        if (is_aggregate_start(current_.type)) {
            return parse_aggregate_expr();
        }
        if (check(TokenType::CASE)) {
            return parse_case_expr();
        }
        if (check(TokenType::IDENTIFIER)) {
            return parse_col();
        }
        if (match(TokenType::LPAREN)) {
            if (check(TokenType::SELECT)) {
                auto query = parse_query_chain();
                expect(TokenType::RPAREN, "expected ')' after subquery");
                return std::make_unique<SubqueryExpr>(std::move(query));
            }
            auto expression = parse_value_expr();
            expect(TokenType::RPAREN, "expected ')' after expression");
            return expression;
        }
        return parse_value();
    }

    std::unique_ptr<Expr> make_arithmetic_expr(TokenType token, std::unique_ptr<Expr> lhs,
                                                std::unique_ptr<Expr> rhs) {
        ArithmeticOp op;
        switch (token) {
        case TokenType::PLUS:
            op = ArithmeticOp::ADD;
            break;
        case TokenType::MINUS:
            op = ArithmeticOp::SUB;
            break;
        case TokenType::STAR:
            op = ArithmeticOp::MUL;
            break;
        case TokenType::SLASH:
            op = ArithmeticOp::DIV;
            break;
        default:
            error("expected arithmetic operator");
        }

        auto is_numeric_literal = [](const Expr* value) {
            return value != nullptr && (value->type == AstType::IntLit || value->type == AstType::FloatLit);
        };
        if (lhs->type == AstType::StringLit || lhs->type == AstType::BoolLit || lhs->type == AstType::NullLit ||
            rhs->type == AstType::StringLit || rhs->type == AstType::BoolLit || rhs->type == AstType::NullLit) {
            error("arithmetic requires numeric operands");
        }
        if (is_numeric_literal(lhs.get()) && is_numeric_literal(rhs.get())) {
            std::unique_ptr<Value> lhs_value(static_cast<Value*>(lhs.release()));
            std::unique_ptr<Value> rhs_value(static_cast<Value*>(rhs.release()));
            return fold_binary(token, std::move(lhs_value), std::move(rhs_value));
        }
        return std::make_unique<ArithmeticExpr>(op, std::move(lhs), std::move(rhs));
    }

    std::unique_ptr<Expr> parse_case_expr() {
        expect(TokenType::CASE, "expected CASE");
        std::vector<CaseWhen> when_clauses;
        while (match(TokenType::WHEN)) {
            auto condition = parse_boolean_expr();
            expect(TokenType::THEN, "expected THEN in CASE expression");
            auto result = parse_value_expr();
            when_clauses.emplace_back(std::move(condition), std::move(result));
        }
        if (when_clauses.empty()) {
            error("CASE requires at least one WHEN clause");
        }
        std::unique_ptr<Expr> else_expr;
        if (match(TokenType::ELSE)) {
            else_expr = parse_value_expr();
        }
        expect(TokenType::END, "expected END after CASE expression");
        return std::make_unique<CaseExpr>(std::move(when_clauses), std::move(else_expr));
    }

    std::unique_ptr<Col> parse_col() {
        std::string first = parse_identifier();
        if (match(TokenType::DOT)) {
            std::string column = parse_identifier();
            return std::make_unique<Col>(std::move(first), std::move(column));
        }
        return std::make_unique<Col>("", std::move(first));
    }

    std::vector<std::unique_ptr<Col>> parse_col_list() {
        std::vector<std::unique_ptr<Col>> columns;
        columns.push_back(parse_col());
        while (match(TokenType::COMMA)) {
            columns.push_back(parse_col());
        }
        return columns;
    }

    std::vector<std::unique_ptr<SetClause>> parse_set_clause_list() {
        std::vector<std::unique_ptr<SetClause>> clauses;
        clauses.push_back(parse_set_clause());
        while (match(TokenType::COMMA)) {
            clauses.push_back(parse_set_clause());
        }
        return clauses;
    }

    std::unique_ptr<SetClause> parse_set_clause() {
        std::string column = parse_identifier();

        // 复合赋值: col += num / col -= num,脱糖为 col = col ± num
        if (match(TokenType::PLUS_ASSIGN)) {
            auto rhs_col = std::make_unique<Col>("", column);
            return std::make_unique<SetClause>(std::move(column), std::move(rhs_col),
                                               parse_numeric_delta_after(TokenType::PLUS), SetOp::SELF_ADD);
        }
        if (match(TokenType::MINUS_ASSIGN)) {
            auto rhs_col = std::make_unique<Col>("", column);
            return std::make_unique<SetClause>(std::move(column), std::move(rhs_col),
                                               parse_numeric_delta_after(TokenType::MINUS), SetOp::SELF_SUB);
        }

        expect(TokenType::EQ, "expected '=' in SET clause");
        // A column reference on the rhs starts the self-referential form
        // (col = col +/- value); any other rhs is a plain value. The two forms have
        // disjoint FIRST sets, so a single token decides -- no speculative parse needed.
        if (check(TokenType::IDENTIFIER)) {
            auto rhs_col = parse_col();
            if (rhs_col->col_name != column) {
                return std::make_unique<SetClause>(std::move(column), parse_arithmetic_additive(std::move(rhs_col)));
            }
            if (match(TokenType::PLUS)) {
                return std::make_unique<SetClause>(std::move(column), std::move(rhs_col),
                                                   parse_numeric_delta_after(TokenType::PLUS), SetOp::SELF_ADD);
            }
            if (match(TokenType::MINUS)) {
                return std::make_unique<SetClause>(std::move(column), std::move(rhs_col),
                                                   parse_numeric_delta_after(TokenType::MINUS), SetOp::SELF_SUB);
            }
            if (match(TokenType::STAR)) {
                return std::make_unique<SetClause>(std::move(column), std::move(rhs_col),
                                                   parse_numeric_delta_after(TokenType::STAR), SetOp::SELF_MUL);
            }
            if (match(TokenType::SLASH)) {
                return std::make_unique<SetClause>(std::move(column), std::move(rhs_col),
                                                   parse_numeric_delta_after(TokenType::SLASH), SetOp::SELF_DIV);
            }
            return std::make_unique<SetClause>(std::move(column), std::move(rhs_col), nullptr, SetOp::ASSIGNMENT);
        }
        return std::make_unique<SetClause>(std::move(column), parse_value_expr());
    }

    std::vector<std::unique_ptr<SelectItem>> parse_select_item_list() {
        std::vector<std::unique_ptr<SelectItem>> items;
        items.push_back(parse_select_item());
        while (match(TokenType::COMMA)) {
            items.push_back(parse_select_item());
        }
        return items;
    }

    std::unique_ptr<SelectItem> parse_select_item() {
        auto expr = parse_value_expr();
        std::string alias;
        if (match(TokenType::AS)) {
            alias = parse_identifier();
        } else if (check(TokenType::IDENTIFIER)) {
            alias = parse_identifier();
        }
        return std::make_unique<SelectItem>(std::move(expr), std::move(alias));
    }

    bool is_aggregate_start(TokenType type) const {
        return type == TokenType::COUNT || type == TokenType::MAX || type == TokenType::MIN || type == TokenType::SUM ||
               type == TokenType::AVG;
    }

    std::unique_ptr<Expr> parse_aggregate_expr() {
        AggFuncType func;
        if (match(TokenType::COUNT)) {
            func = AGG_COUNT;
        } else if (match(TokenType::MAX)) {
            func = AGG_MAX;
        } else if (match(TokenType::MIN)) {
            func = AGG_MIN;
        } else if (match(TokenType::SUM)) {
            func = AGG_SUM;
        } else {
            expect(TokenType::AVG, "expected aggregate function");
            func = AGG_AVG;
        }
        expect(TokenType::LPAREN, "expected '(' after aggregate function");
        if (func == AGG_COUNT && match(TokenType::STAR)) {
            expect(TokenType::RPAREN, "expected ')' after COUNT(*)");
            return std::make_unique<AggExpr>(func, true, nullptr);
        }
        bool is_distinct = match(TokenType::DISTINCT);
        auto column = parse_col();
        expect(TokenType::RPAREN, "expected ')' after aggregate argument");
        return std::make_unique<AggExpr>(func, false, std::move(column), is_distinct);
    }

    bool is_window_function_start(TokenType type) const {
        return type == TokenType::ROW_NUMBER || type == TokenType::RANK || type == TokenType::DENSE_RANK ||
               type == TokenType::LAG || type == TokenType::LEAD;
    }

    WindowFuncType parse_window_func_type() {
        if (match(TokenType::ROW_NUMBER)) {
            return WindowFuncType::ROW_NUMBER;
        }
        if (match(TokenType::RANK)) {
            return WindowFuncType::RANK;
        }
        if (match(TokenType::DENSE_RANK)) {
            return WindowFuncType::DENSE_RANK;
        }
        if (match(TokenType::LAG)) {
            return WindowFuncType::LAG;
        }
        expect(TokenType::LEAD, "expected window function");
        return WindowFuncType::LEAD;
    }

    std::vector<std::unique_ptr<Expr>> parse_window_arguments(WindowFuncType func) {
        std::vector<std::unique_ptr<Expr>> args;
        expect(TokenType::LPAREN, "expected '(' after window function");
        if (func == WindowFuncType::ROW_NUMBER || func == WindowFuncType::RANK ||
            func == WindowFuncType::DENSE_RANK) {
            expect(TokenType::RPAREN, "ranking functions do not accept arguments");
            return args;
        }
        args.push_back(parse_value_expr());
        while (match(TokenType::COMMA)) {
            if ((func == WindowFuncType::LAG || func == WindowFuncType::LEAD) && args.size() >= 3) {
                error("LAG and LEAD accept at most three arguments");
            }
            args.push_back(parse_value_expr());
        }
        expect(TokenType::RPAREN, "expected ')' after window function arguments");
        return args;
    }

    std::unique_ptr<WindowExpr> parse_window_function_expr() {
        auto func = parse_window_func_type();
        auto args = parse_window_arguments(func);
        expect(TokenType::OVER, "expected OVER after window function");
        auto window_spec = parse_window_spec();
        return std::make_unique<WindowExpr>(func, std::move(args), std::move(window_spec.first),
                                            std::move(window_spec.second));
    }

    std::unique_ptr<Expr> parse_sum_avg_expr() {
        const bool is_sum = match(TokenType::SUM);
        if (!is_sum) {
            expect(TokenType::AVG, "expected SUM or AVG");
        }
        const auto func = is_sum ? WindowFuncType::SUM : WindowFuncType::AVG;
        expect(TokenType::LPAREN, "expected '(' after aggregate function");
        const bool is_distinct = match(TokenType::DISTINCT);
        auto argument = parse_value_expr();
        expect(TokenType::RPAREN, "expected ')' after aggregate argument");
        if (!match(TokenType::OVER)) {
            auto column = dynamic_cast<Col*>(argument.get());
            if (column == nullptr) {
                error("aggregate argument must be a column");
            }
            const auto aggregate_func = is_sum ? AGG_SUM : AGG_AVG;
            return std::make_unique<AggExpr>(aggregate_func, false, clone_col(*column), is_distinct);
        }
        if (is_distinct) {
            error("window aggregate does not support DISTINCT");
        }
        auto window_spec = parse_window_spec();
        std::vector<std::unique_ptr<Expr>> args;
        args.push_back(std::move(argument));
        return std::make_unique<WindowExpr>(func, std::move(args), std::move(window_spec.first),
                                            std::move(window_spec.second));
    }

    std::pair<std::vector<std::unique_ptr<Expr>>, std::vector<std::unique_ptr<OrderByItem>>> parse_window_spec() {
        std::vector<std::unique_ptr<Expr>> partition_by;
        std::vector<std::unique_ptr<OrderByItem>> order_by;
        expect(TokenType::LPAREN, "expected '(' after OVER");
        if (match(TokenType::PARTITION)) {
            expect(TokenType::BY, "expected BY after PARTITION");
            partition_by.push_back(parse_value_expr());
            while (match(TokenType::COMMA)) {
                partition_by.push_back(parse_value_expr());
            }
        }
        if (match(TokenType::ORDER)) {
            expect(TokenType::BY, "expected BY after ORDER");
            order_by.push_back(parse_order_item());
            while (match(TokenType::COMMA)) {
                order_by.push_back(parse_order_item());
            }
        }
        expect(TokenType::RPAREN, "expected ')' after window specification");
        return {std::move(partition_by), std::move(order_by)};
    }

    std::vector<std::unique_ptr<Col>> parse_opt_group_clause() {
        if (!match(TokenType::GROUP)) {
            return {};
        }
        expect(TokenType::BY, "expected BY after GROUP");
        return parse_col_list();
    }

    std::vector<std::unique_ptr<HavingExpr>> parse_opt_having_clause() {
        if (!match(TokenType::HAVING)) {
            return {};
        }
        std::vector<std::unique_ptr<HavingExpr>> conds;
        conds.push_back(parse_having_condition());
        while (match(TokenType::AND)) {
            conds.push_back(parse_having_condition());
        }
        return conds;
    }

    std::unique_ptr<HavingExpr> parse_having_condition() {
        auto cond = parse_general_condition();
        auto result = std::make_unique<HavingExpr>(std::move(cond->lhs), cond->op, std::move(cond->rhs), cond->negated);
        result->rhs_upper = std::move(cond->rhs_upper);
        result->rhs_list = std::move(cond->rhs_list);
        return result;
    }

    std::unique_ptr<Expr> parse_general_expr() {
        return parse_value_expr();
    }

    // Fold a binary arithmetic op of two constant literals into a single literal.
    // Only IntLit/FloatLit participate; other value types raise a ParseError. Overflow
    // and division-by-zero are detected here so they surface as parse-time errors.
    std::unique_ptr<Value> fold_binary(TokenType op, std::unique_ptr<Value> lhs, std::unique_ptr<Value> rhs) {
        auto is_int = [](const Value* v) { return v->type == AstType::IntLit; };
        auto is_float = [](const Value* v) { return v->type == AstType::FloatLit; };
        if (!is_int(lhs.get()) && !is_float(lhs.get())) {
            error("arithmetic on non-numeric constant");
        }
        if (!is_int(rhs.get()) && !is_float(rhs.get())) {
            error("arithmetic on non-numeric constant");
        }
        bool both_int = is_int(lhs.get()) && is_int(rhs.get());
        if (both_int) {
            int64_t l = static_cast<const IntLit*>(lhs.get())->val;
            int64_t r = static_cast<const IntLit*>(rhs.get())->val;
            int64_t res = 0;
            switch (op) {
            case TokenType::PLUS:
                res = l + r;
                break;
            case TokenType::MINUS:
                res = l - r;
                break;
            case TokenType::STAR:
                res = l * r;
                break;
            case TokenType::SLASH:
                if (r == 0) {
                    error("division by zero in constant expression");
                }
                res = l / r;
                break;
            default:
                error("invalid arithmetic operator");
            }
            if (res < std::numeric_limits<int>::min() || res > std::numeric_limits<int>::max()) {
                error("constant arithmetic overflow");
            }
            return std::make_unique<IntLit>(static_cast<int>(res), "");
        }
        double l = is_float(lhs.get()) ? static_cast<const FloatLit*>(lhs.get())->val
                                       : static_cast<const IntLit*>(lhs.get())->val;
        double r = is_float(rhs.get()) ? static_cast<const FloatLit*>(rhs.get())->val
                                       : static_cast<const IntLit*>(rhs.get())->val;
        double res = 0;
        switch (op) {
        case TokenType::PLUS:
            res = l + r;
            break;
        case TokenType::MINUS:
            res = l - r;
            break;
        case TokenType::STAR:
            res = l * r;
            break;
        case TokenType::SLASH:
            if (r == 0.0) {
                error("division by zero in constant expression");
            }
            res = l / r;
            break;
        default:
            error("invalid arithmetic operator");
        }
        return std::make_unique<FloatLit>(res, "");
    }

    std::unique_ptr<FromClause> parse_from_clause() {
        auto from = std::make_unique<FromClause>();
        from->tables.push_back(parse_table_ref());
        while (true) {
            if (match(TokenType::COMMA)) {
                from->tables.push_back(parse_table_ref());
                continue;
            }
            JoinType join_type = INNER_JOIN;
            bool requires_on = true;
            if (match(TokenType::CROSS)) {
                expect(TokenType::JOIN, "expected JOIN after CROSS");
                join_type = CROSS_JOIN;
                requires_on = false;
            } else if (match(TokenType::NATURAL)) {
                expect(TokenType::JOIN, "expected JOIN after NATURAL");
                join_type = NATURAL_JOIN;
                requires_on = false;
            } else if (match(TokenType::LEFT)) {
                match(TokenType::OUTER);
                expect(TokenType::JOIN, "expected JOIN after LEFT");
                join_type = LEFT_JOIN;
            } else if (match(TokenType::RIGHT)) {
                match(TokenType::OUTER);
                expect(TokenType::JOIN, "expected JOIN after RIGHT");
                join_type = RIGHT_JOIN;
            } else if (match(TokenType::FULL)) {
                match(TokenType::OUTER);
                expect(TokenType::JOIN, "expected JOIN after FULL");
                join_type = FULL_JOIN;
            } else if (match(TokenType::JOIN)) {
                join_type = INNER_JOIN;
            } else {
                break;
            }
            {
                TableRef left = from->tables.back();
                TableRef right = parse_table_ref();
                from->tables.push_back(right);
                auto join_conds = requires_on ? parse_opt_join_on_clause() : std::vector<std::unique_ptr<BinaryExpr>>();
                std::vector<std::unique_ptr<BinaryExpr>> join_tree_conds;
                join_tree_conds.reserve(join_conds.size());
                for (auto& cond : join_conds) {
                    if (!contains_subquery(cond.get())) {
                        from->conds.push_back(clone_binary_expr(*cond));
                    }
                    join_tree_conds.push_back(std::move(cond));
                }
                from->jointree.push_back(std::make_unique<JoinExpr>(std::move(left), std::move(right),
                                                                    std::move(join_tree_conds), join_type));
                continue;
            }
        }
        return from;
    }

    bool contains_subquery(const Expr* expression) const {
        if (expression == nullptr) {
            return false;
        }
        if (expression->type == AstType::SubqueryExpr) {
            return true;
        }
        if (auto binary = dynamic_cast<const BinaryExpr*>(expression); binary != nullptr) {
            if (contains_subquery(binary->lhs.get()) || contains_subquery(binary->rhs.get()) ||
                contains_subquery(binary->rhs_upper.get())) {
                return true;
            }
            return std::any_of(binary->rhs_list.begin(), binary->rhs_list.end(),
                               [&](const auto& value) { return contains_subquery(value.get()); });
        }
        if (auto arithmetic = dynamic_cast<const ArithmeticExpr*>(expression); arithmetic != nullptr) {
            return contains_subquery(arithmetic->lhs.get()) || contains_subquery(arithmetic->rhs.get());
        }
        if (auto logical = dynamic_cast<const LogicalExpr*>(expression); logical != nullptr) {
            return std::any_of(logical->operands.begin(), logical->operands.end(),
                               [&](const auto& operand) { return contains_subquery(operand.get()); });
        }
        if (auto case_expr = dynamic_cast<const CaseExpr*>(expression); case_expr != nullptr) {
            for (const auto& clause : case_expr->when_clauses) {
                if (contains_subquery(clause.condition.get()) || contains_subquery(clause.result.get())) {
                    return true;
                }
            }
            return contains_subquery(case_expr->else_expr.get());
        }
        return false;
    }

    TableRef parse_table_ref() {
        std::string table = parse_identifier();
        std::string alias;
        if (match(TokenType::AS)) {
            alias = parse_identifier();
        } else if (check(TokenType::IDENTIFIER)) {
            alias = parse_identifier();
        }
        return TableRef(std::move(table), std::move(alias));
    }

    std::vector<std::unique_ptr<BinaryExpr>> parse_opt_join_on_clause() {
        if (!match(TokenType::ON)) {
            return {};
        }
        std::vector<std::unique_ptr<BinaryExpr>> conds;
        conds.push_back(parse_condition());
        while (match(TokenType::AND)) {
            conds.push_back(parse_condition());
        }
        return conds;
    }

    std::vector<std::unique_ptr<OrderByItem>> parse_opt_order_clause() {
        if (!match(TokenType::ORDER)) {
            return {};
        }
        expect(TokenType::BY, "expected BY after ORDER");
        std::vector<std::unique_ptr<OrderByItem>> items;
        items.push_back(parse_order_item());
        while (match(TokenType::COMMA)) {
            items.push_back(parse_order_item());
        }
        return items;
    }

    std::unique_ptr<OrderByItem> parse_order_item() {
        auto expr = parse_general_expr();
        OrderByDir dir = OrderBy_DEFAULT;
        if (match(TokenType::ASC)) {
            dir = OrderBy_ASC;
        } else if (match(TokenType::DESC)) {
            dir = OrderBy_DESC;
        }
        NullsOrder nulls_order = NullsOrder::DEFAULT;
        if (match(TokenType::NULLS)) {
            if (match(TokenType::FIRST)) {
                nulls_order = NullsOrder::FIRST;
            } else {
                expect(TokenType::LAST, "expected FIRST or LAST after NULLS");
                nulls_order = NullsOrder::LAST;
            }
        }
        return std::make_unique<OrderByItem>(std::move(expr), dir, nulls_order);
    }

    PaginationClause parse_opt_pagination_clause() {
        PaginationClause result;
        while (check(TokenType::LIMIT) || check(TokenType::OFFSET)) {
            if (match(TokenType::LIMIT)) {
                if (result.has_limit) {
                    error("duplicate LIMIT clause");
                }
                result.has_limit = true;
                result.limit = parse_int_literal()->val;
            } else {
                expect(TokenType::OFFSET, "expected OFFSET");
                if (result.has_offset) {
                    error("duplicate OFFSET clause");
                }
                result.has_offset = true;
                result.offset = parse_int_literal()->val;
            }
        }
        return result;
    }

    std::string parse_identifier() {
        return token_text(expect(TokenType::IDENTIFIER, "expected identifier"));
    }
};

} // namespace

std::unique_ptr<TreeNode> parse_sql(const std::string& sql) {
    try {
        SqlParser parser(sql);
        return parser.parse();
    } catch (const parser::LexerError& e) {
        throw ParseError(e.what());
    }
}

} // namespace ast
