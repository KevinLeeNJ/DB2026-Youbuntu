#include "parser.h"

#include "lexer.h"

#include <climits>
#include <limits>
#include <iterator>
#include <stdexcept>
#include <utility>

namespace ast {
namespace {

using parser::Lexer;
using parser::Token;
using parser::TokenType;

struct ParseError : public std::runtime_error {
    explicit ParseError(const std::string& message) : std::runtime_error(message) {}
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

        auto result = parse_stmt();
        expect(TokenType::SEMICOLON, "expected ';' after statement");
        expect_end();
        return result;
    }

private:
    Lexer lexer_;
    Token current_;

    void advance() {
        current_ = lexer_.next_token();
        if (current_.type == TokenType::T_ERROR) {
            error("unexpected token");
        }
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
        error("unexpected start of statement");
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
            expect(TokenType::VALUES, "expected VALUES after table name");
            expect(TokenType::LPAREN, "expected '(' before values");
            auto values = parse_value_list();
            expect(TokenType::RPAREN, "expected ')' after values");
            return std::make_unique<InsertStmt>(std::move(table), std::move(values));
        }
        if (match(TokenType::DELETE)) {
            expect(TokenType::FROM, "expected FROM after DELETE");
            std::string table = parse_identifier();
            return std::make_unique<DeleteStmt>(std::move(table), parse_opt_where_clause());
        }
        // UPDATE
        expect(TokenType::UPDATE, "expected DML statement");
        std::string table = parse_identifier();
        expect(TokenType::SET, "expected SET after table name");
        auto clauses = parse_set_clause_list();
        return std::make_unique<UpdateStmt>(std::move(table), std::move(clauses), parse_opt_where_clause());
    }

    std::unique_ptr<TreeNode> parse_dql() {
        if (match(TokenType::EXPLAIN)) {
            expect(TokenType::ANALYZE, "expected ANALYZE after EXPLAIN");
            return std::make_unique<ExplainAnalyze>(parse_select_stmt());
        }
        return parse_select_or_union_wrapper();
    }

    std::unique_ptr<TreeNode> parse_select_or_union_wrapper() {
        expect(TokenType::SELECT, "expected SELECT");
        if (match(TokenType::STAR)) {
            expect(TokenType::FROM, "expected FROM in SELECT");
            if (match(TokenType::LPAREN)) {
                auto union_stmt = parse_union_query();
                expect(TokenType::RPAREN, "expected ')' after union query");
                expect(TokenType::AS, "expected AS after union query");
                std::string alias = parse_identifier();
                auto order = parse_opt_order_clause();
                return std::make_unique<SelectFromUnionStmt>(std::move(union_stmt), std::move(alias), std::move(order));
            }
            return parse_select_tail(true, {});
        }
        return parse_select_stmt_after_select(false);
    }

    std::unique_ptr<SelectStmt> parse_select_stmt() {
        expect(TokenType::SELECT, "expected SELECT");
        return parse_select_stmt_after_select(false);
    }

    std::unique_ptr<SelectStmt> parse_select_stmt_after_select(bool already_consumed_star) {
        bool has_star = already_consumed_star;
        std::vector<std::unique_ptr<SelectItem>> items;
        if (has_star || match(TokenType::STAR)) {
            has_star = true;
        } else {
            items = parse_select_item_list();
        }
        expect(TokenType::FROM, "expected FROM in SELECT");
        return parse_select_tail(has_star, std::move(items));
    }

    std::unique_ptr<SelectStmt> parse_select_tail(bool has_star, std::vector<std::unique_ptr<SelectItem>> items) {
        auto from = parse_from_clause();
        auto conds = std::move(from->conds);
        auto where = parse_opt_where_clause();
        conds.insert(conds.end(), std::make_move_iterator(where.begin()), std::make_move_iterator(where.end()));
        auto group_by = parse_opt_group_clause();
        auto having = parse_opt_having_clause();
        auto order = parse_opt_order_clause();
        auto limit = parse_opt_limit_clause();
        int limit_value = 0;
        if (limit != nullptr) {
            limit_value = static_cast<IntLit*>(limit.get())->val;
        }
        return std::make_unique<SelectStmt>(std::move(items), from->tables, std::move(conds), std::move(group_by),
                                            std::move(having), std::move(order), limit != nullptr, limit_value,
                                            has_star);
    }

    std::unique_ptr<UnionStmt> parse_union_query() {
        auto first = parse_select_stmt();
        expect(TokenType::UNION, "expected UNION in union query");
        std::vector<std::unique_ptr<SelectStmt>> branches;
        branches.push_back(std::move(first));
        branches.push_back(parse_select_stmt());
        while (match(TokenType::UNION)) {
            branches.push_back(parse_select_stmt());
        }
        return std::make_unique<UnionStmt>(std::move(branches));
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
            return std::make_unique<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
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
        error("expected value");
    }

    std::unique_ptr<IntLit> parse_int_literal(bool is_negative = false) {
        Token token = expect(TokenType::VALUE_INT, "expected integer");
        if (token.int_value < std::numeric_limits<int>::min() || token.int_value > std::numeric_limits<int>::max()) {
            error("integer literal out of range");
        }
        int val = static_cast<int>(token.int_value);
        std::string display = token_text(token);
        if (is_negative) {
            val = -val;
            display = "-" + display;
        }
        return std::make_unique<IntLit>(val, std::move(display));
    }

    std::unique_ptr<FloatLit> parse_float_literal(bool is_negative = false) {
        Token token = expect(TokenType::VALUE_FLOAT, "expected float");
        float val = static_cast<float>(token.float_value);
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

    std::vector<std::unique_ptr<BinaryExpr>> parse_opt_where_clause() {
        if (!match(TokenType::WHERE)) {
            return {};
        }
        return parse_where_clause();
    }

    std::vector<std::unique_ptr<BinaryExpr>> parse_where_clause() {
        std::vector<std::unique_ptr<BinaryExpr>> conds;
        conds.push_back(parse_condition());
        while (match(TokenType::AND)) {
            conds.push_back(parse_condition());
        }
        return conds;
    }

    std::unique_ptr<BinaryExpr> parse_condition() {
        auto lhs = parse_col();
        auto op = parse_op();
        auto rhs = parse_expr();
        return std::make_unique<BinaryExpr>(std::move(lhs), op, std::move(rhs));
    }

    std::unique_ptr<Expr> parse_expr() {
        if (check(TokenType::IDENTIFIER)) {
            return parse_col();
        }
        return parse_value();
    }

    SvCompOp parse_op() {
        if (match(TokenType::EQ)) {
            return SV_OP_EQ;
        }
        if (match(TokenType::LT)) {
            return SV_OP_LT;
        }
        if (match(TokenType::GT)) {
            return SV_OP_GT;
        }
        if (match(TokenType::NEQ)) {
            return SV_OP_NE;
        }
        if (match(TokenType::LEQ)) {
            return SV_OP_LE;
        }
        expect(TokenType::GEQ, "expected comparison operator");
        return SV_OP_GE;
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
        expect(TokenType::EQ, "expected '=' in SET clause");
        return std::make_unique<SetClause>(std::move(column), parse_value());
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
        std::unique_ptr<Expr> expr;
        if (is_aggregate_start(current_.type)) {
            expr = parse_aggregate_expr();
        } else if (check(TokenType::IDENTIFIER)) {
            expr = parse_col();
        } else {
            expr = parse_value();
        }
        std::string alias;
        if (match(TokenType::AS)) {
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
        auto column = parse_col();
        expect(TokenType::RPAREN, "expected ')' after aggregate argument");
        return std::make_unique<AggExpr>(func, false, std::move(column));
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
        auto lhs = parse_general_expr();
        auto op = parse_op();
        auto rhs = parse_general_expr();
        return std::make_unique<HavingExpr>(std::move(lhs), op, std::move(rhs));
    }

    std::unique_ptr<Expr> parse_general_expr() {
        if (is_aggregate_start(current_.type)) {
            return parse_aggregate_expr();
        }
        if (check(TokenType::IDENTIFIER)) {
            return parse_col();
        }
        return parse_value();
    }

    std::unique_ptr<FromClause> parse_from_clause() {
        auto from = std::make_unique<FromClause>();
        from->tables.push_back(parse_table_ref());
        while (true) {
            if (match(TokenType::COMMA)) {
                from->tables.push_back(parse_table_ref());
                continue;
            }
            if (match(TokenType::JOIN)) {
                from->tables.push_back(parse_table_ref());
                auto join_conds = parse_opt_join_on_clause();
                from->conds.insert(from->conds.end(), std::make_move_iterator(join_conds.begin()),
                                   std::make_move_iterator(join_conds.end()));
                continue;
            }
            break;
        }
        return from;
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
        return parse_where_clause();
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
        return std::make_unique<OrderByItem>(std::move(expr), dir);
    }

    std::unique_ptr<Value> parse_opt_limit_clause() {
        if (!match(TokenType::LIMIT)) {
            return nullptr;
        }
        auto lit = parse_int_literal();
        return std::unique_ptr<Value>(std::move(lit));
    }

    std::string parse_identifier() {
        return token_text(expect(TokenType::IDENTIFIER, "expected identifier"));
    }
};

} // namespace

std::unique_ptr<TreeNode> parse_sql(const std::string& sql) {
    SqlParser parser(sql);
    return parser.parse();
}

} // namespace ast
