%{
#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace ast;
%}

// request a pure (reentrant) parser
%define api.pure full
// enable location in error handler
%locations
// enable verbose syntax error message
%define parse.error verbose

// keywords
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC ORDER BY GROUP HAVING LIMIT AS
WHERE UPDATE SET SELECT INT CHAR FLOAT INDEX AND JOIN COUNT MAX MIN SUM AVG
EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ENABLE_NESTLOOP ENABLE_SORTMERGE
// non-keywords
%token LEQ NEQ GEQ T_EOF

// type-specific tokens
%token <sv_str> IDENTIFIER VALUE_STRING
%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT
%token <sv_bool> VALUE_BOOL

// specify types for non-terminal symbol
%type <sv_node> stmt dbStmt ddl dml txnStmt setStmt
%type <sv_field> field
%type <sv_fields> fieldList
%type <sv_type_len> type
%type <sv_comp_op> op
%type <sv_expr> expr aggregate_expr having_expr having_rhs
%type <sv_val> value opt_limit_clause
%type <sv_vals> valueList
%type <sv_str> tbName colName opt_alias
%type <sv_strs> tableList colNameList
%type <sv_col> col
%type <sv_cols> colList opt_group_clause
%type <sv_select_item> select_item
%type <sv_select_items> select_item_list selector
%type <sv_set_clause> setClause
%type <sv_set_clauses> setClauses
%type <sv_cond> condition
%type <sv_conds> whereClause optWhereClause
%type <sv_having_cond> having_condition
%type <sv_having_conds> having_clause opt_having_clause
%type <sv_orderby_item> order_item
%type <sv_orderby_items>  order_clause opt_order_clause
%type <sv_orderby_dir> opt_asc_desc
%type <sv_setKnobType> set_knob_type

%%
start:
        stmt ';'
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    |   setStmt
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   SHOW INDEX FROM tbName
    {
        $$ = std::make_shared<ShowIndex>($4);
    }
    ;

setStmt:
        SET set_knob_type '=' VALUE_BOOL
    {
        $$ = std::make_shared<SetStmt>($2, $4);
    }
    ;

ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = std::make_shared<CreateTable>($3, $5);
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>($3);
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>($2);
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<CreateIndex>($3, $5);
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<DropIndex>($3, $5);
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>($3, $6);
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>($3, $4);
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $5);
    }
    |   SELECT selector FROM tableList optWhereClause opt_group_clause opt_having_clause opt_order_clause opt_limit_clause
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $6, $7, $8, $9 != nullptr,
                                          $9 != nullptr ? std::static_pointer_cast<IntLit>($9)->val : 0,
                                          $2.empty());
    }
    ;

fieldList:
        field
    {
        $$ = std::vector<std::shared_ptr<Field>>{$1};
    }
    |   fieldList ',' field
    {
        $$.push_back($3);
    }
    ;

colNameList:
        colName
    {
        $$ = std::vector<std::string>{$1};
    }
    | colNameList ',' colName
    {
        $$.push_back($3);
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>($1, $2);
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    ;

valueList:
        value
    {
        $$ = std::vector<std::shared_ptr<Value>>{$1};
    }
    |   valueList ',' value
    {
        $$.push_back($3);
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<IntLit>($1);
    }
    |   VALUE_FLOAT
    {
        $$ = std::make_shared<FloatLit>($1);
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>($1);
    }
    |   VALUE_BOOL
    {
        $$ = std::make_shared<BoolLit>($1);
    }
    ;

condition:
        col op expr
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    ;

optWhereClause:
        /* epsilon */
    {
        $$ = {};
    }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

whereClause:
        condition 
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   whereClause AND condition
    {
        $$.push_back($3);
    }
    ;

col:
        tbName '.' colName
    {
        $$ = std::make_shared<Col>($1, $3);
    }
    |   colName
    {
        $$ = std::make_shared<Col>("", $1);
    }
    ;

colList:
        col
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
    }
    |   colList ',' col
    {
        $$.push_back($3);
    }
    ;

op:
        '='
    {
        $$ = SV_OP_EQ;
    }
    |   '<'
    {
        $$ = SV_OP_LT;
    }
    |   '>'
    {
        $$ = SV_OP_GT;
    }
    |   NEQ
    {
        $$ = SV_OP_NE;
    }
    |   LEQ
    {
        $$ = SV_OP_LE;
    }
    |   GEQ
    {
        $$ = SV_OP_GE;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{$1};
    }
    |   setClauses ',' setClause
    {
        $$.push_back($3);
    }
    ;

setClause:
        colName '=' value
    {
        $$ = std::make_shared<SetClause>($1, $3);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   select_item_list
    ;

select_item_list:
        select_item
    {
        $$ = std::vector<std::shared_ptr<SelectItem>>{$1};
    }
    |   select_item_list ',' select_item
    {
        $$.push_back($3);
    }
    ;

select_item:
        col opt_alias
    {
        $$ = std::make_shared<SelectItem>(std::static_pointer_cast<Expr>($1), $2);
    }
    |   aggregate_expr opt_alias
    {
        $$ = std::make_shared<SelectItem>($1, $2);
    }
    ;

opt_alias:
        AS IDENTIFIER
    {
        $$ = $2;
    }
    |   /* epsilon */
    {
        $$ = "";
    }
    ;

aggregate_expr:
        COUNT '(' '*' ')'
    {
        $$ = std::make_shared<AggExpr>(AGG_COUNT, true, nullptr);
    }
    |   COUNT '(' col ')'
    {
        $$ = std::make_shared<AggExpr>(AGG_COUNT, false, $3);
    }
    |   MAX '(' col ')'
    {
        $$ = std::make_shared<AggExpr>(AGG_MAX, false, $3);
    }
    |   MIN '(' col ')'
    {
        $$ = std::make_shared<AggExpr>(AGG_MIN, false, $3);
    }
    |   SUM '(' col ')'
    {
        $$ = std::make_shared<AggExpr>(AGG_SUM, false, $3);
    }
    |   AVG '(' col ')'
    {
        $$ = std::make_shared<AggExpr>(AGG_AVG, false, $3);
    }
    ;

opt_group_clause:
        /* epsilon */
    {
        $$ = {};
    }
    |   GROUP BY colList
    {
        $$ = $3;
    }
    ;

opt_having_clause:
        /* epsilon */
    {
        $$ = {};
    }
    |   HAVING having_clause
    {
        $$ = $2;
    }
    ;

having_clause:
        having_condition
    {
        $$ = std::vector<std::shared_ptr<HavingExpr>>{$1};
    }
    |   having_clause AND having_condition
    {
        $$.push_back($3);
    }
    ;

having_condition:
        having_expr op having_rhs
    {
        $$ = std::make_shared<HavingExpr>($1, $2, $3);
    }
    ;

having_expr:
        col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   aggregate_expr
    ;

having_rhs:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   aggregate_expr
    ;

tableList:
        tbName
    {
        $$ = std::vector<std::string>{$1};
    }
    |   tableList ',' tbName
    {
        $$.push_back($3);
    }
    |   tableList JOIN tbName
    {
        $$.push_back($3);
    }
    ;

opt_order_clause:
    ORDER BY order_clause      
    { 
        $$ = $3; 
    }
    |   /* epsilon */
    {
        $$ = {};
    }
    ;

order_clause:
      order_item
    { 
        $$ = std::vector<std::shared_ptr<OrderByItem>>{$1};
    }
    |   order_clause ',' order_item
    {
        $$.push_back($3);
    }
    ;

order_item:
      having_expr  opt_asc_desc
    {
        $$ = std::make_shared<OrderByItem>($1, $2);
    }
    ;

opt_asc_desc:
    ASC          { $$ = OrderBy_ASC;     }
    |  DESC      { $$ = OrderBy_DESC;    }
    |       { $$ = OrderBy_DEFAULT; }
    ;    

opt_limit_clause:
        /* epsilon */
    {
        $$ = nullptr;
    }
    |   LIMIT VALUE_INT
    {
        $$ = std::make_shared<IntLit>($2);
    }
    ;

set_knob_type:
    ENABLE_NESTLOOP { $$ = EnableNestLoop; }
    |   ENABLE_SORTMERGE { $$ = EnableSortMerge; }
    ;

tbName: IDENTIFIER;

colName: IDENTIFIER;
%%
