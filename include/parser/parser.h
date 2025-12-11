#pragma once

#include "common.h"
#include "parser/tokenizer.h"

namespace minidb {

// Forward declarations
struct Statement;
struct Expression;

// Expression types
enum class ExprType {
    LITERAL,
    COLUMN_REF,
    BINARY_OP,
    UNARY_OP,
    AGGREGATE_FUNC,
    SUBQUERY,        // For subqueries
    IN_LIST,         // For IN (value1, value2, ...)
    EXISTS           // For EXISTS (subquery)
};

// Aggregate function types
enum class AggregateType {
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX
};

// Binary operators
enum class BinaryOp {
    ADD, SUB, MUL, DIV,
    EQ, NE, LT, GT, LE, GE,
    AND, OR
};

// Unary operators
enum class UnaryOp {
    NOT, MINUS
};

// Expression node
struct Expression {
    ExprType type;
    
    // For LITERAL
    Value literal_value;
    
    // For COLUMN_REF
    std::string table_name;
    std::string column_name;
    
    // For BINARY_OP
    BinaryOp binary_op;
    std::unique_ptr<Expression> left;
    std::unique_ptr<Expression> right;
    
    // For UNARY_OP
    UnaryOp unary_op;
    std::unique_ptr<Expression> operand;
    
    // For AGGREGATE_FUNC
    AggregateType aggregate_type;
    std::unique_ptr<Expression> aggregate_arg;  // nullptr for COUNT(*)
    bool is_distinct = false;
    
    // For SUBQUERY
    std::unique_ptr<struct SelectStatement> subquery;
    
    // For IN_LIST
    std::vector<Value> in_values;
    
    Expression() : type(ExprType::LITERAL) {}
};

// Join type
enum class JoinType {
    INNER,
    LEFT,
    RIGHT
};

// Join clause
struct JoinClause {
    JoinType type;
    std::string table_name;
    std::string alias;
    std::unique_ptr<Expression> on_condition;
};

// Select column (can be expression, column ref, or aggregate)
struct SelectColumn {
    std::unique_ptr<Expression> expr;
    std::string alias;
    bool is_star = false;  // SELECT *
};

// Column definition for CREATE TABLE
struct ColumnDef {
    std::string name;
    ColumnType type;
    uint16_t size = 0;  // For VARCHAR
    bool is_primary_key = false;
    bool is_nullable = true;
};

// Statement types
enum class StatementType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE_TABLE,
    DROP_TABLE,
    CREATE_INDEX,
    DROP_INDEX,
    BEGIN_TXN,
    COMMIT_TXN,
    ROLLBACK_TXN
};

// SELECT statement
struct SelectStatement {
    std::vector<SelectColumn> select_columns;  // What to select
    bool select_all = false;  // SELECT *
    bool is_distinct = false;
    std::string table_name;
    std::string table_alias;
    std::vector<JoinClause> joins;
    std::unique_ptr<Expression> where_clause;
    std::vector<std::string> group_by;
    std::unique_ptr<Expression> having_clause;
    std::vector<std::pair<std::string, bool>> order_by;  // column, is_ascending
    int limit = -1;
    int offset = 0;
};

// INSERT statement
struct InsertStatement {
    std::string table_name;
    std::vector<std::string> columns;  // Empty means all columns
    std::vector<std::vector<Value>> values;  // Multiple rows
};

// UPDATE statement
struct UpdateStatement {
    std::string table_name;
    std::vector<std::pair<std::string, Value>> assignments;
    std::unique_ptr<Expression> where_clause;
};

// DELETE statement
struct DeleteStatement {
    std::string table_name;
    std::unique_ptr<Expression> where_clause;
};

// CREATE TABLE statement
struct CreateTableStatement {
    std::string table_name;
    std::vector<ColumnDef> columns;
};

// DROP TABLE statement
struct DropTableStatement {
    std::string table_name;
};

// CREATE INDEX statement
struct CreateIndexStatement {
    std::string index_name;
    std::string table_name;
    std::vector<std::string> columns;
    bool is_unique = false;
};

// DROP INDEX statement
struct DropIndexStatement {
    std::string index_name;
    std::string table_name;  // Optional: ON table_name
};

// Transaction statements
struct BeginStatement {
    std::string isolation_level;  // Optional: READ COMMITTED, SERIALIZABLE, etc.
};

struct CommitStatement {};
struct RollbackStatement {};

// Generic statement wrapper
struct Statement {
    StatementType type;
    std::unique_ptr<SelectStatement> select;
    std::unique_ptr<InsertStatement> insert;
    std::unique_ptr<UpdateStatement> update;
    std::unique_ptr<DeleteStatement> delete_stmt;
    std::unique_ptr<CreateTableStatement> create_table;
    std::unique_ptr<DropTableStatement> drop_table;
    std::unique_ptr<CreateIndexStatement> create_index;
    std::unique_ptr<DropIndexStatement> drop_index;
    std::unique_ptr<BeginStatement> begin_txn;
    std::unique_ptr<CommitStatement> commit_txn;
    std::unique_ptr<RollbackStatement> rollback_txn;
};

// Recursive descent SQL parser
class Parser {
public:
    explicit Parser(const std::string& sql);
    
    // Parse and return a statement
    std::unique_ptr<Statement> parse();
    
    // Get error message if parsing failed
    const std::string& getError() const { return error_; }
    bool hasError() const { return !error_.empty(); }

private:
    Tokenizer tokenizer_;
    Token current_token_;
    std::string error_;
    
    // Token handling
    void advance();
    bool check(TokenType type) const;
    bool match(TokenType type);
    void expect(TokenType type, const std::string& message);
    
    // Statement parsers
    std::unique_ptr<Statement> parseStatement();
    std::unique_ptr<SelectStatement> parseSelect();
    std::unique_ptr<InsertStatement> parseInsert();
    std::unique_ptr<UpdateStatement> parseUpdate();
    std::unique_ptr<DeleteStatement> parseDelete();
    std::unique_ptr<CreateTableStatement> parseCreateTable();
    std::unique_ptr<DropTableStatement> parseDropTable();
    std::unique_ptr<CreateIndexStatement> parseCreateIndex();
    std::unique_ptr<DropIndexStatement> parseDropIndex();
    std::unique_ptr<BeginStatement> parseBegin();
    std::unique_ptr<CommitStatement> parseCommit();
    std::unique_ptr<RollbackStatement> parseRollback();
    
    // Expression parser (precedence climbing)
    std::unique_ptr<Expression> parseExpression();
    std::unique_ptr<Expression> parseOr();
    std::unique_ptr<Expression> parseAnd();
    std::unique_ptr<Expression> parseEquality();
    std::unique_ptr<Expression> parseComparison();
    std::unique_ptr<Expression> parseTerm();
    std::unique_ptr<Expression> parseFactor();
    std::unique_ptr<Expression> parseUnary();
    std::unique_ptr<Expression> parsePrimary();
    
    // Helper parsers
    Value parseValue();
    ColumnDef parseColumnDef();
    std::vector<std::string> parseColumnList();
    SelectColumn parseSelectColumn();
    JoinClause parseJoinClause();
    std::unique_ptr<Expression> parseAggregate();
    
    // Error handling
    void setError(const std::string& message);
};

} // namespace minidb
