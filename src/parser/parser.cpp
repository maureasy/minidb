#include "parser/parser.h"

namespace minidb {

Parser::Parser(const std::string& sql) : tokenizer_(sql) {
    advance();
}

void Parser::advance() {
    current_token_ = tokenizer_.nextToken();
}

bool Parser::check(TokenType type) const {
    return current_token_.type == type;
}

bool Parser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

void Parser::expect(TokenType type, const std::string& message) {
    if (!match(type)) {
        setError(message + " (got '" + current_token_.value + "')");
    }
}

void Parser::setError(const std::string& message) {
    if (error_.empty()) {
        error_ = "Parse error at line " + std::to_string(current_token_.line) +
                 ", column " + std::to_string(current_token_.column) + ": " + message;
    }
}

std::unique_ptr<Statement> Parser::parse() {
    return parseStatement();
}

std::unique_ptr<Statement> Parser::parseStatement() {
    auto stmt = std::make_unique<Statement>();
    
    if (match(TokenType::SELECT)) {
        stmt->type = StatementType::SELECT;
        stmt->select = parseSelect();
    } else if (match(TokenType::INSERT)) {
        stmt->type = StatementType::INSERT;
        stmt->insert = parseInsert();
    } else if (match(TokenType::UPDATE)) {
        stmt->type = StatementType::UPDATE;
        stmt->update = parseUpdate();
    } else if (match(TokenType::DELETE)) {
        stmt->type = StatementType::DELETE;
        stmt->delete_stmt = parseDelete();
    } else if (match(TokenType::CREATE)) {
        if (match(TokenType::TABLE)) {
            stmt->type = StatementType::CREATE_TABLE;
            stmt->create_table = parseCreateTable();
        } else if (match(TokenType::INDEX) || match(TokenType::UNIQUE)) {
            bool is_unique = (current_token_.type == TokenType::UNIQUE);
            if (is_unique) match(TokenType::INDEX);
            stmt->type = StatementType::CREATE_INDEX;
            stmt->create_index = parseCreateIndex();
            if (stmt->create_index) stmt->create_index->is_unique = is_unique;
        } else {
            setError("Expected TABLE or INDEX after CREATE");
            return nullptr;
        }
    } else if (match(TokenType::DROP)) {
        if (match(TokenType::TABLE)) {
            stmt->type = StatementType::DROP_TABLE;
            stmt->drop_table = parseDropTable();
        } else if (match(TokenType::INDEX)) {
            stmt->type = StatementType::DROP_INDEX;
            stmt->drop_index = parseDropIndex();
        } else {
            setError("Expected TABLE or INDEX after DROP");
            return nullptr;
        }
    } else if (match(TokenType::BEGIN)) {
        stmt->type = StatementType::BEGIN_TXN;
        stmt->begin_txn = parseBegin();
    } else if (match(TokenType::COMMIT)) {
        stmt->type = StatementType::COMMIT_TXN;
        stmt->commit_txn = parseCommit();
    } else if (match(TokenType::ROLLBACK)) {
        stmt->type = StatementType::ROLLBACK_TXN;
        stmt->rollback_txn = parseRollback();
    } else {
        setError("Expected SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, BEGIN, COMMIT, or ROLLBACK");
        return nullptr;
    }
    
    // Optional semicolon
    match(TokenType::SEMICOLON);
    
    if (hasError()) return nullptr;
    return stmt;
}

std::unique_ptr<SelectStatement> Parser::parseSelect() {
    auto stmt = std::make_unique<SelectStatement>();
    
    // Check for DISTINCT
    if (match(TokenType::DISTINCT)) {
        stmt->is_distinct = true;
    }
    
    // Parse column list
    if (match(TokenType::STAR)) {
        stmt->select_all = true;
    } else {
        do {
            SelectColumn col = parseSelectColumn();
            if (hasError()) return nullptr;
            stmt->select_columns.push_back(std::move(col));
        } while (match(TokenType::COMMA));
    }
    
    // FROM clause
    expect(TokenType::FROM, "Expected FROM");
    if (hasError()) return nullptr;
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    // Optional table alias
    if (match(TokenType::AS)) {
        if (!check(TokenType::IDENTIFIER)) {
            setError("Expected alias after AS");
            return nullptr;
        }
        stmt->table_alias = current_token_.value;
        advance();
    } else if (check(TokenType::IDENTIFIER) && 
               !check(TokenType::WHERE) && !check(TokenType::JOIN) &&
               !check(TokenType::LEFT) && !check(TokenType::RIGHT) &&
               !check(TokenType::INNER) && !check(TokenType::ORDER) &&
               !check(TokenType::GROUP) && !check(TokenType::LIMIT)) {
        stmt->table_alias = current_token_.value;
        advance();
    }
    
    // Optional JOINs
    while (check(TokenType::JOIN) || check(TokenType::LEFT) || 
           check(TokenType::RIGHT) || check(TokenType::INNER)) {
        JoinClause join = parseJoinClause();
        if (hasError()) return nullptr;
        stmt->joins.push_back(std::move(join));
    }
    
    // Optional WHERE clause
    if (match(TokenType::WHERE)) {
        stmt->where_clause = parseExpression();
    }
    
    // Optional GROUP BY
    if (match(TokenType::GROUP)) {
        expect(TokenType::BY, "Expected BY after GROUP");
        if (hasError()) return nullptr;
        
        do {
            if (!check(TokenType::IDENTIFIER)) {
                setError("Expected column name in GROUP BY");
                return nullptr;
            }
            stmt->group_by.push_back(current_token_.value);
            advance();
        } while (match(TokenType::COMMA));
    }
    
    // Optional HAVING
    if (match(TokenType::HAVING)) {
        stmt->having_clause = parseExpression();
    }
    
    // Optional ORDER BY
    if (match(TokenType::ORDER)) {
        expect(TokenType::BY, "Expected BY after ORDER");
        if (hasError()) return nullptr;
        
        do {
            if (!check(TokenType::IDENTIFIER)) {
                setError("Expected column name in ORDER BY");
                return nullptr;
            }
            std::string col = current_token_.value;
            advance();
            
            bool ascending = true;
            if (match(TokenType::DESC)) {
                ascending = false;
            } else {
                match(TokenType::ASC);  // Optional ASC
            }
            
            stmt->order_by.push_back({col, ascending});
        } while (match(TokenType::COMMA));
    }
    
    // Optional LIMIT
    if (match(TokenType::LIMIT)) {
        if (!check(TokenType::INTEGER)) {
            setError("Expected integer after LIMIT");
            return nullptr;
        }
        stmt->limit = std::stoi(current_token_.value);
        advance();
        
        // Optional OFFSET
        if (match(TokenType::OFFSET)) {
            if (!check(TokenType::INTEGER)) {
                setError("Expected integer after OFFSET");
                return nullptr;
            }
            stmt->offset = std::stoi(current_token_.value);
            advance();
        }
    }
    
    return stmt;
}

SelectColumn Parser::parseSelectColumn() {
    SelectColumn col;
    
    // Check for aggregate functions
    if (check(TokenType::COUNT) || check(TokenType::SUM) || 
        check(TokenType::AVG) || check(TokenType::MIN) || check(TokenType::MAX)) {
        col.expr = parseAggregate();
    } else {
        col.expr = parseExpression();
    }
    
    // Optional alias
    if (match(TokenType::AS)) {
        if (!check(TokenType::IDENTIFIER)) {
            setError("Expected alias after AS");
            return col;
        }
        col.alias = current_token_.value;
        advance();
    }
    
    return col;
}

JoinClause Parser::parseJoinClause() {
    JoinClause join;
    join.type = JoinType::INNER;  // Default
    
    // Determine join type
    if (match(TokenType::LEFT)) {
        join.type = JoinType::LEFT;
        match(TokenType::OUTER);  // Optional OUTER
    } else if (match(TokenType::RIGHT)) {
        join.type = JoinType::RIGHT;
        match(TokenType::OUTER);  // Optional OUTER
    } else if (match(TokenType::INNER)) {
        join.type = JoinType::INNER;
    }
    
    expect(TokenType::JOIN, "Expected JOIN");
    if (hasError()) return join;
    
    // Table name
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name after JOIN");
        return join;
    }
    join.table_name = current_token_.value;
    advance();
    
    // Optional alias
    if (match(TokenType::AS)) {
        if (!check(TokenType::IDENTIFIER)) {
            setError("Expected alias after AS");
            return join;
        }
        join.alias = current_token_.value;
        advance();
    } else if (check(TokenType::IDENTIFIER) && !check(TokenType::ON)) {
        join.alias = current_token_.value;
        advance();
    }
    
    // ON condition
    expect(TokenType::ON, "Expected ON after JOIN table");
    if (hasError()) return join;
    
    join.on_condition = parseExpression();
    
    return join;
}

std::unique_ptr<Expression> Parser::parseAggregate() {
    auto expr = std::make_unique<Expression>();
    expr->type = ExprType::AGGREGATE_FUNC;
    
    // Get aggregate type
    if (match(TokenType::COUNT)) {
        expr->aggregate_type = AggregateType::COUNT;
    } else if (match(TokenType::SUM)) {
        expr->aggregate_type = AggregateType::SUM;
    } else if (match(TokenType::AVG)) {
        expr->aggregate_type = AggregateType::AVG;
    } else if (match(TokenType::MIN)) {
        expr->aggregate_type = AggregateType::MIN;
    } else if (match(TokenType::MAX)) {
        expr->aggregate_type = AggregateType::MAX;
    }
    
    expect(TokenType::LPAREN, "Expected ( after aggregate function");
    if (hasError()) return nullptr;
    
    // Check for DISTINCT
    if (match(TokenType::DISTINCT)) {
        expr->is_distinct = true;
    }
    
    // Check for * (COUNT(*))
    if (match(TokenType::STAR)) {
        expr->aggregate_arg = nullptr;  // COUNT(*)
    } else {
        expr->aggregate_arg = parseExpression();
    }
    
    expect(TokenType::RPAREN, "Expected ) after aggregate argument");
    
    return expr;
}

std::unique_ptr<InsertStatement> Parser::parseInsert() {
    auto stmt = std::make_unique<InsertStatement>();
    
    expect(TokenType::INTO, "Expected INTO after INSERT");
    if (hasError()) return nullptr;
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    // Optional column list
    if (match(TokenType::LPAREN)) {
        do {
            if (!check(TokenType::IDENTIFIER)) {
                setError("Expected column name");
                return nullptr;
            }
            stmt->columns.push_back(current_token_.value);
            advance();
        } while (match(TokenType::COMMA));
        
        expect(TokenType::RPAREN, "Expected )");
        if (hasError()) return nullptr;
    }
    
    expect(TokenType::VALUES, "Expected VALUES");
    if (hasError()) return nullptr;
    
    // Parse value rows
    do {
        expect(TokenType::LPAREN, "Expected (");
        if (hasError()) return nullptr;
        
        std::vector<Value> row;
        do {
            row.push_back(parseValue());
            if (hasError()) return nullptr;
        } while (match(TokenType::COMMA));
        
        expect(TokenType::RPAREN, "Expected )");
        if (hasError()) return nullptr;
        
        stmt->values.push_back(std::move(row));
    } while (match(TokenType::COMMA));
    
    return stmt;
}

std::unique_ptr<UpdateStatement> Parser::parseUpdate() {
    auto stmt = std::make_unique<UpdateStatement>();
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    expect(TokenType::SET, "Expected SET");
    if (hasError()) return nullptr;
    
    // Parse assignments
    do {
        if (!check(TokenType::IDENTIFIER)) {
            setError("Expected column name");
            return nullptr;
        }
        std::string col = current_token_.value;
        advance();
        
        expect(TokenType::EQUAL, "Expected = after column name");
        if (hasError()) return nullptr;
        
        Value val = parseValue();
        if (hasError()) return nullptr;
        
        stmt->assignments.push_back({col, val});
    } while (match(TokenType::COMMA));
    
    // Optional WHERE clause
    if (match(TokenType::WHERE)) {
        stmt->where_clause = parseExpression();
    }
    
    return stmt;
}

std::unique_ptr<DeleteStatement> Parser::parseDelete() {
    auto stmt = std::make_unique<DeleteStatement>();
    
    expect(TokenType::FROM, "Expected FROM after DELETE");
    if (hasError()) return nullptr;
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    // Optional WHERE clause
    if (match(TokenType::WHERE)) {
        stmt->where_clause = parseExpression();
    }
    
    return stmt;
}

std::unique_ptr<CreateTableStatement> Parser::parseCreateTable() {
    auto stmt = std::make_unique<CreateTableStatement>();
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    expect(TokenType::LPAREN, "Expected (");
    if (hasError()) return nullptr;
    
    // Parse column definitions
    do {
        ColumnDef col = parseColumnDef();
        if (hasError()) return nullptr;
        stmt->columns.push_back(col);
    } while (match(TokenType::COMMA));
    
    expect(TokenType::RPAREN, "Expected )");
    
    return stmt;
}

std::unique_ptr<DropTableStatement> Parser::parseDropTable() {
    auto stmt = std::make_unique<DropTableStatement>();
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    return stmt;
}

ColumnDef Parser::parseColumnDef() {
    ColumnDef col;
    
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected column name");
        return col;
    }
    col.name = current_token_.value;
    advance();
    
    // Parse type
    if (match(TokenType::INT_TYPE)) {
        col.type = ColumnType::INT;
    } else if (match(TokenType::FLOAT_TYPE)) {
        col.type = ColumnType::FLOAT;
    } else if (match(TokenType::VARCHAR_TYPE)) {
        col.type = ColumnType::VARCHAR;
        // Optional size
        if (match(TokenType::LPAREN)) {
            if (!check(TokenType::INTEGER)) {
                setError("Expected size for VARCHAR");
                return col;
            }
            col.size = static_cast<uint16_t>(std::stoi(current_token_.value));
            advance();
            expect(TokenType::RPAREN, "Expected )");
        } else {
            col.size = 255;  // Default
        }
    } else if (match(TokenType::BOOL_TYPE)) {
        col.type = ColumnType::BOOL;
    } else {
        setError("Expected column type (INT, FLOAT, VARCHAR, BOOL)");
        return col;
    }
    
    // Check for PRIMARY KEY
    if (match(TokenType::PRIMARY)) {
        expect(TokenType::KEY, "Expected KEY after PRIMARY");
        col.is_primary_key = true;
        col.is_nullable = false;
    }
    
    return col;
}

Value Parser::parseValue() {
    if (check(TokenType::INTEGER)) {
        std::string val = current_token_.value;
        advance();
        return static_cast<int64_t>(std::stoll(val));
    } else if (check(TokenType::FLOAT)) {
        std::string val = current_token_.value;
        advance();
        return std::stod(val);
    } else if (check(TokenType::STRING)) {
        std::string val = current_token_.value;
        advance();
        return val;
    } else if (match(TokenType::TRUE_VAL)) {
        return true;
    } else if (match(TokenType::FALSE_VAL)) {
        return false;
    } else if (match(TokenType::NULL_VAL)) {
        return std::monostate{};
    } else if (check(TokenType::MINUS)) {
        advance();
        if (check(TokenType::INTEGER)) {
            int64_t val = -std::stoll(current_token_.value);
            advance();
            return val;
        } else if (check(TokenType::FLOAT)) {
            double val = -std::stod(current_token_.value);
            advance();
            return val;
        }
    }
    
    setError("Expected value");
    return std::monostate{};
}

// Expression parsing with precedence climbing
std::unique_ptr<Expression> Parser::parseExpression() {
    return parseOr();
}

std::unique_ptr<Expression> Parser::parseOr() {
    auto left = parseAnd();
    
    while (match(TokenType::OR)) {
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::BINARY_OP;
        expr->binary_op = BinaryOp::OR;
        expr->left = std::move(left);
        expr->right = parseAnd();
        left = std::move(expr);
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parseAnd() {
    auto left = parseEquality();
    
    while (match(TokenType::AND)) {
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::BINARY_OP;
        expr->binary_op = BinaryOp::AND;
        expr->left = std::move(left);
        expr->right = parseEquality();
        left = std::move(expr);
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parseEquality() {
    auto left = parseComparison();
    
    while (true) {
        BinaryOp op;
        if (match(TokenType::EQUAL)) {
            op = BinaryOp::EQ;
        } else if (match(TokenType::NOT_EQUAL)) {
            op = BinaryOp::NE;
        } else {
            break;
        }
        
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::BINARY_OP;
        expr->binary_op = op;
        expr->left = std::move(left);
        expr->right = parseComparison();
        left = std::move(expr);
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parseComparison() {
    auto left = parseTerm();
    
    while (true) {
        BinaryOp op;
        if (match(TokenType::LESS)) {
            op = BinaryOp::LT;
        } else if (match(TokenType::GREATER)) {
            op = BinaryOp::GT;
        } else if (match(TokenType::LESS_EQUAL)) {
            op = BinaryOp::LE;
        } else if (match(TokenType::GREATER_EQUAL)) {
            op = BinaryOp::GE;
        } else {
            break;
        }
        
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::BINARY_OP;
        expr->binary_op = op;
        expr->left = std::move(left);
        expr->right = parseTerm();
        left = std::move(expr);
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parseTerm() {
    auto left = parseFactor();
    
    while (true) {
        BinaryOp op;
        if (match(TokenType::PLUS)) {
            op = BinaryOp::ADD;
        } else if (match(TokenType::MINUS)) {
            op = BinaryOp::SUB;
        } else {
            break;
        }
        
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::BINARY_OP;
        expr->binary_op = op;
        expr->left = std::move(left);
        expr->right = parseFactor();
        left = std::move(expr);
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parseFactor() {
    auto left = parseUnary();
    
    while (true) {
        BinaryOp op;
        if (match(TokenType::STAR)) {
            op = BinaryOp::MUL;
        } else if (match(TokenType::SLASH)) {
            op = BinaryOp::DIV;
        } else {
            break;
        }
        
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::BINARY_OP;
        expr->binary_op = op;
        expr->left = std::move(left);
        expr->right = parseUnary();
        left = std::move(expr);
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parseUnary() {
    if (match(TokenType::NOT)) {
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::UNARY_OP;
        expr->unary_op = UnaryOp::NOT;
        expr->operand = parseUnary();
        return expr;
    }
    
    if (match(TokenType::MINUS)) {
        auto expr = std::make_unique<Expression>();
        expr->type = ExprType::UNARY_OP;
        expr->unary_op = UnaryOp::MINUS;
        expr->operand = parseUnary();
        return expr;
    }
    
    return parsePrimary();
}

std::unique_ptr<Expression> Parser::parsePrimary() {
    auto expr = std::make_unique<Expression>();
    
    if (match(TokenType::LPAREN)) {
        expr = parseExpression();
        expect(TokenType::RPAREN, "Expected )");
        return expr;
    }
    
    if (check(TokenType::INTEGER)) {
        expr->type = ExprType::LITERAL;
        expr->literal_value = static_cast<int64_t>(std::stoll(current_token_.value));
        advance();
        return expr;
    }
    
    if (check(TokenType::FLOAT)) {
        expr->type = ExprType::LITERAL;
        expr->literal_value = std::stod(current_token_.value);
        advance();
        return expr;
    }
    
    if (check(TokenType::STRING)) {
        expr->type = ExprType::LITERAL;
        expr->literal_value = current_token_.value;
        advance();
        return expr;
    }
    
    if (match(TokenType::TRUE_VAL)) {
        expr->type = ExprType::LITERAL;
        expr->literal_value = true;
        return expr;
    }
    
    if (match(TokenType::FALSE_VAL)) {
        expr->type = ExprType::LITERAL;
        expr->literal_value = false;
        return expr;
    }
    
    if (match(TokenType::NULL_VAL)) {
        expr->type = ExprType::LITERAL;
        expr->literal_value = std::monostate{};
        return expr;
    }
    
    if (check(TokenType::IDENTIFIER)) {
        expr->type = ExprType::COLUMN_REF;
        expr->column_name = current_token_.value;
        advance();
        
        // Check for table.column
        if (match(TokenType::DOT)) {
            if (!check(TokenType::IDENTIFIER)) {
                setError("Expected column name after .");
                return nullptr;
            }
            expr->table_name = expr->column_name;
            expr->column_name = current_token_.value;
            advance();
        }
        
        return expr;
    }
    
    // Check for EXISTS (subquery)
    if (match(TokenType::EXISTS)) {
        expr->type = ExprType::EXISTS;
        expect(TokenType::LPAREN, "Expected ( after EXISTS");
        if (hasError()) return nullptr;
        
        expect(TokenType::SELECT, "Expected SELECT in subquery");
        if (hasError()) return nullptr;
        
        expr->subquery = parseSelect();
        expect(TokenType::RPAREN, "Expected ) after subquery");
        return expr;
    }
    
    setError("Expected expression");
    return nullptr;
}

// CREATE INDEX index_name ON table_name (column1, column2, ...)
std::unique_ptr<CreateIndexStatement> Parser::parseCreateIndex() {
    auto stmt = std::make_unique<CreateIndexStatement>();
    
    // Index name
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected index name");
        return nullptr;
    }
    stmt->index_name = current_token_.value;
    advance();
    
    // ON keyword
    expect(TokenType::ON, "Expected ON after index name");
    if (hasError()) return nullptr;
    
    // Table name
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected table name");
        return nullptr;
    }
    stmt->table_name = current_token_.value;
    advance();
    
    // Column list
    expect(TokenType::LPAREN, "Expected ( after table name");
    if (hasError()) return nullptr;
    
    do {
        if (!check(TokenType::IDENTIFIER)) {
            setError("Expected column name");
            return nullptr;
        }
        stmt->columns.push_back(current_token_.value);
        advance();
    } while (match(TokenType::COMMA));
    
    expect(TokenType::RPAREN, "Expected ) after column list");
    
    return stmt;
}

// DROP INDEX index_name [ON table_name]
std::unique_ptr<DropIndexStatement> Parser::parseDropIndex() {
    auto stmt = std::make_unique<DropIndexStatement>();
    
    // Index name
    if (!check(TokenType::IDENTIFIER)) {
        setError("Expected index name");
        return nullptr;
    }
    stmt->index_name = current_token_.value;
    advance();
    
    // Optional ON table_name
    if (match(TokenType::ON)) {
        if (!check(TokenType::IDENTIFIER)) {
            setError("Expected table name after ON");
            return nullptr;
        }
        stmt->table_name = current_token_.value;
        advance();
    }
    
    return stmt;
}

// BEGIN [TRANSACTION] [isolation_level]
std::unique_ptr<BeginStatement> Parser::parseBegin() {
    auto stmt = std::make_unique<BeginStatement>();
    
    // Optional TRANSACTION keyword
    match(TokenType::TRANSACTION);
    
    // Optional isolation level
    if (match(TokenType::READ)) {
        if (match(TokenType::COMMITTED)) {
            stmt->isolation_level = "READ COMMITTED";
        } else if (match(TokenType::UNCOMMITTED)) {
            stmt->isolation_level = "READ UNCOMMITTED";
        } else {
            setError("Expected COMMITTED or UNCOMMITTED after READ");
        }
    } else if (match(TokenType::REPEATABLE)) {
        expect(TokenType::READ, "Expected READ after REPEATABLE");
        stmt->isolation_level = "REPEATABLE READ";
    } else if (match(TokenType::SERIALIZABLE)) {
        stmt->isolation_level = "SERIALIZABLE";
    }
    
    return stmt;
}

// COMMIT [TRANSACTION]
std::unique_ptr<CommitStatement> Parser::parseCommit() {
    auto stmt = std::make_unique<CommitStatement>();
    match(TokenType::TRANSACTION);  // Optional
    return stmt;
}

// ROLLBACK [TRANSACTION]
std::unique_ptr<RollbackStatement> Parser::parseRollback() {
    auto stmt = std::make_unique<RollbackStatement>();
    match(TokenType::TRANSACTION);  // Optional
    return stmt;
}

} // namespace minidb
