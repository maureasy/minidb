#pragma once

#include "common.h"
#include "parser/parser.h"
#include "catalog/catalog.h"
#include "storage/buffer_pool.h"

namespace minidb {

// Result of a query execution
struct QueryResult {
    bool success = false;
    std::string error_message;
    
    // For SELECT queries
    std::vector<std::string> column_names;
    std::vector<Row> rows;
    
    // For INSERT/UPDATE/DELETE
    int64_t rows_affected = 0;
    
    // For CREATE/DROP
    std::string message;
};

// Query executor
class Executor {
public:
    Executor(Catalog& catalog, BufferPool& buffer_pool);
    
    // Execute a parsed statement
    QueryResult execute(const Statement& stmt);

private:
    Catalog& catalog_;
    BufferPool& buffer_pool_;
    
    // Statement executors
    QueryResult executeSelect(const SelectStatement& stmt);
    QueryResult executeInsert(const InsertStatement& stmt);
    QueryResult executeUpdate(const UpdateStatement& stmt);
    QueryResult executeDelete(const DeleteStatement& stmt);
    QueryResult executeCreateTable(const CreateTableStatement& stmt);
    QueryResult executeDropTable(const DropTableStatement& stmt);
    
    // Row operations
    std::vector<Row> scanTable(const std::string& table_name);
    bool insertRow(const std::string& table_name, const Row& row);
    
    // Expression evaluation
    Value evaluateExpression(const Expression* expr, const Row& row, const TableSchema& schema);
    bool evaluateCondition(const Expression* expr, const Row& row, const TableSchema& schema);
    
    // Row serialization
    std::string serializeRow(const Row& row, const TableSchema& schema);
    Row deserializeRow(const char* data, uint16_t length, const TableSchema& schema);
    
    // Helper functions
    bool matchesWhere(const Expression* where, const Row& row, const TableSchema& schema);
};

} // namespace minidb
