#pragma once

#include "common.h"
#include "parser/parser.h"
#include "catalog/catalog.h"
#include "storage/buffer_pool.h"
#include "storage/wal.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "index/btree.h"

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

// Combined schema for JOINs (multiple tables)
struct CombinedSchema {
    std::vector<std::string> table_names;
    std::vector<TableSchema> schemas;
    std::vector<std::pair<std::string, int>> column_to_table;  // column name -> table index
    
    int getColumnIndex(const std::string& col_name, const std::string& table_name = "") const;
    int getTotalColumns() const;
};

// Query executor
class Executor {
public:
    Executor(Catalog& catalog, BufferPool& buffer_pool, 
             WalManager* wal = nullptr, LockManager* lock_mgr = nullptr);
    
    // Execute a parsed statement
    QueryResult execute(const Statement& stmt);
    
    // Transaction control
    QueryResult executeBegin(const BeginStatement& stmt);
    QueryResult executeCommit();
    QueryResult executeRollback();

private:
    Catalog& catalog_;
    BufferPool& buffer_pool_;
    WalManager* wal_;
    LockManager* lock_mgr_;
    TxnId current_txn_id_ = INVALID_TXN_ID;
    
    // Statement executors
    QueryResult executeSelect(const SelectStatement& stmt);
    QueryResult executeInsert(const InsertStatement& stmt);
    QueryResult executeUpdate(const UpdateStatement& stmt);
    QueryResult executeDelete(const DeleteStatement& stmt);
    QueryResult executeCreateTable(const CreateTableStatement& stmt);
    QueryResult executeDropTable(const DropTableStatement& stmt);
    QueryResult executeCreateIndex(const CreateIndexStatement& stmt);
    QueryResult executeDropIndex(const DropIndexStatement& stmt);
    
    // Row operations
    std::vector<Row> scanTable(const std::string& table_name);
    bool insertRow(const std::string& table_name, const Row& row);
    
    // JOIN operations
    std::vector<Row> executeJoin(const std::string& left_table, 
                                  const std::vector<JoinClause>& joins,
                                  CombinedSchema& combined_schema);
    std::vector<Row> innerJoin(const std::vector<Row>& left_rows,
                                const std::vector<Row>& right_rows,
                                const Expression* on_condition,
                                const CombinedSchema& schema,
                                size_t left_cols);
    std::vector<Row> leftJoin(const std::vector<Row>& left_rows,
                               const std::vector<Row>& right_rows,
                               const Expression* on_condition,
                               const CombinedSchema& schema,
                               size_t left_cols, size_t right_cols);
    
    // Aggregate operations
    Value computeAggregate(AggregateType type, const std::vector<Value>& values);
    std::vector<Row> applyGroupBy(const std::vector<Row>& rows,
                                   const std::vector<std::string>& group_by,
                                   const std::vector<SelectColumn>& select_cols,
                                   const CombinedSchema& schema);
    
    // Expression evaluation
    Value evaluateExpression(const Expression* expr, const Row& row, const TableSchema& schema);
    Value evaluateExpressionCombined(const Expression* expr, const Row& row, const CombinedSchema& schema);
    bool evaluateCondition(const Expression* expr, const Row& row, const TableSchema& schema);
    bool evaluateConditionCombined(const Expression* expr, const Row& row, const CombinedSchema& schema);
    
    // Row serialization
    std::string serializeRow(const Row& row, const TableSchema& schema);
    Row deserializeRow(const char* data, uint16_t length, const TableSchema& schema);
    
    // Helper functions
    bool matchesWhere(const Expression* where, const Row& row, const TableSchema& schema);
    bool matchesWhereCombined(const Expression* where, const Row& row, const CombinedSchema& schema);
    std::string getColumnDisplayName(const SelectColumn& col, const CombinedSchema& schema);
};

} // namespace minidb
