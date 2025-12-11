#pragma once

#include "common.h"
#include "parser/parser.h"
#include "catalog/catalog.h"

namespace minidb {

// Query plan node types
enum class PlanNodeType {
    SEQ_SCAN,       // Full table scan
    INDEX_SCAN,     // B+ tree index scan
    FILTER,         // WHERE clause filter
    PROJECTION,     // SELECT columns
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    SORT,
    LIMIT,
    AGGREGATE,
    GROUP_BY
};

// Statistics for cost estimation
struct TableStats {
    uint64_t row_count;
    uint64_t page_count;
    std::unordered_map<std::string, uint64_t> distinct_values;  // column -> distinct count
};

// Query plan node
struct PlanNode {
    PlanNodeType type;
    std::string table_name;
    std::vector<std::string> columns;
    std::unique_ptr<Expression> predicate;
    std::vector<std::unique_ptr<PlanNode>> children;
    double estimated_cost;
    uint64_t estimated_rows;
    
    // For index scan
    std::string index_column;
    std::optional<int64_t> index_start;
    std::optional<int64_t> index_end;
};

// Query optimizer
class QueryOptimizer {
public:
    explicit QueryOptimizer(Catalog& catalog);
    
    // Optimize a SELECT statement
    std::unique_ptr<PlanNode> optimize(const SelectStatement& stmt);
    
    // Get table statistics
    TableStats getTableStats(const std::string& table_name);

private:
    Catalog& catalog_;
    
    // Optimization methods
    std::unique_ptr<PlanNode> createScanPlan(const std::string& table_name,
                                              const Expression* where);
    std::unique_ptr<PlanNode> createJoinPlan(const std::string& left_table,
                                              const std::vector<JoinClause>& joins);
    
    // Predicate analysis
    bool canUseIndex(const Expression* expr, const std::string& table_name,
                     std::string& index_column, int64_t& value);
    bool isEqualityOnColumn(const Expression* expr, const std::string& column_name,
                            int64_t& value);
    bool isRangeOnColumn(const Expression* expr, const std::string& column_name,
                         int64_t& start, int64_t& end);
    
    // Cost estimation
    double estimateScanCost(const std::string& table_name);
    double estimateIndexScanCost(const std::string& table_name, uint64_t selectivity);
    double estimateJoinCost(double left_rows, double right_rows);
};

} // namespace minidb
