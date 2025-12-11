#include "optimizer/query_optimizer.h"

namespace minidb {

QueryOptimizer::QueryOptimizer(Catalog& catalog) : catalog_(catalog) {}

TableStats QueryOptimizer::getTableStats(const std::string& table_name) {
    TableStats stats;
    auto table_opt = catalog_.getTable(table_name);
    if (table_opt) {
        stats.row_count = table_opt->row_count;
        stats.page_count = (stats.row_count / 100) + 1;  // Estimate ~100 rows per page
    }
    return stats;
}

std::unique_ptr<PlanNode> QueryOptimizer::optimize(const SelectStatement& stmt) {
    std::unique_ptr<PlanNode> plan;
    
    // Step 1: Create base scan/join plan
    if (stmt.joins.empty()) {
        plan = createScanPlan(stmt.table_name, stmt.where_clause.get());
    } else {
        plan = createJoinPlan(stmt.table_name, stmt.joins);
        
        // Add filter for WHERE clause
        if (stmt.where_clause) {
            auto filter = std::make_unique<PlanNode>();
            filter->type = PlanNodeType::FILTER;
            filter->predicate = nullptr;  // Would need to clone expression
            filter->children.push_back(std::move(plan));
            plan = std::move(filter);
        }
    }
    
    // Step 2: Add GROUP BY if needed
    if (!stmt.group_by.empty()) {
        auto group = std::make_unique<PlanNode>();
        group->type = PlanNodeType::GROUP_BY;
        group->columns = stmt.group_by;
        group->children.push_back(std::move(plan));
        plan = std::move(group);
    }
    
    // Step 3: Add projection
    if (!stmt.select_all) {
        auto proj = std::make_unique<PlanNode>();
        proj->type = PlanNodeType::PROJECTION;
        for (const auto& col : stmt.select_columns) {
            if (col.expr && col.expr->type == ExprType::COLUMN_REF) {
                proj->columns.push_back(col.expr->column_name);
            }
        }
        proj->children.push_back(std::move(plan));
        plan = std::move(proj);
    }
    
    // Step 4: Add ORDER BY
    if (!stmt.order_by.empty()) {
        auto sort = std::make_unique<PlanNode>();
        sort->type = PlanNodeType::SORT;
        for (const auto& [col, asc] : stmt.order_by) {
            sort->columns.push_back(col);
        }
        sort->children.push_back(std::move(plan));
        plan = std::move(sort);
    }
    
    // Step 5: Add LIMIT
    if (stmt.limit >= 0) {
        auto limit = std::make_unique<PlanNode>();
        limit->type = PlanNodeType::LIMIT;
        limit->estimated_rows = stmt.limit;
        limit->children.push_back(std::move(plan));
        plan = std::move(limit);
    }
    
    return plan;
}

std::unique_ptr<PlanNode> QueryOptimizer::createScanPlan(const std::string& table_name,
                                                          const Expression* where) {
    auto plan = std::make_unique<PlanNode>();
    plan->table_name = table_name;
    
    auto stats = getTableStats(table_name);
    
    // Check if we can use an index
    std::string index_column;
    int64_t value;
    
    if (where && canUseIndex(where, table_name, index_column, value)) {
        // Use index scan
        plan->type = PlanNodeType::INDEX_SCAN;
        plan->index_column = index_column;
        plan->index_start = value;
        plan->index_end = value;
        plan->estimated_cost = estimateIndexScanCost(table_name, 1);
        plan->estimated_rows = 1;  // Assuming unique key
    } else {
        // Full table scan
        plan->type = PlanNodeType::SEQ_SCAN;
        plan->estimated_cost = estimateScanCost(table_name);
        plan->estimated_rows = stats.row_count;
        
        // Add filter if WHERE clause exists
        if (where) {
            auto filter = std::make_unique<PlanNode>();
            filter->type = PlanNodeType::FILTER;
            filter->estimated_rows = stats.row_count / 10;  // Assume 10% selectivity
            filter->children.push_back(std::move(plan));
            plan = std::move(filter);
        }
    }
    
    return plan;
}

std::unique_ptr<PlanNode> QueryOptimizer::createJoinPlan(const std::string& left_table,
                                                          const std::vector<JoinClause>& joins) {
    // Start with left table scan
    auto plan = std::make_unique<PlanNode>();
    plan->type = PlanNodeType::SEQ_SCAN;
    plan->table_name = left_table;
    
    auto left_stats = getTableStats(left_table);
    plan->estimated_rows = left_stats.row_count;
    plan->estimated_cost = estimateScanCost(left_table);
    
    // Add each join
    for (const auto& join : joins) {
        auto right_scan = std::make_unique<PlanNode>();
        right_scan->type = PlanNodeType::SEQ_SCAN;
        right_scan->table_name = join.table_name;
        
        auto right_stats = getTableStats(join.table_name);
        right_scan->estimated_rows = right_stats.row_count;
        right_scan->estimated_cost = estimateScanCost(join.table_name);
        
        auto join_node = std::make_unique<PlanNode>();
        join_node->type = PlanNodeType::NESTED_LOOP_JOIN;
        join_node->estimated_rows = plan->estimated_rows * right_scan->estimated_rows / 10;
        join_node->estimated_cost = estimateJoinCost(plan->estimated_rows, right_scan->estimated_rows);
        
        join_node->children.push_back(std::move(plan));
        join_node->children.push_back(std::move(right_scan));
        
        plan = std::move(join_node);
    }
    
    return plan;
}

bool QueryOptimizer::canUseIndex(const Expression* expr, const std::string& table_name,
                                  std::string& index_column, int64_t& value) {
    if (!expr) return false;
    
    auto table_opt = catalog_.getTable(table_name);
    if (!table_opt) return false;
    
    // Check if table has a primary key index
    if (!table_opt->primary_key_column.has_value()) return false;
    
    ColumnId pk_col = table_opt->primary_key_column.value();
    if (pk_col >= table_opt->columns.size()) return false;
    
    const std::string& pk_name = table_opt->columns[pk_col].name;
    
    // Check if expression is equality on primary key
    return isEqualityOnColumn(expr, pk_name, value) && (index_column = pk_name, true);
}

bool QueryOptimizer::isEqualityOnColumn(const Expression* expr, const std::string& column_name,
                                         int64_t& value) {
    if (!expr || expr->type != ExprType::BINARY_OP) return false;
    if (expr->binary_op != BinaryOp::EQ) return false;
    
    // Check left = column, right = literal
    if (expr->left && expr->left->type == ExprType::COLUMN_REF &&
        expr->left->column_name == column_name &&
        expr->right && expr->right->type == ExprType::LITERAL) {
        if (std::holds_alternative<int64_t>(expr->right->literal_value)) {
            value = std::get<int64_t>(expr->right->literal_value);
            return true;
        }
    }
    
    // Check right = column, left = literal
    if (expr->right && expr->right->type == ExprType::COLUMN_REF &&
        expr->right->column_name == column_name &&
        expr->left && expr->left->type == ExprType::LITERAL) {
        if (std::holds_alternative<int64_t>(expr->left->literal_value)) {
            value = std::get<int64_t>(expr->left->literal_value);
            return true;
        }
    }
    
    return false;
}

bool QueryOptimizer::isRangeOnColumn(const Expression* expr, const std::string& column_name,
                                      int64_t& start, int64_t& end) {
    // This would handle BETWEEN or combined < and > conditions
    // Simplified implementation
    return false;
}

double QueryOptimizer::estimateScanCost(const std::string& table_name) {
    auto stats = getTableStats(table_name);
    // Cost = number of pages to read
    return static_cast<double>(stats.page_count);
}

double QueryOptimizer::estimateIndexScanCost(const std::string& table_name, uint64_t selectivity) {
    // Index scan cost = height of B+ tree + number of matching records
    // Simplified: assume tree height of 3
    return 3.0 + static_cast<double>(selectivity);
}

double QueryOptimizer::estimateJoinCost(double left_rows, double right_rows) {
    // Nested loop join cost
    return left_rows * right_rows * 0.01;  // Assume some optimization
}

} // namespace minidb
