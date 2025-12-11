#include "executor/executor.h"

namespace minidb {

Executor::Executor(Catalog& catalog, BufferPool& buffer_pool,
                   WalManager* wal, LockManager* lock_mgr)
    : catalog_(catalog), buffer_pool_(buffer_pool), 
      wal_(wal), lock_mgr_(lock_mgr), current_txn_id_(INVALID_TXN_ID),
      optimizer_(std::make_unique<QueryOptimizer>(catalog)) {}

QueryResult Executor::execute(const Statement& stmt) {
    switch (stmt.type) {
        case StatementType::SELECT:
            return executeSelect(*stmt.select);
        case StatementType::INSERT:
            return executeInsert(*stmt.insert);
        case StatementType::UPDATE:
            return executeUpdate(*stmt.update);
        case StatementType::DELETE:
            return executeDelete(*stmt.delete_stmt);
        case StatementType::CREATE_TABLE:
            return executeCreateTable(*stmt.create_table);
        case StatementType::DROP_TABLE:
            return executeDropTable(*stmt.drop_table);
        case StatementType::CREATE_INDEX:
            return executeCreateIndex(*stmt.create_index);
        case StatementType::DROP_INDEX:
            return executeDropIndex(*stmt.drop_index);
        case StatementType::BEGIN_TXN:
            return executeBegin(*stmt.begin_txn);
        case StatementType::COMMIT_TXN:
            return executeCommit();
        case StatementType::ROLLBACK_TXN:
            return executeRollback();
        default:
            QueryResult result;
            result.error_message = "Unknown statement type";
            return result;
    }
}

std::string Executor::serializeRow(const Row& row, const TableSchema& schema) {
    std::string data;
    
    for (size_t i = 0; i < row.size() && i < schema.columns.size(); i++) {
        const Value& val = row[i];
        const ColumnInfo& col = schema.columns[i];
        
        // Type tag
        uint8_t type_tag;
        if (std::holds_alternative<std::monostate>(val)) {
            type_tag = 0;  // NULL
            data += static_cast<char>(type_tag);
        } else if (std::holds_alternative<int64_t>(val)) {
            type_tag = 1;
            data += static_cast<char>(type_tag);
            int64_t v = std::get<int64_t>(val);
            data.append(reinterpret_cast<const char*>(&v), sizeof(int64_t));
        } else if (std::holds_alternative<double>(val)) {
            type_tag = 2;
            data += static_cast<char>(type_tag);
            double v = std::get<double>(val);
            data.append(reinterpret_cast<const char*>(&v), sizeof(double));
        } else if (std::holds_alternative<std::string>(val)) {
            type_tag = 3;
            data += static_cast<char>(type_tag);
            const std::string& s = std::get<std::string>(val);
            uint16_t len = static_cast<uint16_t>(s.length());
            data.append(reinterpret_cast<const char*>(&len), sizeof(uint16_t));
            data.append(s);
        } else if (std::holds_alternative<bool>(val)) {
            type_tag = 4;
            data += static_cast<char>(type_tag);
            uint8_t v = std::get<bool>(val) ? 1 : 0;
            data += static_cast<char>(v);
        }
    }
    
    return data;
}

Row Executor::deserializeRow(const char* data, uint16_t length, const TableSchema& schema) {
    Row row;
    size_t offset = 0;
    
    for (size_t i = 0; i < schema.columns.size() && offset < length; i++) {
        uint8_t type_tag = static_cast<uint8_t>(data[offset++]);
        
        switch (type_tag) {
            case 0:  // NULL
                row.push_back(std::monostate{});
                break;
            case 1: {  // INT
                int64_t v;
                std::memcpy(&v, data + offset, sizeof(int64_t));
                offset += sizeof(int64_t);
                row.push_back(v);
                break;
            }
            case 2: {  // FLOAT
                double v;
                std::memcpy(&v, data + offset, sizeof(double));
                offset += sizeof(double);
                row.push_back(v);
                break;
            }
            case 3: {  // STRING
                uint16_t len;
                std::memcpy(&len, data + offset, sizeof(uint16_t));
                offset += sizeof(uint16_t);
                std::string s(data + offset, len);
                offset += len;
                row.push_back(s);
                break;
            }
            case 4: {  // BOOL
                uint8_t v = static_cast<uint8_t>(data[offset++]);
                row.push_back(v != 0);
                break;
            }
            default:
                row.push_back(std::monostate{});
                break;
        }
    }
    
    return row;
}

std::vector<Row> Executor::scanTable(const std::string& table_name) {
    std::vector<Row> rows;
    
    auto table_opt = catalog_.getTable(table_name);
    if (!table_opt) return rows;
    
    const TableSchema& schema = table_opt.value();
    
    if (schema.first_page == INVALID_PAGE_ID) {
        return rows;  // Empty table
    }
    
    PageId current_page_id = schema.first_page;
    
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_.fetchPage(current_page_id);
        if (!page) break;
        
        // Iterate through all slots
        for (SlotId slot = 0; slot < page->getNumSlots(); slot++) {
            char buffer[PAGE_SIZE];
            uint16_t length;
            
            if (page->getRecord(slot, buffer, length)) {
                Row row = deserializeRow(buffer, length, schema);
                rows.push_back(std::move(row));
            }
        }
        
        PageId next_page = page->getNextPage();
        buffer_pool_.unpinPage(current_page_id, false);
        current_page_id = next_page;
    }
    
    return rows;
}

bool Executor::insertRow(const std::string& table_name, const Row& row) {
    auto table_opt = catalog_.getTable(table_name);
    if (!table_opt) return false;
    
    TableSchema schema = table_opt.value();
    
    std::string data = serializeRow(row, schema);
    
    // Find or create a page with enough space
    Page* page = nullptr;
    PageId page_id = schema.first_page;
    
    if (page_id == INVALID_PAGE_ID) {
        // Create first page
        page = buffer_pool_.newPage(page_id);
        if (!page) return false;
        catalog_.setFirstPage(table_name, page_id);
        schema.first_page = page_id;
    } else {
        // Try to find a page with space
        while (page_id != INVALID_PAGE_ID) {
            page = buffer_pool_.fetchPage(page_id);
            if (!page) return false;
            
            if (page->getFreeSpace() >= data.length() + sizeof(SlotEntry)) {
                break;  // Found a page with space
            }
            
            PageId next_page = page->getNextPage();
            
            if (next_page == INVALID_PAGE_ID) {
                // Need to allocate new page
                PageId new_page_id;
                Page* new_page = buffer_pool_.newPage(new_page_id);
                if (!new_page) {
                    buffer_pool_.unpinPage(page_id, false);
                    return false;
                }
                
                page->setNextPage(new_page_id);
                buffer_pool_.unpinPage(page_id, true);
                
                page = new_page;
                page_id = new_page_id;
                break;
            }
            
            buffer_pool_.unpinPage(page_id, false);
            page_id = next_page;
        }
    }
    
    // Insert the record
    SlotId slot_id;
    bool success = page->insertRecord(data.c_str(), static_cast<uint16_t>(data.length()), slot_id);
    
    if (success) {
        // Log to WAL for crash recovery
        if (wal_ && current_txn_id_ != INVALID_TXN_ID) {
            wal_->logInsert(current_txn_id_, page_id, slot_id, 
                           data.c_str(), static_cast<uint16_t>(data.length()));
        }
        
        catalog_.updateRowCount(table_name, 1);
        
        // Update index if there's a primary key
        if (schema.primary_key_column.has_value()) {
            ColumnId pk_col = schema.primary_key_column.value();
            if (pk_col < row.size() && std::holds_alternative<int64_t>(row[pk_col])) {
                int64_t key = std::get<int64_t>(row[pk_col]);
                BTree* index = catalog_.getIndex(table_name);
                if (index) {
                    index->insert(key, {page_id, slot_id});
                }
            }
        }
    }
    
    buffer_pool_.unpinPage(page_id, success);
    return success;
}

Value Executor::evaluateExpression(const Expression* expr, const Row& row, const TableSchema& schema) {
    if (!expr) return std::monostate{};
    
    switch (expr->type) {
        case ExprType::LITERAL:
            return expr->literal_value;
            
        case ExprType::COLUMN_REF: {
            int idx = schema.getColumnIndex(expr->column_name);
            if (idx >= 0 && idx < static_cast<int>(row.size())) {
                return row[idx];
            }
            return std::monostate{};
        }
        
        case ExprType::BINARY_OP: {
            Value left = evaluateExpression(expr->left.get(), row, schema);
            Value right = evaluateExpression(expr->right.get(), row, schema);
            
            // Handle comparisons and arithmetic
            switch (expr->binary_op) {
                case BinaryOp::EQ:
                    return left == right;
                case BinaryOp::NE:
                    return left != right;
                case BinaryOp::LT:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) < std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) < std::get<double>(right);
                    }
                    if (std::holds_alternative<std::string>(left) && std::holds_alternative<std::string>(right)) {
                        return std::get<std::string>(left) < std::get<std::string>(right);
                    }
                    return false;
                case BinaryOp::GT:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) > std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) > std::get<double>(right);
                    }
                    if (std::holds_alternative<std::string>(left) && std::holds_alternative<std::string>(right)) {
                        return std::get<std::string>(left) > std::get<std::string>(right);
                    }
                    return false;
                case BinaryOp::LE:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) <= std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) <= std::get<double>(right);
                    }
                    return false;
                case BinaryOp::GE:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) >= std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) >= std::get<double>(right);
                    }
                    return false;
                case BinaryOp::AND:
                    return std::holds_alternative<bool>(left) && std::holds_alternative<bool>(right) &&
                           std::get<bool>(left) && std::get<bool>(right);
                case BinaryOp::OR:
                    return std::holds_alternative<bool>(left) && std::holds_alternative<bool>(right) &&
                           (std::get<bool>(left) || std::get<bool>(right));
                case BinaryOp::ADD:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) + std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) + std::get<double>(right);
                    }
                    return std::monostate{};
                case BinaryOp::SUB:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) - std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) - std::get<double>(right);
                    }
                    return std::monostate{};
                case BinaryOp::MUL:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        return std::get<int64_t>(left) * std::get<int64_t>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) * std::get<double>(right);
                    }
                    return std::monostate{};
                case BinaryOp::DIV:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        int64_t r = std::get<int64_t>(right);
                        if (r != 0) return std::get<int64_t>(left) / r;
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        double r = std::get<double>(right);
                        if (r != 0.0) return std::get<double>(left) / r;
                    }
                    return std::monostate{};
            }
            break;
        }
        
        case ExprType::UNARY_OP: {
            Value operand = evaluateExpression(expr->operand.get(), row, schema);
            switch (expr->unary_op) {
                case UnaryOp::NOT:
                    if (std::holds_alternative<bool>(operand)) {
                        return !std::get<bool>(operand);
                    }
                    return std::monostate{};
                case UnaryOp::MINUS:
                    if (std::holds_alternative<int64_t>(operand)) {
                        return -std::get<int64_t>(operand);
                    }
                    if (std::holds_alternative<double>(operand)) {
                        return -std::get<double>(operand);
                    }
                    return std::monostate{};
            }
            break;
        }
        
        default:
            break;
    }
    
    return std::monostate{};
}

bool Executor::evaluateCondition(const Expression* expr, const Row& row, const TableSchema& schema) {
    Value result = evaluateExpression(expr, row, schema);
    if (std::holds_alternative<bool>(result)) {
        return std::get<bool>(result);
    }
    return false;
}

bool Executor::matchesWhere(const Expression* where, const Row& row, const TableSchema& schema) {
    if (!where) return true;
    return evaluateCondition(where, row, schema);
}

// CombinedSchema implementation
int CombinedSchema::getColumnIndex(const std::string& col_name, const std::string& table_name) const {
    int offset = 0;
    for (size_t t = 0; t < schemas.size(); t++) {
        if (!table_name.empty() && table_names[t] != table_name) {
            offset += static_cast<int>(schemas[t].columns.size());
            continue;
        }
        for (size_t c = 0; c < schemas[t].columns.size(); c++) {
            if (schemas[t].columns[c].name == col_name) {
                return offset + static_cast<int>(c);
            }
        }
        offset += static_cast<int>(schemas[t].columns.size());
    }
    return -1;
}

int CombinedSchema::getTotalColumns() const {
    int total = 0;
    for (const auto& schema : schemas) {
        total += static_cast<int>(schema.columns.size());
    }
    return total;
}

bool Executor::matchesWhereCombined(const Expression* where, const Row& row, const CombinedSchema& schema) {
    if (!where) return true;
    return evaluateConditionCombined(where, row, schema);
}

Value Executor::evaluateExpressionCombined(const Expression* expr, const Row& row, const CombinedSchema& schema) {
    if (!expr) return std::monostate{};
    
    switch (expr->type) {
        case ExprType::LITERAL:
            return expr->literal_value;
            
        case ExprType::COLUMN_REF: {
            int idx = schema.getColumnIndex(expr->column_name, expr->table_name);
            if (idx >= 0 && idx < static_cast<int>(row.size())) {
                return row[idx];
            }
            return std::monostate{};
        }
        
        case ExprType::BINARY_OP: {
            Value left = evaluateExpressionCombined(expr->left.get(), row, schema);
            Value right = evaluateExpressionCombined(expr->right.get(), row, schema);
            
            switch (expr->binary_op) {
                case BinaryOp::EQ: return static_cast<bool>(left == right);
                case BinaryOp::NE: return static_cast<bool>(left != right);
                case BinaryOp::LT: {
                    // Handle numeric comparisons with type coercion
                    double l_val = 0, r_val = 0;
                    bool l_num = false, r_num = false;
                    if (std::holds_alternative<int64_t>(left)) { l_val = static_cast<double>(std::get<int64_t>(left)); l_num = true; }
                    else if (std::holds_alternative<double>(left)) { l_val = std::get<double>(left); l_num = true; }
                    if (std::holds_alternative<int64_t>(right)) { r_val = static_cast<double>(std::get<int64_t>(right)); r_num = true; }
                    else if (std::holds_alternative<double>(right)) { r_val = std::get<double>(right); r_num = true; }
                    if (l_num && r_num) return l_val < r_val;
                    if (std::holds_alternative<std::string>(left) && std::holds_alternative<std::string>(right))
                        return std::get<std::string>(left) < std::get<std::string>(right);
                    return false;
                }
                case BinaryOp::GT: {
                    double l_val = 0, r_val = 0;
                    bool l_num = false, r_num = false;
                    if (std::holds_alternative<int64_t>(left)) { l_val = static_cast<double>(std::get<int64_t>(left)); l_num = true; }
                    else if (std::holds_alternative<double>(left)) { l_val = std::get<double>(left); l_num = true; }
                    if (std::holds_alternative<int64_t>(right)) { r_val = static_cast<double>(std::get<int64_t>(right)); r_num = true; }
                    else if (std::holds_alternative<double>(right)) { r_val = std::get<double>(right); r_num = true; }
                    if (l_num && r_num) return l_val > r_val;
                    if (std::holds_alternative<std::string>(left) && std::holds_alternative<std::string>(right))
                        return std::get<std::string>(left) > std::get<std::string>(right);
                    return false;
                }
                case BinaryOp::LE: {
                    double l_val = 0, r_val = 0;
                    bool l_num = false, r_num = false;
                    if (std::holds_alternative<int64_t>(left)) { l_val = static_cast<double>(std::get<int64_t>(left)); l_num = true; }
                    else if (std::holds_alternative<double>(left)) { l_val = std::get<double>(left); l_num = true; }
                    if (std::holds_alternative<int64_t>(right)) { r_val = static_cast<double>(std::get<int64_t>(right)); r_num = true; }
                    else if (std::holds_alternative<double>(right)) { r_val = std::get<double>(right); r_num = true; }
                    if (l_num && r_num) return l_val <= r_val;
                    return false;
                }
                case BinaryOp::GE: {
                    double l_val = 0, r_val = 0;
                    bool l_num = false, r_num = false;
                    if (std::holds_alternative<int64_t>(left)) { l_val = static_cast<double>(std::get<int64_t>(left)); l_num = true; }
                    else if (std::holds_alternative<double>(left)) { l_val = std::get<double>(left); l_num = true; }
                    if (std::holds_alternative<int64_t>(right)) { r_val = static_cast<double>(std::get<int64_t>(right)); r_num = true; }
                    else if (std::holds_alternative<double>(right)) { r_val = std::get<double>(right); r_num = true; }
                    if (l_num && r_num) return l_val >= r_val;
                    return false;
                }
                case BinaryOp::AND:
                    return std::holds_alternative<bool>(left) && std::holds_alternative<bool>(right) &&
                           std::get<bool>(left) && std::get<bool>(right);
                case BinaryOp::OR:
                    return std::holds_alternative<bool>(left) && std::holds_alternative<bool>(right) &&
                           (std::get<bool>(left) || std::get<bool>(right));
                case BinaryOp::ADD:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right))
                        return std::get<int64_t>(left) + std::get<int64_t>(right);
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right))
                        return std::get<double>(left) + std::get<double>(right);
                    return std::monostate{};
                case BinaryOp::SUB:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right))
                        return std::get<int64_t>(left) - std::get<int64_t>(right);
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right))
                        return std::get<double>(left) - std::get<double>(right);
                    return std::monostate{};
                case BinaryOp::MUL:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right))
                        return std::get<int64_t>(left) * std::get<int64_t>(right);
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right))
                        return std::get<double>(left) * std::get<double>(right);
                    return std::monostate{};
                case BinaryOp::DIV:
                    if (std::holds_alternative<int64_t>(left) && std::holds_alternative<int64_t>(right)) {
                        int64_t r = std::get<int64_t>(right);
                        if (r != 0) return std::get<int64_t>(left) / r;
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        double r = std::get<double>(right);
                        if (r != 0.0) return std::get<double>(left) / r;
                    }
                    return std::monostate{};
            }
            break;
        }
        
        case ExprType::UNARY_OP: {
            Value operand = evaluateExpressionCombined(expr->operand.get(), row, schema);
            switch (expr->unary_op) {
                case UnaryOp::NOT:
                    if (std::holds_alternative<bool>(operand)) return !std::get<bool>(operand);
                    return std::monostate{};
                case UnaryOp::MINUS:
                    if (std::holds_alternative<int64_t>(operand)) return -std::get<int64_t>(operand);
                    if (std::holds_alternative<double>(operand)) return -std::get<double>(operand);
                    return std::monostate{};
            }
            break;
        }
        
        case ExprType::EXISTS: {
            // Execute subquery and check if any rows returned
            if (expr->subquery) {
                // Build a temporary result by executing the subquery
                auto subquery_table = catalog_.getTable(expr->subquery->table_name);
                if (!subquery_table) return false;
                
                std::vector<Row> subquery_rows = scanTable(expr->subquery->table_name);
                
                // Apply WHERE clause if present
                if (expr->subquery->where_clause) {
                    std::vector<Row> filtered;
                    for (const auto& r : subquery_rows) {
                        if (matchesWhere(expr->subquery->where_clause.get(), r, subquery_table.value())) {
                            filtered.push_back(r);
                        }
                    }
                    subquery_rows = std::move(filtered);
                }
                
                return !subquery_rows.empty();
            }
            return false;
        }
        
        default:
            break;
    }
    
    return std::monostate{};
}

bool Executor::evaluateConditionCombined(const Expression* expr, const Row& row, const CombinedSchema& schema) {
    Value result = evaluateExpressionCombined(expr, row, schema);
    if (std::holds_alternative<bool>(result)) {
        return std::get<bool>(result);
    }
    return false;
}

std::vector<Row> Executor::innerJoin(const std::vector<Row>& left_rows,
                                      const std::vector<Row>& right_rows,
                                      const Expression* on_condition,
                                      const CombinedSchema& schema,
                                      size_t left_cols) {
    std::vector<Row> result;
    
    for (const auto& left_row : left_rows) {
        for (const auto& right_row : right_rows) {
            // Combine rows
            Row combined;
            combined.insert(combined.end(), left_row.begin(), left_row.end());
            combined.insert(combined.end(), right_row.begin(), right_row.end());
            
            // Check ON condition
            if (matchesWhereCombined(on_condition, combined, schema)) {
                result.push_back(std::move(combined));
            }
        }
    }
    
    return result;
}

std::vector<Row> Executor::leftJoin(const std::vector<Row>& left_rows,
                                     const std::vector<Row>& right_rows,
                                     const Expression* on_condition,
                                     const CombinedSchema& schema,
                                     size_t left_cols, size_t right_cols) {
    std::vector<Row> result;
    
    for (const auto& left_row : left_rows) {
        bool matched = false;
        
        for (const auto& right_row : right_rows) {
            Row combined;
            combined.insert(combined.end(), left_row.begin(), left_row.end());
            combined.insert(combined.end(), right_row.begin(), right_row.end());
            
            if (matchesWhereCombined(on_condition, combined, schema)) {
                result.push_back(std::move(combined));
                matched = true;
            }
        }
        
        // If no match, add left row with NULLs for right side
        if (!matched) {
            Row combined;
            combined.insert(combined.end(), left_row.begin(), left_row.end());
            for (size_t i = 0; i < right_cols; i++) {
                combined.push_back(std::monostate{});
            }
            result.push_back(std::move(combined));
        }
    }
    
    return result;
}

std::vector<Row> Executor::executeJoin(const std::string& left_table, 
                                        const std::vector<JoinClause>& joins,
                                        CombinedSchema& combined_schema) {
    // Start with left table
    auto left_opt = catalog_.getTable(left_table);
    if (!left_opt) return {};
    
    combined_schema.table_names.push_back(left_table);
    combined_schema.schemas.push_back(left_opt.value());
    
    std::vector<Row> result = scanTable(left_table);
    
    // Process each join
    for (const auto& join : joins) {
        auto right_opt = catalog_.getTable(join.table_name);
        if (!right_opt) return {};
        
        combined_schema.table_names.push_back(join.table_name);
        combined_schema.schemas.push_back(right_opt.value());
        
        std::vector<Row> right_rows = scanTable(join.table_name);
        size_t left_cols = 0;
        for (size_t i = 0; i < combined_schema.schemas.size() - 1; i++) {
            left_cols += combined_schema.schemas[i].columns.size();
        }
        size_t right_cols = right_opt.value().columns.size();
        
        switch (join.type) {
            case JoinType::INNER:
                result = innerJoin(result, right_rows, join.on_condition.get(), combined_schema, left_cols);
                break;
            case JoinType::LEFT:
                result = leftJoin(result, right_rows, join.on_condition.get(), combined_schema, left_cols, right_cols);
                break;
            case JoinType::RIGHT:
                // Right join = swap left and right, then do left join
                result = leftJoin(right_rows, result, join.on_condition.get(), combined_schema, right_cols, left_cols);
                break;
        }
    }
    
    return result;
}

Value Executor::computeAggregate(AggregateType type, const std::vector<Value>& values) {
    if (values.empty() && type != AggregateType::COUNT) {
        return std::monostate{};
    }
    
    switch (type) {
        case AggregateType::COUNT:
            return static_cast<int64_t>(values.size());
            
        case AggregateType::SUM: {
            double sum = 0;
            bool has_double = false;
            for (const auto& v : values) {
                if (std::holds_alternative<int64_t>(v)) {
                    sum += static_cast<double>(std::get<int64_t>(v));
                } else if (std::holds_alternative<double>(v)) {
                    sum += std::get<double>(v);
                    has_double = true;
                }
            }
            if (has_double) return sum;
            return static_cast<int64_t>(sum);
        }
        
        case AggregateType::AVG: {
            if (values.empty()) return std::monostate{};
            double sum = 0;
            int count = 0;
            for (const auto& v : values) {
                if (std::holds_alternative<int64_t>(v)) {
                    sum += static_cast<double>(std::get<int64_t>(v));
                    count++;
                } else if (std::holds_alternative<double>(v)) {
                    sum += std::get<double>(v);
                    count++;
                }
            }
            if (count == 0) return std::monostate{};
            return sum / count;
        }
        
        case AggregateType::MIN: {
            Value min_val = values[0];
            for (size_t i = 1; i < values.size(); i++) {
                const auto& v = values[i];
                if (std::holds_alternative<int64_t>(v) && std::holds_alternative<int64_t>(min_val)) {
                    if (std::get<int64_t>(v) < std::get<int64_t>(min_val)) min_val = v;
                } else if (std::holds_alternative<double>(v) && std::holds_alternative<double>(min_val)) {
                    if (std::get<double>(v) < std::get<double>(min_val)) min_val = v;
                } else if (std::holds_alternative<std::string>(v) && std::holds_alternative<std::string>(min_val)) {
                    if (std::get<std::string>(v) < std::get<std::string>(min_val)) min_val = v;
                }
            }
            return min_val;
        }
        
        case AggregateType::MAX: {
            Value max_val = values[0];
            for (size_t i = 1; i < values.size(); i++) {
                const auto& v = values[i];
                if (std::holds_alternative<int64_t>(v) && std::holds_alternative<int64_t>(max_val)) {
                    if (std::get<int64_t>(v) > std::get<int64_t>(max_val)) max_val = v;
                } else if (std::holds_alternative<double>(v) && std::holds_alternative<double>(max_val)) {
                    if (std::get<double>(v) > std::get<double>(max_val)) max_val = v;
                } else if (std::holds_alternative<std::string>(v) && std::holds_alternative<std::string>(max_val)) {
                    if (std::get<std::string>(v) > std::get<std::string>(max_val)) max_val = v;
                }
            }
            return max_val;
        }
    }
    
    return std::monostate{};
}

std::string Executor::getColumnDisplayName(const SelectColumn& col, const CombinedSchema& schema) {
    if (!col.alias.empty()) return col.alias;
    
    if (col.expr) {
        if (col.expr->type == ExprType::COLUMN_REF) {
            return col.expr->column_name;
        } else if (col.expr->type == ExprType::AGGREGATE_FUNC) {
            std::string name;
            switch (col.expr->aggregate_type) {
                case AggregateType::COUNT: name = "COUNT"; break;
                case AggregateType::SUM: name = "SUM"; break;
                case AggregateType::AVG: name = "AVG"; break;
                case AggregateType::MIN: name = "MIN"; break;
                case AggregateType::MAX: name = "MAX"; break;
            }
            if (col.expr->aggregate_arg) {
                if (col.expr->aggregate_arg->type == ExprType::COLUMN_REF) {
                    name += "(" + col.expr->aggregate_arg->column_name + ")";
                } else {
                    name += "(expr)";
                }
            } else {
                name += "(*)";
            }
            return name;
        }
    }
    return "?";
}

std::vector<Row> Executor::applyGroupBy(const std::vector<Row>& rows,
                                         const std::vector<std::string>& group_by,
                                         const std::vector<SelectColumn>& select_cols,
                                         const CombinedSchema& schema) {
    if (group_by.empty() && select_cols.empty()) return rows;
    
    // Build groups
    std::map<std::vector<Value>, std::vector<const Row*>> groups;
    
    for (const auto& row : rows) {
        std::vector<Value> key;
        for (const auto& col_name : group_by) {
            int idx = schema.getColumnIndex(col_name);
            if (idx >= 0 && idx < static_cast<int>(row.size())) {
                key.push_back(row[idx]);
            } else {
                key.push_back(std::monostate{});
            }
        }
        groups[key].push_back(&row);
    }
    
    // If no GROUP BY but has aggregates, treat all rows as one group
    if (group_by.empty()) {
        std::vector<Value> empty_key;
        std::vector<const Row*> all_rows;
        for (const auto& row : rows) {
            all_rows.push_back(&row);
        }
        groups[empty_key] = all_rows;
    }
    
    // Compute result for each group
    std::vector<Row> result;
    for (const auto& [key, group_rows] : groups) {
        Row result_row;
        
        for (const auto& sel_col : select_cols) {
            if (!sel_col.expr) {
                result_row.push_back(std::monostate{});
                continue;
            }
            
            if (sel_col.expr->type == ExprType::AGGREGATE_FUNC) {
                // Collect values for aggregate
                std::vector<Value> agg_values;
                for (const Row* r : group_rows) {
                    if (sel_col.expr->aggregate_arg) {
                        Value v = evaluateExpressionCombined(sel_col.expr->aggregate_arg.get(), *r, schema);
                        if (!std::holds_alternative<std::monostate>(v)) {
                            agg_values.push_back(v);
                        }
                    } else {
                        // COUNT(*)
                        agg_values.push_back(static_cast<int64_t>(1));
                    }
                }
                result_row.push_back(computeAggregate(sel_col.expr->aggregate_type, agg_values));
            } else if (sel_col.expr->type == ExprType::COLUMN_REF) {
                // For non-aggregate columns in GROUP BY, use first row's value
                if (!group_rows.empty()) {
                    int idx = schema.getColumnIndex(sel_col.expr->column_name, sel_col.expr->table_name);
                    if (idx >= 0 && idx < static_cast<int>(group_rows[0]->size())) {
                        result_row.push_back((*group_rows[0])[idx]);
                    } else {
                        result_row.push_back(std::monostate{});
                    }
                }
            } else {
                // Other expressions - evaluate with first row
                if (!group_rows.empty()) {
                    result_row.push_back(evaluateExpressionCombined(sel_col.expr.get(), *group_rows[0], schema));
                } else {
                    result_row.push_back(std::monostate{});
                }
            }
        }
        
        result.push_back(std::move(result_row));
    }
    
    return result;
}

QueryResult Executor::executeSelect(const SelectStatement& stmt) {
    QueryResult result;
    
    auto table_opt = catalog_.getTable(stmt.table_name);
    if (!table_opt) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    const TableSchema& schema = table_opt.value();
    CombinedSchema combined_schema;
    std::vector<Row> all_rows;
    
    // Use optimizer to create execution plan
    auto plan = optimizer_->optimize(stmt);
    
    // Handle JOINs
    if (!stmt.joins.empty()) {
        all_rows = executeJoin(stmt.table_name, stmt.joins, combined_schema);
    } else {
        combined_schema.table_names.push_back(stmt.table_name);
        combined_schema.schemas.push_back(schema);
        
        // Execute based on plan type
        all_rows = executePlan(plan.get(), stmt);
    }
    
    // Apply WHERE clause
    std::vector<Row> filtered_rows;
    for (const auto& row : all_rows) {
        if (matchesWhereCombined(stmt.where_clause.get(), row, combined_schema)) {
            filtered_rows.push_back(row);
        }
    }
    
    // Check if we have aggregates or GROUP BY
    bool has_aggregates = false;
    for (const auto& col : stmt.select_columns) {
        if (col.expr && col.expr->type == ExprType::AGGREGATE_FUNC) {
            has_aggregates = true;
            break;
        }
    }
    
    // Handle SELECT * vs specific columns
    if (stmt.select_all) {
        // SELECT * - all columns from all tables
        for (const auto& s : combined_schema.schemas) {
            for (const auto& col : s.columns) {
                result.column_names.push_back(col.name);
            }
        }
        result.rows = std::move(filtered_rows);
    } else if (has_aggregates || !stmt.group_by.empty()) {
        // GROUP BY / Aggregates
        for (const auto& col : stmt.select_columns) {
            result.column_names.push_back(getColumnDisplayName(col, combined_schema));
        }
        result.rows = applyGroupBy(filtered_rows, stmt.group_by, stmt.select_columns, combined_schema);
        
        // Apply HAVING
        if (stmt.having_clause) {
            std::vector<Row> having_filtered;
            for (const auto& row : result.rows) {
                // Note: HAVING evaluation would need special handling for aggregates
                // This is a simplified version
                having_filtered.push_back(row);
            }
            result.rows = std::move(having_filtered);
        }
    } else {
        // Regular SELECT with specific columns
        for (const auto& col : stmt.select_columns) {
            result.column_names.push_back(getColumnDisplayName(col, combined_schema));
        }
        
        for (const auto& row : filtered_rows) {
            Row selected_row;
            for (const auto& col : stmt.select_columns) {
                if (col.expr) {
                    selected_row.push_back(evaluateExpressionCombined(col.expr.get(), row, combined_schema));
                } else {
                    selected_row.push_back(std::monostate{});
                }
            }
            result.rows.push_back(std::move(selected_row));
        }
    }
    
    // Apply DISTINCT
    if (stmt.is_distinct && !result.rows.empty()) {
        std::vector<Row> unique_rows;
        for (const auto& row : result.rows) {
            bool found = false;
            for (const auto& unique_row : unique_rows) {
                if (row == unique_row) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                unique_rows.push_back(row);
            }
        }
        result.rows = std::move(unique_rows);
    }
    
    // Apply ORDER BY
    if (!stmt.order_by.empty()) {
        std::vector<std::pair<int, bool>> order_info;
        for (const auto& [col_name, ascending] : stmt.order_by) {
            auto it = std::find(result.column_names.begin(), result.column_names.end(), col_name);
            if (it != result.column_names.end()) {
                int idx = static_cast<int>(it - result.column_names.begin());
                order_info.push_back({idx, ascending});
            }
        }
        
        if (!order_info.empty()) {
            std::sort(result.rows.begin(), result.rows.end(),
                [&order_info](const Row& a, const Row& b) {
                    for (const auto& [idx, asc] : order_info) {
                        if (idx >= static_cast<int>(a.size()) || idx >= static_cast<int>(b.size())) continue;
                        
                        const Value& va = a[idx];
                        const Value& vb = b[idx];
                        
                        if (va == vb) continue;
                        
                        bool less = false;
                        if (std::holds_alternative<int64_t>(va) && std::holds_alternative<int64_t>(vb)) {
                            less = std::get<int64_t>(va) < std::get<int64_t>(vb);
                        } else if (std::holds_alternative<double>(va) && std::holds_alternative<double>(vb)) {
                            less = std::get<double>(va) < std::get<double>(vb);
                        } else if (std::holds_alternative<std::string>(va) && std::holds_alternative<std::string>(vb)) {
                            less = std::get<std::string>(va) < std::get<std::string>(vb);
                        }
                        
                        return asc ? less : !less;
                    }
                    return false;
                });
        }
    }
    
    // Apply OFFSET and LIMIT
    if (stmt.offset > 0 && stmt.offset < static_cast<int>(result.rows.size())) {
        result.rows.erase(result.rows.begin(), result.rows.begin() + stmt.offset);
    } else if (stmt.offset >= static_cast<int>(result.rows.size())) {
        result.rows.clear();
    }
    
    if (stmt.limit >= 0 && stmt.limit < static_cast<int>(result.rows.size())) {
        result.rows.resize(stmt.limit);
    }
    
    result.success = true;
    return result;
}

QueryResult Executor::executeInsert(const InsertStatement& stmt) {
    QueryResult result;
    
    auto table_opt = catalog_.getTable(stmt.table_name);
    if (!table_opt) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    const TableSchema& schema = table_opt.value();
    
    // Determine column mapping
    std::vector<int> column_map;
    if (stmt.columns.empty()) {
        // All columns in order
        for (size_t i = 0; i < schema.columns.size(); i++) {
            column_map.push_back(static_cast<int>(i));
        }
    } else {
        for (const auto& col_name : stmt.columns) {
            int idx = schema.getColumnIndex(col_name);
            if (idx < 0) {
                result.error_message = "Column not found: " + col_name;
                return result;
            }
            column_map.push_back(idx);
        }
    }
    
    // Insert each row
    for (const auto& values : stmt.values) {
        if (values.size() != column_map.size()) {
            result.error_message = "Column count mismatch";
            return result;
        }
        
        // Build the full row
        Row row(schema.columns.size(), std::monostate{});
        for (size_t i = 0; i < values.size(); i++) {
            row[column_map[i]] = values[i];
        }
        
        if (!insertRow(stmt.table_name, row)) {
            result.error_message = "Failed to insert row";
            return result;
        }
        result.rows_affected++;
    }
    
    result.success = true;
    result.message = "Inserted " + std::to_string(result.rows_affected) + " row(s)";
    return result;
}

QueryResult Executor::executeUpdate(const UpdateStatement& stmt) {
    QueryResult result;
    
    auto table_opt = catalog_.getTable(stmt.table_name);
    if (!table_opt) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    const TableSchema& schema = table_opt.value();
    
    // Build assignment map
    std::vector<std::pair<int, Value>> assignments;
    for (const auto& [col_name, val] : stmt.assignments) {
        int idx = schema.getColumnIndex(col_name);
        if (idx < 0) {
            result.error_message = "Column not found: " + col_name;
            return result;
        }
        assignments.push_back({idx, val});
    }
    
    // Scan and update matching rows
    if (schema.first_page == INVALID_PAGE_ID) {
        result.success = true;
        result.message = "Updated 0 row(s)";
        return result;
    }
    
    PageId current_page_id = schema.first_page;
    
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_.fetchPage(current_page_id);
        if (!page) break;
        
        bool page_modified = false;
        
        for (SlotId slot = 0; slot < page->getNumSlots(); slot++) {
            char buffer[PAGE_SIZE];
            uint16_t length;
            
            if (page->getRecord(slot, buffer, length)) {
                Row row = deserializeRow(buffer, length, schema);
                
                if (matchesWhere(stmt.where_clause.get(), row, schema)) {
                    // Apply updates
                    for (const auto& [idx, val] : assignments) {
                        row[idx] = val;
                    }
                    
                    // Serialize and update
                    std::string new_data = serializeRow(row, schema);
                    if (page->updateRecord(slot, new_data.c_str(), static_cast<uint16_t>(new_data.length()))) {
                        // Log to WAL for crash recovery
                        if (wal_ && current_txn_id_ != INVALID_TXN_ID) {
                            wal_->logUpdate(current_txn_id_, current_page_id, slot,
                                           buffer, length,
                                           new_data.c_str(), static_cast<uint16_t>(new_data.length()));
                        }
                        result.rows_affected++;
                        page_modified = true;
                    }
                }
            }
        }
        
        PageId next_page = page->getNextPage();
        buffer_pool_.unpinPage(current_page_id, page_modified);
        current_page_id = next_page;
    }
    
    result.success = true;
    result.message = "Updated " + std::to_string(result.rows_affected) + " row(s)";
    return result;
}

QueryResult Executor::executeDelete(const DeleteStatement& stmt) {
    QueryResult result;
    
    auto table_opt = catalog_.getTable(stmt.table_name);
    if (!table_opt) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    const TableSchema& schema = table_opt.value();
    
    if (schema.first_page == INVALID_PAGE_ID) {
        result.success = true;
        result.message = "Deleted 0 row(s)";
        return result;
    }
    
    PageId current_page_id = schema.first_page;
    
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_.fetchPage(current_page_id);
        if (!page) break;
        
        bool page_modified = false;
        
        for (SlotId slot = 0; slot < page->getNumSlots(); slot++) {
            char buffer[PAGE_SIZE];
            uint16_t length;
            
            if (page->getRecord(slot, buffer, length)) {
                Row row = deserializeRow(buffer, length, schema);
                
                if (matchesWhere(stmt.where_clause.get(), row, schema)) {
                    if (page->deleteRecord(slot)) {
                        // Log to WAL for crash recovery
                        if (wal_ && current_txn_id_ != INVALID_TXN_ID) {
                            wal_->logDelete(current_txn_id_, current_page_id, slot,
                                           buffer, length);
                        }
                        result.rows_affected++;
                        page_modified = true;
                        catalog_.updateRowCount(stmt.table_name, -1);
                    }
                }
            }
        }
        
        PageId next_page = page->getNextPage();
        buffer_pool_.unpinPage(current_page_id, page_modified);
        current_page_id = next_page;
    }
    
    result.success = true;
    result.message = "Deleted " + std::to_string(result.rows_affected) + " row(s)";
    return result;
}

QueryResult Executor::executeCreateTable(const CreateTableStatement& stmt) {
    QueryResult result;
    
    if (catalog_.tableExists(stmt.table_name)) {
        result.error_message = "Table already exists: " + stmt.table_name;
        return result;
    }
    
    std::vector<ColumnInfo> columns;
    for (const auto& col_def : stmt.columns) {
        ColumnInfo col;
        col.name = col_def.name;
        col.type = col_def.type;
        col.size = col_def.size;
        col.is_primary_key = col_def.is_primary_key;
        col.is_nullable = col_def.is_nullable;
        columns.push_back(col);
    }
    
    if (!catalog_.createTable(stmt.table_name, columns)) {
        result.error_message = "Failed to create table";
        return result;
    }
    
    result.success = true;
    result.message = "Table created: " + stmt.table_name;
    return result;
}

QueryResult Executor::executeDropTable(const DropTableStatement& stmt) {
    QueryResult result;
    
    if (!catalog_.tableExists(stmt.table_name)) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    // TODO: Delete all pages associated with the table
    
    if (!catalog_.dropTable(stmt.table_name)) {
        result.error_message = "Failed to drop table";
        return result;
    }
    
    result.success = true;
    result.message = "Table dropped: " + stmt.table_name;
    return result;
}

QueryResult Executor::executeCreateIndex(const CreateIndexStatement& stmt) {
    QueryResult result;
    
    if (catalog_.indexExists(stmt.index_name)) {
        result.error_message = "Index already exists: " + stmt.index_name;
        return result;
    }
    
    if (!catalog_.tableExists(stmt.table_name)) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    if (!catalog_.createNamedIndex(stmt.index_name, stmt.table_name, stmt.columns, stmt.is_unique)) {
        result.error_message = "Failed to create index";
        return result;
    }
    
    // Build the index by scanning existing data
    auto table_opt = catalog_.getTable(stmt.table_name);
    if (table_opt && !stmt.columns.empty()) {
        BTree* index = catalog_.getIndexByName(stmt.index_name);
        if (index) {
            int col_idx = table_opt->getColumnIndex(stmt.columns[0]);
            if (col_idx >= 0) {
                std::vector<Row> rows = scanTable(stmt.table_name);
                for (size_t i = 0; i < rows.size(); i++) {
                    if (col_idx < static_cast<int>(rows[i].size())) {
                        const Value& val = rows[i][col_idx];
                        if (std::holds_alternative<int64_t>(val)) {
                            RecordId rid;
                            rid.page_id = static_cast<PageId>(i / 100);
                            rid.slot_id = static_cast<SlotId>(i % 100);
                            index->insert(std::get<int64_t>(val), rid);
                        }
                    }
                }
            }
        }
    }
    
    result.success = true;
    result.message = "Index created: " + stmt.index_name;
    return result;
}

QueryResult Executor::executeDropIndex(const DropIndexStatement& stmt) {
    QueryResult result;
    
    if (!catalog_.indexExists(stmt.index_name)) {
        result.error_message = "Index not found: " + stmt.index_name;
        return result;
    }
    
    if (!catalog_.dropIndex(stmt.index_name)) {
        result.error_message = "Failed to drop index";
        return result;
    }
    
    result.success = true;
    result.message = "Index dropped: " + stmt.index_name;
    return result;
}

QueryResult Executor::executeBegin(const BeginStatement& stmt) {
    QueryResult result;
    
    if (current_txn_id_ != INVALID_TXN_ID) {
        result.error_message = "Transaction already in progress";
        return result;
    }
    
    if (wal_) {
        current_txn_id_ = wal_->beginTransaction();
        result.success = true;
        result.message = "Transaction started (ID: " + std::to_string(current_txn_id_) + ")";
        if (!stmt.isolation_level.empty()) {
            result.message += " with isolation level " + stmt.isolation_level;
        }
    } else {
        // No WAL - simulate transaction
        current_txn_id_ = 1;
        result.success = true;
        result.message = "Transaction started (WAL disabled)";
    }
    
    return result;
}

QueryResult Executor::executeCommit() {
    QueryResult result;
    
    if (current_txn_id_ == INVALID_TXN_ID) {
        result.error_message = "No transaction in progress";
        return result;
    }
    
    if (wal_) {
        wal_->commitTransaction(current_txn_id_);
    }
    
    // Release all locks held by this transaction
    if (lock_mgr_) {
        lock_mgr_->releaseAllLocks(current_txn_id_);
    }
    
    // Flush dirty pages
    buffer_pool_.flushAllPages();
    
    result.success = true;
    result.message = "Transaction committed";
    current_txn_id_ = INVALID_TXN_ID;
    
    return result;
}

QueryResult Executor::executeRollback() {
    QueryResult result;
    
    if (current_txn_id_ == INVALID_TXN_ID) {
        result.error_message = "No transaction in progress";
        return result;
    }
    
    if (wal_) {
        wal_->abortTransaction(current_txn_id_);
    }
    
    // Release all locks held by this transaction
    if (lock_mgr_) {
        lock_mgr_->releaseAllLocks(current_txn_id_);
    }
    
    result.success = true;
    result.message = "Transaction rolled back";
    current_txn_id_ = INVALID_TXN_ID;
    
    return result;
}

std::vector<Row> Executor::executePlan(const PlanNode* plan, const SelectStatement& stmt) {
    if (!plan) {
        return scanTable(stmt.table_name);
    }
    
    // Find the leaf scan node (walk down through FILTER, PROJECTION, etc.)
    const PlanNode* scan_node = plan;
    while (scan_node && !scan_node->children.empty()) {
        // Check if this is a scan node
        if (scan_node->type == PlanNodeType::SEQ_SCAN || 
            scan_node->type == PlanNodeType::INDEX_SCAN) {
            break;
        }
        scan_node = scan_node->children[0].get();
    }
    
    if (!scan_node) {
        return scanTable(stmt.table_name);
    }
    
    switch (scan_node->type) {
        case PlanNodeType::INDEX_SCAN:
            return executeIndexScan(scan_node);
        case PlanNodeType::SEQ_SCAN:
        default:
            return executeSeqScan(scan_node);
    }
}

std::vector<Row> Executor::executeSeqScan(const PlanNode* plan) {
    if (!plan || plan->table_name.empty()) {
        return {};
    }
    return scanTable(plan->table_name);
}

std::vector<Row> Executor::executeIndexScan(const PlanNode* plan) {
    if (!plan || plan->table_name.empty()) {
        return {};
    }
    
    auto table_opt = catalog_.getTable(plan->table_name);
    if (!table_opt) return {};
    
    const TableSchema& schema = table_opt.value();
    
    // Get the index
    BTree* index = catalog_.getIndex(plan->table_name);
    if (!index) {
        // Fall back to sequential scan
        return scanTable(plan->table_name);
    }
    
    std::vector<Row> rows;
    
    // Use index to find matching records
    if (plan->index_start.has_value()) {
        auto record_opt = index->search(plan->index_start.value());
        if (record_opt) {
            RecordId rid = record_opt.value();
            
            // Fetch the page and record
            Page* page = buffer_pool_.fetchPage(rid.page_id);
            if (page) {
                char buffer[PAGE_SIZE];
                uint16_t length;
                
                if (page->getRecord(rid.slot_id, buffer, length)) {
                    // Deserialize the row
                    Row row;
                    size_t offset = 0;
                    
                    for (size_t i = 0; i < schema.columns.size() && offset < length; i++) {
                        uint8_t type_tag = static_cast<uint8_t>(buffer[offset++]);
                        
                        switch (type_tag) {
                            case 0:  // NULL
                                row.push_back(std::monostate{});
                                break;
                            case 1: {  // INT
                                int64_t v;
                                std::memcpy(&v, buffer + offset, sizeof(int64_t));
                                offset += sizeof(int64_t);
                                row.push_back(v);
                                break;
                            }
                            case 2: {  // FLOAT
                                double v;
                                std::memcpy(&v, buffer + offset, sizeof(double));
                                offset += sizeof(double);
                                row.push_back(v);
                                break;
                            }
                            case 3: {  // STRING
                                uint16_t len;
                                std::memcpy(&len, buffer + offset, sizeof(uint16_t));
                                offset += sizeof(uint16_t);
                                std::string s(buffer + offset, len);
                                offset += len;
                                row.push_back(s);
                                break;
                            }
                            case 4: {  // BOOL
                                uint8_t v = static_cast<uint8_t>(buffer[offset++]);
                                row.push_back(v != 0);
                                break;
                            }
                            default:
                                row.push_back(std::monostate{});
                                break;
                        }
                    }
                    
                    rows.push_back(std::move(row));
                }
                
                buffer_pool_.unpinPage(rid.page_id, false);
            }
        }
    }
    
    return rows;
}

} // namespace minidb
