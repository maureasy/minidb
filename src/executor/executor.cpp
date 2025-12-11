#include "executor/executor.h"

namespace minidb {

Executor::Executor(Catalog& catalog, BufferPool& buffer_pool)
    : catalog_(catalog), buffer_pool_(buffer_pool) {}

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

QueryResult Executor::executeSelect(const SelectStatement& stmt) {
    QueryResult result;
    
    auto table_opt = catalog_.getTable(stmt.table_name);
    if (!table_opt) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    const TableSchema& schema = table_opt.value();
    
    // Determine columns to select
    std::vector<int> column_indices;
    if (stmt.columns.empty()) {
        // SELECT * - all columns
        for (size_t i = 0; i < schema.columns.size(); i++) {
            column_indices.push_back(static_cast<int>(i));
            result.column_names.push_back(schema.columns[i].name);
        }
    } else {
        for (const auto& col_name : stmt.columns) {
            int idx = schema.getColumnIndex(col_name);
            if (idx < 0) {
                result.error_message = "Column not found: " + col_name;
                return result;
            }
            column_indices.push_back(idx);
            result.column_names.push_back(col_name);
        }
    }
    
    // Scan table and filter
    std::vector<Row> all_rows = scanTable(stmt.table_name);
    
    for (const auto& row : all_rows) {
        if (matchesWhere(stmt.where_clause.get(), row, schema)) {
            Row selected_row;
            for (int idx : column_indices) {
                if (idx < static_cast<int>(row.size())) {
                    selected_row.push_back(row[idx]);
                } else {
                    selected_row.push_back(std::monostate{});
                }
            }
            result.rows.push_back(std::move(selected_row));
        }
    }
    
    // Apply ORDER BY
    if (!stmt.order_by.empty()) {
        // Get column index for ordering
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
                        
                        bool less = false;
                        bool equal = (va == vb);
                        
                        if (!equal) {
                            if (std::holds_alternative<int64_t>(va) && std::holds_alternative<int64_t>(vb)) {
                                less = std::get<int64_t>(va) < std::get<int64_t>(vb);
                            } else if (std::holds_alternative<double>(va) && std::holds_alternative<double>(vb)) {
                                less = std::get<double>(va) < std::get<double>(vb);
                            } else if (std::holds_alternative<std::string>(va) && std::holds_alternative<std::string>(vb)) {
                                less = std::get<std::string>(va) < std::get<std::string>(vb);
                            }
                            
                            return asc ? less : !less;
                        }
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
                    std::string data = serializeRow(row, schema);
                    if (page->updateRecord(slot, data.c_str(), static_cast<uint16_t>(data.length()))) {
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

} // namespace minidb
