#include "common.h"
#include "storage/file_manager.h"
#include "storage/buffer_pool.h"
#include "catalog/catalog.h"
#include "parser/parser.h"
#include "executor/executor.h"

using namespace minidb;

void printHelp() {
    std::cout << "\n=== MiniDB - A Simple Database Engine ===\n";
    std::cout << "Commands:\n";
    std::cout << "  .help        - Show this help message\n";
    std::cout << "  .tables      - List all tables\n";
    std::cout << "  .schema <t>  - Show schema for table <t>\n";
    std::cout << "  .quit        - Exit the database\n";
    std::cout << "\nSQL Commands:\n";
    std::cout << "  CREATE TABLE name (col1 TYPE, col2 TYPE, ...)\n";
    std::cout << "  DROP TABLE name\n";
    std::cout << "  INSERT INTO name VALUES (val1, val2, ...)\n";
    std::cout << "  SELECT * FROM name [WHERE condition] [ORDER BY col] [LIMIT n]\n";
    std::cout << "  UPDATE name SET col = val [WHERE condition]\n";
    std::cout << "  DELETE FROM name [WHERE condition]\n";
    std::cout << "\nTypes: INT, FLOAT, VARCHAR(n), BOOL\n";
    std::cout << std::endl;
}

void printRow(const std::vector<std::string>& column_names, const Row& row, 
              const std::vector<size_t>& widths) {
    std::cout << "| ";
    for (size_t i = 0; i < row.size(); i++) {
        std::string val = valueToString(row[i]);
        std::cout << val;
        if (val.length() < widths[i]) {
            std::cout << std::string(widths[i] - val.length(), ' ');
        }
        std::cout << " | ";
    }
    std::cout << "\n";
}

void printResult(const QueryResult& result) {
    if (!result.success) {
        std::cout << "Error: " << result.error_message << "\n";
        return;
    }
    
    if (!result.message.empty()) {
        std::cout << result.message << "\n";
        return;
    }
    
    if (result.rows.empty()) {
        std::cout << "(0 rows)\n";
        return;
    }
    
    // Calculate column widths
    std::vector<size_t> widths;
    for (const auto& name : result.column_names) {
        widths.push_back(name.length());
    }
    
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); i++) {
            size_t len = valueToString(row[i]).length();
            if (len > widths[i]) widths[i] = len;
        }
    }
    
    // Print header separator
    std::cout << "+";
    for (size_t w : widths) {
        std::cout << std::string(w + 2, '-') << "+";
    }
    std::cout << "\n";
    
    // Print header
    std::cout << "| ";
    for (size_t i = 0; i < result.column_names.size(); i++) {
        std::cout << result.column_names[i];
        if (result.column_names[i].length() < widths[i]) {
            std::cout << std::string(widths[i] - result.column_names[i].length(), ' ');
        }
        std::cout << " | ";
    }
    std::cout << "\n";
    
    // Print header separator
    std::cout << "+";
    for (size_t w : widths) {
        std::cout << std::string(w + 2, '-') << "+";
    }
    std::cout << "\n";
    
    // Print rows
    for (const auto& row : result.rows) {
        printRow(result.column_names, row, widths);
    }
    
    // Print footer separator
    std::cout << "+";
    for (size_t w : widths) {
        std::cout << std::string(w + 2, '-') << "+";
    }
    std::cout << "\n";
    
    std::cout << "(" << result.rows.size() << " row" << (result.rows.size() != 1 ? "s" : "") << ")\n";
}

int main(int argc, char* argv[]) {
    std::string db_path = "minidb.db";
    std::string catalog_path = "minidb.catalog";
    
    if (argc > 1) {
        db_path = std::string(argv[1]) + ".db";
        catalog_path = std::string(argv[1]) + ".catalog";
    }
    
    std::cout << "MiniDB v1.0 - A Simple Database Engine\n";
    std::cout << "Type .help for usage information\n";
    std::cout << "Database file: " << db_path << "\n\n";
    
    try {
        FileManager file_manager(db_path);
        BufferPool buffer_pool(file_manager, 64);
        Catalog catalog;
        
        // Load catalog if exists
        std::ifstream cat_file(catalog_path);
        if (cat_file.good()) {
            cat_file.close();
            catalog.load(catalog_path);
            std::cout << "Loaded existing database\n";
        }
        
        Executor executor(catalog, buffer_pool);
        
        std::string line;
        std::string input;
        
        while (true) {
            std::cout << "minidb> ";
            std::cout.flush();
            
            if (!std::getline(std::cin, line)) {
                break;
            }
            
            // Trim whitespace
            size_t start = line.find_first_not_of(" \t\r\n");
            if (start == std::string::npos) continue;
            size_t end = line.find_last_not_of(" \t\r\n");
            line = line.substr(start, end - start + 1);
            
            if (line.empty()) continue;
            
            // Handle dot commands
            if (line[0] == '.') {
                if (line == ".quit" || line == ".exit") {
                    break;
                } else if (line == ".help") {
                    printHelp();
                } else if (line == ".tables") {
                    auto tables = catalog.getTableNames();
                    if (tables.empty()) {
                        std::cout << "(no tables)\n";
                    } else {
                        for (const auto& name : tables) {
                            std::cout << name << "\n";
                        }
                    }
                } else if (line.substr(0, 7) == ".schema") {
                    std::string table_name = line.length() > 8 ? line.substr(8) : "";
                    // Trim
                    start = table_name.find_first_not_of(" \t");
                    if (start != std::string::npos) {
                        end = table_name.find_last_not_of(" \t");
                        table_name = table_name.substr(start, end - start + 1);
                    }
                    
                    if (table_name.empty()) {
                        std::cout << "Usage: .schema <table_name>\n";
                    } else {
                        auto table_opt = catalog.getTable(table_name);
                        if (!table_opt) {
                            std::cout << "Table not found: " << table_name << "\n";
                        } else {
                            const auto& schema = table_opt.value();
                            std::cout << "Table: " << schema.name << "\n";
                            std::cout << "Columns:\n";
                            for (const auto& col : schema.columns) {
                                std::cout << "  " << col.name << " " << columnTypeName(col.type);
                                if (col.type == ColumnType::VARCHAR) {
                                    std::cout << "(" << col.size << ")";
                                }
                                if (col.is_primary_key) {
                                    std::cout << " PRIMARY KEY";
                                }
                                std::cout << "\n";
                            }
                            std::cout << "Rows: " << schema.row_count << "\n";
                        }
                    }
                } else {
                    std::cout << "Unknown command: " << line << "\n";
                    std::cout << "Type .help for usage\n";
                }
                continue;
            }
            
            // Accumulate SQL (handle multi-line)
            input += line;
            
            // Check if statement is complete (ends with semicolon)
            if (input.back() != ';') {
                input += " ";
                continue;
            }
            
            // Parse and execute
            Parser parser(input);
            auto stmt = parser.parse();
            
            if (parser.hasError()) {
                std::cout << parser.getError() << "\n";
            } else if (stmt) {
                QueryResult result = executor.execute(*stmt);
                printResult(result);
                
                // Save catalog after modifications
                if (stmt->type == StatementType::CREATE_TABLE ||
                    stmt->type == StatementType::DROP_TABLE ||
                    stmt->type == StatementType::INSERT ||
                    stmt->type == StatementType::UPDATE ||
                    stmt->type == StatementType::DELETE) {
                    catalog.save(catalog_path);
                    buffer_pool.flushAllPages();
                }
            }
            
            input.clear();
        }
        
        // Save before exit
        catalog.save(catalog_path);
        buffer_pool.flushAllPages();
        
        std::cout << "\nGoodbye!\n";
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}
