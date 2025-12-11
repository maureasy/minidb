#include "catalog/catalog.h"
#include <fstream>

namespace minidb {

Catalog::Catalog() {}

Catalog::~Catalog() {}

bool Catalog::createTable(const std::string& name, const std::vector<ColumnInfo>& columns) {
    if (tableExists(name)) {
        return false;
    }
    
    TableSchema schema;
    schema.id = next_table_id_++;
    schema.name = name;
    schema.columns = columns;
    schema.first_page = INVALID_PAGE_ID;
    schema.row_count = 0;
    
    // Assign column IDs and find primary key
    for (size_t i = 0; i < schema.columns.size(); i++) {
        schema.columns[i].id = static_cast<ColumnId>(i);
        if (schema.columns[i].is_primary_key) {
            schema.primary_key_column = schema.columns[i].id;
        }
    }
    
    tables_[name] = schema;
    
    // Create index for the table (on primary key if exists)
    createIndex(name);
    
    return true;
}

bool Catalog::dropTable(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return false;
    }
    
    tables_.erase(it);
    indexes_.erase(name);
    return true;
}

bool Catalog::tableExists(const std::string& name) const {
    return tables_.find(name) != tables_.end();
}

std::optional<TableSchema> Catalog::getTable(const std::string& name) const {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<std::string> Catalog::getTableNames() const {
    std::vector<std::string> names;
    for (const auto& [name, schema] : tables_) {
        names.push_back(name);
    }
    return names;
}

void Catalog::updateRowCount(const std::string& name, int64_t delta) {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        it->second.row_count = static_cast<uint64_t>(
            static_cast<int64_t>(it->second.row_count) + delta);
    }
}

void Catalog::setFirstPage(const std::string& name, PageId page_id) {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        it->second.first_page = page_id;
    }
}

BTree* Catalog::getIndex(const std::string& table_name) {
    auto it = indexes_.find(table_name);
    if (it == indexes_.end()) {
        return nullptr;
    }
    return it->second.get();
}

void Catalog::createIndex(const std::string& table_name) {
    if (indexes_.find(table_name) == indexes_.end()) {
        indexes_[table_name] = std::make_unique<BTree>();
    }
}

void Catalog::save(const std::string& path) {
    std::ofstream file(path, std::ios::binary);
    if (!file) return;
    
    // Write number of tables
    uint32_t num_tables = static_cast<uint32_t>(tables_.size());
    file.write(reinterpret_cast<const char*>(&num_tables), sizeof(uint32_t));
    
    // Write each table schema
    for (const auto& [name, schema] : tables_) {
        // Table name
        uint32_t name_len = static_cast<uint32_t>(name.length());
        file.write(reinterpret_cast<const char*>(&name_len), sizeof(uint32_t));
        file.write(name.data(), name_len);
        
        // Table ID
        file.write(reinterpret_cast<const char*>(&schema.id), sizeof(TableId));
        
        // First page and row count
        file.write(reinterpret_cast<const char*>(&schema.first_page), sizeof(PageId));
        file.write(reinterpret_cast<const char*>(&schema.row_count), sizeof(uint64_t));
        
        // Number of columns
        uint32_t num_cols = static_cast<uint32_t>(schema.columns.size());
        file.write(reinterpret_cast<const char*>(&num_cols), sizeof(uint32_t));
        
        // Write each column
        for (const auto& col : schema.columns) {
            // Column name
            uint32_t col_name_len = static_cast<uint32_t>(col.name.length());
            file.write(reinterpret_cast<const char*>(&col_name_len), sizeof(uint32_t));
            file.write(col.name.data(), col_name_len);
            
            // Column type and properties
            file.write(reinterpret_cast<const char*>(&col.type), sizeof(ColumnType));
            file.write(reinterpret_cast<const char*>(&col.size), sizeof(uint16_t));
            file.write(reinterpret_cast<const char*>(&col.is_primary_key), sizeof(bool));
            file.write(reinterpret_cast<const char*>(&col.is_nullable), sizeof(bool));
            file.write(reinterpret_cast<const char*>(&col.id), sizeof(ColumnId));
        }
        
        // Primary key column
        bool has_pk = schema.primary_key_column.has_value();
        file.write(reinterpret_cast<const char*>(&has_pk), sizeof(bool));
        if (has_pk) {
            ColumnId pk_col = schema.primary_key_column.value();
            file.write(reinterpret_cast<const char*>(&pk_col), sizeof(ColumnId));
        }
    }
    
    // Write next_table_id
    file.write(reinterpret_cast<const char*>(&next_table_id_), sizeof(TableId));
}

void Catalog::load(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return;
    
    tables_.clear();
    indexes_.clear();
    
    // Read number of tables
    uint32_t num_tables;
    file.read(reinterpret_cast<char*>(&num_tables), sizeof(uint32_t));
    
    // Read each table schema
    for (uint32_t t = 0; t < num_tables; t++) {
        TableSchema schema;
        
        // Table name
        uint32_t name_len;
        file.read(reinterpret_cast<char*>(&name_len), sizeof(uint32_t));
        std::string name(name_len, '\0');
        file.read(name.data(), name_len);
        schema.name = name;
        
        // Table ID
        file.read(reinterpret_cast<char*>(&schema.id), sizeof(TableId));
        
        // First page and row count
        file.read(reinterpret_cast<char*>(&schema.first_page), sizeof(PageId));
        file.read(reinterpret_cast<char*>(&schema.row_count), sizeof(uint64_t));
        
        // Number of columns
        uint32_t num_cols;
        file.read(reinterpret_cast<char*>(&num_cols), sizeof(uint32_t));
        
        // Read each column
        for (uint32_t c = 0; c < num_cols; c++) {
            ColumnInfo col;
            
            // Column name
            uint32_t col_name_len;
            file.read(reinterpret_cast<char*>(&col_name_len), sizeof(uint32_t));
            col.name.resize(col_name_len);
            file.read(col.name.data(), col_name_len);
            
            // Column type and properties
            file.read(reinterpret_cast<char*>(&col.type), sizeof(ColumnType));
            file.read(reinterpret_cast<char*>(&col.size), sizeof(uint16_t));
            file.read(reinterpret_cast<char*>(&col.is_primary_key), sizeof(bool));
            file.read(reinterpret_cast<char*>(&col.is_nullable), sizeof(bool));
            file.read(reinterpret_cast<char*>(&col.id), sizeof(ColumnId));
            
            schema.columns.push_back(col);
        }
        
        // Primary key column
        bool has_pk;
        file.read(reinterpret_cast<char*>(&has_pk), sizeof(bool));
        if (has_pk) {
            ColumnId pk_col;
            file.read(reinterpret_cast<char*>(&pk_col), sizeof(ColumnId));
            schema.primary_key_column = pk_col;
        }
        
        tables_[name] = schema;
        createIndex(name);
    }
    
    // Read next_table_id
    file.read(reinterpret_cast<char*>(&next_table_id_), sizeof(TableId));
}

} // namespace minidb
