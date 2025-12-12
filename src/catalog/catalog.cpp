#include "catalog/catalog.h"
#include "storage/buffer_pool.h"
#include <fstream>

namespace minidb {

Catalog::Catalog() {}

Catalog::~Catalog() {}

bool Catalog::createTable(const std::string& name, const std::vector<ColumnInfo>& columns) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (tables_.find(name) != tables_.end()) {
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
    
    // Create index for the table (on primary key if exists) - internal, no lock needed
    if (indexes_.find(name) == indexes_.end()) {
        indexes_[name] = std::make_unique<BTree>();
    }
    
    return true;
}

bool Catalog::dropTable(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return false;
    }
    
    tables_.erase(it);
    indexes_.erase(name);
    return true;
}

bool Catalog::tableExists(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tables_.find(name) != tables_.end();
}

std::optional<TableSchema> Catalog::getTable(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<std::string> Catalog::getTableNames() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> names;
    for (const auto& [name, schema] : tables_) {
        names.push_back(name);
    }
    return names;
}

void Catalog::updateRowCount(const std::string& name, int64_t delta) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        int64_t new_count = static_cast<int64_t>(it->second.row_count) + delta;
        it->second.row_count = (new_count < 0) ? 0 : static_cast<uint64_t>(new_count);
    }
}

void Catalog::setFirstPage(const std::string& name, PageId page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        it->second.first_page = page_id;
    }
}

BTree* Catalog::getIndex(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = indexes_.find(table_name);
    if (it == indexes_.end()) {
        return nullptr;
    }
    return it->second.get();
}

BTree* Catalog::getIndexByName(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return nullptr;
    }
    return it->second.get();
}

void Catalog::createIndex(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (indexes_.find(table_name) == indexes_.end()) {
        indexes_[table_name] = std::make_unique<BTree>();
    }
}

bool Catalog::createNamedIndex(const std::string& index_name, const std::string& table_name,
                                const std::vector<std::string>& columns, bool is_unique) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if index already exists
    if (indexes_.find(index_name) != indexes_.end() ||
        index_info_.find(index_name) != index_info_.end()) {
        return false;
    }
    
    // Check if table exists
    auto table_it = tables_.find(table_name);
    if (table_it == tables_.end()) {
        return false;
    }
    
    // Verify all columns exist
    const TableSchema& table_schema = table_it->second;
    
    for (const auto& col_name : columns) {
        if (table_schema.getColumnIndex(col_name) < 0) {
            return false;
        }
    }
    
    // Create the index
    indexes_[index_name] = std::make_unique<BTree>();
    
    // Store index info
    IndexInfo info;
    info.name = index_name;
    info.table_name = table_name;
    info.columns = columns;
    info.is_unique = is_unique;
    info.is_primary = false;
    index_info_[index_name] = info;
    
    return true;
}

bool Catalog::dropIndex(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return false;
    }
    
    indexes_.erase(it);
    index_info_.erase(index_name);
    return true;
}

bool Catalog::indexExists(const std::string& index_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return indexes_.find(index_name) != indexes_.end() ||
           index_info_.find(index_name) != index_info_.end();
}

std::vector<IndexInfo> Catalog::getIndexesForTable(const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<IndexInfo> result;
    for (const auto& [name, info] : index_info_) {
        if (info.table_name == table_name) {
            result.push_back(info);
        }
    }
    return result;
}

std::vector<std::string> Catalog::getIndexNames() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> names;
    for (const auto& [name, info] : index_info_) {
        names.push_back(name);
    }
    return names;
}

void Catalog::save(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
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
    std::lock_guard<std::mutex> lock(mutex_);
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
        // Create index internally - no lock needed since we already hold it
        if (indexes_.find(name) == indexes_.end()) {
            indexes_[name] = std::make_unique<BTree>();
        }
    }
    
    // Read next_table_id
    file.read(reinterpret_cast<char*>(&next_table_id_), sizeof(TableId));
}

void Catalog::rebuildIndex(const std::string& table_name, BufferPool& buffer_pool) {
    auto table_opt = getTable(table_name);
    if (!table_opt) return;
    
    const TableSchema& schema = table_opt.value();
    
    // Only rebuild if table has a primary key
    if (!schema.primary_key_column.has_value()) return;
    
    BTree* index = getIndex(table_name);
    if (!index) return;
    
    // Clear existing index
    index->clear();
    
    // Scan table and rebuild index
    if (schema.first_page == INVALID_PAGE_ID) return;
    
    PageId current_page_id = schema.first_page;
    ColumnId pk_col = schema.primary_key_column.value();
    
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool.fetchPage(current_page_id);
        if (!page) break;
        
        std::vector<char> buffer(PAGE_SIZE);
        for (SlotId slot = 0; slot < page->getNumSlots(); slot++) {
            uint16_t length;
            
            if (page->getRecord(slot, buffer.data(), length)) {
                // Parse the record to get primary key value
                size_t offset = 0;
                for (ColumnId col = 0; col <= pk_col && offset < length; col++) {
                    if (offset >= length) break;
                    uint8_t type_tag = static_cast<uint8_t>(buffer[offset++]);
                    
                    if (col == pk_col) {
                        if (type_tag == 1 && offset + sizeof(int64_t) <= length) {  // INT
                            int64_t key;
                            std::memcpy(&key, buffer.data() + offset, sizeof(int64_t));
                            index->insert(key, {current_page_id, slot});
                        }
                        break;
                    }
                    
                    // Skip to next column based on type (with bounds checking)
                    switch (type_tag) {
                        case 0: break;  // NULL
                        case 1: offset += sizeof(int64_t); break;  // INT
                        case 2: offset += sizeof(double); break;   // FLOAT
                        case 3: {  // STRING
                            if (offset + sizeof(uint16_t) > length) {
                                offset = length;  // Force exit
                                break;
                            }
                            uint16_t len;
                            std::memcpy(&len, buffer.data() + offset, sizeof(uint16_t));
                            offset += sizeof(uint16_t);
                            if (offset + len > length) {
                                offset = length;  // Force exit
                                break;
                            }
                            offset += len;
                            break;
                        }
                        case 4: offset += 1; break;  // BOOL
                    }
                }
            }
        }
        
        PageId next_page = page->getNextPage();
        buffer_pool.unpinPage(current_page_id, false);
        current_page_id = next_page;
    }
}

} // namespace minidb
