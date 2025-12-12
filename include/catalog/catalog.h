#pragma once

#include "common.h"
#include "index/btree.h"

namespace minidb {

// Column information in a table
struct ColumnInfo {
    std::string name;
    ColumnType type;
    uint16_t size;  // For VARCHAR, max size
    bool is_primary_key;
    bool is_nullable;
    ColumnId id;
};

// Index information
struct IndexInfo {
    std::string name;
    std::string table_name;
    std::vector<std::string> columns;
    bool is_unique;
    bool is_primary;
};

// Table schema
struct TableSchema {
    TableId id;
    std::string name;
    std::vector<ColumnInfo> columns;
    std::optional<ColumnId> primary_key_column;
    PageId first_page;
    uint64_t row_count;
    
    // Find column by name
    std::optional<ColumnInfo> findColumn(const std::string& name) const {
        for (const auto& col : columns) {
            if (col.name == name) return col;
        }
        return std::nullopt;
    }
    
    // Get column index
    int getColumnIndex(const std::string& name) const {
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i].name == name) return static_cast<int>(i);
        }
        return -1;
    }
};

// Catalog manages table metadata
class Catalog {
public:
    Catalog();
    ~Catalog();
    
    // Table operations
    bool createTable(const std::string& name, const std::vector<ColumnInfo>& columns);
    bool dropTable(const std::string& name);
    bool tableExists(const std::string& name) const;
    
    // Get table info
    std::optional<TableSchema> getTable(const std::string& name) const;
    std::vector<std::string> getTableNames() const;
    
    // Update table metadata
    void updateRowCount(const std::string& name, int64_t delta);
    void setFirstPage(const std::string& name, PageId page_id);
    
    // Index management
    BTree* getIndex(const std::string& table_name);
    BTree* getIndexByName(const std::string& index_name);
    void createIndex(const std::string& table_name);
    bool createNamedIndex(const std::string& index_name, const std::string& table_name,
                          const std::vector<std::string>& columns, bool is_unique);
    bool dropIndex(const std::string& index_name);
    bool indexExists(const std::string& index_name) const;
    std::vector<IndexInfo> getIndexesForTable(const std::string& table_name) const;
    std::vector<std::string> getIndexNames() const;
    
    // Rebuild index from existing table data
    void rebuildIndex(const std::string& table_name, class BufferPool& buffer_pool);
    
    // Persistence
    void save(const std::string& path);
    void load(const std::string& path);

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TableSchema> tables_;
    std::unordered_map<std::string, std::unique_ptr<BTree>> indexes_;
    std::unordered_map<std::string, IndexInfo> index_info_;
    TableId next_table_id_ = 1;
};

} // namespace minidb
