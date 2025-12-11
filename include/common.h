#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <variant>
#include <unordered_map>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cstring>

namespace minidb {

// Page configuration
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t MAX_PAGES = 1024;

// Type definitions
using PageId = uint32_t;
using TableId = uint32_t;
using ColumnId = uint16_t;
using SlotId = uint16_t;

// Invalid markers
constexpr PageId INVALID_PAGE_ID = UINT32_MAX;
constexpr TableId INVALID_TABLE_ID = UINT32_MAX;

// Column types supported by the database
enum class ColumnType : uint8_t {
    INT,
    FLOAT,
    VARCHAR,
    BOOL
};

// Value variant type for storing different column values
using Value = std::variant<std::monostate, int64_t, double, std::string, bool>;

// Row is a vector of values
using Row = std::vector<Value>;

// Helper to get column type name
inline std::string columnTypeName(ColumnType type) {
    switch (type) {
        case ColumnType::INT: return "INT";
        case ColumnType::FLOAT: return "FLOAT";
        case ColumnType::VARCHAR: return "VARCHAR";
        case ColumnType::BOOL: return "BOOL";
        default: return "UNKNOWN";
    }
}

// Helper to convert value to string for display
inline std::string valueToString(const Value& val) {
    if (std::holds_alternative<std::monostate>(val)) {
        return "NULL";
    } else if (std::holds_alternative<int64_t>(val)) {
        return std::to_string(std::get<int64_t>(val));
    } else if (std::holds_alternative<double>(val)) {
        return std::to_string(std::get<double>(val));
    } else if (std::holds_alternative<std::string>(val)) {
        return std::get<std::string>(val);
    } else if (std::holds_alternative<bool>(val)) {
        return std::get<bool>(val) ? "TRUE" : "FALSE";
    }
    return "UNKNOWN";
}

// Database exception
class DatabaseException : public std::runtime_error {
public:
    explicit DatabaseException(const std::string& msg) : std::runtime_error(msg) {}
};

} // namespace minidb
