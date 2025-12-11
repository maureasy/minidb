# MiniDB - A Simple Database Engine

A zero-dependency SQL database engine written in C++17. This is a learning project that implements core database concepts from scratch.

## Features

- **Page-based Storage Engine** - 4KB pages with slot-based record storage
- **Buffer Pool** - LRU eviction policy for in-memory page caching
- **B+ Tree Index** - For efficient key lookups on primary keys
- **SQL Parser** - Hand-written recursive descent parser
- **Query Executor** - Supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE

## Supported SQL

```sql
-- Create a table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    active BOOL
);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 30, TRUE);
INSERT INTO users (id, name) VALUES (2, 'Bob');

-- Query data
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25;
SELECT * FROM users ORDER BY name DESC LIMIT 10;

-- Update data
UPDATE users SET age = 31 WHERE id = 1;

-- Delete data
DELETE FROM users WHERE active = FALSE;

-- Drop table
DROP TABLE users;
```

## Building

### Requirements
- C++17 compatible compiler (GCC 8+, Clang 7+, MSVC 2019+)
- CMake 3.16+

### Build Commands

```bash
mkdir build
cd build
cmake ..
cmake --build .
```

### Windows (Visual Studio)
```powershell
mkdir build
cd build
cmake ..
cmake --build . --config Release
```

## Usage

```bash
# Start with default database (minidb.db)
./minidb

# Start with custom database name
./minidb mydata
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `.help` | Show help message |
| `.tables` | List all tables |
| `.schema <table>` | Show table schema |
| `.quit` | Exit the database |

## Architecture

```
minidb/
├── include/
│   ├── common.h           # Common types and utilities
│   ├── storage/
│   │   ├── page.h         # Page format and operations
│   │   ├── file_manager.h # Disk I/O
│   │   └── buffer_pool.h  # Page caching with LRU
│   ├── index/
│   │   └── btree.h        # B+ tree index
│   ├── parser/
│   │   ├── tokenizer.h    # SQL lexer
│   │   └── parser.h       # Recursive descent parser
│   ├── catalog/
│   │   └── catalog.h      # Table metadata management
│   └── executor/
│       └── executor.h     # Query execution
└── src/
    └── [implementations]
```

## Data Types

| Type | Description |
|------|-------------|
| `INT` | 64-bit signed integer |
| `FLOAT` | 64-bit double precision |
| `VARCHAR(n)` | Variable length string (max n chars) |
| `BOOL` | Boolean (TRUE/FALSE) |

## Limitations

This is an educational project with the following limitations:

- No multi-threading / concurrent access
- No crash recovery / write-ahead logging
- No query optimization (full table scans)
- No JOINs (single table queries only)
- No aggregate functions (COUNT, SUM, etc.)
- B+ tree delete doesn't handle underflow

## License

MIT License - Feel free to use for learning!
