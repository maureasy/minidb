# MiniDB

A lightweight, zero-dependency SQL database engine written in C++17.

## Features

- **Storage Engine**: Page-based storage with buffer pool management (LRU eviction)
- **Indexing**: B+ Tree with full delete support (underflow handling via merge/redistribute)
- **SQL Parser**: Hand-written recursive descent parser
- **Query Executor**: Support for DDL and DML operations
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **JOIN Support**: INNER JOIN, LEFT JOIN, RIGHT JOIN
- **Write-Ahead Logging (WAL)**: Crash recovery support
- **Concurrency Control**: Read-write locks and lock manager for multi-threading
- **Query Optimizer**: Cost-based optimization with index scan detection
- **CLI Interface**: Interactive command-line interface

## Supported SQL

### Data Definition Language (DDL)
```sql
CREATE TABLE table_name (
    column1 INT PRIMARY KEY,
    column2 VARCHAR(50),
    column3 FLOAT,
    column4 BOOL
);

DROP TABLE table_name;
```

### Data Manipulation Language (DML)
```sql
-- Insert
INSERT INTO table_name (col1, col2) VALUES (1, 'hello');
INSERT INTO table_name VALUES (1, 'hello', 3.14, true);

-- Select
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name WHERE col1 > 10;
SELECT * FROM table_name ORDER BY col1 DESC LIMIT 10;
SELECT DISTINCT col1 FROM table_name;

-- Aggregate Functions
SELECT COUNT(*) FROM table_name;
SELECT SUM(col1), AVG(col1) FROM table_name;
SELECT MIN(col1), MAX(col1) FROM table_name;
SELECT col1, COUNT(*) FROM table_name GROUP BY col1;

-- JOINs
SELECT t1.col1, t2.col2 FROM table1 t1 
    JOIN table2 t2 ON t1.id = t2.id;
SELECT * FROM table1 LEFT JOIN table2 ON table1.id = table2.id;

-- Update
UPDATE table_name SET col1 = 10 WHERE col2 = 'test';

-- Delete
DELETE FROM table_name WHERE col1 < 5;
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

## Advanced Features

This database now includes several advanced features:

- **Concurrency Control**: Lock manager with shared/exclusive locks, deadlock detection
- **Write-Ahead Logging**: WAL for crash recovery and durability with transaction manager
- **Query Optimizer**: Cost-based optimization with index scan detection
- **JOINs**: INNER JOIN, LEFT JOIN, RIGHT JOIN support
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
- **B+ Tree**: Full delete support with node merge/redistribute
- **Transaction Isolation**: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **CREATE INDEX**: Create custom indexes on any column
- **Subqueries**: EXISTS subquery support

## Supported Index Operations

```sql
-- Create an index
CREATE INDEX idx_name ON table_name (column1, column2);
CREATE UNIQUE INDEX idx_unique ON table_name (column);

-- Drop an index
DROP INDEX idx_name;
```

## Transaction Support

```sql
-- Begin a transaction with isolation level
BEGIN TRANSACTION;
BEGIN TRANSACTION READ COMMITTED;
BEGIN TRANSACTION SERIALIZABLE;

-- Commit or rollback
COMMIT;
ROLLBACK;
```

## License

MIT License - Feel free to use for learning!
