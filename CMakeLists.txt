cmake_minimum_required(VERSION 3.16)
project(minidb VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Source files
set(SOURCES
    src/main.cpp
    src/storage/page.cpp
    src/storage/file_manager.cpp
    src/storage/buffer_pool.cpp
    src/storage/wal.cpp
    src/concurrency/lock_manager.cpp
    src/concurrency/transaction.cpp
    src/optimizer/query_optimizer.cpp
    src/index/btree.cpp
    src/parser/tokenizer.cpp
    src/parser/parser.cpp
    src/catalog/catalog.cpp
    src/executor/executor.cpp
)

# Header files location
include_directories(${CMAKE_SOURCE_DIR}/include)

# Create executable
add_executable(minidb ${SOURCES})

# Platform-specific settings
if(WIN32)
    target_compile_definitions(minidb PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

# Compiler warnings
if(MSVC)
    target_compile_options(minidb PRIVATE /W4)
else()
    target_compile_options(minidb PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Tests
add_subdirectory(tests)
