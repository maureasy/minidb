# Test executable
add_executable(test_fixes test_fixes.cpp)
target_include_directories(test_fixes PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Link with the minidb sources (excluding main.cpp)
target_sources(test_fixes PRIVATE
    ${CMAKE_SOURCE_DIR}/src/storage/page.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/file_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/buffer_pool.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/wal.cpp
    ${CMAKE_SOURCE_DIR}/src/concurrency/lock_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/concurrency/transaction.cpp
    ${CMAKE_SOURCE_DIR}/src/optimizer/query_optimizer.cpp
    ${CMAKE_SOURCE_DIR}/src/index/btree.cpp
    ${CMAKE_SOURCE_DIR}/src/parser/tokenizer.cpp
    ${CMAKE_SOURCE_DIR}/src/parser/parser.cpp
    ${CMAKE_SOURCE_DIR}/src/catalog/catalog.cpp
    ${CMAKE_SOURCE_DIR}/src/executor/executor.cpp
)

if(WIN32)
    target_compile_definitions(test_fixes PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

if(MSVC)
    target_compile_options(test_fixes PRIVATE /W4)
else()
    target_compile_options(test_fixes PRIVATE -Wall -Wextra -Wpedantic)
endif()
