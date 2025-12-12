// Test file for verifying the code review fixes
#include <iostream>
#include <cassert>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>

#include "common.h"
#include "storage/page.h"
#include "storage/file_manager.h"
#include "storage/buffer_pool.h"
#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"

using namespace minidb;

// Test counter
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "Running " #name "... "; \
    try { \
        test_##name(); \
        std::cout << "PASSED\n"; \
        tests_passed++; \
    } catch (const std::exception& e) { \
        std::cout << "FAILED: " << e.what() << "\n"; \
        tests_failed++; \
    } \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        throw std::runtime_error("Assertion failed: " #cond); \
    } \
} while(0)

// =============================================================================
// TEST 1: Integer underflow fix in updateRowCount
// =============================================================================
TEST(integer_underflow_fix) {
    Catalog catalog;
    
    std::vector<ColumnInfo> columns = {
        {"id", ColumnType::INT, 0, true, false, 0}
    };
    catalog.createTable("underflow_test", columns);
    
    // Set row count to 5
    catalog.updateRowCount("underflow_test", 5);
    auto table = catalog.getTable("underflow_test");
    ASSERT(table.has_value());
    ASSERT(table->row_count == 5);
    
    // Try to subtract 10 (would cause underflow without fix)
    catalog.updateRowCount("underflow_test", -10);
    table = catalog.getTable("underflow_test");
    ASSERT(table.has_value());
    
    // Should be 0, not a huge number from underflow
    ASSERT(table->row_count == 0);
    
    catalog.dropTable("underflow_test");
}

// =============================================================================
// TEST 2: Page serialization/deserialization fix
// =============================================================================
TEST(page_serialization_fix) {
    Page page(42);
    
    // Insert some test records
    const char* record1 = "Hello, World!";
    const char* record2 = "Test record 2";
    const char* record3 = "Another test";
    
    SlotId slot1, slot2, slot3;
    ASSERT(page.insertRecord(record1, strlen(record1), slot1));
    ASSERT(page.insertRecord(record2, strlen(record2), slot2));
    ASSERT(page.insertRecord(record3, strlen(record3), slot3));
    
    // Serialize the page
    char buffer[PAGE_SIZE];
    page.serialize(buffer);
    
    // Deserialize into a new page
    Page restored_page;
    restored_page.deserialize(buffer);
    
    // Verify page ID preserved
    ASSERT(restored_page.getPageId() == 42);
    
    // Verify all records can be read back correctly
    char read_buffer[PAGE_SIZE];
    uint16_t length;
    
    ASSERT(restored_page.getRecord(slot1, read_buffer, length));
    ASSERT(length == strlen(record1));
    ASSERT(memcmp(read_buffer, record1, length) == 0);
    
    ASSERT(restored_page.getRecord(slot2, read_buffer, length));
    ASSERT(length == strlen(record2));
    ASSERT(memcmp(read_buffer, record2, length) == 0);
    
    ASSERT(restored_page.getRecord(slot3, read_buffer, length));
    ASSERT(length == strlen(record3));
    ASSERT(memcmp(read_buffer, record3, length) == 0);
}

// =============================================================================
// TEST 3: File offset calculation (page persistence)
// =============================================================================
TEST(file_offset_fix) {
    const std::string test_db = "test_offset_fix.db";
    
    // Clean up any existing test file
    std::remove(test_db.c_str());
    
    {
        FileManager fm(test_db);
        
        // Allocate several pages
        PageId p1 = fm.allocatePage();
        PageId p2 = fm.allocatePage();
        PageId p3 = fm.allocatePage();
        
        // Write data to each page
        Page page1(p1);
        const char* data1 = "Page 1 data";
        SlotId slot;
        page1.insertRecord(data1, strlen(data1), slot);
        fm.writePage(p1, page1);
        
        Page page2(p2);
        const char* data2 = "Page 2 data";
        page2.insertRecord(data2, strlen(data2), slot);
        fm.writePage(p2, page2);
        
        Page page3(p3);
        const char* data3 = "Page 3 data";
        page3.insertRecord(data3, strlen(data3), slot);
        fm.writePage(p3, page3);
        
        // Deallocate middle page (changes free list)
        fm.deallocatePage(p2);
        
        // Allocate again (should reuse p2)
        PageId p4 = fm.allocatePage();
        
        // Write new data
        Page page4(p4);
        const char* data4 = "Page 4 data";
        page4.insertRecord(data4, strlen(data4), slot);
        fm.writePage(p4, page4);
        
        fm.flush();
    }
    
    // Reopen and verify pages 1 and 3 are still intact
    {
        FileManager fm(test_db);
        
        Page page1, page3;
        ASSERT(fm.readPage(0, page1));
        ASSERT(fm.readPage(2, page3));
        
        char buffer[PAGE_SIZE];
        uint16_t length;
        
        ASSERT(page1.getRecord(0, buffer, length));
        ASSERT(memcmp(buffer, "Page 1 data", length) == 0);
        
        ASSERT(page3.getRecord(0, buffer, length));
        ASSERT(memcmp(buffer, "Page 3 data", length) == 0);
    }
    
    // Clean up
    std::remove(test_db.c_str());
}

// =============================================================================
// TEST 4: Thread-safety in Catalog
// =============================================================================
TEST(catalog_thread_safety) {
    Catalog catalog;
    std::atomic<int> success_count{0};
    std::atomic<int> error_count{0};
    
    // Create initial table
    std::vector<ColumnInfo> columns = {
        {"id", ColumnType::INT, 0, true, false, 0}
    };
    catalog.createTable("concurrent_test", columns);
    
    // Spawn multiple threads doing concurrent operations
    std::vector<std::thread> threads;
    const int num_threads = 10;
    const int ops_per_thread = 100;
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&catalog, &success_count, &error_count, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; i++) {
                try {
                    // Mix of read and write operations
                    if (i % 3 == 0) {
                        catalog.updateRowCount("concurrent_test", 1);
                    } else if (i % 3 == 1) {
                        auto table = catalog.getTable("concurrent_test");
                        if (table.has_value()) {
                            success_count++;
                        }
                    } else {
                        catalog.tableExists("concurrent_test");
                        success_count++;
                    }
                } catch (...) {
                    error_count++;
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    // Should have no errors (no crashes, no data races)
    ASSERT(error_count == 0);
    
    // Row count should be exactly num_threads * number of times (i % 3 == 0) for i in [0, ops_per_thread)
    auto table = catalog.getTable("concurrent_test");
    ASSERT(table.has_value());
    // For i in [0, 99], i % 3 == 0 is true for: 0, 3, 6, ... 99 = 34 times
    int increments_per_thread = (ops_per_thread + 2) / 3;  // Ceiling division
    int expected_increments = num_threads * increments_per_thread;
    ASSERT(table->row_count == static_cast<uint64_t>(expected_increments));
    
    catalog.dropTable("concurrent_test");
}

// =============================================================================
// TEST 5: TableLock with shared_ptr (no reference invalidation)
// =============================================================================
TEST(tablelock_shared_ptr) {
    DatabaseLockManager db_lock_mgr;
    
    // Get locks for multiple tables
    auto lock1 = db_lock_mgr.getTableLock("table1");
    auto lock2 = db_lock_mgr.getTableLock("table2");
    
    // Add more tables (may cause map rehash)
    for (int i = 0; i < 100; i++) {
        db_lock_mgr.getTableLock("table_" + std::to_string(i));
    }
    
    // Original locks should still be valid (shared_ptr keeps them alive)
    ASSERT(lock1 != nullptr);
    ASSERT(lock2 != nullptr);
    
    // Should be able to use the locks
    lock1->readLock();
    lock1->readUnlock();
    
    lock2->writeLock();
    lock2->writeUnlock();
}

// =============================================================================
// TEST 6: PageGuard RAII
// =============================================================================
TEST(page_guard_raii) {
    const std::string test_db = "test_page_guard.db";
    std::remove(test_db.c_str());
    
    FileManager fm(test_db);
    BufferPool pool(fm, 16);
    
    PageId page_id;
    
    // Test that PageGuard properly unpins pages
    {
        Page* page = pool.newPage(page_id);
        ASSERT(page != nullptr);
        
        // Create a PageGuard
        PageGuard guard(pool, page_id, page);
        
        // Use the page through the guard
        const char* data = "Test data";
        SlotId slot;
        guard->insertRecord(data, strlen(data), slot);
        guard.setDirty(true);
        
        // Guard goes out of scope - should auto-unpin
    }
    
    // Page should be unpinned now, can be evicted
    // Fetch it again to verify data persisted
    Page* page = pool.fetchPage(page_id);
    ASSERT(page != nullptr);
    
    char buffer[PAGE_SIZE];
    uint16_t length;
    ASSERT(page->getRecord(0, buffer, length));
    ASSERT(memcmp(buffer, "Test data", length) == 0);
    
    pool.unpinPage(page_id, false);
    
    std::remove(test_db.c_str());
}

// =============================================================================
// TEST 7: Checksum in page (compile-time check that it exists)
// =============================================================================
TEST(page_checksum) {
    Page page(1);
    
    const char* data = "Checksum test data";
    SlotId slot;
    page.insertRecord(data, strlen(data), slot);
    
    // Serialize (checksum is calculated here)
    char buffer[PAGE_SIZE];
    page.serialize(buffer);
    
    // The checksum field should be set (at offset of checksum in PageHeader)
    uint32_t checksum;
    memcpy(&checksum, buffer + offsetof(PageHeader, checksum), sizeof(uint32_t));
    
    // Checksum should not be zero for non-empty page
    ASSERT(checksum != 0);
}

// =============================================================================
// MAIN
// =============================================================================
int main() {
    std::cout << "===========================================\n";
    std::cout << "  MiniDB Code Review Fixes - Test Suite\n";
    std::cout << "===========================================\n\n";
    
    RUN_TEST(integer_underflow_fix);
    RUN_TEST(page_serialization_fix);
    RUN_TEST(file_offset_fix);
    RUN_TEST(catalog_thread_safety);
    RUN_TEST(tablelock_shared_ptr);
    RUN_TEST(page_guard_raii);
    RUN_TEST(page_checksum);
    
    std::cout << "\n===========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, " 
              << tests_failed << " failed\n";
    std::cout << "===========================================\n";
    
    return tests_failed > 0 ? 1 : 0;
}
