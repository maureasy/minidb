#pragma once

#include "common.h"
#include <condition_variable>
#include <thread>
#include <atomic>

namespace minidb {

// Lock modes
enum class LockMode {
    SHARED,      // Read lock - multiple readers allowed
    EXCLUSIVE    // Write lock - single writer only
};

// Lock request status
enum class LockStatus {
    WAITING,
    GRANTED,
    ABORTED
};

// Resource ID for locking (can be table, page, or row)
struct ResourceId {
    enum class Type : uint8_t {
        TABLE,
        PAGE,
        ROW
    };
    
    Type type;
    TableId table_id;
    PageId page_id;
    SlotId slot_id;
    
    bool operator==(const ResourceId& other) const {
        return type == other.type && table_id == other.table_id &&
               page_id == other.page_id && slot_id == other.slot_id;
    }
};

// Hash function for ResourceId
struct ResourceIdHash {
    size_t operator()(const ResourceId& rid) const {
        size_t h1 = std::hash<uint8_t>()(static_cast<uint8_t>(rid.type));
        size_t h2 = std::hash<TableId>()(rid.table_id);
        size_t h3 = std::hash<PageId>()(rid.page_id);
        size_t h4 = std::hash<SlotId>()(rid.slot_id);
        return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
    }
};

// Transaction ID for locking
using LockTxnId = uint64_t;

// Lock request
struct LockRequest {
    LockTxnId txn_id;
    LockMode mode;
    LockStatus status;
};

// Lock queue for a resource
struct LockQueue {
    std::list<LockRequest> requests;
    int shared_count = 0;
    bool has_exclusive = false;
    std::condition_variable cv;
};

// Lock manager for concurrency control
class LockManager {
public:
    LockManager();
    ~LockManager();
    
    // Acquire a lock (blocks until lock is granted or timeout)
    bool acquireLock(LockTxnId txn_id, const ResourceId& rid, LockMode mode,
                     int timeout_ms = 5000);
    
    // Release a specific lock
    bool releaseLock(LockTxnId txn_id, const ResourceId& rid);
    
    // Release all locks held by a transaction
    void releaseAllLocks(LockTxnId txn_id);
    
    // Try to upgrade a shared lock to exclusive
    bool upgradeLock(LockTxnId txn_id, const ResourceId& rid);
    
    // Check if a transaction holds a lock
    bool holdsLock(LockTxnId txn_id, const ResourceId& rid, LockMode mode) const;
    
    // Deadlock detection (returns true if deadlock detected)
    bool detectDeadlock();
    
    // Table-level locking shortcuts
    bool lockTable(LockTxnId txn_id, TableId table_id, LockMode mode);
    bool unlockTable(LockTxnId txn_id, TableId table_id);
    
    // Page-level locking shortcuts
    bool lockPage(LockTxnId txn_id, TableId table_id, PageId page_id, LockMode mode);
    bool unlockPage(LockTxnId txn_id, TableId table_id, PageId page_id);
    
    // Row-level locking shortcuts
    bool lockRow(LockTxnId txn_id, TableId table_id, PageId page_id, 
                 SlotId slot_id, LockMode mode);
    bool unlockRow(LockTxnId txn_id, TableId table_id, PageId page_id, SlotId slot_id);

private:
    mutable std::mutex mutex_;
    std::unordered_map<ResourceId, LockQueue, ResourceIdHash> lock_table_;
    
    // Track locks per transaction for easy cleanup
    std::unordered_map<LockTxnId, std::vector<ResourceId>> txn_locks_;
    
    // Helper functions
    bool grantLock(LockQueue& queue, const LockRequest& request);
    void wakeUpWaiters(LockQueue& queue);
};

// Read-write lock for simpler table-level concurrency
class TableLock {
public:
    void readLock() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (writers_ > 0 || waiting_writers_ > 0) {
            read_cv_.wait(lock);
        }
        readers_++;
    }
    
    void readUnlock() {
        std::unique_lock<std::mutex> lock(mutex_);
        readers_--;
        if (readers_ == 0) {
            write_cv_.notify_one();
        }
    }
    
    void writeLock() {
        std::unique_lock<std::mutex> lock(mutex_);
        waiting_writers_++;
        while (readers_ > 0 || writers_ > 0) {
            write_cv_.wait(lock);
        }
        waiting_writers_--;
        writers_++;
    }
    
    void writeUnlock() {
        std::unique_lock<std::mutex> lock(mutex_);
        writers_--;
        read_cv_.notify_all();
        write_cv_.notify_one();
    }
    
    // RAII helpers
    class ReadGuard {
    public:
        explicit ReadGuard(TableLock& lock) : lock_(lock) { lock_.readLock(); }
        ~ReadGuard() { lock_.readUnlock(); }
    private:
        TableLock& lock_;
    };
    
    class WriteGuard {
    public:
        explicit WriteGuard(TableLock& lock) : lock_(lock) { lock_.writeLock(); }
        ~WriteGuard() { lock_.writeUnlock(); }
    private:
        TableLock& lock_;
    };

private:
    std::mutex mutex_;
    std::condition_variable read_cv_;
    std::condition_variable write_cv_;
    int readers_ = 0;
    int writers_ = 0;
    int waiting_writers_ = 0;
};

// Database-level lock manager (simpler approach)
class DatabaseLockManager {
public:
    TableLock& getTableLock(const std::string& table_name) {
        std::lock_guard<std::mutex> lock(mutex_);
        return table_locks_[table_name];
    }
    
    // Global database lock for DDL operations
    void lockDatabase() { db_lock_.writeLock(); }
    void unlockDatabase() { db_lock_.writeUnlock(); }
    void readLockDatabase() { db_lock_.readLock(); }
    void readUnlockDatabase() { db_lock_.readUnlock(); }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, TableLock> table_locks_;
    TableLock db_lock_;
};

} // namespace minidb
