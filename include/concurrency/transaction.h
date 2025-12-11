#pragma once

#include "common.h"
#include "storage/wal.h"
#include "concurrency/lock_manager.h"

namespace minidb {

// Transaction isolation levels
enum class IsolationLevel {
    READ_UNCOMMITTED,   // Dirty reads allowed
    READ_COMMITTED,     // Only committed data visible
    REPEATABLE_READ,    // Snapshot at first read
    SERIALIZABLE        // Full isolation
};

// Transaction state
enum class TransactionState {
    ACTIVE,
    COMMITTED,
    ABORTED
};

// Transaction class
class Transaction {
public:
    Transaction(TxnId txn_id, IsolationLevel isolation = IsolationLevel::READ_COMMITTED);
    
    TxnId getId() const { return txn_id_; }
    IsolationLevel getIsolationLevel() const { return isolation_level_; }
    TransactionState getState() const { return state_; }
    
    void setState(TransactionState state) { state_ = state; }
    
    // Track modified pages for undo
    void addModifiedPage(PageId page_id) { modified_pages_.insert(page_id); }
    const std::unordered_set<PageId>& getModifiedPages() const { return modified_pages_; }
    
    // Track read set for repeatable read / serializable
    void addReadItem(const std::string& table, PageId page, SlotId slot);
    bool hasReadItem(const std::string& table, PageId page, SlotId slot) const;
    
    // Track write set
    void addWriteItem(const std::string& table, PageId page, SlotId slot);
    
    // Snapshot for REPEATABLE_READ and SERIALIZABLE
    void setSnapshotLSN(LSN lsn) { snapshot_lsn_ = lsn; }
    LSN getSnapshotLSN() const { return snapshot_lsn_; }

private:
    TxnId txn_id_;
    IsolationLevel isolation_level_;
    TransactionState state_ = TransactionState::ACTIVE;
    std::unordered_set<PageId> modified_pages_;
    LSN snapshot_lsn_ = INVALID_LSN;
    
    // Read/write sets for conflict detection
    struct ItemKey {
        std::string table;
        PageId page;
        SlotId slot;
        bool operator==(const ItemKey& other) const {
            return table == other.table && page == other.page && slot == other.slot;
        }
    };
    struct ItemKeyHash {
        size_t operator()(const ItemKey& k) const {
            return std::hash<std::string>()(k.table) ^ 
                   (std::hash<PageId>()(k.page) << 1) ^
                   (std::hash<SlotId>()(k.slot) << 2);
        }
    };
    std::unordered_set<ItemKey, ItemKeyHash> read_set_;
    std::unordered_set<ItemKey, ItemKeyHash> write_set_;
};

// Forward declarations
class BufferPool;
class Catalog;

// Transaction Manager
class TransactionManager {
public:
    TransactionManager(WalManager& wal, LockManager& lock_mgr, BufferPool& buffer_pool);
    
    // Transaction lifecycle
    Transaction* beginTransaction(IsolationLevel isolation = IsolationLevel::READ_COMMITTED);
    bool commitTransaction(Transaction* txn);
    bool abortTransaction(Transaction* txn);
    
    // Get current transaction (thread-local in real implementation)
    Transaction* getCurrentTransaction() const { return current_txn_; }
    void setCurrentTransaction(Transaction* txn) { current_txn_ = txn; }
    
    // Recovery
    void recover();
    
    // Visibility check for MVCC (simplified)
    bool isVisible(TxnId writer_txn_id, Transaction* reader_txn) const;

private:
    WalManager& wal_;
    LockManager& lock_mgr_;
    BufferPool& buffer_pool_;
    
    std::unordered_map<TxnId, std::unique_ptr<Transaction>> transactions_;
    Transaction* current_txn_ = nullptr;
    std::mutex mutex_;
    
    // Undo a transaction's changes
    void undoTransaction(Transaction* txn);
    
    // Redo committed transactions during recovery
    void redoTransaction(TxnId txn_id, const std::vector<WalRecordHeader>& records);
};

} // namespace minidb
