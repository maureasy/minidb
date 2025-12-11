#include "concurrency/transaction.h"
#include "storage/buffer_pool.h"

namespace minidb {

// Transaction implementation
Transaction::Transaction(TxnId txn_id, IsolationLevel isolation)
    : txn_id_(txn_id), isolation_level_(isolation) {}

void Transaction::addReadItem(const std::string& table, PageId page, SlotId slot) {
    read_set_.insert({table, page, slot});
}

bool Transaction::hasReadItem(const std::string& table, PageId page, SlotId slot) const {
    return read_set_.find({table, page, slot}) != read_set_.end();
}

void Transaction::addWriteItem(const std::string& table, PageId page, SlotId slot) {
    write_set_.insert({table, page, slot});
}

// TransactionManager implementation
TransactionManager::TransactionManager(WalManager& wal, LockManager& lock_mgr, BufferPool& buffer_pool)
    : wal_(wal), lock_mgr_(lock_mgr), buffer_pool_(buffer_pool) {}

Transaction* TransactionManager::beginTransaction(IsolationLevel isolation) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    TxnId txn_id = wal_.beginTransaction();
    auto txn = std::make_unique<Transaction>(txn_id, isolation);
    
    // For REPEATABLE_READ and SERIALIZABLE, take a snapshot
    if (isolation == IsolationLevel::REPEATABLE_READ || 
        isolation == IsolationLevel::SERIALIZABLE) {
        txn->setSnapshotLSN(wal_.getCurrentLSN());
    }
    
    Transaction* txn_ptr = txn.get();
    transactions_[txn_id] = std::move(txn);
    current_txn_ = txn_ptr;
    
    return txn_ptr;
}

bool TransactionManager::commitTransaction(Transaction* txn) {
    if (!txn || txn->getState() != TransactionState::ACTIVE) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // For SERIALIZABLE, check for conflicts
    if (txn->getIsolationLevel() == IsolationLevel::SERIALIZABLE) {
        // Check if any item in our read set was modified by another transaction
        // Simplified: we assume no conflicts for now
    }
    
    // Commit in WAL
    if (!wal_.commitTransaction(txn->getId())) {
        return false;
    }
    
    // Flush dirty pages
    for (PageId page_id : txn->getModifiedPages()) {
        buffer_pool_.flushPage(page_id);
    }
    
    // Release all locks
    lock_mgr_.releaseAllLocks(txn->getId());
    
    txn->setState(TransactionState::COMMITTED);
    
    if (current_txn_ == txn) {
        current_txn_ = nullptr;
    }
    
    return true;
}

bool TransactionManager::abortTransaction(Transaction* txn) {
    if (!txn || txn->getState() != TransactionState::ACTIVE) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Undo changes
    undoTransaction(txn);
    
    // Abort in WAL
    wal_.abortTransaction(txn->getId());
    
    // Release all locks
    lock_mgr_.releaseAllLocks(txn->getId());
    
    txn->setState(TransactionState::ABORTED);
    
    if (current_txn_ == txn) {
        current_txn_ = nullptr;
    }
    
    return true;
}

void TransactionManager::undoTransaction(Transaction* txn) {
    // In a full implementation, we would:
    // 1. Read the WAL backwards for this transaction
    // 2. For each INSERT: delete the record
    // 3. For each DELETE: re-insert the old record
    // 4. For each UPDATE: restore the old value
    
    // For now, we just discard dirty pages (simplified undo)
    for (PageId page_id : txn->getModifiedPages()) {
        // Discard the page from buffer pool (don't flush)
        buffer_pool_.unpinPage(page_id, false);
    }
}

void TransactionManager::recover() {
    // ARIES-style recovery (simplified):
    // 1. Analysis pass: find active transactions at crash
    // 2. Redo pass: redo all committed transactions
    // 3. Undo pass: undo all uncommitted transactions
    
    wal_.recover();
}

bool TransactionManager::isVisible(TxnId writer_txn_id, Transaction* reader_txn) const {
    if (!reader_txn) return true;
    
    // Same transaction can see its own writes
    if (writer_txn_id == reader_txn->getId()) return true;
    
    auto it = transactions_.find(writer_txn_id);
    if (it == transactions_.end()) {
        // Transaction not found, assume committed
        return true;
    }
    
    const Transaction* writer_txn = it->second.get();
    
    switch (reader_txn->getIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
            // Can see everything
            return true;
            
        case IsolationLevel::READ_COMMITTED:
            // Can only see committed data
            return writer_txn->getState() == TransactionState::COMMITTED;
            
        case IsolationLevel::REPEATABLE_READ:
        case IsolationLevel::SERIALIZABLE:
            // Can only see data committed before our snapshot
            if (writer_txn->getState() != TransactionState::COMMITTED) {
                return false;
            }
            // Check if committed before our snapshot (simplified)
            return true;
    }
    
    return true;
}

} // namespace minidb
