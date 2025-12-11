#include "concurrency/lock_manager.h"
#include <chrono>

namespace minidb {

LockManager::LockManager() {}

LockManager::~LockManager() {}

bool LockManager::acquireLock(LockTxnId txn_id, const ResourceId& rid, LockMode mode,
                               int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // Get or create lock queue for this resource
    LockQueue& queue = lock_table_[rid];
    
    // Check if this transaction already holds a lock on this resource
    for (auto& req : queue.requests) {
        if (req.txn_id == txn_id && req.status == LockStatus::GRANTED) {
            // Already have a lock
            if (req.mode == LockMode::EXCLUSIVE || mode == LockMode::SHARED) {
                return true;  // Already have sufficient lock
            }
            // Need to upgrade from shared to exclusive
            return upgradeLock(txn_id, rid);
        }
    }
    
    // Create lock request
    LockRequest request;
    request.txn_id = txn_id;
    request.mode = mode;
    request.status = LockStatus::WAITING;
    
    // Try to grant immediately
    if (grantLock(queue, request)) {
        request.status = LockStatus::GRANTED;
        queue.requests.push_back(request);
        txn_locks_[txn_id].push_back(rid);
        return true;
    }
    
    // Add to wait queue
    queue.requests.push_back(request);
    auto it = --queue.requests.end();
    txn_locks_[txn_id].push_back(rid);
    
    // Wait for lock with timeout
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    
    while (it->status == LockStatus::WAITING) {
        if (queue.cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            // Timeout - remove request and fail
            it->status = LockStatus::ABORTED;
            queue.requests.erase(it);
            
            // Remove from transaction's lock list
            auto& txn_rids = txn_locks_[txn_id];
            txn_rids.erase(std::find(txn_rids.begin(), txn_rids.end(), rid));
            
            return false;
        }
        
        // Check if we can be granted now
        if (grantLock(queue, *it)) {
            it->status = LockStatus::GRANTED;
            if (it->mode == LockMode::SHARED) {
                queue.shared_count++;
            } else {
                queue.has_exclusive = true;
            }
            return true;
        }
    }
    
    return it->status == LockStatus::GRANTED;
}

bool LockManager::grantLock(LockQueue& queue, const LockRequest& request) {
    if (request.mode == LockMode::SHARED) {
        // Can grant shared if no exclusive lock is held
        return !queue.has_exclusive;
    } else {
        // Can grant exclusive only if no locks are held
        return queue.shared_count == 0 && !queue.has_exclusive;
    }
}

bool LockManager::releaseLock(LockTxnId txn_id, const ResourceId& rid) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = lock_table_.find(rid);
    if (it == lock_table_.end()) {
        return false;
    }
    
    LockQueue& queue = it->second;
    
    // Find and remove the lock request
    for (auto req_it = queue.requests.begin(); req_it != queue.requests.end(); ++req_it) {
        if (req_it->txn_id == txn_id && req_it->status == LockStatus::GRANTED) {
            if (req_it->mode == LockMode::SHARED) {
                queue.shared_count--;
            } else {
                queue.has_exclusive = false;
            }
            queue.requests.erase(req_it);
            
            // Remove from transaction's lock list
            auto& txn_rids = txn_locks_[txn_id];
            auto rid_it = std::find(txn_rids.begin(), txn_rids.end(), rid);
            if (rid_it != txn_rids.end()) {
                txn_rids.erase(rid_it);
            }
            
            // Wake up waiters
            wakeUpWaiters(queue);
            
            return true;
        }
    }
    
    return false;
}

void LockManager::releaseAllLocks(LockTxnId txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = txn_locks_.find(txn_id);
    if (it == txn_locks_.end()) {
        return;
    }
    
    // Copy the list since we'll be modifying it
    std::vector<ResourceId> rids = it->second;
    
    for (const auto& rid : rids) {
        auto queue_it = lock_table_.find(rid);
        if (queue_it == lock_table_.end()) continue;
        
        LockQueue& queue = queue_it->second;
        
        for (auto req_it = queue.requests.begin(); req_it != queue.requests.end(); ) {
            if (req_it->txn_id == txn_id) {
                if (req_it->status == LockStatus::GRANTED) {
                    if (req_it->mode == LockMode::SHARED) {
                        queue.shared_count--;
                    } else {
                        queue.has_exclusive = false;
                    }
                }
                req_it = queue.requests.erase(req_it);
            } else {
                ++req_it;
            }
        }
        
        wakeUpWaiters(queue);
    }
    
    txn_locks_.erase(txn_id);
}

void LockManager::wakeUpWaiters(LockQueue& queue) {
    // Try to grant locks to waiting requests
    for (auto& req : queue.requests) {
        if (req.status == LockStatus::WAITING) {
            if (grantLock(queue, req)) {
                req.status = LockStatus::GRANTED;
                if (req.mode == LockMode::SHARED) {
                    queue.shared_count++;
                } else {
                    queue.has_exclusive = true;
                    break;  // Only one exclusive can be granted
                }
            }
        }
    }
    
    queue.cv.notify_all();
}

bool LockManager::upgradeLock(LockTxnId txn_id, const ResourceId& rid) {
    // Note: This is called with mutex already held
    auto it = lock_table_.find(rid);
    if (it == lock_table_.end()) {
        return false;
    }
    
    LockQueue& queue = it->second;
    
    // Find the shared lock
    for (auto& req : queue.requests) {
        if (req.txn_id == txn_id && req.status == LockStatus::GRANTED) {
            if (req.mode == LockMode::EXCLUSIVE) {
                return true;  // Already exclusive
            }
            
            // Can upgrade only if we're the only shared holder
            if (queue.shared_count == 1 && !queue.has_exclusive) {
                req.mode = LockMode::EXCLUSIVE;
                queue.shared_count--;
                queue.has_exclusive = true;
                return true;
            }
            
            return false;  // Cannot upgrade
        }
    }
    
    return false;
}

bool LockManager::holdsLock(LockTxnId txn_id, const ResourceId& rid, LockMode mode) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = lock_table_.find(rid);
    if (it == lock_table_.end()) {
        return false;
    }
    
    const LockQueue& queue = it->second;
    
    for (const auto& req : queue.requests) {
        if (req.txn_id == txn_id && req.status == LockStatus::GRANTED) {
            if (mode == LockMode::SHARED) {
                return true;  // Any lock satisfies shared requirement
            }
            return req.mode == LockMode::EXCLUSIVE;
        }
    }
    
    return false;
}

bool LockManager::detectDeadlock() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Build wait-for graph
    std::unordered_map<LockTxnId, std::vector<LockTxnId>> wait_for;
    
    for (const auto& [rid, queue] : lock_table_) {
        std::vector<LockTxnId> holders;
        std::vector<LockTxnId> waiters;
        
        for (const auto& req : queue.requests) {
            if (req.status == LockStatus::GRANTED) {
                holders.push_back(req.txn_id);
            } else if (req.status == LockStatus::WAITING) {
                waiters.push_back(req.txn_id);
            }
        }
        
        // Each waiter waits for all holders
        for (LockTxnId waiter : waiters) {
            for (LockTxnId holder : holders) {
                if (waiter != holder) {
                    wait_for[waiter].push_back(holder);
                }
            }
        }
    }
    
    // DFS to detect cycle
    std::unordered_set<LockTxnId> visited;
    std::unordered_set<LockTxnId> rec_stack;
    
    std::function<bool(LockTxnId)> hasCycle = [&](LockTxnId txn) -> bool {
        visited.insert(txn);
        rec_stack.insert(txn);
        
        auto it = wait_for.find(txn);
        if (it != wait_for.end()) {
            for (LockTxnId neighbor : it->second) {
                if (visited.find(neighbor) == visited.end()) {
                    if (hasCycle(neighbor)) return true;
                } else if (rec_stack.find(neighbor) != rec_stack.end()) {
                    return true;  // Cycle detected
                }
            }
        }
        
        rec_stack.erase(txn);
        return false;
    };
    
    for (const auto& [txn, _] : wait_for) {
        if (visited.find(txn) == visited.end()) {
            if (hasCycle(txn)) {
                return true;
            }
        }
    }
    
    return false;
}

bool LockManager::lockTable(LockTxnId txn_id, TableId table_id, LockMode mode) {
    ResourceId rid;
    rid.type = ResourceId::Type::TABLE;
    rid.table_id = table_id;
    rid.page_id = INVALID_PAGE_ID;
    rid.slot_id = 0;
    return acquireLock(txn_id, rid, mode);
}

bool LockManager::unlockTable(LockTxnId txn_id, TableId table_id) {
    ResourceId rid;
    rid.type = ResourceId::Type::TABLE;
    rid.table_id = table_id;
    rid.page_id = INVALID_PAGE_ID;
    rid.slot_id = 0;
    return releaseLock(txn_id, rid);
}

bool LockManager::lockPage(LockTxnId txn_id, TableId table_id, PageId page_id, LockMode mode) {
    ResourceId rid;
    rid.type = ResourceId::Type::PAGE;
    rid.table_id = table_id;
    rid.page_id = page_id;
    rid.slot_id = 0;
    return acquireLock(txn_id, rid, mode);
}

bool LockManager::unlockPage(LockTxnId txn_id, TableId table_id, PageId page_id) {
    ResourceId rid;
    rid.type = ResourceId::Type::PAGE;
    rid.table_id = table_id;
    rid.page_id = page_id;
    rid.slot_id = 0;
    return releaseLock(txn_id, rid);
}

bool LockManager::lockRow(LockTxnId txn_id, TableId table_id, PageId page_id, 
                          SlotId slot_id, LockMode mode) {
    ResourceId rid;
    rid.type = ResourceId::Type::ROW;
    rid.table_id = table_id;
    rid.page_id = page_id;
    rid.slot_id = slot_id;
    return acquireLock(txn_id, rid, mode);
}

bool LockManager::unlockRow(LockTxnId txn_id, TableId table_id, PageId page_id, SlotId slot_id) {
    ResourceId rid;
    rid.type = ResourceId::Type::ROW;
    rid.table_id = table_id;
    rid.page_id = page_id;
    rid.slot_id = slot_id;
    return releaseLock(txn_id, rid);
}

} // namespace minidb
