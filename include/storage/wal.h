#pragma once

#include "common.h"

namespace minidb {

// WAL record types
enum class WalRecordType : uint8_t {
    BEGIN_TXN,
    COMMIT_TXN,
    ABORT_TXN,
    INSERT,
    UPDATE,
    DELETE,
    CHECKPOINT
};

// Transaction ID
using TxnId = uint64_t;
constexpr TxnId INVALID_TXN_ID = 0;

// Log Sequence Number
using LSN = uint64_t;
constexpr LSN INVALID_LSN = 0;

// WAL record header
struct WalRecordHeader {
    LSN lsn;
    LSN prev_lsn;
    TxnId txn_id;
    WalRecordType type;
    uint32_t data_length;
    uint32_t checksum;
};

// WAL record for data modifications
struct WalDataRecord {
    PageId page_id;
    SlotId slot_id;
    uint16_t old_length;
    uint16_t new_length;
    // Followed by old_data and new_data
};

// Write-Ahead Log manager
class WalManager {
public:
    explicit WalManager(const std::string& wal_path);
    ~WalManager();
    
    // Transaction operations
    TxnId beginTransaction();
    bool commitTransaction(TxnId txn_id);
    bool abortTransaction(TxnId txn_id);
    
    // Log record operations
    LSN logInsert(TxnId txn_id, PageId page_id, SlotId slot_id, 
                  const char* data, uint16_t length);
    LSN logUpdate(TxnId txn_id, PageId page_id, SlotId slot_id,
                  const char* old_data, uint16_t old_length,
                  const char* new_data, uint16_t new_length);
    LSN logDelete(TxnId txn_id, PageId page_id, SlotId slot_id,
                  const char* old_data, uint16_t old_length);
    
    // Checkpoint
    void checkpoint();
    
    // Recovery
    void recover();
    
    // Flush log to disk
    void flush();
    
    // Get current LSN
    LSN getCurrentLSN() const { return current_lsn_; }
    
    // Check if WAL is enabled
    bool isEnabled() const { return enabled_; }
    void setEnabled(bool enabled) { enabled_ = enabled; }

private:
    std::string wal_path_;
    std::fstream log_file_;
    LSN current_lsn_ = 1;
    TxnId next_txn_id_ = 1;
    bool enabled_ = true;
    
    // Buffer for log records
    std::vector<char> log_buffer_;
    size_t buffer_offset_ = 0;
    static constexpr size_t BUFFER_SIZE = 64 * 1024;  // 64KB buffer
    
    // Active transactions
    std::unordered_map<TxnId, LSN> active_txns_;  // txn_id -> first_lsn
    
    // Mutex for thread safety
    mutable std::mutex mutex_;
    
    // Helper functions
    LSN appendRecord(const WalRecordHeader& header, const char* data, size_t length);
    uint32_t calculateChecksum(const char* data, size_t length);
    void flushBuffer();
    void openLogFile();
};

} // namespace minidb
