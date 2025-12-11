#include "storage/wal.h"
#include <filesystem>

namespace minidb {

WalManager::WalManager(const std::string& wal_path) : wal_path_(wal_path) {
    log_buffer_.resize(BUFFER_SIZE);
    openLogFile();
}

WalManager::~WalManager() {
    flush();
    if (log_file_.is_open()) {
        log_file_.close();
    }
}

void WalManager::openLogFile() {
    bool file_exists = std::filesystem::exists(wal_path_);
    
    log_file_.open(wal_path_, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!log_file_.is_open()) {
        // Create new file
        log_file_.open(wal_path_, std::ios::out | std::ios::binary);
        log_file_.close();
        log_file_.open(wal_path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    
    if (file_exists && log_file_.is_open()) {
        // Read existing LSN from end of file
        log_file_.seekg(0, std::ios::end);
        auto file_size = log_file_.tellg();
        if (file_size > 0) {
            // Find the last valid record to get current LSN
            // For simplicity, we'll start fresh after crash
            current_lsn_ = 1;
        }
    }
}

uint32_t WalManager::calculateChecksum(const char* data, size_t length) {
    // Simple checksum using CRC-like algorithm
    uint32_t checksum = 0;
    for (size_t i = 0; i < length; i++) {
        checksum = (checksum << 1) ^ static_cast<uint8_t>(data[i]);
    }
    return checksum;
}

TxnId WalManager::beginTransaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    TxnId txn_id = next_txn_id_++;
    
    if (enabled_) {
        WalRecordHeader header;
        header.lsn = current_lsn_;
        header.prev_lsn = INVALID_LSN;
        header.txn_id = txn_id;
        header.type = WalRecordType::BEGIN_TXN;
        header.data_length = 0;
        header.checksum = 0;
        
        LSN lsn = appendRecord(header, nullptr, 0);
        active_txns_[txn_id] = lsn;
    }
    
    return txn_id;
}

bool WalManager::commitTransaction(TxnId txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = active_txns_.find(txn_id);
    if (it == active_txns_.end() && enabled_) {
        return false;
    }
    
    if (enabled_) {
        WalRecordHeader header;
        header.lsn = current_lsn_;
        header.prev_lsn = it->second;
        header.txn_id = txn_id;
        header.type = WalRecordType::COMMIT_TXN;
        header.data_length = 0;
        header.checksum = 0;
        
        appendRecord(header, nullptr, 0);
        active_txns_.erase(it);
        
        // Force flush on commit for durability
        flushBuffer();
    }
    
    return true;
}

bool WalManager::abortTransaction(TxnId txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = active_txns_.find(txn_id);
    if (it == active_txns_.end() && enabled_) {
        return false;
    }
    
    if (enabled_) {
        WalRecordHeader header;
        header.lsn = current_lsn_;
        header.prev_lsn = it->second;
        header.txn_id = txn_id;
        header.type = WalRecordType::ABORT_TXN;
        header.data_length = 0;
        header.checksum = 0;
        
        appendRecord(header, nullptr, 0);
        active_txns_.erase(it);
    }
    
    return true;
}

LSN WalManager::logInsert(TxnId txn_id, PageId page_id, SlotId slot_id,
                          const char* data, uint16_t length) {
    if (!enabled_) return INVALID_LSN;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Build data record
    WalDataRecord data_rec;
    data_rec.page_id = page_id;
    data_rec.slot_id = slot_id;
    data_rec.old_length = 0;
    data_rec.new_length = length;
    
    // Combine record header + data
    size_t total_size = sizeof(WalDataRecord) + length;
    std::vector<char> record_data(total_size);
    std::memcpy(record_data.data(), &data_rec, sizeof(WalDataRecord));
    std::memcpy(record_data.data() + sizeof(WalDataRecord), data, length);
    
    WalRecordHeader header;
    header.lsn = current_lsn_;
    header.prev_lsn = INVALID_LSN;
    header.txn_id = txn_id;
    header.type = WalRecordType::INSERT;
    header.data_length = static_cast<uint32_t>(total_size);
    header.checksum = calculateChecksum(record_data.data(), total_size);
    
    return appendRecord(header, record_data.data(), total_size);
}

LSN WalManager::logUpdate(TxnId txn_id, PageId page_id, SlotId slot_id,
                          const char* old_data, uint16_t old_length,
                          const char* new_data, uint16_t new_length) {
    if (!enabled_) return INVALID_LSN;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    WalDataRecord data_rec;
    data_rec.page_id = page_id;
    data_rec.slot_id = slot_id;
    data_rec.old_length = old_length;
    data_rec.new_length = new_length;
    
    size_t total_size = sizeof(WalDataRecord) + old_length + new_length;
    std::vector<char> record_data(total_size);
    size_t offset = 0;
    std::memcpy(record_data.data() + offset, &data_rec, sizeof(WalDataRecord));
    offset += sizeof(WalDataRecord);
    std::memcpy(record_data.data() + offset, old_data, old_length);
    offset += old_length;
    std::memcpy(record_data.data() + offset, new_data, new_length);
    
    WalRecordHeader header;
    header.lsn = current_lsn_;
    header.prev_lsn = INVALID_LSN;
    header.txn_id = txn_id;
    header.type = WalRecordType::UPDATE;
    header.data_length = static_cast<uint32_t>(total_size);
    header.checksum = calculateChecksum(record_data.data(), total_size);
    
    return appendRecord(header, record_data.data(), total_size);
}

LSN WalManager::logDelete(TxnId txn_id, PageId page_id, SlotId slot_id,
                          const char* old_data, uint16_t old_length) {
    if (!enabled_) return INVALID_LSN;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    WalDataRecord data_rec;
    data_rec.page_id = page_id;
    data_rec.slot_id = slot_id;
    data_rec.old_length = old_length;
    data_rec.new_length = 0;
    
    size_t total_size = sizeof(WalDataRecord) + old_length;
    std::vector<char> record_data(total_size);
    std::memcpy(record_data.data(), &data_rec, sizeof(WalDataRecord));
    std::memcpy(record_data.data() + sizeof(WalDataRecord), old_data, old_length);
    
    WalRecordHeader header;
    header.lsn = current_lsn_;
    header.prev_lsn = INVALID_LSN;
    header.txn_id = txn_id;
    header.type = WalRecordType::DELETE;
    header.data_length = static_cast<uint32_t>(total_size);
    header.checksum = calculateChecksum(record_data.data(), total_size);
    
    return appendRecord(header, record_data.data(), total_size);
}

LSN WalManager::appendRecord(const WalRecordHeader& header, const char* data, size_t length) {
    size_t record_size = sizeof(WalRecordHeader) + length;
    
    // Check if buffer needs flushing
    if (buffer_offset_ + record_size > BUFFER_SIZE) {
        flushBuffer();
    }
    
    // Copy header to buffer
    std::memcpy(log_buffer_.data() + buffer_offset_, &header, sizeof(WalRecordHeader));
    buffer_offset_ += sizeof(WalRecordHeader);
    
    // Copy data to buffer
    if (length > 0 && data != nullptr) {
        std::memcpy(log_buffer_.data() + buffer_offset_, data, length);
        buffer_offset_ += length;
    }
    
    LSN lsn = current_lsn_++;
    return lsn;
}

void WalManager::flushBuffer() {
    if (buffer_offset_ == 0) return;
    
    if (log_file_.is_open()) {
        log_file_.seekp(0, std::ios::end);
        log_file_.write(log_buffer_.data(), buffer_offset_);
        log_file_.flush();
    }
    
    buffer_offset_ = 0;
}

void WalManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    flushBuffer();
}

void WalManager::checkpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!enabled_) return;
    
    WalRecordHeader header;
    header.lsn = current_lsn_;
    header.prev_lsn = INVALID_LSN;
    header.txn_id = INVALID_TXN_ID;
    header.type = WalRecordType::CHECKPOINT;
    header.data_length = 0;
    header.checksum = 0;
    
    appendRecord(header, nullptr, 0);
    flushBuffer();
}

void WalManager::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!log_file_.is_open()) return;
    
    log_file_.seekg(0, std::ios::beg);
    
    std::unordered_map<TxnId, std::vector<WalRecordHeader>> txn_records;
    std::unordered_map<TxnId, bool> committed_txns;
    
    // Phase 1: Scan log and build transaction map
    while (log_file_.good()) {
        WalRecordHeader header;
        log_file_.read(reinterpret_cast<char*>(&header), sizeof(WalRecordHeader));
        
        if (log_file_.gcount() != sizeof(WalRecordHeader)) {
            break;
        }
        
        // Skip data portion
        if (header.data_length > 0) {
            log_file_.seekg(header.data_length, std::ios::cur);
        }
        
        current_lsn_ = std::max(current_lsn_, header.lsn + 1);
        
        switch (header.type) {
            case WalRecordType::BEGIN_TXN:
                txn_records[header.txn_id] = {};
                break;
            case WalRecordType::COMMIT_TXN:
                committed_txns[header.txn_id] = true;
                break;
            case WalRecordType::ABORT_TXN:
                committed_txns[header.txn_id] = false;
                break;
            case WalRecordType::INSERT:
            case WalRecordType::UPDATE:
            case WalRecordType::DELETE:
                txn_records[header.txn_id].push_back(header);
                break;
            case WalRecordType::CHECKPOINT:
                // Clear older records
                break;
        }
    }
    
    // Phase 2: Redo committed transactions (not implemented - would need buffer pool)
    // Phase 3: Undo uncommitted transactions (not implemented - would need buffer pool)
    
    // For now, just clear the active transactions
    active_txns_.clear();
}

} // namespace minidb
