#pragma once

#include "common.h"

namespace minidb {

// Page header structure
struct PageHeader {
    PageId page_id;
    uint16_t num_slots;
    uint16_t free_space_offset;
    uint16_t free_space_end;
    PageId next_page;
    uint32_t checksum;
};

// Slot entry in the slot array
struct SlotEntry {
    uint16_t offset;
    uint16_t length;
    bool is_deleted;
};

// Page class - represents a single page in the database
class Page {
public:
    Page();
    explicit Page(PageId id);
    
    // Accessors
    PageId getPageId() const { return header_.page_id; }
    void setPageId(PageId id) { header_.page_id = id; }
    
    uint16_t getNumSlots() const { return header_.num_slots; }
    uint16_t getFreeSpace() const;
    
    PageId getNextPage() const { return header_.next_page; }
    void setNextPage(PageId next) { header_.next_page = next; }
    
    // Data access
    char* getData() { return data_; }
    const char* getData() const { return data_; }
    
    // Slot operations
    bool insertRecord(const char* data, uint16_t length, SlotId& slot_id);
    bool deleteRecord(SlotId slot_id);
    bool getRecord(SlotId slot_id, char* buffer, uint16_t& length) const;
    bool updateRecord(SlotId slot_id, const char* data, uint16_t length);
    
    // Serialization
    void serialize(char* buffer) const;
    void deserialize(const char* buffer);
    
    // Dirty flag for buffer pool
    bool isDirty() const { return is_dirty_; }
    void setDirty(bool dirty) { is_dirty_ = dirty; }
    
    // Pin count for buffer pool
    int getPinCount() const { return pin_count_; }
    void incrementPinCount() { ++pin_count_; }
    void decrementPinCount() { if (pin_count_ > 0) --pin_count_; }

private:
    PageHeader header_;
    char data_[PAGE_SIZE];
    std::vector<SlotEntry> slots_;
    bool is_dirty_ = false;
    int pin_count_ = 0;
    
    void initializePage();
    uint16_t getSlotArraySize() const;
};

} // namespace minidb
