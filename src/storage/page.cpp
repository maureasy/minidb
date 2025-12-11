#include "storage/page.h"
#include <cstring>

namespace minidb {

Page::Page() {
    initializePage();
}

Page::Page(PageId id) {
    initializePage();
    header_.page_id = id;
}

void Page::initializePage() {
    std::memset(data_, 0, PAGE_SIZE);
    header_.page_id = INVALID_PAGE_ID;
    header_.num_slots = 0;
    header_.free_space_offset = sizeof(PageHeader);
    header_.free_space_end = PAGE_SIZE;
    header_.next_page = INVALID_PAGE_ID;
    header_.checksum = 0;
    slots_.clear();
}

uint16_t Page::getFreeSpace() const {
    return header_.free_space_end - header_.free_space_offset - getSlotArraySize();
}

uint16_t Page::getSlotArraySize() const {
    return static_cast<uint16_t>(slots_.size() * sizeof(SlotEntry));
}

bool Page::insertRecord(const char* data, uint16_t length, SlotId& slot_id) {
    // Check if we have enough space
    uint16_t required_space = length + sizeof(SlotEntry);
    if (getFreeSpace() < required_space) {
        return false;
    }
    
    // Find a deleted slot or create new one
    slot_id = UINT16_MAX;
    for (size_t i = 0; i < slots_.size(); i++) {
        if (slots_[i].is_deleted) {
            slot_id = static_cast<SlotId>(i);
            break;
        }
    }
    
    // Allocate space from the end
    header_.free_space_end -= length;
    uint16_t offset = header_.free_space_end;
    
    // Copy data
    std::memcpy(data_ + offset, data, length);
    
    // Update slot
    if (slot_id == UINT16_MAX) {
        // Create new slot
        SlotEntry slot;
        slot.offset = offset;
        slot.length = length;
        slot.is_deleted = false;
        slots_.push_back(slot);
        slot_id = static_cast<SlotId>(slots_.size() - 1);
    } else {
        // Reuse deleted slot
        slots_[slot_id].offset = offset;
        slots_[slot_id].length = length;
        slots_[slot_id].is_deleted = false;
    }
    
    header_.num_slots = static_cast<uint16_t>(slots_.size());
    is_dirty_ = true;
    return true;
}

bool Page::deleteRecord(SlotId slot_id) {
    if (slot_id >= slots_.size() || slots_[slot_id].is_deleted) {
        return false;
    }
    
    slots_[slot_id].is_deleted = true;
    is_dirty_ = true;
    return true;
}

bool Page::getRecord(SlotId slot_id, char* buffer, uint16_t& length) const {
    if (slot_id >= slots_.size() || slots_[slot_id].is_deleted) {
        return false;
    }
    
    const SlotEntry& slot = slots_[slot_id];
    length = slot.length;
    std::memcpy(buffer, data_ + slot.offset, length);
    return true;
}

bool Page::updateRecord(SlotId slot_id, const char* data, uint16_t length) {
    if (slot_id >= slots_.size() || slots_[slot_id].is_deleted) {
        return false;
    }
    
    SlotEntry& slot = slots_[slot_id];
    
    // If new data fits in existing space, update in place
    if (length <= slot.length) {
        std::memcpy(data_ + slot.offset, data, length);
        slot.length = length;
        is_dirty_ = true;
        return true;
    }
    
    // Otherwise, delete and re-insert
    // Note: This is a simplified approach; real DBs would compact pages
    slots_[slot_id].is_deleted = true;
    SlotId new_slot;
    if (!insertRecord(data, length, new_slot)) {
        // Restore the old slot if insert fails
        slots_[slot_id].is_deleted = false;
        return false;
    }
    
    // Copy to original slot position (swap slots)
    slots_[slot_id] = slots_[new_slot];
    slots_[new_slot].is_deleted = true;
    
    is_dirty_ = true;
    return true;
}

void Page::serialize(char* buffer) const {
    size_t offset = 0;
    
    // Write header
    std::memcpy(buffer + offset, &header_, sizeof(PageHeader));
    offset += sizeof(PageHeader);
    
    // Write number of slots
    uint16_t num_slots = static_cast<uint16_t>(slots_.size());
    std::memcpy(buffer + offset, &num_slots, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    // Write slots
    for (const auto& slot : slots_) {
        std::memcpy(buffer + offset, &slot.offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        std::memcpy(buffer + offset, &slot.length, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        uint8_t deleted = slot.is_deleted ? 1 : 0;
        std::memcpy(buffer + offset, &deleted, sizeof(uint8_t));
        offset += sizeof(uint8_t);
    }
    
    // Write data section
    std::memcpy(buffer + PAGE_SIZE - (PAGE_SIZE - header_.free_space_end), 
                data_ + header_.free_space_end, 
                PAGE_SIZE - header_.free_space_end);
}

void Page::deserialize(const char* buffer) {
    size_t offset = 0;
    
    // Read header
    std::memcpy(&header_, buffer + offset, sizeof(PageHeader));
    offset += sizeof(PageHeader);
    
    // Read number of slots
    uint16_t num_slots;
    std::memcpy(&num_slots, buffer + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    // Read slots
    slots_.clear();
    slots_.reserve(num_slots);
    for (uint16_t i = 0; i < num_slots; i++) {
        SlotEntry slot;
        std::memcpy(&slot.offset, buffer + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        std::memcpy(&slot.length, buffer + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        uint8_t deleted;
        std::memcpy(&deleted, buffer + offset, sizeof(uint8_t));
        slot.is_deleted = (deleted != 0);
        offset += sizeof(uint8_t);
        slots_.push_back(slot);
    }
    
    // Read data section
    std::memcpy(data_, buffer, PAGE_SIZE);
}

} // namespace minidb
