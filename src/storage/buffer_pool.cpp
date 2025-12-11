#include "storage/buffer_pool.h"

namespace minidb {

BufferPool::BufferPool(FileManager& file_manager, size_t pool_size)
    : file_manager_(file_manager), pool_size_(pool_size) {
    
    pages_.resize(pool_size);
    
    // Initialize all frames as free
    for (size_t i = 0; i < pool_size; i++) {
        free_frames_.push_back(i);
    }
}

BufferPool::~BufferPool() {
    flushAllPages();
}

Page* BufferPool::fetchPage(PageId page_id) {
    // Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        pages_[frame_id].incrementPinCount();
        accessFrame(frame_id);
        return &pages_[frame_id];
    }
    
    // Find a frame to use
    size_t frame_id = findVictimFrame();
    if (frame_id == SIZE_MAX) {
        throw DatabaseException("Buffer pool is full and all pages are pinned");
    }
    
    // If frame has a page, evict it
    for (auto& [pid, fid] : page_table_) {
        if (fid == frame_id) {
            if (pages_[frame_id].isDirty()) {
                file_manager_.writePage(pid, pages_[frame_id]);
            }
            page_table_.erase(pid);
            break;
        }
    }
    
    // Load page from disk
    if (!file_manager_.readPage(page_id, pages_[frame_id])) {
        return nullptr;
    }
    
    pages_[frame_id].setPageId(page_id);
    pages_[frame_id].incrementPinCount();
    pages_[frame_id].setDirty(false);
    
    page_table_[page_id] = frame_id;
    accessFrame(frame_id);
    
    return &pages_[frame_id];
}

Page* BufferPool::newPage(PageId& page_id) {
    // Allocate a new page on disk
    page_id = file_manager_.allocatePage();
    
    // Find a frame for the new page
    size_t frame_id = findVictimFrame();
    if (frame_id == SIZE_MAX) {
        file_manager_.deallocatePage(page_id);
        throw DatabaseException("Buffer pool is full and all pages are pinned");
    }
    
    // Evict existing page if necessary
    for (auto& [pid, fid] : page_table_) {
        if (fid == frame_id) {
            if (pages_[frame_id].isDirty()) {
                file_manager_.writePage(pid, pages_[frame_id]);
            }
            page_table_.erase(pid);
            break;
        }
    }
    
    // Initialize the new page
    pages_[frame_id] = Page(page_id);
    pages_[frame_id].incrementPinCount();
    pages_[frame_id].setDirty(true);
    
    page_table_[page_id] = frame_id;
    accessFrame(frame_id);
    
    return &pages_[frame_id];
}

bool BufferPool::unpinPage(PageId page_id, bool is_dirty) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    
    size_t frame_id = it->second;
    pages_[frame_id].decrementPinCount();
    
    if (is_dirty) {
        pages_[frame_id].setDirty(true);
    }
    
    return true;
}

bool BufferPool::flushPage(PageId page_id) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    
    size_t frame_id = it->second;
    file_manager_.writePage(page_id, pages_[frame_id]);
    pages_[frame_id].setDirty(false);
    
    return true;
}

void BufferPool::flushAllPages() {
    for (auto& [page_id, frame_id] : page_table_) {
        if (pages_[frame_id].isDirty()) {
            file_manager_.writePage(page_id, pages_[frame_id]);
            pages_[frame_id].setDirty(false);
        }
    }
    file_manager_.flush();
}

bool BufferPool::deletePage(PageId page_id) {
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        if (pages_[frame_id].getPinCount() > 0) {
            return false;  // Cannot delete a pinned page
        }
        
        // Remove from LRU
        auto lru_it = lru_map_.find(frame_id);
        if (lru_it != lru_map_.end()) {
            lru_list_.erase(lru_it->second);
            lru_map_.erase(lru_it);
        }
        
        // Add frame back to free list
        free_frames_.push_back(frame_id);
        page_table_.erase(it);
    }
    
    file_manager_.deallocatePage(page_id);
    return true;
}

size_t BufferPool::findVictimFrame() {
    // First, try to use a free frame
    if (!free_frames_.empty()) {
        size_t frame_id = free_frames_.back();
        free_frames_.pop_back();
        return frame_id;
    }
    
    // Otherwise, use LRU eviction
    // Find least recently used unpinned page
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        size_t frame_id = *it;
        if (pages_[frame_id].getPinCount() == 0) {
            return frame_id;
        }
    }
    
    return SIZE_MAX;  // All pages are pinned
}

void BufferPool::accessFrame(size_t frame_id) {
    // Remove from current position in LRU list
    auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        lru_list_.erase(it->second);
    }
    
    // Add to front (most recently used)
    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
}

} // namespace minidb
