#pragma once

#include "common.h"
#include "storage/page.h"
#include "storage/file_manager.h"
#include <list>

namespace minidb {

// Buffer pool manages pages in memory with LRU eviction
class BufferPool {
public:
    BufferPool(FileManager& file_manager, size_t pool_size = 64);
    ~BufferPool();
    
    // Fetch a page (loads from disk if not in memory)
    Page* fetchPage(PageId page_id);
    
    // Allocate a new page
    Page* newPage(PageId& page_id);
    
    // Unpin a page (allow it to be evicted)
    bool unpinPage(PageId page_id, bool is_dirty);
    
    // Flush a specific page to disk
    bool flushPage(PageId page_id);
    
    // Flush all pages to disk
    void flushAllPages();
    
    // Delete a page from buffer pool and disk
    bool deletePage(PageId page_id);

private:
    FileManager& file_manager_;
    size_t pool_size_;
    
    // Thread safety
    mutable std::mutex mutex_;
    
    // Page storage
    std::vector<Page> pages_;
    
    // Page table: page_id -> frame index
    std::unordered_map<PageId, size_t> page_table_;
    
    // LRU list: front = most recently used, back = least recently used
    std::list<size_t> lru_list_;
    std::unordered_map<size_t, std::list<size_t>::iterator> lru_map_;
    
    // Free frames
    std::vector<size_t> free_frames_;
    
    // Find a victim frame for eviction
    size_t findVictimFrame();
    
    // Update LRU for a frame
    void accessFrame(size_t frame_id);
};

} // namespace minidb
