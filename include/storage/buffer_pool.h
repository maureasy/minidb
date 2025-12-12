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
    
    // Discard a page from buffer pool (reload from disk on next fetch)
    // Used for transaction abort to discard uncommitted changes
    bool discardPage(PageId page_id);

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

// RAII helper for automatic page unpinning
class PageGuard {
public:
    PageGuard(BufferPool& pool, PageId page_id, Page* page)
        : pool_(pool), page_id_(page_id), page_(page), dirty_(false) {}
    
    ~PageGuard() {
        if (page_) {
            pool_.unpinPage(page_id_, dirty_);
        }
    }
    
    // Non-copyable
    PageGuard(const PageGuard&) = delete;
    PageGuard& operator=(const PageGuard&) = delete;
    
    // Movable
    PageGuard(PageGuard&& other) noexcept
        : pool_(other.pool_), page_id_(other.page_id_), 
          page_(other.page_), dirty_(other.dirty_) {
        other.page_ = nullptr;
    }
    
    PageGuard& operator=(PageGuard&& other) noexcept {
        if (this != &other) {
            if (page_) {
                pool_.unpinPage(page_id_, dirty_);
            }
            page_id_ = other.page_id_;
            page_ = other.page_;
            dirty_ = other.dirty_;
            other.page_ = nullptr;
        }
        return *this;
    }
    
    Page* get() const { return page_; }
    Page* operator->() const { return page_; }
    Page& operator*() const { return *page_; }
    
    void setDirty(bool dirty = true) { dirty_ = dirty; }
    bool isDirty() const { return dirty_; }
    
    // Release ownership without unpinning
    Page* release() {
        Page* p = page_;
        page_ = nullptr;
        return p;
    }
    
    explicit operator bool() const { return page_ != nullptr; }

private:
    BufferPool& pool_;
    PageId page_id_;
    Page* page_;
    bool dirty_;
};

} // namespace minidb
