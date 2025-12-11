#pragma once

#include "common.h"
#include "storage/page.h"

namespace minidb {

// Manages reading/writing pages to disk
class FileManager {
public:
    explicit FileManager(const std::string& db_path);
    ~FileManager();
    
    // Page I/O
    bool readPage(PageId page_id, Page& page);
    bool writePage(PageId page_id, const Page& page);
    
    // Allocate a new page
    PageId allocatePage();
    
    // Deallocate a page (mark as free)
    void deallocatePage(PageId page_id);
    
    // Get number of pages
    PageId getNumPages() const { return num_pages_; }
    
    // Flush all data to disk
    void flush();
    
    // Check if database file exists
    bool exists() const;

private:
    std::string db_path_;
    std::fstream file_;
    PageId num_pages_ = 0;
    std::vector<PageId> free_pages_;
    
    void openOrCreate();
    void readHeader();
    void writeHeader();
};

} // namespace minidb
