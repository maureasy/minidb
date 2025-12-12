#include "storage/file_manager.h"
#include <filesystem>

namespace minidb {

// File header constants
constexpr uint32_t MAGIC_NUMBER = 0x4D494E49;  // "MINI"
constexpr uint32_t VERSION = 1;
constexpr size_t HEADER_SIZE = 64;
constexpr size_t MAX_FREE_LIST_ENTRIES = 1024;  // Fixed allocation for free list
constexpr size_t FREE_LIST_SIZE = MAX_FREE_LIST_ENTRIES * sizeof(PageId);

FileManager::FileManager(const std::string& db_path) : db_path_(db_path) {
    openOrCreate();
}

FileManager::~FileManager() {
    flush();
    if (file_.is_open()) {
        file_.close();
    }
}

void FileManager::openOrCreate() {
    bool file_exists = std::filesystem::exists(db_path_);
    
    if (file_exists) {
        file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (file_.is_open()) {
            readHeader();
        }
    } else {
        // Create new file
        file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
        if (file_.is_open()) {
            num_pages_ = 0;
            writeHeader();
        }
    }
    
    if (!file_.is_open()) {
        throw DatabaseException("Failed to open database file: " + db_path_);
    }
}

void FileManager::readHeader() {
    char header[HEADER_SIZE];
    file_.seekg(0);
    file_.read(header, HEADER_SIZE);
    
    uint32_t magic;
    std::memcpy(&magic, header, sizeof(uint32_t));
    if (magic != MAGIC_NUMBER) {
        throw DatabaseException("Invalid database file format");
    }
    
    uint32_t version;
    std::memcpy(&version, header + 4, sizeof(uint32_t));
    if (version != VERSION) {
        throw DatabaseException("Unsupported database version");
    }
    
    std::memcpy(&num_pages_, header + 8, sizeof(PageId));
    
    // Read free pages list
    uint32_t num_free;
    std::memcpy(&num_free, header + 12, sizeof(uint32_t));
    free_pages_.clear();
    
    // Free pages are stored in fixed area after the header
    if (num_free > 0 && num_free <= MAX_FREE_LIST_ENTRIES) {
        file_.seekg(HEADER_SIZE);
        for (uint32_t i = 0; i < num_free; i++) {
            PageId pid;
            file_.read(reinterpret_cast<char*>(&pid), sizeof(PageId));
            free_pages_.push_back(pid);
        }
    }
}

void FileManager::writeHeader() {
    char header[HEADER_SIZE] = {0};
    
    std::memcpy(header, &MAGIC_NUMBER, sizeof(uint32_t));
    std::memcpy(header + 4, &VERSION, sizeof(uint32_t));
    std::memcpy(header + 8, &num_pages_, sizeof(PageId));
    
    uint32_t num_free = static_cast<uint32_t>(free_pages_.size());
    std::memcpy(header + 12, &num_free, sizeof(uint32_t));
    
    file_.seekp(0);
    file_.write(header, HEADER_SIZE);
    
    // Write free pages list (always write to fixed location)
    file_.seekp(HEADER_SIZE);
    for (size_t i = 0; i < free_pages_.size() && i < MAX_FREE_LIST_ENTRIES; i++) {
        file_.write(reinterpret_cast<const char*>(&free_pages_[i]), sizeof(PageId));
    }
    
    file_.flush();
}

bool FileManager::readPage(PageId page_id, Page& page) {
    if (page_id >= num_pages_) {
        return false;
    }
    
    // Calculate offset (header + fixed free list area + pages)
    size_t page_offset = HEADER_SIZE + FREE_LIST_SIZE + (static_cast<size_t>(page_id) * PAGE_SIZE);
    
    char buffer[PAGE_SIZE];
    file_.seekg(page_offset);
    file_.read(buffer, PAGE_SIZE);
    
    if (file_.gcount() != PAGE_SIZE) {
        return false;
    }
    
    page.deserialize(buffer);
    page.setPageId(page_id);
    return true;
}

bool FileManager::writePage(PageId page_id, const Page& page) {
    if (page_id > num_pages_) {
        return false;
    }
    
    // Calculate offset (header + fixed free list area + pages)
    size_t page_offset = HEADER_SIZE + FREE_LIST_SIZE + (static_cast<size_t>(page_id) * PAGE_SIZE);
    
    char buffer[PAGE_SIZE];
    page.serialize(buffer);
    
    file_.seekp(page_offset);
    file_.write(buffer, PAGE_SIZE);
    file_.flush();
    
    return true;
}

PageId FileManager::allocatePage() {
    PageId new_page_id;
    
    if (!free_pages_.empty()) {
        // Reuse a free page
        new_page_id = free_pages_.back();
        free_pages_.pop_back();
    } else {
        // Allocate new page at end
        new_page_id = num_pages_++;
    }
    
    // Initialize the new page
    Page new_page(new_page_id);
    writePage(new_page_id, new_page);
    writeHeader();
    
    return new_page_id;
}

void FileManager::deallocatePage(PageId page_id) {
    if (page_id < num_pages_) {
        free_pages_.push_back(page_id);
        writeHeader();
    }
}

void FileManager::flush() {
    if (file_.is_open()) {
        writeHeader();
        file_.flush();
    }
}

bool FileManager::exists() const {
    return std::filesystem::exists(db_path_);
}

} // namespace minidb
