#pragma once

#include "common.h"
#include "storage/buffer_pool.h"

namespace minidb {

// Record ID: identifies a record by page and slot
struct RecordId {
    PageId page_id;
    SlotId slot_id;
    
    bool operator==(const RecordId& other) const {
        return page_id == other.page_id && slot_id == other.slot_id;
    }
    
    bool operator!=(const RecordId& other) const {
        return !(*this == other);
    }
};

// B+ Tree for indexing
// This is a simplified in-memory B+ tree for the initial implementation
class BTree {
public:
    explicit BTree(int order = 4);
    ~BTree();
    
    // Insert a key-value pair
    void insert(int64_t key, RecordId value);
    
    // Remove a key
    bool remove(int64_t key);
    
    // Search for a key
    std::optional<RecordId> search(int64_t key) const;
    
    // Range search
    std::vector<RecordId> rangeSearch(int64_t start, int64_t end) const;
    
    // Get all records (full scan)
    std::vector<RecordId> getAllRecords() const;
    
    // Check if tree is empty
    bool isEmpty() const { return root_ == nullptr; }
    
    // Clear the tree
    void clear();

private:
    struct Node {
        bool is_leaf;
        std::vector<int64_t> keys;
        std::vector<Node*> children;      // For internal nodes
        std::vector<RecordId> values;     // For leaf nodes
        Node* next;                        // For leaf nodes (linked list)
        Node* parent;
        
        Node(bool leaf) : is_leaf(leaf), next(nullptr), parent(nullptr) {}
    };
    
    Node* root_;
    int order_;  // Maximum number of children for internal nodes
    
    // Helper functions
    Node* findLeaf(int64_t key) const;
    void insertIntoLeaf(Node* leaf, int64_t key, RecordId value);
    void insertIntoParent(Node* left, int64_t key, Node* right);
    void splitLeaf(Node* leaf);
    void splitInternal(Node* node);
    
    Node* getLeftmostLeaf() const;
    void deleteTree(Node* node);
    
    // Delete helpers (underflow handling)
    void deleteFromLeaf(Node* leaf, size_t pos);
    void deleteFromInternal(Node* node, size_t pos);
    void handleUnderflow(Node* node);
    void borrowFromLeftSibling(Node* node, Node* left_sibling, Node* parent, size_t parent_idx);
    void borrowFromRightSibling(Node* node, Node* right_sibling, Node* parent, size_t parent_idx);
    void mergeWithLeftSibling(Node* node, Node* left_sibling, Node* parent, size_t parent_idx);
    void mergeWithRightSibling(Node* node, Node* right_sibling, Node* parent, size_t parent_idx);
    int getMinKeys() const { return (order_ - 1) / 2; }
    Node* getLeftSibling(Node* node, Node* parent, size_t& parent_idx);
    Node* getRightSibling(Node* node, Node* parent, size_t& parent_idx);
    void updateParentKey(Node* parent, size_t idx, int64_t new_key);
};

} // namespace minidb
