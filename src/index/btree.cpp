#include "index/btree.h"

namespace minidb {

BTree::BTree(int order) : root_(nullptr), order_(order) {
    if (order_ < 3) order_ = 3;  // Minimum order
}

BTree::~BTree() {
    clear();
}

void BTree::clear() {
    if (root_) {
        deleteTree(root_);
        root_ = nullptr;
    }
}

void BTree::deleteTree(Node* node) {
    if (!node) return;
    
    if (!node->is_leaf) {
        for (Node* child : node->children) {
            deleteTree(child);
        }
    }
    delete node;
}

BTree::Node* BTree::findLeaf(int64_t key) const {
    if (!root_) return nullptr;
    
    Node* current = root_;
    while (!current->is_leaf) {
        size_t i = 0;
        while (i < current->keys.size() && key >= current->keys[i]) {
            i++;
        }
        current = current->children[i];
    }
    return current;
}

void BTree::insert(int64_t key, RecordId value) {
    if (!root_) {
        // Create first leaf node
        root_ = new Node(true);
        root_->keys.push_back(key);
        root_->values.push_back(value);
        return;
    }
    
    Node* leaf = findLeaf(key);
    insertIntoLeaf(leaf, key, value);
    
    // Check if leaf needs to split
    if (leaf->keys.size() >= static_cast<size_t>(order_)) {
        splitLeaf(leaf);
    }
}

void BTree::insertIntoLeaf(Node* leaf, int64_t key, RecordId value) {
    // Find position to insert
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    size_t pos = it - leaf->keys.begin();
    
    // Check for duplicate key - update value if exists
    if (it != leaf->keys.end() && *it == key) {
        leaf->values[pos] = value;
        return;
    }
    
    // Insert key and value
    leaf->keys.insert(leaf->keys.begin() + pos, key);
    leaf->values.insert(leaf->values.begin() + pos, value);
}

void BTree::splitLeaf(Node* leaf) {
    Node* new_leaf = new Node(true);
    
    // Calculate split point
    size_t mid = leaf->keys.size() / 2;
    
    // Move half the keys and values to new leaf
    new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
    new_leaf->values.assign(leaf->values.begin() + mid, leaf->values.end());
    
    leaf->keys.resize(mid);
    leaf->values.resize(mid);
    
    // Update linked list pointers
    new_leaf->next = leaf->next;
    leaf->next = new_leaf;
    
    // Insert into parent
    int64_t separator = new_leaf->keys[0];
    insertIntoParent(leaf, separator, new_leaf);
}

void BTree::insertIntoParent(Node* left, int64_t key, Node* right) {
    if (left == root_) {
        // Create new root
        Node* new_root = new Node(false);
        new_root->keys.push_back(key);
        new_root->children.push_back(left);
        new_root->children.push_back(right);
        left->parent = new_root;
        right->parent = new_root;
        root_ = new_root;
        return;
    }
    
    Node* parent = left->parent;
    
    // Find position in parent
    auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), key);
    size_t pos = it - parent->keys.begin();
    
    // Insert key and child pointer
    parent->keys.insert(parent->keys.begin() + pos, key);
    parent->children.insert(parent->children.begin() + pos + 1, right);
    right->parent = parent;
    
    // Check if parent needs to split
    if (parent->keys.size() >= static_cast<size_t>(order_)) {
        splitInternal(parent);
    }
}

void BTree::splitInternal(Node* node) {
    Node* new_node = new Node(false);
    
    size_t mid = node->keys.size() / 2;
    int64_t separator = node->keys[mid];
    
    // Move keys after mid to new node
    new_node->keys.assign(node->keys.begin() + mid + 1, node->keys.end());
    
    // Move children after mid to new node
    new_node->children.assign(node->children.begin() + mid + 1, node->children.end());
    
    // Update parent pointers
    for (Node* child : new_node->children) {
        child->parent = new_node;
    }
    
    node->keys.resize(mid);
    node->children.resize(mid + 1);
    
    insertIntoParent(node, separator, new_node);
}

bool BTree::remove(int64_t key) {
    if (!root_) return false;
    
    Node* leaf = findLeaf(key);
    if (!leaf) return false;
    
    // Find the key in the leaf
    auto it = std::find(leaf->keys.begin(), leaf->keys.end(), key);
    if (it == leaf->keys.end()) return false;
    
    size_t pos = it - leaf->keys.begin();
    leaf->keys.erase(leaf->keys.begin() + pos);
    leaf->values.erase(leaf->values.begin() + pos);
    
    // Note: This is a simplified delete that doesn't handle underflow
    // A full implementation would merge/redistribute nodes
    
    // If root becomes empty
    if (leaf == root_ && leaf->keys.empty()) {
        delete root_;
        root_ = nullptr;
    }
    
    return true;
}

std::optional<RecordId> BTree::search(int64_t key) const {
    Node* leaf = findLeaf(key);
    if (!leaf) return std::nullopt;
    
    auto it = std::find(leaf->keys.begin(), leaf->keys.end(), key);
    if (it == leaf->keys.end()) return std::nullopt;
    
    size_t pos = it - leaf->keys.begin();
    return leaf->values[pos];
}

std::vector<RecordId> BTree::rangeSearch(int64_t start, int64_t end) const {
    std::vector<RecordId> results;
    
    Node* leaf = findLeaf(start);
    if (!leaf) return results;
    
    // Scan through leaves
    while (leaf) {
        for (size_t i = 0; i < leaf->keys.size(); i++) {
            if (leaf->keys[i] >= start && leaf->keys[i] <= end) {
                results.push_back(leaf->values[i]);
            } else if (leaf->keys[i] > end) {
                return results;
            }
        }
        leaf = leaf->next;
    }
    
    return results;
}

BTree::Node* BTree::getLeftmostLeaf() const {
    if (!root_) return nullptr;
    
    Node* current = root_;
    while (!current->is_leaf) {
        current = current->children[0];
    }
    return current;
}

std::vector<RecordId> BTree::getAllRecords() const {
    std::vector<RecordId> results;
    
    Node* leaf = getLeftmostLeaf();
    while (leaf) {
        for (const auto& value : leaf->values) {
            results.push_back(value);
        }
        leaf = leaf->next;
    }
    
    return results;
}

} // namespace minidb
