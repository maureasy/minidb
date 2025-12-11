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
    deleteFromLeaf(leaf, pos);
    
    return true;
}

void BTree::deleteFromLeaf(Node* leaf, size_t pos) {
    leaf->keys.erase(leaf->keys.begin() + pos);
    leaf->values.erase(leaf->values.begin() + pos);
    
    // If this is root, handle specially
    if (leaf == root_) {
        if (leaf->keys.empty()) {
            delete root_;
            root_ = nullptr;
        }
        return;
    }
    
    // Check for underflow
    if (static_cast<int>(leaf->keys.size()) < getMinKeys()) {
        handleUnderflow(leaf);
    }
}

void BTree::deleteFromInternal(Node* node, size_t pos) {
    node->keys.erase(node->keys.begin() + pos);
    node->children.erase(node->children.begin() + pos + 1);
    
    // If this is root
    if (node == root_) {
        if (node->keys.empty()) {
            // Root has no keys, make the only child the new root
            if (!node->children.empty()) {
                root_ = node->children[0];
                root_->parent = nullptr;
                delete node;
            } else {
                delete root_;
                root_ = nullptr;
            }
        }
        return;
    }
    
    // Check for underflow
    if (static_cast<int>(node->keys.size()) < getMinKeys()) {
        handleUnderflow(node);
    }
}

BTree::Node* BTree::getLeftSibling(Node* node, Node* parent, size_t& parent_idx) {
    for (size_t i = 0; i < parent->children.size(); i++) {
        if (parent->children[i] == node) {
            parent_idx = i;
            if (i > 0) {
                return parent->children[i - 1];
            }
            return nullptr;
        }
    }
    return nullptr;
}

BTree::Node* BTree::getRightSibling(Node* node, Node* parent, size_t& parent_idx) {
    for (size_t i = 0; i < parent->children.size(); i++) {
        if (parent->children[i] == node) {
            parent_idx = i;
            if (i < parent->children.size() - 1) {
                return parent->children[i + 1];
            }
            return nullptr;
        }
    }
    return nullptr;
}

void BTree::handleUnderflow(Node* node) {
    Node* parent = node->parent;
    if (!parent) return;  // Root doesn't have underflow issues handled here
    
    size_t parent_idx = 0;
    Node* left_sibling = getLeftSibling(node, parent, parent_idx);
    Node* right_sibling = getRightSibling(node, parent, parent_idx);
    
    // Try to borrow from left sibling
    if (left_sibling && static_cast<int>(left_sibling->keys.size()) > getMinKeys()) {
        borrowFromLeftSibling(node, left_sibling, parent, parent_idx);
        return;
    }
    
    // Try to borrow from right sibling
    if (right_sibling && static_cast<int>(right_sibling->keys.size()) > getMinKeys()) {
        borrowFromRightSibling(node, right_sibling, parent, parent_idx);
        return;
    }
    
    // Must merge - prefer merging with left sibling
    if (left_sibling) {
        mergeWithLeftSibling(node, left_sibling, parent, parent_idx);
    } else if (right_sibling) {
        mergeWithRightSibling(node, right_sibling, parent, parent_idx);
    }
}

void BTree::borrowFromLeftSibling(Node* node, Node* left_sibling, Node* parent, size_t parent_idx) {
    if (node->is_leaf) {
        // Move last key/value from left sibling to front of node
        node->keys.insert(node->keys.begin(), left_sibling->keys.back());
        node->values.insert(node->values.begin(), left_sibling->values.back());
        left_sibling->keys.pop_back();
        left_sibling->values.pop_back();
        
        // Update parent key
        parent->keys[parent_idx - 1] = node->keys[0];
    } else {
        // Internal node: bring down parent key, move up sibling's last key
        node->keys.insert(node->keys.begin(), parent->keys[parent_idx - 1]);
        parent->keys[parent_idx - 1] = left_sibling->keys.back();
        left_sibling->keys.pop_back();
        
        // Move last child from left sibling
        Node* moved_child = left_sibling->children.back();
        left_sibling->children.pop_back();
        node->children.insert(node->children.begin(), moved_child);
        moved_child->parent = node;
    }
}

void BTree::borrowFromRightSibling(Node* node, Node* right_sibling, Node* parent, size_t parent_idx) {
    if (node->is_leaf) {
        // Move first key/value from right sibling to end of node
        node->keys.push_back(right_sibling->keys.front());
        node->values.push_back(right_sibling->values.front());
        right_sibling->keys.erase(right_sibling->keys.begin());
        right_sibling->values.erase(right_sibling->values.begin());
        
        // Update parent key
        parent->keys[parent_idx] = right_sibling->keys[0];
    } else {
        // Internal node: bring down parent key, move up sibling's first key
        node->keys.push_back(parent->keys[parent_idx]);
        parent->keys[parent_idx] = right_sibling->keys.front();
        right_sibling->keys.erase(right_sibling->keys.begin());
        
        // Move first child from right sibling
        Node* moved_child = right_sibling->children.front();
        right_sibling->children.erase(right_sibling->children.begin());
        node->children.push_back(moved_child);
        moved_child->parent = node;
    }
}

void BTree::mergeWithLeftSibling(Node* node, Node* left_sibling, Node* parent, size_t parent_idx) {
    if (node->is_leaf) {
        // Move all keys/values from node to left sibling
        for (size_t i = 0; i < node->keys.size(); i++) {
            left_sibling->keys.push_back(node->keys[i]);
            left_sibling->values.push_back(node->values[i]);
        }
        
        // Update linked list
        left_sibling->next = node->next;
    } else {
        // Internal node: bring down parent key, then merge
        left_sibling->keys.push_back(parent->keys[parent_idx - 1]);
        
        for (size_t i = 0; i < node->keys.size(); i++) {
            left_sibling->keys.push_back(node->keys[i]);
        }
        for (size_t i = 0; i < node->children.size(); i++) {
            left_sibling->children.push_back(node->children[i]);
            node->children[i]->parent = left_sibling;
        }
    }
    
    delete node;
    
    // Remove key and child pointer from parent
    parent->keys.erase(parent->keys.begin() + parent_idx - 1);
    parent->children.erase(parent->children.begin() + parent_idx);
    
    // Handle parent underflow
    if (parent == root_) {
        if (parent->keys.empty()) {
            root_ = left_sibling;
            root_->parent = nullptr;
            delete parent;
        }
    } else if (static_cast<int>(parent->keys.size()) < getMinKeys()) {
        handleUnderflow(parent);
    }
}

void BTree::mergeWithRightSibling(Node* node, Node* right_sibling, Node* parent, size_t parent_idx) {
    if (node->is_leaf) {
        // Move all keys/values from right sibling to node
        for (size_t i = 0; i < right_sibling->keys.size(); i++) {
            node->keys.push_back(right_sibling->keys[i]);
            node->values.push_back(right_sibling->values[i]);
        }
        
        // Update linked list
        node->next = right_sibling->next;
    } else {
        // Internal node: bring down parent key, then merge
        node->keys.push_back(parent->keys[parent_idx]);
        
        for (size_t i = 0; i < right_sibling->keys.size(); i++) {
            node->keys.push_back(right_sibling->keys[i]);
        }
        for (size_t i = 0; i < right_sibling->children.size(); i++) {
            node->children.push_back(right_sibling->children[i]);
            right_sibling->children[i]->parent = node;
        }
    }
    
    delete right_sibling;
    
    // Remove key and child pointer from parent
    parent->keys.erase(parent->keys.begin() + parent_idx);
    parent->children.erase(parent->children.begin() + parent_idx + 1);
    
    // Handle parent underflow
    if (parent == root_) {
        if (parent->keys.empty()) {
            root_ = node;
            root_->parent = nullptr;
            delete parent;
        }
    } else if (static_cast<int>(parent->keys.size()) < getMinKeys()) {
        handleUnderflow(parent);
    }
}

void BTree::updateParentKey(Node* parent, size_t idx, int64_t new_key) {
    if (parent && idx < parent->keys.size()) {
        parent->keys[idx] = new_key;
    }
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
