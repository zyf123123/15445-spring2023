#include "primer/trie.h"
#include <queue>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  int n = key.length();
  std::shared_ptr<const TrieNode> t = root_;
  // std::cout << root_ << root_->children_.empty() << std::endl;
  if (n == 0) {
    char c = '\0';
    if (t->children_.count(c) != 0) {
      t = t->children_.at(c);
      // std::cout << c << std::endl;
    } else {
      return nullptr;
    }
  }
  for (int i = 0; i < n; i++) {
    const char c = key[i];
    if (t && t->children_.count(c) != 0) {
      t = t->children_.at(c);
      // std::cout << c << std::endl;
    } else {
      return nullptr;
    }
  }

  auto res = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(t);

  if (res == nullptr || !res->is_value_node_) {
    return nullptr;
  }

  // std::cout << res->value_ << std::endl;

  return const_cast<T *>(&(*(res->value_)));

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  int n = key.length();
  Trie newt;
  std::queue<std::shared_ptr<TrieNode>> queue;

  if (root_ == nullptr) {
    // std::cout << "root is null" <<  std::endl;
    std::shared_ptr<const TrieNode> newr = std::make_shared<const TrieNode>();
    newt = Trie(newr);
  } else {
    newt = Trie((*root_).Clone());
  }

  std::shared_ptr<TrieNode> t = std::const_pointer_cast<TrieNode>(newt.root_);
  queue.push(t);

  for (int i = 0; i < n - 1; i++) {
    char c = key[i];
    if (t->children_.count(c) != 0) {
      std::unique_ptr<TrieNode> tchild = (*(t->children_[c])).Clone();
      t = std::shared_ptr<TrieNode>(std::move(tchild));
      // t = std::shared_ptr<TrieNode>(tchild_);
      // std::cout << "already exist  " << c << " at " << t << std::endl;
    } else {
      std::shared_ptr<TrieNode> node = std::make_shared<TrieNode>();
      (*t).children_[c] = node;
      // std::cout << "insert " << c << " at " << node << std::endl;
      t = node;
    }
    queue.push(t);
    // std::cout << *(newt.root_->children_);
  }

  for (int i = 0; i < n - 1; i++) {
    char c = key[i];
    auto node = queue.front();
    queue.pop();
    auto nextnode = queue.front();
    node->children_[c] = nextnode;
  }

  char c;
  if (n == 0) {
    c = '\0';
  } else {
    c = key[n - 1];
  }
  // std::unique_ptr<TrieNode> v = std::move(value);
  // (*t).children_[c] = leaf;

  auto node = queue.front();
  if (node->children_.count(c) == 0) {
    TrieNodeWithValue<T> vnode =
        TrieNodeWithValue<T>(std::map<char, std::shared_ptr<const TrieNode>>(), std::make_shared<T>(std::move(value)));
    std::shared_ptr<TrieNode> leaf = std::make_shared<TrieNodeWithValue<T>>(vnode);
    node->children_[c] = leaf;
  } else {
    std::map<char, std::shared_ptr<const TrieNode>> leafchild = (node->children_[c]->children_);
    TrieNodeWithValue<T> vnode = TrieNodeWithValue<T>(leafchild, std::make_shared<T>(std::move(value)));
    std::shared_ptr<TrieNode> leaf = std::make_shared<TrieNodeWithValue<T>>(vnode);
    node->children_[c] = leaf;
  }
  queue.pop();

  // (*t).children_.insert(std::make_pair(c, leaf));

  // std::cout << ((*t).children_.at(c))<< std::endl;

  return newt;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  int n = key.length();
  Trie newt;

  if (root_ == nullptr) {
    // std::cout << "root is null" << std::endl;
    std::shared_ptr<TrieNode> newr = std::make_shared<TrieNode>();
    newt = Trie(newr);
  } else {
    newt = Trie((*root_).Clone());
  }
  std::queue<std::shared_ptr<TrieNode>> queue;
  std::shared_ptr<TrieNode> t = std::const_pointer_cast<TrieNode>(newt.root_);
  queue.push(t);

  for (int i = 0; i < n - 1; i++) {
    const char c = key[i];
    if (t->children_.count(c) != 0) {
      std::unique_ptr<TrieNode> tchild = (*(t->children_[c])).Clone();
      t = std::shared_ptr<TrieNode>(std::move(tchild));
      queue.push(t);
    } else {
      return newt;
    }
  }

  for (int i = 0; i < n - 1; i++) {
    char c = key[i];
    auto node = queue.front();
    queue.pop();
    auto nextnode = queue.front();
    node->children_[c] = nextnode;
  }

  char c;
  if (n == 0) {
    c = '\0';
  } else {
    c = key[n - 1];
  }

  auto node = queue.front();
  if (node->children_.empty()) {
    std::shared_ptr<TrieNode> leaf = std::make_shared<TrieNode>();
    node->children_[c] = leaf;
  } else {
    std::map<char, std::shared_ptr<const TrieNode>> leafchild = (node->children_[c]->children_);
    TrieNode vnode = TrieNode(leafchild);
    std::shared_ptr<TrieNode> leaf = std::make_shared<TrieNode>(vnode);
    node->children_[c] = leaf;
  }
  queue.pop();

  return newt;

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
