//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size, page_id_t page_id) {
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0);
  assert(index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KVPairsAt(int index) const -> const MappingType & {
  assert(index >= 0 && index < GetSize());
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, KeyComparator comparator) const -> int {
  int res = std::lower_bound(
                array_, array_ + GetSize(), MappingType(key, ValueType()),
                [&comparator](MappingType a, MappingType b) -> bool { return comparator(a.first, b.first) < 0; }) -
            array_;
  if (res == GetSize()) {
    return -1;
  }
  if (comparator(array_[res].first, key) != 0) {
    return -1;
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndexInsert(const KeyType &key, KeyComparator comparator) const -> int {
  int res = std::lower_bound(
                array_, array_ + GetSize(), MappingType(key, ValueType()),
                [&comparator](MappingType a, MappingType b) -> bool { return comparator(a.first, b.first) < 0; }) -
            array_;
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator) -> bool {
  int index = KeyIndexInsert(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    // already exists
    return false;
  }
  for (int i = GetSize(); i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) -> void {
  int start = GetSize() / 2 + GetSize() % 2;
  int end = GetSize();
  for (int i = start; i < end; i++) {
    recipient->array_[i - start] = array_[i];
    // std::cout << recipient->array_[i - start].first.ToString() << recipient->array_[i - start].second.ToString()<<
    // std::endl;
  }
  recipient->IncreaseSize(end - start);
  IncreaseSize(-end + start);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFront(BPlusTreeLeafPage *recipient) -> void {
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) -> void {
  int index = KeyIndex(key, comparator);
  if (index == -1) {
    return;
  }
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
