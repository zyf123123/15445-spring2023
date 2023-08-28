//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "concurrency/transaction.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size, page_id_t page_id) {
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index <= GetSize());
  if (index == GetSize()) {
    return array_[index - 1].second;
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndexSearch(const KeyType &key, KeyComparator comparator) const -> int {
  if (GetSize() == 0) {
    // first key in index 0
    return 0;
  }
  // search from the second element
  auto res = std::upper_bound(
      array_, array_ + GetSize(), MappingType(key, ValueType()),
      [&comparator](MappingType a, MappingType b) -> bool { return comparator(a.first, b.first) < 0; });
  if (res == array_) {
    return 0;
  }
  return res - array_ - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndexInsert(const KeyType &key, KeyComparator comparator) const -> int {
  if (GetSize() == 0) {
    // first key in index 0
    return 0;
  }
  // search from the second element
  int res = std::upper_bound(
                array_, array_ + GetSize(), MappingType(key, ValueType()),
                [&comparator](MappingType a, MappingType b) -> bool { return comparator(a.first, b.first) < 0; }) -
            array_;
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator comparator) -> bool {
  int index = KeyIndexInsert(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    return false;
  }

  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = MappingType(key, value);

  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(KeyType key, KeyComparator comparator) -> bool {
  int index = KeyIndexSearch(key, comparator);
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient) -> void {
  int start = GetSize() / 2;
  int end = GetSize();
  for (int i = start; i < end; i++) {
    recipient->array_[i - start] = array_[i];
  }
  recipient->IncreaseSize(end - start);
  IncreaseSize(-end + start);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
