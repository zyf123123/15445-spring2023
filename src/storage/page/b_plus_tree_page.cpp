//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return (page_type_ == IndexPageType::LEAF_PAGE); }
void BPlusTreePage::SetPageType(IndexPageType page_type) {
  assert(page_type == IndexPageType::LEAF_PAGE || page_type == IndexPageType::INTERNAL_PAGE);
  page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) {
  assert(size >= 0 && size <= GetMaxSize());
  size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
  assert(size_ + amount >= 0 && size_ + amount <= GetMaxSize());
  size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) {
  assert(size >= 0);
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int { return GetMaxSize() / 2 + GetMaxSize() % 2; }

auto BPlusTreePage::GetPageId() const -> page_id_t { return page_id_; }

void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

}  // namespace bustub
