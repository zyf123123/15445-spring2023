/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page_id, int pair_index, BufferPoolManager *bpm)
    : leaf_page_id_(leaf_page_id), pair_index_(pair_index), bpm_(bpm) {
  if (leaf_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  leaf_page_guard_ = bpm_->FetchPageRead(leaf_page_id_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto leaf_page = leaf_page_guard_.template As<LeafPage>();
  // std::cout << leaf_page->KVPairsAt(pair_index_).first << " " << leaf_page->GetPageId() << " "
  //          << leaf_page->GetNextPageId() << std::endl;
  return leaf_page->KVPairsAt(pair_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto leaf_page = leaf_page_guard_.template As<LeafPage>();
  if (pair_index_ == leaf_page->GetSize() - 1) {
    leaf_page_id_ = leaf_page->GetNextPageId();
    pair_index_ = 0;
    if (leaf_page_id_ == INVALID_PAGE_ID) {
      return *this;
    }
    leaf_page_guard_ = bpm_->FetchPageRead(leaf_page_id_);
  } else {
    pair_index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
