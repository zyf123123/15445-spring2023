#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.template As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*
 * Helper function to find the leaf page of the input key.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) const -> page_id_t {
  if (IsEmpty()) {
    return INVALID_PAGE_ID;
  }
  auto root_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = root_guard.template As<BPlusTreePage>();
  // find the leaf page
  while (!root_page->IsLeafPage()) {
    auto internal_page = root_guard.template As<InternalPage>();
    int index = internal_page->KeyIndex(key, comparator_);
    if (index == -1) {
      return INVALID_PAGE_ID;
    }
    page_id_t child_id = internal_page->ValueAt(index);
    root_guard = bpm_->FetchPageRead(child_id);
    root_page = root_guard.template As<BPlusTreePage>();
  }

  return root_guard.PageId();
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.

  if (IsEmpty()) {
    return false;
  }
  auto root_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = root_guard.template As<BPlusTreePage>();
  // find the leaf page
  while (!root_page->IsLeafPage()) {
    auto internal_page = root_guard.template As<InternalPage>();
    int index = internal_page->KeyIndex(key, comparator_);
    if (index == -1) {
      return false;
    }
    page_id_t child_id = internal_page->ValueAt(index);
    root_guard = bpm_->FetchPageRead(child_id);
    root_page = root_guard.template As<BPlusTreePage>();
  }
  auto leaf_page = root_guard.template As<LeafPage>();
  int index = leaf_page->KeyIndex(key, comparator_);
  if (index == -1) {
    return false;
  }
  if (result != nullptr) {
    result->push_back(leaf_page->ValueAt(index));
  }
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.root_page_id_ = GetRootPageId();
  // std::cout << "insert key" << key;
  // LOG_INFO("insert key: header page id %d", header_page_id_);
  // if empty, start new tree
  latch_.lock();
  if (IsEmpty()) {
    auto leaf_guard = bpm_->NewPageGuarded(&ctx.root_page_id_);
    auto leaf = leaf_guard.AsMut<LeafPage>();
    this->header_page_id_ = ctx.root_page_id_;
    latch_.unlock();
    leaf->Init(leaf_max_size_, ctx.root_page_id_);
    // LOG_INFO("insert key: leaf page id %d", leaf->GetPageId());
    // LOG_INFO("Create new root: %d", ctx.root_page_id_);
    return leaf->Insert(key, value, comparator_);
  }
  latch_.unlock();
  WritePageGuard root_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  while (root_guard.PageId() != GetRootPageId()) {
    root_guard.Drop();
    // root page id may changed
    ctx.root_page_id_ = GetRootPageId();
    root_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  }

  // LOG_INFO("Fetch write guard: %d", header_page_id_);
  auto root_page = root_guard.template AsMut<BPlusTreePage>();
  // ctx.header_page_ = std::move(root_guard);
  // find leaf page
  int depth = -1;
  while (!root_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(root_page);
    int index = internal_page->KeyIndex(key, comparator_);
    if (index == -1) {
      return false;
    }
    page_id_t child_id = internal_page->ValueAt(index);
    ctx.write_set_.push_back(std::move(root_guard));
    root_guard = bpm_->FetchPageWrite(child_id);
    // LOG_INFO("Fetch write guard: %d", child_id);
    root_page = root_guard.template AsMut<BPlusTreePage>();
    depth++;
  }

  // page_id_t leaf_page_id = root_guard.PageId();
  // auto leaf_guard = bpm_->FetchPageWrite(leaf_page_id);
  auto leaf = root_guard.template AsMut<LeafPage>();
  if (leaf->KeyIndex(key, comparator_) != -1) {
    // has value
    return false;
  }
  // has less than n-1 key values
  if (leaf->GetSize() < leaf->GetMaxSize() - 1) {
    // LOG_INFO("insert key: leaf page id %d", leaf->GetPageId());
    // LOG_INFO("Drop write guard: %d", leaf->GetPageId());
    bool success = leaf->Insert(key, value, comparator_);
    if (depth > 0) {
      auto parent_page = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());
      int index = parent_page->KeyIndex(key, comparator_);
      parent_page->SetKeyAt(index, leaf->KeyAt(0));
    }
    return success;
  }
  // split
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto new_page = bpm_->NewPageGuarded(&new_page_id);
  // LOG_INFO("Create new page: %d", new_page_id);
  auto new_leaf = new_page.AsMut<LeafPage>();
  // auto *new_leaf = reinterpret_cast<LeafPage *>(bpm_->NewPage(&new_page_id)->GetData());
  new_leaf->Init(leaf_max_size_, new_page_id);
  KeyType remove_key = leaf->KeyAt(0);

  leaf->MoveHalfTo(new_leaf);
  if (new_leaf->GetSize() == 0 && comparator_(key, leaf->KeyAt(leaf->GetSize() - 1)) < 0) {
    // key < leaf[0] will be insert at left page
    leaf->MoveLastToFront(new_leaf);
  }
  // insert into right page
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());

  bool success;
  if ((leaf->GetSize() == 0) || (new_leaf->GetSize() != 0 && comparator_(key, new_leaf->KeyAt(0)) < 0)) {
    // insert into left page
    success = leaf->Insert(key, value, comparator_);
  } else {
    // insert into right page
    success = new_leaf->Insert(key, value, comparator_);
  }

  if (!success) {
    return false;
  }

  if (leaf->GetPageId() == GetRootPageId()) {
    // create new root
    // auto new_root = reinterpret_cast<InternalPage *>(bpm_->NewPage(&new_page_id)->GetData());
    new_page = bpm_->NewPageGuarded(&new_page_id);
    auto new_root = new_page.AsMut<InternalPage>();

    new_root->Init(internal_max_size_, new_page_id);
    ctx.root_page_id_ = new_page_id;
    // LOG_INFO("Create new root: %d", new_page_id);
    SetRootPageId(ctx.root_page_id_);

    success = new_root->Insert(leaf->KeyAt(0), leaf->GetPageId(), comparator_);
    success |= new_root->Insert(new_leaf->KeyAt(0), new_leaf->GetPageId(), comparator_);
    // LOG_INFO("insert key: leaf page id %d", leaf->GetPageId());
    // LOG_INFO("insert key: leaf page id %d", new_leaf->GetPageId());
    return success;
  }

  // insert leaf node into parent
  auto parent_page = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());
  parent_page->Remove(remove_key, comparator_);
  parent_page->Insert(leaf->KeyAt(0), leaf->GetPageId(), comparator_);

  if (parent_page->GetSize() <= parent_page->GetMaxSize() - 1) {
    // LOG_INFO("insert key: leaf page id %d", new_leaf->GetPageId());
    return parent_page->Insert(new_leaf->KeyAt(0), new_leaf->GetPageId(), comparator_);
  }

  new_page = bpm_->NewPageGuarded(&new_page_id);
  auto new_parent_page = new_page.AsMut<InternalPage>();
  new_parent_page->Init(internal_max_size_, new_page_id);
  KeyType remove_internal_key = parent_page->KeyAt(0);
  parent_page->MoveHalfTo(new_parent_page);

  if (comparator_(parent_page->KeyAt(parent_page->GetSize() - 1), new_leaf->KeyAt(0)) < 0) {
    // insert into right page
    success = new_parent_page->Insert(new_leaf->KeyAt(0), new_leaf->GetPageId(), comparator_);
  } else {
    // insert into left page
    success = parent_page->Insert(new_leaf->KeyAt(0), new_leaf->GetPageId(), comparator_);
  }

  // imbalance
  if (parent_page->GetSize() > new_parent_page->GetSize() + 1) {
    parent_page->MoveLastToFront(new_parent_page);
  }
  if (new_parent_page->GetSize() > parent_page->GetSize() + 1) {
    new_parent_page->MoveFirstToEnd(parent_page);
  }

  if (parent_page->GetPageId() == GetRootPageId()) {
    // create new root
    new_page = bpm_->NewPageGuarded(&new_page_id);
    auto new_root = new_page.AsMut<InternalPage>();

    new_root->Init(internal_max_size_, new_page_id);
    ctx.root_page_id_ = new_page_id;
    SetRootPageId(ctx.root_page_id_);

    success = new_root->Insert(parent_page->KeyAt(0), parent_page->GetPageId(), comparator_);
    success |= new_root->Insert(new_parent_page->KeyAt(0), new_parent_page->GetPageId(), comparator_);
    // LOG_INFO("insert key: internal page id %d", parent_page->GetPageId());
    // LOG_INFO("insert key: new_internal page id %d", new_parent_page->GetPageId());
    return success;
  }

  depth--;

  InternalPage *new_internal = new_parent_page;
  InternalPage *internal;
  while (depth >= 0) {
    internal = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());
    internal->Remove(remove_internal_key, comparator_);
    internal->Insert(parent_page->KeyAt(0), parent_page->GetPageId(), comparator_);
    // if has less than n key values
    if (internal->GetSize() <= internal->GetMaxSize() - 1) {
      // LOG_INFO("insert key: leaf page id %d", new_internal->GetPageId());
      return internal->Insert(new_internal->KeyAt(0), new_internal->GetPageId(), comparator_);
    }

    // split
    new_page = bpm_->NewPageGuarded(&new_page_id);
    auto new_parent_internal = new_page.AsMut<InternalPage>();
    new_parent_internal->Init(internal_max_size_, new_page_id);
    remove_internal_key = internal->KeyAt(0);
    internal->MoveHalfTo(new_parent_internal);
    if (comparator_(internal->KeyAt(internal->GetSize() - 1), new_internal->KeyAt(0)) < 0) {
      // insert into right page
      success = new_parent_internal->Insert(new_internal->KeyAt(0), new_internal->GetPageId(), comparator_);
    } else {
      // insert into left page
      success = internal->Insert(new_internal->KeyAt(0), new_internal->GetPageId(), comparator_);
    }
    // imbalance
    if (internal->GetSize() > new_parent_internal->GetSize() + 1) {
      internal->MoveLastToFront(new_parent_internal);
    }
    if (new_parent_internal->GetSize() > internal->GetSize() + 1) {
      new_parent_internal->MoveFirstToEnd(internal);
    }

    // if is root page
    if (internal->GetPageId() == GetRootPageId()) {
      // create new root
      new_page = bpm_->NewPageGuarded(&new_page_id);
      auto new_root = new_page.AsMut<InternalPage>();
      new_root->Init(internal_max_size_, new_page_id);
      ctx.root_page_id_ = new_page_id;
      SetRootPageId(ctx.root_page_id_);

      success = new_root->Insert(internal->KeyAt(0), internal->GetPageId(), comparator_);
      success |= new_root->Insert(new_parent_internal->KeyAt(0), new_parent_internal->GetPageId(), comparator_);
      // LOG_INFO("insert key: internal page id %d", parent_page->GetPageId());
      // LOG_INFO("insert key: new_internal page id %d", new_parent_internal->GetPageId());
      return success;
    }
    // insert at next depth
    new_internal = new_parent_internal;
    parent_page = internal;
    depth--;
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  if (IsEmpty()) {
    return;
  }
  // std::cout << "delete key" << key;
  RemoveEntry(key, txn);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  ctx.root_page_id_ = GetRootPageId();

  // // LOG_INFO("remove key: header page id %d", header_page_id_);
  WritePageGuard root_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  while (root_guard.PageId() != GetRootPageId()) {
    root_guard.Drop();
    // root page id may changed
    ctx.root_page_id_ = GetRootPageId();
    root_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  }
  auto root_page = root_guard.template AsMut<BPlusTreePage>();
  int depth = -1;
  bool is_first_leaf = true;
  // find leaf page
  while (!root_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(root_page);
    int index = internal_page->KeyIndex(key, comparator_);
    if (index == -1) {
      return;
    }
    if (index != 0) {
      // not first leaf page
      is_first_leaf = false;
    }
    page_id_t child_id = internal_page->ValueAt(index);
    ctx.write_set_.push_back(std::move(root_guard));
    root_guard = bpm_->FetchPageWrite(child_id);
    root_page = root_guard.template AsMut<BPlusTreePage>();
    depth++;
  }

  auto leaf = root_guard.template AsMut<LeafPage>();
  int lindex = leaf->KeyIndex(key, comparator_);
  if (lindex == -1) {
    // not found
    return;
  }
  if (leaf->GetPageId() == ctx.root_page_id_) {
    // header page is leaf page
    leaf->Remove(key, comparator_);
    leaf->SetNextPageId(INVALID_PAGE_ID);

    // LOG_INFO("remove key: leaf page id %d", leaf->GetPageId());
    if (leaf->GetSize() == 0) {
      // empty
      bpm_->DeletePage(leaf->GetPageId());
      ctx.root_page_id_ = 0;
      SetRootPageId(0);
    }
    return;
  }
  // not root page
  leaf->Remove(key, comparator_);
  // LOG_INFO("remove key: leaf page id %d", leaf->GetPageId());

  if (leaf->GetSize() == 0) {
    // empty, change next page id
    auto int_page = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());
    int index = int_page->KeyIndex(key, comparator_);
    if (is_first_leaf) {
      // no need to change next page id
    } else {
      int d = depth - 1;
      while (index == 0) {
        auto par_page = reinterpret_cast<InternalPage *>(ctx.write_set_[d].template AsMut<BPlusTreePage>());
        index = par_page->KeyIndex(key, comparator_);
        d--;
      }
      index--;
      d++;
      auto par_page = reinterpret_cast<InternalPage *>(ctx.write_set_[d].template AsMut<BPlusTreePage>());
      auto lef_page_id = par_page->ValueAt(index);
      auto lef_guard = bpm_->FetchPageWrite(lef_page_id);
      auto lef_page = lef_guard.template AsMut<BPlusTreePage>();
      while (!lef_page->IsLeafPage()) {
        auto internal_page = reinterpret_cast<InternalPage *>(lef_page);
        lef_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
        lef_guard = bpm_->FetchPageWrite(lef_page_id);
        lef_page = lef_guard.template AsMut<BPlusTreePage>();
      }
      auto lef = lef_guard.template AsMut<LeafPage>();
      lef->SetNextPageId(leaf->GetNextPageId());
    }
    bpm_->DeletePage(leaf->GetPageId());
  } else {
    return;
  }

  // auto int_guard = std::move(ctx.write_set_.back());
  auto int_page = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());
  int_page->Remove(key, comparator_);
  // LOG_INFO("remove key: internal page id %d", int_page->GetPageId());

  // has less than minsize key values, need merge or redistribute
  while (depth >= 0) {
    // auto internal_guard = std::move(ctx.write_set_.back());
    auto internal_page = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());

    // has more than minsize key values
    if (internal_page->GetSize() >= internal_page->GetMinSize()) {
      return;
    }

    // root page
    if (internal_page->GetPageId() == GetRootPageId()) {
      // only one child, root become now's child
      if (internal_page->GetSize() == 1) {
        SetRootPageId(internal_page->ValueAt(0));
        bpm_->DeletePage(internal_page->GetPageId());
        return;
      }
      return;
    }
    depth--;
    // merge or redistribute
    // auto parent_guard = std::move(ctx.write_set_.back());
    auto parent_page = reinterpret_cast<InternalPage *>(ctx.write_set_[depth].template AsMut<BPlusTreePage>());
    // ctx.write_set_.back() = std::move(parent_guard);

    // find slibing page
    int index = parent_page->KeyIndex(key, comparator_);
    if (index == -1) {
      return;
    }
    page_id_t sibling_id;
    if (index == 0) {
      sibling_id = parent_page->ValueAt(index + 1);
    } else {
      sibling_id = parent_page->ValueAt(index - 1);
    }
    auto sibling_guard = bpm_->FetchPageWrite(sibling_id);
    auto sibling_page = sibling_guard.template AsMut<InternalPage>();

    if (sibling_page->GetSize() > sibling_page->GetMinSize()) {
      // redistribute
      if (index == 0) {
        // right sibling
        sibling_page->MoveFirstToEnd(internal_page);
        parent_page->SetKeyAt(1, sibling_page->KeyAt(0));
      } else {
        // left sibling
        sibling_page->MoveLastToFront(internal_page);
        parent_page->SetKeyAt(index, internal_page->KeyAt(0));
      }
      return;
    }
    // merge
    if (index == 0) {
      // right sibling
      KeyType remove_key = sibling_page->KeyAt(0);
      sibling_page->MoveAllTo(internal_page);
      parent_page->Remove(remove_key, comparator_);
      // LOG_INFO("remove key: internal page id %d", parent_page->GetPageId());
      bpm_->DeletePage(sibling_page->GetPageId());
    } else {
      // left sibling
      KeyType remove_key = internal_page->KeyAt(0);
      internal_page->MoveAllTo(sibling_page);
      parent_page->Remove(remove_key, comparator_);
      // LOG_INFO("remove key: internal page id %d", parent_page->GetPageId());
      bpm_->DeletePage(internal_page->GetPageId());
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto guard = bpm_->FetchPageRead(GetRootPageId());
  auto root_page = guard.template As<BPlusTreePage>();
  // find the leaf page
  while (!root_page->IsLeafPage()) {
    auto internal_page = guard.template As<InternalPage>();
    page_id_t child_id = internal_page->ValueAt(0);
    guard = bpm_->FetchPageRead(child_id);
    root_page = guard.template As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(guard.PageId(), 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto root_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = root_guard.template As<BPlusTreePage>();
  // find the leaf page
  while (!root_page->IsLeafPage()) {
    auto internal_page = root_guard.template As<InternalPage>();
    int index = internal_page->KeyIndex(key, comparator_);
    if (index == -1) {
      return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_);
    }
    page_id_t child_id = internal_page->ValueAt(index);
    root_guard = bpm_->FetchPageRead(child_id);
    root_page = root_guard.template As<BPlusTreePage>();
  }
  auto leaf_page = root_guard.template As<LeafPage>();
  int index = leaf_page->KeyIndex(key, comparator_);
  if (index == -1) {
    // not found
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_);
  }
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  latch_.lock();
  page_id_t root_page_id = header_page_id_;
  latch_.unlock();
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t root_page_id) {
  latch_.lock();
  header_page_id_ = root_page_id;
  latch_.unlock();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
