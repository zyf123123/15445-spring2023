//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  /*
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");
  */

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t new_frame_id = INVALID_PAGE_ID;
  // find from free list
  for (auto &it : free_list_) {
    if (it == INVALID_PAGE_ID) {
      continue;
    }
    new_frame_id = it;
    free_list_.remove(it);
    break;
  }
  // find from replacer
  if (new_frame_id == INVALID_PAGE_ID) {
    frame_id_t frame_id = -1;
    if (!replacer_->Evict(&frame_id)) {
      latch_.unlock();
      return nullptr;
    }
    new_frame_id = frame_id;
    if (pages_[new_frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[new_frame_id].page_id_, pages_[new_frame_id].data_);
      pages_[new_frame_id].is_dirty_ = false;
      // FlushPage(pages_[new_frame_id].page_id_);
    }
    page_table_.erase(pages_[new_frame_id].page_id_);
    memset(pages_[new_frame_id].data_, 0, BUSTUB_PAGE_SIZE);
    // DeletePage(pages_[new_frame_id].page_id_);
  }

  // allocate new page
  *page_id = AllocatePage();
  page_table_[*page_id] = new_frame_id;
  pages_[new_frame_id].page_id_ = *page_id;
  pages_[new_frame_id].pin_count_ = 1;
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  latch_.unlock();
  return &pages_[new_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  frame_id_t new_frame_id = INVALID_PAGE_ID;
  // find from buffer pool
  auto frame_id_iter = page_table_.find(page_id);
  if (frame_id_iter != page_table_.end()) {
    new_frame_id = frame_id_iter->second;
    pages_[new_frame_id].pin_count_++;
    replacer_->RecordAccess(new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);
    latch_.unlock();
    return &pages_[new_frame_id];
  }

  // find from free list
  for (auto &it : free_list_) {
    new_frame_id = it;
    free_list_.remove(it);
    break;
  }
  // find from replacer
  if (new_frame_id == INVALID_PAGE_ID) {
    frame_id_t frame_id = -1;
    if (!replacer_->Evict(&frame_id)) {
      latch_.unlock();
      return nullptr;
    }
    new_frame_id = frame_id;
    if (pages_[new_frame_id].is_dirty_) {
      // FlushPage(pages_[new_frame_id].page_id_);
      disk_manager_->WritePage(pages_[new_frame_id].page_id_, pages_[new_frame_id].data_);
      pages_[new_frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[new_frame_id].page_id_);
    memset(pages_[new_frame_id].data_, 0, BUSTUB_PAGE_SIZE);
    // DeletePage(pages_[new_frame_id].page_id_);
  }

  // new page
  pages_[new_frame_id].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[new_frame_id].data_);
  page_table_[page_id] = new_frame_id;
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);
  pages_[new_frame_id].pin_count_ = 1;
  latch_.unlock();
  return &pages_[new_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  // find from buffer pool
  auto frame_id_iter = page_table_.find(page_id);
  if (frame_id_iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  if (pages_[frame_id_iter->second].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[frame_id_iter->second].pin_count_--;
  if (pages_[frame_id_iter->second].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id_iter->second, true);
  }
  if (!pages_[frame_id_iter->second].is_dirty_) {
    pages_[frame_id_iter->second].is_dirty_ = is_dirty;
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  frame_id_t new_frame_id = INVALID_PAGE_ID;
  auto frame_id_iter = page_table_.find(page_id);
  if (frame_id_iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  new_frame_id = frame_id_iter->second;
  disk_manager_->WritePage(page_id, pages_[new_frame_id].data_);
  pages_[new_frame_id].is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &it : page_table_) {
    FlushPage(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  frame_id_t new_frame_id = INVALID_PAGE_ID;
  auto frame_id_iter = page_table_.find(page_id);
  // not find
  if (frame_id_iter == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  new_frame_id = frame_id_iter->second;
  // if pinned return false
  if (pages_[new_frame_id].pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  // delete page
  page_table_.erase(pages_[new_frame_id].page_id_);
  pages_[new_frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[new_frame_id].pin_count_ = 0;
  pages_[new_frame_id].is_dirty_ = false;
  memset(pages_[new_frame_id].data_, 0, BUSTUB_PAGE_SIZE);
  // replacer_->Remove(new_frame_id);
  free_list_.emplace_back(new_frame_id);
  DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  page_table_.insert({next_page_id_, INVALID_PAGE_ID});
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
