//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k) : k_(k) {
  // history_ = std::list<size_t>();
}
void LRUKNode::RecordAccess(size_t timestamp) {
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}
auto LRUKNode::IsEvictable() const { return is_evictable_; }
void LRUKNode::SetIsEvictable(bool isEvictable) { is_evictable_ = isEvictable; }
auto LRUKNode::GetBackwardKDistance() {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return history_.front();
}
auto LRUKNode::GetEarliestTimestamp() const { return history_.front(); }
void LRUKNode::Evict() {
  history_.clear();
  is_evictable_ = false;
}
auto LRUKNode::GetSize() const { return history_.size(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  if (k < 1) {
    throw Exception(ExceptionType::INVALID, "k must be greater than 0");
  }
  this->k_ = k;
  this->replacer_size_ = num_frames;
  this->node_store_ = std::unordered_map<frame_id_t, LRUKNode>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  size_t last_timestamp = 0;
  size_t earliest_timestamp = current_timestamp_;
  bool found = false;
  for (auto &it : node_store_) {
    if (it.second.IsEvictable()) {
      size_t d = it.second.GetBackwardKDistance();
      if (d != std::numeric_limits<size_t>::max()) {
        d = current_timestamp_ - d;
      }

      if (d > last_timestamp) {
        *frame_id = it.first;
        last_timestamp = d;
        earliest_timestamp = it.second.GetEarliestTimestamp();
        found = true;
      } else if (d == last_timestamp) {
        size_t t = it.second.GetEarliestTimestamp();
        if (t < earliest_timestamp) {
          *frame_id = it.first;
          earliest_timestamp = t;
          found = true;
        }
      }
    }
  }
  if (!found) {
    latch_.unlock();
    return false;
  }
  node_store_.at(*frame_id).Evict();
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::INVALID, "frame_id out of range");
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode node = LRUKNode(k_);
    node.RecordAccess(current_timestamp_);
    node_store_.insert({frame_id, node});

  } else {
    node_store_.at(frame_id).RecordAccess(current_timestamp_);
  }
  current_timestamp_++;
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::INVALID, "frame_id out of range");
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    throw Exception(ExceptionType::INVALID, "out of range");
  }

  bool last_evictable = node_store_.at(frame_id).IsEvictable();
  if (last_evictable != set_evictable) {
    node_store_.at(frame_id).SetIsEvictable(set_evictable);
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::INVALID, "frame_id out of range");
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    return;
  }

  if (node_store_.at(frame_id).IsEvictable()) {
    curr_size_--;
    node_store_.erase(frame_id);
  } else {
    throw Exception(ExceptionType::INVALID, "frame_id is not evictable");
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  size_t size = 0;
  latch_.lock();
  for (auto &it : node_store_) {
    if (it.second.IsEvictable() && it.second.GetSize() > 0) {
      size++;
    }
  }
  latch_.unlock();
  return size;
}

}  // namespace bustub
