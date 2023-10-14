//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // check if the transaction is compatible with the lock mode
  CanTxnTakeLock(txn, lock_mode);

  // get lock request queue for the table
  table_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    LockRequestQueue que = LockRequestQueue();
    std::shared_ptr<LockRequestQueue> q = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = q;
    lock_request_queue = std::move(q);
  } else {
    lock_request_queue = table_lock_map_[oid];
  }
  lock_request_queue->queue_latch_.lock();
  table_lock_map_latch_.unlock();

  LOG_INFO("txn %d lock table %d mode %d", txn->GetTransactionId(), oid, lock_mode);
  // check txn in the queue
  std::shared_ptr<LockRequest> lock_request;
  for (const auto &lr : lock_request_queue->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      lock_request = lr;
      break;
    }
  }
  if (lock_request != nullptr && lock_request->granted_) {
    // check if need lock upgrade
    try {
      CanLockUpgrade(txn, lock_request_queue, lock_request->lock_mode_, lock_mode);
    } catch (TransactionAbortException &e) {
      lock_request_queue->queue_latch_.unlock();
      throw e;
    }

    lock_request_queue->upgrading_ = txn->GetTransactionId();
    ReleaseLock(txn, lock_request->lock_mode_, oid, lock_request);
    // remove from request queue
    lock_request_queue->request_queue_.remove(lock_request);
    // lock_request_queue->cv_.notify_all();
    lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    // join into request queue
    auto lr = lock_request_queue->request_queue_.begin();
    for (; lr != lock_request_queue->request_queue_.end(); ++lr) {
      if (!lr->get()->granted_) {
        lock_request_queue->request_queue_.insert(lr, lock_request);
        break;
      }
    }
    if (lr == lock_request_queue->request_queue_.end()) {
      lock_request_queue->request_queue_.emplace_back(lock_request);
    }
  } else {
    lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    // join into request queue
    lock_request_queue->request_queue_.emplace_back(lock_request);
  }
  lock_request_queue->queue_latch_.unlock();

  // try to acquire lock
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  lock_request_queue->queue_latch_.lock();
  while (!GrantLock(txn, lock_mode, oid, lock_request, lock_request_queue)) {
    LOG_INFO("lock queue size %zu", lock_request_queue->request_queue_.size());
    LOG_INFO("txn %d wait lock table %d mode %d", txn->GetTransactionId(), oid, lock_mode);
    lock_request_queue->queue_latch_.unlock();
    lock_request_queue->cv_.wait(lock);
    // if txn is abort
    if (txn->GetState() == TransactionState::ABORTED) {
      // LOG_INFO("txn %d abort", txn->GetTransactionId());
      RemoveRelatedEdges(txn->GetTransactionId());
      lock_request_queue->queue_latch_.lock();
      lock_request_queue->request_queue_.remove(lock_request);
      if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->cv_.notify_all();
      lock_request_queue->queue_latch_.unlock();
      return false;
    }
    lock_request_queue->queue_latch_.lock();
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  lock_request_queue->queue_latch_.unlock();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // check if the txn has the row lock
  if (!txn->GetSharedRowLockSet()->empty()) {
    for (const auto &s_row_lock_set : *txn->GetSharedRowLockSet()) {
      if (!s_row_lock_set.second.empty() && s_row_lock_set.first == oid) {
        LOG_INFO("txn %d unlock table %d but txn dont unlock all row size %zu", txn->GetTransactionId(), oid,
                 s_row_lock_set.second.size());
        for (const auto &rid : s_row_lock_set.second) {
          LOG_INFO("txn %d unlock table %d but txn dont unlock row %d", txn->GetTransactionId(), oid, rid.GetSlotNum());
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      }
    }
  }
  if (!txn->GetExclusiveRowLockSet()->empty()) {
    for (const auto &x_row_lock_set : *txn->GetExclusiveRowLockSet()) {
      if (!x_row_lock_set.second.empty() && x_row_lock_set.first == oid) {
        LOG_INFO("txn %d unlock table %d but txn dont unlock all row size %zu", txn->GetTransactionId(), oid,
                 x_row_lock_set.second.size());
        for (const auto &rid : x_row_lock_set.second) {
          LOG_INFO("txn %d unlock table %d but txn dont unlock row %d", txn->GetTransactionId(), oid, rid.GetSlotNum());
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      }
    }
  }
  // get lock request queue for the table
  table_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    LOG_INFO("txn %d unlock table %d but txn dont got table lock", txn->GetTransactionId(), oid);
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  lock_request_queue = table_lock_map_[oid];
  lock_request_queue->queue_latch_.lock();
  table_lock_map_latch_.unlock();
  LOG_INFO("txn %d unlock table %d", txn->GetTransactionId(), oid);
  // check txn in the queue
  std::shared_ptr<LockRequest> lock_request;
  for (const auto &lr : lock_request_queue->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      lock_request = lr;
      break;
    }
  }
  // remove from request queue
  lock_request_queue->request_queue_.remove(lock_request);
  // notify other txns
  lock_request_queue->cv_.notify_all();
  lock_request_queue->queue_latch_.unlock();

  // change txn state
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
             txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  ReleaseLock(txn, lock_request->lock_mode_, oid, lock_request);

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // check if the transaction is compatible with the lock mode
  CanTxnTakeLock(txn, lock_mode);
  // check if the txn already has the table lock
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    LOG_INFO("txn %d lock row %d table %d mode %d but txn dont got table lock", txn->GetTransactionId(),
             rid.GetSlotNum(), oid, lock_mode);
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  // get lock request queue for the row
  row_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    LockRequestQueue que = LockRequestQueue();
    std::shared_ptr<LockRequestQueue> q = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = q;
    lock_request_queue = std::move(q);
  } else {
    lock_request_queue = row_lock_map_[rid];
  }
  lock_request_queue->queue_latch_.lock();
  row_lock_map_latch_.unlock();
  LOG_INFO("txn %d lock row %d table %d mode %d", txn->GetTransactionId(), rid.GetSlotNum(), oid, lock_mode);
  // check txn in the queue
  std::shared_ptr<LockRequest> lock_request;
  for (const auto &lr : lock_request_queue->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      lock_request = lr;
      break;
    }
  }
  if (lock_request != nullptr && lock_request->granted_) {
    // check if need lock upgrade
    try {
      CanLockUpgrade(txn, lock_request_queue, lock_request->lock_mode_, lock_mode);
    } catch (TransactionAbortException &e) {
      lock_request_queue->queue_latch_.unlock();
      throw e;
    }
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    ReleaseRowLock(txn, lock_request->lock_mode_, oid, rid, lock_request);
    // remove from request queue
    lock_request_queue->request_queue_.remove(lock_request);
    // lock_request_queue->cv_.notify_all();
    lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    // join into request queue
    auto lr = lock_request_queue->request_queue_.begin();
    for (; lr != lock_request_queue->request_queue_.end(); ++lr) {
      if (!lr->get()->granted_) {
        lock_request_queue->request_queue_.insert(lr, lock_request);
        break;
      }
    }
    if (lr == lock_request_queue->request_queue_.end()) {
      lock_request_queue->request_queue_.emplace_back(lock_request);
    }
  } else {
    lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    // join into request queue
    lock_request_queue->request_queue_.emplace_back(lock_request);
  }
  lock_request_queue->queue_latch_.unlock();
  // try to acquire lock
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  lock_request_queue->queue_latch_.lock();
  while (!GrantRowLock(txn, lock_mode, oid, rid, lock_request, lock_request_queue)) {
    LOG_INFO("lock queue size %zu", lock_request_queue->request_queue_.size());
    LOG_INFO("txn %d wait lock row %d table %d mode %d", txn->GetTransactionId(), rid.GetSlotNum(), oid, lock_mode);
    lock_request_queue->queue_latch_.unlock();
    lock_request_queue->cv_.wait(lock);
    // if txn is abort
    if (txn->GetState() == TransactionState::ABORTED) {
      RemoveRelatedEdges(txn->GetTransactionId());
      lock_request_queue->queue_latch_.lock();
      lock_request_queue->request_queue_.remove(lock_request);
      if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->cv_.notify_all();
      lock_request_queue->queue_latch_.unlock();
      return false;
    }
    lock_request_queue->queue_latch_.lock();
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  lock_request_queue->queue_latch_.unlock();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // get lock request queue for the table
  row_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    LOG_INFO("txn %d unlock row %d table %d but txn dont got row lock", txn->GetTransactionId(), rid.GetSlotNum(), oid);
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  lock_request_queue = row_lock_map_[rid];
  lock_request_queue->queue_latch_.lock();
  row_lock_map_latch_.unlock();
  LOG_INFO("txn %d unlock row %d table %d", txn->GetTransactionId(), rid.GetSlotNum(), oid);
  // check txn in the queue
  std::shared_ptr<LockRequest> lock_request;
  for (const auto &lr : lock_request_queue->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      lock_request = lr;
      break;
    }
  }
  // remove from request queue
  lock_request_queue->request_queue_.remove(lock_request);
  ReleaseRowLock(txn, lock_request->lock_mode_, oid, rid, lock_request);
  // notify other txns
  lock_request_queue->cv_.notify_all();
  lock_request_queue->queue_latch_.unlock();
  if (!force) {
    // change txn state
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
               txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1] = std::vector<txn_id_t>();
  }
  for (const auto &t : waits_for_[t1]) {
    if (t == t2) {
      waits_for_latch_.unlock();
      return;
    }
  }
  waits_for_[t1].emplace_back(t2);
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_latch_.unlock();
    return;
  }
  auto edges = waits_for_[t1].begin();
  for (; edges != waits_for_[t1].end(); ++edges) {
    if (*edges == t2) {
      waits_for_[t1].erase(edges);
      break;
    }
  }
  waits_for_latch_.unlock();
}

void LockManager::RemoveRelatedEdges(txn_id_t txn_id) {
  LOG_INFO("remove edges for txn %d", txn_id);
  waits_for_latch_.lock();
  if (waits_for_.find(txn_id) != waits_for_.end()) {
    waits_for_.erase(txn_id);
  }
  /*
  for (auto edge : waits_for_) {
    auto edges = edge.second.begin();
    for (; edges != edge.second.end(); ++edges) {
      if (*edges == txn_id) {
        edge.second.erase(edges);
      }
    }
  }*/
  waits_for_latch_.unlock();
}

auto LockManager::Dfs(txn_id_t start_txn, txn_id_t *youngest_txn) -> bool {
  if (visited_[start_txn]) {
    if (start_txn > *youngest_txn) {
      *youngest_txn = start_txn;
    }
    return true;
  }
  // bool flag = false;
  visited_[start_txn] = true;

  if (waits_for_.find(start_txn) == waits_for_.end()) {
    return false;
  }
  std::vector<txn_id_t> wait_for;
  wait_for.reserve(waits_for_.size());
  for (const auto &edge : waits_for_[start_txn]) {
    wait_for.emplace_back(edge);
  }
  std::sort(wait_for.begin(), wait_for.end());
  for (const auto &t : wait_for) {
    if (Dfs(t, youngest_txn)) {
      if (start_txn > *youngest_txn) {
        *youngest_txn = start_txn;
      }
      visited_[start_txn] = false;
      return true;
    }
  }
  visited_[start_txn] = false;
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  bool flag = false;
  *txn_id = -1;
  std::vector<txn_id_t> wait_for;
  wait_for.reserve(waits_for_.size());
  for (const auto &edge : waits_for_) {
    for (const auto &t : edge.second) {
      LOG_INFO("txn %d wait for %d", edge.first, t);
    }
    wait_for.emplace_back(edge.first);
  }
  std::sort(wait_for.begin(), wait_for.end());
  for (const auto &key : wait_for) {
    auto start_txn = key;
    auto youngest_txn = key;
    if (waits_for_.find(start_txn) == waits_for_.end()) {
      continue;
    }
    if (Dfs(start_txn, &youngest_txn)) {
      if (youngest_txn > *txn_id) {
        *txn_id = youngest_txn;
      }
      flag = true;
      break;
    }
  }
  return flag;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &edge : waits_for_) {
    for (const auto &t : edge.second) {
      edges.emplace_back(edge.first, t);
    }
  }
  return edges;
}

void LockManager::CreateWaitsForGraph() {
  table_lock_map_latch_.lock();
  for (const auto &lrq : table_lock_map_) {
    lrq.second->queue_latch_.lock();
    for (const auto &lr : lrq.second->request_queue_) {
      for (const auto &lr2 : lrq.second->request_queue_) {
        if (lr->txn_id_ != lr2->txn_id_) {
          if (lr->granted_ && !lr2->granted_) {
            AddEdge(lr2->txn_id_, lr->txn_id_);
          } else if (!lr->granted_ && lr2->granted_) {
            AddEdge(lr->txn_id_, lr2->txn_id_);
          }
        }
      }
    }
    lrq.second->queue_latch_.unlock();
  }
  for (const auto &lrq : row_lock_map_) {
    lrq.second->queue_latch_.lock();
    for (const auto &lr : lrq.second->request_queue_) {
      for (const auto &lr2 : lrq.second->request_queue_) {
        if (lr->txn_id_ != lr2->txn_id_) {
          if (lr->granted_ && !lr2->granted_) {
            AddEdge(lr2->txn_id_, lr->txn_id_);
          } else if (!lr->granted_ && lr2->granted_) {
            AddEdge(lr->txn_id_, lr2->txn_id_);
          }
        }
      }
    }
    lrq.second->queue_latch_.unlock();
  }
  table_lock_map_latch_.unlock();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      LOG_INFO("detect deadlock");
      CreateWaitsForGraph();
      for (const auto &edge : waits_for_) {
        txn_id_t txn_id = edge.first;
        if (HasCycle(&txn_id)) {
          LOG_INFO("txn %d abort", txn_id);
          auto txn = txn_manager_->GetTransaction(txn_id);
          txn_manager_->Abort(txn);
          break;
        }
      }
      waits_for_ = std::unordered_map<txn_id_t, std::vector<txn_id_t>>();
      visited_ = std::unordered_map<txn_id_t, bool>();
    }
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  return true;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("Transaction is not active");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  return true;
}

auto LockManager::CanLockUpgrade(Transaction *txn, const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                 LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == requested_lock_mode) {
    // no need to upgrade
    return false;
  }
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    // already upgrading
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(lock_request_queue->upgrading_, AbortReason::UPGRADE_CONFLICT);
  }
  switch (curr_lock_mode) {
    case LockMode::SHARED:
      if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(lock_request_queue->request_queue_.front()->txn_id_,
                                        AbortReason::INCOMPATIBLE_UPGRADE);
      }
    case LockMode::EXCLUSIVE:
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(lock_request_queue->request_queue_.front()->txn_id_,
                                      AbortReason::INCOMPATIBLE_UPGRADE);
    case LockMode::INTENTION_SHARED:
      return true;
    case LockMode::INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(lock_request_queue->request_queue_.front()->txn_id_,
                                        AbortReason::INCOMPATIBLE_UPGRADE);
      }
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE) {
        return true;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(lock_request_queue->request_queue_.front()->txn_id_,
                                        AbortReason::INCOMPATIBLE_UPGRADE);
      }
  }
  return true;
}

auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                            const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  // LOG_INFO("txn %d try grant lock table %d mode %d", txn->GetTransactionId(), oid, lock_mode);
  for (const auto &lr : lock_request_queue->request_queue_) {
    // check if the txn is compatible with the lock mode
    if (!AreLocksCompatible(lock_mode, lr->lock_mode_) && lr->granted_) {
      return false;
    }
  }

  // check priority
  if (lock_request_queue->upgrading_ == INVALID_TXN_ID) {
    for (const auto &lr : lock_request_queue->request_queue_) {
      if (lr == lock_request) {
        break;
      }
      if (!AreLocksCompatible(lock_mode, lr->lock_mode_)) {
        return false;
      }
    }
  } else if (lock_request_queue->upgrading_ != txn->GetTransactionId()) {
    return false;
  }

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
  lock_request->granted_ = true;
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::GrantRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                               const std::shared_ptr<LockRequest> &lock_request,
                               const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (const auto &lr : lock_request_queue->request_queue_) {
    // check if the txn is compatible with the lock mode
    if (!AreLocksCompatible(lock_mode, lr->lock_mode_) && lr->granted_) {
      return false;
    }
  }

  // check priority
  if (lock_request_queue->upgrading_ == INVALID_TXN_ID) {
    for (const auto &lr : lock_request_queue->request_queue_) {
      if (lr == lock_request) {
        break;
      }
      if (!AreLocksCompatible(lock_mode, lr->lock_mode_)) {
        return false;
      }
    }
  } else if (lock_request_queue->upgrading_ != txn->GetTransactionId()) {
    return false;
  }

  switch (lock_mode) {
    case LockMode::SHARED:
      if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
        std::unordered_set<RID> rid_set;
        rid_set.emplace(rid);
        txn->GetSharedRowLockSet()->insert({oid, rid_set});
      } else {
        txn->GetSharedRowLockSet()->at(oid).insert(rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
        std::unordered_set<RID> rid_set;
        rid_set.emplace(rid);
        txn->GetExclusiveRowLockSet()->insert({oid, rid_set});
      } else {
        txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      }
      break;
    default:
      LOG_INFO("txn %d try grant lock row %d table %d mode %d", txn->GetTransactionId(), rid.GetSlotNum(), oid,
               lock_mode);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  lock_request->granted_ = true;
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::ReleaseLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                              const std::shared_ptr<LockRequest> &lock_request) -> bool {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
  lock_request->granted_ = false;
  return true;
}

auto LockManager::ReleaseRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                                 const std::shared_ptr<LockRequest> &lock_request) -> bool {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      if (txn->GetSharedRowLockSet()->at(oid).empty()) {
        txn->GetSharedRowLockSet()->erase(oid);
      }
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      if (txn->GetExclusiveRowLockSet()->at(oid).empty()) {
        txn->GetExclusiveRowLockSet()->erase(oid);
      }
      break;
    default:
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  lock_request->granted_ = false;
  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  switch (row_lock_mode) {
    case LockMode::EXCLUSIVE:

      if (txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        return true;
      }
      break;
    case LockMode::SHARED:
      if (txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
          txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
          txn->IsTableExclusiveLocked(oid)) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_SHARED:
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  return false;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      if (l2 == LockMode::SHARED || l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      break;
    case LockMode::EXCLUSIVE:
      break;
    case LockMode::INTENTION_SHARED:
      if (l2 == LockMode::SHARED || l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE ||
          l2 == LockMode::INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      break;
  }
  return false;
}

}  // namespace bustub
