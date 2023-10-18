//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_heap = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto table_iter = table_heap->MakeEagerIterator();
  table_iter_ = std::make_unique<TableIterator>(std::move(table_iter));
  bool success = true;
  try {
    if (exec_ctx_->IsDelete()) {
      success = this->exec_ctx_->GetLockManager()->LockTable(
          this->exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->GetTableOid());
    } else {
      if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !this->exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->GetTableOid())) {
        success = this->exec_ctx_->GetLockManager()->LockTable(
            this->exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Transaction abort");
  }
  if (!success) {
    throw ExecutionException("Transaction abort");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_->IsEnd()) {
    /*
    if (!exec_ctx_->IsDelete() &&
        this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        !this->exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
      try {
        if (this->exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())) {
          this->exec_ctx_->GetLockManager()->UnlockTable(this->exec_ctx_->GetTransaction(), plan_->GetTableOid());
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("Transaction abort");
      }
    }*/
    return false;
  }

  TupleMeta tuple_meta;
  do {
    if (table_iter_->IsEnd()) {
      /*
      if (!exec_ctx_->IsDelete() &&
          this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !this->exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
        try {
          if (this->exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())) {
            this->exec_ctx_->GetLockManager()->UnlockTable(this->exec_ctx_->GetTransaction(), plan_->GetTableOid());
          }
        } catch (TransactionAbortException &e) {
          throw ExecutionException("Transaction abort");
        }
      }*/
      return false;
    }
    tuple_meta = table_iter_->GetTuple().first;
    *rid = table_iter_->GetRID();
    bool success = true;
    if (!tuple_meta.is_deleted_) {
      try {
        if (exec_ctx_->IsDelete()) {
          success = this->exec_ctx_->GetLockManager()->LockRow(
              this->exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), *rid);
        }
        if (!exec_ctx_->IsDelete()) {
          if (this->exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
              !this->exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), *rid)) {
            success = this->exec_ctx_->GetLockManager()->LockRow(
                this->exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), *rid);
          }
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("Transaction abort");
      }
      if (!success) {
        throw ExecutionException("Transaction abort");
      }

      tuple_meta = table_iter_->GetTuple().first;
      if (!tuple_meta.is_deleted_) {
        *tuple = table_iter_->GetTuple().second;
        break;
      }

      try {
        if (exec_ctx_->IsDelete() &&
            this->exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), *rid)) {
          success = this->exec_ctx_->GetLockManager()->UnlockRow(this->exec_ctx_->GetTransaction(),
                                                                 plan_->GetTableOid(), *rid, true);
        }
        if (!exec_ctx_->IsDelete()) {
          if (this->exec_ctx_->GetTransaction()->IsRowSharedLocked(plan_->GetTableOid(), *rid)) {
            success = this->exec_ctx_->GetLockManager()->UnlockRow(this->exec_ctx_->GetTransaction(),
                                                                   plan_->GetTableOid(), *rid, true);
          }
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("Transaction abort");
      }
      if (!success) {
        throw ExecutionException("Transaction abort");
      }
    }
    ++(*table_iter_);
  } while (tuple_meta.is_deleted_);
  /*
  if (tuple_meta.delete_txn_id_ != this->exec_ctx_->GetTransaction()->GetTransactionId() && exec_ctx_->IsDelete()) {
    this->exec_ctx_->GetLockManager()->UnlockRow(this->exec_ctx_->GetTransaction(), plan_->GetTableOid(),
                                                 table_iter_->GetRID(), true);
  }*/
  try {
    if (!exec_ctx_->IsDelete() &&
        this->exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        this->exec_ctx_->GetTransaction()->IsRowSharedLocked(plan_->GetTableOid(), *rid)) {
      if (!this->exec_ctx_->GetLockManager()->UnlockRow(this->exec_ctx_->GetTransaction(), plan_->GetTableOid(), *rid,
                                                        true)) {
        throw ExecutionException("Transaction abort");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Transaction abort");
  }
  ++(*table_iter_);
  return true;
}

}  // namespace bustub
