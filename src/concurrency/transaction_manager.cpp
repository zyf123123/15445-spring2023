//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  // LOG_INFO("txn %d commit", txn->GetTransactionId());

  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  // Rollback tuples
  auto write_set = txn->GetWriteSet();
  // LOG_INFO("txn %d abort", txn->GetTransactionId());
  while (!write_set->empty()) {
    auto &item = write_set->back();
    // Metadata identifying the table that should be deleted from.
    auto *table = item.table_heap_;
    auto tuple_meta = table->GetTupleMeta(item.rid_);
    if (item.wtype_ == WType::DELETE) {
      tuple_meta.is_deleted_ = false;
      table->UpdateTupleMeta(tuple_meta, item.rid_);
    } else if (item.wtype_ == WType::INSERT) {
      tuple_meta.is_deleted_ = true;
      table->UpdateTupleMeta(tuple_meta, item.rid_);
    } else if (item.wtype_ == WType::UPDATE) {
    }
    write_set->pop_back();
  }

  // Rollback indexes
  auto index_write_set = txn->GetIndexWriteSet();
  // LOG_INFO("index write set size %zu", index_write_set->size());
  while (!index_write_set->empty()) {
    auto &item = index_write_set->back();
    auto *catalog = item.catalog_;
    // Metadata identifying the table that should be deleted from.
    TableInfo *table_info = catalog->GetTable(item.table_oid_);
    IndexInfo *index_info = catalog->GetIndex(item.index_oid_);
    auto new_key = item.tuple_.KeyFromTuple(table_info->schema_, *(index_info->index_->GetKeySchema()),
                                            index_info->index_->GetKeyAttrs());
    if (item.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(new_key, item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(new_key, item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      // Delete the new key and insert the old key
      index_info->index_->DeleteEntry(new_key, item.rid_, txn);
      auto old_key = item.old_tuple_.KeyFromTuple(table_info->schema_, *(index_info->index_->GetKeySchema()),
                                                  index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(old_key, item.rid_, txn);
    }
    index_write_set->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
