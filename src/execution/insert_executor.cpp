//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  first_time_ = true;
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                plan_->TableOid())) {
      throw ExecutionException("Transaction abort");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Transaction abort");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!first_time_) {
    return false;
  }
  int insert_num = 0;
  while (true) {
    Tuple child_tuple{};
    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, rid);

    if (!status) {
      break;
    }
    auto tuple_meta = TupleMeta();
    tuple_meta.is_deleted_ = false;
    tuple_meta.insert_txn_id_ = exec_ctx_->GetTransaction()->GetTransactionId();
    auto tuple_slot = table_heap_->InsertTuple(tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                               exec_ctx_->GetTransaction(), plan_->TableOid());
    if (tuple_slot == std::nullopt) {
      return false;
    }
    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                plan_->TableOid(), tuple_slot.value())) {
        throw ExecutionException("Transaction abort");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Transaction abort");
    }
    auto table_write_record = TableWriteRecord(plan_->TableOid(), tuple_slot.value(), table_heap_);
    table_write_record.wtype_ = WType::INSERT;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_write_record);

    // update index
    auto table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for (auto index_info : index_infos) {
      const auto index_key = child_tuple.KeyFromTuple(exec_ctx_->GetCatalog()->GetTable(table_name)->schema_,
                                                      (index_info->key_schema_), index_info->index_->GetKeyAttrs());
      // std :: cout << "index_key: " << index_key.ToString(&index_info->key_schema_) << std::endl;
      index_info->index_->InsertEntry(index_key, tuple_slot.value(), exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(IndexWriteRecord(
          *rid, plan_->TableOid(), WType::INSERT, child_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()));
    }
    insert_num++;
  }
  char *storage = new char[sizeof(uint32_t) + sizeof(insert_num)];
  uint32_t sz = sizeof(insert_num);
  memcpy(storage, &sz, sizeof(uint32_t));
  memcpy(storage + sizeof(uint32_t), &insert_num, sz);
  tuple->DeserializeFrom(storage);
  delete[] storage;
  first_time_ = false;
  return true;
}

}  // namespace bustub
