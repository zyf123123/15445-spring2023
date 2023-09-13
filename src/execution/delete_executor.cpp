//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  first_time_ = true;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!first_time_) {
    return false;
  }
  int update_num = 0;
  while (true) {
    Tuple child_tuple{};
    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, rid);

    if (!status) {
      break;
    }
    // delete before tuple
    auto tuple_meta_delete = table_info_->table_->GetTupleMeta(*rid);
    // before_tuple.second = Tuple{};
    tuple_meta_delete.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta_delete, *rid);

    // update index
    auto table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for (auto index_info : index_infos) {
      const auto index_key = child_tuple.KeyFromTuple(exec_ctx_->GetCatalog()->GetTable(table_name)->schema_,
                                                      (index_info->key_schema_), index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key, RID(), exec_ctx_->GetTransaction());
    }
    update_num++;
  }
  char *storage = new char[sizeof(uint32_t) + sizeof(update_num)];
  uint32_t sz = sizeof(update_num);
  memcpy(storage, &sz, sizeof(uint32_t));
  memcpy(storage + sizeof(uint32_t), &update_num, sz);
  tuple->DeserializeFrom(storage);
  delete[] storage;
  first_time_ = false;
  return true;
}

}  // namespace bustub
