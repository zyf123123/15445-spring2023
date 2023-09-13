//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  first_time_ = true;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    auto tuple_delete = table_info_->table_->GetTuple(*rid).second;
    // before_tuple.second = Tuple{};
    tuple_meta_delete.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta_delete, *rid);

    // get updated values
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    child_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    auto tuple_meta = TupleMeta();
    tuple_meta.is_deleted_ = false;

    auto tuple_slot = table_info_->table_->InsertTuple(tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                                       exec_ctx_->GetTransaction(), plan_->TableOid());
    if (tuple_slot == std::nullopt) {
      return false;
    }

    // update index
    auto table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for (auto index_info : index_infos) {
      const auto index_key_delete =
          tuple_delete.KeyFromTuple(exec_ctx_->GetCatalog()->GetTable(table_name)->schema_, (index_info->key_schema_),
                                    index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key_delete, RID(), exec_ctx_->GetTransaction());
      const auto index_key = child_tuple.KeyFromTuple(exec_ctx_->GetCatalog()->GetTable(table_name)->schema_,
                                                      (index_info->key_schema_), index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, tuple_slot.value(), exec_ctx_->GetTransaction());
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
