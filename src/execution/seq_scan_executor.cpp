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
  auto table_iter = table_heap->MakeIterator();
  table_iter_ = std::make_unique<TableIterator>(std::move(table_iter));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_->IsEnd()) {
    return false;
  }
  auto tuple_meta = table_iter_->GetTuple().first;
  while (tuple_meta.is_deleted_) {
    ++(*table_iter_);
    if (table_iter_->IsEnd()) {
      return false;
    }
    tuple_meta = table_iter_->GetTuple().first;
  }
  *tuple = table_iter_->GetTuple().second;
  *rid = table_iter_->GetRID();
  ++(*table_iter_);
  return true;
}

}  // namespace bustub
