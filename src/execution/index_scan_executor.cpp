//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  auto iter = tree_->GetBeginIterator();
  iter_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(std::move(iter));
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    return false;
  }
  auto table_name = index_info_->index_->GetMetadata()->GetTableName();
  auto table_heap = exec_ctx_->GetCatalog()->GetTable(table_name)->table_.get();
  while (*iter_ != tree_->GetEndIterator()) {
    auto tuple_rid = (*(*iter_)).second;
    auto tuple_scan = table_heap->GetTuple(tuple_rid);
    if (tuple_scan.first.is_deleted_) {
      ++(*iter_);

      if (iter_->IsEnd()) {
        return false;
      }
      continue;
    }
    *tuple = tuple_scan.second;
    *rid = tuple_rid;
    ++(*iter_);

    break;
  }
  return true;
}

}  // namespace bustub
