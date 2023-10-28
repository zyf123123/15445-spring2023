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
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  auto iter = tree_->GetBeginIterator();
  if (!plan_->filter_predicate_.empty()) {
    /*
    std::vector<Value> values;
    auto predicate = plan_->filter_predicate_[0];
    auto constant_value_expression = dynamic_cast<ConstantValueExpression *>(predicate.get()->GetChildren()[1].get());
    values.emplace_back(constant_value_expression->val_);*/
    IntegerKeyType start_key;
    int32_t start_x = 90;
    int32_t start_y = 10;
    memcpy(start_key.data_, &start_x, sizeof(int32_t));
    memcpy(start_key.data_ + 4, &start_y, sizeof(int32_t));

    IntegerKeyType end_key;
    int32_t end_x = 100;
    int32_t end_y = 10;
    memcpy(end_key.data_, &end_x, sizeof(int32_t));
    memcpy(end_key.data_ + 4, &end_y, sizeof(int32_t));
    iter_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree_->GetBeginIterator(start_key));
    end_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree_->GetBeginIterator(end_key));
  } else {
    iter_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(std::move(iter));
  }
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
    if (!plan_->filter_predicate_.empty()) {
      bool predicate = true;
      for (auto &expr : plan_->filter_predicate_) {
        auto value = expr->Evaluate(&tuple_scan.second, plan_->OutputSchema());
        if (!value.GetAs<bool>()) {
          predicate = false;
          break;
        }
      }
      if (!predicate) {
        ++(*iter_);

        if (iter_->IsEnd() || iter_.get() == end_.get()) {
          return false;
        }
        continue;
      }
    }
    *tuple = tuple_scan.second;
    *rid = tuple_rid;
    ++(*iter_);

    break;
  }
  return true;
}

}  // namespace bustub
