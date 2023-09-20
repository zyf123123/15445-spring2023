//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  auto aht = SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes());
  aht_ = std::make_unique<SimpleAggregationHashTable>(aht);
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  // Get the next tuple
  auto status = child_executor_->Next(&child_tuple, rid);
  if (!status && !first_time_ && *aht_iterator_ == aht_->End()) {
    return false;
  }
  while (first_time_) {
    if (!status) {
      if (plan_->GetGroupBys().empty()) {
        aht_->InsertCombine(AggregateKey(), AggregateValue());
      }
    } else {
      auto agg_key = MakeAggregateKey(&child_tuple);
      auto agg_val = MakeAggregateValue(&child_tuple);
      aht_->InsertCombine(agg_key, agg_val);
    }
    if (!child_executor_->Next(&child_tuple, rid)) {
      break;
    }
  }
  if (first_time_) {
    aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
    first_time_ = false;
  }
  std::vector<Value> values;
  if (*aht_iterator_ != aht_->End()) {
    auto key = (*aht_iterator_).Key().group_bys_;
    auto value = (*aht_iterator_).Val().aggregates_;
    if (key.empty()) {
      values.insert(values.end(), value.begin(), value.end());
    } else {
      values.insert(values.end(), key.begin(), key.end());
      values.insert(values.end(), value.begin(), value.end());
    }
    ++*aht_iterator_;
  }
  if (!values.empty()) {
    *tuple = Tuple(values, &plan_->OutputSchema());
  } else {
    return false;
  }
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
