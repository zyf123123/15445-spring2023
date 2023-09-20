//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  hash_table_ = SimpleHashJoinTable();
  left_executor_->Init();
  right_executor_->Init();
  Tuple right_tuple{};
  RID rid;
  while (true) {
    if (!right_executor_->Next(&right_tuple, &rid)) {
      break;
    }
    HashJoinKey key;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      key.attributes_.emplace_back(expr->Evaluate(&right_tuple, right_executor_->GetOutputSchema()));
    }
    HashJoinValue value = hash_table_.Get(key);
    value.tuples_.emplace_back(right_tuple);
    hash_table_.Insert(key, value);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  std::vector<Value> values;
  while (true) {
    if (right_tuple_num_ == 0) {
      if (!left_executor_->Next(&left_tuple_, rid)) {
        return false;
      }
    }
    HashJoinKey key;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      key.attributes_.emplace_back(expr->Evaluate(&left_tuple_, left_executor_->GetOutputSchema()));
    }
    if (hash_table_.Get(key).tuples_.empty()) {
      // right col dont have value
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (const auto &col : left_schema.GetColumns()) {
          auto value = left_tuple_.GetValue(&left_schema, left_schema.GetColIdx(col.GetName()));
          values.emplace_back(value);
        }
        for (const auto &col : right_schema.GetColumns()) {
          values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
      }
    } else {
      // right col have value
      auto right_tuple = hash_table_.Get(key).tuples_;
      HashJoinKey right_key;
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        right_key.attributes_.emplace_back(
            expr->Evaluate(&right_tuple[right_tuple_num_], right_executor_->GetOutputSchema()));
      }
      if (right_key == key) {
        // hash_t may same
        for (const auto &col : left_schema.GetColumns()) {
          auto value = left_tuple_.GetValue(&left_schema, left_schema.GetColIdx(col.GetName()));
          values.emplace_back(value);
        }
        for (const auto &col : right_schema.GetColumns()) {
          auto value = right_tuple[right_tuple_num_].GetValue(&right_schema, right_schema.GetColIdx(col.GetName()));
          values.emplace_back(value);
        }
      }
      if (right_tuple_num_ != right_tuple.size() - 1) {
        right_tuple_num_++;
      } else {
        right_tuple_num_ = 0;
      }
    }
    if (!values.empty()) {
      break;
    }
  }
  // std::cout << values.size() << plan_->OutputSchema().GetColumnCount() << std::endl;
  *tuple = Tuple(values, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
