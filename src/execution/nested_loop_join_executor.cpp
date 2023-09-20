//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { left_executor_->Init(); }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto predicate_expr = plan_->Predicate();
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();

  if (left_tuple_.GetData() == nullptr) {
    if (!left_executor_->Next(&left_tuple_, rid)) {
      return false;
    }
    right_executor_->Init();
  }

  if (right_tuples_.empty()) {
    Tuple right_tuple{};
    while (true) {
      if (!right_executor_->Next(&right_tuple, rid)) {
        break;
      }
      right_tuples_.emplace_back(right_tuple);
    }
  }
  std::vector<Value> values;
  if (right_tuples_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
    // right table is empty
    // left join null cols
    for (const auto &col : left_schema.GetColumns()) {
      auto value = left_tuple_.GetValue(&left_schema, left_schema.GetColIdx(col.GetName()));
      values.emplace_back(value);
    }
    for (const auto &col : right_schema.GetColumns()) {
      values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
    }
    left_tuple_ = Tuple();
  }

  while (right_tuple_num_ < right_tuples_.size()) {
    auto right_tuple = right_tuples_[right_tuple_num_++];
    Value res_value = predicate_expr->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
    if (!res_value.IsNull() && res_value.GetAs<bool>()) {
      for (const auto &col : left_schema.GetColumns()) {
        auto value = left_tuple_.GetValue(&left_schema, left_schema.GetColIdx(col.GetName()));
        values.emplace_back(value);
      }
      for (const auto &col : right_schema.GetColumns()) {
        auto value = right_tuple.GetValue(&right_schema, right_schema.GetColIdx(col.GetName()));
        values.emplace_back(value);
      }
      is_joined_ = true;
    }

    if (right_tuple_num_ == right_tuples_.size()) {
      if (!is_joined_ && plan_->GetJoinType() == JoinType::LEFT) {
        for (const auto &col : left_schema.GetColumns()) {
          auto value = left_tuple_.GetValue(&left_schema, left_schema.GetColIdx(col.GetName()));
          values.emplace_back(value);
        }
        for (const auto &col : right_schema.GetColumns()) {
          values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
      }
      right_tuple_num_ = 0;
      is_joined_ = false;
      if (!left_executor_->Next(&left_tuple_, rid)) {
        left_tuple_ = Tuple();
        break;
      }
      right_executor_->Init();
    }
    if (!values.empty()) {
      break;
    }
  }

  if (values.empty()) {
    return false;
  }
  *tuple = Tuple(values, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
