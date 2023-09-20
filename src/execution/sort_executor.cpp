#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(),
            [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                           const Tuple &tuple_b) {
              for (const auto &order_key : order_bys) {
                auto value_a = order_key.second->Evaluate(&tuple_a, schema);
                auto value_b = order_key.second->Evaluate(&tuple_b, schema);
                switch (order_key.first) {
                  case OrderByType::INVALID:
                    break;
                  case OrderByType::DEFAULT:
                  case OrderByType::ASC:
                    if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
                      return true;
                    } else if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
                      return false;
                    }
                    break;
                  case OrderByType::DESC:
                    if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
                      return true;
                    } else if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
                      return false;
                    }
                    break;
                }
              }
              return false;
            });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < tuples_.size()) {
    *tuple = tuples_[index_];
    index_++;
    return true;
  }
  return false;
}

}  // namespace bustub
