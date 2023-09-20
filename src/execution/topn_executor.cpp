#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [order_bys = plan_->order_bys_, schema = child_executor_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                          const Tuple &tuple_b) {
    for (const auto &order_key : order_bys) {
      auto value_a = order_key.second->Evaluate(&tuple_a, schema);
      auto value_b = order_key.second->Evaluate(&tuple_b, schema);
      switch (order_key.first) {
        case OrderByType::INVALID:
          break;
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return true;
          } else if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return true;
          } else if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
          break;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    pq.push(tuple);
  }
  uint num = 1;
  while (!pq.empty()) {
    top_entries_.push_back(pq.top());
    pq.pop();
    num++;
    if (num > plan_->GetN()) {
      break;
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < top_entries_.size()) {
    *tuple = top_entries_[index_];
    index_++;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); }

}  // namespace bustub
