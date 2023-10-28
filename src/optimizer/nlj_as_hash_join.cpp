#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    auto expr_pre = nlj_plan.Predicate().get();
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(expr_pre); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            if (left_expr->GetTupleIdx() == 0) {
              // left side expr
              auto left_expr_tuple_0 =
                  std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
              auto right_expr_tuple_0 =
                  std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType());
              return std::make_shared<HashJoinPlanNode>(
                  nlj_plan.output_schema_, optimized_plan->GetChildAt(0), optimized_plan->GetChildAt(1),
                  std::vector<AbstractExpressionRef>{left_expr_tuple_0},
                  std::vector<AbstractExpressionRef>{right_expr_tuple_0}, nlj_plan.GetJoinType());
            }
            if (left_expr->GetTupleIdx() == 1) {
              auto right_expr_tuple_1 =
                  std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType());
              auto left_expr_tuple_1 =
                  std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
              return std::make_shared<HashJoinPlanNode>(
                  nlj_plan.output_schema_, optimized_plan->GetChildAt(0), optimized_plan->GetChildAt(1),
                  std::vector<AbstractExpressionRef>{left_expr_tuple_1},
                  std::vector<AbstractExpressionRef>{right_expr_tuple_1}, nlj_plan.GetJoinType());
            }
          }
        }
      }
    } else if (const auto *expr_logic = dynamic_cast<const LogicExpression *>(expr_pre); expr_logic != nullptr) {
      if (expr_logic->logic_type_ == LogicType::And) {
        std::vector<AbstractExpressionRef> left_exprs;
        std::vector<AbstractExpressionRef> right_exprs;
        for (const auto &left_expr : expr_logic->children_) {
          for (auto const &expr_tuple : left_expr->children_) {
            if (auto column_expr = dynamic_cast<ColumnValueExpression *>(expr_tuple.get());
                column_expr != nullptr && column_expr->GetTupleIdx() == 0) {
              left_exprs.emplace_back(expr_tuple);
            } else {
              right_exprs.emplace_back(expr_tuple);
            }
          }
        }
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
