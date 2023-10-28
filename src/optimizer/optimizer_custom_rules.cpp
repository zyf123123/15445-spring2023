#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"

// Note for 2023 Spring: You can add all optimizer rule implementations and apply the rules as you want in this file.
// Note that for some test cases, we force using starter rules, so that the configuration here won't take effects.
// Starter rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeConstantFolder(p);
  p = OptimizeEliminateTrueFilter(p);
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  // std::cout << p->ToString() << std::endl;
  p = OptimizeFliterPushDown(p);
  // std::cout << p->ToString() << std::endl;
  p = OptimizeColumnPruning(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeScanAsIndexScan(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);

  // std::cout << p->ToString() << std::endl;
  return p;
}

auto Optimizer::MatchIndex(const std::string &table_name, uint32_t index_key_idx)
    -> std::optional<std::tuple<index_oid_t, std::string>> {
  const auto key_attrs = std::vector{index_key_idx};
  for (const auto *index_info : catalog_.GetTableIndexes(table_name)) {
    if (key_attrs == index_info->index_->GetKeyAttrs()) {
      return std::make_optional(std::make_tuple(index_info->index_oid_, index_info->name_));
    }
  }
  return std::nullopt;
}

auto Optimizer::OptimizeScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      const auto &predicate = dynamic_cast<const LogicExpression *>(seq_scan_plan.filter_predicate_.get());
      if (predicate != nullptr) {
        const auto &comp_expr_left = dynamic_cast<const ComparisonExpression *>(predicate->GetChildAt(0).get());
        const auto &comp_expr_right = dynamic_cast<const ComparisonExpression *>(predicate->GetChildAt(1).get());
        if (comp_expr_left != nullptr && comp_expr_right != nullptr) {
          auto column_expr_left = dynamic_cast<const ColumnValueExpression *>(comp_expr_left->GetChildAt(0).get());
          auto constant_expr_left = dynamic_cast<const ConstantValueExpression *>(comp_expr_left->GetChildAt(1).get());
          auto column_expr_right = dynamic_cast<const ColumnValueExpression *>(comp_expr_right->GetChildAt(0).get());
          auto constant_expr_right =
              dynamic_cast<const ConstantValueExpression *>(comp_expr_right->GetChildAt(1).get());
          if (column_expr_left != nullptr && constant_expr_left != nullptr && column_expr_right != nullptr &&
              constant_expr_right != nullptr && !catalog_.GetTableIndexes(seq_scan_plan.table_name_).empty()) {
            const auto index = catalog_.GetTableIndexes(seq_scan_plan.table_name_)[0];
            auto index_scan_plan = std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, index->index_oid_);
            AbstractExpressionRef index_filter_predicate;

            int pos = 0;
            for (auto &expr : index->key_schema_.GetColumns()) {
              if (seq_scan_plan.OutputSchema()
                      .GetColumn(column_expr_left->GetColIdx())
                      .GetName()
                      .find(expr.GetName()) != std::string::npos) {
                auto new_comp_expr = ComparisonExpression(
                    std::make_shared<ColumnValueExpression>(0, pos, column_expr_left->GetReturnType()),
                    comp_expr_left->GetChildAt(1), comp_expr_left->comp_type_);
                index_scan_plan->AddIndexPredicate(std::make_shared<ComparisonExpression>(new_comp_expr));
              }
              if (seq_scan_plan.OutputSchema()
                      .GetColumn(column_expr_right->GetColIdx())
                      .GetName()
                      .find(expr.GetName()) != std::string::npos) {
                auto new_comp_expr = ComparisonExpression(
                    std::make_shared<ColumnValueExpression>(0, pos, column_expr_left->GetReturnType()),
                    comp_expr_right->GetChildAt(1), comp_expr_right->comp_type_);
                index_scan_plan->AddIndexPredicate(std::make_shared<ComparisonExpression>(new_comp_expr));
              }
              pos++;
            }
            return index_scan_plan;
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto ParseLeftPredicateIdx(const ComparisonExpression &comp_expr, const NestedLoopJoinPlanNode &nlj_plan)
    -> ComparisonExpression {
  int left_idx = 0;
  int right_idx = 0;
  auto column_left_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr.GetChildAt(0).get());
  auto column_right_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr.GetChildAt(1).get());
  if (column_left_expr != nullptr) {
    left_idx = static_cast<int>(column_left_expr->GetColIdx());
  }
  if (column_right_expr != nullptr) {
    right_idx = static_cast<int>(column_right_expr->GetColIdx());
  }
  if (column_left_expr == nullptr) {
    auto new_comp_expr =
        ComparisonExpression(comp_expr.GetChildAt(0),
                             std::make_shared<ColumnValueExpression>(0, right_idx, column_right_expr->GetReturnType()),
                             comp_expr.comp_type_);
    return new_comp_expr;
  }
  if (column_right_expr == nullptr) {
    auto new_comp_expr =
        ComparisonExpression(std::make_shared<ColumnValueExpression>(0, left_idx, column_left_expr->GetReturnType()),
                             comp_expr.GetChildAt(1), comp_expr.comp_type_);
    return new_comp_expr;
  }
  return comp_expr;
}

auto ParseRightPredicateIdx(const ComparisonExpression &comp_expr, const NestedLoopJoinPlanNode &nlj_plan)
    -> ComparisonExpression {
  int left_idx = 0;
  int right_idx = 0;
  auto column_left_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr.GetChildAt(0).get());
  auto column_right_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr.GetChildAt(1).get());
  if (column_left_expr != nullptr) {
    left_idx = static_cast<int>(column_left_expr->GetColIdx());
  }
  if (column_right_expr != nullptr) {
    right_idx = static_cast<int>(column_right_expr->GetColIdx());
  }
  if (column_left_expr == nullptr) {
    auto new_comp_expr =
        ComparisonExpression(comp_expr.GetChildAt(0),
                             std::make_shared<ColumnValueExpression>(0, right_idx, column_right_expr->GetReturnType()),
                             comp_expr.comp_type_);
    return new_comp_expr;
  }
  if (column_right_expr == nullptr) {
    auto new_comp_expr =
        ComparisonExpression(std::make_shared<ColumnValueExpression>(0, left_idx, column_left_expr->GetReturnType()),
                             comp_expr.GetChildAt(1), comp_expr.comp_type_);
    return new_comp_expr;
  }
  return comp_expr;
}

auto Optimizer::OptimizeFliterPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    if (child->GetType() == PlanType::Filter) {
      const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*child);
      const auto predicate = dynamic_cast<const LogicExpression *>(filter_plan.GetPredicate().get());
      auto child_plan = child->GetChildren()[0];
      if (predicate != nullptr && child_plan->GetType() == PlanType::NestedLoopJoin) {
        // std::cout << "filter_plan.GetPredicate():" << filter_plan.GetPredicate()->ToString() << std::endl;
        const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*child_plan);
        auto column_expr = dynamic_cast<const ColumnValueExpression *>(predicate->GetChildAt(0)->GetChildAt(0).get());
        if (column_expr == nullptr) {
          column_expr = dynamic_cast<const ColumnValueExpression *>(predicate->GetChildAt(0)->GetChildAt(1).get());
        }
        if (column_expr->GetTupleIdx() == 0) {
          // left join
          auto left_join = nlj_plan.GetLeftPlan();
          left_join = std::make_shared<FilterPlanNode>(nlj_plan.GetLeftPlan()->output_schema_,
                                                       filter_plan.GetPredicate(), left_join);
          NestedLoopJoinPlanNode new_nlj_plan(nlj_plan.output_schema_, left_join, nlj_plan.GetChildren()[1],
                                              nlj_plan.Predicate(), nlj_plan.GetJoinType());
          children.emplace_back(OptimizeFliterPushDown(new_nlj_plan.CloneWithChildren(new_nlj_plan.GetChildren())));
        } else {
          // right join
          auto right_join = nlj_plan.GetRightPlan();
          right_join =
              std::make_shared<FilterPlanNode>(filter_plan.output_schema_, filter_plan.GetPredicate(), right_join);
          NestedLoopJoinPlanNode new_nlj_plan(nlj_plan.output_schema_, nlj_plan.GetChildren()[0], right_join,
                                              nlj_plan.Predicate(), nlj_plan.GetJoinType());
          children.emplace_back(OptimizeFliterPushDown(new_nlj_plan.CloneWithChildren(new_nlj_plan.GetChildren())));
        }

      } else {
        children.emplace_back(OptimizeFliterPushDown(child));
      }
    } else if (child->GetType() == PlanType::NestedLoopJoin) {
      const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*child);
      auto predicate = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
      // LogicExpression left_predicate = LogicExpression(nullptr, nullptr, LogicType::And);
      // LogicExpression right_predicate = LogicExpression(nullptr, nullptr, LogicType::And);
      std::vector<ComparisonExpression> left_predicate;
      std::vector<ComparisonExpression> right_predicate;
      std::vector<ComparisonExpression> new_predicate;
      bool flag = false;
      std::vector<AbstractExpressionRef> exprs;
      while (predicate != nullptr) {
        // std::cout << predicate->ToString() << std::endl;
        for (int num = 0; num < 2; num++) {
          flag = false;
          auto comp_expr = dynamic_cast<const ComparisonExpression *>(predicate->GetChildAt(num).get());
          if (comp_expr == nullptr) {
            continue;
          }
          auto column_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
          auto constant_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->GetChildAt(1).get());
          if (column_expr != nullptr && constant_expr != nullptr) {
            auto col_idx = column_expr->GetColIdx();
            Column column = nlj_plan.GetLeftPlan()->OutputSchema().GetColumn(col_idx);
            if (column_expr->GetTupleIdx() == 0) {
              column = nlj_plan.GetLeftPlan()->OutputSchema().GetColumn(col_idx);
              for (auto &expr : nlj_plan.OutputSchema().GetColumns()) {
                if (expr.GetName() == column.GetName()) {
                  auto new_comp_expr = ParseLeftPredicateIdx(*comp_expr, nlj_plan);
                  // std::cout << new_comp_expr.ToString() << std::endl;
                  left_predicate.emplace_back(new_comp_expr);
                  flag = true;
                }
              }
            } else {
              column = nlj_plan.GetRightPlan()->OutputSchema().GetColumn(col_idx);
              for (auto &expr : nlj_plan.OutputSchema().GetColumns()) {
                if (expr.GetName() == column.GetName()) {
                  auto new_comp_expr = ParseRightPredicateIdx(*comp_expr, nlj_plan);
                  // std::cout << new_comp_expr.ToString() << std::endl;
                  right_predicate.emplace_back(new_comp_expr);
                  flag = true;
                }
              }
            }
          }
          constant_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->GetChildAt(0).get());
          column_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
          if (column_expr != nullptr && constant_expr != nullptr) {
            auto col_idx = column_expr->GetColIdx();
            Column column = nlj_plan.GetLeftPlan()->OutputSchema().GetColumn(col_idx);
            if (column_expr->GetTupleIdx() == 0) {
              column = nlj_plan.GetLeftPlan()->OutputSchema().GetColumn(col_idx);
              for (auto &expr : nlj_plan.OutputSchema().GetColumns()) {
                if (expr.GetName() == column.GetName()) {
                  auto new_comp_expr = ParseLeftPredicateIdx(*comp_expr, nlj_plan);
                  // std::cout << new_comp_expr.ToString() << std::endl;
                  left_predicate.emplace_back(new_comp_expr);
                  flag = true;
                }
              }
            } else {
              column = nlj_plan.GetRightPlan()->OutputSchema().GetColumn(col_idx);
              for (auto &expr : nlj_plan.OutputSchema().GetColumns()) {
                if (expr.GetName() == column.GetName()) {
                  auto new_comp_expr = ParseRightPredicateIdx(*comp_expr, nlj_plan);
                  // << new_comp_expr.ToString() << std::endl;
                  right_predicate.emplace_back(new_comp_expr);
                  flag = true;
                }
              }
            }
          }
          if (!flag) {
            new_predicate.emplace_back(*comp_expr);
          }
        }
        predicate = dynamic_cast<const LogicExpression *>(predicate->GetChildAt(0).get());
      }
      // std::cout << left_predicate.size() << " " << right_predicate.size() << new_predicate.size() << std::endl;

      std::vector<AbstractPlanNodeRef> new_children{child->GetChildren()[0], child->GetChildren()[1]};
      AbstractExpressionRef new_predict;
      AbstractExpressionRef child_new_predict;
      if (new_predicate.size() == 2) {
        // std::cout << new_predicate[0].ToString() << std::endl;

        auto left_column_expr = dynamic_cast<const ColumnValueExpression *>(new_predicate[0].GetChildAt(0).get());
        auto right_column_expr = dynamic_cast<const ColumnValueExpression *>(new_predicate[0].GetChildAt(1).get());
        ColumnValueExpression new_right_column_expr(
            right_column_expr->GetTupleIdx(), right_column_expr->GetColIdx() - 2, right_column_expr->GetReturnType());

        auto new_comp_expr = ComparisonExpression(std::make_shared<ColumnValueExpression>(*left_column_expr),
                                                  std::make_shared<ColumnValueExpression>(new_right_column_expr),
                                                  new_predicate[0].comp_type_);

        AbstractExpressionRef left_expr_ref = std::make_shared<ComparisonExpression>(new_comp_expr);
        AbstractExpressionRef right_expr_ref = std::make_shared<ComparisonExpression>(new_predicate[1]);

        new_predict = right_expr_ref;
        child_new_predict = left_expr_ref;
      } else {
        new_predict = nlj_plan.Predicate();
      }

      if (left_predicate.size() == 2) {
        // std::cout << left_predicate[0].ToString() << std::endl;
        AbstractExpressionRef left_expr_ref = std::make_shared<ComparisonExpression>(left_predicate[0]);
        AbstractExpressionRef right_expr_ref = std::make_shared<ComparisonExpression>(left_predicate[1]);
        LogicExpression left_logic_expr(left_expr_ref, right_expr_ref, LogicType::And);
        AbstractExpressionRef left_expr = std::make_shared<LogicExpression>(left_logic_expr);
        NestedLoopJoinPlanNode left_nlj_plan(
            nlj_plan.GetLeftPlan()->output_schema_, child->GetChildren()[0]->GetChildren()[0],
            child->GetChildren()[0]->GetChildren()[1], child_new_predict, nlj_plan.GetJoinType());
        AbstractPlanNodeRef left_nlj_plan_ref = std::make_shared<NestedLoopJoinPlanNode>(left_nlj_plan);
        FilterPlanNode left_filter_plan(nlj_plan.GetLeftPlan()->output_schema_, left_expr, left_nlj_plan_ref);
        AbstractPlanNodeRef left_plan = std::make_shared<FilterPlanNode>(left_filter_plan);
        new_children[0] = left_plan;
        // std::vector<AbstractPlanNodeRef> left_children{left_plan, child->GetChildren()[1]};
        // children.emplace_back(OptimizeFliterPushDown(child->CloneWithChildren(left_children)));
      }
      if (right_predicate.size() == 2) {
        // std::cout << right_predicate[0].ToString() << std::endl;

        AbstractExpressionRef left_expr_ref = std::make_shared<ComparisonExpression>(right_predicate[0]);
        AbstractExpressionRef right_expr_ref = std::make_shared<ComparisonExpression>(right_predicate[1]);
        LogicExpression right_logic_expr(left_expr_ref, right_expr_ref, LogicType::And);
        AbstractExpressionRef right_expr = std::make_shared<LogicExpression>(right_logic_expr);
        FilterPlanNode right_filter_plan(nlj_plan.GetRightPlan()->output_schema_, right_expr,
                                         nlj_plan.GetChildren()[1]);
        AbstractPlanNodeRef right_plan = std::make_shared<FilterPlanNode>(right_filter_plan);
        new_children[1] = right_plan;
        // std::vector<AbstractPlanNodeRef> right_children{child->GetChildren()[0], right_plan};
        // children.emplace_back(OptimizeFliterPushDown(child->CloneWithChildren(right_children)));
      }
      auto new_child = NestedLoopJoinPlanNode(nlj_plan.output_schema_, new_children[0], new_children[1], new_predict,
                                              nlj_plan.GetJoinType());
      children.emplace_back((OptimizeFliterPushDown(new_child.CloneWithChildren(new_children))));

    } else {
      children.emplace_back(OptimizeFliterPushDown(child));
    }
  }
  return plan->CloneWithChildren(std::move(children));
}

auto Optimizer::OptimizeConstantFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    if (child->GetType() == PlanType::Filter) {
      const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*child);
      // std::cout << "filter_plan.GetPredicate():" << filter_plan.GetPredicate() << std::endl;
      const auto predicate = dynamic_cast<const ComparisonExpression *>(filter_plan.GetPredicate().get());
      if (predicate != nullptr && predicate->comp_type_ == ComparisonType::Equal) {
        const auto left = dynamic_cast<const ConstantValueExpression *>(predicate->GetChildAt(0).get());
        const auto right = dynamic_cast<const ConstantValueExpression *>(predicate->GetChildAt(1).get());
        if (left != nullptr && right != nullptr && left != right) {
          // std::cout << "left:" << left->val_.ToString() << std::endl;
          auto empty_value = std::make_shared<ValuesPlanNode>(child->output_schema_,
                                                              std::vector<std::vector<AbstractExpressionRef>>{});
          return OptimizeConstantFolder(empty_value);
        }
      }
      children.emplace_back(OptimizeConstantFolder(child));
    } else {
      children.emplace_back(OptimizeConstantFolder(child));
    }
  }
  return plan->CloneWithChildren(std::move(children));
}

auto ParseAggregationExpression(const AbstractExpressionRef &expr, std::unordered_map<int, int> &column_idx,
                                const Schema &schema, const AggregationPlanNode &plan,
                                std::vector<AbstractExpressionRef> &exprs, std::vector<AggregationType> &agg_types)
    -> void {
  if (auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); column_value_expr != nullptr) {
    // auto groupby_num = plan.GetGroupBys().size();

    // std::cout << column_value_expr->GetColIdx() << std::endl;
    // columns.emplace_back(schema.GetColumn(column_value_expr->GetColIdx() + groupby_num));
    column_idx.emplace(column_value_expr->GetColIdx(), 1);
    // exprs.emplace_back(plan.GetAggregates()[column_value_expr->GetColIdx()]);
    // agg_types.emplace_back(plan.GetAggregateTypes()[column_value_expr->GetColIdx()]);
  } else {
    for (const auto &child : expr->GetChildren()) {
      ParseAggregationExpression(child, column_idx, schema, plan, exprs, agg_types);
    }
  }
}
auto ParseGroupByExpression(const AbstractExpressionRef &expr, std::vector<Column> &columns, const Schema &schema,
                            const AggregationPlanNode &plan) -> void {
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    columns.emplace_back(schema.GetColumn(column_value_expr->GetColIdx()));
    // column_idx.emplace(column_value_expr->GetColIdx(), 1);
  } else if (const auto *arithmetic_value_expr = dynamic_cast<const ArithmeticExpression *>(expr.get());
             arithmetic_value_expr != nullptr) {
    columns.emplace_back(schema.GetColumn(0));
  } else {
    for (const auto &child : expr->GetChildren()) {
      ParseGroupByExpression(child, columns, schema, plan);
    }
  }
}
auto ParseExpression(const AbstractExpressionRef &expr, std::vector<Column> &columns, const Schema &schema,
                     const ProjectionPlanNode &plan, std::vector<AbstractExpressionRef> &exprs) -> void {
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    columns.emplace_back(schema.GetColumn(column_value_expr->GetColIdx()));
    exprs.emplace_back(plan.GetExpressions()[column_value_expr->GetColIdx()]);
  } else {
    for (const auto &child : expr->GetChildren()) {
      ParseExpression(child, columns, schema, plan, exprs);
    }
  }
}
auto Optimizer::OptimizeColumnPruning(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    if (plan->GetType() == PlanType::Projection) {
      const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
      BUSTUB_ASSERT(plan->children_.size() == 1, "must have exactly one children");
      const auto &child_plan = plan->children_[0];
      const auto &child_schema = child_plan->OutputSchema();
      // Merge two sequential projections
      if (child->GetType() == PlanType::Projection) {
        const auto &child_projection_plan = dynamic_cast<const ProjectionPlanNode &>(*child);
        std::vector<Column> columns;
        std::vector<AbstractExpressionRef> expressions;
        for (const auto &expr : projection_plan.GetExpressions()) {
          ParseExpression(expr, columns, child_schema, child_projection_plan, expressions);
        }
        SchemaRef schema = std::make_shared<Schema>(columns);
        auto projection_plan_node = ProjectionPlanNode(plan->output_schema_, expressions, child);
        children.emplace_back(OptimizeColumnPruning(projection_plan_node.CloneWithChildren(child_plan->GetChildren())));
        return OptimizeColumnPruning(projection_plan_node.CloneWithChildren(child_plan->GetChildren()));
      }
      if (child->GetType() == PlanType::Aggregation) {
        // Merge Projection and Aggregate
        // BUSTUB_ASSERT(child->children_.size() == 1, "must have exactly one children");
        const auto &old_aggregation_plan = dynamic_cast<const AggregationPlanNode &>(*child);
        std::vector<Column> columns;
        std::unordered_map<int, int> column_idx;
        std::vector<AbstractExpressionRef> aggregate_expressions;
        std::vector<AbstractExpressionRef> group_by_expressions = old_aggregation_plan.GetGroupBys();
        std::vector<AggregationType> aggregation_types;

        for (const auto &expr : old_aggregation_plan.GetGroupBys()) {
          ParseGroupByExpression(expr, columns, child->GetChildren()[0]->OutputSchema(), old_aggregation_plan);
        }
        // int pos = 0;
        for (const auto &expr : projection_plan.GetExpressions()) {
          ParseAggregationExpression(expr, column_idx, child_schema, old_aggregation_plan, aggregate_expressions,
                                     aggregation_types);
        }
        std::vector<int> column_idx_vec;
        column_idx_vec.reserve(column_idx.size());
        for (const auto &pair : column_idx) {
          column_idx_vec.emplace_back(pair.first);
        }
        std::sort(column_idx_vec.begin(), column_idx_vec.end());
        for (const auto &idx : column_idx_vec) {
          if (idx < static_cast<int>(old_aggregation_plan.GetGroupBys().size())) {
            continue;
          }
          aggregate_expressions.emplace_back(
              old_aggregation_plan.GetAggregates()[idx - old_aggregation_plan.GetGroupBys().size()]);
          aggregation_types.emplace_back(
              old_aggregation_plan.GetAggregateTypes()[idx - old_aggregation_plan.GetGroupBys().size()]);
          columns.emplace_back(child_schema.GetColumn(idx));
        }

        SchemaRef schema = std::make_shared<Schema>(columns);
        auto aggregation_plan_node =
            AggregationPlanNode(schema, old_aggregation_plan.GetChildPlan(), group_by_expressions,
                                aggregate_expressions, aggregation_types);
        children.emplace_back(
            OptimizeColumnPruning(aggregation_plan_node.CloneWithChildren(child_plan->GetChildren())));
      } else {
        children.emplace_back(OptimizeColumnPruning(child));
      }
    } else {
      children.emplace_back(OptimizeColumnPruning(child));
    }
  }
  return plan->CloneWithChildren(std::move(children));
}

}  // namespace bustub
