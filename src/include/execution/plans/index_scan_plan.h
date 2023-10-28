//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>

#include <vector>
#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output The output format of this scan plan node
   * @param table_oid The identifier of table to be scanned
   */
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid)
      : AbstractPlanNode(std::move(output), {}), index_oid_(index_oid) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  auto AddIndexPredicate(const AbstractExpressionRef &expr) -> void { filter_predicate_.emplace_back(expr); }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table whose tuples should be scanned. */
  index_oid_t index_oid_;

  // Add anything you want here for index lookup
  std::vector<AbstractExpressionRef> filter_predicate_;

 protected:
  auto PlanNodeToString() const -> std::string override {
    auto result = fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
    for (auto &expr : filter_predicate_) {
      result += fmt::format("{{ filter={} }}", expr);
    }
    return result;
  }
};

}  // namespace bustub
