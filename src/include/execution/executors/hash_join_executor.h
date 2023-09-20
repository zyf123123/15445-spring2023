//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> attributes_;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint i = 0; i < attributes_.size(); i++) {
      if (attributes_[i].CompareEquals(other.attributes_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
  auto Hash() -> hash_t {
    size_t hash = 0;
    for (const auto &attr : attributes_) {
      if (!attr.IsNull()) {
        hash = bustub::HashUtil::CombineHashes(hash, bustub::HashUtil::HashValue(&attr));
      }
    }
    return hash;
  }
};

struct HashJoinValue {
  /** The hash join values */
  std::vector<Tuple> tuples_;
};

class SimpleHashJoinTable {
 public:
  SimpleHashJoinTable() = default;

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  void Insert(HashJoinKey &key, HashJoinValue &value) { ht_[key.Hash()] = HashJoinValue(value); }

  auto Get(HashJoinKey &key) -> HashJoinValue {
    auto value = ht_.find(key.Hash());
    if (value == ht_.end()) {
      return HashJoinValue{};
    }
    return value->second;
  }

 private:
  std::unordered_map<hash_t, HashJoinValue> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The child executor that produces tuples from the left side of join. */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The child executor that produces tuples from the right side of join. */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** The expression to compute the left JOIN key. */
  SimpleHashJoinTable hash_table_;
  /** The tuple from the left child executor. */
  Tuple left_tuple_;
  /** right tuple number in hash table*/
  uint right_tuple_num_ = 0;
};

}  // namespace bustub
