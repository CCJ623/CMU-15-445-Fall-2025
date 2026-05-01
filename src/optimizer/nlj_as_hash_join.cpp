//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

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

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  const auto &predicate = nlj_plan.Predicate();
  if (predicate == nullptr) {
    return optimized_plan;
  }

  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;

  auto extract_keys = [&left_keys, &right_keys](auto &&self, const AbstractExpressionRef &expr) -> bool {
    if (auto logic = std::dynamic_pointer_cast<LogicExpression>(expr)) {
      if (logic->logic_type_ != LogicType::And) {  // only support logic AND
        return false;
      }
      return self(self, logic->GetChildAt(0)) && self(self, logic->GetChildAt(1));
    }

    // not logic expr
    auto comp = std::dynamic_pointer_cast<ComparisonExpression>(expr);
    if (comp == nullptr || comp->comp_type_ != ComparisonType::Equal) {  // only support equal
      return false;
    }

    auto left_child = std::dynamic_pointer_cast<ColumnValueExpression>(comp->GetChildAt(0));
    auto right_child = std::dynamic_pointer_cast<ColumnValueExpression>(comp->GetChildAt(1));

    if (left_child == nullptr || right_child == nullptr) {
      return false;
    }

    if (left_child->GetTupleIdx() == 0 && right_child->GetTupleIdx() == 1) {
      left_keys.push_back(left_child);
      right_keys.push_back(right_child);
      return true;
    }
    if (left_child->GetTupleIdx() == 1 && right_child->GetTupleIdx() == 0) {
      left_keys.push_back(right_child);
      right_keys.push_back(left_child);
      return true;
    }

    return false;
  };

  // 4. Match and Replace
  if (extract_keys(extract_keys, predicate)) {
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_keys, right_keys, nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub