//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "binder/bound_expression.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

#include "optimizer/optimizer.h"
#include "type/value.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  using ComparisonExpressionRef = std::shared_ptr<ComparisonExpression>;
  using LogicExpressionRef = std::shared_ptr<LogicExpression>;
  using ColumnValueExpressionRef = std::shared_ptr<ColumnValueExpression>;
  using ConstantValueExpressionRef = std::shared_ptr<ConstantValueExpression>;

  // get column and constant in a comparision expression
  // comparision must be equal type
  // return copy of ComparisonExpressionRef and ConstantValueExpressionRef
  // return nullopt if invalid
  auto get_column_and_constant = [](const ComparisonExpressionRef &comparision)
      -> std::optional<std::pair<ColumnValueExpressionRef, ConstantValueExpressionRef>> {
    if (comparision == nullptr) {
      return std::nullopt;
    }
    // only support equal
    if (comparision->comp_type_ != ComparisonType::Equal) {
      return std::nullopt;
    }

    auto left = comparision->GetChildAt(0);
    auto right = comparision->GetChildAt(1);

    // try left as column
    {
      const auto column = std::dynamic_pointer_cast<ColumnValueExpression>(left);
      const auto constant = std::dynamic_pointer_cast<ConstantValueExpression>(right);
      if (column != nullptr && constant != nullptr) {
        return std::make_pair(column, constant);
      }
    }

    // try right as column
    {
      const auto column = std::dynamic_pointer_cast<ColumnValueExpression>(right);
      const auto constant = std::dynamic_pointer_cast<ConstantValueExpression>(left);
      if (column != nullptr && constant != nullptr) {
        return std::make_pair(column, constant);
      }
    }

    return std::nullopt;
  };

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  // still seq scan plan
  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  BUSTUB_ASSERT(seq_scan_plan.children_.empty(), "Optimizer::OptimizeSeqScanAsIndexScan(): must have zero children");
  const auto &filter = seq_scan_plan.filter_predicate_;

  // comparision expression
  if (auto comparision = std::dynamic_pointer_cast<ComparisonExpression>(filter); comparision != nullptr) {
    auto column_constant_pair = get_column_and_constant(comparision);
    // invalid
    if (!column_constant_pair.has_value()) {
      return optimized_plan;
    }

    auto [column, constant] = *column_constant_pair;
    auto indexes = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
    std::optional<index_oid_t> index_oid = std::nullopt;
    for (const auto &index_info : indexes) {
      auto key_attr = index_info->index_->GetKeyAttrs();
      if (key_attr.size() == 1 && key_attr.front() == column->GetColIdx()) {
        index_oid = index_info->index_oid_;
        break;
      }
    }

    // no index on this column
    if (!index_oid.has_value()) {
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> point_keys{};
    point_keys.push_back(constant);
    return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.GetTableOid(), *index_oid,
                                               nullptr, point_keys);
  }

  std::optional<uint32_t> column_index = std::nullopt;
  std::optional<index_oid_t> index_oid = std::nullopt;
  std::vector<AbstractExpressionRef> point_keys{};

  // extract logic recursively
  // if failed, set column_index = nullopt, and clear point_keys
  auto extract_logic = [&get_column_and_constant, &column_index, &index_oid, &point_keys,
                        &table_name = seq_scan_plan.table_name_,
                        &catalog = this->catalog_](auto &&self, const LogicExpressionRef &logic) {
    // only suppor or
    if (logic->logic_type_ != LogicType::Or) {
      column_index.reset();
      point_keys.clear();
      return;
    }

    const auto &left_child = logic->GetChildAt(0);
    const auto &right_child = logic->GetChildAt(1);
    std::vector<ComparisonExpressionRef> comparisions;

    // deal with left child
    auto child_logic = std::dynamic_pointer_cast<LogicExpression>(left_child);
    if (child_logic != nullptr) {  // left child is logic
      self(self, child_logic);
      if (!column_index.has_value() || point_keys.empty()) {
        column_index.reset();
        point_keys.clear();
        return;
      }
    } else {
      auto child_comparision = std::dynamic_pointer_cast<ComparisonExpression>(left_child);
      if (child_comparision == nullptr) {  // neither logic nor comparison, failed
        column_index.reset();
        point_keys.clear();
        return;
      }
      comparisions.push_back(child_comparision);
    }

    // deal with right child
    child_logic = std::dynamic_pointer_cast<LogicExpression>(right_child);
    if (child_logic != nullptr) {  // right child is logic
      self(self, child_logic);
      if (!column_index.has_value() || point_keys.empty()) {
        column_index.reset();
        point_keys.clear();
        return;
      }
    } else {
      auto child_comparision = std::dynamic_pointer_cast<ComparisonExpression>(right_child);
      if (child_comparision == nullptr) {  // neither logic nor comparison, failed
        column_index.reset();
        point_keys.clear();
        return;
      }
      comparisions.push_back(child_comparision);
    }

    // deal with all comparisions
    for (const auto &comparision : comparisions) {
      auto pair = get_column_and_constant(comparision);
      if (!pair.has_value()) {
        column_index.reset();
        point_keys.clear();
        return;
      }
      auto [column, constant] = *pair;
      if (!column_index.has_value()) {  // init column
        column_index = column->GetColIdx();
        auto indexes = catalog.GetTableIndexes(table_name);
        for (const auto &index_info : indexes) {
          auto key_attr = index_info->index_->GetKeyAttrs();
          if (key_attr.size() == 1 && key_attr.front() == column->GetColIdx()) {
            index_oid = index_info->index_oid_;
            break;
          }
        }

        if (!index_oid.has_value()) {  // no index on this column
          column_index.reset();
          point_keys.clear();
          return;
        }
      } else if (column->GetColIdx() != *column_index) {  // different column
        column_index.reset();
        point_keys.clear();
        return;
      }

      // add constant to point_keys
      // skip if redundant
      if (std::find_if(point_keys.cbegin(), point_keys.cend(), [val = constant->val_](const auto &c) {
            return val.CompareEquals(std::dynamic_pointer_cast<ConstantValueExpression>(c)->val_);
          }) == point_keys.cend()) {
        point_keys.push_back(constant);
      }
    }
  };

  // logic expression
  if (auto logic = std::dynamic_pointer_cast<LogicExpression>(filter); logic != nullptr) {
    extract_logic(extract_logic, logic);
    if (!column_index.has_value() || point_keys.empty()) {  // extract failed
      return optimized_plan;
    }

    return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.GetTableOid(), *index_oid,
                                               nullptr, point_keys);
  }

  // can not convert to index scan
  return optimized_plan;
}

}  // namespace bustub
