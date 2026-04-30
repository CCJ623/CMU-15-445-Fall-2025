//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      inner_table_(*(exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->table_)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();
  outer_tuples_.clear();
  outer_iter_ = outer_tuples_.cbegin();
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get();
}

auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (tuple_batch->size() < batch_size) {
    // fill outer tuples
    if (std::vector<bustub::RID> placeholder; outer_iter_ >= outer_tuples_.cend()) {
      outer_tuples_.clear();
      outer_tuples_.reserve(batch_size);
      if (!child_executor_->Next(&outer_tuples_, &placeholder, batch_size)) {  // outer drained, exit
        break;
      }
      outer_iter_ = outer_tuples_.cbegin();
      if (outer_tuples_.empty()) {  // outer return a empty batch
        continue;
      }
    }

    const auto &outer_tuple = *outer_iter_;
    const auto &predicate = plan_->KeyPredicate();
    auto index_key = predicate->Evaluate(&outer_tuple, child_executor_->GetOutputSchema());
    std::vector<RID> rids;
    index_->ScanKey(Tuple{{index_key}, index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(rids.size() <= 1, "NestedIndexJoinExecutor::Next(): duplicate key");

    auto deal_with_not_match = [this, &outer_tuple, &tuple_batch, &rid_batch]() {
      switch (plan_->GetJoinType()) {
        case JoinType::INNER: {
          break;
        }
        case JoinType::LEFT: {
          std::vector<Value> values;
          const auto &left_schema = child_executor_->GetOutputSchema();
          const auto &right_schema = plan_->InnerTableSchema();
          values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
          // combine left and right NULL tuple
          for (decltype(left_schema.GetColumnCount()) i = 0; i < left_schema.GetColumnCount(); ++i) {
            values.push_back(outer_tuple.GetValue(&left_schema, i));
          }
          for (decltype(right_schema.GetColumnCount()) i = 0; i < right_schema.GetColumnCount(); ++i) {
            values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
          }

          tuple_batch->emplace_back(values, &GetOutputSchema());
          rid_batch->emplace_back();
          break;
        }
        default: {
          throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
        }
      }
    };

    if (rids.empty()) {  // not found
      deal_with_not_match();
    }

    for (const auto &rid : rids) {
      auto [meta, inner_tuple] = inner_table_.GetTuple(rid);
      if (meta.is_deleted_) {  // not found
        deal_with_not_match();
        continue;
      }

      std::vector<Value> values;
      const auto &left_schema = child_executor_->GetOutputSchema();
      const auto &right_schema = plan_->InnerTableSchema();
      values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
      // combine left and right tuple
      for (decltype(left_schema.GetColumnCount()) i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(outer_tuple.GetValue(&left_schema, i));
      }
      for (decltype(right_schema.GetColumnCount()) i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(inner_tuple.GetValue(&right_schema, i));
      }
      tuple_batch->emplace_back(values, &GetOutputSchema());
      rid_batch->emplace_back();
    }
    ++outer_iter_;
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
