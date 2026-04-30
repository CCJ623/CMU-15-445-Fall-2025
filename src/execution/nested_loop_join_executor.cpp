//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  inner_tuples_.clear();
  outer_tuples_.clear();
  inner_iter_ = inner_tuples_.cend();
  outer_iter_ = outer_tuples_.cend();
  is_inner_tuples_constructed_ = false;
  has_outer_tuple_matched_any_inner_tuple_ = false;
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  const auto &predicate = plan_->Predicate();
  while (tuple_batch->size() < batch_size) {
    // construct inner tuples
    if (!is_inner_tuples_constructed_) {
      right_executor_->Init();
      inner_tuples_.clear();
      inner_tuples_.reserve(batch_size);
      inner_iter_ = inner_tuples_.cend();
      std::vector<Tuple> buffer;
      buffer.reserve(batch_size);
      std::vector<bustub::RID> placeholder;
      while (right_executor_->Next(&buffer, &placeholder, batch_size)) {
        for (auto &value : buffer) {
          inner_tuples_.push_back(std::move(value));
        }
      }
      inner_iter_ = inner_tuples_.cbegin();
      is_inner_tuples_constructed_ = true;
    }

    // fill outer tuples
    if (std::vector<bustub::RID> placeholder; outer_iter_ >= outer_tuples_.cend()) {
      outer_tuples_.clear();
      outer_iter_ = outer_tuples_.cend();
      outer_tuples_.reserve(batch_size);
      if (!left_executor_->Next(&outer_tuples_, &placeholder, batch_size)) {  // outer drained, exit
        break;
      }
      outer_iter_ = outer_tuples_.cbegin();
      if (outer_tuples_.empty()) {  // outer return a empty batch
        continue;
      }
    }

    const auto &outer_tuple = *outer_iter_;
    // find matched inner tuple
    inner_iter_ = std::find_if(inner_iter_, inner_tuples_.cend(),
                               [&predicate, &outer_tuple, &left_schema = this->left_executor_->GetOutputSchema(),
                                &right_schema = this->right_executor_->GetOutputSchema()](const auto &inner_tuple) {
                                 if (predicate == nullptr) {
                                   return true;
                                 }
                                 Value result =
                                     predicate->EvaluateJoin(&outer_tuple, left_schema, &inner_tuple, right_schema);
                                 return !result.IsNull() && result.GetAs<bool>();
                               });

    if (inner_iter_ != inner_tuples_.cend()) {  // found
      const auto &inner_tuple = *inner_iter_;
      std::vector<Value> values;
      const auto &left_schema = left_executor_->GetOutputSchema();
      const auto &right_schema = right_executor_->GetOutputSchema();
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
      ++inner_iter_;
      has_outer_tuple_matched_any_inner_tuple_ = true;
      continue;
    }

    // not found
    switch (plan_->GetJoinType()) {
      case JoinType::INNER: {
        break;
      }
      case JoinType::LEFT: {
        if (has_outer_tuple_matched_any_inner_tuple_) {
          break;
        }
        std::vector<Value> values;
        const auto &left_schema = left_executor_->GetOutputSchema();
        const auto &right_schema = right_executor_->GetOutputSchema();
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
    ++outer_iter_;
    inner_iter_ = inner_tuples_.cbegin();
    has_outer_tuple_matched_any_inner_tuple_ = false;
    is_inner_tuples_constructed_ = false;
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
