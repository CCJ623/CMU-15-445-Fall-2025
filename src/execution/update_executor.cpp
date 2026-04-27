//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/macros.h"

#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid()).get()),
      child_executor_(std::move(child_executor)),
      count_(std::nullopt) {}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (!count_.has_value()) {
    return false;
  }

  auto &catalog = *exec_ctx_->GetCatalog();
  auto indexes = catalog.GetTableIndexes(table_info_->name_);

  while (true) {
    child_tuples_.clear();
    child_rids_.clear();
    if (!child_executor_->Next(&child_tuples_, &child_rids_, batch_size)) {
      break;
    }

    for (size_t i = 0; i < child_tuples_.size(); ++i) {
      // generate new tuple
      std::vector<Value> values;
      values.reserve(plan_->target_expressions_.size());
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&child_tuples_[i], child_executor_->GetOutputSchema()));
      }
      Tuple new_tuple{values, &table_info_->schema_};
      Tuple &old_tuple = child_tuples_[i];

      // delete old tuple
      table_info_->table_->UpdateTupleMeta(TupleMeta{timestamp_t{0}, true}, child_rids_[i]);
      auto new_rid = table_info_->table_->InsertTuple({timestamp_t{0}, false}, new_tuple);
      auto old_rid = child_rids_[i];
      if (!new_rid.has_value()) {
        continue;
      }
      // update indexes
      for (auto &index_info : indexes) {
        auto &index = *index_info->index_;
        index.DeleteEntry(
            old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            old_rid, exec_ctx_->GetTransaction());
        index.InsertEntry(
            new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            *new_rid, exec_ctx_->GetTransaction());
      }
      ++(*count_);
    }
  }

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, static_cast<int32_t>(*count_));
  tuple_batch->clear();
  tuple_batch->push_back(Tuple{values, &plan_->OutputSchema()});
  count_ = std::nullopt;
  return true;
}

}  // namespace bustub
