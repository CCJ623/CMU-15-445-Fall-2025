//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "common/macros.h"

#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid()).get()),
      child_executor_(std::move(child_executor)),
      count_(std::nullopt) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
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

    for (auto &tuple : child_tuples_) {
      auto rid = table_info_->table_->InsertTuple({timestamp_t{0}, false}, tuple);
      if (!rid.has_value()) {
        continue;
      }
      // update indexes
      for (auto &index_info : indexes) {
        auto key = tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        auto &index = *index_info->index_;
        index.InsertEntry(key, *rid, exec_ctx_->GetTransaction());
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
