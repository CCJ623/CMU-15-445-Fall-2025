//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include <optional>
#include <vector>
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(std::nullopt) {}

void IndexScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get();
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get();

  if (plan_->pred_keys_.empty()) {
    // scan mode, init iter
    if (auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_); tree != nullptr) {
      iter_.emplace(tree->GetBeginIterator());
    } else {
      iter4_.emplace(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_)->GetBeginIterator());
    }
    return;
  }

  // point look up mode
  pred_keys_index_ = 0;
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (plan_->pred_keys_.empty()) {
    // scan mode
    if (iter_.has_value()) {
      for (; tuple_batch->size() < batch_size && !iter_->IsEnd(); ++(*iter_)) {
        auto rid = (*(*iter_)).second;
        auto [meta, tuple] = table_info_->table_->GetTuple(rid);
        if (meta.is_deleted_) {
          continue;
        }

        if (plan_->filter_predicate_ != nullptr) {
          // has filter
          auto value = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
          if (value.IsNull() || !value.GetAs<bool>()) {
            // failed filter
            continue;
          }
          // passed filter
        }

        // no filter
        tuple_batch->push_back(tuple);
        rid_batch->push_back(rid);
      }
    } else if (iter4_.has_value()) {  // for small key
      for (; tuple_batch->size() < batch_size && !iter4_->IsEnd(); ++(*iter4_)) {
        auto rid = (*(*iter4_)).second;
        auto [meta, tuple] = table_info_->table_->GetTuple(rid);
        if (meta.is_deleted_) {
          continue;
        }

        if (plan_->filter_predicate_ != nullptr) {
          // has filter
          auto value = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
          if (value.IsNull() || !value.GetAs<bool>()) {
            // failed filter
            continue;
          }
          // passed filter
        }

        // no filter
        tuple_batch->push_back(tuple);
        rid_batch->push_back(rid);
      }
    }

    return !tuple_batch->empty();
  }

  // point look up

  for (; pred_keys_index_ < plan_->pred_keys_.size() && tuple_batch->size() < batch_size; ++pred_keys_index_) {
    const auto &expr = *plan_->pred_keys_[pred_keys_index_];
    std::vector<Value> values;
    std::vector<RID> rids;
    values.push_back(expr.Evaluate(nullptr, table_info_->schema_));
    index_->ScanKey(Tuple{values, index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    // key not found
    if (rids.empty()) {
      continue;
    }
    BUSTUB_ASSERT(rids.size() == 1, "IndexScanExecutor::Next(): duplicate key");

    const auto &rid = rids.front();
    auto [meta, tuple] = table_info_->table_->GetTuple(rid);
    if (meta.is_deleted_) {
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      // has filter
      auto value = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {
        // failed filter
        continue;
      }
      // passed filter
    }

    // no filter
    tuple_batch->push_back(tuple);
    rid_batch->push_back(rid);
  }

  return !tuple_batch->empty();
}
}  // namespace bustub
