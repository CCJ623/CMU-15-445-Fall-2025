//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <memory>
#include <vector>
#include "common/macros.h"

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  aht_iterator_ = aht_.Begin();
  is_fetch_done_ = false;
  is_empty_and_no_group_by_done_ = false;
}

/**
 * Yield the next tuple batch from the aggregation.
 * @param[out] tuple_batch The next batch of tuples produced by the aggregation
 * @param[out] rid_batch The next batch of tuple RIDs produced by the aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if any tuples were produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  if (!is_fetch_done_) {
    std::vector<Tuple> child_tuples;
    std::vector<RID> child_rids;
    while (true) {
      child_tuples.clear();
      child_rids.clear();
      if (!child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
        break;
      }

      for (const auto &tuple : child_tuples) {
        auto agg_key = MakeAggregateKey(&tuple);
        auto agg_value = MakeAggregateValue(&tuple);

        aht_.InsertCombine(agg_key, agg_value);
      }
    }

    is_fetch_done_ = true;
    aht_iterator_ = aht_.Begin();
  }

  tuple_batch->clear();
  rid_batch->clear();

  // empty table and no group by
  // return a init tuple
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    if (is_empty_and_no_group_by_done_) {
      return false;
    }
    std::vector<Value> values;
    for (const auto &val : aht_.GenerateInitialAggregateValue().aggregates_) {
      values.push_back(val);
    }
    tuple_batch->emplace_back(values, &GetOutputSchema());
    rid_batch->push_back(RID{});
    is_empty_and_no_group_by_done_ = true;
    return true;
  }

  for (; aht_iterator_ != aht_.End() && tuple_batch->size() < batch_size; ++aht_iterator_) {
    std::vector<Value> values;
    for (const auto &key : aht_iterator_.Key().group_bys_) {
      values.push_back(key);
    }
    for (const auto &value : aht_iterator_.Val().aggregates_) {
      values.push_back(value);
    }
    tuple_batch->emplace_back(values, &GetOutputSchema());
    rid_batch->push_back(RID{});
  }

  return !tuple_batch->empty();
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
