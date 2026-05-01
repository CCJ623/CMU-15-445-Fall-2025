//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "execution/plans/hash_join_plan.h"
#include "fmt/base.h"
#include "fmt/format.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  phase_ = Phase::kBuild;
  left_partition_.fill(INVALID_PAGE_ID);
  right_partition_.fill(INVALID_PAGE_ID);
  probe_state_.Init();
}

/**
 * Yield the next tuple batch from the hash join.
 * @param[out] tuple_batch The next tuple batch produced by the hash join
 * @param[out] rid_batch The next tuple RID batch produced by the hash join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (phase_ == Phase::kBuild) {  // build
    auto build = [&bpm = *exec_ctx_->GetBufferPoolManager(), batch_size](
                     AbstractExecutor &executor, const std::vector<AbstractExpressionRef> &join_key_expressions,
                     std::array<page_id_t, NUM_PARTITION> &partition) {
      std::vector<Tuple> child_tuples;
      std::vector<RID> child_rids;

      // save all guards needed, avoiding frequently flush
      std::array<std::optional<WritePageGuard>, NUM_PARTITION> guards;
      while (executor.Next(&child_tuples, &child_rids, batch_size)) {
        for (const auto &tuple : child_tuples) {
          // hash keys
          size_t pos =
              std::hash<HashJoinKey>{}({tuple, executor.GetOutputSchema(), join_key_expressions}) % partition.size();
          auto &page_id = partition[pos];
          if (page_id == INVALID_PAGE_ID) {  // create head page
            page_id = bpm.NewPage();
            BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "HashJoinExecutor::Next(): out of memory");
            guards[pos] = bpm.CheckedWritePage(partition[pos]);
            BUSTUB_ASSERT(guards[pos].has_value(), "HashJoinExecutor::Next(): out of memory");
            auto guard = &(guards[pos]);
            auto page = (*guard)->AsMut<IntermediateResultPage>();
            page->Init();
          }

          auto guard = &(guards[pos]);
          auto page = (*guard)->AsMut<IntermediateResultPage>();
          if (page->FreeSpaceForTuple() < static_cast<int64_t>(tuple.GetLength())) {  // no space, new a page
            auto old_page_id = page_id;
            page_id = bpm.NewPage();
            BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "HashJoinExecutor::Next(): out of memory");
            *guard = bpm.CheckedWritePage(page_id);
            BUSTUB_ASSERT(guards[pos].has_value(), "HashJoinExecutor::Next(): out of memory");
            page = (*guard)->AsMut<IntermediateResultPage>();
            page->Init();
            page->SetNextPageId(old_page_id);
          }

          // now page has enough space for tuple
          page->PushBackTuple(tuple);
        }

        child_tuples.clear();
        child_rids.clear();
      }
    };

    build(*left_child_, plan_->LeftJoinKeyExpressions(), left_partition_);
    build(*right_child_, plan_->RightJoinKeyExpressions(), right_partition_);
    phase_ = Phase::kProbe;

    // set state
    probe_state_.partition_index_ = 0;
    probe_state_.left_index_ = 0;
    probe_state_.hash_table_.Clear();
    probe_state_.has_outer_tuple_matched_any_inner_tuple_ = false;
  }

  // probe, use lambda to jump out multi loop
  [this, &tuple_batch, &rid_batch, batch_size]() {
    const auto &left_expressions = plan_->LeftJoinKeyExpressions();
    const auto &right_expressions = plan_->RightJoinKeyExpressions();
    BUSTUB_ASSERT(left_expressions.size() == right_expressions.size(),
                  "HashJoinExecutor::Next(): expressions size not match");
    std::vector<Value> buffer_to_constuct_tuple;
    const auto &left_schema = left_child_->GetOutputSchema();
    const auto &right_schema = right_child_->GetOutputSchema();
    buffer_to_constuct_tuple.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

    // work with every partitions
    for (auto &partition_index = probe_state_.partition_index_; partition_index < NUM_PARTITION;
         ++partition_index, probe_state_.hash_table_.Clear()) {
      // build right hash table
      auto &hash_table = probe_state_.hash_table_;
      auto &right_iter = probe_state_.right_iter_;
      auto &right_range_end_iter = probe_state_.right_range_end_iter_;
      if (hash_table.IsEmpty()) {
        right_iter.reset();
        right_range_end_iter.reset();

        // if right table is empty, let hash table be empty, otherwise build hash table
        if (auto &right_page_id = right_partition_[partition_index]; right_page_id != INVALID_PAGE_ID) {
          auto right_guard = exec_ctx_->GetBufferPoolManager()->ReadPage(right_page_id);
          const auto &right_page = right_guard.As<IntermediateResultPage>();

          for (size_t right_index = 0; right_index < right_page->GetNumTuples(); ++right_index) {
            auto right_tuple = right_page->GetTuple(right_index);
            HashJoinKey key{right_tuple, right_child_->GetOutputSchema(), plan_->RightJoinKeyExpressions()};
            hash_table.Insert(key, right_tuple);
          }

          right_guard.Drop();
          exec_ctx_->GetBufferPoolManager()->DeletePage(right_page_id);
          right_page_id = INVALID_PAGE_ID;
        }
      }

      // scan left page
      for (auto &left_page_id = left_partition_[partition_index]; left_page_id != INVALID_PAGE_ID;
           probe_state_.left_index_ = 0) {
        auto left_guard = exec_ctx_->GetBufferPoolManager()->ReadPage(left_page_id);
        const auto &left_page = left_guard.As<IntermediateResultPage>();

        for (auto &left_index = probe_state_.left_index_; left_index < left_page->GetNumTuples();
             ++left_index, right_iter.reset(), right_range_end_iter.reset()) {
          if (tuple_batch->size() >= batch_size) {  // output buffer is full
            return;
          }

          auto left_tuple = left_page->GetTuple(left_index);

          // setup iters
          if (!right_iter.has_value() || !right_range_end_iter.has_value()) {
            auto range =
                hash_table.EqualRange({left_tuple, left_child_->GetOutputSchema(), plan_->LeftJoinKeyExpressions()});
            right_iter = range.first;
            right_range_end_iter = range.second;
            probe_state_.has_outer_tuple_matched_any_inner_tuple_ = false;
          }

          if (*right_iter == *right_range_end_iter) {  // not found
            right_iter.reset();
            right_range_end_iter.reset();
            switch (plan_->GetJoinType()) {
              case JoinType::INNER: {
                break;
              }
              case JoinType::LEFT: {
                if (probe_state_.has_outer_tuple_matched_any_inner_tuple_) {
                  break;
                }
                buffer_to_constuct_tuple.clear();
                // combine left and right NULL tuple
                for (decltype(left_schema.GetColumnCount()) i = 0; i < left_schema.GetColumnCount(); ++i) {
                  buffer_to_constuct_tuple.push_back(left_tuple.GetValue(&left_schema, i));
                }
                for (decltype(right_schema.GetColumnCount()) i = 0; i < right_schema.GetColumnCount(); ++i) {
                  buffer_to_constuct_tuple.push_back(
                      ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
                }

                tuple_batch->emplace_back(buffer_to_constuct_tuple, &GetOutputSchema());
                rid_batch->emplace_back();
                break;
              }
              default: {
                BUSTUB_ASSERT(1, "HashJoinExecutor::Next(): invalid join type");
              }
            }
            continue;
          }

          // found
          probe_state_.has_outer_tuple_matched_any_inner_tuple_ = true;
          for (; *right_iter != *right_range_end_iter; ++(*right_iter)) {
            if (tuple_batch->size() >= batch_size) {
              return;
            }
            const auto &right_tuple = right_iter->Val();
            buffer_to_constuct_tuple.clear();
            // combine left and right tuple
            for (decltype(left_schema.GetColumnCount()) i = 0; i < left_schema.GetColumnCount(); ++i) {
              buffer_to_constuct_tuple.push_back(left_tuple.GetValue(&left_schema, i));
            }
            for (decltype(right_schema.GetColumnCount()) i = 0; i < right_schema.GetColumnCount(); ++i) {
              buffer_to_constuct_tuple.push_back(right_tuple.GetValue(&right_schema, i));
            }
            tuple_batch->emplace_back(buffer_to_constuct_tuple, &GetOutputSchema());
            rid_batch->emplace_back();
          }
        }

        auto next_page_id = left_page->GetNextPageId();
        left_guard.Drop();
        exec_ctx_->GetBufferPoolManager()->DeletePage(left_page_id);
        left_page_id = next_page_id;
      }
    }
  }();

  return !tuple_batch->empty();
}

}  // namespace bustub
