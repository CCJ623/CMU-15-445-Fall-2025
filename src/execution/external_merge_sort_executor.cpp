//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <array>
#include <queue>
#include <type_traits>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "fmt/format.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"
#include "type/vector_type.h"

namespace bustub {

template <typename T, typename = void>
struct IsFixedSize : std::false_type {};

template <typename T>
struct IsFixedSize<T, std::void_t<decltype(std::tuple_size<T>::value)>> : std::true_type {};

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  result_.clear();
  iter_ = result_.cend();
  is_merge_sort_done_ = false;
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge sort.
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (!is_merge_sort_done_) {
    // read all tuples and store in disk
    auto bpm = exec_ctx_->GetBufferPoolManager();
    std::vector<MergeSortRun> runs;

    // read tuples from child and push back to sort_entries_buffer
    // when buffer is bigger than page size, drain buffer as much as possible
    size_t sum_tuples_length = 0;
    std::vector<SortEntry> sort_entries_buffer;
    while (child_executor_->Next(tuple_batch, rid_batch, batch_size)) {
      for (auto &tuple : *tuple_batch) {
        if (IntermediateResultPage::DATA_SIZE < sum_tuples_length) {
          // buffer fullfill a page, sort tuples and create a new page to store tuples
          std::sort(sort_entries_buffer.begin(), sort_entries_buffer.end(), cmp_);
          do {
            auto page_id = bpm->NewPage();
            BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "ExternalMergeSortExecutor: Out of memory");
            auto guard = bpm->CheckedWritePage(page_id);
            auto &page = *guard->AsMut<IntermediateResultPage>();
            page.Init();
            auto iter = sort_entries_buffer.begin();
            for (; iter != sort_entries_buffer.end() &&
                   page.FreeSpaceForTuple() >= static_cast<int>(iter->second.GetLength());
                 ++iter) {
              page.PushBackTuple(iter->second);
              sum_tuples_length -= iter->second.GetLength();
            }
            runs.push_back(MergeSortRun{std::vector<page_id_t>{guard->GetPageId()}, bpm});
            // truncate buffer
            sort_entries_buffer.erase(sort_entries_buffer.begin(), iter);
          } while (IntermediateResultPage::DATA_SIZE < sum_tuples_length);
        }

        sum_tuples_length += tuple.GetLength();
        sort_entries_buffer.emplace_back(
            GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema()), std::move(tuple));
      }
    }

    if (!sort_entries_buffer.empty()) {
      // deal with remain tuples
      std::sort(sort_entries_buffer.begin(), sort_entries_buffer.end(), cmp_);
      do {
        auto page_id = bpm->NewPage();
        BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "ExternalMergeSortExecutor: Out of memory");
        auto guard = bpm->CheckedWritePage(page_id);
        auto &page = *guard->AsMut<IntermediateResultPage>();
        page.Init();
        auto iter = sort_entries_buffer.begin();
        for (; iter != sort_entries_buffer.end() &&
               page.FreeSpaceForTuple() >= static_cast<int>(iter->second.GetLength());
             ++iter) {
          page.PushBackTuple(iter->second);
          sum_tuples_length -= iter->second.GetLength();
        }
        runs.push_back(MergeSortRun{std::vector<page_id_t>{guard->GetPageId()}, bpm});
        // truncate buffer
        sort_entries_buffer.erase(sort_entries_buffer.begin(), iter);
      } while (IntermediateResultPage::DATA_SIZE < sum_tuples_length);
    }

    // external merge sort
    // input a runs container
    // do merge sort on these runs
    // output a new run
    auto do_merge_sort = [&bpm, &compare = this->cmp_, &plan = plan_](auto &&old_runs) -> MergeSortRun {
      using ContainerType = std::decay_t<decltype(old_runs)>;
      // generate K iters
      // first is iter, second is end_iter
      auto iter_pairs = []() {
        if constexpr (IsFixedSize<ContainerType>::value) {
          return std::array<std::pair<MergeSortRun::Iterator, MergeSortRun::Iterator>,
                            std::tuple_size_v<ContainerType>>{};
        } else {
          return std::vector<std::pair<MergeSortRun::Iterator, MergeSortRun::Iterator>>{};
        }
      }();

      if constexpr (IsFixedSize<ContainerType>::value) {
        for (size_t i = 0; i < old_runs.size(); ++i) {
          iter_pairs[i].first = old_runs[i].Begin();
          iter_pairs[i].second = old_runs[i].End();
        }
      } else {
        iter_pairs.reserve(old_runs.size());
        for (auto &run : old_runs) {
          iter_pairs.emplace_back(run.Begin(), run.End());
        }
      }

      // read tuples from K way external and push to pq
      using SortEntryKPair = std::pair<SortEntry, size_t>;
      auto sort_entry_k_pair_comparator = [&compare](const SortEntryKPair &lhs, const SortEntryKPair &rhs) -> bool {
        // why rhs at left, lhs at right here?
        // because pq will put element at bottom which satisifed the comparator
        // so we need to swap the lhs and rhs
        return compare(rhs.first, lhs.first);
        // return compare(lhs.first, rhs.first);
      };
      std::priority_queue<SortEntryKPair, std::vector<SortEntryKPair>, decltype(sort_entry_k_pair_comparator)> pq(
          sort_entry_k_pair_comparator);
      std::vector<page_id_t> page_ids{bpm->NewPage()};
      auto guard = bpm->CheckedWritePage(page_ids.back());
      auto page = guard->AsMut<IntermediateResultPage>();
      page->Init();

      // init pq
      for (size_t k = 0; k < iter_pairs.size(); ++k) {
        auto &[iter, end_iter] = iter_pairs[k];
        if (iter == end_iter) {
          continue;
        }

        auto tuple = *iter;
        pq.push(
            SortEntryKPair{{GenerateSortKey(tuple, plan->GetOrderBy(), plan->OutputSchema()), std::move(tuple)}, k});
        ++iter;
      }

      while (!pq.empty()) {
        auto tuple = std::move(pq.top().first.second);
        auto k = pq.top().second;
        pq.pop();

        auto tuple_length = tuple.GetLength();
        if (page->FreeSpaceForTuple() < static_cast<int>(tuple_length)) {
          guard = bpm->CheckedWritePage(bpm->NewPage());
          page_ids.push_back(guard->GetPageId());
          page = guard->AsMut<IntermediateResultPage>();
          page->Init();
        }
        page->PushBackTuple(tuple);

        auto &[iter, end_iter] = iter_pairs[k];
        if (iter != end_iter) {
          auto next_tuple = *iter;
          pq.push(SortEntryKPair{
              {GenerateSortKey(next_tuple, plan->GetOrderBy(), plan->OutputSchema()), std::move(next_tuple)}, k});
          ++iter;
        }
      }

      return {page_ids, bpm};
    };

    // do merge sort until only one run, which means finished
    while (runs.size() > 1) {
      std::vector<MergeSortRun> new_runs;
      size_t i = 0;

      // first part of runs
      for (; i < runs.size() / K * K; i += K) {
        std::array<MergeSortRun, K> old_runs;
        for (size_t k = 0; k < K; ++k) {
          old_runs[k] = std::move(runs[i + k]);
        }
        new_runs.push_back(do_merge_sort(old_runs));
      }

      // last part of runs
      {
        std::vector<MergeSortRun> old_runs;
        for (; i < runs.size(); ++i) {
          old_runs.push_back(std::move(runs[i]));
        }
        if (!old_runs.empty()) {
          if (old_runs.size() == 1) {
            new_runs.push_back(std::move(old_runs[0]));
          } else {
            new_runs.push_back(do_merge_sort(old_runs));
          }
        }
      }

      runs = std::move(new_runs);
    }

    // move tuples to result_
    if (!runs.empty()) {
      for (auto iter = runs.back().Begin(), end_iter = runs.back().End(); iter != end_iter; ++iter) {
        result_.push_back(*iter);
      }
    }

    iter_ = result_.cbegin();
    is_merge_sort_done_ = true;
  }

  for (; iter_ != result_.cend() && tuple_batch->size() < batch_size; ++iter_) {
    tuple_batch->push_back(*iter_);
    rid_batch->emplace_back();
  }

  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
