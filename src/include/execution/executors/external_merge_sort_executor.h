//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  // delete all pages
  ~MergeSortRun() {
    for (const auto &page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
  }

  MergeSortRun(const MergeSortRun &) = delete;
  auto operator=(const MergeSortRun &) = delete;

  MergeSortRun(MergeSortRun &&other) noexcept : pages_(std::move(other.pages_)), bpm_(other.bpm_) {
    other.pages_.clear();
  }

  auto operator=(MergeSortRun &&other) noexcept -> MergeSortRun & {
    if (this != &other) {
      for (const auto &page_id : pages_) {
        bpm_->DeletePage(page_id);
      }
      pages_ = std::move(other.pages_);
      bpm_ = other.bpm_;
      other.pages_.clear();
    }
    return *this;
  }

  auto GetPageCount() const -> size_t { return pages_.size(); }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      if (iter_ == page_end_iter_) {
        BUSTUB_ASSERT(page_index_ + 1 < run_->GetPageCount(), "MergeSortRun::operator++(): ++ end iter");
        ++page_index_;
        guard_ = run_->bpm_->CheckedReadPage(run_->pages_[page_index_]);
        iter_ = guard_->As<IntermediateResultPage>()->Begin();
        page_end_iter_ = guard_->As<IntermediateResultPage>()->End();
      } else {
        ++iter_;
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() -> Tuple { return *iter_; }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && page_index_ == other.page_index_ && iter_ == other.iter_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    // explicit Iterator(const MergeSortRun *run) : run_(run) {}
    Iterator(const MergeSortRun *run, ReadPageGuard &&guard, size_t page_index, IntermediateResultPage::Iterator iter,
             IntermediateResultPage::Iterator page_end_iter)
        : run_(run), guard_(std::move(guard)), page_index_(page_index), iter_(iter) {}

    Iterator(const MergeSortRun *run, ReadPageGuard &&guard, size_t page_index)
        : Iterator(run, std::forward<ReadPageGuard>(guard), page_index, guard.As<IntermediateResultPage>()->Begin(),
                   guard.As<IntermediateResultPage>()->End()) {}

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_{nullptr};

    /**
     * TODO(P3): Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    std::optional<ReadPageGuard> guard_{std::nullopt};
    size_t page_index_{std::numeric_limits<size_t>::max()};
    IntermediateResultPage::Iterator iter_;
    IntermediateResultPage::Iterator page_end_iter_;
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() -> Iterator {
    if (pages_.empty()) {
      return {};
    }
    auto guard = bpm_->CheckedReadPage(pages_.front());
    return {this, std::move(*guard), 0};
  }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() -> Iterator {
    if (pages_.empty()) {
      return {};
    }
    auto guard = bpm_->CheckedReadPage(pages_.back());
    auto iter = guard->As<IntermediateResultPage>()->End();
    return {this, std::move(*guard), pages_.size() - 1, iter, iter};
  }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO(P3): You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> result_;
  std::vector<Tuple>::const_iterator iter_;

  bool is_merge_sort_done_{false};
};

}  // namespace bustub
