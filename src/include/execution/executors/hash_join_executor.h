//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

constexpr size_t NUM_PARTITION = 100;

/**
 * A simplified hash table that has all the necessary functionality for hash join.
 */
class HashJoinHashTable {
 public:
  /**
   * Construct a new HashJoinHashTable instance.
   */
  HashJoinHashTable() = default;

  /**
   * Inserts a value into the hash table.
   * @param agg_key the key to be inserted
   * @param agg_val the tuple to be inserted
   */
  void Insert(const HashJoinKey &key, const Tuple &tuple) { hash_table_.insert({key, tuple}); }
  auto IsEmpty() { return hash_table_.empty(); }

  /**
   * Clear the hash table
   */
  void Clear() { hash_table_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_multimap<HashJoinKey, Tuple>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const HashJoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const Tuple & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Hash join map */
    std::unordered_multimap<HashJoinKey, Tuple>::const_iterator iter_;
  };

  auto EqualRange(const HashJoinKey &key) -> std::pair<Iterator, Iterator> {
    auto [first, second] = hash_table_.equal_range(key);
    return {Iterator{first}, Iterator{second}};
  }

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{hash_table_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{hash_table_.cend()}; }

 private:
  /** The hash table is just a multi-map from hash join keys to tuples */
  std::unordered_multimap<HashJoinKey, Tuple> hash_table_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  enum class Phase : uint8_t { kInvalid = 0, kBuild, kProbe } phase_;
  std::array<page_id_t, NUM_PARTITION> left_partition_;
  std::array<page_id_t, NUM_PARTITION> right_partition_;

  // probe phase breakpoint state
  struct ProbeState {
    void Init() {
      partition_index_ = -1;
      left_index_ = -1;

      hash_table_.Clear();
      right_iter_.reset();
      right_range_end_iter_.reset();

      has_outer_tuple_matched_any_inner_tuple_ = false;
    }

    uint32_t partition_index_ = -1;
    uint32_t left_index_ = -1;

    HashJoinHashTable hash_table_{};
    std::optional<HashJoinHashTable::Iterator> right_iter_;
    std::optional<HashJoinHashTable::Iterator> right_range_end_iter_;

    bool has_outer_tuple_matched_any_inner_tuple_ = false;
  } probe_state_;
};

}  // namespace bustub
