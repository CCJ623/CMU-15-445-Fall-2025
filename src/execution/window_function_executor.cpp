//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>
#include "binder/bound_order_by.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  BUSTUB_ASSERT(std::all_of(plan_->columns_.cbegin(), plan_->columns_.cend(),
                            [](const AbstractExpressionRef &expression) {
                              return std::dynamic_pointer_cast<const ColumnValueExpression>(expression) != nullptr;
                            }),
                "WindowFunctionExecutor::WindowFunctionExecutor(): invalid columns");
  for (const auto &pair : plan_->window_functions_) {
    BUSTUB_ASSERT(std::all_of(pair.second.partition_by_.cbegin(), pair.second.partition_by_.cend(),
                              [](const AbstractExpressionRef &expression) {
                                return std::dynamic_pointer_cast<const ColumnValueExpression>(expression) != nullptr;
                              }),
                  "WindowFunctionExecutor::WindowFunctionExecutor(): invalid partition by");
    BUSTUB_ASSERT(std::all_of(pair.second.order_by_.cbegin(), pair.second.order_by_.cend(),
                              [](const OrderBy &order_by) {
                                return std::dynamic_pointer_cast<const ColumnValueExpression>(std::get<2>(order_by)) !=
                                       nullptr;
                              }),
                  "WindowFunctionExecutor::WindowFunctionExecutor(): invalid order by");
  }
}

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  iter_ = tuples_.end();
  phase_ = Phase::kBuild;
}

/**
 * Yield the next tuple batch from the window aggregation.
 * @param[out] tuple_batch The next tuple batch produced by the window aggregation
 * @param[out] rid_batch The next tuple RID batch produced by the window aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (phase_ == Phase::kBuild) {
    // get tuples from child
    // assign unique id to every tuple
    using IdType = size_t;
    using TupleWithIdType = std::pair<Tuple, IdType>;
    std::vector<TupleWithIdType> buffer;
    IdType id = 0;
    for (tuple_batch->clear(), rid_batch->clear(); child_executor_->Next(tuple_batch, rid_batch, batch_size);
         tuple_batch->clear(), rid_batch->clear()) {
      for (auto &tuple : *tuple_batch) {
        buffer.emplace_back(std::move(tuple), id++);
      }
    }

    // init sort
    {
      auto &window_function = plan_->window_functions_.begin()->second;
      std::vector<OrderBy> order_by;
      order_by.reserve(window_function.partition_by_.size() + window_function.order_by_.size());
      for (const auto &partition : window_function.partition_by_) {
        order_by.emplace_back(OrderByType::DEFAULT, OrderByNullType::DEFAULT, partition);
      }
      for (const auto &order : window_function.order_by_) {
        order_by.push_back(order);
      }

      // sort tuples
      // index is id
      std::vector<SortKey> sort_keys;
      sort_keys.reserve(buffer.size());
      for (const auto &[tuple, id] : buffer) {
        sort_keys.emplace_back(GenerateSortKey(tuple, order_by, child_executor_->GetOutputSchema()));
      }
      TupleComparator tuple_comparator{order_by};
      std::sort(buffer.begin(), buffer.end(),
                [&tuple_comparator, &sort_keys](const TupleWithIdType &lhs, TupleWithIdType &rhs) -> bool {
                  return tuple_comparator(sort_keys[lhs.second], sort_keys[rhs.second]);
                });
    }

    // sort and apply window function
    // index is ID
    std::vector<std::unordered_map<uint32_t, Value>> window_function_result{buffer.size()};

    // deal with every window function
    for (auto &[column_index, window_function] : plan_->window_functions_) {
      // generator new order by, insert partition by to the front of order by
      std::vector<OrderBy> order_by;
      order_by.reserve(window_function.partition_by_.size() + window_function.order_by_.size());
      for (const auto &partition : window_function.partition_by_) {
        order_by.emplace_back(OrderByType::DEFAULT, OrderByNullType::DEFAULT, partition);
      }
      for (const auto &order : window_function.order_by_) {
        order_by.push_back(order);
      }

      // sort tuples
      std::vector<std::pair<SortEntry, IdType>> sorted_tuples;
      sorted_tuples.reserve(buffer.size());
      TupleComparator tuple_comparator{order_by};
      for (const auto &[tuple, id] : buffer) {
        sorted_tuples.emplace_back(
            SortEntry{GenerateSortKey(tuple, order_by, child_executor_->GetOutputSchema()), tuple}, id);
      }
      std::sort(sorted_tuples.begin(), sorted_tuples.end(),
                [&tuple_comparator](const std::pair<SortEntry, IdType> &lhs, const std::pair<SortEntry, IdType> &rhs)
                    -> bool { return tuple_comparator(lhs.first, rhs.first); });

      // helper function
      // give a start iter
      // return a iter at the begin of next partition
      auto find_next_partition = [&sorted_tuples, &schema = child_executor_->GetOutputSchema(),
                                  &partition_by =
                                      window_function.partition_by_](decltype(sorted_tuples)::const_iterator start) {
        BUSTUB_ASSERT(start <= sorted_tuples.cend(), "WindowFunctionExecutor::Next(): invalid iterator");
        if (start == sorted_tuples.cend()) {
          return start;
        }
        auto &start_tuple = start->first.second;
        std::vector<Value> partition_keys;
        partition_keys.reserve(partition_by.size());
        for (const auto &partition : partition_by) {
          partition_keys.push_back(partition->Evaluate(&start_tuple, schema));
        }
        return std::find_if(
            start, sorted_tuples.cend(),
            [&start_tuple = start->first.second, &schema, &partition_by, &partition_keys](const auto &pair) {
              // if any partition is not match, then it is a new partition
              auto &end_tuple = pair.first.second;
              for (size_t i = 0; i < partition_by.size(); ++i) {
                auto key = partition_by[i]->Evaluate(&end_tuple, schema);
                auto is_not_equal = (partition_keys[i].CompareEquals(key) == CmpBool::CmpFalse);
                if (is_not_equal) {
                  return true;
                }
              }
              return false;
            });
      };

      // helper function
      // give a start iter, and a work function, user can do some work while walking through sorted_tuples
      // we will pass a tuple to work function
      // return a iter at the begin of next partition
      auto find_next_partition_with_work_function =
          [&sorted_tuples, &schema = child_executor_->GetOutputSchema(), &partition_by = window_function.partition_by_](
              decltype(sorted_tuples)::const_iterator start, auto &&work_function) {
            return std::find_if(
                start, sorted_tuples.cend(),
                [&start_tuple = start->first.second, &schema, &partition_by,
                 &work_function](const decltype(sorted_tuples)::value_type &pair) {
                  // if any partition is not match, then it is a new partition
                  auto is_reach_new_partition = std::any_of(
                      partition_by.cbegin(), partition_by.cend(),
                      [&start_tuple, &end_tuple = pair.first.second, &schema](const AbstractExpressionRef &partition) {
                        auto lhs = partition->Evaluate(&start_tuple, schema);
                        auto rhs = partition->Evaluate(&end_tuple, schema);
                        auto is_not_equal = (lhs.CompareEquals(rhs) == CmpBool::CmpFalse);
                        return is_not_equal;
                      });
                  if (!is_reach_new_partition) {
                    work_function(pair.first.second);
                  }
                  return is_reach_new_partition;
                });
          };

      switch (window_function.type_) {
        case WindowFunctionType::CountStarAggregate: {
          // empty partition by and empty order by
          if (window_function.partition_by_.empty() && window_function.order_by_.empty()) {
            for (auto &[sort_entry, id] : sorted_tuples) {
              window_function_result[id][column_index] = ValueFactory::GetIntegerValue(sorted_tuples.size());
            }
            break;
          }

          // empty order by
          if (window_function.order_by_.empty()) {
            for (auto first = sorted_tuples.cbegin(), last = find_next_partition(first); first < sorted_tuples.cend();
                 first = last, last = find_next_partition(first)) {
              for (auto iter = first; iter < last; ++iter) {
                auto id = iter->second;
                window_function_result[id][column_index] = ValueFactory::GetIntegerValue(last - first);
              }
            }
            break;
          }

          // nothing is empty
          for (auto first = sorted_tuples.cbegin(), last = find_next_partition(first); first < sorted_tuples.cend();
               first = last, last = find_next_partition(first)) {
            for (auto iter = first; iter < last; ++iter) {
              auto id = iter->second;
              window_function_result[id][column_index] = ValueFactory::GetIntegerValue(iter - first + 1);
            }
          }
          break;
        }
        case WindowFunctionType::CountAggregate: {
          auto column = std::dynamic_pointer_cast<ColumnValueExpression>(window_function.function_);
          BUSTUB_ASSERT(column != nullptr, "WindowFunctionExecutor::Next(): can not count non-column");
          size_t count = 0;
          // do count helper function, if satisfied, ++count
          auto do_count = [&count, &schema = child_executor_->GetOutputSchema(),
                           col_index = column->GetColIdx()](const Tuple &tuple) -> size_t {
            if (!tuple.GetValue(&schema, col_index).IsNull()) {
              ++count;
            }
            return count;
          };

          // empty partition by and empty order by
          if (window_function.partition_by_.empty() && window_function.order_by_.empty()) {
            // count all tuples
            for (auto &pair : sorted_tuples) {
              do_count(pair.first.second);
            }
            // put in result
            for (auto &[sort_entry, id] : sorted_tuples) {
              window_function_result[id][column_index] = ValueFactory::GetIntegerValue(count);
            }
            break;
          }

          // empty order by
          if (window_function.order_by_.empty()) {
            for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, do_count);
                 first < sorted_tuples.cend();
                 count = 0, first = last, last = find_next_partition_with_work_function(first, do_count)) {
              for (auto iter = first; iter < last; ++iter) {
                auto id = iter->second;
                window_function_result[id][column_index] = ValueFactory::GetIntegerValue(count);
              }
            }
            break;
          }

          // nothing is empty
          // index is id
          std::vector<size_t> accumulate_counts;
          auto work_function = [&do_count, &accumulate_counts](const Tuple &tuple) {
            accumulate_counts.push_back(do_count(tuple));
          };
          for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, work_function);
               first < sorted_tuples.cend(); count = 0, accumulate_counts.clear(), first = last,
                    last = find_next_partition_with_work_function(first, work_function)) {
            for (auto iter = first; iter < last; ++iter) {
              auto id = iter->second;
              window_function_result[id][column_index] = ValueFactory::GetIntegerValue(accumulate_counts[iter - first]);
            }
          }
          break;
        }
        case WindowFunctionType::SumAggregate: {
          auto column = std::dynamic_pointer_cast<ColumnValueExpression>(window_function.function_);
          BUSTUB_ASSERT(column != nullptr, "WindowFunctionExecutor::Next(): can not count non-column");
          auto value_type_id = child_executor_->GetOutputSchema().GetColumn(column->GetColIdx()).GetType();
          Value sum{value_type_id};
          BUSTUB_ASSERT(sum.CheckInteger(), "WindowFunctionExecutor::Next(): try to sum a non-integer column");
          // do count helper function, if satisfied, ++count
          auto do_sum = [&sum, &schema = child_executor_->GetOutputSchema(),
                         col_index = column->GetColIdx()](const Tuple &tuple) -> Value {
            auto value = tuple.GetValue(&schema, col_index);
            if (!value.IsNull()) {
              if (sum.IsNull()) {
                sum = value;
              } else {
                sum = sum.Add(value);
              }
            }
            return sum;
          };

          // empty partition by and empty order by
          if (window_function.partition_by_.empty() && window_function.order_by_.empty()) {
            // count all tuples
            for (auto &pair : sorted_tuples) {
              do_sum(pair.first.second);
            }
            // put in result
            for (auto &[sort_entry, id] : sorted_tuples) {
              window_function_result[id][column_index] = sum;
            }
            break;
          }

          // empty order by
          if (window_function.order_by_.empty()) {
            for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, do_sum);
                 first < sorted_tuples.cend(); sum = Value{value_type_id}, first = last,
                      last = find_next_partition_with_work_function(first, do_sum)) {
              for (auto iter = first; iter < last; ++iter) {
                auto id = iter->second;
                window_function_result[id][column_index] = sum;
              }
            }
            break;
          }

          // nothing is empty
          // index is id
          std::vector<Value> accumulate_sums;
          auto work_function = [&do_sum, &accumulate_sums](const Tuple &tuple) {
            accumulate_sums.push_back(do_sum(tuple));
          };
          for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, work_function);
               first < sorted_tuples.cend(); sum = Value{value_type_id}, accumulate_sums.clear(), first = last,
                    last = find_next_partition_with_work_function(first, work_function)) {
            for (auto iter = first; iter < last; ++iter) {
              auto id = iter->second;
              window_function_result[id][column_index] = accumulate_sums[iter - first];
            }
          }
          break;
        }
        case WindowFunctionType::MinAggregate: {
          auto column = std::dynamic_pointer_cast<ColumnValueExpression>(window_function.function_);
          BUSTUB_ASSERT(column != nullptr, "WindowFunctionExecutor::Next(): can not count non-column");
          auto value_type_id = child_executor_->GetOutputSchema().GetColumn(column->GetColIdx()).GetType();
          Value min{value_type_id};
          BUSTUB_ASSERT(min.CheckInteger(), "WindowFunctionExecutor::Next(): try to compare a non-integer column");
          // do count helper function, if satisfied, ++count
          auto do_min = [&min, &schema = child_executor_->GetOutputSchema(),
                         col_index = column->GetColIdx()](const Tuple &tuple) -> Value {
            auto value = tuple.GetValue(&schema, col_index);
            if (!value.IsNull()) {
              if (min.IsNull()) {
                min = value;
              } else {
                min = min.Min(value);
              }
            }
            return min;
          };

          // empty partition by and empty order by
          if (window_function.partition_by_.empty() && window_function.order_by_.empty()) {
            // count all tuples
            for (auto &pair : sorted_tuples) {
              do_min(pair.first.second);
            }
            // put in result
            for (auto &[sort_entry, id] : sorted_tuples) {
              window_function_result[id][column_index] = min;
            }
            break;
          }

          // empty order by
          if (window_function.order_by_.empty()) {
            for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, do_min);
                 first < sorted_tuples.cend(); min = Value{value_type_id}, first = last,
                      last = find_next_partition_with_work_function(first, do_min)) {
              for (auto iter = first; iter < last; ++iter) {
                auto id = iter->second;
                window_function_result[id][column_index] = min;
              }
            }
            break;
          }

          // nothing is empty
          // index is id
          std::vector<Value> accumulate_mins;
          auto work_function = [&do_min, &accumulate_mins](const Tuple &tuple) {
            accumulate_mins.push_back(do_min(tuple));
          };
          for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, work_function);
               first < sorted_tuples.cend(); min = Value{value_type_id}, accumulate_mins.clear(), first = last,
                    last = find_next_partition_with_work_function(first, work_function)) {
            for (auto iter = first; iter < last; ++iter) {
              auto id = iter->second;
              window_function_result[id][column_index] = accumulate_mins[iter - first];
            }
          }
          break;
        }
        case WindowFunctionType::MaxAggregate: {
          auto column = std::dynamic_pointer_cast<ColumnValueExpression>(window_function.function_);
          BUSTUB_ASSERT(column != nullptr, "WindowFunctionExecutor::Next(): can not count non-column");
          auto value_type_id = child_executor_->GetOutputSchema().GetColumn(column->GetColIdx()).GetType();
          Value max{value_type_id};
          BUSTUB_ASSERT(max.CheckInteger(), "WindowFunctionExecutor::Next(): try to compare a non-integer column");
          // do count helper function, if satisfied, ++count
          auto do_max = [&max, &schema = child_executor_->GetOutputSchema(),
                         col_index = column->GetColIdx()](const Tuple &tuple) -> Value {
            auto value = tuple.GetValue(&schema, col_index);
            if (!value.IsNull()) {
              if (max.IsNull()) {
                max = value;
              } else {
                max = max.Max(value);
              }
            }
            return max;
          };

          // empty partition by and empty order by
          if (window_function.partition_by_.empty() && window_function.order_by_.empty()) {
            // count all tuples
            for (auto &pair : sorted_tuples) {
              do_max(pair.first.second);
            }
            // put in result
            for (auto &[sort_entry, id] : sorted_tuples) {
              window_function_result[id][column_index] = max;
            }
            break;
          }

          // empty order by
          if (window_function.order_by_.empty()) {
            for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, do_max);
                 first < sorted_tuples.cend(); max = Value{value_type_id}, first = last,
                      last = find_next_partition_with_work_function(first, do_max)) {
              for (auto iter = first; iter < last; ++iter) {
                auto id = iter->second;
                window_function_result[id][column_index] = max;
              }
            }
            break;
          }

          // nothing is empty
          // index is id
          std::vector<Value> accumulate_maxs;
          auto work_function = [&do_max, &accumulate_maxs](const Tuple &tuple) {
            accumulate_maxs.push_back(do_max(tuple));
          };
          for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, work_function);
               first < sorted_tuples.cend(); max = Value{value_type_id}, accumulate_maxs.clear(), first = last,
                    last = find_next_partition_with_work_function(first, work_function)) {
            for (auto iter = first; iter < last; ++iter) {
              auto id = iter->second;
              window_function_result[id][column_index] = accumulate_maxs[iter - first];
            }
          }
          break;
        }
        case WindowFunctionType::Rank: {
          // auto column = std::dynamic_pointer_cast<ColumnValueExpression>(window_function.function_);
          // BUSTUB_ASSERT(column != nullptr, "WindowFunctionExecutor::Next(): can not count non-column");
          size_t rank = 0;
          size_t row_count = 0;
          SortKey previous_sort_key;
          // do rank helper function, if satisfied, rank = row_count
          auto do_rank = [&rank, &row_count, &previous_sort_key, &schema = child_executor_->GetOutputSchema(),
                          &order_by, &tuple_comparator](const Tuple &tuple) -> size_t {
            ++row_count;
            auto curr_sort_key = GenerateSortKey(tuple, order_by, schema);
            // if current_sort_key != previous_sort_key, move rank
            if (previous_sort_key.empty() || !tuple_comparator.IsEqual(previous_sort_key, curr_sort_key)) {
              previous_sort_key = std::move(curr_sort_key);
              rank = row_count;
            }

            return rank;
          };

          // empty partition by and empty order by
          if (window_function.partition_by_.empty() && window_function.order_by_.empty()) {
            // count all tuples
            for (auto &pair : sorted_tuples) {
              do_rank(pair.first.second);
            }
            // put in result
            for (auto &[sort_entry, id] : sorted_tuples) {
              window_function_result[id][column_index] = ValueFactory::GetIntegerValue(rank);
            }
            break;
          }

          // empty order by
          if (window_function.order_by_.empty()) {
            for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, do_rank);
                 first < sorted_tuples.cend(); rank = 0, row_count = 0, previous_sort_key.clear(), first = last,
                      last = find_next_partition_with_work_function(first, do_rank)) {
              for (auto iter = first; iter < last; ++iter) {
                auto id = iter->second;
                window_function_result[id][column_index] = ValueFactory::GetIntegerValue(rank);
              }
            }
            break;
          }

          // nothing is empty
          // index is id
          std::vector<size_t> accumulate_ranks;
          auto work_function = [&do_rank, &accumulate_ranks](const Tuple &tuple) {
            accumulate_ranks.push_back(do_rank(tuple));
          };
          for (auto first = sorted_tuples.cbegin(), last = find_next_partition_with_work_function(first, work_function);
               first < sorted_tuples.cend();
               rank = 0, row_count = 0, previous_sort_key.clear(), accumulate_ranks.clear(), first = last,
                    last = find_next_partition_with_work_function(first, work_function)) {
            for (auto iter = first; iter < last; ++iter) {
              auto id = iter->second;
              window_function_result[id][column_index] = ValueFactory::GetIntegerValue(accumulate_ranks[iter - first]);
            }
          }
          break;
        }
        default:
          throw NotImplementedException("invalid window function type");
      }
    }

    // select
    for (const auto &[tuple, id] : buffer) {
      std::vector<Value> values{plan_->columns_.size()};
      for (size_t i = 0; i < plan_->columns_.size(); ++i) {
        auto column = std::dynamic_pointer_cast<ColumnValueExpression>(plan_->columns_[i]);
        auto column_index = column->GetColIdx();
        // skip placeholder
        if (column_index == std::numeric_limits<uint32_t>::max()) {
          continue;
        }
        values[i] = tuple.GetValue(&child_executor_->GetOutputSchema(), column_index);
      }

      // insert window function value
      auto results = window_function_result[id];
      for (const auto &[column_index, value] : results) {
        values[column_index] = value;
      }

      // store tuple
      tuples_.emplace_back(std::move(values), plan_->output_schema_.get());
    }

    iter_ = tuples_.begin();
    phase_ = Phase::kTransfer;
  }

  for (; iter_ != tuples_.cend() && tuple_batch->size() < batch_size; ++iter_) {
    tuple_batch->push_back(std::move(*iter_));
    rid_batch->emplace_back();
  }
  return !tuple_batch->empty();
}
}  // namespace bustub
