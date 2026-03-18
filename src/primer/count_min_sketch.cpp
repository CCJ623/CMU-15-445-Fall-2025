//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("Invalid width or depth.");
  }
  result_.resize(width_ * depth_, 0);

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  hash_functions_ = std::move(other.hash_functions_);
  result_ = std::move(other.result_);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  width_ = other.width_;
  depth_ = other.depth_;
  hash_functions_ = std::move(other.hash_functions_);
  result_ = std::move(other.result_);
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (decltype(depth_) i = 0; i < depth_; ++i) {
    result_[GetIndex(i, hash_functions_[i](item))] += 1;
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (size_t index = 0, end = width_ * depth_; index < end; ++index) {
    result_[index] += other.result_[index];
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  decltype(Count(item)) count = std::numeric_limits<decltype(count)>::max();
  for (decltype(depth_) row = 0; row < depth_; ++row) {
    decltype(width_) col = hash_functions_[row](item);
    count = std::min(count, static_cast<decltype(count)>(result_[GetIndex(row, col)]));
  }

  return count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (auto &element : result_) {
    element = 0;
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  using ResultType = decltype(TopK(k, candidates));
  ResultType result;
  result.reserve(k);
  auto compare = [](typename ResultType::value_type lhs, typename ResultType::value_type rhs) {
    return lhs.second < rhs.second;
  };
  std::priority_queue<typename ResultType::value_type, std::vector<typename ResultType::value_type>, decltype(compare)>
      pq(compare);
  for (const auto &item : candidates) {
    pq.push(std::make_pair(item, Count(item)));
  }
  for (decltype(k) i = 0; i < k; ++i) {
    result.push_back(pq.top());
    pq.pop();
  }
  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
