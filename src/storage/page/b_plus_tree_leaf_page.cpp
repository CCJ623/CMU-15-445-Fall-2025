//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <optional>
#include <sstream>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT(0 < max_size && static_cast<size_t>(max_size) <= LEAF_PAGE_SLOT_CNT,
                "BPlusTreeInternalPage::Init(): max_size out of range");

  SetSize(0);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  next_page_id_ = INVALID_PAGE_ID;
  num_tombstones_ = 0;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size, const std::vector<MappingType> &kvs) {
  Init(max_size);

  BUSTUB_ASSERT(0 <= kvs.size() && kvs.size() <= static_cast<size_t>(GetMaxSize()),
                "BPlusTreeInternalPage::Init(): kvs.size() out of range");

  SetSize(kvs.size());
  for (size_t index = 0; index != kvs.size(); ++index) {
    key_array_[index] = kvs[index].first;
    rid_array_[index] = kvs[index].second;
  }
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> result;
  result.reserve(num_tombstones_);
  std::transform(tombstones_, tombstones_ + num_tombstones_, std::back_inserter(result),
                 [this](size_t index) { return key_array_[index]; });
  return result;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(0 <= index && index <= GetSize(), "KeyAt(): index out of range");
  return key_array_[index];
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(0 <= index && index < GetSize(), "ValueAt(): index out of range");

  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  auto result = std::lower_bound(cbegin(key_array_), cend(key_array_), key, ConvertToStdComparator(comparator));
  if (result == cend(key_array_) || comparator(key, *result) != 0) {
    // key not exist
    return std::nullopt;
  }
  return ValueAt(std::distance(cbegin(key_array_), result));
}

// assume key is not presented
// insert KV
// split if full and tomb is empty
// return second half if split, nullopt if not split
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> std::optional<std::vector<MappingType>> {
  BUSTUB_ASSERT(!Contains(key, comparator), "BPlusTreeLeafPage::Insert(): try to insert a key which already exist");

  if (!IsFull()) {
    // not full
    InsertHelper(key, value, comparator);
    return std::nullopt;
  }

  // page is full, try to clear tombstones
  EvictTombstones();
  if (!IsFull()) {
    // evict success
    InsertHelper(key, value, comparator);
    return std::nullopt;
  }

  /*
  first, we find where to insert
  we assume that we already insert key into key_array_
  then, we calculate the mid_index based on this assumption, making second half is bigger(equal)
  warning: any index is index of value_array_, so when calculating with key_array_, we must add 1
  */

  auto begin_iter = begin(key_array_);
  auto end_iter = end(key_array_);
  auto assume_end_iter = end_iter + 1;
  auto insert_iter = std::upper_bound(begin(key_array_), end(key_array_), key, ConvertToStdComparator(comparator));
  auto split_begin_iter = begin_iter + std::distance(begin_iter, assume_end_iter) / 2;
  // result is begin with a invalid key
  std::vector<MappingType> result;
  result.reserve(std::distance(split_begin_iter, assume_end_iter));
  if (insert_iter < split_begin_iter) {
    // insert into first half
    // copy last element of first half to front of second half
    result.emplace_back(*(split_begin_iter - 1), *(GetCorrespondValueIter(split_begin_iter - 1)));
    // copy rest of value to second half
    for (auto iter = split_begin_iter; iter != end_iter; ++iter) {
      result.emplace_back(*iter, *GetCorrespondValueIter(iter));
    }
    SetSize(std::distance(begin(rid_array_), GetCorrespondValueIter(split_begin_iter - 1)));
    InsertHelper(insert_iter, key, value, comparator);
  } else {
    // insert into second half
    // copy kv before insert_iter
    for (auto iter = split_begin_iter; iter != insert_iter; ++iter) {
      result.emplace_back(*iter, *GetCorrespondValueIter(iter));
    }
    // insert target kv
    result.emplace_back(key, value);
    // copy kv after insert index
    for (auto iter = insert_iter; iter != end_iter; ++iter) {
      result.emplace_back(*iter, *GetCorrespondValueIter(iter));
    }
    SetSize(std::distance(begin(rid_array_), GetCorrespondValueIter(split_begin_iter)));
  }

  BUSTUB_ASSERT(!IsInsufficient(), "BPlusTreeLeafPage::Insert(): first half become insufficient after split");
  BUSTUB_ASSERT(result.size() >= static_cast<size_t>(GetMinSize()),
                "BPlusTreeLeafPage::Insert(): second half become insufficient after split");
  return result;
}

// return true if key in key_array_ and not in tombstones
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Contains(const KeyType &key, const KeyComparator &comparator) const -> bool {
  auto is_in_key_array = std::binary_search(begin(key_array_), end(key_array_), key, comparator);
  if (!is_in_key_array) {
    return false;
  }
  auto tombstones = GetTombstones();
  return std::find_if(tombstones.cbegin(), tombstones.cend(), [&key, &comparator](const auto &tomb_key) {
           return comparator(key, tomb_key) == 0;
         }) == tombstones.cend();
}

// assume not full
// assume key is not exist
// assume iter is valid
// assume *iter = key is ordered
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertHelper(KeyIterType key_iter, const KeyType &key, const ValueType &value,
                                              const KeyComparator &comparator) {
  BUSTUB_ASSERT(!IsFull(), "BPlusTreeInternalPage::InsertHelper(): try to insert to a full page");
  BUSTUB_ASSERT(!Contains(key, comparator),
                "BPlusTreeInternalPage::InsertHelper(): try to insert a key which already exist");
  BUSTUB_ASSERT(
      std::upper_bound(begin(key_array_), end(key_array_), key, ConvertToStdComparator(comparator)) == key_iter,
      "BPlusTreeInternalPage::InsertHelper(): try to insert a key to a unordered place");

  auto value_iter = GetCorrespondValueIter(key_iter);
  std::move_backward(key_iter, end(key_array_), end(key_array_) + 1);
  *key_iter = key;
  std::move_backward(value_iter, end(rid_array_), end(rid_array_) + 1);
  *value_iter = value;
  ChangeSizeBy(1);
}

// assume not full
// assume key is not exist
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertHelper(const KeyType &key, const ValueType &value,
                                              const KeyComparator &comparator) {
  BUSTUB_ASSERT(!IsFull(), "BPlusTreeLeafPage::InsertHelper(): try to insert to a full page");
  BUSTUB_ASSERT(!Contains(key, comparator),
                "BPlusTreeLeafPage::InsertHelper(): try to insert a key which already exist");

  auto key_iter = std::upper_bound(begin(key_array_), end(key_array_), key, ConvertToStdComparator(comparator));
  return InsertHelper(key_iter, key, value, comparator);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::EvictTombstones() {
  if (num_tombstones_ == 0) {
    return;
  }
  auto evict_index = tombstones_[num_tombstones_ - 1];
  auto key_evict_iter = begin(key_array_) + evict_index;
  auto value_evict_iter = begin(rid_array_) + evict_index;
  std::copy(key_evict_iter + 1, end(key_array_), key_evict_iter);
  std::copy(value_evict_iter + 1, end(rid_array_), value_evict_iter);
  --num_tombstones_;
  ChangeSizeBy(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
