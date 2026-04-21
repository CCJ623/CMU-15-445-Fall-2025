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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> const KeyType & {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> const ValueType & {
  BUSTUB_ASSERT(0 <= index && index < GetSize(), "BPlusTreeLeafPage::ValueAt(): index out of range");

  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindLowKey(const KeyType &key, const KeyComparator &comparator) const -> size_t {
  auto key_iter = std::lower_bound(Cbegin(key_array_), Cend(key_array_), key, ConvertToStdComparator(comparator));
  return std::distance(Cbegin(key_array_), key_iter);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  auto key_iter = std::lower_bound(Cbegin(key_array_), Cend(key_array_), key, ConvertToStdComparator(comparator));
  if (key_iter == Cend(key_array_) || comparator(key, *key_iter) != 0) {
    // key not exist
    return std::nullopt;
  }
  // key exist
  auto index = std::distance(Cbegin(key_array_), key_iter);
  if (TombstonesContains(index)) {
    return std::nullopt;
  }
  return ValueAt(index);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                           bool is_in_tombstones) {
  BUSTUB_ASSERT(!Contains(key, comparator), "BPlusTreeLeafPage::PushFront(): try to insert a key which already exist");
  BUSTUB_ASSERT(!IsFull(), "BPlusTreeLeafPage::PushFront(): try to insert a key to a full page");
  BUSTUB_ASSERT(comparator(key, KeyAt(0)) < 0, "BPlusTreeLeafPage::PushFront(): try to push front a key >= key[0]");

  InsertHelper(Begin(key_array_), key, value, comparator);
  if (is_in_tombstones) {
    TombstonesPushFront(0);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                          bool is_in_tombstones) {
  BUSTUB_ASSERT(!Contains(key, comparator), "BPlusTreeLeafPage::PushBack(): try to insert a key which already exist");
  BUSTUB_ASSERT(!IsFull(), "BPlusTreeLeafPage::PushBack(): try to insert a key to a full page");
  BUSTUB_ASSERT(comparator(key, *(End(key_array_) - 1)) > 0,
                "BPlusTreeLeafPage::PushFront(): try to push front a key <= key_array.back()");

  InsertHelper(End(key_array_), key, value, comparator);
  if (is_in_tombstones) {
    TombstonesPushBack(GetSize() - 1);
  }
}

// assume key is not presented
// insert KV
// split if full and tomb is empty
// return second half if split, nullopt if not split
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> std::optional<std::vector<MappingType>> {
  BUSTUB_ASSERT(!Contains(key, comparator), "BPlusTreeLeafPage::Insert(): try to insert a key which already exist");

  {
    auto index = FindLowKey(key, comparator);
    if (comparator(KeyAt(index), key) == 0 && std::binary_search(TombstonesBegin(), TombstonesEnd(), index)) {
      // key in tombstones, revive
      rid_array_[index] = value;
      TombstonesRemove(index);
      return std::nullopt;
    }
  }

  if (!IsFull()) {
    // not full
    InsertHelper(key, value, comparator);
    return std::nullopt;
  }

  // page is full, try to clear tombstones
  if (!IsTombstonesEmpty()) {
    // can evict tombstones
    EvictTombstones();
    InsertHelper(key, value, comparator);
    return std::nullopt;
  }

  // need to split

  /*
  first, we find where to insert
  we assume that we already insert key into key_array_
  then, we calculate the mid_index based on this assumption, making second half is bigger(equal)
  warning: any index is index of value_array_, so when calculating with key_array_, we must add 1
  */

  auto begin_iter = Begin(key_array_);
  auto end_iter = End(key_array_);
  auto assume_end_iter = end_iter + 1;
  auto insert_iter = std::upper_bound(Begin(key_array_), End(key_array_), key, ConvertToStdComparator(comparator));
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
    SetSize(std::distance(Begin(rid_array_), GetCorrespondValueIter(split_begin_iter - 1)));
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
    SetSize(std::distance(Begin(rid_array_), GetCorrespondValueIter(split_begin_iter)));
  }

  BUSTUB_ASSERT(!IsInsufficient(), "BPlusTreeLeafPage::Insert(): first half become insufficient after split");
  BUSTUB_ASSERT(result.size() >= static_cast<size_t>(GetMinSize()),
                "BPlusTreeLeafPage::Insert(): second half become insufficient after split");
  return result;
}

// remove KV at index
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(size_t index) {
  BUSTUB_ASSERT(0 <= index && index < static_cast<size_t>(GetSize()),
                "BPlusTreeInternalPage::Remove(): index out of range");

  if (!IsTombstonesFull()) {
    // tombstones not full, add to it
    TombstonesInsert(index);
  } else if (!IsTombstonesEmpty()) {
    // tombstones full, evict
    auto evict_index = EvictTombstones();
    if (evict_index < static_cast<size_t>(index)) {
      --index;
    }
    TombstonesInsert(index);
  } else {
    // tombstones not enabled
    auto key_evict_iter = Begin(key_array_) + index;
    auto value_evict_iter = Begin(rid_array_) + index;
    std::copy(key_evict_iter + 1, End(key_array_), key_evict_iter);
    std::copy(value_evict_iter + 1, End(rid_array_), value_evict_iter);
    ChangeSizeBy(-1);
  }
}

// remove key
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  BUSTUB_ASSERT(GetValue(key, comparator) != std::nullopt,
                "BPlusTreeInternalPage::Remove(): try to remove a non-exist key");

  auto key_iter = std::lower_bound(Begin(key_array_), End(key_array_), key, ConvertToStdComparator(comparator));
  auto key_index = std::distance(Begin(key_array_), key_iter);
  Remove(key_index);
}

// return true if key in key_array_ and not in tombstones
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Contains(const KeyType &key, const KeyComparator &comparator) const -> bool {
  auto is_in_key_array =
      std::binary_search(Begin(key_array_), End(key_array_), key, ConvertToStdComparator(comparator));
  if (!is_in_key_array) {
    return false;
  }
  auto tombstones = GetTombstones();
  return std::find_if(tombstones.cbegin(), tombstones.cend(), [&key, &comparator](const auto &tomb_key) {
           return comparator(key, tomb_key) == 0;
         }) == tombstones.cend();
}

// assume page is healthy
// return min kv and whether it is in tombstones
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EvictMinKv() -> std::pair<MappingType, bool> {
  BUSTUB_ASSERT(IsHealthy(), "BPlusTreeInternalPage::InsertHelper(): try to evict from a unhealthy page");

  MappingType result;
  size_t index = 0;
  result.first = KeyAt(index);
  result.second = ValueAt(index);
  bool is_in_tombstones = (*TombstonesBegin() == index);
  // remove kv
  std::copy(Begin(key_array_) + 1, End(key_array_), Begin(key_array_));
  std::copy(Begin(rid_array_) + 1, End(rid_array_), Begin(rid_array_));
  ChangeSizeBy(-1);
  // decrease tombstones because i remove the min kv
  std::for_each(TombstonesBegin(), TombstonesEnd(), [](auto &index) { index = index - 1; });
  if (is_in_tombstones) {
    // kv in tombstones
    TombstonesPopFront();
    return {result, true};
  }
  // kv not in tombstones
  return {result, false};
}

// assume page is healthy
// return max kv and whether it is in tombstones
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EvictMaxKv() -> std::pair<MappingType, bool> {
  BUSTUB_ASSERT(IsHealthy(), "BPlusTreeInternalPage::InsertHelper(): try to evict from a unhealthy page");

  MappingType result;
  size_t index = std::distance(Begin(key_array_), End(key_array_)) - 1;
  result.first = KeyAt(index);
  result.second = ValueAt(index);
  bool is_in_tombstones = (*(TombstonesEnd() - 1) == index);
  // remove kv
  ChangeSizeBy(-1);
  // decrease tombstones because i remove the min kv
  std::for_each(TombstonesBegin(), TombstonesEnd(), [](auto &index) { index = index - 1; });
  if (is_in_tombstones) {
    // kv in tombstones
    TombstonesPopBack();
    return {result, true};
  }
  // kv not in tombstones
  return {result, false};
}

// assume another is at right of this, in another words, all another's key are bigger than this
// merge key, value, tombstones
// set next page id to skip merged page
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage &another, const KeyComparator &comparator) {
  BUSTUB_ASSERT(GetSize() + another.GetSize() >= GetMinSize(), "BPlusTreeInternalPage::Merge(): invalid after merge");
  if (GetSize() != 0 && another.GetSize() != 0) {
    BUSTUB_ASSERT(comparator(*(Cend(key_array_) - 1), *another.Cbegin(another.key_array_)) < 0,
                  "BPlusTreeInternalPage::Merge(): unorder merge");
  }

  auto key_iter = another.Cbegin(another.key_array_);
  auto key_end = another.Cend(another.key_array_);
  auto value_iter = another.Cbegin(another.rid_array_);
  auto tombstones_iter = another.TombstonesBegin();

  for (; key_iter != key_end; ++key_iter, ++value_iter) {
    const auto &key = *key_iter;
    const auto &value = *value_iter;
    bool is_in_tombstones = false;
    if (tombstones_iter != another.TombstonesEnd() &&
        *tombstones_iter == static_cast<size_t>(std::distance(another.Cbegin(another.key_array_), key_iter))) {
      // key in tombstones
      is_in_tombstones = true;
      ++tombstones_iter;
    }
    if (is_in_tombstones && IsTombstonesFull()) {
      // no place for this KV
      continue;
    }
    PushBack(key, value, comparator, is_in_tombstones);
  }

  SetNextPageId(another.GetNextPageId());
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
      std::upper_bound(Begin(key_array_), End(key_array_), key, ConvertToStdComparator(comparator)) == key_iter,
      "BPlusTreeInternalPage::InsertHelper(): try to insert a key to a unordered place");

  auto value_iter = GetCorrespondValueIter(key_iter);
  std::move_backward(key_iter, End(key_array_), End(key_array_) + 1);
  *key_iter = key;
  std::move_backward(value_iter, End(rid_array_), End(rid_array_) + 1);
  *value_iter = value;

  // adjust tombstones
  auto index = std::distance(Begin(key_array_), key_iter);
  auto tombstones_iter = std::lower_bound(TombstonesBegin(), TombstonesEnd(), index);
  std::for_each(tombstones_iter, TombstonesEnd(), [](auto &index) { ++index; });
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

  auto key_iter = std::upper_bound(Begin(key_array_), End(key_array_), key, ConvertToStdComparator(comparator));
  return InsertHelper(key_iter, key, value, comparator);
}

// key/value may move after eviction
// always evict the biggest index, all elements in front of it will not be affected
// all elements behind it will move forward
// return evicted index
// user must reacquire index based on return index
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EvictTombstones() -> size_t {
  BUSTUB_ASSERT(!IsTombstonesEmpty(), "BPlusTreeLeafPage::EvictTombstones(): try to evict a empty tombstones");

  auto evict_index = tombstones_[num_tombstones_ - 1];
  auto key_evict_iter = Begin(key_array_) + evict_index;
  auto value_evict_iter = Begin(rid_array_) + evict_index;
  std::copy(key_evict_iter + 1, End(key_array_), key_evict_iter);
  std::copy(value_evict_iter + 1, End(rid_array_), value_evict_iter);
  --num_tombstones_;
  ChangeSizeBy(-1);
  return evict_index;
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
