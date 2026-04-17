//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT(0 < max_size && static_cast<size_t>(max_size) <= INTERNAL_PAGE_SLOT_CNT,
                "BPlusTreeInternalPage::Init(): max_size out of range");

  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size, const std::vector<MappingType> &kvs) {
  Init(max_size);

  BUSTUB_ASSERT(0 <= kvs.size() && kvs.size() <= static_cast<size_t>(GetMaxSize()),
                "BPlusTreeInternalPage::Init(): kvs.size() out of range");

  SetSize(kvs.size());
  for (size_t index = 0; index != kvs.size(); ++index) {
    key_array_[index] = kvs[index].first;
    page_id_array_[index] = kvs[index].second;
  }
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(0 < index && index < GetSize(), "KeyAt(): index out of range");

  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(0 < index && index < GetSize(), "SetKeyAt(): index out of range");

  key_array_[index] = key;
}

/**
 * @brief Set value at the specified index.
 *
 * @param index The index of the value to set. Index must be non-zero.
 * @param value The new value for value
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(0 <= index && index < GetSize(), "SetKeyAt(): index out of range");

  page_id_array_[index] = value;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(0 <= index && index < GetSize(), "ValueAt(): index out of range");

  return page_id_array_[index];
}

// return page id that key belong
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto target_iter = std::upper_bound(cbegin(key_array_), cend(key_array_), key, ConvertToStdComparator(comparator));
  auto index = std::distance(cbegin(key_array_), target_iter);
  return ValueAt(index);
}

// get the spliit position when inserting key
// assume page is full
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSplitPos(const KeyType &key, const KeyComparator &comparator) const -> uint {
  BUSTUB_ASSERT(IsFull(), "BPlusTreeInternalPage::GetSplitPos(): try to get split position of a non-full page");

  auto mid_index = std::distance(cbegin(key_array_), cend(key_array_)) / 2;
  const auto &mid_key = KeyAt(mid_index);
  if (comparator(key, mid_key) < 0) {
    return mid_index;
  }
  return mid_index + 1;
}

// assume key is not exist in this page
// return nullopt if no split happened
// return second half if split happened
// because this is a internal page, we will push up the first key of second half
// so we must make sure that second_half.size() >= first_half.size()
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> std::optional<std::vector<MappingType>> {
  BUSTUB_ASSERT(!Contains(key, comparator), "BPlusTreeLeafPage::Insert(): try to insert a key which already exist");

  if (!IsFull()) {
    // not full
    InsertHelper(key, value, comparator);
    return std::nullopt;
  }

  // full, need to split

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
    SetSize(std::distance(begin(page_id_array_), GetCorrespondValueIter(split_begin_iter - 1)));
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
    SetSize(std::distance(begin(page_id_array_), GetCorrespondValueIter(split_begin_iter)));
  }

  // root page do not need half full
  BUSTUB_ASSERT(!IsInsufficient(), "BPlusTreeInternalPage::Insert(): first half become insufficient after split");
  BUSTUB_ASSERT(result.size() >= static_cast<size_t>(GetMinSize()),
                "BPlusTreeInternalPage::Insert(): second half become insufficient after split");
  return result;
}

// return true if key in key_array_
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Contains(const KeyType &key, const KeyComparator &comparator) const -> bool {
  return std::binary_search(begin(key_array_), end(key_array_), key, ConvertToStdComparator(comparator));
}

// assume not full
// assume key is not exist
// assume iter is valid
// assume *iter = key is ordered
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertHelper(KeyIterType key_iter, const KeyType &key, const ValueType &value,
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
  std::move_backward(value_iter, end(page_id_array_), end(page_id_array_) + 1);
  *value_iter = value;
  ChangeSizeBy(1);
}

// assume not full
// assume key is not exist
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertHelper(const KeyType &key, const ValueType &value,
                                                  const KeyComparator &comparator) {
  BUSTUB_ASSERT(!IsFull(), "BPlusTreeInternalPage::InsertHelper(): try to insert to a full page");
  BUSTUB_ASSERT(!Contains(key, comparator),
                "BPlusTreeInternalPage::InsertHelper(): try to insert a key which already exist");

  auto key_iter = std::upper_bound(begin(key_array_), end(key_array_), key, ConvertToStdComparator(comparator));
  return InsertHelper(key_iter, key, value, comparator);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
