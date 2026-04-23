//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.h
//
// Identification: src/include/storage/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sys/types.h>
#include <algorithm>
#include <iterator>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value_factory.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key in key_array_ always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  using KeyIterType = KeyType *;
  using ConstKeyIterType = const std::remove_pointer_t<KeyIterType> *;
  using ValueIterType = ValueType *;
  using ConstValueIterType = const std::remove_pointer_t<ValueIterType> *;

  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);
  void Init(int max_size, const std::vector<MappingType> &kvs);

  auto KeyAt(int index) const -> KeyType;
  auto BackKey() const -> const KeyType & { return *End(key_array_); }
  auto FrontKey() const -> const KeyType & { return *Begin(key_array_); }

  void SetKeyAt(int index, const KeyType &key, const KeyComparator &comparator);
  void SetValueAt(int index, const ValueType &value);

  /**
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  auto ValueIndex(const ValueType &value) const -> int;

  auto ValueAt(int index) const -> ValueType;
  auto GetValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType;
  auto GetValueIndex(const KeyType &key, const KeyComparator &comparator) const -> size_t;
  void PushFront(const KeyType &key, const ValueType &value, const KeyComparator &comparator);
  void PushBack(const KeyType &key, const ValueType &value, const KeyComparator &comparator);
  auto GetSplitPos(const KeyType &key, const KeyComparator &comparator) const -> uint;
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
      -> std::optional<std::vector<MappingType>>;
  void Remove(size_t index);
  void Remove(const KeyType &key, const KeyComparator &comparator);
  auto Contains(const KeyType &key, const KeyComparator &comparator) const -> bool;
  auto EvictMinKv() -> MappingType;
  auto EvictMaxKv() -> MappingType;
  void Merge(BPlusTreeInternalPage &another, const KeyType &mid_key, const KeyComparator &comparator);
  auto FindKey(const KeyType &key, const KeyComparator &comparator) const -> std::optional<size_t> {
    auto key_iter = std::lower_bound(Begin(key_array_), End(key_array_), key, comparator);
    if (comparator(key, *key_iter) != 0) {
      return std::nullopt;
    }
    return std::distance(Begin(key_array_), key_iter) + 1;
  }
  auto GetSiblings(const KeyType &key, const KeyComparator &comparator) const -> std::pair<page_id_t, page_id_t>;
  auto GetSiblings(size_t index) const -> std::pair<page_id_t, page_id_t>;

  auto IsEmpty() const -> bool { return GetSize() <= 1; }

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // Array members for page data.
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed

  void InsertHelper(KeyIterType key_iter, const KeyType &key, const ValueType &value, const KeyComparator &comparator);
  void InsertHelper(const KeyType &key, const ValueType &value, const KeyComparator &comparator);

  auto ConvertToStdComparator(const KeyComparator &comparator) const {
    return [&comparator](const KeyType &lhs, const KeyType &rhs) { return comparator(lhs, rhs) < 0; };
  }

  template <typename T, std::size_t N>
  auto Begin(T (&array)[N]) const -> T * {
    if constexpr (std::is_same_v<std::remove_const_t<T>, KeyType>) {
      // KeyType
      return std::begin(array) + 1;
    }
    // ValueType
    return std::begin(array);
  }

  template <typename T, std::size_t N>
  auto Cbegin(T (&array)[N]) const -> const T * {
    return Begin(array);
  }

  template <typename T, std::size_t N>
  auto End(T (&array)[N]) const -> T * {
    return std::begin(array) + GetSize();
  }

  template <typename T, std::size_t N>
  auto Cend(T (&array)[N]) const -> const T * {
    return End(array);
  }

  auto GetCorrespondValueIter(ConstKeyIterType key_iter) const -> ConstValueIterType {
    return Cbegin(page_id_array_) + std::distance(Cbegin(key_array_), (key_iter)) + 1;
  }

  auto GetCorrespondValueIter(KeyIterType key_iter) -> ValueIterType {
    return Begin(page_id_array_) + std::distance(Begin(key_array_), (key_iter)) + 1;
  }
};

}  // namespace bustub
