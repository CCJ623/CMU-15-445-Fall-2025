//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)
#define LEAF_PAGE_SLOT_CNT                                                                               \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - sizeof(size_t) - (LEAF_PAGE_TOMB_CNT * sizeof(size_t))) / \
   (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf pages also contain a fixed buffer of "tombstone" indexes for entries
 * that have been deleted.
 *
 * Leaf page format (keys are stored in order, tomb order is up to you):
 *  --------------------
 * | HEADER | TOMB_SIZE | (where TOMB_SIZE is num_tombstones_)
 *  --------------------
 *  -----------------------------------
 * | TOMB(0) | TOMB(1) | ... | TOMB(k) |
 *  -----------------------------------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTreeLeafPage : public BPlusTreePage {
  using KeyIterType = KeyType *;
  using ConstKeyIterType = const std::remove_pointer_t<KeyIterType> *;
  using ValueIterType = ValueType *;
  using ConstValueIterType = const std::remove_pointer_t<ValueIterType> *;

 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);
  void Init(int max_size, const std::vector<MappingType> &kvs);

  auto GetTombstones() const -> std::vector<KeyType>;

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> const KeyType &;
  auto BackKey() const -> const KeyType & { return *End(key_array_); }
  auto FrontKey() const -> const KeyType & { return *Begin(key_array_); }
  auto ValueAt(int index) const -> const ValueType &;
  auto FindLowKey(const KeyType &key, const KeyComparator &comparator) const -> size_t;
  auto GetValue(const KeyType &key, const KeyComparator &comparator) const -> std::optional<ValueType>;
  void PushFront(const KeyType &key, const ValueType &value, const KeyComparator &comparator, bool is_in_tombstones);
  void PushBack(const KeyType &key, const ValueType &value, const KeyComparator &comparator, bool is_in_tombstones);
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
      -> std::optional<std::vector<MappingType>>;
  void Remove(size_t index);
  void Remove(const KeyType &key, const KeyComparator &comparator);
  auto Contains(const KeyType &key, const KeyComparator &comparator) const -> bool;
  auto EvictMinKv() -> std::pair<MappingType, bool>;
  auto EvictMaxKv() -> std::pair<MappingType, bool>;
  void Merge(BPlusTreeLeafPage &another, const KeyComparator &comparator);
  // return true if leaf is really full
  auto IsLeafFull() const -> bool { return IsFull() && num_tombstones_ == 0; }

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(tombkey1, tombkey2, ...|key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    auto tombs = GetTombstones();
    for (size_t i = 0; i < tombs.size(); i++) {
      kstr.append(std::to_string(tombs[i].ToString()));
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    kstr.append("|");

    for (int i = 0; i < GetSize(); i++) {
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
  page_id_t next_page_id_;
  size_t num_tombstones_;
  // Fixed-size tombstone buffer (indexes into key_array_ / rid_array_).
  // ordered by indexes
  size_t tombstones_[LEAF_PAGE_TOMB_CNT];
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed

  void InsertHelper(KeyIterType key_iter, const KeyType &key, const ValueType &value, const KeyComparator &comparator);
  void InsertHelper(const KeyType &key, const ValueType &value, const KeyComparator &comparator);
  auto EvictTombstones() -> size_t;
  auto IsTombstonesFull() const -> bool { return num_tombstones_ == LEAF_PAGE_TOMB_CNT; }
  auto IsTombstonesEmpty() const -> bool { return num_tombstones_ == 0; }
  auto TombstonesContains(size_t index) const -> bool {
    return std::binary_search(TombstonesBegin(), TombstonesEnd(), index);
  }
  void TombstonesInsert(size_t index) {
    BUSTUB_ASSERT(!IsTombstonesFull(), "BPlusTreeLeafPage::TombstonesInsert(): tombstones is full");
    auto insert_iter = std::upper_bound(TombstonesBegin(), TombstonesEnd(), index);
    std::move_backward(insert_iter, TombstonesEnd(), TombstonesEnd() + 1);
    *insert_iter = index;
    ++num_tombstones_;
  }
  void TombstonesPushFront(size_t index) {
    BUSTUB_ASSERT(!IsTombstonesFull(), "BPlusTreeLeafPage::TombstonesPushFront(): tombstones is full");
    BUSTUB_ASSERT(std::upper_bound(TombstonesBegin(), TombstonesEnd(), index) == TombstonesBegin(),
                  "BPlusTreeLeafPage::TombstonesPushFront(): index in unordered");

    std::move_backward(TombstonesBegin(), TombstonesEnd(), TombstonesEnd() + 1);
    tombstones_[0] = index;
    ++num_tombstones_;
  }
  void TombstonesPushBack(size_t index) {
    BUSTUB_ASSERT(!IsTombstonesFull(), "BPlusTreeLeafPage::TombstonesPushBack(): tombstones is full");
    BUSTUB_ASSERT(std::upper_bound(TombstonesBegin(), TombstonesEnd(), index) == TombstonesEnd(),
                  "BPlusTreeLeafPage::TombstonesPushBack(): index in unordered");
    tombstones_[num_tombstones_] = index;
    ++num_tombstones_;
  }
  void TombstonesPopFront() {
    BUSTUB_ASSERT(!IsTombstonesEmpty(), "BPlusTreeLeafPage::TombstonesPopFront(): tombstones is empty");

    std::copy(TombstonesBegin() + 1, TombstonesEnd(), TombstonesBegin());
    --num_tombstones_;
  }
  void TombstonesPopBack() {
    BUSTUB_ASSERT(!IsTombstonesEmpty(), "BPlusTreeLeafPage::TombstonesPushBack(): tombstones is empty");
    --num_tombstones_;
  }

  auto ConvertToStdComparator(const KeyComparator &comparator) const {
    return [&comparator](const KeyType &lhs, const KeyType &rhs) { return comparator(lhs, rhs) < 0; };
  }

  template <typename T, std::size_t N>
  auto Begin(T (&array)[N]) const -> T * {
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

  auto TombstonesBegin() -> size_t * { return tombstones_; }
  auto TombstonesBegin() const -> const size_t * { return tombstones_; }
  auto TombstonesEnd() -> size_t * { return tombstones_ + num_tombstones_; }
  auto TombstonesEnd() const -> const size_t * { return tombstones_ + num_tombstones_; }

  auto GetCorrespondValueIter(ConstKeyIterType key_iter) const -> ConstValueIterType {
    return Cbegin(rid_array_) + std::distance(Cbegin(key_array_), (key_iter));
  }

  auto GetCorrespondValueIter(KeyIterType key_iter) -> ValueIterType {
    return Begin(rid_array_) + std::distance(Begin(key_array_), (key_iter));
  }
};

}  // namespace bustub
