//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/generic_key.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(TracedBufferPoolManager *bpm, page_id_t page_id, size_t offset,
                                  KeyComparator comparator)
    : bpm_(bpm), page_id_(page_id), offset_(offset), comparator_(comparator) {
  if (page_id_ != INVALID_PAGE_ID) {
    guard_ = bpm_->ReadPage(page_id_);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto &page = *(guard_.As<LeafPage>());
  return {page.KeyAt(offset_), page.ValueAt(offset_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    // end
    return *this;
  }

  const KeyType &key = (guard_.As<LeafPage>())->KeyAt(offset_);
  auto tombstones = (guard_.As<LeafPage>())->GetTombstones();
  auto tombstones_iter =
      std::upper_bound(tombstones.cbegin(), tombstones.cend(), key, ConvertToStdComparator<KeyType>(comparator_));
  ++offset_;
  while (true) {
    // find valid key
    auto &page = *(guard_.As<LeafPage>());

    // make sure offset is right
    BUSTUB_ASSERT(tombstones_iter == tombstones.cend() || comparator_(page.KeyAt(offset_), *tombstones_iter) <= 0,
                  "IndexIterator::operator++(): offset wrong");

    if (offset_ == static_cast<size_t>(page.GetSize())) {
      // jump to next page
      offset_ = 0;
      page_id_ = page.GetNextPageId();
      if (page_id_ == INVALID_PAGE_ID) {
        guard_ = ReadPageGuard{};
        return *this;
      } else {
        guard_ = bpm_->ReadPage(page_id_);
        tombstones = (guard_.As<LeafPage>())->GetTombstones();
        tombstones_iter = tombstones.cbegin();
      }
      continue;
    }

    // same page
    const auto &key = page.KeyAt(offset_);
    if (tombstones_iter == tombstones.cend() || comparator_(key, *tombstones_iter) != 0) {
      // impossible to be in tombstones
      return *this;
    }
    ++offset_;
    ++tombstones_iter;
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
