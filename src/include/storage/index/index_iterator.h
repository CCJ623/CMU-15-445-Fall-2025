//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(TracedBufferPoolManager *bpm, page_id_t page_id, size_t offset, KeyComparator comparator);
  IndexIterator(IndexIterator &&another) = default;
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return bpm_ == itr.bpm_ && page_id_ == itr.page_id_ && offset_ == itr.offset_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  TracedBufferPoolManager *bpm_;
  page_id_t page_id_;
  ReadPageGuard guard_;
  size_t offset_;
  KeyComparator comparator_;
};

}  // namespace bustub
