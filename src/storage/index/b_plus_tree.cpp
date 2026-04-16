//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <algorithm>
#include <optional>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header = guard.As<BPlusTreeHeaderPage>();
  return header->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 *
 * my solution:
 * get root page
 * crab grab until leaf node
 * fetch value from leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context context;
  // read root page
  auto header_guard = bpm_->ReadPage(header_page_id_);
  context.root_page_id_ = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (context.root_page_id_ == INVALID_PAGE_ID) {
    // no root page
    return false;
  }
  context.read_set_.push_back(bpm_->ReadPage(context.root_page_id_));
  // got root page guard, drop header page guard
  header_guard.Drop();

  auto internal_page = context.read_set_.back().As<InternalPage>();
  // walk to leaf page
  // crab grab lock
  while (!internal_page->IsLeafPage()) {
    context.read_set_.push_back(bpm_->ReadPage(internal_page->GetValue(key, comparator_)));
    internal_page = context.read_set_.back().As<InternalPage>();
    context.read_set_.pop_front();
  }

  auto leaf_page = context.read_set_.back().As<LeafPage>();
  auto value_opt = leaf_page->GetValue(key, comparator_);
  if (value_opt == std::nullopt) {
    return false;
  }
  result->push_back(*value_opt);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 *
 * my solution:
 * use optimistic way: first acquire read latches all way down
 * if split, acquire write latches all way down
 * find correct leaf node (acquire latches all way down)
 * insert kv in order
 * split if exceed limit size
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context context;

  // optimistic try to insert, return false if failed
  auto optimistic_try = [this, &context, &key, &value]() -> bool {
    // read root page
    auto read_header_page_guard = bpm_->ReadPage(header_page_id_);
    ReadPageGuard internal_page_guard;
    WritePageGuard leaf_page_guard;
    context.root_page_id_ = read_header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
    if (context.root_page_id_ == INVALID_PAGE_ID) {
      // no root page, create a new one
      read_header_page_guard.Drop();
      // header page could be modified here, between two latches
      context.header_page_ = bpm_->WritePage(header_page_id_);
      auto &header_page = *(context.header_page_->AsMut<BPlusTreeHeaderPage>());
      if (header_page.root_page_id_ == INVALID_PAGE_ID) {
        // header page is not modified
        auto new_page_id = bpm_->NewPage();
        auto new_page_guard = bpm_->WritePage(new_page_id);
        auto &new_page = *(new_page_guard.AsMut<LeafPage>());
        new_page.Init(leaf_max_size_);
        context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_page_id;
        context.root_page_id_ = new_page_id;
      } else {
        // header page is modified
        context.root_page_id_ = header_page.root_page_id_;
      }

      // because we drop read page guard of header page in this section
      // we need to acquire root page latch here
      leaf_page_guard = bpm_->WritePage(context.root_page_id_);
    } else if (auto root_page_guard = bpm_->ReadPage(context.root_page_id_);
               root_page_guard.As<InternalPage>()->IsLeafPage()) {
      // root is leaf
      root_page_guard.Drop();
      leaf_page_guard = bpm_->WritePage(context.root_page_id_);
    } else {
      internal_page_guard = bpm_->ReadPage(context.root_page_id_);
      // root page is not leaf, walk down
      // walk to leaf page
      // crab grab lock
      auto internal_page = internal_page_guard.As<InternalPage>();
      while (true) {
        auto next_page_guard = bpm_->ReadPage(internal_page->GetValue(key, comparator_));
        if (next_page_guard.template As<InternalPage>()->IsLeafPage()) {
          // get write page guard of leaf page and quit
          auto leaf_page_id = next_page_guard.GetPageId();
          next_page_guard.Drop();
          leaf_page_guard = bpm_->WritePage(leaf_page_id);
          break;
        }
        internal_page = next_page_guard.template As<InternalPage>();
      }
    }

    // leaf_page_guard is ready here

    if (auto &leaf_page = *(leaf_page_guard.AsMut<LeafPage>()); leaf_page.Contains(key, comparator_)) {
      // key already exist
      return false;
    }
    if (auto &leaf_page = *(leaf_page_guard.AsMut<LeafPage>()); !leaf_page.IsLeafFull()) {
      // leaf not full, no need to split
      leaf_page.Insert(key, value, comparator_);
      return true;
    }

    // need to split
    // reacquire write latches from top
    return false;
  };

  if (optimistic_try()) {
    // success
    return true;
  }

  // optimistic failed
  // pessimistic way
  context.header_page_ = bpm_->WritePage(header_page_id_);
  context.root_page_id_ = context.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;
  context.write_set_.push_back(bpm_->WritePage(context.root_page_id_));

  // acquire write latches all way down to leaf
  for (auto internal_page = context.write_set_.back().AsMut<InternalPage>(); !internal_page->IsLeafPage();
       internal_page = context.write_set_.back().AsMut<InternalPage>()) {
    context.write_set_.push_back(bpm_->WritePage(internal_page->GetValue(key, comparator_)));
  }

  auto leaf_page_id = context.write_set_.back().GetPageId();
  if (auto &leaf_page = *(context.write_set_.back().AsMut<LeafPage>()); leaf_page.Contains(key, comparator_)) {
    // key already exist
    return false;
  }

  // key not exist, can insert
  auto &leaf_page = *(context.write_set_.back().AsMut<LeafPage>());
  auto leaf_second_half = leaf_page.Insert(key, value, comparator_);
  if (leaf_second_half == std::nullopt) {
    // no split
    return true;
  }

  // split happened
  // need to insert all way up
  // new page for second half
  auto leaf_new_page_id = bpm_->NewPage();
  // write second half to new page
  auto leaf_new_page_guard = bpm_->WritePage(leaf_new_page_id);
  auto &leaf_new_page = *(leaf_new_page_guard.AsMut<LeafPage>());
  leaf_new_page.Init(leaf_max_size_, *leaf_second_half);
  // set next page id
  leaf_new_page.SetNextPageId(leaf_page.GetNextPageId());
  leaf_page.SetNextPageId(leaf_new_page_id);

  // return new root page guard
  auto create_new_root_page = [this, &context](const KeyType &key, page_id_t left, page_id_t right) {
    auto new_page_id = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page_id);
    auto &new_page = *(new_page_guard.AsMut<InternalPage>());
    new_page.Init(internal_max_size_);
    new_page.SetSize(2);
    new_page.SetKeyAt(1, key);
    new_page.SetValueAt(0, left);
    new_page.SetValueAt(1, right);
    context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_page_id;
    context.root_page_id_ = new_page_id;
    return std::move(new_page_guard);
  };

  if (context.write_set_.back().GetPageId() == context.root_page_id_) {
    // leaf is root, create a new root and return
    create_new_root_page(leaf_second_half->front().first, leaf_page_id, leaf_new_page_id);
    return true;
  }

  // leaf is not root, need to insert all way up
  // rend() - 1 is root page guard
  // stop at root page
  KeyType push_up_key = leaf_second_half->front().first;
  page_id_t push_up_value = leaf_new_page_id;
  for (auto upstream_iter = context.write_set_.rbegin() + 1, root_iter = context.write_set_.rend() - 1;
       upstream_iter != root_iter; ++upstream_iter) {
    auto &upstream_page = *((context.write_set_.rbegin() + 1)->AsMut<InternalPage>());
    auto upstream_second_half = upstream_page.Insert(push_up_key, push_up_value, comparator_);

    if (upstream_second_half == std::nullopt) {
      // upstream not split, return
      return true;
    }
    // upstream split
    // new page for second half
    auto new_page_id = bpm_->NewPage();
    // write second half to new page
    auto new_page_guard = bpm_->WritePage(new_page_id);
    auto &new_page = *(new_page_guard.AsMut<InternalPage>());
    new_page.Init(internal_max_size_, *upstream_second_half);
    // setup push up key/value for next loop
    // key[0] in internal page is invalid, so we need to grab it from upstream_second_half
    push_up_key = upstream_second_half->front().first;
    push_up_value = new_page_id;
  }

  // reach root page
  auto &root_page = *(context.write_set_.front().AsMut<InternalPage>());
  auto root_second_half = root_page.Insert(push_up_key, push_up_value, comparator_);

  if (root_second_half == std::nullopt) {
    // root not split, return
    return true;
  }

  // root split, create new root and return
  // new page for second half
  auto new_root_page_id = bpm_->NewPage();
  // write second half to new page
  auto new_root_page_guard = bpm_->WritePage(new_root_page_id);
  auto &new_root_page = *(new_root_page_guard.AsMut<InternalPage>());
  new_root_page.Init(internal_max_size_, *root_second_half);
  // setup push up key/value for next loop
  // key[0] in internal page is invalid, so we need to grab it from upstream_second_half
  push_up_key = root_second_half->front().first;
  push_up_value = new_root_page_id;
  create_new_root_page(push_up_key, context.root_page_id_, push_up_value);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return bpm_->ReadPage(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
