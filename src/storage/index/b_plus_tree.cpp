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
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stack>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "fmt/base.h"
#include "fmt/format.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

namespace b_plus_tree {
std::mutex Debuger::latch;
}  // namespace b_plus_tree

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
    context.read_set_.back() = bpm_->ReadPage(internal_page->GetValue(key, comparator_));
    internal_page = context.read_set_.back().As<InternalPage>();
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

  // optimistic try to insert
  enum class OptimisticTryResult { kSuccess, kFailed, kKeyExist };
  auto optimistic_try = [this, &key, &value]() -> OptimisticTryResult {
    Context context;
    // read root page
    auto read_header_page_guard = bpm_->ReadPage(header_page_id_);
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
      context.header_page_ = std::nullopt;
    } else if (auto root_page_guard = bpm_->ReadPage(context.root_page_id_);
               root_page_guard.As<InternalPage>()->IsLeafPage()) {
      // root is leaf
      root_page_guard.Drop();
      leaf_page_guard = bpm_->WritePage(context.root_page_id_);
      read_header_page_guard.Drop();
    } else {
      // we must maintain two page guard, because we need to get write page guard of leaf page
      auto internal_page_guard = bpm_->ReadPage(context.root_page_id_);
      read_header_page_guard.Drop();
      // root page is not leaf, walk down
      // walk to leaf page
      // crab grab lock
      while (true) {
        auto &internal_page = *(internal_page_guard.As<InternalPage>());
        auto next_page_id = internal_page.GetValue(key, comparator_);
        auto next_page_guard = bpm_->ReadPage(next_page_id);
        if (next_page_guard.template As<InternalPage>()->IsLeafPage()) {
          // get write page guard of leaf page and quit
          auto leaf_page_id = next_page_guard.GetPageId();
          next_page_guard.Drop();
          // leaf page can be modified here
          leaf_page_guard = bpm_->WritePage(leaf_page_id);
          break;
        }
        internal_page_guard = std::move(next_page_guard);
      }
    }

    // leaf_page_guard is ready here

    if (auto &leaf_page = *(leaf_page_guard.As<LeafPage>()); leaf_page.Contains(key, comparator_)) {
      // key already exist
      return OptimisticTryResult::kKeyExist;
    }
    if (auto &leaf_page = *(leaf_page_guard.As<LeafPage>()); !leaf_page.IsLeafFull()) {
      // leaf not full, no need to split
      leaf_page_guard.AsMut<LeafPage>()->Insert(key, value, comparator_);
      return OptimisticTryResult::kSuccess;
    }

    // need to split
    // reacquire write latches from top
    return OptimisticTryResult::kFailed;
  };

  auto optimistic_try_result = optimistic_try();
  if (optimistic_try_result == OptimisticTryResult::kSuccess) {
    // success
    return true;
  }
  if (optimistic_try_result == OptimisticTryResult::kKeyExist) {
    // key exist
    return false;
  }

  // optimistic failed
  // pessimistic way
  Context context;
  // contxt.write_set_ is a set of page need to be modified
  {
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    context.root_page_id_ = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
    if (context.root_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
    // get root page guard, then release head page guard
    auto root_page_guard = bpm_->WritePage(context.root_page_id_);
    if (root_page_guard.As<InternalPage>()->IsFull()) {
      // root is full, it may be modified, so we need to hold latch of header page
      context.header_page_ = std::move(header_page_guard);
    }
    context.write_set_.push_back(std::move(root_page_guard));
  }

  // acquire write latches all way down to leaf
  for (auto internal_page = context.write_set_.back().As<InternalPage>(); !internal_page->IsLeafPage();
       internal_page = context.write_set_.back().As<InternalPage>()) {
    auto next_page_guard = bpm_->WritePage(internal_page->GetValue(key, comparator_));
    if (!next_page_guard.template As<InternalPage>()->IsFull()) {
      // next page is not full, do not need to insert to upstream
      // we can release all write latches of upstream
      context.write_set_.clear();
      context.header_page_ = std::nullopt;
    }
    context.write_set_.push_back(std::move(next_page_guard));
  }

  // context.write_set_ all need to be modified
  BUSTUB_ASSERT(std::all_of(context.write_set_.cbegin() + 1, context.write_set_.cend(),
                            [](const auto &guard) { return guard.template As<InternalPage>()->IsFull(); }),
                "BPlusTree::Insert(): invalid context.write_set_");

  auto leaf_page_id = context.write_set_.back().GetPageId();
  if (auto &leaf_page = *(context.write_set_.back().As<LeafPage>()); leaf_page.Contains(key, comparator_)) {
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

  // return new root page guard
  auto create_new_root_page = [this, &context](const KeyType &key, page_id_t left, page_id_t right) {
    auto new_page_id = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page_id);
    auto &new_page = *(new_page_guard.AsMut<InternalPage>());
    new_page.Init(internal_max_size_);
    new_page.SetSize(1);
    new_page.SetValueAt(0, left);
    new_page.PushBack(key, right, comparator_);
    BUSTUB_ASSERT(context.header_page_ != std::nullopt,
                  "BPlusTree::Insert()::create_new_root_page(): header page guard is not held");
    context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_page_id;
    context.root_page_id_ = new_page_id;
    return std::move(new_page_guard);
  };

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

  if (leaf_page_id == context.root_page_id_) {
    // leaf is root, create a new root and return
    create_new_root_page(leaf_second_half->front().first, leaf_page_id, leaf_new_page_id);
    return true;
  }

  // release leaf page
  context.write_set_.pop_back();

  // push up KV to upstream page
  // return nullopt if upstream page not split
  // return upstream's push up KV if upstream page split
  auto insert_to_upstream_page = [this](InternalPage &upstream_page, const KeyType &key,
                                        const page_id_t &value) -> std::optional<std::pair<KeyType, page_id_t>> {
    auto second_half = upstream_page.Insert(key, value, comparator_);

    if (second_half == std::nullopt) {
      // upstream not split, return
      return std::nullopt;
    }
    // upstream split
    // new page for second half
    auto new_page_id = bpm_->NewPage();
    // write second half to new page
    auto new_page_guard = bpm_->WritePage(new_page_id);
    auto &new_page = *(new_page_guard.AsMut<InternalPage>());
    new_page.Init(internal_max_size_, *second_half);
    // setup push up key/value for next loop
    // key[0] in internal page is invalid, so we need to grab it from upstream_second_half
    return std::make_pair(std::move(second_half->front().first), new_page_id);
  };

  // leaf is not root, need to insert all way up
  // rend() - 1 is last page guard
  // stop at last page
  KeyType push_up_key = std::move(leaf_second_half->front().first);
  page_id_t push_up_value = leaf_new_page_id;
  for (auto upstream_iter = context.write_set_.rbegin(); upstream_iter != context.write_set_.rend() - 1;
       upstream_iter = context.write_set_.rbegin()) {
    auto &upstream_page = *(upstream_iter->AsMut<InternalPage>());
    auto next_kv = insert_to_upstream_page(upstream_page, push_up_key, push_up_value);
    BUSTUB_ASSERT(next_kv != std::nullopt, "BPlusTree::Insert(): upstream must split");
    // push up success, release current page
    context.write_set_.pop_back();
    // if (next_kv == std::nullopt) {
    //   // upstream not split
    //   return true;
    // }
    // upstream split
    // setup push up key/value for next loop
    // key[0] in internal page is invalid, so we need to grab it from upstream_second_half
    push_up_key = std::move(next_kv->first);
    push_up_value = next_kv->second;
  }

  // reach last page
  BUSTUB_ASSERT(context.write_set_.size() == 1, "BPlusTree::Insert(): context write set has not only root page");
  auto &last_page = *(context.write_set_.back().AsMut<InternalPage>());
  auto last_page_id = context.write_set_.back().GetPageId();
  auto next_kv = insert_to_upstream_page(last_page, push_up_key, push_up_value);
  context.write_set_.pop_back();

  if (last_page_id == context.root_page_id_) {
    // last page is root page
    if (next_kv == std::nullopt) {
      // root not split
      return true;
    }

    // root split
    create_new_root_page(next_kv->first, context.root_page_id_, next_kv->second);
    return true;
  }

  // last page is not root page
  // impossible to split
  BUSTUB_ASSERT(next_kv == std::nullopt, "BPlusTree::Insert(): last page is impossible to split");
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
 *
 * my solution:
 * if leaf is half full after deletion, delete and return
 * else if sibling is half full after deletion, borrow from him
 * else merge sibling (must have same upstream), merge all way up if necessary
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  b_plus_tree::Debuger debuger{
      "Remove", [this, &key]() { return fmt::format("key[{}]\n{}\n", key.ToString(), DrawBPlusTree()); }};

  if (IsEmpty()) {
    debuger.Print("Remove(): is empty\n");
    return;
  }

  // optimistic try to remove, return false if failed
  auto optimistic_try = [this, &key, &debuger]() -> bool {
    debuger.Print("Remove(): optimistic try\n");

    Context context;
    // read root page
    auto read_header_page_guard = bpm_->ReadPage(header_page_id_);
    WritePageGuard leaf_page_guard;
    context.root_page_id_ = read_header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
    if (context.root_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
    if (auto root_page_guard = bpm_->ReadPage(context.root_page_id_);
        root_page_guard.As<InternalPage>()->IsLeafPage()) {
      // root is leaf
      root_page_guard.Drop();
      leaf_page_guard = bpm_->WritePage(context.root_page_id_);
      read_header_page_guard.Drop();
    } else {
      // we must maintain two page guard, because we need to get write page guard of leaf page
      auto internal_page_guard = bpm_->ReadPage(context.root_page_id_);
      read_header_page_guard.Drop();
      // root page is not leaf, walk down
      // walk to leaf page
      // crab grab lock
      while (true) {
        auto &internal_page = *(internal_page_guard.As<InternalPage>());
        auto next_page_id = internal_page.GetValue(key, comparator_);
        auto next_page_guard = bpm_->ReadPage(next_page_id);
        if (next_page_guard.template As<InternalPage>()->IsLeafPage()) {
          // get write page guard of leaf page and quit
          auto leaf_page_id = next_page_guard.GetPageId();
          next_page_guard.Drop();
          // leaf can be changed here
          leaf_page_guard = bpm_->WritePage(leaf_page_id);
          break;
        }
        internal_page_guard = std::move(next_page_guard);
      }
    }

    // leaf_page_guard is ready here
    auto &leaf_page = *(leaf_page_guard.As<LeafPage>());
    if (!leaf_page.Contains(key, comparator_)) {
      // key not exist
      debuger.Print(fmt::format("Remove()::optimistic_try(): key[{}] not exist\n", key.ToString()));

      return true;
    }
    // key exist
    if (leaf_page_guard.GetPageId() == context.root_page_id_) {
      // leaf is root, remove and return
      leaf_page_guard.AsMut<LeafPage>()->Remove(key, comparator_);
      if (!leaf_page.IsEmpty()) {
        debuger.Print(fmt::format("Remove()::optimistic_try(): leaf is root and not empty, remove and return\n"));

        return true;
      }
      // root is empty
      // delete empty root page
      auto leaf_page_id = leaf_page_guard.GetPageId();
      leaf_page_guard.Drop();
      // somebody can change the tree here, between two latches
      context.header_page_ = bpm_->WritePage(header_page_id_);
      if (context.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_ == leaf_page_id &&
          bpm_->ReadPage(leaf_page_id).As<LeafPage>()->IsEmpty()) {
        // tree is not changed
        debuger.Print(fmt::format("Remove(): root empty, delete root[{}]\n", leaf_page_id));

        bpm_->DeletePage(leaf_page_id);
        context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
      }
      return true;
    }
    // leaf is not root
    if (leaf_page.IsHealthy()) {
      // leaf is healthy, no need to merge
      leaf_page_guard.AsMut<LeafPage>()->Remove(key, comparator_);
      return true;
    }

    // leaf is not root and unhealthy, need to merge
    // reacquire write latches from top
    return false;
  };

  if (optimistic_try()) {
    // success
    debuger.Print("Remove(): optimistic try success\n");

    return;
  }

  debuger.Print("Remove(): optimistic try failed\n");

  // optimistic failed
  // leaf is not root
  // pessimistic way
  debuger.Print("Remove(): pessimistic try\n");

  Context context;
  {
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    context.root_page_id_ = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
    if (context.root_page_id_ == INVALID_PAGE_ID) {
      return;
    }
    // get root page guard, then release head page guard
    auto root_page_guard = bpm_->WritePage(context.root_page_id_);
    if (auto &root_page = *root_page_guard.As<InternalPage>();
        (!root_page.IsLeafPage() && root_page.GetSize() == 2) || (root_page.IsLeafPage() && root_page.GetSize() == 1)) {
      // root may be modified, so we need to hold latch of header page
      context.header_page_ = std::move(header_page_guard);
    }
    context.write_set_.push_back(std::move(root_page_guard));
  }

  // stores value index all way down to leaf
  std::stack<size_t> value_index_stack;
  // acquire write latches all way down to leaf
  for (auto internal_page = context.write_set_.back().As<InternalPage>(); !internal_page->IsLeafPage();
       internal_page = context.write_set_.back().As<InternalPage>()) {
    auto index = internal_page->GetValueIndex(key, comparator_);
    auto next_page_guard = bpm_->WritePage(internal_page->ValueAt(index));
    if (next_page_guard.template As<InternalPage>()->IsHealthy()) {
      // next page is healthy, do not need to modify upstreams
      // we can release all write latches of upstream
      context.write_set_.clear();
      context.header_page_ = std::nullopt;
      value_index_stack = {};
    }
    value_index_stack.push(index);
    context.write_set_.push_back(std::move(next_page_guard));
  }

  // context.write_set_ all need to be modified
  BUSTUB_ASSERT(std::all_of(context.write_set_.cbegin() + 1, context.write_set_.cend(),
                            [](const auto &guard) { return guard.template As<InternalPage>()->IsCritical(); }),
                "BPlusTree::Remove(): invalid context.write_set_");

  auto leaf_page_id = context.write_set_.back().GetPageId();
  if (auto &leaf_page = *(context.write_set_.back().As<LeafPage>()); !leaf_page.Contains(key, comparator_)) {
    // key not exist

    debuger.Print(fmt::format("Remove(): key[{}] not exist\n", key.ToString()));

    return;
  }

  // key exist, can remove
  auto &leaf_page = *(context.write_set_.back().AsMut<LeafPage>());
  leaf_page.Remove(key, comparator_);

  if (leaf_page_id == context.root_page_id_) {
    if (!leaf_page.IsEmpty()) {
      return;
    }
    // delete empty root page
    debuger.Print(fmt::format("Remove(): root empty, delete root[{}]\n", leaf_page_id));
    BUSTUB_ASSERT(context.header_page_ != std::nullopt, "BPlusTree::Insert()::: header page guard is not held");

    context.write_set_.pop_back();
    bpm_->DeletePage(leaf_page_id);
    context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    return;
  }
  if (!leaf_page.IsInsufficient()) {
    // no need to merge
    return;
  }

  // non-root page and need merge
  // first try steal from siblings
  // if can't steal, then merge sibling and adjust upstream page
  // if merge happend, this function will remove a pair of KV in upstream page
  // user should check whether upstream page state after calling this function
  auto steal_or_merge = [this, &context, &debuger](auto &current_page, auto &upstream_page, size_t index) {
    using CurrentPageType = std::decay_t<decltype(current_page)>;

    auto [left_page_id, right_page_id] = upstream_page.GetSiblings(index);

    // find best matched page to steal
    WritePageGuard target_page_guard;
    auto current_page_id = context.write_set_.back().GetPageId();
    bool is_target_left;
    if (left_page_id == INVALID_PAGE_ID) {
      // no left, steal from right
      target_page_guard = bpm_->WritePage(right_page_id);
      is_target_left = false;
    } else if (right_page_id == INVALID_PAGE_ID) {
      // no right, steal from left
      target_page_guard = bpm_->WritePage(left_page_id);
      is_target_left = true;
    } else {
      // both left and right exist, steal from bigger one, select right if equal
      WritePageGuard left_page_guard = bpm_->WritePage(left_page_id);
      WritePageGuard right_page_guard = bpm_->WritePage(right_page_id);
      auto &left_page = *(left_page_guard.AsMut<CurrentPageType>());
      auto &right_page = *(right_page_guard.AsMut<CurrentPageType>());
      if (right_page.GetSize() >= left_page.GetSize()) {
        target_page_guard = std::move(right_page_guard);
        is_target_left = false;
      } else {
        target_page_guard = std::move(left_page_guard);
        is_target_left = true;
      }
    }

    auto &target_page = *(target_page_guard.AsMut<CurrentPageType>());
    if constexpr (std::is_same_v<CurrentPageType, LeafPage>) {
      // leaf page
      // may steal a tomb key, must keep stealling until being to critical
      // if can not be critical, then go down to merge
      while (target_page.IsHealthy()) {
        // we need to push up key to upstream
        size_t push_up_index;
        KeyType push_up_key;
        if (is_target_left) {
          // target at left
          push_up_index = index;
          auto [kv, is_in_tombstones] = target_page.EvictMaxKv();
          current_page.PushFront(kv.first, kv.second, comparator_, is_in_tombstones);
          push_up_key = current_page.FrontKey();

          debuger.Print(fmt::format("Remove(): this[{}] steal from left[{}], push up key[{}], at index[{}]\n",
                                    current_page_id, target_page_guard.GetPageId(), push_up_key.ToString(),
                                    push_up_index));
        } else {
          // target at right
          push_up_index = index + 1;
          // is leaf
          auto [kv, is_in_tombstones] = target_page.EvictMinKv();
          current_page.PushBack(kv.first, kv.second, comparator_, is_in_tombstones);
          push_up_key = target_page.FrontKey();

          debuger.Print(fmt::format("Remove(): this[{}] steal from right[{}], push up key[{}], at index[{}]\n",
                                    current_page_id, target_page_guard.GetPageId(), push_up_key.ToString(),
                                    push_up_index));
        }
        if (current_page.IsInsufficient()) {
          // still insufficient, keep going
          continue;
        }
        // success
        // push up key and return
        upstream_page.SetKeyAt(push_up_index, push_up_key, comparator_);
        return;
      }
    } else {
      // internal page
      if (target_page.IsHealthy()) {
        // healthy, no need to merge, steal and return
        // we need to push up key to upstream
        size_t push_up_index;
        KeyType push_up_key;
        if (is_target_left) {
          // target at left
          push_up_index = index;
          auto kv = target_page.EvictMaxKv();
          // exchange key with upstream
          current_page.PushFront(upstream_page.KeyAt(push_up_index), kv.second, comparator_);
          push_up_key = kv.first;

          debuger.Print(fmt::format("Remove(): this[{}] steal from left[{}], push up key[{}], at index[{}]\n",
                                    current_page_id, target_page_guard.GetPageId(), push_up_key.ToString(),
                                    push_up_index));

        } else {
          // target at right
          push_up_index = index + 1;
          auto kv = target_page.EvictMinKv();
          // exchange key with upstream
          current_page.PushBack(upstream_page.KeyAt(push_up_index), kv.second, comparator_);
          push_up_key = kv.first;

          debuger.Print(fmt::format("Remove(): this[{}] steal from right[{}], push up key[{}], at index[{}]\n",
                                    current_page_id, target_page_guard.GetPageId(), push_up_key.ToString(),
                                    push_up_index));
        }
        // exchange key with upstream
        upstream_page.SetKeyAt(push_up_index, push_up_key, comparator_);
        return;
      }
    }

    // not healthy, need to merge
    page_id_t delete_page_id;
    if (is_target_left) {
      // target page is at left side
      debuger.Print(fmt::format("Remove(): left[{}] merge this[{}]\n", target_page_guard.GetPageId(), current_page_id));

      if constexpr (std::is_same_v<CurrentPageType, LeafPage>) {
        // leaf page, merge and remove key in upstream
        target_page.Merge(current_page, comparator_);
      } else {
        // internal page, merge and insert key pull from upstream
        const auto &mid_key = upstream_page.KeyAt(index);
        target_page.Merge(current_page, mid_key, comparator_);
      }
      upstream_page.Remove(index);
      delete_page_id = context.write_set_.back().GetPageId();
      context.write_set_.back() = std::move(target_page_guard);
    } else {
      // target page is at right side
      debuger.Print(
          fmt::format("Remove(): this[{}] merge right[{}]\n", current_page_id, target_page_guard.GetPageId()));

      if constexpr (std::is_same_v<CurrentPageType, LeafPage>) {
        // leaf page, merge and remove key in upstream
        current_page.Merge(target_page, comparator_);
      } else {
        // internal page, merge and insert key pull from upstream
        const auto &mid_key = upstream_page.KeyAt(index + 1);
        current_page.Merge(target_page, mid_key, comparator_);
      }
      upstream_page.Remove(index + 1);
      delete_page_id = right_page_id;
      target_page_guard.Drop();
    }

    // delete merged page
    while (!bpm_->DeletePage(delete_page_id)) {
    }
  };

  page_id_t new_root_page_id;
  // do steal_or_merge at leaf page
  {
    auto &upstream_page = *(context.write_set_.rbegin() + 1)->AsMut<InternalPage>();
    steal_or_merge(leaf_page, upstream_page, value_index_stack.top());
    value_index_stack.pop();
    if (!upstream_page.IsInsufficient()) {
      // upstream do not need merge, return
      return;
    }
    new_root_page_id = context.write_set_.back().GetPageId();
    context.write_set_.pop_back();
  }

  // need to merge all way up
  // rend() - 1 is last page guard
  // stop at last page
  for (auto current_iter = context.write_set_.rbegin(), upstream_iter = current_iter + 1;
       current_iter != context.write_set_.rend() - 1;
       current_iter = context.write_set_.rbegin(), upstream_iter = current_iter + 1) {
    auto &current_page = *(current_iter->AsMut<InternalPage>());
    auto &upstream_page = *(upstream_iter->AsMut<InternalPage>());
    auto current_value_index = value_index_stack.top();
    value_index_stack.pop();

    steal_or_merge(current_page, upstream_page, current_value_index);
    if (!upstream_page.IsInsufficient()) {
      // no need to merge
      return;
    }
    // continue merging
    new_root_page_id = context.write_set_.back().GetPageId();
    context.write_set_.pop_back();
  }

  // reach root page
  BUSTUB_ASSERT(context.write_set_.size() == 1, "BPlusTree::Insert(): context write set has not only root page");
  auto &last_page = *(context.write_set_.back().AsMut<InternalPage>());
  auto last_page_id = context.write_set_.back().GetPageId();
  if (last_page_id != context.root_page_id_) {
    // last not root
    return;
  }

  // last is root
  if (last_page.IsEmpty()) {
    // delete empty root page
    // assign new root page
    debuger.Print(fmt::format("Remove(): root empty, new root[{}]\n", new_root_page_id));
    BUSTUB_ASSERT(context.header_page_ != std::nullopt, "BPlusTree::Remove(): header page guard is not held");

    context.write_set_.pop_back();
    bpm_->DeletePage(context.root_page_id_);
    context.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
  }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  auto header_page_read_guard = bpm_->ReadPage(header_page_id_);
  auto page_guard = bpm_->ReadPage(header_page_read_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  header_page_read_guard.Drop();
  for (auto page = page_guard.As<InternalPage>(); !page->IsLeafPage();
       page_guard = bpm_->ReadPage(page->ValueAt(0)), page = page_guard.As<InternalPage>()) {
  }
  const auto &page = *page_guard.As<LeafPage>();
  INDEXITERATOR_TYPE iter(bpm_.get(), page_guard.GetPageId(), 0, comparator_);
  if (page.TombstonesContains(0)) {
    // first key is in tombstones
    ++iter;
  }
  return std::move(iter);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  auto header_page_read_guard = bpm_->ReadPage(header_page_id_);
  auto page_guard = bpm_->ReadPage(header_page_read_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  header_page_read_guard.Drop();
  for (auto page = page_guard.As<InternalPage>(); !page->IsLeafPage();
       page_guard = bpm_->ReadPage(page->GetValue(key, comparator_)), page = page_guard.As<InternalPage>()) {
  }
  auto &page = *(page_guard.As<LeafPage>());
  auto key_index = page.FindLowKey(key, comparator_);
  if (key_index == static_cast<size_t>(page.GetSize())) {
    // key not exist
    return End();
  }
  return INDEXITERATOR_TYPE(bpm_.get(), page_guard.GetPageId(), key_index, comparator_);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_.get(), INVALID_PAGE_ID, 0, comparator_);
}

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
