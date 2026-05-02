#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
  using TupleLength = uint32_t;
  struct Header {
    page_id_t next_page_id_ = INVALID_PAGE_ID;
    uint32_t num_tuples_ = 0;
  } header_;

 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  IntermediateResultPage();
  explicit IntermediateResultPage(page_id_t next_page_id) : header_({next_page_id}) {}

  void Init() {
    header_.next_page_id_ = INVALID_PAGE_ID;
    header_.num_tuples_ = 0;
  }

  /** Iterator for iterating on the IntermediateResultPage tuples. */
  class Iterator {
    friend IntermediateResultPage;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple.
     */
    auto operator++() -> Iterator & {
      ptr_ += (sizeof(TupleLength) + GetLength(ptr_));
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() const -> Tuple {
      auto length = GetLength(ptr_);
      return {{}, ptr_ + sizeof(length), length};
    }

    /**
     * Checks whether two iterators are pointing to the same tuple.
     */
    auto operator==(const Iterator &other) const -> bool { return this->ptr_ == other.ptr_; }

    /**
     * Checks whether two iterators are pointing to different tuples.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

    auto GetPtr() const -> const char * { return ptr_; }

   private:
    explicit Iterator(const char *ptr) : ptr_(ptr) {}
    auto GetLength(const char *ptr) const -> TupleLength { return *reinterpret_cast<const TupleLength *>(ptr); }

    const char *ptr_{nullptr};
  };

  void SetNextPageId(page_id_t next_page_id) { header_.next_page_id_ = next_page_id; }

  auto Begin() const -> Iterator { return Iterator{GetPtr(0)}; }
  auto End() const -> Iterator { return Iterator{GetEndPtr()}; }

  auto GetData() -> char * { return data_.data(); }
  auto GetData() const -> const char * { return data_.data(); }
  auto GetNumTuples() const -> decltype(auto) { return header_.num_tuples_; }
  auto GetNextPageId() const -> page_id_t { return header_.next_page_id_; }
  // free space for tuple data, not including tuple length field
  auto FreeSpaceForTuple() const -> int { return data_.end() - GetEndPtr() - sizeof(TupleLength); }
  // free space for data, including tuple length field
  auto FreeSpaceForData() const -> uint32_t { return data_.end() - GetEndPtr(); }

  auto GetTuple(uint32_t index) const -> Tuple {
    BUSTUB_ASSERT(index < header_.num_tuples_, "IntermediateResultPage::GetTuple(): index out of range");

    Iterator iter{GetPtr(index)};
    return *iter;
  }

  void PushBackTuple(const Tuple &tuple) {
    BUSTUB_ASSERT(tuple.GetLength() < data_.size() - sizeof(TupleLength),
                  "IntermediateResultPage::PushBackTuple(): tuple too big");
    auto ptr = GetEndPtr();
    BUSTUB_ASSERT(FreeSpaceForTuple() >= static_cast<int>(tuple.GetLength()),
                  "IntermediateResultPage::PushBackTuple(): no enough space");
    auto length = tuple.GetLength();
    *reinterpret_cast<TupleLength *>(ptr) = length;
    ptr += sizeof(TupleLength);
    std::memcpy(ptr, tuple.GetData(), length);
    header_.num_tuples_++;
  }

  static constexpr size_t DATA_SIZE{BUSTUB_PAGE_SIZE - sizeof(Header)};

 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */

  // tuple layout |[tuple length](4B)|[data](variable)|
  std::array<char, DATA_SIZE> data_;

  auto GetPtr(uint32_t index) const -> const char * {
    BUSTUB_ASSERT(index <= header_.num_tuples_, "IntermediateResultPage::GetTuple(): index out of range");

    Iterator iter{GetData()};
    for (uint32_t i = 0; i < index; ++i) {
      ++iter;
    }
    return iter.GetPtr();
  }
  auto GetPtr(uint32_t index) -> char * { return const_cast<char *>(std::as_const(*this).GetPtr(index)); }
  auto GetEndPtr() -> decltype(data_)::pointer { return GetPtr(header_.num_tuples_); }
  auto GetEndPtr() const -> decltype(data_)::const_pointer { return GetPtr(header_.num_tuples_); }
};

}  // namespace bustub
