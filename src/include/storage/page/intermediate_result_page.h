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
  void SetNextPageId(page_id_t next_page_id) { header_.next_page_id_ = next_page_id; }

  auto GetData() -> char * { return data_.data(); }
  auto GetData() const -> const char * { return data_.data(); }
  auto GetNumTuples() const -> decltype(auto) { return header_.num_tuples_; }
  auto GetNextPageId() const -> page_id_t { return header_.next_page_id_; }
  // free space for tuple data, not including tuple length field
  auto FreeSpaceForTuple() const -> int { return data_.end() - GetEndPtr() - sizeof(TupleLength); }
  // free space for data, including tuple length field
  auto FreeSpaceForData() const -> uint32_t { return data_.end() - GetEndPtr(); }

  auto GetPtr(uint32_t index) -> char * {
    BUSTUB_ASSERT(index <= header_.num_tuples_, "IntermediateResultPage::GetTuple(): index out of range");

    auto ptr = data_.data();
    for (uint32_t i = 0; i < index; ++i) {
      ptr += (sizeof(TupleLength) + *reinterpret_cast<TupleLength *>(ptr));
    }
    return ptr;
  }
  auto GetPtr(uint32_t index) const -> const char * {
    BUSTUB_ASSERT(index <= header_.num_tuples_, "IntermediateResultPage::GetTuple(): index out of range");

    auto ptr = data_.data();
    for (uint32_t i = 0; i < index; ++i) {
      ptr += (sizeof(TupleLength) + *reinterpret_cast<const TupleLength *>(ptr));
    }
    return ptr;
  }

  auto GetTuple(uint32_t index) const -> Tuple {
    BUSTUB_ASSERT(index < header_.num_tuples_, "IntermediateResultPage::GetTuple(): index out of range");

    auto ptr = GetPtr(index);
    auto length = GetLength(ptr);
    return {{}, ptr + sizeof(length), length};
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

 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */

  auto GetLength(const char *ptr) const -> TupleLength { return *reinterpret_cast<const TupleLength *>(ptr); }

  struct Header {
    page_id_t next_page_id_ = INVALID_PAGE_ID;
    uint32_t num_tuples_ = 0;
  } header_;

  // tuple layout |[tuple length](4B)|[data](variable)|
  std::array<char, BUSTUB_PAGE_SIZE - sizeof(Header)> data_;

  auto GetEndPtr() -> decltype(data_)::pointer { return GetPtr(header_.num_tuples_); }
  auto GetEndPtr() const -> decltype(data_)::const_pointer { return GetPtr(header_.num_tuples_); }
};

}  // namespace bustub
