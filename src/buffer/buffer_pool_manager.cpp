//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <utility>
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::unique_lock<std::mutex> lock{*bpm_latch_};

  auto frame_ptr = AllocateFrame(lock);
  if (frame_ptr == nullptr) {
    return INVALID_PAGE_ID;
  }

  // Only increment after we successfully got a frame.
  page_id_t new_page_id = next_page_id_.fetch_add(1);
  frame_ptr->page_id_ = new_page_id;
  page_table_[new_page_id] = frame_ptr->frame_id_;

  return frame_ptr->page_id_;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  std::scoped_lock lock{*bpm_latch_};

  auto frame_ptr = FindFrameInMemory(page_id);

  if (frame_ptr == nullptr) {
    // not in buffer
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  // in buffer
  auto &frame = *frame_ptr;

  if (frame.pin_count_ > 0) {
    // is pinned
    return false;
  }

  // not pinned
  disk_scheduler_->DeallocatePage(frame.page_id_);
  page_table_.erase(frame.page_id_);
  replacer_->Remove(frame.frame_id_);
  frame_ptr->Reset();
  free_frames_.push_back(frame_ptr->frame_id_);

  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  std::unique_lock<std::mutex> lock{*bpm_latch_};
  auto frame_ptr = FindPageInMemoryAndDisk(page_id, lock);
  if (frame_ptr == nullptr) {
    // not found or buffer full
    return std::nullopt;
  }

  ++frame_ptr->pin_count_;
  frame_ptr->is_dirty_ = true;
  replacer_->RecordAccess(frame_ptr->frame_id_, page_id);
  replacer_->SetEvictable(frame_ptr->frame_id_, false);
  lock.unlock();
  return WritePageGuard{page_id, frame_ptr, replacer_, bpm_latch_, disk_scheduler_};
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::unique_lock<std::mutex> lock{*bpm_latch_};
  auto frame_ptr = FindPageInMemoryAndDisk(page_id, lock);
  if (frame_ptr == nullptr) {
    // not found or buffer full
    return std::nullopt;
  }

  ++frame_ptr->pin_count_;
  replacer_->RecordAccess(frame_ptr->frame_id_, page_id);
  replacer_->SetEvictable(frame_ptr->frame_id_, false);
  lock.unlock();
  return ReadPageGuard{page_id, frame_ptr, replacer_, bpm_latch_, disk_scheduler_};
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  auto frame_ptr = FindFrameInMemory(page_id);

  if (frame_ptr == nullptr) {
    // not in memory
    return false;
  }

  auto &frame = *frame_ptr;
  if (frame.is_dirty_) {
    Flush(frame);
    frame.is_dirty_ = false;
  }

  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  std::unique_lock<std::mutex> lock{*bpm_latch_};

  auto frame_ptr = FindFrameInMemory(page_id);
  if (frame_ptr == nullptr) {
    // not in memory
    return false;
  }

  // protect frame
  ++frame_ptr->pin_count_;
  // release bpm lock FIRST
  lock.unlock();

  std::unique_lock<std::shared_mutex> frame_lock{frame_ptr->rwlatch_};
  if (frame_ptr->is_dirty_) {
    // wait for flush
    Flush(*frame_ptr);
  }
  frame_lock.unlock();

  lock.lock();
  --frame_ptr->pin_count_;

  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  for (const auto &pair : page_table_) {
    auto frame_ptr = frames_[pair.second];
    auto &frame = *frame_ptr;
    if (frame.is_dirty_) {
      Flush(frame);
      frame.is_dirty_ = false;
    }
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  std::unique_lock<std::mutex> lock{*bpm_latch_};
  std::vector<std::shared_ptr<FrameHeader>> to_flush;

  for (const auto &pair : page_table_) {
    auto frame_ptr = frames_[pair.second];
    ++frame_ptr->pin_count_;
    to_flush.push_back(frame_ptr);
  }

  lock.unlock();

  for (auto &frame_ptr : to_flush) {
    std::unique_lock<std::shared_mutex> frame_lock{frame_ptr->rwlatch_};
    if (frame_ptr->is_dirty_) {
      Flush(*frame_ptr);
    }
  }

  lock.lock();
  for (auto &frame_ptr : to_flush) {
    --frame_ptr->pin_count_;
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  std::scoped_lock lock{*bpm_latch_};
  auto frame_ptr = FindFrameInMemory(page_id);
  if (frame_ptr == nullptr) {
    // not found in memory
    return std::nullopt;
  }

  // in memory
  return frame_ptr->pin_count_;
}

auto BufferPoolManager::AllocateFrame(std::unique_lock<std::mutex> &bpm_lock) -> std::shared_ptr<FrameHeader> {
  std::shared_ptr<FrameHeader> result;

  while (free_frames_.empty()) {
    // buffer full
    // try evict
    auto frame_id = replacer_->Evict();
    if (frame_id == std::nullopt) {
      return nullptr;
    }

    if (DeallocateFrame(*frame_id, bpm_lock)) {
      break;
    }
  }

  BUSTUB_ASSERT(!free_frames_.empty(), "free_frames should not be empty");

  // buffer not full
  result = frames_[free_frames_.back()];
  free_frames_.pop_back();
  return result;
}

auto BufferPoolManager::DeallocateFrame(FrameHeader &frame, std::unique_lock<std::mutex> &bpm_lock) -> bool {
  BUSTUB_ASSERT(frame.pin_count_ == 0, "try to deallocate frame which is pinned");

  if (frame.is_dirty_) {
    // pin to prevent another thread deleting or pulling it before we finish driving it to disk!
    ++frame.pin_count_;

    std::unique_lock<std::shared_mutex> frame_lock{frame.rwlatch_};

    auto done = ScheduleFlush(frame);

    // release bpm lock and wait for IO safely
    bpm_lock.unlock();
    bool success = done.get();
    if (!success) {
      throw std::runtime_error("Flush failed");
    }

    frame.is_dirty_ = false;
    bpm_lock.lock();

    --frame.pin_count_;

    if (frame.pin_count_ > 0) {
      // frame was pinned while doing IO and not holding bpm_lock
      // cannot flush, someone wants to use this page!
      // frame_lock is unlocked automatically when leaving scope
      return false;
    }

    page_table_.erase(frame.page_id_);
    frame.Reset();
    free_frames_.push_back(frame.frame_id_);
    return true;
  }

  if (frame.pin_count_ > 0) {
    return false;
  }

  // frame is clean, don't need to flush
  page_table_.erase(frame.page_id_);
  frame.Reset();
  free_frames_.push_back(frame.frame_id_);
  return true;
}

auto BufferPoolManager::DeallocateFrame(frame_id_t frame_id, std::unique_lock<std::mutex> &bpm_lock) -> bool {
  BUSTUB_ASSERT(0 <= frame_id && static_cast<size_t>(frame_id) < frames_.size(), "frame id out of range");
  return DeallocateFrame(*frames_[frame_id], bpm_lock);
}

auto BufferPoolManager::DeallocateFrameByPageID(page_id_t page_id, std::unique_lock<std::mutex> &bpm_lock) -> bool {
  auto iter = page_table_.find(page_id);
  BUSTUB_ASSERT(iter != page_table_.end(), "page id is not mapped");
  return DeallocateFrame(iter->second, bpm_lock);
}

auto BufferPoolManager::FindFrameInMemory(page_id_t page_id) -> std::shared_ptr<FrameHeader> {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return nullptr;
  }

  BUSTUB_ASSERT(0 <= iter->second && static_cast<size_t>(iter->second) < frames_.size(), "frame id out of range");
  return frames_[iter->second];
}

auto BufferPoolManager::FindPageInMemoryAndDisk(page_id_t page_id, std::unique_lock<std::mutex> &bpm_lock)
    -> std::shared_ptr<FrameHeader> {
  // try find in buffer
  auto frame_ptr = FindFrameInMemory(page_id);

  if (frame_ptr == nullptr) {
    // not in memory
    // try find in disk
    auto allocated_frame = AllocateFrame(bpm_lock);
    if (allocated_frame == nullptr) {
      // buffer is full
      return nullptr;
    }

    // re-check in case another thread already allocated it while we waited for eviction.
    frame_ptr = FindFrameInMemory(page_id);
    if (frame_ptr != nullptr) {
      // another thread allocated it! return ours to free list
      free_frames_.push_back(allocated_frame->frame_id_);
      return frame_ptr;
    }

    // it is ours to allocate!
    frame_ptr = allocated_frame;
    frame_ptr->page_id_ = page_id;
    page_table_[page_id] = frame_ptr->frame_id_;

    // find in disk
    // protect the pin count to prevent eviction
    ++frame_ptr->pin_count_;

    // lock frame to avoid being access in IO phase
    // Safe to do while holding bpm_lock because frame comes from free list (was pin_count == 0)
    std::unique_lock<std::shared_mutex> frame_lock{frame_ptr->rwlatch_, std::defer_lock};
    frame_lock.lock();

    // release bpm latch in IO phase
    bpm_lock.unlock();

    // IO phase
    auto promise = disk_scheduler_->CreatePromise();
    auto done = promise.get_future();
    DiskRequest request{false, frame_ptr->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> requests;
    requests.push_back(std::move(request));
    disk_scheduler_->Schedule(requests);

    // IO return
    auto success = done.get();
    frame_lock.unlock();
    bpm_lock.lock();
    --frame_ptr->pin_count_;

    if (!success) {
      // IO error
      DeallocateFrame(*frame_ptr, bpm_lock);
      return nullptr;
    }
  }

  return frame_ptr;
}

auto BufferPoolManager::ScheduleFlush(FrameHeader &frame) -> std::future<bool> {
  auto promise = disk_scheduler_->CreatePromise();
  auto done = promise.get_future();
  DiskRequest request{true, frame.GetDataMut(), frame.page_id_, std::move(promise)};
  std::vector<DiskRequest> request_vector;
  request_vector.push_back(std::move(request));
  disk_scheduler_->Schedule(request_vector);
  return done;
}

void BufferPoolManager::Flush(FrameHeader &frame) {
  auto done = ScheduleFlush(frame);

  // IO return
  if (!done.get()) {
    // disk error
    throw std::runtime_error("Flush failed");
  }

  frame.is_dirty_ = false;
}

}  // namespace bustub
