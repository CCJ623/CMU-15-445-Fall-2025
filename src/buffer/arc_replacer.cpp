// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include "common/config.h"
#include "fmt/format.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {
  Log(fmt::format("[ArcReplacer] Initialized with num_frames={}", num_frames));
}

/**
 * @brief Write a log entry to LOG_PATH when DEBUG_MODE is enabled.
 */
void ArcReplacer::Log(std::string_view msg) {
  if constexpr (DEBUG_MODE) {
    std::ofstream log_file{std::string{LOG_PATH}, std::ios::app};
    log_file << msg << "\n";
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock{latch_};

  Log(fmt::format("[Evict] Called. mru.size={}, mfu.size={}, mru_target={}, curr_size={}", mru_.size(), mfu_.size(),
                  mru_target_size_, curr_size_));

  std::optional<frame_id_t> result;
  if (mru_.size() >= mru_target_size_) {
    Log("[Evict] mru.size >= mru_target: trying MRU first");

    // search evictable frame in mru
    result = TryEvictFromList(ArcStatus::MRU);

    if (result != std::nullopt) {
      Log(fmt::format("[Evict] Evicted frame {} from MRU", *result));
      return result;
    }

    Log("[Evict] MRU exhausted, falling back to MFU");
    result = TryEvictFromList(ArcStatus::MFU);
    if (result != std::nullopt) {
      Log(fmt::format("[Evict] Evicted frame {} from MFU (fallback)", *result));
    } else {
      Log("[Evict] No evictable frame found in either list");
    }
  } else {
    Log("[Evict] mru.size < mru_target: trying MFU first");

    // search evictable frame in mfu
    result = TryEvictFromList(ArcStatus::MFU);

    if (result != std::nullopt) {
      Log(fmt::format("[Evict] Evicted frame {} from MFU", *result));
      return result;
    }

    Log("[Evict] MFU exhausted, falling back to MRU");
    result = TryEvictFromList(ArcStatus::MRU);
    if (result != std::nullopt) {
      Log(fmt::format("[Evict] Evicted frame {} from MRU (fallback)", *result));
    } else {
      Log("[Evict] No evictable frame found in either list");
    }
  }

  return result;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock{latch_};

  Log(fmt::format("[RecordAccess] frame_id={}, page_id={}", frame_id, page_id));

  auto alive_iter = alive_map_.find(frame_id);
  auto ghost_iter = ghost_map_.find(page_id);

  if (alive_iter != alive_map_.end()) {
    // alive
    auto &frame = *alive_iter->second;
    Log(fmt::format("[RecordAccess] Cache hit: frame {} is alive in {}", frame_id,
                    frame.arc_status_ == ArcStatus::MRU ? "MRU" : "MFU"));
    if (frame.arc_status_ == ArcStatus::MRU) {
      mru_.erase(frame.alive_iterator_);
    } else if (frame.arc_status_ == ArcStatus::MFU) {
      mfu_.erase(frame.alive_iterator_);
    } else {
      throw std::runtime_error("Inconsistent ARC state: Alive frame has ghost status!");
    }

    mfu_.push_front(frame.frame_id_);
    frame.alive_iterator_ = mfu_.begin();
    frame.arc_status_ = ArcStatus::MFU;
    Log(fmt::format("[RecordAccess] Moved frame {} to MFU front. mfu.size={}", frame_id, mfu_.size()));

  } else if (ghost_iter != ghost_map_.end()) {
    // ghost
    auto &frame = *ghost_iter->second;
    if (frame.arc_status_ == ArcStatus::MRU_GHOST) {
      // MRU ghost
      size_t old_target = mru_target_size_;
      if (mru_ghost_.size() >= mfu_ghost_.size()) {
        mru_target_size_ = std::min(replacer_size_, mru_target_size_ + 1);
      } else {
        mru_target_size_ = std::min(replacer_size_, mru_target_size_ + mfu_ghost_.size() / mru_ghost_.size());
      }
      Log(fmt::format("[RecordAccess] Ghost hit in MRU_GHOST (page {}): mru_target {} -> {}", page_id, old_target,
                      mru_target_size_));
      mru_ghost_.erase(frame.ghost_iterator_);

    } else if (frame.arc_status_ == ArcStatus::MFU_GHOST) {
      // MFU ghost
      size_t old_target = mru_target_size_;
      if (mfu_ghost_.size() >= mru_ghost_.size()) {
        mru_target_size_ = std::max(decltype(mru_target_size_){1}, mru_target_size_) - 1;
      } else {
        auto decrease_size = mru_ghost_.size() / mfu_ghost_.size();
        mru_target_size_ = std::max(decrease_size, mru_target_size_) - decrease_size;
      }
      Log(fmt::format("[RecordAccess] Ghost hit in MFU_GHOST (page {}): mru_target {} -> {}", page_id, old_target,
                      mru_target_size_));
      mfu_ghost_.erase(frame.ghost_iterator_);

    } else {
      throw std::runtime_error("Inconsistent ARC state: Ghost frame has alive status!");
    }

    frame.frame_id_ = frame_id;
    alive_map_[frame.frame_id_] = ghost_iter->second;
    ghost_map_.erase(ghost_iter);
    mfu_.push_front(frame.frame_id_);
    frame.alive_iterator_ = mfu_.begin();
    frame.arc_status_ = ArcStatus::MFU;
    Log(fmt::format("[RecordAccess] Resurrected page {} as frame {} into MFU front. mfu.size={}", page_id, frame_id,
                    mfu_.size()));

  } else {
    // freshman
    Log(fmt::format("[RecordAccess] Freshman miss: frame_id={}, page_id={}", frame_id, page_id));
    if (mru_.size() + mru_ghost_.size() == replacer_size_) {
      // mru and it's ghost reach replacer_size_, it should not be bigger
      // evict mru ghost
      Log(fmt::format("[RecordAccess] MRU+MRU_GHOST == replacer_size: evicting MRU ghost (page {})",
                      mru_ghost_.back()));
      ghost_map_.erase(mru_ghost_.back());
      mru_ghost_.pop_back();
    } else if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
      // replacer is full and mru is short
      // evict mfu ghost
      Log(fmt::format("[RecordAccess] All lists full: evicting MFU ghost (page {})", mfu_ghost_.back()));
      ghost_map_.erase(mfu_ghost_.back());
      mfu_ghost_.pop_back();
    }

    auto frame_ptr = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    auto &frame = *frame_ptr;
    alive_map_.insert({frame_id, frame_ptr});
    mru_.push_front(frame_id);
    frame.alive_iterator_ = mru_.begin();
    Log(fmt::format("[RecordAccess] Added frame {} (page {}) to MRU front (non-evictable). mru.size={}", frame_id,
                    page_id, mru_.size()));
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock{latch_};
  auto iter = alive_map_.find(frame_id);

  // check invalid frame id
  if (iter == alive_map_.end()) {
    Log(fmt::format("[SetEvictable] ERROR: frame_id={} not found in alive_map", frame_id));
    throw std::out_of_range("Frame ID out of range");
  }

  if (iter->second->evictable_ != set_evictable) {
    if (set_evictable) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
    Log(fmt::format("[SetEvictable] frame_id={}: evictable {} -> {}, curr_size={}", frame_id, !set_evictable,
                    set_evictable, curr_size_));
    iter->second->evictable_ = set_evictable;
  } else {
    Log(fmt::format("[SetEvictable] frame_id={}: evictable already={}, no change", frame_id, set_evictable));
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock{latch_};
  auto iter = alive_map_.find(frame_id);

  // specified frame is not found
  if (iter == alive_map_.end()) {
    Log(fmt::format("[Remove] frame_id={} not found, returning", frame_id));
    return;
  }

  auto &frame = *iter->second;

  if (!iter->second->evictable_) {
    Log(fmt::format("[Remove] ERROR: frame_id={} is non-evictable, aborting", frame_id));
    throw std::logic_error("Attempted to remove a non-evictable frame!");
  }

  switch (frame.arc_status_) {
    case ArcStatus::MRU:
      Log(fmt::format("[Remove] Removing frame {} from MRU", frame_id));
      mru_.erase(frame.alive_iterator_);
      break;
    case ArcStatus::MFU:
      Log(fmt::format("[Remove] Removing frame {} from MFU", frame_id));
      mfu_.erase(frame.alive_iterator_);
      break;
    case ArcStatus::MRU_GHOST:
    case ArcStatus::MFU_GHOST:
      throw std::runtime_error("Inconsistent ARC state: Alive frame has ghost status!");
      break;
    default:
      throw std::runtime_error("Frame's ARC state is corrupted");
      break;
  }

  alive_map_.erase(frame.frame_id_);
  --curr_size_;
  Log(fmt::format("[Remove] Frame {} removed successfully. curr_size={}", frame_id, curr_size_));
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock{latch_};
  Log(fmt::format("[Size] curr_size={}", curr_size_));
  return curr_size_;
}

auto ArcReplacer::TryEvictFromList(ArcStatus list_type) -> std::optional<frame_id_t> {
  if (list_type != ArcStatus::MRU && list_type != ArcStatus::MRU) {
  }

  // init variables
  auto [alive_list, ghost_list, alive_list_type_string, ghost_type] =
      [this, list_type]() -> std::tuple<decltype(mru_) &, decltype(mru_ghost_) &, std::string_view, ArcStatus> {
    if (list_type == ArcStatus::MRU) {
      return {mru_, mru_ghost_, "MRU", ArcStatus::MRU_GHOST};
    }
    if (list_type == ArcStatus::MFU) {
      return {mfu_, mfu_ghost_, "MFU", ArcStatus::MFU_GHOST};
    }
    throw std::runtime_error("Try evict from non-alive list");
  }();

  Log(fmt::format("[TryEvict] Scanning {} for evictable frame (list.size={})", alive_list_type_string,
                  alive_list.size()));
  for (auto iter = alive_list.rbegin(); iter != alive_list.rend(); ++iter) {
    if (auto map_iter = alive_map_.find(*iter); map_iter != alive_map_.end() && map_iter->second->evictable_) {
      auto frame_ptr = map_iter->second;
      auto &frame = *frame_ptr;

      ghost_list.push_front(frame.page_id_);
      alive_list.erase(std::next(iter).base());
      ghost_map_.insert({frame.page_id_, frame_ptr});
      alive_map_.erase(map_iter);

      frame.arc_status_ = ghost_type;
      frame.ghost_iterator_ = ghost_list.begin();
      frame.evictable_ = false;

      --curr_size_;

      Log(fmt::format("[TryEvict] Evicted frame {} (page {}) from {} -> {}_GHOST. curr_size={}", frame.frame_id_,
                      frame.page_id_, alive_list_type_string, alive_list_type_string, curr_size_));
      return frame.frame_id_;
    }
  }
  Log(fmt::format("[TryEvict] No evictable frame found in {}", alive_list_type_string));

  // no evict
  return std::nullopt;
}

}  // namespace bustub
