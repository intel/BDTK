#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include <boost/noncopyable.hpp>

namespace DB {

/** Memory pool to append something. For example, short strings.
 * Usage scenario:
 * - put lot of strings inside pool, keep their addresses;
 * - addresses remain valid during lifetime of pool;
 * - at destruction of pool, all memory is freed;
 * - memory is allocated and freed by large MemoryChunks;
 * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
 */
class Arena : private boost::noncopyable {
 private:
  static size_t roundUpToPageSize(size_t s, size_t page_size) { return 0; }

  /// If MemoryChunks size is less than 'linear_growth_threshold', then use exponential
  /// growth, otherwise - linear growth
  ///  (to not allocate too much excessive memory).
  size_t nextSize(size_t min_next_size) const { return 0; }

  /// Add next contiguous MemoryChunk of memory with size not less than specified.
  void addMemoryChunk(size_t min_size) {}

 public:
  /// Get piece of memory, without alignment.
  char* alloc(size_t size) { return nullptr; }

  /// Get piece of memory with alignment
  char* alignedAlloc(size_t size, size_t alignment) { return nullptr; }

  template <typename T>
  T* alloc() {
    return nullptr;
  }

  /** Rollback just performed allocation.
   * Must pass size not more that was just allocated.
   * Return the resulting head pointer, so that the caller can assert that
   * the allocation it intended to roll back was indeed the last one.
   */
  void* rollback(size_t size) { return nullptr; }

  /** Begin or expand a contiguous range of memory.
   * 'range_start' is the start of range. If nullptr, a new range is
   * allocated.
   * If there is no space in the current MemoryChunk to expand the range,
   * the entire range is copied to a new, bigger memory MemoryChunk, and the value
   * of 'range_start' is updated.
   * If the optional 'start_alignment' is specified, the start of range is
   * kept aligned to this value.
   *
   * NOTE This method is usable only for the last allocation made on this
   * Arena. For earlier allocations, see 'realloc' method.
   */
  char* allocContinue(size_t additional_bytes,
                      char const*& range_start,
                      size_t start_alignment = 0) {
    return nullptr;
  }

  /// NOTE Old memory region is wasted.
  char* realloc(const char* old_data, size_t old_size, size_t new_size) {
    return nullptr;
  }

  char* alignedRealloc(const char* old_data,
                       size_t old_size,
                       size_t new_size,
                       size_t alignment) {
    return nullptr;
  }

  /// Insert string without alignment.
  const char* insert(const char* data, size_t size) { return nullptr; }

  const char* alignedInsert(const char* data, size_t size, size_t alignment) {
    return nullptr;
  }

  /// Size of MemoryChunks in bytes.
  size_t size() const { return 0; }

  /// Bad method, don't use it -- the MemoryChunks are not your business, the entire
  /// purpose of the arena code is to manage them for you, so if you find
  /// yourself having to use this method, probably you're doing something wrong.
  size_t remainingSpaceInCurrentMemoryChunk() const { return 0; }
};

}  // namespace DB
