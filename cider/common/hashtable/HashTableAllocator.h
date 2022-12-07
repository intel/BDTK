#pragma once

#include <../../cider/include/cider/CiderAllocator.h>

/**
 * We are going to use the entire memory we allocated when resizing a hash
 * table, so it makes sense to pre-fault the pages so that page faults don't
 * interrupt the resize loop. Set the allocator parameter accordingly.
 */
using HashTableAllocator = CiderDefaultAllocator;

// template <size_t initial_bytes = 64>
// using HashTableAllocatorWithStackMemory = AllocatorWithStackMemory<HashTableAllocator,
// initial_bytes>;
