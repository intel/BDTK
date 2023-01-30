/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) 2016-2022 ClickHouse, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <common/hashtable/Hash.h>
#include <common/hashtable/HashTable.h>
#include <common/hashtable/HashTableAllocator.h>

namespace cider::hashtable {
namespace ErrorCodes {
extern const int LOGICAL_ERROR;
}

/** NOTE HashSet could only be used for memmoveable (position independent) types.
 * Example: std::string is not position independent in libstdc++ with C++11 ABI or in
 * libc++. Also, key must be of type, that zero bytes is compared equals to zero key.
 */

template <typename Key,
          typename TCell,
          typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrowerWithPrecalculation<>,
          typename Allocator = HashTableAllocator>
class HashSetTable : public HashTable<Key, TCell, Hash, Grower, Allocator> {
 public:
  using Self = HashSetTable;
  using Cell = TCell;

  using Base = HashTable<Key, TCell, Hash, Grower, Allocator>;
  using typename Base::LookupResult;

  void merge(const Self& rhs) {
    if (!this->hasZero() && rhs.hasZero()) {
      this->setHasZero();
      ++this->m_size;
    }

    for (size_t i = 0; i < rhs.grower.bufSize(); ++i)
      if (!rhs.buf[i].isZero(*this))
        this->insert(rhs.buf[i].getValue());
  }

  // TODO(Deegue): Implement and enable later
  // void readAndMerge(DB::ReadBuffer & rb)
  // {
  //     Cell::State::read(rb);

  //     size_t new_size = 0;
  //     DB::readVarUInt(new_size, rb);

  //     this->resize(new_size);

  //     for (size_t i = 0; i < new_size; ++i)
  //     {
  //         Cell x;
  //         x.read(rb);
  //         this->insert(x.getValue());
  //     }
  // }
};

// TODO(Deegue): Implement and enable later
// template <typename Key,
//           typename TCell,  /// Supposed to have no state (HashTableNoState)
//           typename Hash = DefaultHash<Key>,
//           typename Grower = TwoLevelHashTableGrower<>,
//           typename Allocator = HashTableAllocator>
// class TwoLevelHashSetTable
//     : public TwoLevelHashTable<Key,
//                                TCell,
//                                Hash,
//                                Grower,
//                                Allocator,
//                                HashSetTable<Key, TCell, Hash, Grower, Allocator>> {
//  public:
//   using Self = TwoLevelHashSetTable;
//   using Base = TwoLevelHashTable<Key,
//                                  TCell,
//                                  Hash,
//                                  Grower,
//                                  Allocator,
//                                  HashSetTable<Key, TCell, Hash, Grower, Allocator>>;

//   using Base::Base;

/// Writes its content in a way that it will be correctly read by HashSetTable.
/// Used by uniqExact to preserve backward compatibility.
// void writeAsSingleLevel(DB::WriteBuffer & wb) const
// {
//     DB::writeVarUInt(this->size(), wb);

//     bool zero_written = false;
//     for (size_t i = 0; i < Base::NUM_BUCKETS; ++i)
//     {
//         if (this->impls[i].hasZero())
//         {
//             if (zero_written)
//                 throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No more than one
//                 zero value expected");
//             this->impls[i].zeroValue()->write(wb);
//             zero_written = true;
//         }
//     }

//     static constexpr HashTableNoState state;
//     for (auto ptr = this->begin(); ptr != this->end(); ++ptr)
//         if (!ptr.getPtr()->isZero(state))
//             ptr.getPtr()->write(wb);
// }
// };

template <typename Key, typename Hash, typename TState = HashTableNoState>
struct HashSetCellWithSavedHash : public HashTableCell<Key, Hash, TState> {
  using Base = HashTableCell<Key, Hash, TState>;

  size_t saved_hash;

  HashSetCellWithSavedHash() : Base() {}  //-V730
  HashSetCellWithSavedHash(const Key& key_, const typename Base::State& state)
      : Base(key_, state) {}  //-V730

  bool keyEquals(const Key& key_) const { return bitEquals(this->key, key_); }
  bool keyEquals(const Key& key_, size_t hash_) const {
    return saved_hash == hash_ && bitEquals(this->key, key_);
  }
  bool keyEquals(const Key& key_, size_t hash_, const typename Base::State&) const {
    return keyEquals(key_, hash_);
  }

  void setHash(size_t hash_value) { saved_hash = hash_value; }
  size_t getHash(const Hash& /*hash_function*/) const { return saved_hash; }
};

template <typename Key,
          typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrowerWithPrecalculation<>,
          typename Allocator = HashTableAllocator>
using HashSet = HashSetTable<Key, HashTableCell<Key, Hash>, Hash, Grower, Allocator>;

// TODO(Deegue): Implement and enable later
// template <typename Key,
//           typename Hash = DefaultHash<Key>,
//           typename Grower = TwoLevelHashTableGrower<>,
//           typename Allocator = HashTableAllocator>
// using TwoLevelHashSet =
//     TwoLevelHashSetTable<Key, HashTableCell<Key, Hash>, Hash, Grower, Allocator>;

// template <typename Key, typename Hash, size_t initial_size_degree>
// using HashSetWithStackMemory = HashSet<
//     Key,
//     Hash,
//     HashTableGrower<initial_size_degree>,
//     HashTableAllocatorWithStackMemory<
//         (1ULL << initial_size_degree)
//         * sizeof(HashTableCell<Key, Hash>)>>;

template <typename Key,
          typename Hash = DefaultHash<Key>,
          typename Grower = HashTableGrowerWithPrecalculation<>,
          typename Allocator = HashTableAllocator>
using HashSetWithSavedHash =
    HashSetTable<Key, HashSetCellWithSavedHash<Key, Hash>, Hash, Grower, Allocator>;

// template <typename Key, typename Hash, size_t initial_size_degree>
// using HashSetWithSavedHashWithStackMemory = HashSetWithSavedHash<
//     Key,
//     Hash,
//     HashTableGrower<initial_size_degree>,
//     HashTableAllocatorWithStackMemory<
//         (1ULL << initial_size_degree)
//         * sizeof(HashSetCellWithSavedHash<Key, Hash>)>>;

}  // namespace cider::hashtable
