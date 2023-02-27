/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include <common/hashtable/FixedHashTable.h>
#include <common/hashtable/FixedJoinHashTable.h>
#include <common/hashtable/HashMap.h>
#include <vector>

namespace cider::hashtable {
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell {
  using Mapped = TMapped;
  using State = TState;

  using value_type = PairNoInit<Key, Mapped>;
  using mapped_type = TMapped;

  bool full;
  Mapped mapped;

  FixedHashMapCell() {}
  FixedHashMapCell(const Key&, const State&) : full(true) {}
  FixedHashMapCell(const value_type& value_, const State&)
      : full(true), mapped(value_.second) {}

  const VoidKey getKey() const { return {}; }
  Mapped& getMapped() { return mapped; }
  const Mapped& getMapped() const { return mapped; }

  bool isZero(const State&) const { return !full; }
  void setZero() { full = false; }

  /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped
  /// field.
  ///  Note that we have to assemble a continuous layout for the value_type on each call
  ///  of getValue().
  struct CellExt {
    CellExt() {}
    CellExt(Key&& key_, const FixedHashMapCell* ptr_)
        : key(key_), ptr(const_cast<FixedHashMapCell*>(ptr_)) {}
    void update(Key&& key_, const FixedHashMapCell* ptr_) {
      key = key_;
      ptr = const_cast<FixedHashMapCell*>(ptr_);
    }
    Key key;
    FixedHashMapCell* ptr;

    const Key& getKey() const { return key; }
    Mapped& getMapped() { return ptr->mapped; }
    const Mapped& getMapped() const { return ptr->mapped; }
    const value_type getValue() const { return {key, ptr->mapped}; }
  };
};

// In case when we can encode empty cells with zero mapped values.
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapImplicitZeroCell {
  using Mapped = TMapped;
  using State = TState;

  using value_type = PairNoInit<Key, Mapped>;
  using mapped_type = TMapped;

  Mapped mapped;

  FixedHashMapImplicitZeroCell() {}
  FixedHashMapImplicitZeroCell(const Key&, const State&) {}
  FixedHashMapImplicitZeroCell(const value_type& value_, const State&)
      : mapped(value_.second) {}

  const VoidKey getKey() const { return {}; }
  Mapped& getMapped() { return mapped; }
  const Mapped& getMapped() const { return mapped; }

  bool isZero(const State&) const { return !mapped; }
  void setZero() { mapped = {}; }

  // Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped
  // field.
  // Note that we have to assemble a continuous layout for the value_type on each call
  // of getValue().
  struct CellExt {
    CellExt() {}
    CellExt(Key&& key_, const FixedHashMapImplicitZeroCell* ptr_)
        : key(key_), ptr(const_cast<FixedHashMapImplicitZeroCell*>(ptr_)) {}
    void update(Key&& key_, const FixedHashMapImplicitZeroCell* ptr_) {
      key = key_;
      ptr = const_cast<FixedHashMapImplicitZeroCell*>(ptr_);
    }
    Key key;
    FixedHashMapImplicitZeroCell* ptr;

    const Key& getKey() const { return key; }
    Mapped& getMapped() { return ptr->mapped; }
    const Mapped& getMapped() const { return ptr->mapped; }
    const value_type getValue() const { return {key, ptr->mapped}; }
  };
};

// In case when we can encode empty cells with zero mapped values.
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedJoinHashMapCell {
  using Mapped = TMapped;
  using State = TState;

  using value_type = PairNoInit<Key, std::vector<Mapped>>;
  using mapped_type = TMapped;

  std::vector<Mapped> mapped;

  FixedJoinHashMapCell() {}
  FixedJoinHashMapCell(const Key&, const State&) {}
  FixedJoinHashMapCell(const value_type& value_, const State&) : mapped(value_.second) {}

  const VoidKey getKey() const { return {}; }
  std::vector<Mapped> getMapped() { return mapped; }
  const std::vector<Mapped> getMapped() const { return mapped; }

  void put(Mapped value) { mapped.emplace_back(value); }

  bool isZero(const State&) const { return mapped.size() == 0; }
  void setZero() { mapped.clear(); }

  // Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped
  // field.
  // Note that we have to assemble a continuous layout for the value_type on each call
  // of getValue().
  struct CellExt {
    CellExt() {}
    CellExt(Key&& key_, const FixedJoinHashMapCell* ptr_)
        : key(key_), ptr(const_cast<FixedJoinHashMapCell*>(ptr_)) {}
    void update(Key&& key_, const FixedJoinHashMapCell* ptr_) {
      key = key_;
      ptr = const_cast<FixedJoinHashMapCell*>(ptr_);
    }
    Key key;
    FixedJoinHashMapCell* ptr;

    const Key& getKey() const { return key; }
    std::vector<Mapped> getMapped() { return ptr->mapped; }
    const std::vector<Mapped> getMapped() const { return ptr->mapped; }
    const value_type getValue() const { return {key, ptr->mapped}; }
  };
};

template <typename Key,
          typename Mapped,
          typename Cell = FixedHashMapCell<Key, Mapped>,
          typename Size = FixedHashTableStoredSize<Cell>,
          typename Allocator = HashTableAllocator>
class FixedHashMap : public FixedHashTable<Key, Cell, Size, Allocator> {
 public:
  using Base = FixedHashTable<Key, Cell, Size, Allocator>;
  using Self = FixedHashMap;
  using LookupResult = typename Base::LookupResult;

  using Base::Base;

  template <typename Func, bool>
  void ALWAYS_INLINE mergeToViaEmplace(Self& that, Func&& func) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      typename Self::LookupResult res_it;
      bool inserted;
      that.emplace(it->getKey(), res_it, inserted, it.getHash());
      func(res_it->getMapped(), it->getMapped(), inserted);
    }
  }

  template <typename Func>
  void ALWAYS_INLINE mergeToViaFind(Self& that, Func&& func) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      auto res_it = that.find(it->getKey(), it.getHash());
      if (!res_it)
        func(it->getMapped(), it->getMapped(), false);
      else
        func(res_it->getMapped(), it->getMapped(), true);
    }
  }

  template <typename Func>
  void forEachValue(Func&& func) {
    for (auto& v : *this)
      func(v.getKey(), v.getMapped());
  }

  template <typename Func>
  void forEachMapped(Func&& func) {
    for (auto& v : *this)
      func(v.getMapped());
  }

  Mapped& ALWAYS_INLINE operator[](const Key& x) {
    LookupResult it;
    bool inserted;
    this->emplace(x, it, inserted);
    if (inserted)
      new (&it->getMapped()) Mapped();

    return it->getMapped();
  }
};

template <typename Key,
          typename Mapped,
          typename Cell = FixedJoinHashMapCell<Key, Mapped>,
          typename Size = FixedJoinHashTableStoredSize<Cell>,
          typename Allocator = HashTableAllocator>
class FixedJoinHashMap : public FixedJoinHashTable<Key, Cell, Size, Allocator> {
 public:
  using Base = FixedJoinHashTable<Key, Cell, Size, Allocator>;
  using Self = FixedJoinHashMap;
  using LookupResult = typename Base::LookupResult;

  using Base::Base;

  template <typename Func, bool>
  void ALWAYS_INLINE mergeToViaEmplace(Self& that, Func&& func) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      typename Self::LookupResult res_it;
      bool inserted;
      that.emplace(it->getKey(), res_it, inserted, it.getHash());
      func(res_it->getMapped(), it->getMapped(), inserted);
    }
  }

  template <typename Func>
  void ALWAYS_INLINE mergeToViaFind(Self& that, Func&& func) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      auto res_it = that.find(it->getKey(), it.getHash());
      if (!res_it)
        func(it->getMapped(), it->getMapped(), false);
      else
        func(res_it->getMapped(), it->getMapped(), true);
    }
  }

  void ALWAYS_INLINE merge_other_hashtables(Self& that) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      auto res_it = that.find(it->getKey(), it.getHash());

      if (res_it) {
        std::vector<Mapped> res_mapped = res_it->getMapped();
        std::vector<Mapped> it_mapped = it->getMapped();
        it_mapped.insert(it_mapped.begin(), res_mapped.begin(), res_mapped.end());
      }
    }
  }

  template <typename Func>
  void forEachValue(Func&& func) {
    for (auto& v : *this)
      func(v.getKey(), v.getMapped());
  }

  template <typename Func>
  void forEachMapped(Func&& func) {
    for (auto& v : *this)
      func(v.getMapped());
  }

  void insert(const Key& x, const Mapped& v) {
    LookupResult it;
    bool inserted;
    this->emplace(x, it, inserted);
    it->getMapped().emplace_back(v);
  }
};

// It is mainly used for 1-bytes key, like int8 etc.
template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedImplicitZeroHashMapWithStoredSize =
    FixedHashMap<Key,
                 Mapped,
                 FixedHashMapImplicitZeroCell<Key, Mapped>,
                 FixedHashTableStoredSize<FixedHashMapImplicitZeroCell<Key, Mapped>>,
                 Allocator>;

// It is mainly used for 2-bytes key, like int16 etc.
template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedImplicitZeroHashMapWithCalculatedSize =
    FixedHashMap<Key,
                 Mapped,
                 FixedHashMapImplicitZeroCell<Key, Mapped>,
                 FixedHashTableCalculatedSize<FixedHashMapImplicitZeroCell<Key, Mapped>>,
                 Allocator>;

// It is mainly used for 1-bytes key, like int8 etc.
template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedJoinHashMapWithStoredSize =
    FixedJoinHashMap<Key,
                     Mapped,
                     FixedJoinHashMapCell<Key, Mapped>,
                     FixedJoinHashTableStoredSize<FixedJoinHashMapCell<Key, Mapped>>,
                     Allocator>;

// It is mainly used for 2-bytes key, like int16 etc.
template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
using FixedJoinHashMapWithCalculatedSize =
    FixedJoinHashMap<Key,
                     Mapped,
                     FixedJoinHashMapCell<Key, Mapped>,
                     FixedJoinHashTableCalculatedSize<FixedJoinHashMapCell<Key, Mapped>>,
                     Allocator>;

}  // namespace cider::hashtable
