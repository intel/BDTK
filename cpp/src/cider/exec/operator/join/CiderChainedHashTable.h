/*
 * Copyright (c) 2022 Intel Corporation.
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

/*
A hash map for join. Uses open addressing with linear probing.
Advantages:
  - Predictable performance. Doesn't use the allocator unless load factor
    grows beyond 50%. Linear probing ensures cash efficency.
  - Desgin for no delete/erase action, makes it faster on insert and find
  - Allow duplicate keys
Disadvantages:
  - Significant performance degradation at high load factors.
  - Maximum load factor hard coded to 50%, memory inefficient.
 */
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>
#include "exec/operator/join/BaseHashTable.h"
#include "exec/operator/join/HashTableUtils.h"

namespace cider_hashtable {

template <typename Key,
          typename Value,
          typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<void>,
          typename Grower = void,
          typename Allocator = std::allocator<std::pair<table_key<Key>, Value>>>
class ChainedHashTable
    : public BaseHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator> {
 public:
  using key_type = table_key<Key>;
  using mapped_type = Value;
  using value_type = std::pair<key_type, Value>;
  using size_type = std::size_t;
  using hasher = Hash;
  using key_equal = KeyEqual;
  using allocator_type = Allocator;
  using reference = value_type&;
  using buckets = std::vector<std::vector<value_type, allocator_type>>;

 public:
  // chained hashtable initial size should be fixed
  ChainedHashTable(size_type bucket_count = 16384,
                   const allocator_type& alloc = allocator_type())
      : buckets_(alloc) {
    size_t pow2 = 1;
    while (pow2 < bucket_count) {
      pow2 <<= 1;
    }
    buckets_.resize(pow2);
  }

  allocator_type get_allocator() const noexcept { return buckets_.get_allocator(); }

  // Capacity
  bool empty() const noexcept { return size() == 0; }

  void clear() { buckets_.clear(); }

  size_type size() const noexcept { return size_; }

  bool insert(const std::pair<key_type, Value>& value) {
    return emplace_impl(value.first, value.second);
  }

  bool insert(std::pair<key_type, Value>&& value) {
    return emplace_impl(value.first, std::move(value.second));
  }

  void emplace(Key key, Value value, bool& inserted) {
    inserted = emplace_impl(key, std::move(value));
  }

  // not supported
  void emplace(Key key, Value value, size_t hash_value, bool& inserted) {}

  bool emplace(Key key, Value value) { return emplace_impl(key, std::move(value)); }
  // not supported
  bool emplace(Key key, Value value, size_t hash_value) {}

  void reserve(size_type count) {
    if (count > buckets_.size()) {
      buckets_.resize(count);
    }
  }

  // TODO: assert key and value types
  void merge_other_hashtables(
      const std::vector<
          std::shared_ptr<BaseHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>>>&
          otherTables);

  Value find(const Key key) { return find_impl(key); }
  Value find(const Key key, size_t hash_value) { return find_impl(key); }
  // find
  std::vector<mapped_type> findAll(const Key key) { return find_all_impl(key); }
  std::vector<mapped_type> findAll(const Key key, size_t hash_value) {
    return find_all_impl(key);
  }
  // not supported
  bool erase(const Key key) { return false; }
  bool erase(const Key key, size_t hash_value) { return false; }

  bool contains(const Key key) { return contains_impl(key); }
  bool contains(const Key key, size_t hash_value) { return contains_impl(key); }

  // Bucket interface
  size_type bucket_count() const noexcept { return buckets_.size(); }

  const buckets& get_buckets() { return buckets_; }

  // Observers
  hasher hash_function() const { return hasher(); }

  key_equal key_eq() const { return key_equal(); }

 private:
  template <typename K, typename V>
  bool emplace_impl(const K& key, const V& value);

  template <typename K>
  bool contains_impl(const K& key);

  template <typename K>
  mapped_type find_impl(const K& key);

  template <typename K>
  std::vector<mapped_type> find_all_impl(const K& key);

  template <typename K>
  size_t key_to_idx(const K& key) const noexcept(noexcept(hasher()(key)));

 private:
  buckets buckets_;
  size_t size_ = 0;
};
}  // namespace cider_hashtable

// separate the implementations into cpp files instead of h file
// to isolate the implementation from codegen.
// use include cpp as a method to avoid maintaining too many template
// declaration in cpp file.
#include "exec/operator/join/CiderChainedHashTable.cpp"
