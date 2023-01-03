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
  - Predictable performance. Doesn'Value use the allocator unless load factor
    grows beyond 50%. Linear probing ensures cash efficency.
  - Desgin for no delete/erase action, makes it faster on insert and find
  - Allow duplicate keys
Disadvantages:
  - Significant performance degradation at high load factors.
  - Maximum load factor hard coded to 50%, memory inefficient.
 */
#pragma once

#include <exec/operator/join/BaseHashTable.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>
#include "cider/CiderException.h"

namespace cider_hashtable {

template <typename KeyType>
struct table_key {
  KeyType key;
  bool is_not_null;
  std::size_t duplicate_num;
};

// TODO: extends JoinHashTable class and a hashtable basic interface
template <typename Key,
          typename Value,
          typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<void>,
          typename Grower = void,
          typename Allocator = std::allocator<std::pair<table_key<Key>, Value>>>
class LinearProbeHashTable
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
  using buckets = std::vector<value_type, allocator_type>;

  template <typename IterVal>
  struct hashtable_iterator {
    using difference_type = std::ptrdiff_t;
    using value_type = IterVal;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::forward_iterator_tag;

    bool operator==(const hashtable_iterator& other) const {
      return other.hm_ == hm_ && other.idx_ == idx_;
    }
    bool operator!=(const hashtable_iterator& other) const { return !(other == *this); }

    hashtable_iterator& operator++() {
      ++idx_;
      advance_past_empty();
      return *this;
    }

    reference operator*() const { return hm_->buckets_[idx_]; }
    pointer operator->() const { return &hm_->buckets_[idx_]; }

   private:
    explicit hashtable_iterator(LinearProbeHashTable* hm) : hm_(hm) {
      advance_past_empty();
    }
    explicit hashtable_iterator(const LinearProbeHashTable* hm)
        : hm_(const_cast<LinearProbeHashTable*>(hm)) {
      advance_past_empty();
    }
    explicit hashtable_iterator(LinearProbeHashTable* hm, size_type idx)
        : hm_(hm), idx_(idx) {}
    explicit hashtable_iterator(const LinearProbeHashTable* hm, const size_type idx)
        : hm_(const_cast<LinearProbeHashTable*>(hm)), idx_(idx) {}
    template <typename OtherIterVal>
    hashtable_iterator(const hashtable_iterator<OtherIterVal>& other)
        : hm_(other.hm_), idx_(other.idx_) {}

    void advance_past_empty() {
      while (idx_ < hm_->buckets_.size() &&
             (hm_->buckets_[idx_].first.is_not_null == false)) {
        ++idx_;
      }
    }

    LinearProbeHashTable* hm_ = nullptr;
    typename LinearProbeHashTable::size_type idx_ = 0;
    friend LinearProbeHashTable;
  };

  using iterator = hashtable_iterator<value_type>;
  using const_iterator = hashtable_iterator<const value_type>;

 public:
  LinearProbeHashTable(size_type bucket_count = 16,
                       Key empty_key = NULL,
                       const allocator_type& alloc = allocator_type())
      : empty_key_({empty_key, false, 0}), buckets_(alloc) {
    size_t pow2 = 1;
    while (pow2 < bucket_count) {
      pow2 <<= 1;
    }
    buckets_.resize(pow2, std::make_pair(empty_key_, Value()));
  }

  LinearProbeHashTable(const LinearProbeHashTable& other, size_type bucket_count)
      : LinearProbeHashTable(bucket_count, other.empty_key_.key, other.get_allocator()) {
    for (auto it = other.begin(); it != other.end(); ++it) {
      insert(*it);
    }
  }

  allocator_type get_allocator() const noexcept { return buckets_.get_allocator(); }

  // Iterators
  iterator begin() noexcept { return iterator(this); }

  const_iterator begin() const noexcept { return const_iterator(this); }

  const_iterator cbegin() const noexcept { return const_iterator(this); }

  iterator end() noexcept { return iterator(this, buckets_.size()); }

  const_iterator end() const noexcept { return const_iterator(this, buckets_.size()); }

  const_iterator cend() const noexcept { return const_iterator(this, buckets_.size()); }

  // Capacity
  bool empty() const noexcept { return size() == 0; }

  void clear() { buckets_.clear(); }

  size_type size() const noexcept { return size_; }

  size_type max_size() const noexcept { return buckets_.max_size() / 2; }

  bool insert(const std::pair<key_type, Value>& value) {
    return emplace_impl(value.first.key, value.second);
  }

  bool insert(std::pair<key_type, Value>&& value) {
    return emplace_impl(value.first.key, std::move(value.second));
  }

  template <typename... Args>
  bool insert(Args&&... args) {
    return emplace_impl(std::forward<Args>(args)...);
  }

  template <typename... Args>
  bool emplace(Args&&... args) {
    return emplace_impl(std::forward<Args>(args)...);
  }

  void emplace(Key key, Value value, bool inserted) {
    inserted = emplace_impl(key, value);
  }

  // not supported
  void emplace(Key key, Value value, size_t hash_value, bool inserted) {}

  bool emplace(Key key, Value value) { return emplace_impl(key, std::move(value)); }
  // not supported
  bool emplace(Key key, Value value, size_t hash_value) {}

  // TODO: assert key and value types
  void merge_other_hashtables(
      const std::vector<std::unique_ptr<LinearProbeHashTable>>& otherTables);

  void swap(LinearProbeHashTable& other) noexcept;

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

  size_type max_bucket_count() const noexcept { return buckets_.max_size(); }

  // Hash policy
  void rehash(size_type count) {
    // comment out due to may adjust load factor in future
    // count = std::max(count, size() * 2);
    LinearProbeHashTable other(*this, count);
    swap(other);
  }

  void reserve(size_type count) {
    if (count * 2 > buckets_.size()) {
      rehash(count * 2);
    }
  }

  // Observers
  hasher hash_function() const { return hasher(); }

  key_equal key_eq() const { return key_equal(); }

 private:
  template <typename K, typename... Args>
  bool emplace_impl(const K& key, Args&&... args);

  template <typename K>
  bool contains_impl(const K& key);

  template <typename K>
  mapped_type find_impl(const K& key);

  template <typename K>
  std::vector<mapped_type> find_all_impl(const K& key);

  template <typename K>
  size_t key_to_idx(const K& key) const noexcept(noexcept(hasher()(key)));

  size_t probe_next(size_t idx) const noexcept;

 private:
  key_type empty_key_;
  buckets buckets_;
  size_t size_ = 0;
};
}  // namespace cider_hashtable

#include <exec/operator/join/CiderLinearProbingHashTable.cpp>