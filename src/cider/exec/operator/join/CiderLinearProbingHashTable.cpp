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

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K, typename... Args>
bool cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::emplace_impl(
        const K& key,
        Args&&... args) {
  reserve(size_ + 1);
  for (size_t idx = key_to_idx(key);; idx = probe_next(idx)) {
    if (!buckets_[idx].first.is_not_null) {
      buckets_[idx].second = mapped_type(std::forward<Args>(args)...);
      buckets_[idx].first.key = key;
      buckets_[idx].first.is_not_null = true;
      size_++;
      return true;
    } else if (key_equal()(buckets_[idx].first.key, key)) {
      buckets_[idx].first.duplicate_num++;
    }
  }
  return false;
}

// TODO: assert key and value types
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
void cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::
        merge_other_hashtables(
            const std::vector<std::unique_ptr<LinearProbeHashTable>>& otherTables) {
  int total_size = 0;
  for (const auto& table_ptr : otherTables) {
    total_size += table_ptr->size();
  }
  rehash(total_size);
  for (const auto& table_ptr : otherTables) {
    for (auto it = table_ptr->begin(); it != table_ptr->end(); ++it)
      insert((*it).first.key, (*it).second);
  }
}

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
void cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::swap(
        LinearProbeHashTable& other) noexcept {
  std::swap(buckets_, other.buckets_);
  std::swap(size_, other.size_);
  std::swap(empty_key_, other.empty_key_);
}

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
bool cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::contains_impl(
        const K& key) {
  for (size_t idx = key_to_idx(key);; idx = probe_next(idx)) {
    if (key_equal()(buckets_[idx].first.key, key) && buckets_[idx].first.is_not_null) {
      return true;
    } else if (buckets_[idx].first.is_not_null == false) {
      return false;
    }
  }
  return false;
}

// todo: set an empty value
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
Value cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::find_impl(
        const K& key) {
  for (size_t idx = key_to_idx(key);; idx = probe_next(idx)) {
    if (key_equal()(buckets_[idx].first.key, key) && buckets_[idx].first.is_not_null) {
      return buckets_[idx].second;
    } else if (buckets_[idx].first.is_not_null == false) {
      break;
    }
  }
  return buckets_[0].second;
}

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
std::vector<Value> cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::find_all_impl(
        const K& key) {
  std::vector<Value> vec;
  int duplicate_num = 0;
  for (size_t idx = key_to_idx(key);; idx = probe_next(idx)) {
    if (key_equal()(buckets_[idx].first.key, key) && buckets_[idx].first.is_not_null) {
      if (vec.size() == 0) {
        duplicate_num = buckets_[idx].first.duplicate_num;
      }
      vec.push_back(buckets_[idx].second);
      if (duplicate_num == 0) {
        return vec;
      }
    } else if (buckets_[idx].first.is_not_null == false ||
               (vec.size() == duplicate_num + unsigned(1))) {
      return vec;
    }
  }
  return vec;
}
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
size_t cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::key_to_idx(
        const K& key) const noexcept(noexcept(hasher()(key))) {
  const size_t mask = buckets_.size() - 1;
  return hasher()(key) & mask;
}
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
size_t cider_hashtable::
    LinearProbeHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::probe_next(
        size_t idx) const noexcept {
  const size_t mask = buckets_.size() - 1;
  return (idx + 1) & mask;
}