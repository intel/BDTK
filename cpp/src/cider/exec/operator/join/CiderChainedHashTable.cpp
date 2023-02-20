/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

namespace cider_hashtable {
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K, typename V>
bool ChainedHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::emplace_impl(
    const K& key,
    const V& value) {
  size_t idx = key_to_idx(key);
  table_key<K> inner_key = {key, true, 0};
  buckets_[idx].push_back({inner_key, value});
  size_++;
  return true;
}

// TODO: assert key and value types
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
void ChainedHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::
    merge_other_hashtables(
        const std::vector<std::shared_ptr<
            BaseHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>>>& otherTables) {
  for (const auto& table_ptr_tmp : otherTables) {
    ChainedHashTable* table_ptr = dynamic_cast<ChainedHashTable*>(table_ptr_tmp.get());
    auto other_table_bucket = table_ptr->get_buckets();
    size_ += table_ptr->size();
    size_type bucket_size = bucket_count();
    for (int i = 0; i < bucket_size; i++) {
      buckets_[i].insert(
          buckets_[i].end(), other_table_bucket[i].begin(), other_table_bucket[i].end());
    }
  }
}

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
bool ChainedHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::contains_impl(
    const K& key) {
  size_t idx = key_to_idx(key);
  std::vector<value_type> slot = buckets_[idx];
  for (auto element : slot) {
    if (key_equal()(element.first.key, key)) {
      return true;
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
Value ChainedHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::find_impl(
    const K& key) {
  size_t idx = key_to_idx(key);
  std::vector<value_type> slot = buckets_[idx];
  for (auto element : slot) {
    if (key_equal()(element.first.key, key)) {
      return element.second;
    }
  }
  return Value();
}

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
std::vector<Value>
ChainedHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::find_all_impl(
    const K& key) {
  std::vector<Value> result;
  size_t idx = key_to_idx(key);
  std::vector<value_type> slot = buckets_[idx];
  for (auto element : slot) {
    if (key_equal()(element.first.key, key)) {
      result.push_back(element.second);
    }
  }
  return result;
}
template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
template <typename K>
size_t ChainedHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>::key_to_idx(
    const K& key) const noexcept(noexcept(hasher()(key))) {
  const size_t mask = buckets_.size() - 1;
  return hasher()(key) & mask;
}
}  // namespace cider_hashtable