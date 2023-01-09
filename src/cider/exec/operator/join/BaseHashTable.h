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
#include <memory>
#include <vector>
namespace cider_hashtable {

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
class BaseHashTable {
 public:
  // insert data
  virtual bool emplace(Key key, Value value) = 0;
  // inserted is the flag used to indicate whether the insertion result is successful or
  // not
  virtual void emplace(Key key, Value value, bool inserted) = 0;
  // hash_value is the hash result of the key
  virtual bool emplace(Key key, Value value, size_t hash_value) = 0;
  // hash_value is the hash result of the key
  // inserted is the flag used to indicate whether the insertion result is successful or
  // not
  virtual void emplace(Key key, Value value, size_t hash_value, bool inserted) = 0;

  // find one result that matched the key, for agg and non-duplicated value cases
  virtual Value find(const Key key) = 0;
  // hash_value is the hash result of the key
  virtual Value find(const Key key, size_t hash_value) = 0;

  // find all results that matched the key, for join and duplicated value cases
  virtual std::vector<Value> findAll(const Key key) = 0;
  // hash_value is the hash result of the key
  virtual std::vector<Value> findAll(const Key key, size_t hash_value) = 0;

  // Earse the data that match the key
  virtual bool erase(const Key key) = 0;
  // hash_value is the hash result of the key
  virtual bool erase(const Key key, size_t hash_value) = 0;

  // judge whether this key is included
  virtual bool contains(const Key key) = 0;
  // hash_value is the hash result of the key
  virtual bool contains(const Key key, size_t hash_value) = 0;

  // return the elements number in this hashtable
  virtual std::size_t size() const = 0;

  // check size and call rehash if needed
  virtual void reserve(size_t num_elements) = 0;

  // judge whether the hashtable is empty
  virtual bool empty() const = 0;

  // delete elements in this hashtable
  virtual void clear() = 0;

  // merge other hashtables
  virtual void merge_other_hashtables(
      const std::vector<
          std::unique_ptr<BaseHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>>>&
          otherTables) = 0;
};
}  // namespace cider_hashtable