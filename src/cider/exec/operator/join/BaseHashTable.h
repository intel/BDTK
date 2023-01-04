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
namespace cider_hashtable {

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
class BaseHashTable {
 public:
  // (Recommended) Should be automatically inlined
  virtual void emplace(Key key, Value value, bool inserted) = 0;
  // Improve performance for string cases
  virtual void emplace(Key key, Value value, size_t hash_value, bool inserted) = 0;
  // For single call
  virtual bool emplace(Key key, Value value) = 0;
  // Improve performance for string cases
  virtual bool emplace(Key key, Value value, size_t hash_value) = 0;

  // For agg and non-duplicated value cases
  virtual Value find(const Key key) = 0;
  virtual Value find(const Key key, size_t hash_value) = 0;

  // For join and duplicated value cases (Values is an internal data structure, for now in
  // join Values is vector<Value>)
  virtual std::vector<Value> findAll(const Key key) = 0;
  virtual std::vector<Value> findAll(const Key key, size_t hash_value) = 0;

  // For spill cases
  virtual bool erase(const Key key) = 0;
  virtual bool erase(const Key key, size_t hash_value) = 0;

  virtual bool contains(const Key key) = 0;
  virtual bool contains(const Key key, size_t hash_value) = 0;

  virtual std::size_t size() const = 0;

  // check and call rehash if needed
  virtual void reserve(size_t num_elements) = 0;

  virtual bool empty() const = 0;

  virtual void clear() = 0;
};
}  // namespace cider_hashtable