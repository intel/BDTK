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

#include <folly/FBVector.h>
#include <folly/container/F14Map.h>

// used as test
template <typename Key, typename Value>
class DuplicateF14map {
 public:
  DuplicateF14map(){};
  folly::fbvector<Value> lookup(Key key) {
    auto iter = f14_map_.find(key);
    if (iter != f14_map_.end()) {
      return iter->second;
    }
    folly::fbvector<Value> empty_res;
    return empty_res;
  }

  void insert(Key&& key, Value&& value) {
    if (f14_map_.count(key) == 0) {
      f14_map_[key] = folly::fbvector<Value>{value};
    } else {
      auto tmp_value = &f14_map_[key];
      tmp_value->push_back(value);
    }
  }
  folly::F14FastMap<Key, folly::fbvector<Value>>& getMap() { return f14_map_; }

 private:
  folly::F14FastMap<Key, folly::fbvector<Value>> f14_map_;
};