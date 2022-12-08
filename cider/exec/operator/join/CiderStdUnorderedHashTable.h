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
// used as test
template <typename Key, typename Value>
class DuplicateStdHashmap {
 public:
  DuplicateStdHashmap(){};
  std::vector<Value> lookup(Key key) {
    auto iter = umap_.find(key);
    if (iter != umap_.end()) {
      return iter->second;
    }
    std::vector<Value> empty_res;
    return empty_res;
  }

  void insert(Key&& key, Value&& value) {
    if (umap_.count(key) == 0) {
      umap_[key] = std::vector<Value>{value};
    } else {
      auto tmp_value = &umap_[key];
      tmp_value->push_back(value);
    }
  }
  std::unordered_map<Key, std::vector<Value>>& getMap() { return umap_; }

 private:
  std::unordered_map<Key, std::vector<Value>> umap_;
};