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

#include "cider/CiderException.h"
#include "exec/operator/join/BaseHashTable.h"
#include "exec/operator/join/CiderChainedHashTable.h"
#include "exec/operator/join/CiderLinearProbingHashTable.h"

namespace cider_hashtable {

// To be added
// This enum is used for cider internal only
// Outside cider will need to define their own enum
enum HashTableType { LINEAR_PROBING, CHAINED, FIXED_INT8, FIXED_INT16 };

template <typename Key,
          typename Value,
          typename Hash,
          typename KeyEqual,
          typename Grower,
          typename Allocator>
class HashTableSelector {
 public:
  // TODO: Define hashtable selection strategy in here
  HashTableType getHashTableTypeForJoin(std::string data_type,
                                        int initial_size,
                                        double ratio) {}

  template <typename... Args>
  std::unique_ptr<BaseHashTable<Key, Value, Hash, KeyEqual, Grower, Allocator>>
  createForJoin(HashTableType hashtable_type, Args&&... args);
};

}  // namespace cider_hashtable

// separate the implementations into cpp files instead of h file
// to isolate the implementation from codegen.
// use include cpp as a method to avoid maintaining too many template
// declaration in cpp file.
#include "exec/operator/join/HashTableSelector.cpp"
