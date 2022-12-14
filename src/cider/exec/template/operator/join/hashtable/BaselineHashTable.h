/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#include "exec/template/CompilationOptions.h"
#include "exec/template/operator/join/hashtable/HashJoin.h"
#include "util/memory/Buffer/AbstractBuffer.h"
#include "util/memory/BufferProvider.h"

#include "exec/template/operator/join/hashtable/HashTable.h"

class BaselineHashTable : public HashTable {
 public:
  // CPU constructor
  BaselineHashTable(HashType layout,
                    const size_t entry_count,
                    const size_t emitted_keys_count,
                    const size_t hash_table_size)
      : cpu_hash_table_buff_size_(hash_table_size)
      , layout_(layout)
      , entry_count_(entry_count)
      , emitted_keys_count_(emitted_keys_count) {
    cpu_hash_table_buff_.reset(new int8_t[cpu_hash_table_buff_size_]);
  }

  BaselineHashTable(BufferProvider* buffer_provider,
                    HashType layout,
                    const size_t entry_count,
                    const size_t emitted_keys_count,
                    const size_t hash_table_size,
                    const size_t device_id)
      : layout_(layout)
      , entry_count_(entry_count)
      , emitted_keys_count_(emitted_keys_count) {
    UNREACHABLE();
  }

  ~BaselineHashTable() {}

  size_t getHashTableBufferSize() const override {
    return cpu_hash_table_buff_size_ *
           sizeof(decltype(cpu_hash_table_buff_)::element_type);
  }

  int8_t* getCpuBuffer() override {
    return reinterpret_cast<int8_t*>(cpu_hash_table_buff_.get());
  }

  HashType getLayout() const override { return layout_; }
  size_t getEntryCount() const override { return entry_count_; }
  size_t getEmittedKeysCount() const override { return emitted_keys_count_; }

 private:
  std::unique_ptr<int8_t[]> cpu_hash_table_buff_;
  size_t cpu_hash_table_buff_size_;
  HashType layout_;
  size_t entry_count_;         // number of keys in the hash table
  size_t emitted_keys_count_;  // number of keys emitted across all rows
};
