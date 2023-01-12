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

#include <memory>
#include <vector>

#include "exec/template/operator/join/hashtable/HashTable.h"

class PerfectHashTable : public HashTable {
 public:
  // CPU constructor
  PerfectHashTable(BufferProvider* buffer_provider,
                   const HashType layout,
                   const size_t entry_count,
                   const size_t emitted_keys_count)
      : buffer_provider_(buffer_provider)
      , layout_(layout)
      , entry_count_(entry_count)
      , emitted_keys_count_(emitted_keys_count) {
    cpu_hash_table_buff_size_ = layout_ == HashType::OneToOne
                                    ? entry_count_
                                    : 2 * entry_count_ + emitted_keys_count_;
    cpu_hash_table_buff_.reset(new int32_t[cpu_hash_table_buff_size_]);
  }

  ~PerfectHashTable() {}

  size_t getHashTableBufferSize() const override {
    return cpu_hash_table_buff_size_ *
           sizeof(decltype(cpu_hash_table_buff_)::element_type);
  }

  HashType getLayout() const override { return layout_; }

  int8_t* getCpuBuffer() override {
    return reinterpret_cast<int8_t*>(cpu_hash_table_buff_.get());
  }

  size_t getEntryCount() const override { return entry_count_; }

  size_t getEmittedKeysCount() const override { return emitted_keys_count_; }

 private:
  BufferProvider* buffer_provider_;
  std::unique_ptr<int32_t[]> cpu_hash_table_buff_;
  size_t cpu_hash_table_buff_size_;

  HashType layout_;
  size_t entry_count_;         // number of keys in the hash table
  size_t emitted_keys_count_;  // number of keys emitted across all rows
};
