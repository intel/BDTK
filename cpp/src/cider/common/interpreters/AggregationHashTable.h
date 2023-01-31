/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) 2016-2022 ClickHouse, Inc.
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

#include <common/Arena.h>
#include <common/hashtable/FixedHashMap.h>
#include <common/hashtable/HashMap.h>
#include <type/data/sqltypes.h>

#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_set>

#include "cider/CiderException.h"

namespace cider::hashtable {

using AggregateDataPtr = int8_t*;

// It's a wrap for null key, "base" should be one of the HashTables.
template <typename Base>
struct AggregatedHashTableWithNullKey : public Base {
  using Base::Base;

  bool hasNullKeyData() const { return has_null_key_data_; }
  const AggregateDataPtr& getNullKeyData() const { return null_key_data_; }
  size_t size() const { return Base::size() + (has_null_key_data_ ? 1 : 0); }
  bool empty() const { return Base::empty() && !has_null_key_data_; }
  void clear() {
    Base::clear();
    has_null_key_data_ = false;
  }
  void clearAndShrink() {
    Base::clearAndShrink();
    has_null_key_data_ = false;
  }

 private:
  bool has_null_key_data_ = false;
  AggregateDataPtr null_key_data_ = nullptr;
};

using AggregatedHashTableWithUInt8Key =
    FixedImplicitZeroHashMapWithCalculatedSize<uint8_t, AggregateDataPtr>;

using AggregatedHashTableWithUInt16Key =
    FixedImplicitZeroHashMapWithStoredSize<uint16_t, AggregateDataPtr>;

using AggregatedHashTableWithUInt32Key =
    HashMap<uint32_t, AggregateDataPtr, HashCRC32<uint32_t>>;

using AggregatedHashTableWithUInt64Key =
    HashMap<uint64_t, AggregateDataPtr, HashCRC32<uint64_t>>;

template <typename... Types>
using HashTableWithNullKey = AggregatedHashTableWithNullKey<HashMapTable<Types...>>;

using AggregatedHashTableWithNullableUInt8Key =
    AggregatedHashTableWithNullKey<AggregatedHashTableWithUInt8Key>;

using AggregatedHashTableWithNullableUInt16Key =
    AggregatedHashTableWithNullKey<AggregatedHashTableWithUInt16Key>;

class AggregationHashTable;

struct AggregationMethod : private boost::noncopyable {
  enum class Type {
    EMPTY = 0,
    SERIALIZED = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,
    without_key,
  };
  Type type = Type::EMPTY;

  AggregationMethod() {}

  ~AggregationMethod();
};

class AggKey {
 public:
  AggKey(bool is_null, int8_t* addr, uint32_t len) {
    this->is_null_ = is_null;
    this->addr_ = addr;
    this->len_ = len;
  }

  bool isNull() const { return is_null_; }

  int8_t* getAddr() const { return addr_; }

  uint32_t getLen() const { return len_; }

  const bool operator==(const AggKey& key) const {
    return this->is_null_ == key.isNull() &&
           std::memcmp(this->addr_, key.getAddr(), std::min(this->len_, key.getLen()));
  }

 private:
  bool is_null_;
  int8_t* addr_;
  uint32_t len_;
};

class AggregationHashTable final {
 public:
  // key_types: all key types
  // init_addr: initial value addr
  // init_len: initial value length
  // `init_addr` and `init_len` describe the init value of a value in HashTable.
  // The memory layout can of any kind and should be designed by users.
  AggregationHashTable(std::vector<SQLTypes> key_types, int8_t* addr, uint32_t len);

  // raw_key: Layout of keys should be aligned to 16 like below:
  // |<-- key1_isNUll -->|<-- pad_1 -->|<-- key1_values -->|<-- key2_isNull -->| .....
  // |<- 8bit ->|<- 8bit ->|<-- key1_values -->|<-- key2_isNull -->| .....
  // `keyn_values` will be like:
  // |<-- v1_int8 -->|<-- pad -->| or |<-- v1_int32 -->| or |<-- v1_bool -->|<-- pad -->|
  // |<- 8bit ->|<- 8bit ->| or |<--- 32bit  --->| or |<- 8bit ->|<- 8bit ->|
  // return: start position of value
  AggregateDataPtr get(int8_t* raw_key);

  AggregateDataPtr get(std::vector<AggKey> agg_keys);

  // key_addr: Same as `key_addr` in get
  // return: AggKey stored in HashTable
  // This function is to transfer keys formatted in codegen to AggKey in HashTable.
  // It will try to merge keys to a primitive type in multiple key cases.
  // If failed, serialize all keys to one key in Type::SERIALIZED.
  AggKey transferToAggKey(int8_t* key_addr);

  // Dump all value of the HashTable.
  // TODO(Deegue): Here need to be discussed, what to return?
  // std::vector<AggregateDataPtr> dump() {
  //   std::vector<AggregateDataPtr> res(key_set_.size());
  //   for (auto key : key_set_) {
  //     if (SQLTypes::kTINYINT == key_types_[0]) {
  //       int8_t key_v = (reinterpret_cast<int8_t*>(key.addr))[0];
  //       ;
  //       res.emplace(agg_ht_uint8_[key_v]);
  //     } else if (SQLTypes::kSMALLINT == key_types_[0]) {
  //       int16_t key_v = (reinterpret_cast<int16_t*>(key.addr))[0];
  //       ;
  //       res.emplace(agg_ht_uint16_[key_v]);
  //     }
  //   }
  //   return res;
  // }

 private:
  std::vector<SQLTypes> key_types_;
  int8_t* init_val_;
  uint32_t init_len_;
  // std::unordered_set<AggKey> key_set_;
  AggregationMethod::Type agg_method_;
  AggregatedHashTableWithUInt8Key agg_ht_uint8_;
  AggregatedHashTableWithUInt16Key agg_ht_uint16_;
  AggregatedHashTableWithUInt32Key agg_ht_uint32_;
  AggregatedHashTableWithUInt64Key agg_ht_uint64_;

  // Select the aggregation method based on the number and types of keys.
  AggregationMethod::Type chooseAggregationMethod();
};
}  // namespace cider::hashtable
