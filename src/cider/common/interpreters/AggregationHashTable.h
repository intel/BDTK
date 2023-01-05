/*
 * Copyright (c) 2022 Intel Corporation.
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

namespace cider {

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

template <typename... Types>
using HashTableWithNullKey = AggregatedHashTableWithNullKey<HashMapTable<Types...>>;

using AggregatedHashTableWithNullableUInt8Key =
    AggregatedHashTableWithNullKey<AggregatedHashTableWithUInt8Key>;

using AggregatedDataWithNullableUInt16Key =
    AggregatedHashTableWithNullKey<AggregatedHashTableWithUInt16Key>;

class AggregationHashTable;
HashTableAllocator allocator;

struct AggregationMethod : private boost::noncopyable {
  enum class Type {
    EMPTY = 0,
    INT8 = 1,
    INT16 = 2,
    SERIALIZED = 3,
    without_key,
  };
  Type type = Type::EMPTY;

  AggregationMethod() {}

  ~AggregationMethod();
};

class AggKey {
 public:
  bool is_null;
  int8_t* addr;
  uint32_t len;

  AggKey(bool is_null, int8_t* addr, uint32_t len) {
    this->is_null = is_null;
    this->addr = addr;
    this->len = len;
  }

  bool isNull() const { return is_null; }

  int8_t* getAddr() const { return addr; }

  uint32_t getLen() const { return len; }

  const bool operator==(const AggKey& key) const {
    return this->is_null == key.is_null &&
           std::memcmp(this->addr, key.addr, std::min(this->len, key.len));
  }
};

class AggregationHashTable final {
 public:
  // key_types: all key types
  // init_addr: initial value addr
  // init_len: initial value length
  // `init_addr` and `init_len` describe the init value of a value in HashTable.
  // The memory layout can of any kind and should be designed by users.
  explicit AggregationHashTable(std::vector<SQLTypes> key_types,
                                int8_t* addr,
                                uint32_t len)
      : key_types_(key_types), init_val_(addr), init_len_(len) {
    // TODO: use agg_method to construct the specific HashTable instead of all
    agg_method_ = chooseAggregationMethod();
  }

  // raw_key: Layout of keys should be aligned to 16 like below:
  // |<-- key1_isNUll -->|<-- pad_1 -->|<-- key1_values -->|<-- key2_isNull -->| .....
  // |<- 8bit ->|<- 8bit ->|<-- key1_values -->|<-- key2_isNull -->| .....
  // `keyn_values` will be like:
  // |<-- v1_int8 -->|<-- pad -->| or |<-- v1_int32 -->| or |<-- v1_bool -->|<-- pad -->|
  // |<- 8bit ->|<- 8bit ->| or |<--- 32bit  --->| or |<- 8bit ->|<- 8bit ->|
  // return: start position of value
  AggregateDataPtr get(int8_t* raw_key) {
    std::vector<AggKey> keys = transferToAggKey(raw_key);
    // key_set_.emplace(key);

    for (int i = 0; i < key_types_.size(); i++) {
      if (SQLTypes::kTINYINT == key_types_[i]) {
        uint8_t key_v = (reinterpret_cast<uint8_t*>(keys[0].addr))[0];
        if (agg_ht_uint8_[key_v] == nullptr) {
          // Allocate memory of values here since value type like non-fixed length address
          // cannot be new in hash table. This case should be manually handled and it's
          // better to use an Arena for better memory efficiency.
          agg_ht_uint8_[key_v] = allocator.allocate(init_len_);
          std::memcpy(agg_ht_uint8_[key_v], init_val_, init_len_);
        }
        return agg_ht_uint8_[key_v];
      }
      if (SQLTypes::kSMALLINT == key_types_[i]) {
        uint16_t key_v = (reinterpret_cast<uint16_t*>(keys[0].addr))[0];
        if (agg_ht_uint16_[key_v] == nullptr) {
          // Allocate memory of values here since value type like non-fixed length address
          // cannot be new in hash table. This case should be manually handled and it's
          // better to use an Arena for better memory efficiency.
          agg_ht_uint16_[key_v] = allocator.allocate(init_len_);
          std::memcpy(agg_ht_uint16_[key_v], init_val_, init_len_);
        }
        return agg_ht_uint16_[key_v];
      }
    }
    CIDER_THROW(CiderRuntimeException, "Unsupported key type");
  }

  AggregateDataPtr get(std::vector<AggKey> agg_keys) {
    CIDER_THROW(CiderRuntimeException, "Unsupported key type");
  }

  // key_addr: Same as `key_addr` in get
  // return: AggKey stored in HashTable
  // This function is to transfer keys formatted in codegen to AggKey in HashTable.
  // It will try to merge keys to a primitive type in multiple key cases.
  // If failed, serialize all keys to one key in Type::SERIALIZED.
  std::vector<AggKey> transferToAggKey(int8_t* key_addr) {
    // Single key
    if (1 == key_types_.size()) {
      if (SQLTypes::kTINYINT == key_types_[0]) {
        bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
        AggKey key(is_null, key_addr + 2, 1);
        return std::vector<AggKey>{key};
      }
      if (SQLTypes::kSMALLINT == key_types_[0]) {
        bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
        AggKey key(is_null, key_addr + 2, 2);
        return std::vector<AggKey>{key};
      }
      // TODO: Support more types.
    }
    CIDER_THROW(CiderRuntimeException, "Unsupported Aggregation key");
    // TODO: Multiple keys, find out if keys can be arranged to primitive types like
    // int32/int64... If not, serialize the key and set the key type to Type::serialized.
  }

  // Dump all value of the HashTable.
  // TODO: Here need to be discussed, what to return?
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

  // Select the aggregation method based on the number and types of keys.
  AggregationMethod::Type chooseAggregationMethod() {
    // Single key
    if (1 == key_types_.size()) {
      if (SQLTypes::kTINYINT == key_types_[0]) {
        return AggregationMethod::Type::INT8;
      }
      if (SQLTypes::kSMALLINT == key_types_[0]) {
        return AggregationMethod::Type::INT16;
      }
      // TODO: Support more types.
    }
    // TODO: Support multiple keys.
    return AggregationMethod::Type::EMPTY;
  }
};
}  // namespace cider
