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

#include "common/interpreters/AggregationHashTable.h"

namespace cider::hashtable {

HashTableAllocator allocator;

// key_types: all key types
// init_addr: initial value addr
// init_len: initial value length
// `init_addr` and `init_len` describe the init value of a value in HashTable.
// The memory layout can of any kind and should be designed by users.
AggregationHashTable::AggregationHashTable(std::vector<SQLTypes> key_types,
                                           int8_t* addr,
                                           uint32_t len)
    : key_types_(key_types), init_val_(addr), init_len_(len) {
  // TODO(Deegue): use agg_method to construct the specific HashTable instead of all
  agg_method_ = chooseAggregationMethod();
}

// raw_key: Layout of keys should be aligned to 16 like below:
// |<-- key1_isNUll -->|<-- pad_1 -->|<-- key1_values -->|<-- key2_isNull -->| .....
// |<- 8bit ->|<- 8bit ->|<-- key1_values -->|<-- key2_isNull -->| .....
// `keyn_values` will be like:
// |<-- v1_int8 -->|<-- pad -->| or |<-- v1_int32 -->| or |<-- v1_bool -->|<-- pad -->|
// |<- 8bit ->|<- 8bit ->| or |<--- 32bit  --->| or |<- 8bit ->|<- 8bit ->|
// return: start position of value
AggregateDataPtr AggregationHashTable::get(int8_t* raw_key) {
  // Transfer all keys to one AggKey
  AggKey key = transferToAggKey(raw_key);
  // key_set_.emplace(key);

  for (int i = 0; i < key_types_.size(); i++) {
    if (SQLTypes::kTINYINT == key_types_[i]) {
      uint8_t key_v = (reinterpret_cast<uint8_t*>(key.getAddr()))[0];
      if (agg_ht_uint8_[key_v] == nullptr) {
        // Allocate memory of values here since value type like non-fixed length address
        // cannot be new in hash table. This case should be manually handled and it's
        // better to use an Arena for better memory efficiency.
        agg_ht_uint8_[key_v] = allocator.allocate(init_len_);
        std::memcpy(agg_ht_uint8_[key_v], init_val_, init_len_);
      }
      return agg_ht_uint8_[key_v];
    } else if (SQLTypes::kSMALLINT == key_types_[i]) {
      uint16_t key_v = (reinterpret_cast<uint16_t*>(key.getAddr()))[0];
      if (agg_ht_uint16_[key_v] == nullptr) {
        // Allocate memory of values here since value type like non-fixed length address
        // cannot be new in hash table. This case should be manually handled and it's
        // better to use an Arena for better memory efficiency.
        agg_ht_uint16_[key_v] = allocator.allocate(init_len_);
        std::memcpy(agg_ht_uint16_[key_v], init_val_, init_len_);
      }
      return agg_ht_uint16_[key_v];
    } else if (SQLTypes::kINT == key_types_[i]) {
      uint32_t key_v = (reinterpret_cast<uint32_t*>(key.getAddr()))[0];
      if (agg_ht_uint32_[key_v] == nullptr) {
        agg_ht_uint32_[key_v] = allocator.allocate(init_len_);
        std::memcpy(agg_ht_uint32_[key_v], init_val_, init_len_);
      }
      return agg_ht_uint32_[key_v];
    } else if (SQLTypes::kBIGINT == key_types_[i]) {
      uint64_t key_v = (reinterpret_cast<uint64_t*>(key.getAddr()))[0];
      if (agg_ht_uint64_[key_v] == nullptr) {
        agg_ht_uint64_[key_v] = allocator.allocate(init_len_);
        std::memcpy(agg_ht_uint64_[key_v], init_val_, init_len_);
      }
      return agg_ht_uint64_[key_v];
    } else if (SQLTypes::kFLOAT == key_types_[i]) {
      float key_v = (reinterpret_cast<float*>(key.getAddr()))[0];
      if (agg_ht_float_[key_v] == nullptr) {
        agg_ht_float_[key_v] = allocator.allocate(init_len_);
        std::memcpy(agg_ht_float_[key_v], init_val_, init_len_);
      }
      return agg_ht_float_[key_v];
    } else if (SQLTypes::kDOUBLE == key_types_[i]) {
      double key_v = (reinterpret_cast<double*>(key.getAddr()))[0];
      if (agg_ht_double_[key_v] == nullptr) {
        agg_ht_double_[key_v] = allocator.allocate(init_len_);
        std::memcpy(agg_ht_double_[key_v], init_val_, init_len_);
      }
      return agg_ht_double_[key_v];
    }
  }
  CIDER_THROW(CiderRuntimeException, "Unsupported key type");
}

AggregateDataPtr AggregationHashTable::get(std::vector<AggKey> agg_keys) {
  CIDER_THROW(CiderRuntimeException, "Unsupported key type");
}

// key_addr: Same as `key_addr` in get
// return: AggKey stored in HashTable
// This function is to transfer keys formatted in codegen to AggKey in HashTable.
// It will try to merge keys to a primitive type in multiple key cases.
// If failed, serialize all keys to one key in Type::SERIALIZED.
AggKey AggregationHashTable::transferToAggKey(int8_t* key_addr) {
  // Single key
  if (1 == key_types_.size()) {
    if (SQLTypes::kTINYINT == key_types_[0]) {
      bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
      AggKey key(is_null, key_addr + 2, 1);
      return key;
    } else if (SQLTypes::kSMALLINT == key_types_[0]) {
      bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
      AggKey key(is_null, key_addr + 2, 2);
      return key;
    } else if (SQLTypes::kINT == key_types_[0]) {
      bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
      AggKey key(is_null, key_addr + 2, 4);
      return key;
    } else if (SQLTypes::kBIGINT == key_types_[0]) {
      bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
      AggKey key(is_null, key_addr + 2, 8);
      return key;
    } else if (SQLTypes::kFLOAT == key_types_[0]) {
      bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
      AggKey key(is_null, key_addr + 2, 4);
      return key;
    } else if (SQLTypes::kDOUBLE == key_types_[0]) {
      bool is_null = (reinterpret_cast<bool*>(key_addr))[0];
      AggKey key(is_null, key_addr + 2, 8);
      return key;
    }
    // TODO(Deegue): Support more types.
  }
  CIDER_THROW(CiderRuntimeException, "Unsupported Aggregation key");
  // TODO(Deegue): Multiple keys, find out if keys can be arranged to primitive types like
  // int32/int64... If not, serialize the key and set the key type to Type::serialized.
}

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

// Select the aggregation method based on the number and types of keys.
AggregationMethod::Type AggregationHashTable::chooseAggregationMethod() {
  // Single key
  if (1 == key_types_.size()) {
    if (SQLTypes::kTINYINT == key_types_[0]) {
      return AggregationMethod::Type::INT8;
    } else if (SQLTypes::kSMALLINT == key_types_[0]) {
      return AggregationMethod::Type::INT16;
    } else if (SQLTypes::kINT == key_types_[0]) {
      return AggregationMethod::Type::INT32;
    } else if (SQLTypes::kBIGINT == key_types_[0]) {
      return AggregationMethod::Type::INT64;
    } else if (SQLTypes::kFLOAT == key_types_[0]) {
      return AggregationMethod::Type::FLOAT;
    } else if (SQLTypes::kDOUBLE == key_types_[0]) {
      return AggregationMethod::Type::DOUBLE;
    }
    // TODO(Deegue): Support more types.
  }
  // TODO(Deegue): Support multiple keys.
  return AggregationMethod::Type::EMPTY;
}
}  // namespace cider::hashtable
