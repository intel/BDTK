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

#ifndef CIDER_CIDERAGGHASHTABLE_H
#define CIDER_CIDERAGGHASHTABLE_H

#include <cstddef>
#include <memory>
#include <vector>

#include "../thirdparty/robin-hood-hashing/src/include/robin_hood.h"
#include "CiderAggHashTableUtils.h"
#include "cider/CiderAllocator.h"
#include "cider/CiderTypes.h"
#include "function/hash/MurmurHash.h"
#include "type/data/sqltypes.h"
#include "util/CiderBitUtils.h"
#include "util/sqldefs.h"

#include "exec/template/common/descriptors/CountDistinctDescriptor.h"

extern bool g_bigint_count;

class RelAlgExecutionUnit;
class QueryMemoryDescriptor;

struct CiderAggHashTableEntryInfo {
  bool is_key;
  size_t slot_offset;
  SQLAgg agg_type;
  SQLTypeInfo sql_type_info;
  SQLTypeInfo arg_type_info;
  int64_t init_val;
  CountDistinctDescriptor count_distinct_desc;
};

// Represents a unique string value and its mapping to a small integer range for use as
// part of a normalized key or array index.
// TODO: use template to support other data types like Date etc.
class UStringVal {
 public:
  explicit UStringVal() {
    size_ = 0;
    data_ = 0;
  }

  explicit UStringVal(int64_t value) {
    size_ = sizeof(int64_t);
    data_ = value;
  }

  explicit UStringVal(const uint8_t* value, uint32_t size) {
    size_ = size;
    data_ = reinterpret_cast<uint64_t>(value);
  }

  uint64_t data() const { return data_; }

  template <typename T>
  void setData(T data) {
    data_ = reinterpret_cast<uint64_t>(data);
  }

  uint32_t size() const { return size_; }

  uint32_t id() const { return id_; }

  void setId(uint32_t id) { id_ = id; }

 private:
  uint64_t data_;
  uint32_t size_;
  uint32_t id_;
};

struct UStringValHash {
  size_t operator()(const UStringVal& usv) const {
    return robin_hood::hash_bytes(reinterpret_cast<char*>(usv.data()), usv.size());
  }
};

struct UStringValCmp {
  bool operator()(const UStringVal& lsv, const UStringVal& rsv) const {
    return lsv.size() == rsv.size() &&
           !std::memcmp(reinterpret_cast<uint8_t*>(lsv.data()),
                        reinterpret_cast<uint8_t*>(rsv.data()),
                        lsv.size());
  }
};

class CiderStringHasher {
 public:
  // id = 0 => empty string, id = -1 => null
  static constexpr int64_t kIdNullString = -1;
  static constexpr int64_t kIdEmptyString = 0;

  int64_t lookupIdByValue(CiderByteArray value);
  const CiderByteArray lookupValueById(int64_t id) const;

 private:
  // Stop counting distinct values after this threshold and revert to regular hash.
  static constexpr int32_t kMaxDistinct = 100'000;
  static constexpr uint32_t kStringBufferUnitSize = 1024;
  static constexpr uint64_t kMaxDistinctStringsBytes = 1 << 20;

  bool distinctOverflow_ = false;

  //   string
  // 		  ↓		insert
  // uStringVals   <----------------------------------
  //   		↓		not found       4 update string ptr    |
  // 1 generate an id                                |
  // 2 cache string ptr to stringCacheVec            |
  // 3 copy to contiguous memory uStringValStorage_	--
  robin_hood::unordered_flat_set<UStringVal, UStringValHash, UStringValCmp> uStringVals;
  std::vector<UStringVal> stringCacheVec;

  // Memory for unique string values.
  std::vector<std::string> uStringValStorage_;
  uint64_t distinctStringsBytes_ = 0;

  void copyStringToLocal(UStringVal* uString);
};

class CiderHasher {
 public:
  enum HashMode { kRangeHash, kDirectHash };
  struct kColumnInfo {
    const SQLTypes type;
    int64_t min;
    int64_t max;
    const int64_t null_val;
    bool need_rehash;
  };

  explicit CiderHasher(const std::vector<SQLTypeInfo>& key_types,
                       size_t key_size,
                       bool use_null_vector)
      : key_num_(key_types.size())
      , key_size_(key_size)
      , use_null_vector_(use_null_vector)
      , null_vector_slot_num_(
            use_null_vector ? (((key_num_ + 8 * key_size - 1) / (8 * key_size))) : 0)
      , mode_(CiderHasher::initMode(key_types))
      , need_rehash_(false)
      , cols_range_(kRangeHash == mode_ ? initColsRange(key_types)
                                        : std::vector<kColumnInfo>())
      , cols_range_ptr_(cols_range_.data()) {}

  template <typename KeyT>
  int64_t hash(const KeyT* keys) {
    const int64_t* key_vec = reinterpret_cast<const int64_t*>(keys);
    switch (mode_) {
      case kRangeHash:
        return rangeHash<KeyT>(key_vec);
      case kDirectHash:
        return directHash<KeyT>(key_vec);
    }
  }

  std::vector<kColumnInfo>& getKeyColumnInfo() { return cols_range_; }

  bool needRehash() const { return need_rehash_; }

  void setHashMode(HashMode mode) {
    mode_ = mode;
    need_rehash_ = false;
    for (auto& info : cols_range_) {
      info.need_rehash = false;
    }
  }

  uint64_t updateHashMode(const uint64_t entry_num_limit,
                          const uint64_t entry_num_limit_max);
  HashMode getHashMode() { return mode_; }

 private:
  bool updateHashKeyRange(kColumnInfo& col_info, const uint64_t entry_num_limit);

  template <typename KeyT>
  int64_t rangeHash(const int64_t* keys) {
    const KeyT* key_vec = reinterpret_cast<const KeyT*>(keys);
    const uint8_t* null_vec = reinterpret_cast<const uint8_t*>(key_vec + key_num_);
    int64_t ans = 0, multi_num = 1;

    for (size_t i = 0; i < key_num_; ++i) {
      if ((use_null_vector_ && CiderBitUtils::isBitSetAt(null_vec, i)) ||
          (!use_null_vector_ && key_vec[i] != cols_range_ptr_[i].null_val)) {
        int not_exceed = (key_vec[i] <= cols_range_ptr_[i].max) +
                         (key_vec[i] >= cols_range_ptr_[i].min);
        if (not_exceed == 2) {
          ans += multi_num * (key_vec[i] - cols_range_ptr_[i].min + 1);
        } else {
          cols_range_ptr_[i].max =
              key_vec[i] > cols_range_ptr_[i].max ? key_vec[i] : cols_range_ptr_[i].max;
          cols_range_ptr_[i].min =
              key_vec[i] < cols_range_ptr_[i].min ? key_vec[i] : cols_range_ptr_[i].min;
          need_rehash_ = true;
          cols_range_ptr_[i].need_rehash = true;
        }
      }
      multi_num *= cols_range_ptr_[i].max - cols_range_ptr_[i].min + 2;
    }
    return ans;
  }

  template <typename KeyT>
  int64_t directHash(const int64_t* keys) {
    return MurmurHash3(keys, sizeof(KeyT) * (key_num_ + null_vector_slot_num_), 0);
  }

  // Initialize kColumnInfo for each key columns to decide hash mode
  static std::vector<kColumnInfo> initColsRange(
      const std::vector<SQLTypeInfo>& key_types);

  static HashMode initMode(const std::vector<SQLTypeInfo>& key_types);
  static bool supportRangeHash(SQLTypeInfo key_type);

  const size_t key_num_;
  const size_t key_size_;
  const bool use_null_vector_;
  const size_t null_vector_slot_num_;
  HashMode mode_;
  bool need_rehash_;
  std::vector<kColumnInfo> cols_range_;
  kColumnInfo* cols_range_ptr_;
};

class CiderAggHashTable {
 public:
  friend class CiderAggHashTableRowIterator;

  CiderAggHashTable(const std::unique_ptr<QueryMemoryDescriptor>& query_mem_desc,
                    const std::shared_ptr<RelAlgExecutionUnit>& rel_alg_exec_unit,
                    std::shared_ptr<CiderAllocator> allocator,
                    size_t buffer_memory_limit = 16777216,  // 16M
                    bool force_direct_hash = false,
                    size_t buffers_num = 1);
  virtual ~CiderAggHashTable();
  void resetBuffer(size_t buffer_index);

  int8_t** getBuffersPtr() { return buffer_memory_.data(); }

  int8_t* getBuffersPtrAt(size_t index) { return buffer_memory_[index]; }
  uint8_t* getBufferEmptyMapAt(size_t index) {
    return buffer_empty_map_[index].as<uint8_t>();
  }

  size_t getBufferNum() const { return buffers_num_; }

  CiderAggHashTableBufferRuntimeState& getRuntimeStateAt(size_t index) {
    return runtime_state_[index];
  }

  const CiderAggHashTableBufferRuntimeState& getRuntimeStateAt(size_t index) const {
    return runtime_state_[index];
  }

  size_t getBufferWidth() const { return buffer_width_; }
  size_t getSlotWidth() const { return slot_width_; }
  size_t getActualDataWidth(size_t column_index) const;
  const CiderAggHashTableEntryInfo& getColEntryInfo(size_t column_index) const {
    CHECK_LT(column_index, columns_num_);
    return cols_info_[column_index];
  }
  size_t getKeyColNum() const { return key_columns_num_; }
  size_t getTargetColNum() const { return columns_num_ - key_columns_num_; }
  size_t getColNum() const { return columns_num_; }
  size_t getKeyNullVectorOffset() const { return group_key_null_offset_; }
  size_t getTargetNullVectorOffset() const { return group_target_null_offset_; }

  const std::vector<size_t>& getTargetIndexMap() const { return target_index_map_; }
  CiderHasher& getHasher() { return hasher_; }
  const CiderHasher& getHasher() const { return hasher_; }
  CiderStringHasher& getStringHasher() { return stringHasher_; }
  const CiderStringHasher& getStringHasher() const { return stringHasher_; }

  CiderAggHashTableRowIteratorPtr getRowIterator(size_t buffer_index);
  std::string toString() const;

  int64_t* NEVER_INLINE getGroupTargetPtr(const int64_t* keys);

  bool rehash();

 private:
  std::vector<CiderAggHashTableEntryInfo> fillColsInfo();
  std::vector<int8_t> fillRowData();
  int8_t* allocateBufferAt(size_t buffer_id);
  void freeBufferAt(size_t buffer_id);
  size_t getNextRowIndex(const int8_t* buffer_ptr,
                         const uint8_t* empty_map_ptr,
                         const size_t start_row_index) const;

  template <typename T>
  size_t getNextRowIndexColumnar(const int8_t* buffer_ptr,
                                 const size_t start_row_index) const;
  size_t getNextRowIndexRow(const int8_t* buffer_ptr,
                            const uint8_t* empty_map_ptr,
                            const size_t start_row_index) const;

  template <CiderHasher::HashMode mode,
            typename KeyT,
            std::enable_if_t<mode == CiderHasher::kRangeHash, bool> = true>
  int64_t* getGroupTargetPtrImpl(const KeyT* keys);

  template <CiderHasher::HashMode mode,
            typename KeyT,
            std::enable_if_t<mode == CiderHasher::kDirectHash, bool> = true>
  int64_t* getGroupTargetPtrImpl(const KeyT* keys);

  template <typename KeyT>
  int64_t* getGroupTargetPtrImpl(const int64_t* keys);

  void fillTargetIndexMap();

  void initCountDistinctInBuffer(size_t buffer_index);

  std::vector<SQLTypeInfo> getKeyTypeInfo();

  void updateBufferCapacity(size_t memory_usage_upper);

  size_t buffer_entry_num_;          // number of entries per buffer
  size_t buffers_num_;               // number of buffers
  size_t effective_key_slot_width_;  // effective width of key
  size_t slot_width_;                // width of slot (byte)
  size_t columns_num_;               // number of columns
  size_t key_columns_num_;           // number of key columns
  size_t row_width_;
  size_t buffer_width_;
  size_t buffer_memory_limit_;

  QueryMemoryDescriptor* query_mem_desc_;
  RelAlgExecutionUnit* rel_alg_exec_unit_;
  std::vector<int8_t*> buffer_memory_;
  std::vector<CiderBitUtils::CiderBitVector<>> buffer_empty_map_;
  std::vector<CiderAggHashTableBufferRuntimeState> runtime_state_;
  std::vector<CiderAggHashTableEntryInfo> cols_info_;
  std::vector<int8_t> initial_row_data_;
  std::vector<size_t> target_index_map_;

  std::shared_ptr<CiderAllocator> allocator_;

  CiderHasher hasher_;
  CiderStringHasher stringHasher_;

  size_t group_key_null_offset_;
  size_t group_target_offset_;
  size_t group_target_null_offset_;
};

#endif
