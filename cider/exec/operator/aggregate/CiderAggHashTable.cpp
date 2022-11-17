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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "exec/template/CompilationOptions.h"
#include "exec/template/OutputBufferInitialization.h"
#include "exec/template/RelAlgExecutionUnit.h"
#include "exec/template/common/descriptors/QueryMemoryDescriptor.h"
#include "function/scalar/RuntimeFunctions.h"

#include "exec/operator/aggregate/CiderAggHashTable.h"

std::vector<CiderHasher::kColumnInfo> CiderHasher::initColsRange(
    const std::vector<SQLTypeInfo>& key_types) {
  std::vector<CiderHasher::kColumnInfo> ans;
  ans.reserve(key_types.size());

  for (auto type_info : key_types) {
    switch (type_info.get_type()) {
      case kBOOLEAN:
        ans.push_back({kBOOLEAN, 0, -1, inline_int_null_value<int8_t>(), false});
        break;
      case kTINYINT:
        ans.push_back({kTINYINT, 0, -1, inline_int_null_value<int8_t>(), false});
        break;
      case kSMALLINT:
        ans.push_back({kSMALLINT, 0, -1, inline_int_null_value<int16_t>(), false});
        break;
      case kINT:
        ans.push_back({kINT, 0, -1, inline_int_null_value<int32_t>(), false});
        break;
      case kTEXT:
      case kVARCHAR:
      case kDECIMAL:
      case kBIGINT:
        ans.push_back({kBIGINT, 0, -1, inline_int_null_value<int64_t>(), false});
        break;
      default:
        CIDER_THROW(CiderUnsupportedException,
                    fmt::format("type name is {}", type_info.get_type_name()));
    }
  }

  return ans;
}

CiderHasher::HashMode CiderHasher::initMode(const std::vector<SQLTypeInfo>& key_types) {
  for (auto type_info : key_types) {
    if (!CiderHasher::supportRangeHash(type_info)) {
      return kDirectHash;
    }
  }
  return kRangeHash;
}

bool CiderHasher::supportRangeHash(SQLTypeInfo key_type) {
  switch (key_type.get_type()) {
    case kBOOLEAN:
    case kTEXT:
    case kVARCHAR:
    case kDECIMAL:
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT:
      return true;
    default:
      return false;
  }
}

bool CiderHasher::updateHashKeyRange(CiderHasher::kColumnInfo& col_info,
                                     const uint64_t entry_num_limit) {
  int64_t range;
  if (__builtin_sub_overflow(col_info.max, col_info.min, &range) ||
      range > entry_num_limit - 2) {
    return false;
  }
  ++range;

  int64_t reserve = (range >> 2);
  if (get_min_value(col_info.type) + reserve > col_info.min) {
    col_info.min = get_min_value(col_info.type);
  } else {
    col_info.min -= reserve;
  }

  if (get_max_value(col_info.type) - reserve < col_info.max) {
    col_info.max = get_max_value(col_info.type);
  } else {
    col_info.max += reserve;
  }
  return true;
}

// Given current entry num limit and maximun entry num limit to decide new hash mode and
// returns new entry num limit.
uint64_t CiderHasher::updateHashMode(const uint64_t entry_num_limit,
                                     const uint64_t entry_num_limit_max) {
  if (getHashMode() == kDirectHash) {
    return ((entry_num_limit_max >> 1) < entry_num_limit) ? entry_num_limit_max
                                                          : (entry_num_limit << 1);
  }

  if (getHashMode() == kRangeHash) {
    auto& range_info = getKeyColumnInfo();
    uint64_t expect_entry_num = 1;
    for (auto& info : range_info) {
      if (info.need_rehash) {
        // Update estimated range
        if (!updateHashKeyRange(info, entry_num_limit_max)) {
          setHashMode(kDirectHash);
          return entry_num_limit_max;
        }
      }

      int64_t range;
      if (__builtin_sub_overflow(info.max, info.min, &range) ||
          range > entry_num_limit_max - 2) {
        setHashMode(CiderHasher::kDirectHash);
        return entry_num_limit_max;
      }

      range += 2;  // One entry for null.
      if (__builtin_mul_overflow(expect_entry_num, range, &expect_entry_num)) {
        setHashMode(kDirectHash);
        return entry_num_limit_max;
      }
    }

    if (expect_entry_num > entry_num_limit_max) {
      setHashMode(CiderHasher::kDirectHash);
      return entry_num_limit_max;
    } else {
      setHashMode(CiderHasher::kRangeHash);
      return expect_entry_num;
    }
  }

  setHashMode(CiderHasher::kDirectHash);
  return entry_num_limit_max;
}

void CiderStringHasher::copyStringToLocal(UStringVal* uString) {
  auto size = uString->size();
  if (distinctStringsBytes_ > kMaxDistinctStringsBytes) {
    distinctOverflow_ = true;
    return;
  }
  if (size > kStringBufferUnitSize) {
    CIDER_THROW(CiderCompileException,
                fmt::format("Failed to store a string of lenth {}", size));
  }
  if (uStringValStorage_.empty()) {
    uStringValStorage_.emplace_back();
    uStringValStorage_.back().reserve(std::max(kStringBufferUnitSize, size));
    distinctStringsBytes_ += uStringValStorage_.back().capacity();
  }
  auto str = &uStringValStorage_.back();
  if (str->size() + size > str->capacity()) {
    uStringValStorage_.emplace_back();
    uStringValStorage_.back().reserve(std::max(kStringBufferUnitSize, size));
    distinctStringsBytes_ += uStringValStorage_.back().capacity();
    str = &uStringValStorage_.back();
  }
  auto start = str->size();
  str->resize(start + size);
  std::memcpy(str->data() + start, reinterpret_cast<char*>(uString->data()), size);
  uString->setData(str->data() + start);
}

int64_t CiderStringHasher::lookupIdByValue(CiderByteArray value) {
  if (!value.ptr) {
    return 0;
  }
  if (!distinctOverflow_) {
    UStringVal uString(value.ptr, value.len);
    auto id = uStringVals.size() + 1;
    uString.setId(id);
    auto pair = uStringVals.insert(uString);
    if (pair.second) {
      if (uStringVals.size() > kMaxDistinct) {
        distinctOverflow_ = true;
        CIDER_THROW(CiderRuntimeException, "Overflow in distinct string hash set.");
      }
      copyStringToLocal(&*pair.first);
      stringCacheVec.emplace_back(*pair.first);
    }
    return uStringVals.find(uString)->id();
  } else {
    CIDER_THROW(CiderRuntimeException, "Overflow in distinct string hash set.");
  }
  return -1;
}

const CiderByteArray CiderStringHasher::lookupValueById(int64_t id) const {
  if (0 == id || -1 == id) {
    return {0, nullptr};
  }
  UStringVal uString = stringCacheVec[id - 1];
  CiderByteArray value(uString.size(), reinterpret_cast<uint8_t*>(uString.data()));
  return value;
}

CiderAggHashTable::CiderAggHashTable(
    const std::unique_ptr<QueryMemoryDescriptor>& query_mem_desc,
    const std::shared_ptr<RelAlgExecutionUnit>& rel_alg_exec_unit,
    std::shared_ptr<CiderAllocator> allocator,
    size_t buffer_memory_limit,
    bool force_direct_hash,
    size_t buffers_num)
    : buffer_entry_num_(0)
    , buffers_num_(buffers_num)
    , effective_key_slot_width_(query_mem_desc->getEffectiveKeyWidth())
    , slot_width_(query_mem_desc->getCompactByteWidth()
                      ? query_mem_desc->getCompactByteWidth()
                      : query_mem_desc->getEffectiveKeyWidth())
    , columns_num_(query_mem_desc->hasKeylessHash()
                       ? query_mem_desc->getSlotCount()
                       : (query_mem_desc->getGroupbyColCount() +
                          query_mem_desc->getBufferColSlotCount()))
    , key_columns_num_(query_mem_desc->getKeyCount())
    , row_width_(query_mem_desc->getRowSize())
    , buffer_memory_limit_(buffer_memory_limit)
    , query_mem_desc_(query_mem_desc.get())
    , rel_alg_exec_unit_(rel_alg_exec_unit.get())
    , buffer_memory_(buffers_num, nullptr)
    , buffer_empty_map_(buffers_num, CiderBitUtils::CiderBitVector<>(allocator, 0))
    , runtime_state_(
          buffers_num_,
          {query_mem_desc->getEntryCount()})  // TODO: Fix Runtime State initialization.
    , cols_info_(fillColsInfo())
    , initial_row_data_(row_width_, 0)
    , target_index_map_(query_mem_desc->getSlotCount())
    , allocator_(allocator)
    , hasher_(getKeyTypeInfo(),
              effective_key_slot_width_,
              query_mem_desc->useCiderDataFormat()) {
  buffer_memory_limit_ >>= 3;
  buffer_memory_limit_ <<= 3;

  if (query_mem_desc_->useCiderDataFormat()) {
    initial_row_data_ = fillRowData();
  }

  fillTargetIndexMap();

  // Initialization of initial row data vector.
  const auto row_data_ptr = initial_row_data_.data();
  for (const auto& col_info : cols_info_) {
    std::memcpy(row_data_ptr + col_info.slot_offset, &col_info.init_val, slot_width_);
  }

  if (force_direct_hash || hasher_.getHashMode() == CiderHasher::kDirectHash) {
    hasher_.setHashMode(CiderHasher::kDirectHash);
    updateBufferCapacity(buffer_memory_limit_);  // TODO: Memory efficient
    for (size_t i = 0; i < buffers_num; ++i) {
      allocateBufferAt(i);
      resetBuffer(i);
      initCountDistinctInBuffer(i);
    }
  } else if (hasher_.getHashMode() == CiderHasher::kRangeHash) {
    updateBufferCapacity(row_width_ + 7);  // Allocate memory for null
    for (size_t i = 0; i < buffers_num; ++i) {
      allocateBufferAt(i);
      resetBuffer(i);
      initCountDistinctInBuffer(i);
    }
  }
}

CiderAggHashTable::~CiderAggHashTable() {
  for (size_t i = 0; i < buffers_num_; ++i) {
    freeBufferAt(i);
  }
}

std::vector<CiderAggHashTableEntryInfo> CiderAggHashTable::fillColsInfo() {
  std::vector<CiderAggHashTableEntryInfo> cols_info(columns_num_);

  size_t offset = 0, col_slot_context_index = 0;
  bool is_key = true;
  auto groupby_expr_iter = rel_alg_exec_unit_->groupby_exprs.begin();
  auto target_expr_iter = rel_alg_exec_unit_->target_exprs.begin();

  auto init_vals_vec =
      init_agg_val_vec(rel_alg_exec_unit_->target_exprs, {}, *query_mem_desc_);
  CHECK_EQ(init_vals_vec.size(), columns_num_ - key_columns_num_);
  auto init_vals_vec_iter = init_vals_vec.begin();

  for (size_t i = 0; i < columns_num_; ++i) {
    cols_info[i].count_distinct_desc =
        i < key_columns_num_
            ? CountDistinctDescriptor{CountDistinctImplType::Invalid,
                                      SQLTypes::kNULLT,
                                      0,
                                      0,
                                      false,
                                      0}
            : query_mem_desc_->getCountDistinctDescriptor(i - key_columns_num_);
    cols_info[i].is_key = is_key;
    cols_info[i].slot_offset = offset;

    if (is_key) {
      cols_info[i].agg_type = kSINGLE_VALUE;
      cols_info[i].init_val = 0;

      auto expr = groupby_expr_iter->get();
      CHECK(expr);
      cols_info[i].sql_type_info = expr->get_type_info();
      cols_info[i].arg_type_info = cols_info[i].sql_type_info;

      offset += effective_key_slot_width_;
      ++groupby_expr_iter;

      if ((i + 1) == key_columns_num_) {
        is_key = false;
        if (query_mem_desc_->useCiderDataFormat()) {
          group_key_null_offset_ = offset;
          CHECK_EQ(group_key_null_offset_,
                   query_mem_desc_->getNullVectorOffsetOfGroupKeys());

          offset += ((key_columns_num_ + 7) >> 3);
        }
        offset = align_to_int64(offset);
        group_target_offset_ = offset;
      }
    } else {
      while (0 == query_mem_desc_->getPaddedSlotWidthBytes(col_slot_context_index)) {
        ++col_slot_context_index;
        ++target_expr_iter;
      }

      cols_info[i].sql_type_info = (*target_expr_iter)->get_type_info();
      cols_info[i].init_val = *init_vals_vec_iter;

      offset += slot_width_;

      auto expr = dynamic_cast<Analyzer::AggExpr*>(*target_expr_iter);
      if (expr) {
        cols_info[i].agg_type = expr->get_aggtype();
        if (auto arg = expr->get_arg()) {
          cols_info[i].arg_type_info = arg->get_type_info();
        } else {
          cols_info[i].arg_type_info = cols_info[i].sql_type_info;
        }

        if (cols_info[i].agg_type == kAVG) {
          cols_info[i].sql_type_info =
              cols_info[i].arg_type_info;  // Reference to get_target_info()
          ++i;
          ++col_slot_context_index;
          ++init_vals_vec_iter;
          CHECK_LT(i, columns_num_);

          cols_info[i].is_key = false;
          cols_info[i].agg_type = kCOUNT;
          cols_info[i].slot_offset = offset;
          cols_info[i].sql_type_info = SQLTypeInfo(g_bigint_count ? kBIGINT : kINT);
          cols_info[i].arg_type_info = cols_info[i].sql_type_info;
          cols_info[i].init_val = *init_vals_vec_iter;

          offset += slot_width_;
        }
      } else {
        cols_info[i].agg_type = kSINGLE_VALUE;
        cols_info[i].arg_type_info = cols_info[i].sql_type_info;
      }

      ++col_slot_context_index;
      ++init_vals_vec_iter;
      ++target_expr_iter;

      if (query_mem_desc_->useCiderDataFormat() && (i + 1) == columns_num_) {
        group_target_null_offset_ = offset;
        offset += ((columns_num_ - key_columns_num_ + 7) >> 3);
      }
    }
  }
  offset = align_to_int64(offset);

  row_width_ = offset;
  CHECK_EQ(row_width_, query_mem_desc_->getRowSize());
  buffer_width_ = row_width_ * buffer_entry_num_;

  return cols_info;
}
// The memory layout of initial_row_data_ is
// key1 key2 ... key_null_buff target1 target2 ... target_null_buff
// null buffer uses bit to represent NULL(0) or NOT NULL(1)
//"SELECT col_i32, SUM(col_i32), COUNT(*) FROM test GROUP BY col_i32");
// The targets null buffer starts at 32nd byte, the index of COUNT(*) in cols_info_ is 2,
// that in null buffer is 1, because col_i32 is also a groupby key.
std::vector<int8_t> CiderAggHashTable::fillRowData() {
  std::vector<int8_t> row_data(row_width_, 0);
  size_t group_targets_null_vec_index = query_mem_desc_->getRowSizeWithoutNullVec();
  for (auto i = 0, j = 0; i < cols_info_.size(); i++) {
    if (cols_info_[i].is_key == false) {
      if (cols_info_[i].agg_type == kCOUNT) {
        CiderBitUtils::setBitAt(
            (uint8_t*)(row_data.data() + group_targets_null_vec_index + (j >> 3)), j % 8);
      }
      j++;
    }
  }
  return row_data;
}

std::vector<SQLTypeInfo> CiderAggHashTable::getKeyTypeInfo() {
  std::vector<SQLTypeInfo> key_type_info;
  key_type_info.reserve(key_columns_num_);

  for (auto& info : cols_info_) {
    if (!info.is_key) {
      break;
    }
    key_type_info.push_back(info.sql_type_info);
  }

  return key_type_info;
}

void CiderAggHashTable::fillTargetIndexMap() {
  size_t target_index_map_index = 0, target_index = 0, mapped_key_count = 0;
  auto& targets = rel_alg_exec_unit_->target_exprs;
  for (; target_index < targets.size(); ++target_index) {
    auto agg_expr = dynamic_cast<Analyzer::AggExpr*>(targets[target_index]);
    if (agg_expr) {
      target_index_map_[target_index_map_index] =
          target_index_map_index + key_columns_num_ - mapped_key_count;

      if (kAVG == agg_expr->get_aggtype()) {
        ++target_index_map_index;
        target_index_map_[target_index_map_index] =
            target_index_map_index + key_columns_num_ - mapped_key_count;
      }
    } else {
      if (query_mem_desc_->targetGroupbyIndicesSize() &&
          query_mem_desc_->getTargetGroupbyIndex(target_index) >= 0) {
        target_index_map_[target_index_map_index] =
            query_mem_desc_->getTargetGroupbyIndex(target_index);
        ++mapped_key_count;
      } else {
        target_index_map_[target_index_map_index] =
            target_index_map_index + key_columns_num_ - mapped_key_count;
      }
    }
    ++target_index_map_index;
  }
}

std::string CiderAggHashTable::toString() const {
  std::stringstream ss;
  ss << "Buffer Num: " << buffers_num_ << "\n";
  ss << "Per Buffer Entry Num: " << buffer_entry_num_ << "\n";
  ss << "Buffer Width: " << buffer_width_ << "\n";
  ss << "Effective Key Slot Width: " << effective_key_slot_width_ << "\n";
  ss << "Slot Width: " << slot_width_ << "\n";
  ss << "Columns Num: " << columns_num_ << "\n";
  ss << "Key Columns Num: " << key_columns_num_ << "\n";
  ss << "Row Width: " << row_width_ << "\n";

  ss << "TargetIndexMap: [";
  for (auto index : target_index_map_) {
    ss << index << ", ";
  }
  ss << "]\n";

  ss << "Entry Info:"
     << "\n";
  size_t i = 0;
  for (auto& entry_info : cols_info_) {
    ss << "( " << i << ", isKey: " << (entry_info.is_key ? "True" : "False")
       << ", offset: " << entry_info.slot_offset
       << ", aggType: " << ::toString(entry_info.agg_type)
       << ", sqlTypeInfo: " << entry_info.sql_type_info.toString()
       << ", argTypeInfo: " << entry_info.arg_type_info.toString()
       << ", initVal: " << std::hex << entry_info.init_val << std::dec << " )\n";
    ++i;
  }

  return ss.str();
}

int64_t* CiderAggHashTable::getGroupTargetPtr(const int64_t* keys) {
  return 8 == effective_key_slot_width_ ? getGroupTargetPtrImpl<int64_t>(keys)
                                        : getGroupTargetPtrImpl<int32_t>(keys);
}

template <CiderHasher::HashMode mode,
          typename KeyT,
          std::enable_if_t<mode == CiderHasher::kRangeHash, bool>>
int64_t* CiderAggHashTable::getGroupTargetPtrImpl(const KeyT* keys) {
  auto hash_val = hasher_.hash<KeyT>(keys);

  if (hasher_.needRehash()) {
    return nullptr;
  }

  int8_t* buffer_ptr = getBuffersPtrAt(0);
  uint8_t* empty_map_ptr = getBufferEmptyMapAt(0);
  CiderAggHashTableBufferRuntimeState& buffer_state = getRuntimeStateAt(0);
  auto row_ptr = reinterpret_cast<KeyT*>(buffer_ptr + row_width_ * hash_val);

  if (CiderBitUtils::isBitSetAt(empty_map_ptr, hash_val)) {
    buffer_state.insertExistEntrySuccess();
  } else {
    CiderBitUtils::setBitAt(empty_map_ptr, hash_val);
    memcpy(row_ptr, keys, group_target_offset_);
    buffer_state.insertNewEntrySuccess();
  }

  int8_t* target_ptr_i8 = reinterpret_cast<int8_t*>(row_ptr) + group_target_offset_;

  return reinterpret_cast<int64_t*>(target_ptr_i8);
}

template <CiderHasher::HashMode mode,
          typename KeyT,
          std::enable_if_t<mode == CiderHasher::kDirectHash, bool>>
int64_t* CiderAggHashTable::getGroupTargetPtrImpl(const KeyT* keys) {
  auto hash_val = hasher_.hash<KeyT>(keys);

  if (hasher_.needRehash()) {
    return nullptr;
  }

  uint64_t start_pos = hash_val % buffer_entry_num_;
  int8_t* buffer_ptr = getBuffersPtrAt(0);
  uint8_t* empty_map_ptr = getBufferEmptyMapAt(0);
  CiderAggHashTableBufferRuntimeState& buffer_state = getRuntimeStateAt(0);

  auto group_match = [ buffer_ptr, empty_map_ptr, &buffer_state, this ](
      uint64_t pos, const KeyT* keys) -> int64_t* __attribute__((__always_inline__)) {
    auto offset = row_width_ * pos;
    auto row_ptr = reinterpret_cast<KeyT*>(buffer_ptr + offset);
    if (CiderBitUtils::isBitSetAt(empty_map_ptr, pos)) {
      if (memcmp(row_ptr, keys, group_target_offset_) == 0) {
        auto row_ptr_i8 = reinterpret_cast<int8_t*>(row_ptr) + group_target_offset_;
        buffer_state.insertExistEntrySuccess();
        return reinterpret_cast<int64_t*>(row_ptr_i8);
      }
      return nullptr;
    } else {
      // New Entry
      memcpy(row_ptr, keys, group_target_offset_);
      CiderBitUtils::setBitAt(empty_map_ptr, pos);
      buffer_state.insertNewEntrySuccess();
      auto row_ptr_i8 = reinterpret_cast<int8_t*>(row_ptr) + group_target_offset_;
      return reinterpret_cast<int64_t*>(row_ptr_i8);
    }
  };  // NOLINT

  int64_t* matching_group = group_match(start_pos, keys);
  if (matching_group) {
    return matching_group;
  }

  uint64_t pos = (start_pos + 1) % buffer_entry_num_;
  while (pos != start_pos) {
    matching_group = group_match(pos, keys);
    if (matching_group) {
      return matching_group;
    }
    pos = (pos + 1) % buffer_entry_num_;
  }

  buffer_state.insertFailed();
  return nullptr;
}

template <typename KeyT>
int64_t* CiderAggHashTable::getGroupTargetPtrImpl(const int64_t* keys) {
  const KeyT* key_vec = reinterpret_cast<const KeyT*>(keys);
  switch (hasher_.getHashMode()) {
    case CiderHasher::kRangeHash:
      return getGroupTargetPtrImpl<CiderHasher::kRangeHash>(key_vec);
    case CiderHasher::kDirectHash:
      return getGroupTargetPtrImpl<CiderHasher::kDirectHash>(key_vec);
  }
}

int8_t* CiderAggHashTable::allocateBufferAt(size_t buffer_id) {
  CHECK_LT(buffer_id, buffers_num_);
  if (nullptr == buffer_memory_[buffer_id]) {
    buffer_memory_[buffer_id] = allocator_->allocate(buffer_width_ * sizeof(int8_t));
    buffer_empty_map_[buffer_id] =
        CiderBitUtils::CiderBitVector<>(allocator_, buffer_entry_num_);
  } else {
    LOG(ERROR) << "Existing a buffer";
  }

  return buffer_memory_[buffer_id];
}

void CiderAggHashTable::freeBufferAt(size_t buffer_id) {
  CHECK_LT(buffer_id, buffers_num_);
  if (buffer_memory_[buffer_id]) {
    allocator_->deallocate(buffer_memory_[buffer_id], buffer_width_ * sizeof(int8_t));
  }
}

void CiderAggHashTable::resetBuffer(size_t buffer_index) {
  auto target_ptr = buffer_memory_[buffer_index];
  CHECK(target_ptr);
  const auto row_data_ptr = initial_row_data_.data();
  for (size_t i = 0; i < buffer_entry_num_; ++i, target_ptr += row_width_) {
    std::memcpy(target_ptr, row_data_ptr, row_width_);
  }

  buffer_empty_map_[buffer_index].resetBits(0);
}

void CiderAggHashTable::initCountDistinctInBuffer(size_t buffer_index) {
  // TODO: free distinct buffer
  if (!query_mem_desc_->hasCountDistinct()) {
    return;
  }
  auto target_ptr = buffer_memory_[buffer_index];
  // For each row, check whether count distinct buffer allocated or not
  // TODO: free distinct buffer
  for (auto& col_info : cols_info_) {
    auto col_ptr = target_ptr + col_info.slot_offset;
    if (col_info.count_distinct_desc.impl_type_ != CountDistinctImplType::Invalid) {
      for (size_t i = 0; i < buffer_entry_num_; ++i) {
        const int64_t* vals = reinterpret_cast<const int64_t*>(col_ptr);
        if (vals[0] == 0) {
          if (col_info.count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
            const int64_t MAX_BITMAP_BITS{4 * 1000 * 1000L};
            CHECK_LT(col_info.count_distinct_desc.bitmap_sz_bits, MAX_BITMAP_BITS);
            const auto bitmap_byte_sz =
                col_info.count_distinct_desc.bitmapPaddedSizeBytes();
            int8_t* buffer = allocator_->allocate(bitmap_byte_sz);
            int64_t address = reinterpret_cast<int64_t>(buffer);
            std::memcpy(col_ptr, &address, slot_width_);
          } else {
            auto count_distinct_set = new robin_hood::unordered_set<int64_t>();
            int64_t address = reinterpret_cast<int64_t>(count_distinct_set);
            std::memcpy(col_ptr, &address, slot_width_);
          }
        }
        // To next row
        col_ptr += row_width_;
      }
    }
  }
}

void CiderAggHashTable::updateBufferCapacity(size_t memory_usage_upper) {
  memory_usage_upper = std::min(memory_usage_upper, buffer_memory_limit_);
  memory_usage_upper >>= 3;
  memory_usage_upper <<= 3;

  buffer_entry_num_ = memory_usage_upper / row_width_;
  buffer_width_ = buffer_entry_num_ * row_width_;
}

bool CiderAggHashTable::rehash() {
  uint64_t buffer_entry_limit = buffer_memory_limit_;
  buffer_entry_limit /= row_width_;

  if (hasher_.getHashMode() == CiderHasher::kDirectHash &&
      buffer_entry_num_ == buffer_entry_limit) {
    // Need to spill
    return false;
  }

  uint64_t new_buffer_entry_num =
      hasher_.updateHashMode(buffer_entry_num_, buffer_entry_limit);
  auto prev_buffer_entry_num = buffer_entry_num_;
  updateBufferCapacity(new_buffer_entry_num * row_width_ + 7);

  auto prev_buffer_ptr = buffer_memory_[0];
  buffer_memory_[0] = nullptr;
  CiderBitUtils::CiderBitVector<> prev_empty_map = std::move(buffer_empty_map_[0]);

  allocateBufferAt(0);
  resetBuffer(0);

  const size_t target_offset = (columns_num_ == key_columns_num_)
                                   ? row_width_
                                   : cols_info_[key_columns_num_].slot_offset;
  for (size_t i = 0; i < prev_buffer_entry_num; ++i) {
    if (CiderBitUtils::isBitSetAt(prev_empty_map.as<uint8_t>(), i)) {
      const int64_t* prev_row =
          reinterpret_cast<const int64_t*>(prev_buffer_ptr + row_width_ * i);
      auto target_ptr = getGroupTargetPtr(prev_row);
      memcpy(target_ptr, prev_row + (target_offset >> 3), row_width_ - target_offset);
    }
  }
  initCountDistinctInBuffer(0);
  delete[] prev_buffer_ptr;
  return true;
}

size_t CiderAggHashTable::getActualDataWidth(size_t column_index) const {
  CHECK_LT(column_index, columns_num_);

  if (cols_info_[column_index].is_key) {
    return effective_key_slot_width_;
  }
  // In non-agg expressions, float type(group-by key) will be cast into double
  // In Cider data format, targets with float-float partten will follow the float-spec
  // size.
  else if (kFLOAT == cols_info_[column_index].sql_type_info.get_type() &&
           kFLOAT == cols_info_[column_index].arg_type_info.get_type() &&
           (query_mem_desc_->useCiderDataFormat() ||
            cols_info_[column_index].agg_type != kSINGLE_VALUE)) {
    return sizeof(float);
  }
  return slot_width_;
}

size_t CiderAggHashTable::getNextRowIndex(const int8_t* buffer_ptr,
                                          const uint8_t* empty_map_ptr,
                                          const size_t start_row_index) const {
  return getNextRowIndexRow(buffer_ptr, empty_map_ptr, start_row_index);
}

size_t CiderAggHashTable::getNextRowIndexRow(const int8_t* buffer_ptr,
                                             const uint8_t* empty_map_ptr,
                                             const size_t start_row_index) const {
  for (size_t pos = start_row_index; pos < buffer_entry_num_; ++pos) {
    if (CiderBitUtils::isBitSetAt(empty_map_ptr, pos)) {
      return pos;
    }
  }
  return buffer_entry_num_;
}

template <typename T>
size_t CiderAggHashTable::getNextRowIndexColumnar(const int8_t* buffer_ptr,
                                                  const size_t start_row_index) const {
  const T* empty_keys_ptr = reinterpret_cast<const T*>(initial_row_data_.data());
  size_t bound = buffer_entry_num_;

  for (size_t i = start_row_index; i < bound; ++i) {
    for (size_t j = 0; j < key_columns_num_; ++j) {
      const T* col_ptr =
          reinterpret_cast<const T*>(buffer_ptr + cols_info_[j].slot_offset);
      if (empty_keys_ptr[j] != col_ptr[i]) {
        return i;
      }
    }
  }

  return bound;
}

CiderAggHashTableRowIterator::CiderAggHashTableRowIterator(CiderAggHashTable* table_ptr,
                                                           size_t buffer_id)
    : row_index_(0)
    , buffer_ptr_(table_ptr->getBuffersPtrAt(buffer_id))
    , empty_map_ptr_(table_ptr->getBufferEmptyMapAt(buffer_id))
    , table_ptr_(table_ptr)
    , buffer_runtime_state_(&table_ptr->getRuntimeStateAt(buffer_id)) {
  CHECK(buffer_ptr_);
  row_index_ = table_ptr->getNextRowIndex(buffer_ptr_, empty_map_ptr_, row_index_);
}

bool CiderAggHashTableRowIterator::toNextRow() {
  if (row_index_ == table_ptr_->buffer_entry_num_) {
    return false;
  }

  row_index_ = table_ptr_->getNextRowIndex(buffer_ptr_, empty_map_ptr_, row_index_ + 1);

  return row_index_ != table_ptr_->buffer_entry_num_;
}

const CiderAggHashTableEntryInfo& CiderAggHashTableRowIterator::getColumnInfo(
    size_t column_index) const {
  CHECK_LT(column_index, table_ptr_->columns_num_);
  return table_ptr_->cols_info_[column_index];
}

const int32_t* CiderAggHashTableRowIterator::getColumn(size_t column_index) const {
  CHECK_LT(column_index, table_ptr_->columns_num_);
  if (row_index_ == table_ptr_->buffer_entry_num_) {
    return nullptr;
  }

  const auto col_ptr = buffer_ptr_ + table_ptr_->cols_info_[column_index].slot_offset +
                       table_ptr_->row_width_ * row_index_;
  return reinterpret_cast<int32_t*>(col_ptr);
}

const int8_t* CiderAggHashTableRowIterator::getColumnBase(size_t column_index) const {
  CHECK_LT(column_index, table_ptr_->columns_num_);
  if (row_index_ == table_ptr_->buffer_entry_num_) {
    return nullptr;
  }

  const auto col_ptr = buffer_ptr_ + table_ptr_->row_width_ * row_index_;
  return col_ptr;
}

size_t CiderAggHashTableRowIterator::getColumnActualWidth(size_t column_index) const {
  return table_ptr_->getActualDataWidth(column_index);
}

CiderAggHashTableRowIteratorPtr CiderAggHashTable::getRowIterator(size_t buffer_index) {
  return std::make_unique<CiderAggHashTableRowIterator>(this, buffer_index);
}

bool CiderAggHashTableRowIterator::finished() const {
  return row_index_ == table_ptr_->buffer_entry_num_;
}

int32_t CiderAggHashTableRowIterator::getColumnTypeInfo(size_t column_index) const {
  return getColumnInfo(column_index).arg_type_info.get_type();
}

void CiderAggHashTableDeleter::operator()(CiderAggHashTable* ptr) const {
  delete ptr;
}
