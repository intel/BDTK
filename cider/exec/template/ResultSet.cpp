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

/**
 * @file    ResultSet.cpp
 * @brief   Basic constructors and methods of the row set interface.
 */

#include "ResultSet.h"
#include "Execute.h"
#include "InPlaceSort.h"
#include "OutputBufferInitialization.h"
#include "function/scalar/RuntimeFunctions.h"
#include "type/data/InlineNullValues.h"
#include "util/Intervals.h"
#include "util/SqlTypesLayout.h"
#include "util/checked_alloc.h"
#include "util/likely.h"
#include "util/parallel_sort.h"
#include "util/thread_count.h"
#include "util/threading.h"

#ifdef HAVE_TBB
#include "tbb/parallel_sort.h"
#endif

#include <algorithm>
#include <atomic>
#include <bitset>
#include <future>
#include <numeric>

constexpr int64_t uninitialized_cached_row_count{-1};

ResultSet::ResultSet(const std::vector<TargetInfo>& targets,
                     const std::vector<std::vector<const int8_t*>>& col_buffers,
                     const std::vector<std::vector<int64_t>>& frag_offsets,
                     const std::vector<int64_t>& consistent_frag_sizes,
                     const int device_id,
                     const QueryMemoryDescriptor& query_mem_desc,
                     const std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                     BufferProvider* buffer_provider,
                     const int db_id_for_dict,
                     const unsigned block_size,
                     const unsigned grid_size)
    : targets_(targets)
    , device_id_(device_id)
    , query_mem_desc_(query_mem_desc)
    , crt_row_buff_idx_(0)
    , fetched_so_far_(0)
    , drop_first_(0)
    , keep_first_(0)
    , row_set_mem_owner_(row_set_mem_owner)
    , block_size_(block_size)
    , grid_size_(grid_size)
    , col_buffers_{col_buffers}
    , frag_offsets_{frag_offsets}
    , consistent_frag_sizes_{consistent_frag_sizes}
    , buffer_provider_(buffer_provider)
    , db_id_for_dict_(db_id_for_dict)
    , separate_varlen_storage_valid_(false)
    , just_explain_(false)
    , for_validation_only_(false)
    , cached_row_count_(uninitialized_cached_row_count) {}

ResultSet::~ResultSet() {}

SQLTypeInfo ResultSet::getColType(const size_t col_idx) const {
  if (just_explain_) {
    return SQLTypeInfo(kTEXT, false);
  }
  CHECK_LT(col_idx, targets_.size());
  return targets_[col_idx].agg_kind == kAVG ? SQLTypeInfo(kDOUBLE, false)
                                            : targets_[col_idx].sql_type;
}

QueryMemoryDescriptor ResultSet::fixupQueryMemoryDescriptor(
    const QueryMemoryDescriptor& query_mem_desc) {
  auto query_mem_desc_copy = query_mem_desc;
  query_mem_desc_copy.resetGroupColWidths(
      std::vector<int8_t>(query_mem_desc_copy.getGroupbyColCount(), 8));
  if (query_mem_desc.didOutputColumnar()) {
    return query_mem_desc_copy;
  }
  query_mem_desc_copy.alignPaddedSlots();
  return query_mem_desc_copy;
}

ALWAYS_INLINE
void fill_empty_key_32(int32_t* key_ptr_i32, const size_t key_count) {
  for (size_t i = 0; i < key_count; ++i) {
    key_ptr_i32[i] = EMPTY_KEY_32;
  }
}

ALWAYS_INLINE
void fill_empty_key_64(int64_t* key_ptr_i64, const size_t key_count) {
  for (size_t i = 0; i < key_count; ++i) {
    key_ptr_i64[i] = EMPTY_KEY_64;
  }
}

void ResultSet::fill_empty_key(void* key_ptr,
                               const size_t key_count,
                               const size_t key_width) {
  switch (key_width) {
    case 4: {
      auto key_ptr_i32 = reinterpret_cast<int32_t*>(key_ptr);
      fill_empty_key_32(key_ptr_i32, key_count);
      break;
    }
    case 8: {
      auto key_ptr_i64 = reinterpret_cast<int64_t*>(key_ptr);
      fill_empty_key_64(key_ptr_i64, key_count);
      break;
    }
    default:
      CHECK(false);
  }
}
