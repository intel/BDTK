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
#ifndef QUERYENGINE_RESULTSET_H
#define QUERYENGINE_RESULTSET_H

#include "CardinalityEstimator.h"
#include "TargetValue.h"
#include "exec/template/common/descriptors/QueryMemoryDescriptor.h"
#include "util/memory/BufferProvider.h"
#include "util/memory/Chunk/Chunk.h"
#include "util/quantile.h"

#include <atomic>
#include <functional>
#include <list>
#include <utility>

class ResultSet {
 public:
  ResultSet(const std::vector<TargetInfo>& targets,
            const std::vector<std::vector<const int8_t*>>& col_buffers,
            const std::vector<std::vector<int64_t>>& frag_offsets,
            const std::vector<int64_t>& consistent_frag_sizes,
            const int device_id,
            const QueryMemoryDescriptor& query_mem_desc,
            const std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
            BufferProvider* buffer_provider,
            const int db_id_for_dict,
            const unsigned block_size,
            const unsigned grid_size);

  ~ResultSet();

  SQLTypeInfo getColType(const size_t col_idx) const;

  // Called from the executor because in the new ResultSet we assume the 'padded' field
  // in SlotSize already contains the padding, whereas in the executor it's computed.
  // Once the buffer initialization moves to ResultSet we can remove this method.
  static QueryMemoryDescriptor fixupQueryMemoryDescriptor(const QueryMemoryDescriptor&);

  static void fill_empty_key(void* key_ptr,
                             const size_t key_count,
                             const size_t key_width);

 private:
  const std::vector<TargetInfo> targets_;
  const int device_id_;
  QueryMemoryDescriptor query_mem_desc_;
  mutable size_t crt_row_buff_idx_;
  mutable size_t fetched_so_far_;
  size_t drop_first_;
  size_t keep_first_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;

  unsigned block_size_{0};
  unsigned grid_size_{0};

  std::vector<std::vector<std::vector<const int8_t*>>> col_buffers_;
  std::vector<std::vector<std::vector<int64_t>>> frag_offsets_;
  std::vector<std::vector<int64_t>> consistent_frag_sizes_;

  BufferProvider* buffer_provider_{nullptr};
  const int db_id_for_dict_{-1};

  // only used by serialization
  using SerializedVarlenBufferStorage = std::vector<std::string>;

  bool separate_varlen_storage_valid_;
  const bool just_explain_;
  bool for_validation_only_;
  mutable std::atomic<int64_t> cached_row_count_;

  friend class ColumnarResults;
};

inline bool is_real_str_or_array(const TargetInfo& target_info) {
  return (!target_info.is_agg || target_info.agg_kind == kSAMPLE) &&
         (target_info.sql_type.is_array() ||
          (target_info.sql_type.is_string() &&
           target_info.sql_type.get_compression() == kENCODING_NONE));
}

inline size_t get_slots_for_target(const TargetInfo& target_info,
                                   const bool separate_varlen_storage) {
  if (target_info.is_agg) {
    if (target_info.agg_kind == kAVG || is_real_str_or_array(target_info)) {
      return 2;
    } else {
      return 1;
    }
  } else {
    if (is_real_str_or_array(target_info) && !separate_varlen_storage) {
      return 2;
    } else {
      return 1;
    }
  }
}

inline size_t advance_slot(const size_t j,
                           const TargetInfo& target_info,
                           const bool separate_varlen_storage) {
  return j + get_slots_for_target(target_info, separate_varlen_storage);
}

#endif  // QUERYENGINE_RESULTSET_H
