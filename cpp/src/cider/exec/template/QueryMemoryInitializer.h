/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#include "ResultSet.h"
#include "exec/template/common/descriptors/QueryMemoryDescriptor.h"

class QueryMemoryInitializer {
 public:
  // Row-based execution constructor
  QueryMemoryInitializer(const RelAlgExecutionUnit& ra_exe_unit,
                         const QueryMemoryDescriptor& query_mem_desc,
                         const int device_id,
                         const ExecutorDispatchMode dispatch_mode,
                         const bool output_columnar,
                         const int64_t num_rows,
                         const std::vector<std::vector<const int8_t*>>& col_buffers,
                         const std::vector<std::vector<uint64_t>>& frag_offsets,
                         std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                         const size_t thread_idx,
                         const Executor* executor);

  const auto getCountDistinctBitmapPtr() const { return count_distinct_bitmap_mem_; }

  const auto getCountDistinctHostPtr() const { return count_distinct_bitmap_host_mem_; }

  const auto getCountDistinctBitmapBytes() const {
    return count_distinct_bitmap_mem_bytes_;
  }

  // TODO: lazy init (maybe lazy init count distinct above, too?)
  const auto getVarlenOutputHostPtr() const { return varlen_output_buffer_host_ptr_; }

  const auto getVarlenOutputPtr() const { return varlen_output_buffer_; }

  int64_t getAggInitValForIndex(const size_t index) const {
    CHECK_LT(index, init_agg_vals_.size());
    return init_agg_vals_[index];
  }

  const auto getGroupByBuffersPtr() {
    return reinterpret_cast<int64_t**>(group_by_buffers_.data());
  }

  const auto getGroupByBuffersSize() const { return group_by_buffers_.size(); }

  const auto getNumBuffers() const {
    CHECK_EQ(num_buffers_, group_by_buffers_.size());
    return num_buffers_;
  }

 private:
  void initGroupByBuffer(int64_t* buffer,
                         const RelAlgExecutionUnit& ra_exe_unit,
                         const QueryMemoryDescriptor& query_mem_desc,
                         const bool output_columnar,
                         const Executor* executor);

  bool useVectorRowGroupsInit(const size_t row_size, const size_t entries) const;

  void initRowGroups(const QueryMemoryDescriptor& query_mem_desc,
                     int64_t* groups_buffer,
                     const std::vector<int64_t>& init_vals,
                     const int32_t groups_buffer_entry_count,
                     const size_t warp_size,
                     const Executor* executor);

  void initColumnarGroups(const QueryMemoryDescriptor& query_mem_desc,
                          int64_t* groups_buffer,
                          const std::vector<int64_t>& init_vals,
                          const Executor* executor);

  using QuantileParam = std::optional<double>;
  void initColumnsPerRow(const QueryMemoryDescriptor& query_mem_desc,
                         int8_t* row_ptr,
                         const std::vector<int64_t>& init_vals,
                         const std::vector<int64_t>& bitmap_sizes,
                         const std::vector<QuantileParam>& quantile_params);

  std::vector<int64_t> allocateCountDistinctBuffers(
      const QueryMemoryDescriptor& query_mem_desc,
      const bool deferred,
      const Executor* executor);

  int64_t allocateCountDistinctBitmap(const size_t bitmap_byte_sz);

  int64_t allocateCountDistinctSet();

  std::vector<QuantileParam> allocateTDigests(const QueryMemoryDescriptor& query_mem_desc,
                                              const bool deferred,
                                              const Executor* executor);

  void applyStreamingTopNOffsetCpu(const QueryMemoryDescriptor& query_mem_desc,
                                   const RelAlgExecutionUnit& ra_exe_unit);

  const int64_t num_rows_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  std::vector<std::unique_ptr<ResultSet>> result_sets_;

  std::vector<int64_t> init_agg_vals_;

  size_t num_buffers_;
  std::vector<int64_t*> group_by_buffers_;
  unsigned long long varlen_output_buffer_;
  int8_t* varlen_output_buffer_host_ptr_;

  unsigned long long count_distinct_bitmap_mem_;
  size_t count_distinct_bitmap_mem_bytes_;
  int8_t* count_distinct_bitmap_crt_ptr_;
  int8_t* count_distinct_bitmap_host_mem_;

  std::vector<Data_Namespace::AbstractBuffer*> temporary_buffers_;

  const size_t thread_idx_;

  friend class Executor;  // Accesses result_sets_
  friend class QueryExecutionContext;
};
