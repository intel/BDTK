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

#include "exec/template/WindowContext.h"

#include <numeric>

#include "exec/template/Execute.h"
#include "exec/template/OutputBufferInitialization.h"
#include "exec/template/TypePunning.h"
#include "exec/template/common/descriptors/CountDistinctDescriptor.h"
#include "function/scalar/RuntimeFunctions.h"
#include "type/data/funcannotations.h"
#include "util/checked_alloc.h"

// Non-partitioned version (no join table provided)
WindowFunctionContext::WindowFunctionContext(
    const Analyzer::WindowFunction* window_func,
    const size_t elem_count,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner)
    : window_func_(window_func)
    , partitions_(nullptr)
    , elem_count_(elem_count)
    , output_(nullptr)
    , partition_start_(nullptr)
    , partition_end_(nullptr)
    , row_set_mem_owner_(row_set_mem_owner)
    , dummy_count_(elem_count)
    , dummy_offset_(0)
    , dummy_payload_(nullptr) {
  CHECK_LE(elem_count_, static_cast<size_t>(std::numeric_limits<int32_t>::max()));
  if (elem_count_ > 0) {
    dummy_payload_ =
        reinterpret_cast<int32_t*>(checked_malloc(elem_count_ * sizeof(int32_t)));
    std::iota(dummy_payload_, dummy_payload_ + elem_count_, int32_t(0));
  }
}

// Partitioned version
WindowFunctionContext::WindowFunctionContext(
    const Analyzer::WindowFunction* window_func,
    const std::shared_ptr<HashJoin>& partitions,
    const size_t elem_count,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner)
    : window_func_(window_func)
    , partitions_(partitions)
    , elem_count_(elem_count)
    , output_(nullptr)
    , partition_start_(nullptr)
    , partition_end_(nullptr)
    , row_set_mem_owner_(row_set_mem_owner)
    , dummy_count_(elem_count)
    , dummy_offset_(0)
    , dummy_payload_(nullptr) {
  CHECK(partitions_);  // This version should have hash table
}

WindowFunctionContext::~WindowFunctionContext() {
  free(partition_start_);
  free(partition_end_);
  if (dummy_payload_) {
    free(dummy_payload_);
  }
}

void WindowFunctionContext::addOrderColumn(
    const int8_t* column,
    const Analyzer::ColumnVar* col_var,
    const std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner) {
  order_columns_owner_.push_back(chunks_owner);
  order_columns_.push_back(column);
}

namespace {

// Converts the sorted indices to a mapping from row position to row number.
std::vector<int64_t> index_to_row_number(const int64_t* index, const size_t index_size) {
  std::vector<int64_t> row_numbers(index_size);
  for (size_t i = 0; i < index_size; ++i) {
    row_numbers[index[i]] = i + 1;
  }
  return row_numbers;
}

// Returns true iff the current element is greater than the previous, according to the
// comparator. This is needed because peer rows have to have the same rank.
bool advance_current_rank(
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator,
    const int64_t* index,
    const size_t i) {
  if (i == 0) {
    return false;
  }
  return comparator(index[i - 1], index[i]);
}

// Computes the mapping from row position to rank.
std::vector<int64_t> index_to_rank(
    const int64_t* index,
    const size_t index_size,
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator) {
  std::vector<int64_t> rank(index_size);
  size_t crt_rank = 1;
  for (size_t i = 0; i < index_size; ++i) {
    if (advance_current_rank(comparator, index, i)) {
      crt_rank = i + 1;
    }
    rank[index[i]] = crt_rank;
  }
  return rank;
}

// Computes the mapping from row position to dense rank.
std::vector<int64_t> index_to_dense_rank(
    const int64_t* index,
    const size_t index_size,
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator) {
  std::vector<int64_t> dense_rank(index_size);
  size_t crt_rank = 1;
  for (size_t i = 0; i < index_size; ++i) {
    if (advance_current_rank(comparator, index, i)) {
      ++crt_rank;
    }
    dense_rank[index[i]] = crt_rank;
  }
  return dense_rank;
}

// Computes the mapping from row position to percent rank.
std::vector<double> index_to_percent_rank(
    const int64_t* index,
    const size_t index_size,
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator) {
  std::vector<double> percent_rank(index_size);
  size_t crt_rank = 1;
  for (size_t i = 0; i < index_size; ++i) {
    if (advance_current_rank(comparator, index, i)) {
      crt_rank = i + 1;
    }
    percent_rank[index[i]] =
        index_size == 1 ? 0 : static_cast<double>(crt_rank - 1) / (index_size - 1);
  }
  return percent_rank;
}

// Computes the mapping from row position to cumulative distribution.
std::vector<double> index_to_cume_dist(
    const int64_t* index,
    const size_t index_size,
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator) {
  std::vector<double> cume_dist(index_size);
  size_t start_peer_group = 0;
  while (start_peer_group < index_size) {
    size_t end_peer_group = start_peer_group + 1;
    while (end_peer_group < index_size &&
           !advance_current_rank(comparator, index, end_peer_group)) {
      ++end_peer_group;
    }
    for (size_t i = start_peer_group; i < end_peer_group; ++i) {
      cume_dist[index[i]] = static_cast<double>(end_peer_group) / index_size;
    }
    start_peer_group = end_peer_group;
  }
  return cume_dist;
}

// Computes the mapping from row position to the n-tile statistic.
std::vector<int64_t> index_to_ntile(const int64_t* index,
                                    const size_t index_size,
                                    const size_t n) {
  std::vector<int64_t> row_numbers(index_size);
  if (!n) {
    CIDER_THROW(CiderCompileException, "NTILE argument cannot be zero");
  }
  const size_t tile_size = (index_size + n - 1) / n;
  for (size_t i = 0; i < index_size; ++i) {
    row_numbers[index[i]] = i / tile_size + 1;
  }
  return row_numbers;
}

// The element size in the result buffer for the given window function kind. Currently
// it's always 8.
size_t window_function_buffer_element_size(const SqlWindowFunctionKind /*kind*/) {
  return 8;
}

// Extracts the integer constant from a constant expression.
size_t get_int_constant_from_expr(const Analyzer::Expr* expr) {
  const auto lag_constant = dynamic_cast<const Analyzer::Constant*>(expr);
  if (!lag_constant) {
    CIDER_THROW(CiderCompileException,
                "LAG with non-constant lag argument not supported yet");
  }
  const auto& lag_ti = lag_constant->get_type_info();
  switch (lag_ti.get_type()) {
    case kSMALLINT: {
      return lag_constant->get_constval().smallintval;
    }
    case kINT: {
      return lag_constant->get_constval().intval;
    }
    case kBIGINT: {
      return lag_constant->get_constval().bigintval;
    }
    default: {
      LOG(FATAL) << "Invalid type for the lag argument";
    }
  }
  return 0;
}

// Gets the lag or lead argument canonicalized as lag (lag = -lead).
int64_t get_lag_or_lead_argument(const Analyzer::WindowFunction* window_func) {
  CHECK(window_func->getKind() == SqlWindowFunctionKind::LAG ||
        window_func->getKind() == SqlWindowFunctionKind::LEAD);
  const auto& args = window_func->getArgs();
  if (args.size() == 3) {
    CIDER_THROW(CiderCompileException, "LAG with default not supported yet");
  }
  if (args.size() == 2) {
    const int64_t lag_or_lead =
        static_cast<int64_t>(get_int_constant_from_expr(args[1].get()));
    return window_func->getKind() == SqlWindowFunctionKind::LAG ? lag_or_lead
                                                                : -lag_or_lead;
  }
  CHECK_EQ(args.size(), size_t(1));
  return window_func->getKind() == SqlWindowFunctionKind::LAG ? 1 : -1;
}

// Redistributes the original_indices according to the permutation given by
// output_for_partition_buff, reusing it as an output buffer.
void apply_permutation_to_partition(int64_t* output_for_partition_buff,
                                    const int32_t* original_indices,

                                    const size_t partition_size) {
  std::vector<int64_t> new_output_for_partition_buff(partition_size);
  for (size_t i = 0; i < partition_size; ++i) {
    new_output_for_partition_buff[i] = original_indices[output_for_partition_buff[i]];
  }
  std::copy(new_output_for_partition_buff.begin(),
            new_output_for_partition_buff.end(),
            output_for_partition_buff);
}

// Applies a lag to the given sorted_indices, reusing it as an output buffer.
void apply_lag_to_partition(const int64_t lag,
                            const int32_t* original_indices,
                            int64_t* sorted_indices,
                            const size_t partition_size) {
  std::vector<int64_t> lag_sorted_indices(partition_size, -1);
  for (int64_t idx = 0; idx < static_cast<int64_t>(partition_size); ++idx) {
    int64_t lag_idx = idx - lag;
    if (lag_idx < 0 || lag_idx >= static_cast<int64_t>(partition_size)) {
      continue;
    }
    lag_sorted_indices[idx] = sorted_indices[lag_idx];
  }
  std::vector<int64_t> lag_original_indices(partition_size);
  for (size_t k = 0; k < partition_size; ++k) {
    const auto lag_index = lag_sorted_indices[k];
    lag_original_indices[sorted_indices[k]] =
        lag_index != -1 ? original_indices[lag_index] : -1;
  }
  std::copy(lag_original_indices.begin(), lag_original_indices.end(), sorted_indices);
}

// Computes first value function for the given output_for_partition_buff, reusing it as an
// output buffer.
void apply_first_value_to_partition(const int32_t* original_indices,
                                    int64_t* output_for_partition_buff,
                                    const size_t partition_size) {
  const auto first_value_idx = original_indices[output_for_partition_buff[0]];
  std::fill(output_for_partition_buff,
            output_for_partition_buff + partition_size,
            first_value_idx);
}

// Computes last value function for the given output_for_partition_buff, reusing it as an
// output buffer.
void apply_last_value_to_partition(const int32_t* original_indices,
                                   int64_t* output_for_partition_buff,
                                   const size_t partition_size) {
  std::copy(
      original_indices, original_indices + partition_size, output_for_partition_buff);
}

void index_to_partition_end(
    const int8_t* partition_end,
    const size_t off,
    const int64_t* index,
    const size_t index_size,
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator) {
  int64_t partition_end_handle = reinterpret_cast<int64_t>(partition_end);
  for (size_t i = 0; i < index_size; ++i) {
    if (advance_current_rank(comparator, index, i)) {
      agg_count_distinct_bitmap(&partition_end_handle, off + i - 1, 0);
    }
  }
  CHECK(index_size);
  agg_count_distinct_bitmap(&partition_end_handle, off + index_size - 1, 0);
}

bool pos_is_set(const int64_t bitset, const int64_t pos) {
  return (reinterpret_cast<const int8_t*>(bitset))[pos >> 3] & (1 << (pos & 7));
}

// Write value to pending integer outputs collected for all the peer rows. The end of
// groups is represented by the bitset.
template <class T>
void apply_window_pending_outputs_int(const int64_t handle,
                                      const int64_t value,
                                      const int64_t bitset,
                                      const int64_t pos) {
  if (!pos_is_set(bitset, pos)) {
    return;
  }
  auto& pending_output_slots = *reinterpret_cast<std::vector<void*>*>(handle);
  for (auto pending_output_slot : pending_output_slots) {
    *reinterpret_cast<T*>(pending_output_slot) = value;
  }
  pending_output_slots.clear();
}

}  // namespace

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_int64(const int64_t handle,
                                                                  const int64_t value,
                                                                  const int64_t bitset,
                                                                  const int64_t pos) {
  apply_window_pending_outputs_int<int64_t>(handle, value, bitset, pos);
}

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_int32(const int64_t handle,
                                                                  const int64_t value,
                                                                  const int64_t bitset,
                                                                  const int64_t pos) {
  apply_window_pending_outputs_int<int32_t>(handle, value, bitset, pos);
}

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_int16(const int64_t handle,
                                                                  const int64_t value,
                                                                  const int64_t bitset,
                                                                  const int64_t pos) {
  apply_window_pending_outputs_int<int16_t>(handle, value, bitset, pos);
}

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_int8(const int64_t handle,
                                                                 const int64_t value,
                                                                 const int64_t bitset,
                                                                 const int64_t pos) {
  apply_window_pending_outputs_int<int8_t>(handle, value, bitset, pos);
}

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_double(const int64_t handle,
                                                                   const double value,
                                                                   const int64_t bitset,
                                                                   const int64_t pos) {
  if (!pos_is_set(bitset, pos)) {
    return;
  }
  auto& pending_output_slots = *reinterpret_cast<std::vector<void*>*>(handle);
  for (auto pending_output_slot : pending_output_slots) {
    *reinterpret_cast<double*>(pending_output_slot) = value;
  }
  pending_output_slots.clear();
}

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_float(const int64_t handle,
                                                                  const float value,
                                                                  const int64_t bitset,
                                                                  const int64_t pos) {
  if (!pos_is_set(bitset, pos)) {
    return;
  }
  auto& pending_output_slots = *reinterpret_cast<std::vector<void*>*>(handle);
  for (auto pending_output_slot : pending_output_slots) {
    *reinterpret_cast<double*>(pending_output_slot) = value;
  }
  pending_output_slots.clear();
}

extern "C" RUNTIME_EXPORT void apply_window_pending_outputs_float_columnar(
    const int64_t handle,
    const float value,
    const int64_t bitset,
    const int64_t pos) {
  if (!pos_is_set(bitset, pos)) {
    return;
  }
  auto& pending_output_slots = *reinterpret_cast<std::vector<void*>*>(handle);
  for (auto pending_output_slot : pending_output_slots) {
    *reinterpret_cast<float*>(pending_output_slot) = value;
  }
  pending_output_slots.clear();
}

// Add a pending output slot to be written back at the end of a peer row group.
extern "C" RUNTIME_EXPORT void add_window_pending_output(void* pending_output,
                                                         const int64_t handle) {
  reinterpret_cast<std::vector<void*>*>(handle)->push_back(pending_output);
}

// Returns true iff the aggregate window function requires special multiplicity handling
// to ensure that peer rows have the same value for the window function.
bool window_function_requires_peer_handling(const Analyzer::WindowFunction* window_func) {
  if (!window_function_is_aggregate(window_func->getKind())) {
    return false;
  }
  if (window_func->getOrderKeys().empty()) {
    return true;
  }
  switch (window_func->getKind()) {
    case SqlWindowFunctionKind::MIN:
    case SqlWindowFunctionKind::MAX: {
      return false;
    }
    default: {
      return true;
    }
  }
}

const Analyzer::WindowFunction* WindowFunctionContext::getWindowFunction() const {
  return window_func_;
}

const int8_t* WindowFunctionContext::output() const {
  return output_;
}

const int64_t* WindowFunctionContext::aggregateState() const {
  CHECK(window_function_is_aggregate(window_func_->getKind()));
  return &aggregate_state_.val;
}

const int64_t* WindowFunctionContext::aggregateStateCount() const {
  CHECK(window_function_is_aggregate(window_func_->getKind()));
  return &aggregate_state_.count;
}

int64_t WindowFunctionContext::aggregateStatePendingOutputs() const {
  CHECK(window_function_is_aggregate(window_func_->getKind()));
  return reinterpret_cast<int64_t>(&aggregate_state_.outputs);
}

const int8_t* WindowFunctionContext::partitionStart() const {
  return partition_start_;
}

const int8_t* WindowFunctionContext::partitionEnd() const {
  return partition_end_;
}

size_t WindowFunctionContext::elementCount() const {
  return elem_count_;
}

namespace {

template <class T>
bool integer_comparator(const int8_t* order_column_buffer,
                        const SQLTypeInfo& ti,
                        const int32_t* partition_indices,
                        const int64_t lhs,
                        const int64_t rhs,
                        const bool nulls_first) {
  const auto values = reinterpret_cast<const T*>(order_column_buffer);
  const auto lhs_val = values[partition_indices[lhs]];
  const auto rhs_val = values[partition_indices[rhs]];
  const auto null_val = inline_fixed_encoding_null_val(ti);
  if (lhs_val == null_val && rhs_val == null_val) {
    return false;
  }
  if (lhs_val == null_val && rhs_val != null_val) {
    return nulls_first;
  }
  if (rhs_val == null_val && lhs_val != null_val) {
    return !nulls_first;
  }
  return lhs_val < rhs_val;
}

}  // namespace

void WindowFunctionContext::computePartition(
    int64_t* output_for_partition_buff,
    const size_t partition_size,
    const size_t off,
    const Analyzer::WindowFunction* window_func,
    const std::function<bool(const int64_t lhs, const int64_t rhs)>& comparator) {
  switch (window_func->getKind()) {
    case SqlWindowFunctionKind::ROW_NUMBER: {
      const auto row_numbers =
          index_to_row_number(output_for_partition_buff, partition_size);
      std::copy(row_numbers.begin(), row_numbers.end(), output_for_partition_buff);
      break;
    }
    case SqlWindowFunctionKind::RANK: {
      const auto rank =
          index_to_rank(output_for_partition_buff, partition_size, comparator);
      std::copy(rank.begin(), rank.end(), output_for_partition_buff);
      break;
    }
    case SqlWindowFunctionKind::DENSE_RANK: {
      const auto dense_rank =
          index_to_dense_rank(output_for_partition_buff, partition_size, comparator);
      std::copy(dense_rank.begin(), dense_rank.end(), output_for_partition_buff);
      break;
    }
    case SqlWindowFunctionKind::PERCENT_RANK: {
      const auto percent_rank =
          index_to_percent_rank(output_for_partition_buff, partition_size, comparator);
      std::copy(percent_rank.begin(),
                percent_rank.end(),
                reinterpret_cast<double*>(may_alias_ptr(output_for_partition_buff)));
      break;
    }
    case SqlWindowFunctionKind::CUME_DIST: {
      const auto cume_dist =
          index_to_cume_dist(output_for_partition_buff, partition_size, comparator);
      std::copy(cume_dist.begin(),
                cume_dist.end(),
                reinterpret_cast<double*>(may_alias_ptr(output_for_partition_buff)));
      break;
    }
    case SqlWindowFunctionKind::NTILE: {
      const auto& args = window_func->getArgs();
      CHECK_EQ(args.size(), size_t(1));
      const auto n = get_int_constant_from_expr(args.front().get());
      const auto ntile = index_to_ntile(output_for_partition_buff, partition_size, n);
      std::copy(ntile.begin(), ntile.end(), output_for_partition_buff);
      break;
    }
    case SqlWindowFunctionKind::LAG:
    case SqlWindowFunctionKind::LEAD: {
      const auto lag_or_lead = get_lag_or_lead_argument(window_func);
      const auto partition_row_offsets = payload() + off;
      apply_lag_to_partition(
          lag_or_lead, partition_row_offsets, output_for_partition_buff, partition_size);
      break;
    }
    case SqlWindowFunctionKind::FIRST_VALUE: {
      const auto partition_row_offsets = payload() + off;
      apply_first_value_to_partition(
          partition_row_offsets, output_for_partition_buff, partition_size);
      break;
    }
    case SqlWindowFunctionKind::LAST_VALUE: {
      const auto partition_row_offsets = payload() + off;
      apply_last_value_to_partition(
          partition_row_offsets, output_for_partition_buff, partition_size);
      break;
    }
    case SqlWindowFunctionKind::AVG:
    case SqlWindowFunctionKind::MIN:
    case SqlWindowFunctionKind::MAX:
    case SqlWindowFunctionKind::SUM:
    case SqlWindowFunctionKind::COUNT: {
      const auto partition_row_offsets = payload() + off;
      if (window_function_requires_peer_handling(window_func)) {
        index_to_partition_end(
            partitionEnd(), off, output_for_partition_buff, partition_size, comparator);
      }
      apply_permutation_to_partition(
          output_for_partition_buff, partition_row_offsets, partition_size);
      break;
    }
    default: {
      CIDER_THROW(
          CiderCompileException,
          "Window function not supported yet: " + ::toString(window_func->getKind()));
    }
  }
}

void WindowFunctionContext::fillPartitionStart() {
  CountDistinctDescriptor partition_start_bitmap{CountDistinctImplType::Bitmap,
                                                 SQLTypes::kNULLT,
                                                 0,
                                                 static_cast<int64_t>(elem_count_),
                                                 false,
                                                 1};
  partition_start_ = static_cast<int8_t*>(
      checked_calloc(partition_start_bitmap.bitmapPaddedSizeBytes(), 1));
  int64_t partition_count = partitionCount();
  std::vector<size_t> partition_offsets(partition_count);
  std::partial_sum(counts(), counts() + partition_count, partition_offsets.begin());
  auto partition_start_handle = reinterpret_cast<int64_t>(partition_start_);
  agg_count_distinct_bitmap(&partition_start_handle, 0, 0);
  for (int64_t i = 0; i < partition_count - 1; ++i) {
    agg_count_distinct_bitmap(&partition_start_handle, partition_offsets[i], 0);
  }
}

void WindowFunctionContext::fillPartitionEnd() {
  CountDistinctDescriptor partition_start_bitmap{CountDistinctImplType::Bitmap,
                                                 SQLTypes::kNULLT,
                                                 0,
                                                 static_cast<int64_t>(elem_count_),
                                                 false,
                                                 1};
  partition_end_ = static_cast<int8_t*>(
      checked_calloc(partition_start_bitmap.bitmapPaddedSizeBytes(), 1));
  int64_t partition_count = partitionCount();
  std::vector<size_t> partition_offsets(partition_count);
  std::partial_sum(counts(), counts() + partition_count, partition_offsets.begin());
  auto partition_end_handle = reinterpret_cast<int64_t>(partition_end_);
  for (int64_t i = 0; i < partition_count - 1; ++i) {
    if (partition_offsets[i] == 0) {
      continue;
    }
    agg_count_distinct_bitmap(&partition_end_handle, partition_offsets[i] - 1, 0);
  }
  if (elem_count_) {
    agg_count_distinct_bitmap(&partition_end_handle, elem_count_ - 1, 0);
  }
}

const int32_t* WindowFunctionContext::payload() const {
  if (partitions_) {
    return reinterpret_cast<const int32_t*>(partitions_->getJoinHashBuffer(0) +
                                            partitions_->payloadBufferOff());
  }
  return dummy_payload_;  // non-partitioned window function
}

const int32_t* WindowFunctionContext::offsets() const {
  if (partitions_) {
    return reinterpret_cast<const int32_t*>(partitions_->getJoinHashBuffer(0) +
                                            partitions_->offsetBufferOff());
  }
  return &dummy_offset_;
}

const int32_t* WindowFunctionContext::counts() const {
  if (partitions_) {
    return reinterpret_cast<const int32_t*>(partitions_->getJoinHashBuffer(0) +
                                            partitions_->countBufferOff());
  }
  return &dummy_count_;
}

size_t WindowFunctionContext::partitionCount() const {
  if (partitions_) {
    const auto partition_count = counts() - offsets();
    CHECK_GE(partition_count, 0);
    return partition_count;
  }
  return 1;  // non-partitioned window function
}

void WindowProjectNodeContext::addWindowFunctionContext(
    std::unique_ptr<WindowFunctionContext> window_function_context,
    const size_t target_index) {
  const auto it_ok = window_contexts_.emplace(
      std::make_pair(target_index, std::move(window_function_context)));
  CHECK(it_ok.second);
}

const WindowFunctionContext* WindowProjectNodeContext::activateWindowFunctionContext(
    Executor* executor,
    const size_t target_index) const {
  const auto it = window_contexts_.find(target_index);
  CHECK(it != window_contexts_.end());
  executor->active_window_function_ = it->second.get();
  return executor->active_window_function_;
}

void WindowProjectNodeContext::resetWindowFunctionContext(Executor* executor) {
  executor->active_window_function_ = nullptr;
}

WindowFunctionContext* WindowProjectNodeContext::getActiveWindowFunctionContext(
    Executor* executor) {
  return executor->active_window_function_;
}

WindowProjectNodeContext* WindowProjectNodeContext::create(Executor* executor) {
  executor->window_project_node_context_owned_ =
      std::make_unique<WindowProjectNodeContext>();
  return executor->window_project_node_context_owned_.get();
}

const WindowProjectNodeContext* WindowProjectNodeContext::get(Executor* executor) {
  return executor->window_project_node_context_owned_.get();
}

void WindowProjectNodeContext::reset(Executor* executor) {
  executor->window_project_node_context_owned_ = nullptr;
  executor->active_window_function_ = nullptr;
}
