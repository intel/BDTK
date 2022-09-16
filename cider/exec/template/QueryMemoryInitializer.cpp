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

#include "QueryMemoryInitializer.h"

#include "Execute.h"
#include "OutputBufferInitialization.h"
#include "ResultSet.h"
#include "StreamingTopN.h"
#include "util/Logger.h"

#include <robin_hood.h>
#include <x86intrin.h>
#include "util/checked_alloc.h"

// 8 GB, the limit of perfect hash group by under normal conditions
int64_t g_bitmap_memory_limit{8LL * 1000 * 1000 * 1000};

extern bool g_optimize_row_initialization;
extern size_t g_max_memory_allocation_size;

namespace {

inline void check_total_bitmap_memory(const QueryMemoryDescriptor& query_mem_desc) {
  const int32_t groups_buffer_entry_count = query_mem_desc.getEntryCount();
  checked_int64_t total_bytes_per_group = 0;
  const size_t num_count_distinct_descs =
      query_mem_desc.getCountDistinctDescriptorsSize();
  for (size_t i = 0; i < num_count_distinct_descs; i++) {
    const auto count_distinct_desc = query_mem_desc.getCountDistinctDescriptor(i);
    if (count_distinct_desc.impl_type_ != CountDistinctImplType::Bitmap) {
      continue;
    }
    total_bytes_per_group += count_distinct_desc.bitmapPaddedSizeBytes();
  }
  int64_t total_bytes{0};
  try {
    total_bytes = static_cast<int64_t>(total_bytes_per_group * groups_buffer_entry_count);
  } catch (...) {
    // Absurd amount of memory, merely computing the number of bits overflows int64_t.
    // Don't bother to report the real amount, this is unlikely to ever happen.
    CIDER_THROW(CiderOutOfMemoryException,
                "Not enough CPU memory available to allocate " +
                    std::to_string(std::numeric_limits<int64_t>::max() / 8) + " bytes");
  }
  if (total_bytes >= g_bitmap_memory_limit) {
    CIDER_THROW(CiderOutOfMemoryException,
                "Not enough CPU memory available to allocate " +
                    std::to_string(total_bytes) + " bytes");
  }
}

int64_t* alloc_group_by_buffer(const size_t numBytes,
                               const size_t thread_idx,
                               RowSetMemoryOwner* mem_owner) {
  return reinterpret_cast<int64_t*>(mem_owner->allocate(numBytes, thread_idx));
}

inline int64_t get_consistent_frag_size(const std::vector<uint64_t>& frag_offsets) {
  if (frag_offsets.size() < 2) {
    return int64_t(-1);
  }
  const auto frag_size = frag_offsets[1] - frag_offsets[0];
  for (size_t i = 2; i < frag_offsets.size(); ++i) {
    const auto curr_size = frag_offsets[i] - frag_offsets[i - 1];
    if (curr_size != frag_size) {
      return int64_t(-1);
    }
  }
  return !frag_size ? std::numeric_limits<int64_t>::max()
                    : static_cast<int64_t>(frag_size);
}

inline std::vector<int64_t> get_consistent_frags_sizes(
    const std::vector<std::vector<uint64_t>>& frag_offsets) {
  if (frag_offsets.empty()) {
    return {};
  }
  std::vector<int64_t> frag_sizes;
  for (size_t tab_idx = 0; tab_idx < frag_offsets[0].size(); ++tab_idx) {
    std::vector<uint64_t> tab_offs;
    for (auto& offsets : frag_offsets) {
      tab_offs.push_back(offsets[tab_idx]);
    }
    frag_sizes.push_back(get_consistent_frag_size(tab_offs));
  }
  return frag_sizes;
}

inline std::vector<int64_t> get_consistent_frags_sizes(
    const std::vector<Analyzer::Expr*>& target_exprs,
    const std::vector<int64_t>& table_frag_sizes) {
  std::vector<int64_t> col_frag_sizes;
  for (auto expr : target_exprs) {
    if (const auto col_var = dynamic_cast<Analyzer::ColumnVar*>(expr)) {
      if (col_var->get_rte_idx() < 0) {
        CHECK_EQ(-1, col_var->get_rte_idx());
        col_frag_sizes.push_back(int64_t(-1));
      } else {
        col_frag_sizes.push_back(table_frag_sizes[col_var->get_rte_idx()]);
      }
    } else {
      col_frag_sizes.push_back(int64_t(-1));
    }
  }
  return col_frag_sizes;
}

inline std::vector<std::vector<int64_t>> get_col_frag_offsets(
    const std::vector<Analyzer::Expr*>& target_exprs,
    const std::vector<std::vector<uint64_t>>& table_frag_offsets) {
  std::vector<std::vector<int64_t>> col_frag_offsets;
  for (auto& table_offsets : table_frag_offsets) {
    std::vector<int64_t> col_offsets;
    for (auto expr : target_exprs) {
      if (const auto col_var = dynamic_cast<Analyzer::ColumnVar*>(expr)) {
        if (col_var->get_rte_idx() < 0) {
          CHECK_EQ(-1, col_var->get_rte_idx());
          col_offsets.push_back(int64_t(-1));
        } else {
          CHECK_LT(static_cast<size_t>(col_var->get_rte_idx()), table_offsets.size());
          col_offsets.push_back(
              static_cast<int64_t>(table_offsets[col_var->get_rte_idx()]));
        }
      } else {
        col_offsets.push_back(int64_t(-1));
      }
    }
    col_frag_offsets.push_back(col_offsets);
  }
  return col_frag_offsets;
}

constexpr size_t const_log2(size_t v) {
  if (v == 1) {
    return 0;
  } else {
    assert(!(v & 1));
    return const_log2(v / 2) + 1;
  }
}

// Return a min number of rows required to fill a pattern
// divisible by vec_size.
template <size_t vec_size>
size_t get_num_rows_for_vec_sample(const size_t row_size) {
  auto rem = row_size & (vec_size - 1);
  if (!rem) {
    return 1;
  }

  auto vp2 = const_log2(vec_size);
  auto p2 = 0;
  while (!(rem & 1)) {
    rem >>= 1;
    p2++;
  }

  return 1 << (vp2 - p2);
}

// It's assumed sample has 64 extra bytes to handle alignment.
__attribute__((target("avx512f"))) void spread_vec_sample(int8_t* dst,
                                                          const size_t dst_size,
                                                          const int8_t* sample_ptr,
                                                          const size_t sample_size) {
  assert((reinterpret_cast<uint64_t>(dst) & 0x3F) ==
         (reinterpret_cast<uint64_t>(sample_ptr) & 0x3F));
  // Align buffers.
  int64_t align_bytes = ((64ULL - reinterpret_cast<uint64_t>(dst)) & 0x3F);
  memcpy(dst, sample_ptr, align_bytes);

  int8_t* align_dst = dst + align_bytes;
  const int8_t* align_sample = sample_ptr + align_bytes;
  size_t rem = dst_size - align_bytes;
  size_t rem_scalar = rem % sample_size;
  size_t rem_vector = rem - rem_scalar;

  // Aligned vector part.
  auto vecs = sample_size / 64;
  auto vec_end = align_dst + rem_vector;
  while (align_dst < vec_end) {
    for (size_t i = 0; i < vecs; ++i) {
      __m512i vec_val =
          _mm512_load_si512(reinterpret_cast<const __m512i*>(align_sample) + i);
      _mm512_stream_si512(reinterpret_cast<__m512i*>(align_dst) + i, vec_val);
    }
    align_dst += sample_size;
  }

  // Scalar tail.
  memcpy(align_dst, align_sample, rem_scalar);
}

__attribute__((target("default"))) void spread_vec_sample(int8_t* dst,
                                                          const size_t dst_size,
                                                          const int8_t* sample_ptr,
                                                          const size_t sample_size) {
  size_t rem = dst_size;
  while (rem >= sample_size) {
    memcpy(dst, sample_ptr, sample_size);
    rem -= sample_size;
    dst += sample_size;
  }
  memcpy(dst, sample_ptr, rem);
}

}  // namespace

// Row-based execution constructor
QueryMemoryInitializer::QueryMemoryInitializer(
    const RelAlgExecutionUnit& ra_exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const int device_id,
    const ExecutorDispatchMode dispatch_mode,
    const bool output_columnar,
    const int64_t num_rows,
    const std::vector<std::vector<const int8_t*>>& col_buffers,
    const std::vector<std::vector<uint64_t>>& frag_offsets,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    const size_t thread_idx,
    const Executor* executor)
    : num_rows_(num_rows)
    , row_set_mem_owner_(row_set_mem_owner)
    , init_agg_vals_(executor->plan_state_->init_agg_vals_)
    , num_buffers_(1)
    , varlen_output_buffer_(0)
    , varlen_output_buffer_host_ptr_(nullptr)
    , count_distinct_bitmap_mem_(0)
    , count_distinct_bitmap_mem_bytes_(0)
    , count_distinct_bitmap_crt_ptr_(nullptr)
    , count_distinct_bitmap_host_mem_(nullptr)
    , thread_idx_(thread_idx) {
  CHECK(output_columnar);

  const auto& consistent_frag_sizes = get_consistent_frags_sizes(frag_offsets);
  if (consistent_frag_sizes.empty()) {
    // No fragments in the input, no underlying buffers will be needed.
    return;
  }
  if (!ra_exe_unit.use_bump_allocator) {
    check_total_bitmap_memory(query_mem_desc);
  }
  if (ra_exe_unit.estimator) {
    return;
  }

  const auto thread_count = 1;

  size_t group_buffer_size{0};
  if (ra_exe_unit.use_bump_allocator) {
    // For kernel per fragment execution, just allocate a buffer equivalent to the size of
    // the fragment
    if (dispatch_mode == ExecutorDispatchMode::KernelPerFragment) {
      group_buffer_size = num_rows * query_mem_desc.getRowSize();
    } else {
      group_buffer_size = g_max_memory_allocation_size / query_mem_desc.getRowSize();
    }
  } else {
    group_buffer_size = query_mem_desc.getBufferSizeBytes(ra_exe_unit, thread_count);
  }
  CHECK_GE(group_buffer_size, size_t(0));

  const auto group_buffers_count = !query_mem_desc.isGroupBy() ? 1 : num_buffers_;
  int64_t* group_by_buffer_template{nullptr};
  if (group_buffers_count > 1) {
    group_by_buffer_template = reinterpret_cast<int64_t*>(
        row_set_mem_owner_->allocate(group_buffer_size, thread_idx_));
    initGroupByBuffer(
        group_by_buffer_template, ra_exe_unit, query_mem_desc, output_columnar, executor);
  }

  const auto step = size_t(1);
  const auto index_buffer_qw = size_t(0);
  const auto actual_group_buffer_size =
      group_buffer_size + index_buffer_qw * sizeof(int64_t);
  CHECK_GE(actual_group_buffer_size, group_buffer_size);

  if (query_mem_desc.hasVarlenOutput()) {
    const auto varlen_buffer_elem_size_opt = query_mem_desc.varlenOutputBufferElemSize();
    CHECK(varlen_buffer_elem_size_opt);  // TODO(adb): relax
    auto varlen_output_buffer = reinterpret_cast<int64_t*>(row_set_mem_owner_->allocate(
        query_mem_desc.getEntryCount() * varlen_buffer_elem_size_opt.value()));
    num_buffers_ += 1;
    group_by_buffers_.push_back(varlen_output_buffer);
  }

  for (size_t i = 0; i < group_buffers_count; i += step) {
    auto group_by_buffer = alloc_group_by_buffer(
        actual_group_buffer_size, thread_idx_, row_set_mem_owner_.get());

    if (group_by_buffer_template) {
      memcpy(
          group_by_buffer + index_buffer_qw, group_by_buffer_template, group_buffer_size);
    } else {
      initGroupByBuffer(group_by_buffer + index_buffer_qw,
                        ra_exe_unit,
                        query_mem_desc,
                        output_columnar,
                        executor);
    }
    group_by_buffers_.push_back(group_by_buffer);
    for (size_t j = 1; j < step; ++j) {
      group_by_buffers_.push_back(nullptr);
    }
    const auto column_frag_offsets =
        get_col_frag_offsets(ra_exe_unit.target_exprs, frag_offsets);
    const auto column_frag_sizes =
        get_consistent_frags_sizes(ra_exe_unit.target_exprs, consistent_frag_sizes);
  }
}

void QueryMemoryInitializer::initGroupByBuffer(
    int64_t* buffer,
    const RelAlgExecutionUnit& ra_exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const bool output_columnar,
    const Executor* executor) {
  if (output_columnar) {
    // When we run CPU projection code, we always compute a number of matched rows.
    // This number is then used for columnar output buffer compaction (see
    // compactProjectionBuffersCpu). This means we would never read uninitialized keys.
    if (query_mem_desc.getQueryDescriptionType() != QueryDescriptionType::Projection) {
      initColumnarGroups(query_mem_desc, buffer, init_agg_vals_, executor);
    }
  } else {
    auto rows_ptr = buffer;
    auto actual_entry_count = query_mem_desc.getEntryCount();
    const auto thread_count = 1;
    auto warp_size = 1;
    if (query_mem_desc.useStreamingTopN()) {
      const auto node_count_size = thread_count * sizeof(int64_t);
      memset(rows_ptr, 0, node_count_size);
      const auto n = ra_exe_unit.sort_info.offset + ra_exe_unit.sort_info.limit;
      const auto rows_offset = streaming_top_n::get_rows_offset_of_heaps(n, thread_count);
      memset(rows_ptr + thread_count, -1, rows_offset - node_count_size);
      rows_ptr += rows_offset / sizeof(int64_t);
      actual_entry_count = n * thread_count;
      warp_size = 1;
    }
    initRowGroups(query_mem_desc,
                  rows_ptr,
                  init_agg_vals_,
                  actual_entry_count,
                  warp_size,
                  executor);
  }
}

bool QueryMemoryInitializer::useVectorRowGroupsInit(const size_t row_size,
                                                    const size_t entries) const {
  if (!g_optimize_row_initialization) {
    return false;
  }

  // Assume 512-bit vector size. Don't bother if
  // the sample is too big.
  auto rows_per_sample = get_num_rows_for_vec_sample<64>(row_size);
  return entries / rows_per_sample > 3;
}

void QueryMemoryInitializer::initRowGroups(const QueryMemoryDescriptor& query_mem_desc,
                                           int64_t* groups_buffer,
                                           const std::vector<int64_t>& init_vals,
                                           const int32_t groups_buffer_entry_count,
                                           const size_t warp_size,
                                           const Executor* executor) {
  const size_t key_count{query_mem_desc.getGroupbyColCount()};
  const size_t row_size{query_mem_desc.getRowSize()};
  const size_t col_base_off{query_mem_desc.getColOffInBytes(0)};

  auto agg_bitmap_size = allocateCountDistinctBuffers(query_mem_desc, true, executor);
  auto quantile_params = allocateTDigests(query_mem_desc, true, executor);
  auto buffer_ptr = reinterpret_cast<int8_t*>(groups_buffer);

  const auto query_mem_desc_fixedup =
      ResultSet::fixupQueryMemoryDescriptor(query_mem_desc);

  auto const is_true = [](auto const& x) { return static_cast<bool>(x); };
  // not COUNT DISTINCT / APPROX_COUNT_DISTINCT / APPROX_QUANTILE
  // we fallback to default implementation in that cases
  if (!std::any_of(agg_bitmap_size.begin(), agg_bitmap_size.end(), is_true) &&
      !std::any_of(quantile_params.begin(), quantile_params.end(), is_true) &&
      useVectorRowGroupsInit(row_size, groups_buffer_entry_count * warp_size)) {
    auto rows_per_sample = get_num_rows_for_vec_sample<64>(row_size);
    auto sample_size = row_size * rows_per_sample;
    // Additional bytes are required to achieve the same alignment
    // as groups_buffer has and still use aligned vector operations to
    // copy the whole sample.
    std::vector<int8_t> vec_sample(sample_size + 128);
    int8_t* sample_ptr = vec_sample.data();
    sample_ptr += (reinterpret_cast<uint64_t>(buffer_ptr) -
                   reinterpret_cast<uint64_t>(sample_ptr)) &
                  0x3F;

    for (size_t i = 0; i < rows_per_sample; ++i) {
      initColumnsPerRow(query_mem_desc_fixedup,
                        sample_ptr + row_size * i + col_base_off,
                        init_vals,
                        agg_bitmap_size,
                        quantile_params);
    }

    size_t rows_count = groups_buffer_entry_count;
    if (query_mem_desc.hasKeylessHash()) {
      CHECK(warp_size >= 1);
      CHECK(key_count == 1 || warp_size == 1);
      rows_count *= warp_size;
    } else {
      for (size_t i = 0; i < rows_per_sample; ++i) {
        ResultSet::fill_empty_key(
            sample_ptr + row_size * i, key_count, query_mem_desc.getEffectiveKeyWidth());
      }
    }

    // Duplicate the first 64 bytes of the sample to enable
    // copy after alignment.
    memcpy(sample_ptr + sample_size, sample_ptr, 64);

    spread_vec_sample(buffer_ptr, rows_count * row_size, sample_ptr, sample_size);
  } else {
    if (query_mem_desc.hasKeylessHash()) {
      CHECK(warp_size >= 1);
      CHECK(key_count == 1 || warp_size == 1);
      for (size_t warp_idx = 0; warp_idx < warp_size; ++warp_idx) {
        for (size_t bin = 0; bin < static_cast<size_t>(groups_buffer_entry_count);
             ++bin, buffer_ptr += row_size) {
          initColumnsPerRow(query_mem_desc_fixedup,
                            &buffer_ptr[col_base_off],
                            init_vals,
                            agg_bitmap_size,
                            quantile_params);
        }
      }
      return;
    }

    for (size_t bin = 0; bin < static_cast<size_t>(groups_buffer_entry_count);
         ++bin, buffer_ptr += row_size) {
      ResultSet::fill_empty_key(
          buffer_ptr, key_count, query_mem_desc.getEffectiveKeyWidth());
      initColumnsPerRow(query_mem_desc_fixedup,
                        &buffer_ptr[col_base_off],
                        init_vals,
                        agg_bitmap_size,
                        quantile_params);
    }
  }
}

namespace {

template <typename T>
int8_t* initColumnarBuffer(T* buffer_ptr, const T init_val, const uint32_t entry_count) {
  static_assert(sizeof(T) <= sizeof(int64_t), "Unsupported template type");
  for (uint32_t i = 0; i < entry_count; ++i) {
    buffer_ptr[i] = init_val;
  }
  return reinterpret_cast<int8_t*>(buffer_ptr + entry_count);
}

}  // namespace

void QueryMemoryInitializer::initColumnarGroups(
    const QueryMemoryDescriptor& query_mem_desc,
    int64_t* groups_buffer,
    const std::vector<int64_t>& init_vals,
    const Executor* executor) {
  CHECK(groups_buffer);
  for (const auto target_expr : executor->plan_state_->target_exprs_) {
    const auto agg_info = get_target_info(target_expr, g_bigint_count);
    CHECK(!is_distinct_target(agg_info));
  }
  const int32_t agg_col_count = query_mem_desc.getSlotCount();
  auto buffer_ptr = reinterpret_cast<int8_t*>(groups_buffer);

  const auto groups_buffer_entry_count = query_mem_desc.getEntryCount();
  if (!query_mem_desc.hasKeylessHash()) {
    const size_t key_count{query_mem_desc.getGroupbyColCount()};
    for (size_t i = 0; i < key_count; ++i) {
      buffer_ptr = initColumnarBuffer<int64_t>(reinterpret_cast<int64_t*>(buffer_ptr),
                                               EMPTY_KEY_64,
                                               groups_buffer_entry_count);
    }
  }

  if (query_mem_desc.getQueryDescriptionType() != QueryDescriptionType::Projection) {
    // initializing all aggregate columns:
    int32_t init_val_idx = 0;
    for (int32_t i = 0; i < agg_col_count; ++i) {
      if (query_mem_desc.getPaddedSlotWidthBytes(i) > 0) {
        CHECK_LT(static_cast<size_t>(init_val_idx), init_vals.size());
        switch (query_mem_desc.getPaddedSlotWidthBytes(i)) {
          case 1:
            buffer_ptr = initColumnarBuffer<int8_t>(
                buffer_ptr, init_vals[init_val_idx++], groups_buffer_entry_count);
            break;
          case 2:
            buffer_ptr =
                initColumnarBuffer<int16_t>(reinterpret_cast<int16_t*>(buffer_ptr),
                                            init_vals[init_val_idx++],
                                            groups_buffer_entry_count);
            break;
          case 4:
            buffer_ptr =
                initColumnarBuffer<int32_t>(reinterpret_cast<int32_t*>(buffer_ptr),
                                            init_vals[init_val_idx++],
                                            groups_buffer_entry_count);
            break;
          case 8:
            buffer_ptr =
                initColumnarBuffer<int64_t>(reinterpret_cast<int64_t*>(buffer_ptr),
                                            init_vals[init_val_idx++],
                                            groups_buffer_entry_count);
            break;
          case 0:
            break;
          default:
            CHECK(false);
        }

        buffer_ptr = align_to_int64(buffer_ptr);
      }
    }
  }
}

void QueryMemoryInitializer::initColumnsPerRow(
    const QueryMemoryDescriptor& query_mem_desc,
    int8_t* row_ptr,
    const std::vector<int64_t>& init_vals,
    const std::vector<int64_t>& bitmap_sizes,
    const std::vector<QuantileParam>& quantile_params) {
  int8_t* col_ptr = row_ptr;
  size_t init_vec_idx = 0;
  for (size_t col_idx = 0; col_idx < query_mem_desc.getSlotCount();
       col_ptr += query_mem_desc.getNextColOffInBytesRowOnly(col_ptr, col_idx++)) {
    const int64_t bm_sz{bitmap_sizes[col_idx]};
    int64_t init_val{0};
    if (bm_sz && query_mem_desc.isGroupBy()) {
      // COUNT DISTINCT / APPROX_COUNT_DISTINCT
      CHECK_EQ(static_cast<size_t>(query_mem_desc.getPaddedSlotWidthBytes(col_idx)),
               sizeof(int64_t));
      init_val =
          bm_sz > 0 ? allocateCountDistinctBitmap(bm_sz) : allocateCountDistinctSet();
      ++init_vec_idx;
    } else if (query_mem_desc.isGroupBy() && quantile_params[col_idx]) {
      auto const q = *quantile_params[col_idx];
      // allocate for APPROX_QUANTILE only when slot is used
      init_val = reinterpret_cast<int64_t>(row_set_mem_owner_->nullTDigest(q));
      ++init_vec_idx;
    } else {
      if (query_mem_desc.getPaddedSlotWidthBytes(col_idx) > 0) {
        CHECK_LT(init_vec_idx, init_vals.size());
        init_val = init_vals[init_vec_idx++];
      }
    }
    switch (query_mem_desc.getPaddedSlotWidthBytes(col_idx)) {
      case 1:
        *col_ptr = static_cast<int8_t>(init_val);
        break;
      case 2:
        *reinterpret_cast<int16_t*>(col_ptr) = (int16_t)init_val;
        break;
      case 4:
        *reinterpret_cast<int32_t*>(col_ptr) = (int32_t)init_val;
        break;
      case 8:
        *reinterpret_cast<int64_t*>(col_ptr) = init_val;
        break;
      case 0:
        continue;
      default:
        CHECK(false);
    }
  }
}

// deferred is true for group by queries; initGroups will allocate a bitmap
// for each group slot
std::vector<int64_t> QueryMemoryInitializer::allocateCountDistinctBuffers(
    const QueryMemoryDescriptor& query_mem_desc,
    const bool deferred,
    const Executor* executor) {
  const size_t agg_col_count{query_mem_desc.getSlotCount()};
  std::vector<int64_t> agg_bitmap_size(deferred ? agg_col_count : 0);

  CHECK_GE(agg_col_count, executor->plan_state_->target_exprs_.size());
  for (size_t target_idx = 0; target_idx < executor->plan_state_->target_exprs_.size();
       ++target_idx) {
    const auto target_expr = executor->plan_state_->target_exprs_[target_idx];
    const auto agg_info = get_target_info(target_expr, g_bigint_count);
    if (is_distinct_target(agg_info)) {
      CHECK(agg_info.is_agg &&
            (agg_info.agg_kind == kCOUNT || agg_info.agg_kind == kAPPROX_COUNT_DISTINCT));
      CHECK(!agg_info.sql_type.is_varlen());

      const size_t agg_col_idx = query_mem_desc.getSlotIndexForSingleSlotCol(target_idx);
      CHECK_LT(static_cast<size_t>(agg_col_idx), agg_col_count);

      CHECK_EQ(static_cast<size_t>(query_mem_desc.getLogicalSlotWidthBytes(agg_col_idx)),
               sizeof(int64_t));
      const auto& count_distinct_desc =
          query_mem_desc.getCountDistinctDescriptor(target_idx);
      CHECK(count_distinct_desc.impl_type_ != CountDistinctImplType::Invalid);
      if (count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
        const auto bitmap_byte_sz = count_distinct_desc.bitmapPaddedSizeBytes();
        if (deferred) {
          agg_bitmap_size[agg_col_idx] = bitmap_byte_sz;
        } else {
          init_agg_vals_[agg_col_idx] = allocateCountDistinctBitmap(bitmap_byte_sz);
        }
      } else {
        CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
        if (deferred) {
          agg_bitmap_size[agg_col_idx] = -1;
        } else {
          init_agg_vals_[agg_col_idx] = allocateCountDistinctSet();
        }
      }
    }
  }

  return agg_bitmap_size;
}

int64_t QueryMemoryInitializer::allocateCountDistinctBitmap(const size_t bitmap_byte_sz) {
  if (count_distinct_bitmap_host_mem_) {
    CHECK(count_distinct_bitmap_crt_ptr_);
    auto ptr = count_distinct_bitmap_crt_ptr_;
    count_distinct_bitmap_crt_ptr_ += bitmap_byte_sz;
    row_set_mem_owner_->addCountDistinctBuffer(
        ptr, bitmap_byte_sz, /*physial_buffer=*/false);
    return reinterpret_cast<int64_t>(ptr);
  }
  return reinterpret_cast<int64_t>(
      row_set_mem_owner_->allocateCountDistinctBuffer(bitmap_byte_sz, thread_idx_));
}

int64_t QueryMemoryInitializer::allocateCountDistinctSet() {
  auto count_distinct_set = new robin_hood::unordered_set<int64_t>();
  row_set_mem_owner_->addCountDistinctSet(count_distinct_set);
  return reinterpret_cast<int64_t>(count_distinct_set);
}

std::vector<QueryMemoryInitializer::QuantileParam>
QueryMemoryInitializer::allocateTDigests(const QueryMemoryDescriptor& query_mem_desc,
                                         const bool deferred,
                                         const Executor* executor) {
  size_t const slot_count = query_mem_desc.getSlotCount();
  size_t const ntargets = executor->plan_state_->target_exprs_.size();
  CHECK_GE(slot_count, ntargets);
  std::vector<QuantileParam> quantile_params(deferred ? slot_count : 0);

  for (size_t target_idx = 0; target_idx < ntargets; ++target_idx) {
    auto const target_expr = executor->plan_state_->target_exprs_[target_idx];
    if (auto const agg_expr = dynamic_cast<const Analyzer::AggExpr*>(target_expr)) {
      if (agg_expr->get_aggtype() == kAPPROX_QUANTILE) {
        size_t const agg_col_idx =
            query_mem_desc.getSlotIndexForSingleSlotCol(target_idx);
        CHECK_LT(agg_col_idx, slot_count);
        CHECK_EQ(query_mem_desc.getLogicalSlotWidthBytes(agg_col_idx),
                 static_cast<int8_t>(sizeof(int64_t)));
        auto const q = agg_expr->get_arg1()->get_constval().doubleval;
        if (deferred) {
          quantile_params[agg_col_idx] = q;
        } else {
          // allocate for APPROX_QUANTILE only when slot is used
          init_agg_vals_[agg_col_idx] =
              reinterpret_cast<int64_t>(row_set_mem_owner_->nullTDigest(q));
        }
      }
    }
  }
  return quantile_params;
}

namespace {

// in-place compaction of output buffer
void compact_projection_buffer_for_cpu_columnar(
    const QueryMemoryDescriptor& query_mem_desc,
    int8_t* projection_buffer,
    const size_t projection_count) {
  // the first column (row indices) remains unchanged.
  CHECK(projection_count <= query_mem_desc.getEntryCount());
  constexpr size_t row_index_width = sizeof(int64_t);
  size_t buffer_offset1{projection_count * row_index_width};
  // other columns are actual non-lazy columns for the projection:
  for (size_t i = 0; i < query_mem_desc.getSlotCount(); i++) {
    if (query_mem_desc.getPaddedSlotWidthBytes(i) > 0) {
      auto column_proj_size =
          projection_count * query_mem_desc.getPaddedSlotWidthBytes(i);
      auto buffer_offset2 = query_mem_desc.getColOffInBytes(i);
      if (buffer_offset1 + column_proj_size >= buffer_offset2) {
        // overlapping
        std::memmove(projection_buffer + buffer_offset1,
                     projection_buffer + buffer_offset2,
                     column_proj_size);
      } else {
        std::memcpy(projection_buffer + buffer_offset1,
                    projection_buffer + buffer_offset2,
                    column_proj_size);
      }
      buffer_offset1 += align_to_int64(column_proj_size);
    }
  }
}

}  // namespace

void QueryMemoryInitializer::applyStreamingTopNOffsetCpu(
    const QueryMemoryDescriptor& query_mem_desc,
    const RelAlgExecutionUnit& ra_exe_unit) {
  const size_t buffer_start_idx = query_mem_desc.hasVarlenOutput() ? 1 : 0;
  CHECK_EQ(group_by_buffers_.size(), buffer_start_idx + 1);

  const auto rows_copy = streaming_top_n::get_rows_copy_from_heaps(
      group_by_buffers_[buffer_start_idx],
      query_mem_desc.getBufferSizeBytes(ra_exe_unit, 1),
      ra_exe_unit.sort_info.offset + ra_exe_unit.sort_info.limit,
      1);
  CHECK_EQ(rows_copy.size(),
           query_mem_desc.getEntryCount() * query_mem_desc.getRowSize());
  memcpy(group_by_buffers_[buffer_start_idx], &rows_copy[0], rows_copy.size());
}
