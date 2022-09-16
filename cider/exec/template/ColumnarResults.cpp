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

#include "ColumnarResults.h"
#include "ErrorHandling.h"
#include "Execute.h"
#include "exec/template/common/descriptors/RowSetMemoryOwner.h"
#include "util/Intervals.h"
#include "util/likely.h"
#include "util/thread_count.h"

#include <atomic>
#include <future>
#include <numeric>

extern bool g_enable_non_kernel_time_query_interrupt;
namespace {

inline int64_t fixed_encoding_nullable_val(const int64_t val,
                                           const SQLTypeInfo& type_info) {
  if (type_info.get_compression() != kENCODING_NONE) {
    CHECK(type_info.get_compression() == kENCODING_FIXED ||
          type_info.get_compression() == kENCODING_DICT);
    auto logical_ti = get_logical_type_info(type_info);
    if (val == inline_int_null_val(logical_ti)) {
      return inline_fixed_encoding_null_val(type_info);
    }
  }
  return val;
}

}  // namespace

ColumnarResults::ColumnarResults(std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                                 const int8_t* one_col_buffer,
                                 const size_t num_rows,
                                 const SQLTypeInfo& target_type,
                                 const size_t thread_idx,
                                 Executor* executor)
    : column_buffers_(1)
    , num_rows_(num_rows)
    , target_types_{target_type}
    , parallel_conversion_(false)
    , direct_columnar_conversion_(false)
    , thread_idx_(thread_idx)
    , executor_(executor) {
  auto timer = DEBUG_TIMER(__func__);
  const bool is_varlen =
      target_type.is_array() ||
      (target_type.is_string() && target_type.get_compression() == kENCODING_NONE);

  if (is_varlen) {
    throw ColumnarConversionNotSupported();
  }
  const auto buf_size = num_rows * target_type.get_size();
  column_buffers_[0] =
      reinterpret_cast<int8_t*>(row_set_mem_owner->allocate(buf_size, thread_idx_));
  memcpy(((void*)column_buffers_[0]), one_col_buffer, buf_size);
}

std::unique_ptr<ColumnarResults> ColumnarResults::mergeResults(
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    const std::vector<std::unique_ptr<ColumnarResults>>& sub_results) {
  if (sub_results.empty()) {
    return nullptr;
  }
  const auto total_row_count = std::accumulate(
      sub_results.begin(),
      sub_results.end(),
      size_t(0),
      [](const size_t init, const std::unique_ptr<ColumnarResults>& result) {
        return init + result->size();
      });
  std::unique_ptr<ColumnarResults> merged_results(
      new ColumnarResults(total_row_count, sub_results[0]->target_types_));
  const auto col_count = sub_results[0]->column_buffers_.size();
  const auto nonempty_it = std::find_if(
      sub_results.begin(),
      sub_results.end(),
      [](const std::unique_ptr<ColumnarResults>& needle) { return needle->size(); });
  if (nonempty_it == sub_results.end()) {
    return nullptr;
  }
  for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
    const auto byte_width = (*nonempty_it)->getColumnType(col_idx).get_size();
    auto write_ptr = row_set_mem_owner->allocate(byte_width * total_row_count);
    merged_results->column_buffers_.push_back(write_ptr);
    for (auto& rs : sub_results) {
      CHECK_EQ(col_count, rs->column_buffers_.size());
      if (!rs->size()) {
        continue;
      }
      CHECK_EQ(byte_width, rs->getColumnType(col_idx).get_size());
      memcpy(write_ptr, rs->column_buffers_[col_idx], rs->size() * byte_width);
      write_ptr += rs->size() * byte_width;
    }
  }
  return merged_results;
}

/*
 * This function processes and decodes its input TargetValue
 * and write it into its corresponding column buffer's cell (with corresponding
 * row and column indices)
 *
 * NOTE: this is not supposed to be processing varlen types, and they should be
 * handled differently outside this function.
 */
inline void ColumnarResults::writeBackCell(const TargetValue& col_val,
                                           const size_t row_idx,
                                           const size_t column_idx) {
  const auto scalar_col_val = boost::get<ScalarTargetValue>(&col_val);
  CHECK(scalar_col_val);
  auto i64_p = boost::get<int64_t>(scalar_col_val);
  const auto& type_info = target_types_[column_idx];
  if (i64_p) {
    const auto val = fixed_encoding_nullable_val(*i64_p, type_info);
    switch (target_types_[column_idx].get_size()) {
      case 1:
        ((int8_t*)column_buffers_[column_idx])[row_idx] = static_cast<int8_t>(val);
        break;
      case 2:
        ((int16_t*)column_buffers_[column_idx])[row_idx] = static_cast<int16_t>(val);
        break;
      case 4:
        ((int32_t*)column_buffers_[column_idx])[row_idx] = static_cast<int32_t>(val);
        break;
      case 8:
        ((int64_t*)column_buffers_[column_idx])[row_idx] = val;
        break;
      default:
        CHECK(false);
    }
  } else {
    CHECK(target_types_[column_idx].is_fp());
    switch (target_types_[column_idx].get_type()) {
      case kFLOAT: {
        auto float_p = boost::get<float>(scalar_col_val);
        ((float*)column_buffers_[column_idx])[row_idx] = static_cast<float>(*float_p);
        break;
      }
      case kDOUBLE: {
        auto double_p = boost::get<double>(scalar_col_val);
        ((double*)column_buffers_[column_idx])[row_idx] = static_cast<double>(*double_p);
        break;
      }
      default:
        CHECK(false);
    }
  }
}