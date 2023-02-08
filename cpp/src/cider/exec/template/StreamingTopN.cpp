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

#include "StreamingTopN.h"
#include "util/checked_alloc.h"

namespace streaming_top_n {

size_t get_heap_size(const size_t row_size, const size_t n, const size_t thread_count) {
  const auto row_size_quad = row_size / sizeof(int64_t);
  return (1 + n + row_size_quad * n) * thread_count * sizeof(int64_t);
}

size_t get_rows_offset_of_heaps(const size_t n, const size_t thread_count) {
  return (1 + n) * thread_count * sizeof(int64_t);
}

std::vector<int8_t> get_rows_copy_from_heaps(const int64_t* heaps,
                                             const size_t heaps_size,
                                             const size_t n,
                                             const size_t thread_count) {
  const auto rows_offset = streaming_top_n::get_rows_offset_of_heaps(n, thread_count);
  const auto row_buff_size = heaps_size - rows_offset;
  std::vector<int8_t> rows_copy(row_buff_size);
  const auto rows_ptr = reinterpret_cast<const int8_t*>(heaps) + rows_offset;
  std::memcpy(&rows_copy[0], rows_ptr, row_buff_size);
  return rows_copy;
}

}  // namespace streaming_top_n

size_t get_heap_key_slot_index(const std::vector<Analyzer::Expr*>& target_exprs,
                               const size_t target_idx) {
  size_t slot_idx = 0;
  for (size_t i = 0; i < target_idx; ++i) {
    auto agg_info = get_target_info(target_exprs[i], g_bigint_count);
    slot_idx = advance_slot(slot_idx, agg_info, false);
  }
  return slot_idx;
}
