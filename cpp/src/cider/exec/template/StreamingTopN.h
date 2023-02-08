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

#ifndef QUERYENGINE_STREAMINGTOPN_H
#define QUERYENGINE_STREAMINGTOPN_H

#include <cstddef>
#include <cstdint>
#include <vector>

namespace streaming_top_n {

size_t get_heap_size(const size_t row_size, const size_t n, const size_t thread_count);

size_t get_rows_offset_of_heaps(const size_t n, const size_t thread_count);

std::vector<int8_t> get_rows_copy_from_heaps(const int64_t* heaps,
                                             const size_t heaps_size,
                                             const size_t n,
                                             const size_t thread_count);

}  // namespace streaming_top_n

struct RelAlgExecutionUnit;

namespace Analyzer {
class Expr;
}  // namespace Analyzer

// Compute the slot index where the target given by target_idx is stored, where
// target_exprs is the list all projected expressions.
size_t get_heap_key_slot_index(const std::vector<Analyzer::Expr*>& target_exprs,
                               const size_t target_idx);

#endif  // QUERYENGINE_STREAMINGTOPN_H
