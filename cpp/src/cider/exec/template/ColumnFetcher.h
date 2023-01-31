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

#include "exec/plan/parser/Translator.h"
#include "exec/template/ColumnarResults.h"
#include "exec/template/operator/join/hashtable/runtime/HashJoinRuntime.h"
#include "util/memory/DataProvider.h"
class ColumnFetcher {
 public:
  //! Gets one chunk's pointer and element count on either CPU
  static std::pair<const int8_t*, size_t> getOneColumnFragment(
      Executor* executor,
      const Analyzer::ColumnVar& hash_col,
      const Fragmenter_Namespace::FragmentInfo& fragment,
      const Data_Namespace::MemoryLevel effective_mem_lvl,
      const int device_id,
      const size_t thread_idx,
      std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache);

  //! Creates a JoinColumn struct containing an array of JoinChunk structs.
  static JoinColumn makeJoinColumn(
      Executor* executor,
      const Analyzer::ColumnVar& hash_col,
      const std::vector<Fragmenter_Namespace::FragmentInfo>& fragments,
      const Data_Namespace::MemoryLevel effective_mem_lvl,
      const int device_id,
      const size_t thread_idx,
      std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
      std::vector<std::shared_ptr<void>>& malloc_owner,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache);

 private:
  static const int8_t* transferColumnIfNeeded(
      const ColumnarResults* columnar_results,
      const int col_id,
      const Data_Namespace::MemoryLevel memory_level,
      const int device_id);
};
