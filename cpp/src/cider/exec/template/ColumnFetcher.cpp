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

#include "exec/template/ColumnFetcher.h"

#include <memory>

#include "cider/CiderException.h"
#include "exec/template/Execute.h"
#include "type/data/sqltypes.h"
#include "util/Intervals.h"
#include "util/encode/ArrayNoneEncoder.h"
#include "util/likely.h"

extern bool g_enable_non_kernel_time_query_interrupt;

//! Gets a column fragment chunk on CPU depending on the effective
//! memory level parameter. Returns a buffer pointer and an element count.
std::pair<const int8_t*, size_t> ColumnFetcher::getOneColumnFragment(
    Executor* executor,
    const Analyzer::ColumnVar& hash_col,
    const Fragmenter_Namespace::FragmentInfo& fragment,
    const Data_Namespace::MemoryLevel effective_mem_lvl,
    const int device_id,
    const size_t thread_idx,
    std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache) {
  CHECK(data_provider);
  static std::mutex columnar_conversion_mutex;
  auto timer = DEBUG_TIMER(__func__);
  if (fragment.isEmptyPhysicalFragment()) {
    return {nullptr, 0};
  }
  const auto table_id = hash_col.get_table_id();
  const auto col_info = hash_col.get_column_info();
  const int8_t* col_buff = nullptr;
  if (table_id >= 0) {  // real table
    /* chunk_meta_it is used here to retrieve chunk numBytes and
       numElements. Apparently, their values are often zeros. If we
       knew how to predict the zero values, calling
       getChunkMetadataMap could be avoided to skip
       synthesize_metadata calls. */
    // Cider will not use and cannot build this map, so remove it.
    // auto chunk_meta_it = fragment.getChunkMetadataMap().find(hash_col.get_column_id());
    // CHECK(chunk_meta_it != fragment.getChunkMetadataMap().end());
    ChunkKey chunk_key{col_info->db_id,
                       fragment.physicalTableId,
                       hash_col.get_column_id(),
                       fragment.fragmentId};
    const auto chunk = data_provider->getChunk(
        col_info,
        chunk_key,
        effective_mem_lvl,
        effective_mem_lvl == Data_Namespace::CPU_LEVEL ? 0 : device_id,
        0,
        0);
    // chunk_meta_it->second->numBytes,
    // chunk_meta_it->second->numElements);
    chunks_owner.push_back(chunk);
    CHECK(chunk);
    auto ab = chunk->getBuffer();
    CHECK(ab->getMemoryPtr());
    col_buff = reinterpret_cast<int8_t*>(ab->getMemoryPtr());
  } else {  // temporary table
    const ColumnarResults* col_frag{nullptr};
    {
      std::lock_guard<std::mutex> columnar_conversion_guard(columnar_conversion_mutex);
      const auto frag_id = fragment.fragmentId;
      if (column_cache.empty() || !column_cache.count(table_id)) {
        column_cache.insert(std::make_pair(
            table_id, std::unordered_map<int, std::shared_ptr<const ColumnarResults>>()));
      }
      col_frag = column_cache[table_id][frag_id].get();
    }
    col_buff = transferColumnIfNeeded(
        col_frag,
        hash_col.get_column_id(),
        effective_mem_lvl,
        effective_mem_lvl == Data_Namespace::CPU_LEVEL ? 0 : device_id);
  }
  return {col_buff, fragment.getNumTuples()};
}

//! makeJoinColumn() creates a JoinColumn struct containing a array of
//! JoinChunk structs, col_chunks_buff, malloced in CPU memory. Although
//! the col_chunks_buff array is in CPU memory here, each JoinChunk struct
//! contains an int8_t* pointer from getOneColumnFragment(), col_buff,
//! that can point to either CPU memory depending on the
//! effective_mem_lvl parameter. The
//! malloc_owner parameter will have the malloced array appended. The
//! chunks_owner parameter will be appended with the chunks.
JoinColumn ColumnFetcher::makeJoinColumn(
    Executor* executor,
    const Analyzer::ColumnVar& hash_col,
    const std::vector<Fragmenter_Namespace::FragmentInfo>& fragments,
    const Data_Namespace::MemoryLevel effective_mem_lvl,
    const int device_id,
    const size_t thread_idx,
    std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
    std::vector<std::shared_ptr<void>>& malloc_owner,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache) {
  CHECK(!fragments.empty());

  size_t col_chunks_buff_sz = sizeof(struct JoinChunk) * fragments.size();
  // TODO: needs an allocator owner
  auto col_chunks_buff = reinterpret_cast<int8_t*>(
      malloc_owner.emplace_back(checked_malloc(col_chunks_buff_sz), free).get());
  auto join_chunk_array = reinterpret_cast<struct JoinChunk*>(col_chunks_buff);

  size_t num_elems = 0;
  size_t num_chunks = 0;
  for (auto& frag : fragments) {
    if (g_enable_non_kernel_time_query_interrupt &&
        executor->checkNonKernelTimeInterrupted()) {
      CIDER_THROW(CiderRuntimeException,
                  fmt::format("Query execution failed with error code {}",
                              std::to_string(Executor::ERR_INTERRUPTED)));
    }
    auto [col_buff, elem_count] = getOneColumnFragment(
        executor,
        hash_col,
        frag,
        effective_mem_lvl,
        effective_mem_lvl == Data_Namespace::CPU_LEVEL ? 0 : device_id,
        thread_idx,
        chunks_owner,
        data_provider,
        column_cache);
    if (col_buff != nullptr) {
      num_elems += elem_count;
      join_chunk_array[num_chunks] = JoinChunk{col_buff, elem_count};
    } else {
      continue;
    }
    ++num_chunks;
  }

  int elem_sz = hash_col.get_type_info().get_size();
  CHECK_GT(elem_sz, 0);

  return {col_chunks_buff,
          col_chunks_buff_sz,
          num_chunks,
          num_elems,
          static_cast<size_t>(elem_sz)};
}

const int8_t* ColumnFetcher::transferColumnIfNeeded(
    const ColumnarResults* columnar_results,
    const int col_id,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id) {
  if (!columnar_results) {
    return nullptr;
  }
  const auto& col_buffers = columnar_results->getColumnBuffers();
  CHECK_LT(static_cast<size_t>(col_id), col_buffers.size());
  return col_buffers[col_id];
}
