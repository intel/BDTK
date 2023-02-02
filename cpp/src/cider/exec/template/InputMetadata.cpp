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

#include "InputMetadata.h"
#include "Execute.h"

#include "util/memory/Fragmenter.h"

#include <future>

InputTableInfoCache::InputTableInfoCache(Executor* executor) : executor_(executor) {}

namespace {

Fragmenter_Namespace::TableInfo copy_table_info(
    const Fragmenter_Namespace::TableInfo& table_info) {
  Fragmenter_Namespace::TableInfo table_info_copy;
  table_info_copy.chunkKeyPrefix = table_info.chunkKeyPrefix;
  table_info_copy.fragments = table_info.fragments;
  table_info_copy.setPhysicalNumTuples(table_info.getPhysicalNumTuples());
  return table_info_copy;
}

}  // namespace

SQLTypeInfo TemporaryTable::getColType(const size_t col_idx) const {
  return results_.front()->getColType(col_idx);
}

Fragmenter_Namespace::TableInfo InputTableInfoCache::getTableInfo(const int table_id) {
  const auto it = cache_.find(table_id);
  if (it != cache_.end()) {
    const auto& table_info = it->second;
    return copy_table_info(table_info);
  }
  const auto data_provider = executor_->getDataProvider();
  CHECK(data_provider);
  auto table_info = data_provider->getTableMetadata(executor_->getDatabaseId(), table_id);
  auto it_ok = cache_.emplace(table_id, copy_table_info(table_info));
  CHECK(it_ok.second);
  return copy_table_info(table_info);
}

void InputTableInfoCache::clear() {
  decltype(cache_)().swap(cache_);
}

namespace {

bool uses_int_meta(const SQLTypeInfo& col_ti) {
  return col_ti.is_integer() || col_ti.is_decimal() || col_ti.is_time() ||
         col_ti.is_boolean() ||
         (col_ti.is_string() && col_ti.get_compression() == kENCODING_DICT);
}

}  // namespace

size_t get_frag_count_of_table(const int table_id, Executor* executor) {
  const auto temporary_tables = executor->getTemporaryTables();
  CHECK(temporary_tables);
  auto it = temporary_tables->find(table_id);
  if (it != temporary_tables->end()) {
    CHECK_GE(int(0), table_id);
    return size_t(1);
  } else {
    const auto table_info = executor->getTableInfo(table_id);
    return table_info.fragments.size();
  }
}

const ChunkMetadataMap& Fragmenter_Namespace::FragmentInfo::getChunkMetadataMap() const {
  return chunkMetadataMap;
}

ChunkMetadataMap Fragmenter_Namespace::FragmentInfo::getChunkMetadataMapPhysicalCopy()
    const {
  ChunkMetadataMap metadata_map;
  for (const auto& [column_id, chunk_metadata] : chunkMetadataMap) {
    metadata_map[column_id] = std::make_shared<ChunkMetadata>(*chunk_metadata);
  }
  return metadata_map;
}

size_t Fragmenter_Namespace::FragmentInfo::getNumTuples() const {
  std::unique_ptr<std::lock_guard<std::mutex>> lock;
  if (resultSetMutex) {
    lock.reset(new std::lock_guard<std::mutex>(*resultSetMutex));
  }
  CHECK_EQ(!!resultSet, !!resultSetMutex);
  return numTuples;
}

size_t Fragmenter_Namespace::TableInfo::getNumTuples() const {
  if (!fragments.empty() && fragments.front().resultSet) {
    return fragments.front().getNumTuples();
  }
  return numTuples;
}

size_t Fragmenter_Namespace::TableInfo::getNumTuplesUpperBound() const {
  return numTuples;
}

size_t Fragmenter_Namespace::TableInfo::getFragmentNumTuplesUpperBound() const {
  size_t fragment_num_tupples_upper_bound = 0;
  for (const auto& fragment : fragments) {
    fragment_num_tupples_upper_bound =
        std::max(fragment.getNumTuples(), fragment_num_tupples_upper_bound);
  }
  return fragment_num_tupples_upper_bound;
}
