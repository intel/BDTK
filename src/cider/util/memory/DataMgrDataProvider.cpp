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

#include "DataMgrDataProvider.h"
#include "util/encode/ArrayNoneEncoder.h"
#include "util/encode/StringNoneEncoder.h"

#include "DataMgr.h"

DataMgrDataProvider::DataMgrDataProvider(DataMgr* data_mgr) : data_mgr_(data_mgr) {}

std::shared_ptr<Chunk_NS::Chunk> DataMgrDataProvider::getChunk(
    ColumnInfoPtr col_info,
    const ChunkKey& key,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_id,
    const size_t num_bytes,
    const size_t num_elems) {
  AbstractBuffer* buffer = nullptr;
  AbstractBuffer* index_buf = nullptr;

  if (col_info->type.is_varlen() && !col_info->type.is_fixlen_array()) {
    ChunkKey subKey = key;
    subKey.push_back(1);  // 1 for the main buffer_
    buffer = data_mgr_->getChunkBuffer(subKey, memory_level, device_id, num_bytes);
    subKey.pop_back();
    subKey.push_back(2);  // 2 for the index buffer_
    index_buf = data_mgr_->getChunkBuffer(
        subKey,
        memory_level,
        device_id,
        (num_elems + 1) * sizeof(StringOffsetT));  // always record n+1 offsets so string
                                                   // length can be calculated
    switch (col_info->type.get_type()) {
      case kARRAY: {
        auto array_encoder = dynamic_cast<ArrayNoneEncoder*>(buffer->getEncoder());
        CHECK(array_encoder);
        array_encoder->setIndexBuffer(index_buf);
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        CHECK_EQ(kENCODING_NONE, col_info->type.get_compression());
        auto str_encoder = dynamic_cast<StringNoneEncoder*>(buffer->getEncoder());
        CHECK(str_encoder);
        str_encoder->setIndexBuffer(index_buf);
        break;
      }
      default:
        UNREACHABLE();
    }
  } else {
    buffer = data_mgr_->getChunkBuffer(key, memory_level, device_id, num_bytes);
  }

  return std::make_shared<Chunk_NS::Chunk>(Chunk_NS::Chunk(buffer, index_buf, col_info));
}
