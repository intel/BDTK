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

/*
 * @file Chunk.cpp
 */

#include "Chunk.h"

#include "util/encode/ArrayNoneEncoder.h"
#include "util/encode/StringNoneEncoder.h"

namespace Chunk_NS {

void Chunk::unpinBuffer() {
  if (buffer_) {
    buffer_->unPin();
  }
  if (index_buf_) {
    index_buf_->unPin();
  }
}

void Chunk::initEncoder() {
  buffer_->initEncoder(column_info_->type);
  if (column_info_->type.is_varlen() && !column_info_->type.is_fixlen_array()) {
    switch (column_info_->type.get_type()) {
      case kARRAY: {
        ArrayNoneEncoder* array_encoder =
            dynamic_cast<ArrayNoneEncoder*>(buffer_->getEncoder());
        array_encoder->setIndexBuffer(index_buf_);
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        CHECK_EQ(kENCODING_NONE, column_info_->type.get_compression());
        StringNoneEncoder* str_encoder =
            dynamic_cast<StringNoneEncoder*>(buffer_->getEncoder());
        str_encoder->setIndexBuffer(index_buf_);
        break;
      }
      default:
        CHECK(false);
    }
  }
}

ChunkIter Chunk::begin_iterator(const std::shared_ptr<ChunkMetadata>& chunk_metadata,
                                int start_idx,
                                int skip) const {
  ChunkIter it;
  it.type_info = column_info_->type;
  it.skip = skip;
  it.skip_size = column_info_->type.get_size();
  if (it.skip_size < 0) {  // if it's variable length
    it.current_pos = it.start_pos =
        index_buf_->getMemoryPtr() + start_idx * sizeof(StringOffsetT);
    it.end_pos = index_buf_->getMemoryPtr() + index_buf_->size() - sizeof(StringOffsetT);
    it.second_buf = buffer_->getMemoryPtr();
  } else {
    it.current_pos = it.start_pos = buffer_->getMemoryPtr() + start_idx * it.skip_size;
    it.end_pos = buffer_->getMemoryPtr() + buffer_->size();
    it.second_buf = nullptr;
  }
  it.num_elems = chunk_metadata->numElements;
  return it;
}
}  // namespace Chunk_NS
