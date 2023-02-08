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

#ifndef CIDER_CIDERARROWDATAPROVIDER_H
#define CIDER_CIDERARROWDATAPROVIDER_H

#include "util/encode/ArrayNoneEncoder.h"
#include "util/encode/StringNoneEncoder.h"
#include "util/memory/DataProvider.h"

#include "cider/CiderBatch.h"
#include "util/memory/CiderBuffer.h"

// Lifecycle of CiderBatchDataProvider should be shorter than CiderBatch, so we will hold
// a reference of the original CiderBatch. And this class will be set to Compile method
// which need a DataProvider.
class CiderArrowDataProvider : public DataProvider {
 public:
  explicit CiderArrowDataProvider(const CiderBatch& ciderBatch)
      : ciderBatch_(ciderBatch) {}
  std::shared_ptr<Chunk_NS::Chunk> getChunk(
      ColumnInfoPtr col_info,
      const ChunkKey& key,
      const Data_Namespace::MemoryLevel memory_level,
      const int device_id,
      const size_t num_bytes,
      const size_t num_elems) override {
    Data_Namespace::AbstractBuffer* buffer =
        // new CiderBuffer(ciderBatch_.column(key[CHUNK_KEY_COLUMN_IDX]));
        new CiderBuffer((int8_t*)ciderBatch_.arrow_column(key[CHUNK_KEY_COLUMN_IDX]));
    Data_Namespace::AbstractBuffer* index_buf = nullptr;
    return std::make_shared<Chunk_NS::Chunk>(
        Chunk_NS::Chunk(buffer, index_buf, col_info));
  }

  Fragmenter_Namespace::TableInfo getTableMetadata(int db_id,
                                                   int table_id) const override {
    UNREACHABLE();
  };

  const DictDescriptor* getDictMetadata(int db_id,
                                        int dict_id,
                                        bool load_dict = true) const override {
    UNREACHABLE();
  };

 private:
  const CiderBatch& ciderBatch_;
};

#endif  // CIDER_CIDERARROWDATAPROVIDER_H
