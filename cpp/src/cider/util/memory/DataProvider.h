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
#pragma once

#include <memory>

#include "DictDescriptor.h"
#include "Fragmenter.h"
#include "type/schema/ColumnInfo.h"
#include "util/MemoryLevel.h"
#include "util/memory/Chunk/Chunk.h"
#include "util/types.h"

class DataProvider {
 public:
  // fetch data
  virtual std::shared_ptr<Chunk_NS::Chunk> getChunk(
      ColumnInfoPtr col_info,
      const ChunkKey& key,
      const Data_Namespace::MemoryLevel memory_level,
      const int device_id,
      const size_t num_bytes,
      const size_t num_elems) = 0;

  virtual Fragmenter_Namespace::TableInfo getTableMetadata(int db_id,
                                                           int table_id) const = 0;

  virtual const DictDescriptor* getDictMetadata(int db_id,
                                                int dict_id,
                                                bool load_dict = true) const = 0;
};

using DataProviderPtr = std::shared_ptr<DataProvider>;
