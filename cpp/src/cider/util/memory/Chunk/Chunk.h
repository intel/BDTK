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

/**
 * @file Chunk.h
 *
 */

#pragma once

#include <list>
#include <memory>

#include "ChunkMetadata.h"
#include "type/data/sqltypes.h"
#include "type/schema/ColumnInfo.h"
#include "util/ChunkIter.h"
#include "util/memory/Buffer/AbstractBuffer.h"
#include "util/toString.h"

using Data_Namespace::AbstractBuffer;
using Data_Namespace::MemoryLevel;

namespace Data_Namespace {
class DataMgr;
}

using Data_Namespace::DataMgr;

namespace Chunk_NS {

class Chunk {
 public:
  Chunk() : buffer_(nullptr), index_buf_(nullptr), column_info_(nullptr) {}

  explicit Chunk(ColumnInfoPtr col_info)
      : buffer_(nullptr), index_buf_(nullptr), column_info_(col_info) {}

  Chunk(AbstractBuffer* b, AbstractBuffer* ib, ColumnInfoPtr col_info)
      : buffer_(b), index_buf_(ib), column_info_(col_info) {}

  ~Chunk() { unpinBuffer(); }

  ColumnInfoPtr getColumnInfo() const { return column_info_; }

  void setColumnInfo(ColumnInfoPtr col_info) { column_info_ = col_info; }

  int getTableId() const { return column_info_->table_id; }

  int getColumnId() const { return column_info_->column_id; }

  const SQLTypeInfo& getColumnType() const { return column_info_->type; }

  const std::string& getColumnName() const { return column_info_->name; }

  ChunkIter begin_iterator(const std::shared_ptr<ChunkMetadata>&,
                           int start_idx = 0,
                           int skip = 1) const;

  AbstractBuffer* getBuffer() const { return buffer_; }

  AbstractBuffer* getIndexBuf() const { return index_buf_; }

  void initEncoder();

  std::string toString() const {
    return ::typeName(this) + "(buffer=" + ::toString(buffer_) +
           ", index_buf=" + ::toString(index_buf_) +
           ", column_info=" + ::toString(*column_info_) + ")";
  }

 private:
  AbstractBuffer* buffer_;
  AbstractBuffer* index_buf_;
  ColumnInfoPtr column_info_;

  void unpinBuffer();
};

}  // namespace Chunk_NS
