/*
 * Copyright (c) 2022 Intel Corporation.
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

#ifndef CIDER_CIDERAGGHASHTABLEUTILS_H
#define CIDER_CIDERAGGHASHTABLEUTILS_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

class CiderAggHashTable;
class CiderAggHashTableRowIterator;
struct CiderAggHashTableEntryInfo;

using CiderAggHashTableRowIteratorPtr = std::unique_ptr<CiderAggHashTableRowIterator>;

class CiderAggHashTableBufferRuntimeState {
 public:
  CiderAggHashTableBufferRuntimeState(size_t buffer_entry_num)  // NOLINT
      : buffer_entry_num_(buffer_entry_num)
      , empty_entry_num_(buffer_entry_num)
      , curr_row_index_(0) {}

  void resetForNextBatch() { curr_row_index_ = 0; }

  void bufferCleared() { empty_entry_num_ = buffer_entry_num_; }

  const std::vector<size_t>& getRowIndexNeedSpillVec() const {
    return row_index_need_spill_vec_;
  }

  size_t getEmptyEntryNum() const { return empty_entry_num_; }
  size_t getNonEmptyEntryNum() const { return buffer_entry_num_ - empty_entry_num_; }
  void addEmptyEntryNum(size_t num) { empty_entry_num_ += num; }

  void insertNewEntrySuccess() {
    --empty_entry_num_;
    ++curr_row_index_;
  }

  void insertExistEntrySuccess() { ++curr_row_index_; }

  void insertFailed() {
    row_index_need_spill_vec_.reserve(getRowIndexNeedSpillVecSize());
    row_index_need_spill_vec_.push_back(curr_row_index_);
    ++curr_row_index_;
  }

  static size_t getRowIndexNeedSpillVecSize() {
    static size_t row_index_need_spill_vec_size = 1000;
    return row_index_need_spill_vec_size;
  }

 private:
  const size_t buffer_entry_num_;
  size_t empty_entry_num_;
  size_t curr_row_index_;
  std::vector<size_t> row_index_need_spill_vec_;
};

class CiderAggHashTableRowIterator {
 public:
  CiderAggHashTableRowIterator(CiderAggHashTable* table_ptr, size_t buffer_id);

  bool toNextRow();
  const int32_t* getColumn(size_t column_index) const;

  // Get column base address, current row address for row
  // layout and current column address for column layout.
  const int8_t* getColumnBase(size_t column_index = 0) const;
  size_t getColumnActualWidth(size_t column_index) const;
  const CiderAggHashTableEntryInfo& getColumnInfo(size_t column_index) const;

  // For test only
  int32_t getColumnTypeInfo(size_t column_index) const;

  bool finished() const;
  CiderAggHashTableBufferRuntimeState& getRuntimeState() {
    return *buffer_runtime_state_;
  }

 private:
  size_t row_index_;
  int8_t* buffer_ptr_;
  uint8_t* empty_map_ptr_;
  CiderAggHashTable* table_ptr_;
  CiderAggHashTableBufferRuntimeState* buffer_runtime_state_;
};

struct CiderAggHashTableDeleter {
  void operator()(CiderAggHashTable*) const;
};

#endif
