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

#ifndef COLUMNAR_RESULTS_H
#define COLUMNAR_RESULTS_H
#include "ResultSet.h"
#include "util/SqlTypesLayout.h"

#include "util/checked_alloc.h"

#include <memory>
#include <unordered_map>

class ColumnarConversionNotSupported : public std::runtime_error {
 public:
  ColumnarConversionNotSupported()
      : std::runtime_error(
            "Columnar conversion not supported for variable length types") {}
};

/**
 * A helper data structure to track non-empty entries in the input buffer
 * Currently only used for direct columnarization with columnar outputs.
 * Each bank is assigned to a thread so that concurrent updates of the
 * data structure is non-blocking.
 */
class ColumnBitmap {
 public:
  ColumnBitmap(const size_t num_elements_per_bank, size_t num_banks)
      : bitmaps_(num_banks, std::vector<bool>(num_elements_per_bank, false)) {}

  inline bool get(const size_t index, const size_t bank_index) const {
    CHECK_LT(bank_index, bitmaps_.size());
    CHECK_LT(index, bitmaps_[bank_index].size());
    return bitmaps_[bank_index][index];
  }

  inline void set(const size_t index, const size_t bank_index, const bool val) {
    CHECK_LT(bank_index, bitmaps_.size());
    CHECK_LT(index, bitmaps_[bank_index].size());
    bitmaps_[bank_index][index] = val;
  }

 private:
  std::vector<std::vector<bool>> bitmaps_;
};

class ColumnarResults {
 public:
  ColumnarResults(const std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                  const int8_t* one_col_buffer,
                  const size_t num_rows,
                  const SQLTypeInfo& target_type,
                  const size_t thread_idx,
                  Executor* executor);

  static std::unique_ptr<ColumnarResults> mergeResults(
      const std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
      const std::vector<std::unique_ptr<ColumnarResults>>& sub_results);

  const std::vector<int8_t*>& getColumnBuffers() const { return column_buffers_; }

  const size_t size() const { return num_rows_; }

  const SQLTypeInfo& getColumnType(const int col_id) const {
    CHECK_GE(col_id, 0);
    CHECK_LT(static_cast<size_t>(col_id), target_types_.size());
    return target_types_[col_id];
  }

  bool isParallelConversion() const { return parallel_conversion_; }
  bool isDirectColumnarConversionPossible() const { return direct_columnar_conversion_; }

 protected:
  std::vector<int8_t*> column_buffers_;
  size_t num_rows_;

 private:
  ColumnarResults(const size_t num_rows, const std::vector<SQLTypeInfo>& target_types)
      : num_rows_(num_rows), target_types_(target_types) {}
  inline void writeBackCell(const TargetValue& col_val,
                            const size_t row_idx,
                            const size_t column_idx);
  const std::vector<SQLTypeInfo> target_types_;
  bool parallel_conversion_;         // multi-threaded execution of columnar conversion
  bool direct_columnar_conversion_;  // whether columnar conversion might happen directly
  // with minimal ussage of result set's iterator access
  size_t thread_idx_;
  Executor* executor_;
};

using ColumnCacheMap =
    std::unordered_map<int,
                       std::unordered_map<int, std::shared_ptr<const ColumnarResults>>>;

#endif  // COLUMNAR_RESULTS_H
