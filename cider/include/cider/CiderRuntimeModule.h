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

#ifndef CIDER_CIDERRUNTIMEMODULE_H
#define CIDER_CIDERRUNTIMEMODULE_H

#include <cstdint>
#include <memory>
#include <vector>

#include "cider/CiderAllocator.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderInterface.h"
// TODO: remove agg header files form public API
#include "exec/operator/aggregate/CiderAggHashTable.h"
#include "exec/operator/aggregate/CiderAggHashTableUtils.h"
#include "exec/operator/aggregate/CiderAggTargetColExtractor.h"

// Runtime API
// should have a compileModule and input data, produce output data
class CiderRuntimeModule {
 public:
  enum ReturnCode { kNoMoreOutput, kMoreOutput };

  /// \brief CiderRuntimeModule manage the JIT code, and take the responsibility for data
  /// processing
  ///
  /// \param allocator allocate memory for intermediate and result. user can construct an
  /// allocator from upstream memory pool, and then memory allocated will be in the pool.
  CiderRuntimeModule(
      std::shared_ptr<CiderCompilationResult> ciderCompilationResult,
      CiderCompilationOption ciderCompilationOption = CiderCompilationOption::defaults(),
      CiderExecutionOption ciderExecutionOption = CiderExecutionOption::defaults(),
      std::shared_ptr<CiderAllocator> allocator =
          std::make_shared<CiderDefaultAllocator>());

  // Group-by Agg related functions
  std::unique_ptr<CiderAggHashTableRowIterator> getGroupByAggHashTableIteratorAt(
      size_t index);
  const std::string convertGroupByAggHashTableToString() const;  // For test only
  const std::string convertQueryMemDescToString() const;         // For test only
  size_t getGroupByAggHashTableBufferNum() const;
  bool isGroupBy() const;
  bool hasCountDistinct() const;

  /// \brief Proceess the input CiderBatch, the output CiderBatch will be allocated inside
  /// the function
  ///
  /// \param in_batch the input CiderBatch will be processed.
  void processNextBatch(const CiderBatch& in_batch);

  /// \brief Fetch results and clear any intermediate data
  ///
  /// \param[<in>] max_row        max row by one fetch, 0 for not limit.
  /// \return ReturnCode indicates whether all rows fetched. CiderBatch store the results.
  std::pair<ReturnCode, std::unique_ptr<CiderBatch>> fetchResults(int32_t max_row = 0);

  static std::string getErrorMessageFromCode(const int32_t error_code);

  SortInfo getSortInfo();

 private:
  void initCiderAggGroupByHashTable();
  void initCiderAggTargetColExtractors();
  // fetching non-blocking results
  // should only be called after process batch
  void fetchNonBlockingResults(int32_t start_row,
                               int64_t* flattened_out,
                               CiderBatch& outBatch,
                               std::vector<size_t> column_size);
  // helper function for fecth results
  template <typename T>
  void extract_column(int32_t start_row,
                      int64_t max_read_rows,
                      size_t col_offset,
                      size_t col_step,
                      int64_t* flattened_out,
                      int8_t* out_col);

  void extract_varchar_column(int32_t start_row,
                              int64_t max_read_rows,
                              size_t col_offset,
                              size_t col_step,
                              int64_t* flattened_out,
                              int8_t* out_col);

  void extract_varchar_column_from_dict(int32_t start_row,
                                        int64_t max_read_rows,
                                        size_t col_offset,
                                        size_t col_step,
                                        int64_t* flattened_out,
                                        int8_t* out_col);

  // extract 64bits decimal column value
  void extract_decimal_column(int32_t scale,
                              int32_t start_row,
                              int64_t max_read_rows,
                              size_t col_offset,
                              size_t col_step,
                              int64_t* flattened_out,
                              int8_t* out_col);

  void groupByProcessImpl(const int8_t*** input_cols,
                          uint8_t* row_skip_mask,
                          const uint64_t* num_fragments,
                          int64_t* num_rows_ptr,
                          uint64_t* flatened_frag_offsets,
                          int32_t* total_matched_init,
                          int64_t** group_by_output_buffer,
                          const int64_t* join_hash_tables_ptr);
  void resetAggVal();
  CiderBatch setSchemaAndUpdateCountDistinctResIfNeed(
      std::unique_ptr<CiderBatch> output_batch);
  std::shared_ptr<CiderCompilationResult> ciderCompilationResult_;
  CiderCompilationOption ciderCompilationOption_;
  CiderExecutionOption ciderExecutionOption_;
  std::shared_ptr<CiderAllocator> allocator_;
  bool hoist_literals_;
  int32_t scan_limit_;
  uint32_t num_tables_;
  int32_t error_code_;
  std::vector<int8_t> literal_buff_;
  std::vector<int64_t> init_agg_vals_;
  std::vector<int64_t> join_hash_tables_;
  std::unique_ptr<CiderAggHashTable, CiderAggHashTableDeleter> group_by_agg_hashtable_;
  std::vector<std::unique_ptr<CiderAggTargetColExtractor>> group_by_agg_extractors_;
  std::unique_ptr<CiderAggHashTableRowIterator> group_by_agg_iterator_;
  CiderBatch build_table_;
  SQLTypeInfo output_type_;

  // intermediate result for project and non_groupby_agg
  CiderBatch one_batch_result_{allocator_};
  int32_t total_matched_rows_{0};
  int32_t fetched_rows_{0};
  constexpr static size_t kMaxOutputRows = 1000;

  bool is_group_by_;
};

#endif  // CIDER_CIDERRUNTIMEMODULE_H
