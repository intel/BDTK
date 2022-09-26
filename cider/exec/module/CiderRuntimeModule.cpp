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

#define CIDERBATCH_WITH_ARROW

#include "cider/CiderRuntimeModule.h"
#include "CiderCompilationResultImpl.h"
#include "cider/CiderException.h"
#include "cider/batch/StructBatch.h"
#include "exec/operator/aggregate/CiderAggHashTable.h"
#include "exec/operator/aggregate/CiderAggHashTableUtils.h"
#include "exec/operator/aggregate/CiderAggTargetColExtractor.h"
#include "exec/operator/aggregate/CiderAggTargetColExtractorBuilder.h"
#include "exec/plan/parser/ConverterHelper.h"
#include "exec/plan/parser/TypeUtils.h"
#include "exec/template/CountDistinct.h"
#include "exec/template/OutputBufferInitialization.h"

using agg_query = void (*)(const int8_t***,  // col_buffers
                           const uint64_t*,  // num_fragments
                           const int64_t*,   // num_rows
                           const uint64_t*,  // frag_row_offsets
                           const int32_t*,   // max_matched
                           int32_t*,         // matched_rows
                           const int64_t*,   // init_agg_value
                           int64_t**,        // out
                           int32_t*,         // error_code
                           const uint32_t*,  // num_tables_
                           const int64_t*);  // join_hash_tables_ptr

using agg_query_hoist_literals = void (*)(const int8_t***,  // col_buffers
                                          const uint64_t*,  // num_fragments
                                          const int8_t*,    // literals
                                          const int64_t*,   // num_rows
                                          const uint64_t*,  // frag_row_offsets
                                          const int32_t*,   // max_matched
                                          int32_t*,         // matched_rows
                                          const int64_t*,   // init_agg_value
                                          int64_t**,        // out
                                          int32_t*,         // error_code
                                          const uint32_t*,  // num_tables_
                                          const int64_t*);  // join_hash_tables_ptr

CiderRuntimeModule::CiderRuntimeModule(
    std::shared_ptr<CiderCompilationResult> ciderCompilationResult,
    CiderCompilationOption ciderCompilationOption,
    CiderExecutionOption ciderExecutionOption,
    std::shared_ptr<CiderAllocator> allocator)
    : ciderCompilationResult_(ciderCompilationResult)
    , ciderCompilationOption_(ciderCompilationOption)
    , ciderExecutionOption_(ciderExecutionOption)
    , allocator_(allocator) {
  hoist_literals_ = ciderCompilationOption.hoist_literals;
  num_tables_ = 1;
  error_code_ = 0;
  literal_buff_ = ciderCompilationResult->getHoistLiteral();
  build_table_ = std::move(ciderCompilationResult_->impl_->build_table_);
  join_hash_tables_ =
      ciderCompilationResult->impl_->compilation_result_.join_hash_table_ptrs;

  auto& rel_alg_exe = ciderCompilationResult_->impl_->rel_alg_exe_unit_;

  // pass concrete agg func to set different init_agg_vals
  init_agg_vals_ = init_agg_val(rel_alg_exe->target_exprs,
                                rel_alg_exe->simple_quals,
                                *ciderCompilationResult_->impl_->query_mem_desc_,
                                allocator_);

  is_group_by_ = rel_alg_exe->groupby_exprs.size() && rel_alg_exe->groupby_exprs.front();
  if (is_group_by_) {
    initCiderAggGroupByHashTable();
    initCiderAggTargetColExtractors();
    group_by_agg_iterator_ = nullptr;
  }

  if (ciderCompilationOption_.use_cider_data_format && !is_group_by_) {
    auto agg_col_count = init_agg_vals_.size();
    for (size_t i = 0; i < agg_col_count; i++) {
      init_agg_vals_.push_back(0);
    }
  }
}

std::unique_ptr<CiderBatch> CiderRuntimeModule::prepareOneBatchOutput(int64_t len) {
  ArrowSchema* schema = CiderBatchUtils::convertCiderTableSchemaToArrowSchema(
      ciderCompilationResult_->getOutputCiderTableSchema());
  auto root_batch = StructBatch::Create(schema);

  std::function<void(CiderBatch*)> build_function =
      [len, &build_function](CiderBatch* batch) -> void {
    batch->resizeBatch(len);
    // Allocate nulls and assigned as not_null by default. Currently, all children will be
    // allocated with nulls.
    batch->getMutableNulls();
    for (size_t i = 0; i < batch->getChildrenNum(); ++i) {
      auto child = batch->getChildAt(i);
      build_function(child.get());
    }
  };

  build_function(root_batch.get());
  return root_batch;
}

inline bool query_has_join(const std::shared_ptr<RelAlgExecutionUnit>& ra_exe_unit) {
  return (std::count_if(ra_exe_unit->join_quals.begin(),
                        ra_exe_unit->join_quals.end(),
                        [](const auto& join_condition) {
                          return join_condition.type == JoinType::INNER ||
                                 join_condition.type == JoinType::LEFT;
                        }) > 0);
}

void CiderRuntimeModule::processNextBatch(const CiderBatch& in_batch) {
  INJECT_TIMER(CiderRuntimeModule_NextBatch);
  // reset fetched_rows, ignore data not fetched.
  bool use_cider_data_format = ciderCompilationOption_.use_cider_data_format;
  fetched_rows_ = 0;

  const int8_t** multifrag_col_buffers[2]{nullptr};
  int total_col_num = use_cider_data_format
                          ? in_batch.getChildrenNum()
                          : in_batch.column_num() + build_table_.column_num();

  const int8_t** col_buffers = reinterpret_cast<const int8_t**>(
      allocator_->allocate(sizeof(int8_t**) * (total_col_num)));

  if (use_cider_data_format) {
    const void** children_arraies = in_batch.getChildrenArrayPtr();
    for (int64_t i = 0; i < in_batch.getChildrenNum(); ++i) {
      col_buffers[i] = reinterpret_cast<const int8_t*>(children_arraies[i]);
    }
  } else {
    for (int64_t i = 0; i < in_batch.column_num(); ++i) {
      col_buffers[i] = in_batch.column(i);
    }
    for (int64_t i = 0; i < build_table_.column_num(); ++i) {
      col_buffers[i + in_batch.column_num()] = build_table_.column(i);
    }
  }
  if (col_buffers) {
    multifrag_col_buffers[0] = col_buffers;
  }
  const int8_t*** multifrag_cols_ptr{
      nullptr == multifrag_col_buffers[0] ? nullptr : multifrag_col_buffers};
  const uint64_t num_fragments =
      multifrag_cols_ptr ? static_cast<uint64_t>(1) : uint64_t(0);

  const auto& query_mem_desc = ciderCompilationResult_->impl_->query_mem_desc_;
  bool is_project =
      query_mem_desc->getQueryDescriptionType() == QueryDescriptionType::Projection;
  bool is_non_groupby_agg = query_mem_desc->getQueryDescriptionType() ==
                            QueryDescriptionType::NonGroupedAggregate;
  scan_limit_ = use_cider_data_format ? in_batch.getLength() : in_batch.row_num();
  // for join scenario, max our row may be cross product
  if (query_has_join(ciderCompilationResult_->impl_->rel_alg_exe_unit_)) {
    scan_limit_ *= build_table_.row_num();
  }

  // if you call processNextBatch() multi times before fetchResults(),
  // we just save the last batch results for project
  // non_groupby_agg: use init_agg_vals_ to accumulate results
  // groupby_agg: use group_by_agg_hashtable_ to accuumulate results
  int64_t** out_vec;
  int64_t* tmp_buffer;
  if (is_project) {
    // row memory layout
    if (use_cider_data_format) {
      one_batch_result_ptr_ = prepareOneBatchOutput(scan_limit_);
      tmp_buffer =
          (int64_t*)const_cast<void**>(one_batch_result_ptr_->getChildrenArrayPtr());
      out_vec = &tmp_buffer;
    } else {
      one_batch_result_.resize(scan_limit_, (int64_t)query_mem_desc->getRowSize());
      tmp_buffer = const_cast<int64_t*>(
          reinterpret_cast<const int64_t*>(one_batch_result_.column(0)));
      out_vec = &tmp_buffer;
    }
  } else if (is_non_groupby_agg) {
    // columnar memory layout
    std::vector<size_t> column_type_size(init_agg_vals_.size(), sizeof(int64_t));
    one_batch_result_.resize(1, column_type_size);
    out_vec = const_cast<int64_t**>(
        reinterpret_cast<const int64_t**>(one_batch_result_.table()));
  } else if (is_group_by_) {
    // int64_t* group_by_output_buffer =
    //     reinterpret_cast<int64_t*>(group_by_agg_hashtable_.get());
    // out_vec = &group_by_output_buffer;
    tmp_buffer = reinterpret_cast<int64_t*>(group_by_agg_hashtable_.get());
    out_vec = &tmp_buffer;
  }

  std::vector<int64_t> flatened_num_rows;
  flatened_num_rows.push_back(use_cider_data_format ? in_batch.getLength()
                                                    : in_batch.row_num());
  if (build_table_.row_num()) {
    flatened_num_rows.push_back(build_table_.row_num());
  }
  int64_t* num_rows_ptr = flatened_num_rows.data();
  std::vector<uint64_t> flatened_frag_offsets;
  flatened_frag_offsets.push_back(0);

  int32_t matched_rows = 0;

  const int64_t* join_hash_tables_ptr =
      join_hash_tables_.size() == 1
          ? reinterpret_cast<const int64_t*>(join_hash_tables_[0])
          : (join_hash_tables_.size() > 1
                 ? reinterpret_cast<const int64_t*>(&join_hash_tables_[0])
                 : nullptr);
  {
    INJECT_TIMER(CiderRuntimeModule_Execution);

    if (hoist_literals_) {
      if (is_group_by_) {
        CiderBitUtils::CiderBitVector<> row_skip_mask(
            allocator_,
            use_cider_data_format ? in_batch.getLength() : in_batch.row_num());
        multifrag_cols_ptr[1] = row_skip_mask.as<const int8_t*>();

        groupByProcessImpl(multifrag_cols_ptr,
                           row_skip_mask.as<uint8_t>(),
                           &num_fragments,
                           num_rows_ptr,
                           flatened_frag_offsets.data(),
                           &matched_rows,
                           out_vec,
                           join_hash_tables_ptr);
      } else {
        reinterpret_cast<agg_query_hoist_literals>(ciderCompilationResult_->func())(
            multifrag_cols_ptr,
            &num_fragments,
            literal_buff_.data(),
            num_rows_ptr,
            flatened_frag_offsets.data(),
            &scan_limit_,
            &matched_rows,
            init_agg_vals_.data(),
            out_vec,
            &error_code_,
            &num_tables_,
            join_hash_tables_ptr);
      }
    } else {
      if (is_group_by_) {
        reinterpret_cast<agg_query>(ciderCompilationResult_->func())(
            multifrag_cols_ptr,
            &num_fragments,
            num_rows_ptr,
            flatened_frag_offsets.data(),
            &scan_limit_,
            &matched_rows,
            nullptr,
            out_vec,
            &error_code_,
            &num_tables_,
            join_hash_tables_ptr);
      } else {
        reinterpret_cast<agg_query>(ciderCompilationResult_->func())(
            multifrag_cols_ptr,
            &num_fragments,
            num_rows_ptr,
            flatened_frag_offsets.data(),
            &scan_limit_,
            &matched_rows,
            init_agg_vals_.data(),
            out_vec,
            &error_code_,
            &num_tables_,
            join_hash_tables_ptr);
      }
    }
  }
  allocator_->deallocate(reinterpret_cast<int8_t*>(col_buffers),
                         sizeof(int8_t**) * (total_col_num));
  if (error_code_ != 0) {
    CIDER_THROW(CiderRuntimeException, getErrorMessageFromCode(error_code_));
  }
  //  ciderCompilationResult_->impl_->ciderStringDictionaryProxy_.
  // update total_matched_rows_
  if (is_project) {
    total_matched_rows_ = matched_rows;
  } else if (is_non_groupby_agg) {
    // cache intermediate results into init_agg_vals
    const int8_t** outBuffers = one_batch_result_.table();
    int32_t column_num = ciderCompilationResult_->impl_->query_mem_desc_->getSlotCount();
    for (int i = 0; i < column_num; i++) {
      const int64_t* result = reinterpret_cast<const int64_t*>(outBuffers[i]);
      const int64_t* result_null =
          reinterpret_cast<const int64_t*>(outBuffers[i + column_num]);
      init_agg_vals_[i] = result[0];
      if (use_cider_data_format) {
        init_agg_vals_[i + column_num] = result_null[0];
      }
    }
    // total_matched_rows_ = one_batch_result_.row_num(); // always 1
  } else {
    total_matched_rows_ += matched_rows;
  }
}

#define INDEX_SIZE 8
#define EIGHT_BYTES 8
void CiderRuntimeModule::fetchNonBlockingResults(int32_t start_row,
                                                 int64_t* flattened_out,
                                                 CiderBatch& outBatch,
                                                 std::vector<size_t> column_size) {
  // follow IR write logic, it writes out results as flattened row
  // it always use 8 bytes(an int64 or double or 64bit pointer) to represent one valid
  // value. The first 8 bytes is row index, which means row id of the original input.
  // following values are column value of this row, numeric values use 64 bits(8 bytes).
  // For string type, it uses 2 64-bits value, the first is pointer, the latter is length.
  CHECK_EQ(outBatch.column_num(), column_size.size());
  size_t col_num = outBatch.column_num();
  int64_t row_num = outBatch.row_num();

  // step is row size in 8 bytes, equal to query_mem_desc->getRowSize() / 8
  size_t step = ciderCompilationResult_->impl_->query_mem_desc_->getRowSize() / 8;
  std::vector<size_t> offsets(col_num, 0);
  for (int i = 0; i < col_num; i++) {
    offsets[i] = ciderCompilationResult_->impl_->query_mem_desc_->getColOffInBytes(i) /
                 EIGHT_BYTES;
  }

  auto schema = ciderCompilationResult_->getOutputCiderTableSchema();
  if (!outBatch.schema()) {
    outBatch.set_schema(std::make_shared<CiderTableSchema>(schema));
  }
  for (size_t i = 0; i < col_num; i++) {
    int8_t* col = const_cast<int8_t*>(outBatch.column(i));
    substrait::Type type = schema.getColumnTypeById(i);
    // need to convert flattened row to columns as outbatch
    switch (type.kind_case()) {
      case substrait::Type::kBool:
      case substrait::Type::kI8:
        extract_column<int8_t>(start_row, row_num, offsets[i], step, flattened_out, col);
        break;
      case substrait::Type::kI16:
        extract_column<int16_t>(start_row, row_num, offsets[i], step, flattened_out, col);
        break;
      case substrait::Type::kI32:
        extract_column<int32_t>(start_row, row_num, offsets[i], step, flattened_out, col);
        break;
      case substrait::Type::kI64:
      case substrait::Type::kDate:
      case substrait::Type::kTime:
      case substrait::Type::kTimestamp:
        extract_column<int64_t>(start_row, row_num, offsets[i], step, flattened_out, col);
        break;
      case substrait::Type::kFp32:
        extract_column<float>(start_row, row_num, offsets[i], step, flattened_out, col);
        break;
      case substrait::Type::kFp64:
        extract_column<double>(start_row, row_num, offsets[i], step, flattened_out, col);
        break;
      case substrait::Type::kString:
      case substrait::Type::kVarchar:
      case substrait::Type::kFixedChar: {
        int col_len =
            (i == col_num - 1) ? (step - offsets[i]) : ((offsets[i + 1] - offsets[i]));
        if (col_len == 1) {  // string function, decode from StringDictionary
          extract_varchar_column_from_dict(
              start_row, row_num, offsets[i], step, flattened_out, col);
        } else if (col_len == 2) {
          extract_varchar_column(
              start_row, row_num, offsets[i], step, flattened_out, col);
        } else {
          CIDER_THROW(CiderRuntimeException, fmt::format("col_len is {}", col_len));
        }
        break;
      }
      case substrait::Type::kDecimal: {
        extract_decimal_column(type.decimal().scale(),
                               start_row,
                               row_num,
                               offsets[i],
                               step,
                               flattened_out,
                               col);
        break;
      }
      default:
        LOG(ERROR) << "Unsupported type: " << type.kind_case();
    }
  }
}

template <typename T>
void CiderRuntimeModule::extract_column(int32_t start_row,
                                        int64_t max_read_rows,
                                        size_t col_offset,
                                        size_t col_step,
                                        int64_t* flattened_out,
                                        int8_t* out_col) {
  T* col_out_buffer = reinterpret_cast<T*>(out_col);
  int64_t i = 0;
  size_t cur_col_offset = start_row * col_step + col_offset;
  if (std::is_floating_point<T>::value) {
    // float is stored as 64bits
    double* flattened_out_buffer = reinterpret_cast<double*>(flattened_out);
    while (i < max_read_rows) {
      // cast to cover float case
      col_out_buffer[i++] = (T)flattened_out_buffer[cur_col_offset];
      cur_col_offset += col_step;
    }
  } else {
    // for other types, it uses 64bit to store a value
    while (i < max_read_rows) {
      col_out_buffer[i++] = flattened_out[cur_col_offset];
      cur_col_offset += col_step;
    }
  }
}

void CiderRuntimeModule::extract_varchar_column_from_dict(int32_t start_row,
                                                          int64_t max_read_rows,
                                                          size_t col_offset,
                                                          size_t col_step,
                                                          int64_t* flattened_out,
                                                          int8_t* out_col) {
  CiderByteArray* col_out_buffer = reinterpret_cast<CiderByteArray*>(out_col);
  int64_t i = 0;
  size_t cur_col_offset = start_row * col_step + col_offset;
  while (i < max_read_rows) {
    int64_t key = flattened_out[cur_col_offset];
    std::string s =
        ciderCompilationResult_->impl_->ciderStringDictionaryProxy_->getString(key);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(allocator_->allocate(s.length()));
    std::memcpy(ptr, s.c_str(), s.length());
    col_out_buffer[i].ptr = reinterpret_cast<const uint8_t*>(ptr);
    col_out_buffer[i].len = (uint32_t)s.length();
    i++;
    cur_col_offset += col_step;
  }
}

void CiderRuntimeModule::extract_varchar_column(int32_t start_row,
                                                int64_t max_read_rows,
                                                size_t col_offset,
                                                size_t col_step,
                                                int64_t* flattened_out,
                                                int8_t* out_col) {
  CiderByteArray* col_out_buffer = reinterpret_cast<CiderByteArray*>(out_col);
  int64_t i = 0;
  size_t cur_col_offset = start_row * col_step + col_offset;
  while (i < max_read_rows) {
    col_out_buffer[i].ptr =
        reinterpret_cast<const uint8_t*>(flattened_out[cur_col_offset]);
    col_out_buffer[i].len = (uint32_t)(flattened_out[cur_col_offset + 1]);
    i++;
    cur_col_offset += col_step;
  }
}

void CiderRuntimeModule::extract_decimal_column(int32_t scale,
                                                int32_t start_row,
                                                int64_t max_read_rows,
                                                size_t col_offset,
                                                size_t col_step,
                                                int64_t* flattened_out,
                                                int8_t* out_col) {
  double* col_out_buffer = reinterpret_cast<double*>(out_col);
  int64_t i = 0;
  size_t cur_col_offset = start_row * col_step + col_offset;
  while (i < max_read_rows) {
    col_out_buffer[i++] = (flattened_out[cur_col_offset] / pow(10, scale));
    cur_col_offset += col_step;
  }
}

void CiderRuntimeModule::groupByProcessImpl(const int8_t*** input_cols,
                                            uint8_t* row_skip_mask,
                                            const uint64_t* num_fragments,
                                            int64_t* num_rows_ptr,
                                            uint64_t* flatened_frag_offsets,
                                            int32_t* matched_rows,
                                            int64_t** group_by_output_buffer,
                                            const int64_t* join_hash_tables_ptr) {
  reinterpret_cast<agg_query_hoist_literals>(ciderCompilationResult_->func())(
      input_cols,
      num_fragments,
      literal_buff_.data(),
      num_rows_ptr,
      flatened_frag_offsets,
      &scan_limit_,
      matched_rows,
      nullptr,
      group_by_output_buffer,
      &error_code_,
      &num_tables_,
      join_hash_tables_ptr);
  if (error_code_ != 0) {
    CIDER_THROW(CiderRuntimeException, getErrorMessageFromCode(error_code_));
  }
  if (CiderBitUtils::countSetBits(row_skip_mask, *num_rows_ptr) != *num_rows_ptr) {
    // Need rehash, default hash mode priority: RangeHash -> DirectHash -> Spill
    if (group_by_agg_hashtable_->rehash()) {
      groupByProcessImpl(input_cols,
                         row_skip_mask,
                         num_fragments,
                         num_rows_ptr,
                         flatened_frag_offsets,
                         matched_rows,
                         group_by_output_buffer,
                         join_hash_tables_ptr);
    } else {
      // TODO: Spill
      CIDER_THROW(CiderRuntimeException, "Group-by aggregation spill not supported.");
    }
  }
}

std::pair<CiderRuntimeModule::ReturnCode, std::unique_ptr<CiderBatch>>
CiderRuntimeModule::fetchResults(int32_t max_row) {
  INJECT_TIMER(CiderRuntimeModule_FetchResult);
  const auto& query_mem_desc_t =
      ciderCompilationResult_->impl_->query_mem_desc_->getQueryDescriptionType();
  bool is_project = query_mem_desc_t == QueryDescriptionType::Projection;
  if (is_project) {
    if (ciderCompilationOption_.use_cider_data_format) {
      std::function<void(CiderBatch*)> process_function =
          [row_num = total_matched_rows_, &process_function](CiderBatch* batch) -> void {
        batch->setLength(row_num);
        batch->setNullCount(row_num -
                            CiderBitUtils::countSetBits(batch->getNulls(), row_num));
        for (size_t i = 0; i < batch->getChildrenNum(); ++i) {
          auto child = batch->getChildAt(i);
          process_function(child.get());
        }
      };

      process_function(one_batch_result_ptr_.get());
      return std::make_pair(kNoMoreOutput, std::move(one_batch_result_ptr_));
    } else {
      auto schema = ciderCompilationResult_->getOutputCiderTableSchema();
      int column_num = schema.getColumnCount();

      std::vector<size_t> column_size;
      for (int i = 0; i < column_num; i++) {
        column_size.push_back(schema.GetColumnTypeSize(i));
      }

      int32_t row_num = total_matched_rows_ - fetched_rows_;
      if (max_row > 0) {
        row_num = std::min(row_num, max_row);
      }
      auto project_result =
          std::make_unique<CiderBatch>(row_num, column_size, allocator_);
      project_result->set_schema(std::make_shared<CiderTableSchema>(schema));
      int64_t* row_buffer = const_cast<int64_t*>(
          reinterpret_cast<const int64_t*>(one_batch_result_.column(0)));
      fetchNonBlockingResults(fetched_rows_, row_buffer, *project_result, column_size);

      fetched_rows_ += row_num;
      return std::make_pair(kNoMoreOutput, std::move(project_result));
    }
  }

  bool is_non_groupby_agg = query_mem_desc_t == QueryDescriptionType::NonGroupedAggregate;
  if (is_non_groupby_agg) {
    // FIXME: one_batch_result_ is moved, can't call processNextBatch again.
    // just ignore max_row here.
    auto non_groupby_agg_result =
        std::make_unique<CiderBatch>(std::move(one_batch_result_));
    auto out_batch =
        setSchemaAndUpdateCountDistinctResIfNeed(std::move(non_groupby_agg_result));
    // reset need to be done after data consumed, like bitmap for count(distinct)
    resetAggVal();
    return std::make_pair(kNoMoreOutput,
                          std::move(std::make_unique<CiderBatch>(std::move(out_batch))));
  }

  // for group_by_agg

  // FIXME: some testcase(CiderAggHashTableTest) does not setup output table schema
  // correctly.
  // int column_num =
  // ciderCompilationResult_->getOutputCiderTableSchema().getColumnCount();
  int column_num = ciderCompilationResult_->impl_->rel_alg_exe_unit_->target_exprs.size();
  std::vector<size_t> column_size;
  for (int i = 0; i < column_num; i++) {
    column_size.push_back(sizeof(int64_t));
  }
  // TODO: should we read all data or just some row by once?
  int32_t row_num = max_row > 0 ? max_row : kMaxOutputRows;
  std::unique_ptr<CiderBatch> groupby_agg_result;
  if (ciderCompilationOption_.use_cider_data_format) {
    auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(output_type_);
    groupby_agg_result = std::make_unique<StructBatch>(schema, allocator_);
  } else {
    groupby_agg_result = std::make_unique<CiderBatch>(row_num, column_size, allocator_);
  }

  if (!group_by_agg_iterator_) {
    group_by_agg_iterator_ = group_by_agg_hashtable_->getRowIterator(0);
  }

  size_t row_addr_vec_size =
      group_by_agg_iterator_->getRuntimeState().getNonEmptyEntryNum();
  std::vector<const int8_t*> row_base_addrs;
  row_base_addrs.reserve(row_addr_vec_size);

  size_t curr_size = 0;
  while (!group_by_agg_iterator_->finished() && curr_size < row_num) {
    row_base_addrs.push_back(group_by_agg_iterator_->getColumnBase());
    group_by_agg_iterator_->toNextRow();
    ++curr_size;
  }
  if (group_by_agg_iterator_->finished()) {
    group_by_agg_iterator_->getRuntimeState().addEmptyEntryNum(curr_size);
    group_by_agg_iterator_ = nullptr;
  }

  if (ciderCompilationOption_.use_cider_data_format) {
    groupby_agg_result->resizeBatch(curr_size, true);
  } else {
    groupby_agg_result->set_row_num(curr_size);
  }
  for (size_t i = 0, col_id = 0; i < group_by_agg_extractors_.size(); ++i) {
    if (group_by_agg_extractors_[i]) {
      if (ciderCompilationOption_.use_cider_data_format) {
        auto child = groupby_agg_result->getChildAt(col_id);
        group_by_agg_extractors_[i]->extract(row_base_addrs, child.get());
      } else {
        group_by_agg_extractors_[i]->extract(
            row_base_addrs, const_cast<int8_t*>(groupby_agg_result->column(col_id)));
      }
      ++col_id;
    }
  }

  return std::make_pair(
      group_by_agg_iterator_ ? kMoreOutput : kNoMoreOutput,
      std::move(std::make_unique<CiderBatch>(
          setSchemaAndUpdateCountDistinctResIfNeed(std::move(groupby_agg_result)))));
}

CiderAggHashTableRowIteratorPtr CiderRuntimeModule::getGroupByAggHashTableIteratorAt(
    size_t index) {
  if (group_by_agg_hashtable_) {
    return group_by_agg_hashtable_->getRowIterator(index);
  } else {
    LOG(ERROR) << "Not a group-by aggregation module.";
    return nullptr;
  }
}

void CiderRuntimeModule::initCiderAggGroupByHashTable() {
  group_by_agg_hashtable_ = std::unique_ptr<CiderAggHashTable, CiderAggHashTableDeleter>(
      new CiderAggHashTable(ciderCompilationResult_->impl_->query_mem_desc_,
                            ciderCompilationResult_->impl_->rel_alg_exe_unit_,
                            allocator_),
      CiderAggHashTableDeleter());
}

size_t CiderRuntimeModule::getGroupByAggHashTableBufferNum() const {
  return group_by_agg_hashtable_ ? group_by_agg_hashtable_->getBufferNum() : 0;
}

const std::string CiderRuntimeModule::convertGroupByAggHashTableToString() const {
  return group_by_agg_hashtable_ ? group_by_agg_hashtable_->toString() : "";
}

bool CiderRuntimeModule::isGroupBy() const {
  return is_group_by_;
}

void CiderRuntimeModule::initCiderAggTargetColExtractors() {
  auto& target_index_map = group_by_agg_hashtable_->getTargetIndexMap();
  size_t col_num = target_index_map.size();
  group_by_agg_extractors_ =
      std::vector<std::unique_ptr<CiderAggTargetColExtractor>>(col_num);

  const auto& output_hint =
      ciderCompilationResult_->getOutputCiderTableSchema().getColHints();

  std::vector<bool> is_partial_avg_sum(col_num, false);
  for (size_t i = 0, j = 0; i < output_hint.size() && j < col_num; ++j) {
    auto target_col_index = target_index_map[j];
    auto& col_info = group_by_agg_hashtable_->getColEntryInfo(target_col_index);
    bool is_partial_avg = (ColumnHint::PartialAVG == output_hint[i]),
         is_sum = (col_info.agg_type == kSUM);

    is_partial_avg_sum[j] = (is_partial_avg && is_sum);

    if (!is_partial_avg) {
      ++i;
    }
  }

  std::vector<SQLTypeInfo> children;
  children.reserve(col_num);
  for (size_t i = 0; i < col_num; ++i) {
    auto target_col_index = target_index_map[i];
    auto& col_info = group_by_agg_hashtable_->getColEntryInfo(target_col_index);

    group_by_agg_extractors_[i] =
        CiderAggTargetColExtractorBuilder::buildCiderAggTargetColExtractor(
            group_by_agg_hashtable_.get(), target_col_index, is_partial_avg_sum[i]);

    // TODO: Partial Avg
    if (kAVG == col_info.agg_type) {
      children.emplace_back(kDOUBLE, col_info.arg_type_info.get_notnull());
      ++i;
    } else {
      children.emplace_back(col_info.sql_type_info);
    }
  }
  output_type_ = SQLTypeInfo(kSTRUCT, true, children);
}

const std::string CiderRuntimeModule::convertQueryMemDescToString() const {
  return ciderCompilationResult_->impl_->query_mem_desc_->toString();
}

void CiderRuntimeModule::resetAggVal() {
  const size_t agg_col_count =
      ciderCompilationResult_->impl_->query_mem_desc_->getSlotCount();
  for (int i = 0; i < init_agg_vals_.size(); i++) {
    if (ciderCompilationOption_.use_cider_data_format && i >= agg_col_count) {
      init_agg_vals_[i] = 0;
      continue;
    }
    if (ciderCompilationResult_->impl_->query_mem_desc_->hasCountDistinct()) {
      auto count_distinct_desc =
          ciderCompilationResult_->impl_->query_mem_desc_->getCountDistinctDescriptor(i);
      if (count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
        auto bitmap_handle = reinterpret_cast<int8_t*>(init_agg_vals_[i]);
        std::memset(bitmap_handle, 0, count_distinct_desc.bitmapPaddedSizeBytes());
      } else if (count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet) {
        reinterpret_cast<robin_hood::unordered_set<int64_t>*>(init_agg_vals_[i])->clear();
      }
    } else {
      init_agg_vals_[i] = 0;
    }
  }
}

CiderBatch CiderRuntimeModule::setSchemaAndUpdateCountDistinctResIfNeed(
    std::unique_ptr<CiderBatch> output_batch) {
  int column_num = ciderCompilationResult_->getOutputCiderTableSchema().getColumnCount();
  auto schema = std::make_shared<CiderTableSchema>(
      ciderCompilationResult_->getOutputCiderTableSchema());
  output_batch->set_schema(schema);
  // no need to update if no count(distinct)
  if (ciderCompilationResult_->impl_->query_mem_desc_->hasCountDistinct()) {
    const int8_t** outBuffers = output_batch->table();
    for (int i = 0; i < output_batch->column_num(); i++) {
      if (ciderCompilationResult_->impl_->query_mem_desc_->getCountDistinctDescriptor(i)
              .impl_type_ != CountDistinctImplType::Invalid) {
        int64_t* result =
            const_cast<int64_t*>(reinterpret_cast<const int64_t*>(outBuffers[i]));
        for (int j = 0; j < output_batch->row_num(); j++) {
          int64_t address = result[j];
          result[j] = count_distinct_set_address(
              address,
              ciderCompilationResult_->impl_->query_mem_desc_->getCountDistinctDescriptor(
                  i),
              allocator_);
        }
      }
    }
  }

  if (ciderCompilationResult_->impl_->query_mem_desc_->getQueryDescriptionType() ==
      QueryDescriptionType::NonGroupedAggregate) {
    // for non-groupby partial avg, sum(int) may has double output Type thus
    // need cast original int value(based on cider rule) to double
    std::vector<TargetInfo> target_infos = target_exprs_to_infos(
        ciderCompilationResult_->impl_->rel_alg_exe_unit_->target_exprs,
        *ciderCompilationResult_->impl_->query_mem_desc_);
    const int8_t** outBuffers = output_batch->table();
    for (int i = 0; i < schema->getColumnTypes().size(); i++) {
      int flatten_index = schema->getFlattenColIndex(i);
      if (schema->getColHints()[i] == ColumnHint::PartialAVG &&
          target_infos[flatten_index].agg_kind == SQLAgg::kSUM &&
          target_infos[flatten_index].agg_arg_type.is_integer() &&
          generator::getSQLTypeInfo(schema->getColumnTypeById(i).struct_().types(0))
                  .get_type() == SQLTypes::kDOUBLE) {
        int64_t* result = const_cast<int64_t*>(
            reinterpret_cast<const int64_t*>(outBuffers[flatten_index]));
        double value = (double)result[0];
        double* cast_buffer = reinterpret_cast<double*>(result);
        cast_buffer[0] = value;
      }
    }
  }
  return std::move(*output_batch);
}

std::string CiderRuntimeModule::getErrorMessageFromCode(const int32_t error_code) {
  switch (error_code) {
    case Executor::ERR_DIV_BY_ZERO:
      return "Division by zero";
    case Executor::ERR_OVERFLOW_OR_UNDERFLOW:
      return "Overflow or underflow";
    case Executor::ERR_OUT_OF_TIME:
      return "Query execution has exceeded the time limit";
    case Executor::ERR_INTERRUPTED:
      return "Query execution has been interrupted";
    case Executor::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES:
      return "Multiple distinct values encountered";
    case Executor::ERR_WIDTH_BUCKET_INVALID_ARGUMENT:
      return "Arguments of WIDTH_BUCKET function does not satisfy the condition";
    default:
      return "Cider Runtime Other error: code " + std::to_string(error_code);
  }
}
