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

#include "CiderAggTargetColExtractorBuilder.h"
#include "CiderAggTargetColExtractorTmpl.h"

std::unique_ptr<CiderAggTargetColExtractor>
CiderAggTargetColExtractorBuilder::buildCiderAggTargetColExtractor(
    const CiderAggHashTable* hash_table,
    size_t col_index,
    bool force_double_output) {
  CHECK_LT(col_index, hash_table->getColNum());
  auto& col_info = hash_table->getColEntryInfo(col_index);

  switch (col_info.agg_type) {
    case kAVG:
      return buildAVGAggExtractor(hash_table, col_index);
    case kCOUNT:
      return buildCountAggExtractor(hash_table, col_index);
    default:
      return buildSimpleAggExtractor(hash_table, col_index, force_double_output);
  }
}

std::unique_ptr<CiderAggTargetColExtractor>
CiderAggTargetColExtractorBuilder::buildSimpleAggExtractor(
    const CiderAggHashTable* hash_table,
    size_t col_index,
    bool force_double_output) {
  CHECK_LT(col_index, hash_table->getColNum());
  auto& col_info = hash_table->getColEntryInfo(col_index);
  size_t actual_size = hash_table->getActualDataWidth(col_index);

  if (force_double_output) {
    switch (col_info.sql_type_info.get_type()) {
      case kFLOAT:
      case kDOUBLE:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<float, double>>(
                "FLOAT_DOUBLE", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<double, double>>(
                "DOUBLE_DOUBLE", col_index, hash_table);
        }
      case kBOOLEAN:
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kDECIMAL:
      case kBIGINT:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, double>>(
                "INT32_DOUBLE", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, double>>(
                "INT64_DOUBLE", col_index, hash_table);
        }
      default:
        LOG(ERROR) << "Unsupported force-double output type: "
                   << col_info.sql_type_info.get_type_name();
        return nullptr;
    }
  } else {
    switch (col_info.sql_type_info.get_type()) {
      case kFLOAT:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<float, float>>(
                "FLOAT_FLOAT", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<double, float>>(
                "DOUBLE_FLOAT", col_index, hash_table);
        }
      case kDOUBLE:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<float, double>>(
                "FLOAT_DOUBLE", col_index, hash_table);

          case 8:
            return std::make_unique<SimpleAggExtractor<double, double>>(
                "DOUBLE_DOUBLE", col_index, hash_table);
        }
      case kBOOLEAN:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, int8_t>>(
                "INT32_BOOL", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, int8_t>>(
                "INT64_BOOL", col_index, hash_table);
        }
      case kTINYINT:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, int8_t>>(
                "INT32_INT8", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, int8_t>>(
                "INT64_INT8", col_index, hash_table);
        }
      case kSMALLINT:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, int16_t>>(
                "INT32_INT16", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, int16_t>>(
                "INT64_INT16", col_index, hash_table);
        }
      case kINT:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, int32_t>>(
                "INT32_INT32", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, int32_t>>(
                "INT64_INT32", col_index, hash_table);
        }
      case kDECIMAL:
      case kBIGINT:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, int64_t>>(
                "INT32_INT64", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, int64_t>>(
                "INT64_INT64", col_index, hash_table);
        }
      case kTEXT:
      case kVARCHAR:
        switch (actual_size) {
          case 4:
            return std::make_unique<SimpleAggExtractor<int32_t, VarCharPlaceHolder>>(
                "INT32_STRING", col_index, hash_table);
          case 8:
            return std::make_unique<SimpleAggExtractor<int64_t, VarCharPlaceHolder>>(
                "INT64_STRING", col_index, hash_table);
        }
      default:
        LOG(ERROR) << "Unsupported type: " << col_info.sql_type_info.get_type_name();
        return nullptr;
    }
  }
}

std::unique_ptr<CiderAggTargetColExtractor>
CiderAggTargetColExtractorBuilder::buildAVGAggExtractor(
    const CiderAggHashTable* hash_table,
    size_t col_index) {
  CHECK_LT(col_index, hash_table->getColNum() - 1);
  auto& sum_col_info = hash_table->getColEntryInfo(col_index);
  size_t sum_actual_size = hash_table->getActualDataWidth(col_index);
  size_t count_actual_size = hash_table->getActualDataWidth(col_index + 1);

  switch (sum_col_info.arg_type_info.get_type()) {
    case kFLOAT:
    case kDOUBLE:
      // Float Type
      switch (sum_actual_size) {
        case 4:
          switch (count_actual_size) {
            case 4:
              return std::make_unique<AVGAggExtractor<float, int32_t, double>>(
                  "AVG_FLOAT_INT32_DOUBLE", col_index, hash_table);
            case 8:
              return std::make_unique<AVGAggExtractor<float, int64_t, double>>(
                  "AVG_FLOAT_INT64_DOUBLE", col_index, hash_table);
          }
        case 8:
          return std::make_unique<AVGAggExtractor<double, int64_t, double>>(
              "AVG_DOUBLE_INT64_DOUBLE", col_index, hash_table);
      }
    case kDECIMAL:
      // Decimal Type
      return std::make_unique<AVGAggExtractor<DecimalPlaceHolder, int64_t, double>>(
          "AVG_DECIMAL_INT64_DOUBLE", col_index, hash_table);
    default:
      // Integer Type
      switch (sum_actual_size) {
        case 4:
          return std::make_unique<AVGAggExtractor<int32_t, int32_t, double>>(
              "AVG_INT32_INT32_DOUBLE", col_index, hash_table);
        case 8:
          return std::make_unique<AVGAggExtractor<int64_t, int64_t, double>>(
              "AVG_INT64_INT64_DOUBLE", col_index, hash_table);
      }
  }
  return nullptr;
}

std::unique_ptr<CiderAggTargetColExtractor>
CiderAggTargetColExtractorBuilder::buildCountAggExtractor(
    const CiderAggHashTable* hash_table,
    size_t col_index) {
  CHECK_LT(col_index, hash_table->getColNum());
  auto& col_info = hash_table->getColEntryInfo(col_index);
  size_t actual_size = hash_table->getActualDataWidth(col_index);

  switch (col_info.sql_type_info.get_type()) {
    case kINT:
    case kBIGINT:
      switch (actual_size) {
        case 4:
          return std::make_unique<CountAggExtractor<int32_t, int64_t>>(
              "INT32_INT64", col_index, hash_table);
        case 8:
          return std::make_unique<CountAggExtractor<int64_t, int64_t>>(
              "INT64_INT64", col_index, hash_table);
      }
    default:
      LOG(ERROR) << "Unsupported count output type: "
                 << col_info.sql_type_info.get_type_name();
      return nullptr;
  }
}