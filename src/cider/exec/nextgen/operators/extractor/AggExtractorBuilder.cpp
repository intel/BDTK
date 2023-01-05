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
#include "exec/nextgen/operators/extractor/AggExtractorBuilder.h"

namespace cider::exec::nextgen::operators {
std::unique_ptr<NextgenAggExtractor> NextgenAggExtractorBuilder::buildNextgenAggExtractor(
    const int8_t* buffer,
    context::AggExprsInfo& info) {
  switch (info.agg_type_) {
    case SQLAgg::kAVG:
      return buildAVGAggExtractor(buffer);
    default:
      return buildBasicAggExtractor(buffer, info);
  }
}

std::unique_ptr<NextgenAggExtractor> NextgenAggExtractorBuilder::buildBasicAggExtractor(
    const int8_t* buffer,
    context::AggExprsInfo& info) {
  size_t actual_size = info.byte_size_;

  switch (info.sql_type_info_.get_type()) {
    case kTINYINT:
      switch (actual_size) {
        case 4:
          return std::make_unique<NextgenBasicAggExtractor<int32_t, int8_t>>(
              "INT32_INT8", buffer, info);
        case 8:
          return std::make_unique<NextgenBasicAggExtractor<int64_t, int8_t>>(
              "INT64_INT8", buffer, info);
      }
    case kSMALLINT:
      switch (actual_size) {
        case 4:
          return std::make_unique<NextgenBasicAggExtractor<int32_t, int16_t>>(
              "INT32_INT16", buffer, info);
        case 8:
          return std::make_unique<NextgenBasicAggExtractor<int64_t, int16_t>>(
              "INT64_INT16", buffer, info);
      }
    case kINT:
      switch (actual_size) {
        case 4:
          return std::make_unique<NextgenBasicAggExtractor<int32_t, int32_t>>(
              "INT32_INT32", buffer, info);
        case 8:
          return std::make_unique<NextgenBasicAggExtractor<int64_t, int32_t>>(
              "INT64_INT32", buffer, info);
      }

    case kBIGINT:
      switch (actual_size) {
        case 4:
          return std::make_unique<NextgenBasicAggExtractor<int32_t, int64_t>>(
              "INT32_INT64", buffer, info);
        case 8:
          return std::make_unique<NextgenBasicAggExtractor<int64_t, int64_t>>(
              "INT64_INT64", buffer, info);
      }
    case kFLOAT:
      switch (actual_size) {
        case 4:
          return std::make_unique<NextgenBasicAggExtractor<float, float>>(
              "FLOAT_FLOAT", buffer, info);
        case 8:
          return std::make_unique<NextgenBasicAggExtractor<double, float>>(
              "DOUBLE_FLOAT", buffer, info);
      }
    case kDOUBLE:
      switch (actual_size) {
        case 4:
          return std::make_unique<NextgenBasicAggExtractor<float, double>>(
              "FLOAT_DOUBLE", buffer, info);

        case 8:
          return std::make_unique<NextgenBasicAggExtractor<double, double>>(
              "DOUBLE_DOUBLE", buffer, info);
      }
    case kBOOLEAN:
    case kDECIMAL:
    case kTEXT:
    case kVARCHAR:
    default:
      LOG(ERROR) << "Unsupported type. ";
      return nullptr;
  }
}

std::unique_ptr<NextgenAggExtractor> NextgenAggExtractorBuilder::buildAVGAggExtractor(
    const int8_t* buffer) {
  LOG(FATAL) << "Avg agg extractor is not support yet";
  return nullptr;
}
}  // namespace cider::exec::nextgen::operators
