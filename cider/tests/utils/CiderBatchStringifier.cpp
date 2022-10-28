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

#include "CiderBatchStringifier.h"
#include "CiderBatchChecker.h"

StructBatchStringifier::StructBatchStringifier(CiderBatch* batch) {
  // fill child_stringifiers_ with stringifiers of corresponding types
  auto col_num = batch->getChildrenNum();
  for (auto col_index = 0; col_index < col_num; ++col_index) {
    auto child = batch->getChildAt(col_index);
    switch (child->getCiderType()) {
      case SQLTypes::kBOOLEAN:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<bool>>());
        break;
      case SQLTypes::kTINYINT:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<int8_t>>());
        break;
      case SQLTypes::kSMALLINT:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<int16_t>>());
        break;
      case SQLTypes::kINT:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<int32_t>>());
        break;
      case SQLTypes::kBIGINT:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<int64_t>>());
        break;
      case SQLTypes::kFLOAT:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<float>>());
        break;
      case SQLTypes::kDOUBLE:
        child_stringifiers_.emplace_back(
            std::make_unique<ScalarBatchStringifier<double>>());
        break;
      case SQLTypes::kDECIMAL:
        child_stringifiers_.emplace_back(std::make_unique<DecimalBatchStringifier>());
        break;
      case SQLTypes::kSTRUCT:
        child_stringifiers_.emplace_back(
            std::make_unique<StructBatchStringifier>(child.get()));
        break;
      default:
        CIDER_THROW(CiderUnsupportedException, "Unsupported type for stringification");
    }
  }
}

std::string StructBatchStringifier::stringifyValueAt(CiderBatch* batch, int row_index) {
  if (!batch) {
    CIDER_THROW(CiderRuntimeException, "StructBatch is nullptr.");
  }

  auto valid_bitmap = batch->getNulls();
  if (valid_bitmap && !CiderBitUtils::isBitSetAt(valid_bitmap, row_index)) {
    // this usually should not happen, values in struct batch are expected to be valid
    // but just in case
    return NULL_VALUE;
  }

  int col_num = batch->getChildrenNum();
  CHECK_EQ(col_num, child_stringifiers_.size());

  ConcatenatedRow row;
  for (auto col_index = 0; col_index < col_num; ++col_index) {
    auto child = batch->getChildAt(col_index);
    auto& col_stringifier = child_stringifiers_[col_index];
    auto value_str = col_stringifier->stringifyValueAt(child.get(), row_index);
    row.addCol(value_str);
  }
  row.finish();
  return row.getString();
}

uint8_t DecimalBatchStringifier::getScale(const ScalarBatch<__int128_t>* batch) {
  auto type_str = std::string(batch->getArrowFormatString());
  uint8_t scale = std::stoi(type_str.substr(type_str.find(',') + 1));
  return scale;
}

uint8_t DecimalBatchStringifier::getWidth(const ScalarBatch<__int128_t>* batch) {
  auto type_str = std::string(batch->getArrowFormatString());
  auto start = type_str.find(':') + 1;
  auto end = type_str.find(',');
  uint8_t width = std::stoi(type_str.substr(start, end - start));
  return width;
}

std::string DecimalBatchStringifier::stringifyValueAt(CiderBatch* batch, int row_index) {
  auto scalar_batch = batch->as<ScalarBatch<__int128_t>>();
  if (!scalar_batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr, maybe check your casting?");
  }

  auto scale = getScale(scalar_batch);
  auto width = getWidth(scalar_batch);
  auto data_buffer = scalar_batch->getRawData();
  auto valid_bitmap = scalar_batch->getNulls();

  if (valid_bitmap && !CiderBitUtils::isBitSetAt(valid_bitmap, row_index)) {
    return NULL_VALUE;
  } else {
    __int128_t value = data_buffer[row_index];

    if (!scale) {
      // integral type
      return CiderInt128Utils::Int128ToString(value);
    } else {
      // fixed-point decimal
      std::stringstream fps;
      fps.clear();
      double value_fp64 = CiderInt128Utils::Decimal128ToDouble(value, width, scale);
      fps << std::setprecision(16) << value_fp64;
      return fps.str();
    }
  }
}

template <typename T>
std::string ScalarBatchStringifier<T>::stringifyValueAt(CiderBatch* batch,
                                                        int row_index) {
  auto scalar_batch = batch->as<ScalarBatch<T>>();
  if (!scalar_batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr, maybe check your casting?");
  }

  auto data_buffer = scalar_batch->getRawData();
  auto valid_bitmap = scalar_batch->getNulls();

  if (valid_bitmap && !CiderBitUtils::isBitSetAt(valid_bitmap, row_index)) {
    return NULL_VALUE;
  } else {
    T value = data_buffer[row_index];
    return std::to_string(static_cast<int64_t>(value));
  }
}

template <>
std::string ScalarBatchStringifier<float>::stringifyValueAt(CiderBatch* batch,
                                                            int row_index) {
  auto scalar_batch = batch->as<ScalarBatch<float>>();
  if (!scalar_batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr, maybe check your casting?");
  }

  auto data_buffer = scalar_batch->getRawData();
  auto valid_bitmap = scalar_batch->getNulls();

  if (valid_bitmap && !CiderBitUtils::isBitSetAt(valid_bitmap, row_index)) {
    return NULL_VALUE;
  } else {
    std::stringstream fps;
    fps.clear();
    float value = data_buffer[row_index];
    fps << std::setprecision(16) << value;
    return fps.str();
  }
}

template <>
std::string ScalarBatchStringifier<double>::stringifyValueAt(CiderBatch* batch,
                                                             int row_index) {
  auto scalar_batch = batch->as<ScalarBatch<double>>();
  if (!scalar_batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr, maybe check your casting?");
  }

  auto data_buffer = scalar_batch->getRawData();
  auto valid_bitmap = scalar_batch->getNulls();

  if (valid_bitmap && !CiderBitUtils::isBitSetAt(valid_bitmap, row_index)) {
    return NULL_VALUE;
  } else {
    std::stringstream fps;
    fps.clear();
    double value = data_buffer[row_index];
    fps << std::setprecision(16) << value;
    return fps.str();
  }
}

template <>
std::string ScalarBatchStringifier<bool>::stringifyValueAt(CiderBatch* batch,
                                                           int row_index) {
  auto scalar_batch = batch->as<ScalarBatch<bool>>();
  if (!scalar_batch) {
    CIDER_THROW(CiderRuntimeException,
                "ScalarBatch is nullptr, maybe check your casting?");
  }

  auto data_buffer = scalar_batch->getRawData();
  auto valid_bitmap = scalar_batch->getNulls();

  if (valid_bitmap && !CiderBitUtils::isBitSetAt(valid_bitmap, row_index)) {
    return NULL_VALUE;
  } else {
    return std::to_string(
        static_cast<int64_t>(CiderBitUtils::isBitSetAt(data_buffer, row_index)));
  }
}
