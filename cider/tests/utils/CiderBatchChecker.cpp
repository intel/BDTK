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

#include "CiderBatchChecker.h"
#include "Utils.h"

bool CiderBatchChecker::checkEq(
    const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
    const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
    const bool ignore_order) {
  if (expected_batches.size() == 0 || actual_batches.size() == 0) {
    std::cout << "Input params error, shouldn't exist empty vector." << std::endl;
    return false;
  }

  int actual_row_num = 0;
  int expected_col_num = expected_batches[0]->column_num();
  int expected_row_num = expected_batches[0]->row_num();
  auto expected_schema = expected_batches[0]->schema();

  bool schema_check = true;
  // 1.Col num check
  //  check col num of each expected batch
  for (auto i = 1; i < expected_batches.size(); i++) {
    if (expected_batches[i]->column_num() != expected_col_num) {
      std::cout << "Not all col nums of expected batches are equal." << std::endl;
      schema_check = false;
    }
    // 2.Column count check
    // we don't check each column type since data
    // can be the same while schemas are different.
    if (!checkColumnCount(expected_schema, expected_batches[i]->schema())) {
      std::cout << "Not all schemas of expected batches are the same in expected batches."
                << std::endl;
      schema_check = false;
    }
    expected_row_num += expected_batches[i]->row_num();
  }

  // check col num of each actual batch
  for (auto i = 0; i < actual_batches.size(); i++) {
    if (actual_batches[i]->column_num() != expected_col_num) {
      std::cout << "Not all col nums of actual batches are same to those of expected "
                   "batches. "
                << "Expected col num is " << expected_col_num << ", while actual batch "
                << i << " has " << actual_batches[i]->column_num() << " cols."
                << std::endl;
      schema_check = false;
    }
    // 2.Column count check
    // we don't check each column type since data
    // can be the same while schemas are different.
    if (!checkColumnCount(expected_schema, actual_batches[i]->schema())) {
      std::cout << "Not all schemas of actual batches are the same as expected batches."
                << std::endl;
      schema_check = false;
    }
    actual_row_num += actual_batches[i]->row_num();
  }

  // skip schema check when row num is zero
  if (!schema_check && expected_row_num && actual_row_num) {
    return false;
  }

  // 3. Row num check
  if (expected_row_num != actual_row_num) {
    std::cout << "Expected row num and actual row num are not equal. "
              << "Actual row num is " << actual_row_num << ", while expected row num is "
              << expected_row_num << std::endl;
    return false;
  }

  // 4. Handle one to one batch check by memcmp which is more efficient.
  if (expected_batches.size() == 1 && actual_batches.size() == 1) {
    int i = 0;
    for (; i < expected_col_num; i++) {
      ::substrait::Type col_type =
          SchemaUtils::getFlattenColumnTypeById(expected_schema, i);
      if (!checkOneBatchEqual(col_type,
                              expected_batches[0]->column(i),
                              0,
                              actual_batches[0]->column(i),
                              0,
                              expected_row_num)) {
        break;
      }
    }
    if (i == expected_col_num) {
      return true;
    }
    // Memory unmatched does not mean data not the same in some cases when data order
    // is not fixed. So let's make row check.
  }

  // 5. Transfer cider batch vectors to ConcatenatedRow vectors
  std::vector<ConcatenatedRow> expected_row_vector =
      toConcatenatedRowVector(expected_batches);
  std::vector<ConcatenatedRow> actual_row_vector =
      toConcatenatedRowVector(actual_batches);

  // 6. Compare two row vector
  return compareRowVectors(expected_row_vector, actual_row_vector, ignore_order);
}

std::vector<ConcatenatedRow> CiderBatchChecker::toConcatenatedRowVector(
    const std::vector<std::shared_ptr<CiderBatch>>& cider_batches) {
  std::vector<ConcatenatedRow> total_row;
  for (int i = 0; i < cider_batches.size(); i++) {
    auto batch = cider_batches[i];
    auto col_num = batch->column_num();
    for (int row_index = 0; row_index < batch->row_num(); row_index++) {
      ConcatenatedRow row;
      for (int col_index = 0; col_index < col_num; col_index++) {
        auto type = SchemaUtils::getFlattenColumnTypeById(batch->schema(), col_index);
        auto buffer = batch->column(col_index);
        // we use stringstream to keep the precision instead of std::to_string which
        // will only keep 6 digital numbers in fractional part.
        std::stringstream fpStr;
        switch (type.kind_case()) {
          case substrait::Type::kFp64:
          case substrait::Type::kDecimal: {
            fpStr.clear();
            double value1 = extract_value<double>(buffer, row_index);
            if (value1 == std::numeric_limits<double>::min()) {
              row.addCol(NULL_VALUE);
            } else {
              fpStr << std::setprecision(16) << value1;
              row.addCol(fpStr.str());
            }
          } break;
          case substrait::Type::kFp32: {
            fpStr.clear();
            float value1 = extract_value<float>(buffer, row_index);
            if (value1 == std::numeric_limits<float>::min()) {
              row.addCol(NULL_VALUE);
            } else {
              fpStr << std::setprecision(16) << value1;
              row.addCol(fpStr.str());
            }
          } break;
          case substrait::Type::kBool:
          case substrait::Type::kI8:
            update_row<int8_t>(row, buffer, row_index);
            break;
          case substrait::Type::kI16:
            update_row<int16_t>(row, buffer, row_index);
            break;
          case substrait::Type::kI32:
            update_row<int32_t>(row, buffer, row_index);
            break;
          case substrait::Type::kI64:
          case substrait::Type::kDate:
          case substrait::Type::kTime:
          case substrait::Type::kTimestamp:
            update_row<int64_t>(row, buffer, row_index);
            break;
          case substrait::Type::kString:
          case substrait::Type::kVarchar:
          case substrait::Type::kFixedChar:
            row.addCol(extract_varchar_value(buffer, row_index));
            break;
          default:
            CIDER_THROW(CiderCompileException,
                        fmt::format("Unsupported type {}", type.kind_case()));
        }
        if (col_index == col_num - 1) {
          row.finish();
        }
      }
      total_row.push_back(row);
    }
  }
  return total_row;
}

bool CiderBatchChecker::checkByteArrayEqual(const int8_t* expected_buffer,
                                            const int64_t expected_buffer_offset,
                                            const int8_t* actual_buffer,
                                            const int64_t actual_buffer_offset,
                                            const int row_num,
                                            const bool ignore_order) {
  const CiderByteArray* expected_array =
      (const CiderByteArray*)(expected_buffer +
                              sizeof(CiderByteArray) * expected_buffer_offset);
  const CiderByteArray* actual_array =
      (const CiderByteArray*)(actual_buffer +
                              sizeof(CiderByteArray) * actual_buffer_offset);
  for (int i = 0; i < row_num; i++) {
    if (!checkCiderByteArrayEqual(expected_array[i], actual_array[i])) {
      return false;
    }
  }
  return true;
}

bool CiderBatchChecker::checkCiderByteArrayEqual(CiderByteArray expected,
                                                 CiderByteArray actual) {
  return (expected.len == actual.len) &&
         (0 == std::memcmp(expected.ptr, actual.ptr, expected.len));
}

bool CiderBatchChecker::checkByteArrayEqual(const int8_t* expected_buffer,
                                            const int64_t expected_buffer_offset,
                                            const int8_t* actual_buffer,
                                            const int64_t actual_buffer_offset,
                                            const int row_num) {
  const CiderByteArray* expected_array =
      (const CiderByteArray*)(expected_buffer +
                              sizeof(CiderByteArray) * expected_buffer_offset);
  const CiderByteArray* actual_array =
      (const CiderByteArray*)(actual_buffer +
                              sizeof(CiderByteArray) * actual_buffer_offset);
  for (int i = 0; i < row_num; i++) {
    if (!checkCiderByteArrayEqual(expected_array[i], actual_array[i])) {
      return false;
    }
  }
  return true;
}

bool CiderBatchChecker::compareRowVectors(
    std::vector<ConcatenatedRow> expected_row_vector,
    std::vector<ConcatenatedRow> actual_row_vector,
    bool ignore_order) {
    for(auto expect : expected_row_vector){
      std::cout << expect.getString() << std::endl;
    }
    for(auto actual : actual_row_vector){
      std::cout << actual.getString() << std::endl;
    }

  if (expected_row_vector.size() == actual_row_vector.size()) {
    std::map<size_t, int64_t> check_map;
    if (ignore_order) {
      for (int row = 0; row < actual_row_vector.size(); row++) {
        auto actual_key = actual_row_vector[row].getHashValue();
        auto expected_key = expected_row_vector[row].getHashValue();
        check_map[actual_key] =
            check_map.find(actual_key) == check_map.end() ? 1 : check_map[actual_key] + 1;
        check_map[expected_key] = check_map.find(expected_key) == check_map.end()
                                      ? -1
                                      : check_map[expected_key] - 1;
      }
      for (auto entry : check_map) {
        if (entry.second) {
          std::cout << "Non-ordered data not match." << std::endl;
          return false;
        }
      }
      return true;
    } else {
      for (int row = 0; row < actual_row_vector.size(); row++) {
        auto actual_key = actual_row_vector[row].getHashValue();
        auto expected_key = expected_row_vector[row].getHashValue();
        if (actual_key != expected_key) {
          std::cout << "Ordered data not match." << std::endl;
          return false;
        }
      }
    }
  } else {
    std::cout << "The number of row does not match. "
              << "Expected row vector size is " << expected_row_vector.size()
              << ", while actual row vector size is " << actual_row_vector.size()
              << std::endl;
    return false;
  }
  return true;
}
