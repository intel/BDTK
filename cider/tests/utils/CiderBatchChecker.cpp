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
#include "CiderBatchStringifier.h"
#include "Utils.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"

std::vector<ConcatenatedRow> CiderBatchChecker::arrowToConcatenatedRowVector(
    const std::vector<std::shared_ptr<CiderBatch>>& cider_batches) {
  std::vector<ConcatenatedRow> total_row;
  for (auto batch : cider_batches) {
    auto col_num = batch->getChildrenNum();

    auto root_stringifier = std::make_unique<StructBatchStringifier>(batch.get());
    for (int row_index = 0; row_index < batch->getLength(); row_index++) {
      auto batch_str = root_stringifier->stringifyValueAt(batch.get(), row_index);
      CHECK_NE(batch_str, NULL_VALUE);

      ConcatenatedRow row(col_num, batch_str);
      row.finish();
      total_row.push_back(row);
    }
  }

  return total_row;
}

bool CiderBatchChecker::colNumCheck(
    const std::vector<std::shared_ptr<CiderBatch>>& batches,
    int expected_col_num) {
  for (auto i = 0; i < batches.size(); ++i) {
    if (batches[i]->getChildrenNum() != expected_col_num) {
      std::cout << "Batch " << i << " is not having the same number of cols: "
                << "Expected " << expected_col_num << "; Got "
                << batches[i]->getChildrenNum() << std::endl;
      return false;
    }
  }
  return true;
}

int CiderBatchChecker::getTotalNumOfRows(
    const std::vector<std::shared_ptr<CiderBatch>>& batches) {
  int num_rows = 0;
  for (auto batch : batches) {
    num_rows += batch->getLength();
  }
  return num_rows;
}

bool CiderBatchChecker::checkValidityBitmapEqual(const CiderBatch* expected_batch,
                                                 const CiderBatch* actual_batch) {
  auto expected_null_cnt = expected_batch->getNullCount();
  auto actual_null_cnt = actual_batch->getNullCount();
  auto row_num = expected_batch->getLength();

  // skip checking null_count if it is not yet computed (-1)
  if (expected_null_cnt != -1 && actual_null_cnt != -1 &&
      expected_null_cnt != actual_null_cnt) {
    std::cout << "expected_null_count != actual_null_count. "
              << "Expected: " << expected_null_cnt << ". Actual: " << actual_null_cnt
              << std::endl;
    return false;
  }

  auto expected_buffer = expected_batch->getNulls();
  auto actual_buffer = actual_batch->getNulls();

  if (!expected_buffer && !actual_buffer) {
    // both buffers are nullptr, no need to check
    return true;
  } else if (expected_buffer && actual_buffer) {
    // both buffers exist
    return CiderBitUtils::CheckBitVectorEq(expected_buffer, actual_buffer, row_num);
  } else {
    // one is nullptr but the other is not, null_count of the other batch must be 0
    if (expected_buffer) {
      // actual values are all valid, expected null count should be 0
      auto test_null_cnt = expected_null_cnt == -1
                               ? CiderBitUtils::countUnsetBits(expected_buffer, row_num)
                               : expected_null_cnt;
      return test_null_cnt == 0;
    } else {
      // expected values are all valid, actual null count should be 0
      auto test_null_cnt = actual_null_cnt == -1
                               ? CiderBitUtils::countUnsetBits(actual_buffer, row_num)
                               : actual_null_cnt;
      return test_null_cnt == 0;
    }
  }
}

template <typename T>
bool CiderBatchChecker::checkOneScalarBatchEqual(const ScalarBatch<T>* expected_batch,
                                                 const ScalarBatch<T>* actual_batch) {
  if (!expected_batch || !actual_batch) {
    // casting a ScalarBatch to types different from its original type will yield nullptr
    // however, the underlying data can be the same even if the types are different
    // so we skip memcmp and check results by ConcatenatedRow hashing
    std::cout << "One or more ScalarBatches are null_ptr in checkOneScalarBatchEqual. "
              << "This can be caused by casting a ScalarBatch to a wrong type."
              << std::endl;
    return false;
  }
  auto expected_data_buffer = expected_batch->getRawData();
  auto actual_data_buffer = actual_batch->getRawData();

  int row_num = actual_batch->getLength();

  // compare nulls
  bool null_buffer_eq = checkValidityBitmapEqual(expected_batch, actual_batch);
  if (!null_buffer_eq) {
    std::cout << "Null buffer memcmp failed." << std::endl;
    return false;
  }

  // compare data
  bool data_buffer_eq = true;
  data_buffer_eq = !memcmp(expected_data_buffer, actual_data_buffer, row_num * sizeof(T));
  if (!data_buffer_eq) {
    std::cout << "Data buffer memcmp failed." << std::endl;
    return false;
  }
  return true;
}

bool CiderBatchChecker::checkOneStructBatchEqual(CiderBatch* expected_batch,
                                                 CiderBatch* actual_batch) {
  // compare nulls
  bool null_buffer_eq =
      checkValidityBitmapEqual(const_cast<const CiderBatch*>(expected_batch),
                               const_cast<const CiderBatch*>(actual_batch));
  if (!null_buffer_eq) {
    std::cout << "Null buffer memcmp failed." << std::endl;
    return false;
  }

  // compare all children
  auto expected_col_num = expected_batch->getChildrenNum();
  int i = 0;
  for (i = 0; i < expected_col_num; ++i) {
    bool is_equal = true;
    auto expected_child = expected_batch->getChildAt(i);
    auto actual_child = actual_batch->getChildAt(i);
    switch (expected_child->getCiderType()) {
      case SQLTypes::kTINYINT:
        is_equal =
            checkOneScalarBatchEqual<int8_t>(expected_child->as<ScalarBatch<int8_t>>(),
                                             actual_child->as<ScalarBatch<int8_t>>());
        break;
      case SQLTypes::kSMALLINT:
        is_equal =
            checkOneScalarBatchEqual<int16_t>(expected_child->as<ScalarBatch<int16_t>>(),
                                              actual_child->as<ScalarBatch<int16_t>>());
        break;
      case SQLTypes::kINT:
        is_equal =
            checkOneScalarBatchEqual<int32_t>(expected_child->as<ScalarBatch<int32_t>>(),
                                              actual_child->as<ScalarBatch<int32_t>>());
        break;
      case SQLTypes::kBIGINT:
        is_equal =
            checkOneScalarBatchEqual<int64_t>(expected_child->as<ScalarBatch<int64_t>>(),
                                              actual_child->as<ScalarBatch<int64_t>>());
        break;
      case SQLTypes::kFLOAT:
        is_equal =
            checkOneScalarBatchEqual<float>(expected_child->as<ScalarBatch<float>>(),
                                            actual_child->as<ScalarBatch<float>>());
        break;
      case SQLTypes::kDOUBLE:
        is_equal =
            checkOneScalarBatchEqual<double>(expected_child->as<ScalarBatch<double>>(),
                                             actual_child->as<ScalarBatch<double>>());
        break;
      case SQLTypes::kSTRUCT:
        is_equal = checkOneStructBatchEqual(expected_child.get(), actual_child.get());
        break;
      default:
        CIDER_THROW(CiderRuntimeException, "Unsupported type for checking.");
    }
    if (!is_equal) {
      std::cout << "checkOneStructBatch failed at child: " << i << std::endl;
      return false;
    }
  }
  return true;
}

bool CiderBatchChecker::checkArrowEq(
    const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
    const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
    const bool ignore_order) {
  if (expected_batches.size() == 0 || actual_batches.size() == 0) {
    std::cout << "Input params error, shouldn't exist empty vector." << std::endl;
    return false;
  }

  /// NOTE: (YBRua) We are not checking the actual ArrowSchema
  /// because this method is a protected member, and we cannot get the schema
  // auto expected_schema = expected_batches[0]->getArrowSchema();

  // 1. Column & schema check:
  // expected_batches and actual_batches must have the same number of columns
  bool schema_check = true;  // this flag indicates whether we pass step 1
  int expected_col_num = expected_batches[0]->getChildrenNum();

  schema_check &= colNumCheck(expected_batches, expected_col_num);
  schema_check &= colNumCheck(actual_batches, expected_col_num);

  int expected_row_num = getTotalNumOfRows(expected_batches);
  int actual_row_num = getTotalNumOfRows(actual_batches);

  // consider check fail if num of cols do not match
  // unless the results does not contain tuples
  if (!schema_check && expected_row_num && actual_row_num) {
    return false;
  }

  // 2. Row num check:
  // expected result and actual result must have the same number of rows (tuples)
  if (expected_row_num != actual_row_num) {
    std::cout << "expected_row_num != actual_row_num: "
              << "Expected: " << expected_row_num << ". Actual: " << actual_row_num
              << std::endl;
    return false;
  }

  // no need for further check if outputs contain 0 tuples
  if (expected_row_num == 0 && actual_row_num == 0) {
    return true;
  }

  // 3. Hi-efficiency data check via memcmp
  // if input only have one batch, we can check with memcmp() which is more efficient
  // This check is only a shortcut
  // even if it fails, it does not necessarily mean the results are wrong
  // we can goto step 4 and check the results row-by-row
  if (expected_batches.size() == 1 && actual_batches.size() == 1) {
    bool one_batch_check =
        checkOneStructBatchEqual(expected_batches[0].get(), actual_batches[0].get());
    if (one_batch_check) {
      return true;
    }
  }

  // 4. Row vector check
  // convert CiderBatches to vectors of concatenated rows, and compare the rows
  auto expected_rowvec = arrowToConcatenatedRowVector(expected_batches);
  auto actual_rowvec = arrowToConcatenatedRowVector(actual_batches);

  return compareRowVectors(expected_rowvec, actual_rowvec, ignore_order);
}

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

bool CiderBatchChecker::checkArrowEqTemp(
    const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
    const std::vector<std::shared_ptr<CiderBatch>>& actual_batches) {
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
    if (actual_batches[i]->getChildrenNum() != expected_col_num) {
      std::cout << "Not all col nums of actual batches are same to those of expected "
                   "batches. "
                << "Expected col num is " << expected_col_num << ", while actual batch "
                << i << " has " << actual_batches[i]->getChildrenNum() << " cols."
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
    actual_row_num += actual_batches[i]->getLength();
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
  return true;
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
