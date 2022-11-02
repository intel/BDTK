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

#ifndef CIDER_CIDERBATCHCHECKER_H
#define CIDER_CIDERBATCHCHECKER_H

#include <string.h>
#include <iomanip>
#include <numeric>
#include <sstream>
#include "cider/CiderBatch.h"
#include "cider/CiderException.h"
#include "cider/CiderTableSchema.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"

class ConcatenatedRow {
 public:
  ConcatenatedRow() { col_num_ = 0; }

  ConcatenatedRow(int32_t col_num, std::string str) : col_num_{col_num}, str_{str} {}

  void addCol(std::string str) {
    str_.append(str).append(",");
    col_num_++;
  }

  inline void finish() { hash_val_ = hash_(str_); }

  inline std::string getString() { return str_; }
  inline size_t getHashValue() { return hash_val_; }

 private:
  int32_t col_num_;
  std::string str_;
  std::hash<std::string> hash_;
  size_t hash_val_;
};

#define NULL_VALUE "null"

class CiderBatchChecker {
 public:
  // Transfer int type to int64, fp type to double, make each col value together with
  // comma seperated and get a hash value. Use this hash value to check equal. For
  // non-ordered case, use hash value for key and count the value.
  static std::vector<ConcatenatedRow> toConcatenatedRowVector(
      const std::vector<std::shared_ptr<CiderBatch>>& cider_batches);

  static std::vector<ConcatenatedRow> arrowToConcatenatedRowVector(
      const std::vector<std::shared_ptr<CiderBatch>>& cider_batches);

  static bool checkNotEq(const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
                         const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
                         const bool ignore_order = false) {
    return !checkEq(expected_batches, actual_batches, ignore_order);
  }

  static bool checkNotEq(const std::shared_ptr<CiderBatch>& expected_batch,
                         const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
                         const bool ignore_order = false) {
    return !checkEq(expected_batch, actual_batches, ignore_order);
  }

  static bool checkNotEq(const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
                         const std::shared_ptr<CiderBatch>& actual_batch,
                         const bool ignore_order = false) {
    return !checkEq(expected_batches, actual_batch, ignore_order);
  }

  static bool checkNotEq(const std::shared_ptr<CiderBatch>& expected_batch,
                         const std::shared_ptr<CiderBatch>& actual_batch,
                         const bool ignore_order = false) {
    return !checkEq(expected_batch, actual_batch, ignore_order);
  }

  // The procedure will be like:
  // 1. Check col num of each batches for both expected and actual sides.
  // 2. Check column count in schema of each batches.
  // 3. Check row num of each batches for both expected and actual sides.
  // 4. Use memcmp to check if it's one to one batch which is more efficient.
  // 5. If check failed or is multi batch check, transfer these two cider batch vectors
  // to two ConcatenatedRow vectors.
  // 6. Compare these two vectors row by row.
  static bool checkEq(const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
                      const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
                      const bool ignore_order = false);

  static bool checkEq(const std::shared_ptr<CiderBatch>& expected_batch,
                      const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
                      const bool ignore_order = false) {
    std::vector<std::shared_ptr<CiderBatch>> expected_batches{expected_batch};
    return checkEq(expected_batches, actual_batches, ignore_order);
  }

  static bool checkEq(const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
                      const std::shared_ptr<CiderBatch>& actual_batch,
                      const bool ignore_order = false) {
    std::vector<std::shared_ptr<CiderBatch>> actual_batches{actual_batch};
    return checkEq(expected_batches, actual_batches, ignore_order);
  }

  static bool checkEq(const std::shared_ptr<CiderBatch>& expected_batch,
                      const std::shared_ptr<CiderBatch>& actual_batch,
                      const bool ignore_order = false) {
    std::vector<std::shared_ptr<CiderBatch>> expected_batches{expected_batch};
    std::vector<std::shared_ptr<CiderBatch>> actual_batches{actual_batch};
    return checkEq(expected_batches, actual_batches, ignore_order);
  }

  // DO NOT deprecate this one. This is a new method.
  // This method follows the logic of the old checkEq method
  // 1. Col num check: two batches should have the same number of columns
  // 2. Row num check: two batches should have the same total number of rows
  // 3. memcmp check: use memcmp if both vectors contain only one batch
  //    which is more efficient and works as a shortcut
  // 4. Full check: if previous check fails, or cannot be applied,
  //    convert inputs to vectors of ConcatenatedRow and check with hashing
  static bool checkArrowEq(
      const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
      const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
      const bool ignore_order = false);

  static bool checkArrowEq(std::shared_ptr<CiderBatch> expected_batch,
                           std::shared_ptr<CiderBatch> actual_batch,
                           const bool ignore_order = false) {
    std::vector<std::shared_ptr<CiderBatch>> expected_batches{expected_batch};
    std::vector<std::shared_ptr<CiderBatch>> actual_batches{actual_batch};
    return checkArrowEq(expected_batches, actual_batches, ignore_order);
  }

  static bool checkArrowEq(std::shared_ptr<CiderBatch> expected_batch,
                           const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
                           const bool ignore_order = false) {
    std::vector<std::shared_ptr<CiderBatch>> expected_batches{expected_batch};
    return checkArrowEq(expected_batches, actual_batches, ignore_order);
  }

  static bool checkArrowEq(
      const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
      std::shared_ptr<CiderBatch> actual_batch,
      const bool ignore_order = false) {
    std::vector<std::shared_ptr<CiderBatch>> actual_batches{actual_batch};
    return checkArrowEq(expected_batches, actual_batches, ignore_order);
  }

  // To be deprecated. actual_batch will be arrow based batch, just check whether
  // row/column are equal, won't check actual data.
  static bool checkArrowEqTemp(
      const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
      const std::vector<std::shared_ptr<CiderBatch>>& actual_batches);

  static bool checkArrowEqTemp(
      const std::vector<std::shared_ptr<CiderBatch>>& expected_batches,
      const std::shared_ptr<CiderBatch>& actual_batch) {
    std::vector<std::shared_ptr<CiderBatch>> actual_batches{actual_batch};
    return checkArrowEqTemp(expected_batches, actual_batches);
  }

 private:
#define CALL_CHECK_IMPL(C_TYPE)    \
  return checkBufferEqual<C_TYPE>( \
      expected_buffer, expected_offset, actual_buffer, actual_offset, row_num);

  static int getTotalNumOfRows(const std::vector<std::shared_ptr<CiderBatch>>& batches);

  static bool colNumCheck(const std::vector<std::shared_ptr<CiderBatch>>& batches,
                          int expected_col_num);

  template <typename T>
  static bool checkBufferEqual(const int8_t* expected_buffer,
                               const int64_t expected_buffer_offset,
                               const int8_t* actual_buffer,
                               const int64_t actual_buffer_offset,
                               const int row_num) {
    return !memcmp(expected_buffer + sizeof(T) * expected_buffer_offset,
                   actual_buffer + sizeof(T) * actual_buffer_offset,
                   row_num * sizeof(T));
  }

  static bool checkValidityBitmapEqual(const CiderBatch* expected_batch,
                                       const CiderBatch* actual_batch);

  template <typename T>
  static bool checkOneScalarBatchEqual(const ScalarBatch<T>* expected_batch,
                                       const ScalarBatch<T>* actual_batch);

  static bool checkOneStructBatchEqual(CiderBatch* expected_batch,
                                       CiderBatch* actual_batch);

  static bool checkOneVarcharBatchEqual(const VarcharBatch* expected_batch,
                                        const VarcharBatch* actual_batch);

  static bool checkByteArrayEqual(const int8_t* expected_buffer,
                                  const int64_t expected_buffer_offset,
                                  const int8_t* actual_buffer,
                                  const int64_t actual_buffer_offset,
                                  const int row_num,
                                  const bool ignore_order);

  template <typename T>
  static inline T extract_value(const int8_t* buffer, const int index) {
    T* t_buffer = (T*)buffer;
    return t_buffer[index];
  }

  static std::string extract_varchar_value(const int8_t* buffer, const int index) {
    CiderByteArray* byteArrayPtr = (CiderByteArray*)buffer;
    int64_t i = 0;
    uint32_t len = byteArrayPtr[index].len;
    char str[len];
    std::memcpy(&str, byteArrayPtr[index].ptr, len);
    return str;
  }

  template <typename T>
  static inline void update_row(ConcatenatedRow& row,
                                const int8_t* buffer,
                                const int index) {
    T value = extract_value<T>(buffer, index);
    if (value == std::numeric_limits<T>::min()) {
      row.addCol(NULL_VALUE);
    } else {
      row.addCol(std::to_string((int64_t)value));
    }
  }

  static bool checkOneBatchEqual(::substrait::Type& col_type,
                                 const int8_t* expected_buffer,
                                 const int64_t expected_offset,
                                 const int8_t* actual_buffer,
                                 const int64_t actual_offset,
                                 const int row_num) {
    switch (col_type.kind_case()) {
      case substrait::Type::kBool:
      case substrait::Type::kI8:
        CALL_CHECK_IMPL(int8_t);
      case substrait::Type::kI64:
      case substrait::Type::kDate:
      case substrait::Type::kTime:
      case substrait::Type::kTimestamp:
        CALL_CHECK_IMPL(int64_t);
      case substrait::Type::kI32:
        CALL_CHECK_IMPL(int32_t);
      case substrait::Type::kI16:
        CALL_CHECK_IMPL(int16_t);
      case substrait::Type::kFp32:
        CALL_CHECK_IMPL(float);
      case substrait::Type::kFp64:
      case substrait::Type::kDecimal:
        CALL_CHECK_IMPL(double);
      // FIXME: String or Varchar use nullptr for now.
      case substrait::Type::kString:
      case substrait::Type::kVarchar:
      case substrait::Type::kFixedChar:
        return checkByteArrayEqual(
            expected_buffer, expected_offset, actual_buffer, actual_offset, row_num);
      default:
        CIDER_THROW(CiderUnsupportedException,
                    "Unsupported substrait type " + std::to_string(col_type.kind_case()));
    }
  }

  static bool checkColumnCount(const std::shared_ptr<CiderTableSchema> expected_schema,
                               const std::shared_ptr<CiderTableSchema> actual_schema) {
    if ((expected_schema == nullptr || actual_schema == nullptr) ||
        (expected_schema->getColumnCount() != actual_schema->getColumnCount())) {
      return false;
    }
    return true;
  }

  static bool checkCiderByteArrayEqual(CiderByteArray expected, CiderByteArray actual);

  static bool checkByteArrayEqual(const int8_t* expected_buffer,
                                  const int64_t expected_buffer_offset,
                                  const int8_t* actual_buffer,
                                  const int64_t actual_buffer_offset,
                                  const int row_num);

  static bool compareRowVectors(std::vector<ConcatenatedRow> expected_row_vector,
                                std::vector<ConcatenatedRow> actual_row_vector,
                                bool ignore_order);
};

#endif
