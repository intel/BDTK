/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#ifndef CIDER_ARROW_STRINGIFIER_H
#define CIDER_ARROW_STRINGIFIER_H

#include <string>
#include "exec/module/batch/ArrowABI.h"
#include "tests/utils/CiderInt128.h"

namespace cider::test::util {

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

class ArrowStringifier {
 public:
  virtual std::string stringifyValueAt(const struct ArrowArray* array,
                                       const struct ArrowSchema* schema,
                                       int row_index) = 0;

 protected:
  bool isNullAt(const struct ArrowArray* array, int row_index);
};

class ArrowStructStringifier : public ArrowStringifier {
 public:
  void init(const struct ArrowArray* array, const struct ArrowSchema* schema);
  // have to call init method before call stringifyValueAt, and input array/schema have to
  // be same.
  std::string stringifyValueAt(const struct ArrowArray* array,
                               const struct ArrowSchema* schema,
                               int row_index);

 private:
  std::vector<std::unique_ptr<ArrowStringifier>> children_stringifiers_;
};

template <typename T>
class ArrowScalarStringifier : public ArrowStringifier {
 public:
  std::string stringifyValueAt(const struct ArrowArray* array,
                               const struct ArrowSchema* schema,
                               int row_index);
};

class ArrowVarcharStringifier : public ArrowStringifier {
 public:
  std::string stringifyValueAt(const struct ArrowArray* array,
                               const struct ArrowSchema* schema,
                               int row_index);
};
}  // namespace cider::test::util
#endif
