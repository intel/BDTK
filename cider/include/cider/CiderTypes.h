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

#ifndef CIDERTYPES_H
#define CIDERTYPES_H

#include <string>

const int kSecondsInOneDay = 24 * 60 * 60;

class CiderTimeType {};
class CiderTimeStampType {};

// this class is used to better gen date type column. Cider will use a 64 bit integer to
// represent Date type, the value is epoch from 1970-01-01 in seconds, if you want to
// convert to days elapsed, you need to divide 24*60*60.
class CiderDateType {
 public:
  explicit CiderDateType(const std::string& string_val)
      : valid_(validate(string_val))
      , string_val_(string_val)
      , int64_val_(toInt64(string_val)) {}

  // TODO: impl
  explicit CiderDateType(const int64_t int64_val)
      : valid_(validate(int64_val))
      , int64_val_(int64_val)
      , string_val_(toString(int64_val)) {}

  static int64_t toInt64(const std::string& string_val);
  // TODO: impl
  static std::string toString(const int64_t int64_val) { return ""; }

  int64_t getInt64Val() const { return int64_val_; }
  std::string getStringVal() const { return string_val_; }

 private:
  // TODO: impl
  static bool validate(const std::string& string_val) { return true; }
  static bool validate(const int64_t int64_val) { return true; }

  const bool valid_;
  const std::string string_val_;
  const int64_t int64_val_;
};

// same as Apache Parquet FixedLenByteArray definition. Use ptr as start address of the
// string since the length is same, and defined in column schema.
// NOTE: not used yet.
struct CiderFixedLenByteArray {
  CiderFixedLenByteArray() : ptr(nullptr) {}
  explicit CiderFixedLenByteArray(const uint8_t* ptr) : ptr(ptr) {}
  const uint8_t* ptr;
};

// same as Apache Parquet ByteArray definition. use ptr and len represent a string/varchar
// TODO: what about null ?
struct CiderByteArray {
  CiderByteArray() : len(0), ptr(nullptr) {}
  CiderByteArray(uint32_t len, const uint8_t* ptr) : len(len), ptr(ptr) {}

  static std::string toString(const CiderByteArray& ciderByteArray);

  uint32_t len;
  const uint8_t* ptr;
};

// TODO: Placeholder for arrow array layout.
struct CiderArrowByteArray {
  uint32_t len;
  uint8_t* ptr;
  uint32_t* offsets;
};
#endif  // CIDERTYPES_H
