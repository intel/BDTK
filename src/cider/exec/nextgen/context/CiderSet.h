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

#ifndef CIDER_SET_H
#define CIDER_SET_H

#include "cider/CiderException.h"
#include "robin_hood.h"

namespace cider::exec::nextgen::context {

class CiderSet {
 public:
  CiderSet() {}
  virtual ~CiderSet() = default;
  virtual void insert(int8_t key_val) = 0;
  virtual void insert(int16_t key_val) = 0;
  virtual void insert(int32_t key_val) = 0;
  virtual void insert(int64_t key_val) = 0;
  virtual void insert(float key_val) = 0;
  virtual void insert(double key_val) = 0;
  virtual void insert(std::string key_val) = 0;

  virtual bool contains(int8_t key_val) = 0;
  virtual bool contains(int16_t key_val) = 0;
  virtual bool contains(int32_t key_val) = 0;
  virtual bool contains(int64_t key_val) = 0;
  virtual bool contains(float key_val) = 0;
  virtual bool contains(double key_val) = 0;
  virtual bool contains(std::string key_val) = 0;
};

class CiderInt64Set : public CiderSet {
 public:
  CiderInt64Set() : CiderSet() {}

  void insert(int8_t key_val) { set_.insert((int64_t)key_val); }

  void insert(int16_t key_val) { set_.insert((int64_t)key_val); }

  void insert(int32_t key_val) { set_.insert((int64_t)key_val); }

  void insert(int64_t key_val) { set_.insert((int64_t)key_val); }

  void insert(float key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderInt64Set doesn't support insert float value.");
  }

  void insert(double key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderInt64Set doesn't support insert double value.");
  }

  void insert(std::string key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderInt64Set doesn't support insert string value.");
  }

  bool contains(int8_t key_val) { return set_.contains((int64_t)key_val); }

  bool contains(int16_t key_val) { return set_.contains((int64_t)key_val); }

  bool contains(int32_t key_val) { return set_.contains((int64_t)key_val); }

  bool contains(int64_t key_val) { return set_.contains(key_val); }

  bool contains(float key_val) {
    CIDER_THROW(CiderRuntimeException, "CiderInt64Set doesn't support find float value.");
  }

  bool contains(double key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderInt64Set doesn't support find double value.");
  }

  bool contains(std::string key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderInt64Set doesn't support find string value.");
  }

 private:
  robin_hood::unordered_set<int64_t> set_;
};

class CiderDoubleSet : public CiderSet {
 public:
  CiderDoubleSet() : CiderSet() {}

  void insert(int8_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support insert int8_t value.");
  }

  void insert(int16_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support insert int16_t value.");
  }

  void insert(int32_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support insert int32_t value.");
  }

  void insert(int64_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support insert int64_t value.");
  }

  void insert(float key_val) { set_.insert((double)key_val); }

  void insert(double key_val) { set_.insert(key_val); }

  void insert(std::string key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support insert string value.");
  }

  bool contains(int8_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support find int8_t value.");
  }

  bool contains(int16_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support find int16_t value.");
  }

  bool contains(int32_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support find int32_t value.");
  }

  bool contains(int64_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support find int64_t value.");
  }

  bool contains(float key_val) { return set_.contains((double)key_val); }

  bool contains(double key_val) { return set_.contains(key_val); }

  bool contains(std::string key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderDoubleSet doesn't support find string value.");
  }

 private:
  robin_hood::unordered_set<double> set_;
};

class CiderStringSet : public CiderSet {
 public:
  CiderStringSet() : CiderSet() {}

  void insert(int8_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support insert int8_t value.");
  }

  void insert(int16_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support insert int16_t value.");
  }

  void insert(int32_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support insert int32_t value.");
  }

  void insert(int64_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support insert int64_t value.");
  }

  void insert(float key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support insert float value.");
  }

  void insert(double key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support insert double value.");
  }

  void insert(std::string key_val) { set_.insert(key_val); }

  bool contains(int8_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support find int8_t value.");
  }

  bool contains(int16_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support find int16_t value.");
  }

  bool contains(int32_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support find int32_t value.");
  }

  bool contains(int64_t key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support find int64_t value.");
  }

  bool contains(float key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support find float value.");
  }

  bool contains(double key_val) {
    CIDER_THROW(CiderRuntimeException,
                "CiderStringSet doesn't support find double value.");
  }

  bool contains(std::string key_val) { return set_.contains(key_val); }

 private:
  robin_hood::unordered_set<std::string> set_;
};

using CiderSetPtr = std::unique_ptr<CiderSet>;

}  // namespace cider::exec::nextgen::context
#endif  // CIDER_SET_H
