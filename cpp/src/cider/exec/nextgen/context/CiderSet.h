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

#ifndef CIDER_SET_H
#define CIDER_SET_H

#include "robin_hood.h"

namespace cider::exec::nextgen::context {

class CiderSet {
 public:
  CiderSet() = default;
  virtual ~CiderSet() = default;

#define DEF_CIDER_SET_INSERT(type) virtual void insert(type key_val);

  DEF_CIDER_SET_INSERT(int8_t)
  DEF_CIDER_SET_INSERT(int16_t)
  DEF_CIDER_SET_INSERT(int32_t)
  DEF_CIDER_SET_INSERT(int64_t)
  DEF_CIDER_SET_INSERT(float)
  DEF_CIDER_SET_INSERT(double)
  DEF_CIDER_SET_INSERT(std::string&)

#define DEF_CIDER_SET_CONTAINS(type) virtual bool contains(type key_val);

  DEF_CIDER_SET_CONTAINS(int8_t)
  DEF_CIDER_SET_CONTAINS(int16_t)
  DEF_CIDER_SET_CONTAINS(int32_t)
  DEF_CIDER_SET_CONTAINS(int64_t)
  DEF_CIDER_SET_CONTAINS(float)
  DEF_CIDER_SET_CONTAINS(double)
  DEF_CIDER_SET_CONTAINS(std::string&)

#undef DEF_CIDER_SET_INSERT
#undef DEF_CIDER_SET_CONTAINS
};

class CiderInt64Set : public CiderSet {
 public:
  using CiderSet::CiderSet;

  void insert(int8_t key_val) override;
  void insert(int16_t key_val) override;
  void insert(int32_t key_val) override;
  void insert(int64_t key_val) override;
  bool contains(int8_t key_val) override;
  bool contains(int16_t key_val) override;
  bool contains(int32_t key_val) override;
  bool contains(int64_t key_val) override;

 private:
  robin_hood::unordered_set<int64_t> set_;
};

class CiderDoubleSet : public CiderSet {
 public:
  using CiderSet::CiderSet;

  void insert(float key_val) override;
  void insert(double key_val) override;
  bool contains(float key_val) override;
  bool contains(double key_val) override;

 private:
  robin_hood::unordered_set<double> set_;
};

class CiderStringSet : public CiderSet {
 public:
  using CiderSet::CiderSet;

  void insert(std::string& key_val) override;
  bool contains(std::string& key_val) override;

 private:
  robin_hood::unordered_set<std::string> set_;
};

using CiderSetPtr = std::unique_ptr<CiderSet>;

}  // namespace cider::exec::nextgen::context
#endif  // CIDER_SET_H
