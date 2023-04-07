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

#include "exec/nextgen/context/CiderSet.h"

#include "include/cider/CiderException.h"

namespace cider::exec::nextgen::context {
#define DEF_CIDER_SET_INSERT(type)                                                 \
  void CiderSet::insert(type key_val) {                                            \
    std::string name(typeid(*this).name());                                        \
    CIDER_THROW(CiderRuntimeException, name + " doesn't support insert " + #type); \
  }
DEF_CIDER_SET_INSERT(int8_t)
DEF_CIDER_SET_INSERT(int16_t)
DEF_CIDER_SET_INSERT(int32_t)
DEF_CIDER_SET_INSERT(int64_t)
DEF_CIDER_SET_INSERT(float)
DEF_CIDER_SET_INSERT(double)
DEF_CIDER_SET_INSERT(std::string&)

#define DEF_CIDER_SET_CONTAINS(type)                                               \
  bool CiderSet::contains(type key_val) {                                          \
    std::string name(typeid(*this).name());                                        \
    CIDER_THROW(CiderRuntimeException, name + " doesn't support search " + #type); \
  }
DEF_CIDER_SET_CONTAINS(int8_t)
DEF_CIDER_SET_CONTAINS(int16_t)
DEF_CIDER_SET_CONTAINS(int32_t)
DEF_CIDER_SET_CONTAINS(int64_t)
DEF_CIDER_SET_CONTAINS(float)
DEF_CIDER_SET_CONTAINS(double)
DEF_CIDER_SET_CONTAINS(std::string&)

#undef DEF_CIDER_SET_INSERT
#undef DEF_CIDER_SET_CONTAINS

void CiderInt64Set::insert(int8_t key_val) {
  set_.insert((int64_t)key_val);
}

void CiderInt64Set::insert(int16_t key_val) {
  set_.insert((int64_t)key_val);
}

void CiderInt64Set::insert(int32_t key_val) {
  set_.insert((int64_t)key_val);
}

void CiderInt64Set::insert(int64_t key_val) {
  set_.insert((int64_t)key_val);
}

bool CiderInt64Set::contains(int8_t key_val) {
  return set_.contains((int64_t)key_val);
}

bool CiderInt64Set::contains(int16_t key_val) {
  return set_.contains((int64_t)key_val);
}

bool CiderInt64Set::contains(int32_t key_val) {
  return set_.contains((int64_t)key_val);
}

bool CiderInt64Set::contains(int64_t key_val) {
  return set_.contains(key_val);
}

void CiderDoubleSet::insert(float key_val) {
  set_.insert((double)key_val);
}

void CiderDoubleSet::insert(double key_val) {
  set_.insert(key_val);
}

bool CiderDoubleSet::contains(float key_val) {
  return set_.contains((double)key_val);
}

bool CiderDoubleSet::contains(double key_val) {
  return set_.contains(key_val);
}

void CiderStringSet::insert(std::string& key_val) {
  set_.insert(key_val);
}

bool CiderStringSet::contains(std::string& key_val) {
  return set_.contains(key_val);
}
};  // namespace cider::exec::nextgen::context
