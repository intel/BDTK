/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
#ifndef DICTREF_H
#define DICTREF_H

#include <cstdint>
#include <cstdlib>
#include <functional>

struct dict_ref_t {
  int32_t dbId;
  int32_t dictId;

  dict_ref_t() {}
  dict_ref_t(int32_t db_id, int32_t dict_id) : dbId(db_id), dictId(dict_id) {}

  inline bool operator==(const struct dict_ref_t& rhs) const {
    return this->dictId == rhs.dictId && this->dbId == rhs.dbId;
  }

  inline struct dict_ref_t& operator=(const struct dict_ref_t& rhs) {
    this->dbId = rhs.dbId;
    this->dictId = rhs.dictId;
    return *this;
  }

  inline bool operator<(const struct dict_ref_t& rhs) const {
    return (this->dbId < rhs.dbId)
               ? true
               : (this->dbId == rhs.dbId) ? this->dictId < rhs.dictId : false;
  }

  inline size_t operator()(const struct dict_ref_t& ref) const noexcept {
    std::hash<int32_t> int32_hash;
    return int32_hash(ref.dictId) ^ (int32_hash(ref.dbId) << 2);
  }
};

using DictRef = struct dict_ref_t;

#endif
