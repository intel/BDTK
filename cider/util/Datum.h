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

/**
 * @file		Datum.h
 * @brief	 Definitions for core Datum union type
 *
 */

#pragma once

#include "type/data/funcannotations.h"

#ifndef __CUDACC__
#include <string>
#endif

struct VarlenDatum {
  size_t length;
  int8_t* pointer;
  bool is_null;

  VarlenDatum() : length(0), pointer(nullptr), is_null(true) {}
  virtual ~VarlenDatum() {}

  VarlenDatum(const size_t l, int8_t* p, const bool n)
      : length(l), pointer(p), is_null(n) {}
};

union Datum {
  int8_t boolval;
  int8_t tinyintval;
  int16_t smallintval;
  int32_t intval;
  int64_t bigintval;
  float floatval;
  double doubleval;
  VarlenDatum* arrayval;
#ifndef __CUDACC__
  std::string* stringval;  // string value
#endif
};
