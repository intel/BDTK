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

#ifndef CIDER_CIDERDECIMAL128_H
#define CIDER_CIDERDECIMAL128_H

#include <cstdint>
#include <limits>
#include <string>

#include "cider/CiderBatch.h"

class CiderInt128Utils {
 public:
  static std::string Int128ToString(__int128_t input);
  static std::string Decimal128ToString(__int128_t input,
                                        uint8_t precision,
                                        uint8_t scale);
  static double Decimal128ToDouble(__int128_t input, uint8_t precision, uint8_t scale);
};

#endif
