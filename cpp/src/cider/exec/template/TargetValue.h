/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#ifndef QUERYENGINE_TARGETVALUE_H
#define QUERYENGINE_TARGETVALUE_H

#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include "util/Logger.h"

#include "cider/CiderException.h"
#include "type/data/sqltypes.h"

#include <cstdint>
#include <string>
#include <vector>

struct InternalTargetValue {
  int64_t i1;
  int64_t i2;

  enum class ITVType { Int, Pair, Str, Arr, Null };

  ITVType ty;

  explicit InternalTargetValue(const int64_t i1_) : i1(i1_), ty(ITVType::Int) {}

  explicit InternalTargetValue(const int64_t i1_, const int64_t i2_)
      : i1(i1_), i2(i2_), ty(ITVType::Pair) {}

  explicit InternalTargetValue(const std::string* s)
      : i1(reinterpret_cast<int64_t>(s)), ty(ITVType::Str) {}

  explicit InternalTargetValue(const std::vector<int64_t>* v)
      : i1(reinterpret_cast<int64_t>(v)), ty(ITVType::Arr) {}

  explicit InternalTargetValue() : ty(ITVType::Null) {}

  std::string strVal() const { return *reinterpret_cast<std::string*>(i1); }

  std::vector<int64_t> arrVal() const {
    return *reinterpret_cast<std::vector<int64_t>*>(i1);
  }

  bool isInt() const { return ty == ITVType::Int; }

  bool isPair() const { return ty == ITVType::Pair; }

  bool isNull() const { return ty == ITVType::Null; }

  bool isStr() const { return ty == ITVType::Str; }

  bool operator<(const InternalTargetValue& other) const {
    switch (ty) {
      case ITVType::Int:
        CHECK(other.ty == ITVType::Int);
        return i1 < other.i1;
      case ITVType::Pair:
        CHECK(other.ty == ITVType::Pair);
        if (i1 != other.i1) {
          return i1 < other.i1;
        }
        return i2 < other.i2;
      case ITVType::Str:
        CHECK(other.ty == ITVType::Str);
        return strVal() < other.strVal();
      case ITVType::Null:
        return false;
      default:
        CIDER_THROW(CiderUnsupportedException,
                    fmt::format("ty is {}, other is {}", ty, other.strVal()));
    }
  }

  bool operator==(const InternalTargetValue& other) const {
    return !(*this < other || other < *this);
  }
};

using NullableString = boost::variant<std::string, void*>;
using ScalarTargetValue = boost::variant<int64_t, double, float, NullableString>;
using ArrayTargetValue = boost::optional<std::vector<ScalarTargetValue>>;
using TargetValue = boost::variant<ScalarTargetValue, ArrayTargetValue>;

#endif  // QUERYENGINE_TARGETVALUE_H
