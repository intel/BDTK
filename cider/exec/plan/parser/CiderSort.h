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

/**
 * @file    CiderSort.h
 * @brief   Cider sort
 **/

#pragma once

#include <functional>
#include <vector>
#include "exec/template/RelAlgExecutionUnit.h"
#include "substrait/type.pb.h"

namespace generator {

enum class SortDirection { Ascending, Descending };

enum class NullSortedPosition { First, Last };

class SortField {
 public:
  SortField(const size_t field,
            const SortDirection sort_dir,
            const NullSortedPosition nulls_pos)
      : field_(field), sort_dir_(sort_dir), nulls_pos_(nulls_pos) {}

  bool operator==(const SortField& that) const {
    return field_ == that.field_ && sort_dir_ == that.sort_dir_ &&
           nulls_pos_ == that.nulls_pos_;
  }

  size_t getField() const { return field_; }

  SortDirection getSortDir() const { return sort_dir_; }

  NullSortedPosition getNullsPosition() const { return nulls_pos_; }

  std::string toString() const {
    return cat(::typeName(this),
               "(",
               std::to_string(field_),
               ", sort_dir=",
               (sort_dir_ == SortDirection::Ascending ? "asc" : "desc"),
               ", null_pos=",
               (nulls_pos_ == NullSortedPosition::First ? "nulls_first" : "nulls_last"),
               ")");
  }

  size_t toHash() const {
    auto hash = boost::hash_value(field_);
    boost::hash_combine(hash, sort_dir_ == SortDirection::Ascending ? "a" : "d");
    boost::hash_combine(hash, nulls_pos_ == NullSortedPosition::First ? "f" : "l");
    return hash;
  }

 private:
  const size_t field_;
  const SortDirection sort_dir_;
  const NullSortedPosition nulls_pos_;
};

std::vector<Analyzer::OrderEntry> translate_collation(
    const std::vector<SortField>& sort_fields);

enum CompairResult { Equal, Greater, Less };

struct ResultSetComparator {
  ResultSetComparator(const SortInfo& sort_info,
                      const std::vector<substrait::Type>& types)
      : sort_info_(sort_info), types_(types) {}
  bool isSubtraitIntegerType(const ::substrait::Type& type) const;
  bool isSubtraitFloatType(const ::substrait::Type& type) const;
  bool isSubtraitDateTimeType(const ::substrait::Type& type) const;
  bool isSubtraitStringType(const ::substrait::Type& type) const;
  bool isSubtraitDecimalType(const ::substrait::Type& type) const;
  bool isSubtraitBoolType(const ::substrait::Type& type) const;
  bool isNull(const int8_t* value_ptr, const ::substrait::Type& type) const;
  bool operator()(const std::vector<int8_t*>& lhs, const std::vector<int8_t*>& rhs) const;
  CompairResult compairValue(const int8_t* lhs_value_ptr,
                             const int8_t* rhs_value_ptr,
                             const ::substrait::Type& type) const;
  const SortInfo& sort_info_;
  const std::vector<substrait::Type>& types_;
};

}  // namespace generator
