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

#pragma once

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"

#include "function/FunctionLookupEngine.h"

namespace validator {

const std::unordered_set<int> supported_types{substrait::Type::kBool,
                                              substrait::Type::kFp32,
                                              substrait::Type::kFp64,
                                              substrait::Type::kI8,
                                              substrait::Type::kI16,
                                              substrait::Type::kI32,
                                              substrait::Type::kI64,
                                              substrait::Type::kTimestamp,
                                              substrait::Type::kVarchar,
                                              substrait::Type::kFixedChar,
                                              substrait::Type::kDate,
                                              substrait::Type::kTime,
                                              substrait::Type::kString};

class SingleNodeValidator {
 public:
  static bool validate(const substrait::Rel& rel_node,
                       const std::unordered_map<int, std::string>& func_map,
                       std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);

  // get outputTypes of a rel node
  static std::vector<substrait::Type> getRelOutputTypes(const substrait::Rel& rel_node);

 private:
  static std::vector<substrait::Type> getRelOutputTypes(
      const substrait::Rel& rel_node,
      const std::vector<substrait::Type>& input_types);

  static substrait::Type getExprOutputType(
      const substrait::Expression& s_expr,
      const std::vector<substrait::Type>& input_types);

  static bool isSupportedAllTypes(const std::vector<substrait::Type>& types);

  static bool validate(const substrait::ReadRel& read_rel,
                       const std::unordered_map<int, std::string>& func_map,
                       std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
  static bool validate(const substrait::FilterRel& filter_rel,
                       const std::unordered_map<int, std::string>& func_map,
                       std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
  static bool validate(const substrait::ProjectRel& proj_rel,
                       const std::unordered_map<int, std::string>& func_map,
                       std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
  static bool validate(const substrait::AggregateRel& agg_rel,
                       const std::unordered_map<int, std::string>& func_map,
                       std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
  static bool validate(const substrait::JoinRel& join_rel,
                       const std::unordered_map<int, std::string>& func_map,
                       std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
  static void functionLookup(const substrait::Expression& s_expr,
                             const std::vector<substrait::Type>& input_types,
                             const std::unordered_map<int, std::string>& func_map,
                             std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
  static void functionLookup(const substrait::AggregateFunction& agg_expr,
                             const std::vector<substrait::Type>& input_types,
                             const std::unordered_map<int, std::string>& func_map,
                             std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr);
};
}  // namespace validator
