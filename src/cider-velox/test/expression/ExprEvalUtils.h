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

#include "VeloxToCiderExpr.h"
#include "cider/CiderRuntimeModule.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

namespace facebook::velox::plugin {
using RowTypePtr = std::shared_ptr<const RowType>;

class ExprEvalUtils {
 public:
  static RelAlgExecutionUnit getMockedRelAlgEU(
      std::shared_ptr<const core::ITypedExpr> v_expr,
      RowTypePtr row_type,
      std::string eu_group);
  static std::vector<InputTableInfo> buildInputTableInfo();

  static AggregatedColRange buildDefaultColRangeCache(RowTypePtr row_type);

 private:
  static SQLTypeInfo getCiderType(const std::shared_ptr<const velox::Type> expr_type,
                                  bool isNullable);
  static Analyzer::Expr* getExpr(std::shared_ptr<const Analyzer::Expr> expr);
};
}  // namespace facebook::velox::plugin
