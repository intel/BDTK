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
 * @file    CiderExprEvaluator.h
 * @brief   Provide expression evaluation API
 **/

#ifndef CIDER_CIDEREXPREVALUATOR_H
#define CIDER_CIDEREXPREVALUATOR_H

#include "cider/CiderBatch.h"
#include "cider/CiderRuntimeModule.h"
#include "substrait/algebra.pb.h"

using namespace generator;
class CiderExprEvaluator {
 public:
  explicit CiderExprEvaluator(
      std::vector<::substrait::Expression*> exprs,
      std::vector<::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
          funcs_info,
      ::substrait::NamedStruct* schema,
      const generator::ExprType& expr_type);

  CiderBatch eval(const CiderBatch& in_batch);

 private:
  std::shared_ptr<CiderRuntimeModule> runner_;
};
#endif  // CIDER_CIDEREXPREVALUATOR_H
