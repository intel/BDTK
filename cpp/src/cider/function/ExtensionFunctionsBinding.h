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
#ifndef CIDER_FUNCTION_EXTENSIONFUNCTIONSBINDING_H
#define CIDER_FUNCTION_EXTENSIONFUNCTIONSBINDING_H

#include "function/ExtensionFunctionsWhitelist.h"

#include "type/data/sqltypes.h"
#include "type/plan/Analyzer.h"

#include <tuple>
#include <vector>

namespace Analyzer {
class FunctionOper;
}  // namespace Analyzer

ExtensionFunction bind_function(std::string name,
                                Analyzer::ExpressionPtrVector func_args);

ExtensionFunction bind_function(std::string name,
                                Analyzer::ExpressionPtrVector func_args);

ExtensionFunction bind_function(const Analyzer::FunctionOper* function_oper);

#endif  // CIDER_FUNCTION_EXTENSIONFUNCTIONSBINDING_H
