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

/**
 * @file    QueryCompilationDescriptor.h
 * @brief   Container for compilation results and assorted options for a single execution
 * unit.
 */

#pragma once

#include "exec/template/CgenState.h"
#include "exec/template/CompilationContext.h"
#include "exec/template/PlanState.h"

struct CompilationResult {
  std::shared_ptr<CompilationContext> generated_code;
  std::unordered_map<int, CgenState::LiteralValues> literal_values;
  bool output_columnar;
  std::string llvm_ir;
  std::vector<int64_t> join_hash_table_ptrs;
};
