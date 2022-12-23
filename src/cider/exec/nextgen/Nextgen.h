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
#ifndef EXEC_NEXTGEN_NEXTGEN_H
#define EXEC_NEXTGEN_NEXTGEN_H

#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/parsers/Parser.h"
#include "exec/nextgen/transformer/Transformer.h"

namespace cider::exec::nextgen {

using QueryFunc = int32_t (*)(int8_t*, int8_t*);

std::unique_ptr<context::CodegenContext> compile(
    const RelAlgExecutionUnit& eu,
    const jitlib::CompilationOptions& co = jitlib::CompilationOptions{});

}  // namespace cider::exec::nextgen

enum ERROR_CODE {
  ERR_DIV_BY_ZERO = 1,
  ERR_OUT_OF_SLOTS = 3,
  ERR_UNSUPPORTED_SELF_JOIN = 4,
  ERR_OUT_OF_CPU_MEM = 6,
  ERR_OVERFLOW_OR_UNDERFLOW = 7,
  ERR_OUT_OF_TIME = 9,
  ERR_INTERRUPTED = 10,
  ERR_COLUMNAR_CONVERSION_NOT_SUPPORTED = 11,
  ERR_TOO_MANY_LITERALS = 12,
  ERR_STRING_CONST_IN_RESULTSET = 13,
  ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES = 15,
  ERR_WIDTH_BUCKET_INVALID_ARGUMENT = 16
};

#endif  // EXEC_NEXTGEN_NEXTGEN_H
