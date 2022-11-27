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

std::unique_ptr<context::CodegenContext> compile(const RelAlgExecutionUnit& ra_exe_unit,
                                                 jitlib::CompilationOptions co);

}  // namespace cider::exec::nextgen

#endif  // EXEC_NEXTGEN_NEXTGEN_H
