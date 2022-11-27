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

#include "exec/nextgen/Nextgen.h"

namespace cider::exec::nextgen {

std::unique_ptr<context::CodegenContext> compile(const RelAlgExecutionUnit& ra_exe_unit,
                                                 jitlib::CompilationOptions co) {
  auto context = std::make_unique<context::CodegenContext>();
  jitlib::LLVMJITModule module("codegen", true, co);

  auto builder = [&ra_exe_unit, &context](jitlib::JITFunction* function) {
    context->setJITFunction(function);
    auto pipeline = parsers::toOpPipeline(ra_exe_unit);
    auto translator = transformer::Transformer::toTranslator(pipeline);
    translator->consume(*context);
    function->createReturn();
  };

  jitlib::JITFunctionPointer func =
      jitlib::JITFunctionBuilder()
          .registerModule(module)
          .setFuncName("query_func")
          .addReturn(jitlib::JITTypeTag::VOID)
          .addParameter(jitlib::JITTypeTag::POINTER, "context", jitlib::JITTypeTag::INT8)
          .addParameter(jitlib::JITTypeTag::POINTER, "input", jitlib::JITTypeTag::INT8)
          .addProcedureBuilder(builder)
          .build();

  module.finish();

  return context;
}

}  // namespace cider::exec::nextgen
