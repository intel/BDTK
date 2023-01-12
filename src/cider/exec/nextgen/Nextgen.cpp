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

#include <memory>

#include "jitlib/base/JITFunction.h"

namespace cider::exec::nextgen {

std::unique_ptr<context::CodegenContext> compile(
    RelAlgExecutionUnit& ra_exe_unit,
    const jitlib::CompilationOptions& co,
    const context::CodegenOptions& codegen_options) {
  auto codegen_ctx = std::make_unique<context::CodegenContext>();
  auto module = std::make_shared<jitlib::LLVMJITModule>("codegen", true, co);

  auto builder = [&ra_exe_unit, &codegen_ctx, &codegen_options](
                     jitlib::JITFunctionPointer function) {
    codegen_ctx->setJITFunction(function);
    codegen_ctx->setCodegenOptions(codegen_options);
    auto pipeline = parsers::toOpPipeline(ra_exe_unit);
    auto translator = transformer::Transformer::toTranslator(pipeline);
    translator->consume(*codegen_ctx);
    auto ret_val = function->createVariable(cider::jitlib::JITTypeTag::INT32, "ret", 0);
    function->createReturn(*ret_val.get());
  };

  jitlib::JITFunctionPointer func =
      jitlib::JITFunctionBuilder()
          .registerModule(*module)
          .setFuncName("query_func")
          .addReturn(jitlib::JITTypeTag::INT32)
          .addParameter(jitlib::JITTypeTag::POINTER, "context", jitlib::JITTypeTag::INT8)
          .addParameter(jitlib::JITTypeTag::POINTER, "input", jitlib::JITTypeTag::INT8)
          .addProcedureBuilder(builder)
          .build();

  module->finish();

  codegen_ctx->setJITModule(std::move(module));

  return codegen_ctx;
}

}  // namespace cider::exec::nextgen
