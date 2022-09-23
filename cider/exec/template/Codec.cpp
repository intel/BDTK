/*
 * Copyright (c) 2022 Intel Corporation.
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

#include "Codec.h"
#include <llvm/IR/Constants.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>
#include "LLVMGlobalContext.h"
#include "cider/CiderException.h"
#include "util/Logger.h"

llvm::CallInst* Decoder::extractBufferAt(llvm::Module* module,
                                         llvm::Value* byte_stream,
                                         size_t index) const {
  auto& context = module->getContext();

  auto col_buffer_extractor =
      module->getFunction("cider_ColDecoder_extractArrowBuffersAt");
  CHECK(col_buffer_extractor);
  auto col_buffer_extract_inst = llvm::CallInst::Create(
      col_buffer_extractor,
      {byte_stream, llvm::ConstantInt::get(llvm::Type::getInt64Ty(context), index)});

  return ir_builder_->Insert(col_buffer_extract_inst);
}

llvm::CallInst* Decoder::extractNullVector(llvm::Module* module,
                                           llvm::Value* byte_stream) const {
  if (!nullable_) {
    return nullptr;
  }
  return extractBufferAt(module, byte_stream, 0);
}

FixedWidthInt::FixedWidthInt(const size_t byte_width,
                             llvm::IRBuilder<>* ir_builder,
                             bool nullable)
    : Decoder(ir_builder, nullable), byte_width_{byte_width} {}

llvm::Instruction* FixedWidthInt::codegenDecode(llvm::Value* byte_stream,
                                                llvm::Value* pos,
                                                llvm::Module* module) const {
  auto& context = module->getContext();
  auto f = module->getFunction("fixed_width_int_decode");
  CHECK(f);
  llvm::Value* args[] = {
      byte_stream,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      pos};
  return llvm::CallInst::Create(f, args);
}

std::vector<llvm::Instruction*> FixedWidthInt::codegenDecode(llvm::Module* module,
                                                             llvm::Value* byte_stream,
                                                             llvm::Value* pos) const {
  auto& context = module->getContext();

  auto col_buffer = extractBufferAt(module, byte_stream, 1);
  auto nulls = extractNullVector(module, byte_stream);

  auto f = module->getFunction("fixed_width_int_decode");
  CHECK(f);
  llvm::Value* args[] = {
      col_buffer,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      pos};

  if (nulls) {
    auto get_is_null = module->getFunction("check_bit_vector_clear");
    CHECK(get_is_null);
    return {llvm::CallInst::Create(f, args),
            llvm::CallInst::Create(get_is_null, {nulls, pos})};
  } else {
    return {llvm::CallInst::Create(f, args), nullptr};
  }
}

FixedWidthUnsigned::FixedWidthUnsigned(const size_t byte_width,
                                       llvm::IRBuilder<>* ir_builder,
                                       bool nullable)
    : Decoder(ir_builder, nullable), byte_width_{byte_width} {}

llvm::Instruction* FixedWidthUnsigned::codegenDecode(llvm::Value* byte_stream,
                                                     llvm::Value* pos,
                                                     llvm::Module* module) const {
  auto& context = module->getContext();
  auto f = module->getFunction("fixed_width_unsigned_decode");
  CHECK(f);
  llvm::Value* args[] = {
      byte_stream,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      pos};
  return llvm::CallInst::Create(f, args);
}

std::vector<llvm::Instruction*> FixedWidthUnsigned::codegenDecode(
    llvm::Module* module,
    llvm::Value* byte_stream,
    llvm::Value* pos) const {
  auto& context = module->getContext();

  auto col_buffer = extractBufferAt(module, byte_stream, 1);
  auto nulls = extractNullVector(module, byte_stream);

  auto f = module->getFunction("fixed_width_unsigned_decode");
  CHECK(f);
  llvm::Value* args[] = {
      col_buffer,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      pos};

  if (nulls) {
    auto get_is_null = module->getFunction("check_bit_vector_clear");
    CHECK(get_is_null);
    return {llvm::CallInst::Create(f, args),
            llvm::CallInst::Create(get_is_null, {nulls, pos})};
  } else {
    return {llvm::CallInst::Create(f, args), nullptr};
  }
}

DiffFixedWidthInt::DiffFixedWidthInt(const size_t byte_width,
                                     const int64_t baseline,
                                     llvm::IRBuilder<>* ir_builder,
                                     bool nullable)
    : Decoder(ir_builder, nullable), byte_width_{byte_width}, baseline_{baseline} {}

llvm::Instruction* DiffFixedWidthInt::codegenDecode(llvm::Value* byte_stream,
                                                    llvm::Value* pos,
                                                    llvm::Module* module) const {
  auto& context = module->getContext();
  auto f = module->getFunction("diff_fixed_width_int_decode");
  CHECK(f);
  llvm::Value* args[] = {
      byte_stream,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), baseline_),
      pos};
  return llvm::CallInst::Create(f, args);
}

std::vector<llvm::Instruction*> DiffFixedWidthInt::codegenDecode(llvm::Module* module,
                                                                 llvm::Value* byte_stream,
                                                                 llvm::Value* pos) const {
  auto& context = module->getContext();

  auto col_buffer = extractBufferAt(module, byte_stream, 1);
  auto nulls = extractNullVector(module, byte_stream);

  auto f = module->getFunction("diff_fixed_width_int_decode");
  CHECK(f);
  llvm::Value* args[] = {
      col_buffer,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), baseline_),
      pos};

  if (nulls) {
    auto get_is_null = module->getFunction("check_bit_vector_clear");
    CHECK(get_is_null);
    return {llvm::CallInst::Create(f, args),
            llvm::CallInst::Create(get_is_null, {nulls, pos})};
  } else {
    return {llvm::CallInst::Create(f, args), nullptr};
  }
}

FixedWidthReal::FixedWidthReal(const bool is_double,
                               llvm::IRBuilder<>* ir_builder,
                               bool nullable)
    : Decoder(ir_builder, nullable), is_double_(is_double) {}

llvm::Instruction* FixedWidthReal::codegenDecode(llvm::Value* byte_stream,
                                                 llvm::Value* pos,
                                                 llvm::Module* module) const {
  auto f = module->getFunction(is_double_ ? "fixed_width_double_decode"
                                          : "fixed_width_float_decode");
  CHECK(f);
  llvm::Value* args[] = {byte_stream, pos};
  return llvm::CallInst::Create(f, args);
}

std::vector<llvm::Instruction*> FixedWidthReal::codegenDecode(llvm::Module* module,
                                                              llvm::Value* byte_stream,
                                                              llvm::Value* pos) const {
  auto col_buffer = extractBufferAt(module, byte_stream, 1);
  auto nulls = extractNullVector(module, byte_stream);

  auto f = module->getFunction(is_double_ ? "fixed_width_double_decode"
                                          : "fixed_width_float_decode");
  CHECK(f);
  llvm::Value* args[] = {col_buffer, pos};

  if (nulls) {
    auto get_is_null = module->getFunction("check_bit_vector_clear");
    CHECK(get_is_null);
    return {llvm::CallInst::Create(f, args),
            llvm::CallInst::Create(get_is_null, {nulls, pos})};
  } else {
    return {llvm::CallInst::Create(f, args), nullptr};
  }
}

FixedWidthSmallDate::FixedWidthSmallDate(const size_t byte_width,
                                         llvm::IRBuilder<>* ir_builder,
                                         bool nullable)
    : Decoder(ir_builder, nullable)
    , byte_width_{byte_width}
    , null_val_{byte_width == 4 ? NULL_INT : NULL_SMALLINT} {}

llvm::Instruction* FixedWidthSmallDate::codegenDecode(llvm::Value* byte_stream,
                                                      llvm::Value* pos,
                                                      llvm::Module* module) const {
  auto& context = module->getContext();
  auto f = module->getFunction("fixed_width_small_date_decode");
  CHECK(f);
  llvm::Value* args[] = {
      byte_stream,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), null_val_),
      llvm::ConstantInt::get(llvm::Type::getInt64Ty(context), ret_null_val_),
      pos};
  return llvm::CallInst::Create(f, args);
}

std::vector<llvm::Instruction*> FixedWidthSmallDate::codegenDecode(
    llvm::Module* module,
    llvm::Value* byte_stream,
    llvm::Value* pos) const {
  CIDER_THROW(CiderCompileException,
              "FixedWidthSmallDate decoder is not fully supported.");

  auto& context = module->getContext();

  auto col_buffer = extractBufferAt(module, byte_stream, 1);
  auto nulls = extractNullVector(module, byte_stream);

  auto f = module->getFunction("fixed_width_small_date_decode");
  CHECK(f);
  llvm::Value* args[] = {
      col_buffer,
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), byte_width_),
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(context), null_val_),
      llvm::ConstantInt::get(llvm::Type::getInt64Ty(context), ret_null_val_),
      pos};

  if (nulls) {
    auto get_is_null = module->getFunction("check_bit_vector_clear");
    CHECK(get_is_null);
    return {llvm::CallInst::Create(f, args),
            llvm::CallInst::Create(get_is_null, {nulls, pos})};
  } else {
    return {llvm::CallInst::Create(f, args), nullptr};
  }
}
