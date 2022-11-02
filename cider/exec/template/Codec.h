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

#ifndef QUERYENGINE_CODEC_H
#define QUERYENGINE_CODEC_H

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>

#include "type/data/sqltypes.h"

class Decoder {
 public:
  Decoder(llvm::IRBuilder<>* ir_builder, bool nullable)
      : ir_builder_(ir_builder), nullable_(nullable) {}

  [[deprecated]] virtual llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                                          llvm::Value* pos,
                                                          llvm::Module* module) const = 0;

  // Returns {value, null} for fixed-size data
  virtual std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                        llvm::Value* byte_stream,
                                                        llvm::Value* pos) const = 0;

  virtual ~Decoder() {}

 protected:
  llvm::CallInst* extractBufferAt(llvm::Module* module,
                                  llvm::Value* byte_stream,
                                  size_t index) const;

  llvm::CallInst* extractNullVector(llvm::Module* module, llvm::Value* byte_stream) const;

 private:
  llvm::IRBuilder<>* ir_builder_;
  bool nullable_;
};

class FixedWidthInt : public Decoder {
 public:
  FixedWidthInt(const size_t byte_width,
                llvm::IRBuilder<>* ir_builder,
                bool nullable = false);
  llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                   llvm::Value* pos,
                                   llvm::Module* module) const override;

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;

 private:
  const size_t byte_width_;
};

class FixedWidthBool : public Decoder {
 public:
  FixedWidthBool(llvm::IRBuilder<>* ir_builder, bool nullable = false);

  [[deprecated]] llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                                  llvm::Value* pos,
                                                  llvm::Module* module) const override {
    return nullptr;
  }

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;
};

class FixedWidthUnsigned : public Decoder {
 public:
  FixedWidthUnsigned(const size_t byte_width,
                     llvm::IRBuilder<>* ir_builder,
                     bool nullable = false);
  [[deprecated]] llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                                  llvm::Value* pos,
                                                  llvm::Module* module) const override;

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;

 private:
  const size_t byte_width_;
};

class DiffFixedWidthInt : public Decoder {
 public:
  DiffFixedWidthInt(const size_t byte_width,
                    const int64_t baseline,
                    llvm::IRBuilder<>* ir_builder,
                    bool nullable = false);
  [[deprecated]] llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                                  llvm::Value* pos,
                                                  llvm::Module* module) const override;

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;

 private:
  const size_t byte_width_;
  const int64_t baseline_;
};

class FixedWidthReal : public Decoder {
 public:
  FixedWidthReal(const bool is_double,
                 llvm::IRBuilder<>* ir_builder,
                 bool nullable = false);
  [[deprecated]] llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                                  llvm::Value* pos,
                                                  llvm::Module* module) const override;

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;

 private:
  const bool is_double_;
};

class FixedWidthSmallDate : public Decoder {
 public:
  FixedWidthSmallDate(const size_t byte_width,
                      llvm::IRBuilder<>* ir_builder,
                      bool nullable = false);
  [[deprecated]] llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                                  llvm::Value* pos,
                                                  llvm::Module* module) const override;

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;

 private:
  const size_t byte_width_;
  const int32_t null_val_;
  static constexpr int64_t ret_null_val_ = NULL_BIGINT;
};

class VarcharDecoder : public Decoder {
 public:
  VarcharDecoder(const size_t byte_width,
                 llvm::IRBuilder<>* ir_builder,
                 bool nullable = false);

  llvm::Instruction* codegenDecode(llvm::Value* byte_stream,
                                   llvm::Value* pos,
                                   llvm::Module* module) const override;

  std::vector<llvm::Instruction*> codegenDecode(llvm::Module* module,
                                                llvm::Value* byte_stream,
                                                llvm::Value* pos) const override;

 private:
  const size_t byte_width_;
};

#endif  // QUERYENGINE_CODEC_H
