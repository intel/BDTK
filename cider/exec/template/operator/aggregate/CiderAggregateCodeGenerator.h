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
#ifndef CIDER_AGGREGATE_CODE_GENERATOR_H
#define CIDER_AGGREGATE_CODE_GENERATOR_H

#include "exec/template/CodegenColValues.h"
#include "util/TargetInfo.h"

struct CgenState;

class AggregateCodeGenerator {
 public:
  virtual void codegen(CodegenColValues* input,
                       llvm::Value* output_buffer,
                       llvm::Value* index,
                       llvm::Value* output_null_buffer = nullptr) const = 0;

 protected:
  llvm::Value* castToIntPtrTyIn(llvm::Value* val, const size_t bitWidth) const;

  std::string base_fname_;
  TargetInfo target_info_;
  size_t slot_size_;
  size_t target_width_;
  size_t arg_width_;
  CgenState* cgen_state_;
};

// Aggregate code generator for SUM, AVG, MIN, MAX, Group-By ID.
class SimpleAggregateCodeGenerator : public AggregateCodeGenerator {
 public:
  static std::unique_ptr<AggregateCodeGenerator> Make(const std::string& base_fname,
                                                      TargetInfo target_info,
                                                      size_t slot_size,
                                                      CgenState* cgen_state);

  void codegen(CodegenColValues* input,
               llvm::Value* output_buffer,
               llvm::Value* index,
               llvm::Value* output_null_buffer = nullptr) const override;

 private:
  bool is_float_float_;  // Both arg and sql type is float.
};

// Aggregate code generator for Project ID.
class ProjectIDCodeGenerator : public SimpleAggregateCodeGenerator {
 public:
  static std::unique_ptr<AggregateCodeGenerator> Make(const std::string& base_fname,
                                                      TargetInfo target_info,
                                                      CgenState* cgen_state);
};

// Aggregate code generator for COUNT(*), COUNT(col), DISTINCT COUNT(col).
class CountAggregateCodeGenerator : public AggregateCodeGenerator {
 public:
  static std::unique_ptr<AggregateCodeGenerator> Make(const std::string& base_fname,
                                                      TargetInfo target_info,
                                                      size_t slot_size,
                                                      CgenState* cgen_state,
                                                      bool has_arg);

  void codegen(CodegenColValues* input,
               llvm::Value* output_buffer,
               llvm::Value* index,
               llvm::Value* output_null_buffer = nullptr) const override;

 protected:
  bool has_arg_;
};

#endif
