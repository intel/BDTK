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
#ifndef CODEGEN_COL_VALUES_H
#define CODEGEN_COL_VALUES_H

#include <llvm/IR/Value.h>

#define DEF_CODEGEN_COL_VALUES_GET_FUN(fname, member) \
  llvm::Value* get##fname() { return member; }        \
  const llvm::Value* get##fname() const { return member; }

#define DEF_CODEGEN_COL_VALUES_SET_FUN(fname, member) \
  void set##fname(llvm::Value* value) { member = value; }

#define DEF_CODEGEN_COL_VALUES_MEMBER(member, variable) \
 public:                                                \
  DEF_CODEGEN_COL_VALUES_GET_FUN(member, variable)      \
  DEF_CODEGEN_COL_VALUES_SET_FUN(member, variable)      \
 private:                                               \
  llvm::Value* variable;

#define DEF_CODEGEN_COL_VALUES_COPY_FUN(cname)              \
  std::unique_ptr<CodegenColValues> copy() const override { \
    return std::make_unique<cname>(*this);                  \
  }

// Different data type may have multiple LLVM instructions. To pass these instructions
// between different expr nodes, define various container class inherit from
// CodegenColValues as an unified interface.
class CodegenColValues {
 public:
  virtual ~CodegenColValues() = default;

  virtual std::unique_ptr<CodegenColValues> copy() const = 0;
};

class NullableColValues : public CodegenColValues {
 public:
  NullableColValues(llvm::Value* null = nullptr) : null_(null) {}

  DEF_CODEGEN_COL_VALUES_COPY_FUN(NullableColValues)

  DEF_CODEGEN_COL_VALUES_MEMBER(Null, null_)
};

class FixedSizeColValues : public NullableColValues {
 public:
  FixedSizeColValues(llvm::Value* value, llvm::Value* null = nullptr)
      : NullableColValues(null), value_(value) {}

  DEF_CODEGEN_COL_VALUES_COPY_FUN(FixedSizeColValues)

  DEF_CODEGEN_COL_VALUES_MEMBER(Value, value_)
};

class MultipleValueColValues : public NullableColValues {
 public:
  MultipleValueColValues(std::vector<llvm::Value*> values, llvm::Value* null = nullptr)
      : NullableColValues(null), values_(values) {}
  std::unique_ptr<CodegenColValues> copy() const override {
    return std::make_unique<MultipleValueColValues>(*this);
  }
  std::vector<llvm::Value*> getValues() { return values_; }
  const std::vector<llvm::Value*> getValues() const { return values_; }
  llvm::Value* getValueAt(int index) { return values_[index]; }

 private:
  std::vector<llvm::Value*> values_;
};

#endif
