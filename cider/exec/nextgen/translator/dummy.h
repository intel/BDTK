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
#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_DUMMY_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_DUMMY_H

#include <initializer_list>
#include <memory>
#include <utility>
#include <vector>

#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>

#include "type/plan/Analyzer.h"

class Context {
 public:
  Context() : llvm_context_(new llvm::LLVMContext()), ir_builder_(*llvm_context_) {
    // llvm_module_ = llvm::CloneModule(
    //                    *get_rt_module(),
    //                    vmap_,
    //                    [always_clone](const llvm::GlobalValue* gv) {
    //                      auto func = llvm::dyn_cast<llvm::Function>(gv);
    //                      if (!func) {
    //                        return true;
    //                      }
    //                      return (func->getLinkage() ==
    //                                  llvm::GlobalValue::LinkageTypes::PrivateLinkage ||
    //                              func->getLinkage() ==
    //                                  llvm::GlobalValue::LinkageTypes::InternalLinkage
    //                                  ||
    //                              (CodeGenerator::alwaysCloneRuntimeFunction(func)));
    //                    })
    //                    .release();
    // llvm_context_ = llvm_module_->getContext();
  }
  llvm::Module* llvm_module_;
  llvm::LLVMContext* llvm_context_;
  llvm::Function* query_func_;
  llvm::IRBuilder<> ir_builder_;
};

class JITTuple {
 public:
  JITTuple() {}
  JITTuple(llvm::Value* val, llvm::Value* null) : value_(val), null_(null) {}
  llvm::Value* getValue() { return value_; }
  llvm::Value* getNull() { return null_; }

 private:
  llvm::Value* value_;
  llvm::Value* null_;
};

class OpNode {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;
};

class Translator {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;

  Translator() = default;
  virtual ~Translator() = default;

  virtual void consume(Context& context, const JITTuple& input) = 0;

 protected:
  std::shared_ptr<Translator> successor_;
};

#endif
