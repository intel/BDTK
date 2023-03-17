/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#include "exec/nextgen/jitlib/llvmjit/LLVMJITControlFlow.h"

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITValue.h"
#include "util/Logger.h"

namespace cider::jitlib {

static llvm::Value* castToBool(llvm::IRBuilder<>& builder, llvm::Value* value) {
  if (value->getType()->isIntegerTy() && value->getType()->getIntegerBitWidth() != 1) {
    return builder.CreateICmpNE(value, llvm::ConstantInt::get(value->getType(), 0));
  } else {
    return value;
  }
}

void LLVMIfBuilder::build() {
  CHECK(condition_);
  auto condition_bb =
      llvm::BasicBlock::Create(func_.getContext(), ".If_Condition", &func_);
  builder_.CreateBr(condition_bb);
  builder_.SetInsertPoint(condition_bb);

  JITValuePointer condition_value = condition_();
  LLVMJITValue& condition_llvm_value = static_cast<LLVMJITValue&>(*condition_value);

  auto true_bb = llvm::BasicBlock::Create(func_.getContext(), ".If_True", &func_);
  auto false_bb = llvm::BasicBlock::Create(func_.getContext(), ".If_False", &func_);
  auto after_bb = llvm::BasicBlock::Create(func_.getContext(), ".After_If", &func_);

  builder_.CreateCondBr(
      castToBool(builder_, condition_llvm_value.load()), true_bb, false_bb);

  builder_.SetInsertPoint(true_bb);
  if (if_true_) {
    if_true_();
    if (builder_.GetInsertBlock()->empty() ||
        !builder_.GetInsertBlock()->back().isTerminator()) {
      builder_.CreateBr(after_bb);
    }
  } else {
    builder_.CreateBr(after_bb);
  }

  builder_.SetInsertPoint(false_bb);
  if (if_false_) {
    if_false_();
    if (builder_.GetInsertBlock()->empty() ||
        !builder_.GetInsertBlock()->back().isTerminator()) {
      builder_.CreateBr(after_bb);
    }
  } else {
    builder_.CreateBr(after_bb);
  }

  if (after_bb->hasNPredecessorsOrMore(1)) {
    builder_.SetInsertPoint(after_bb);
  } else {
    after_bb->removeFromParent();
    delete after_bb;
  }
}

void LLVMLoopBuilder::build() {
  CHECK(condition_);
  auto condition_bb =
      llvm::BasicBlock::Create(func_.getContext(), ".For_Condition", &func_);
  builder_.CreateBr(condition_bb);
  builder_.SetInsertPoint(condition_bb);

  JITValuePointer condition_value = condition_();
  LLVMJITValue& condition_llvm_value = static_cast<LLVMJITValue&>(*condition_value);

  auto for_body = llvm::BasicBlock::Create(func_.getContext(), ".Loop_Body", &func_);
  body_block_ = for_body;
  auto for_update = llvm::BasicBlock::Create(func_.getContext(), ".Loop_Update", &func_);
  update_block_ = for_update;
  auto after_for = llvm::BasicBlock::Create(func_.getContext(), ".After_Loop", &func_);
  builder_.CreateCondBr(
      castToBool(builder_, condition_llvm_value.load()), for_body, after_for);

  builder_.SetInsertPoint(for_body);
  if (loop_body_) {
    loop_body_(this);
  }
  builder_.CreateBr(for_update);

  builder_.SetInsertPoint(for_update);
  if (update_) {
    update_();
  }
  auto loop_br = builder_.CreateBr(condition_bb);

  builder_.SetInsertPoint(after_for);

  if (scope_noalias_) {
    llvm::MDNode* access_group = llvm::MDNode::getDistinct(func_.getContext(), {});
    for (llvm::BasicBlock* block : {condition_bb, for_body, for_update}) {
      for (auto& inst : block->getInstList()) {
        if (inst.mayReadOrWriteMemory()) {
          inst.setMetadata(llvm::LLVMContext::MD_access_group, access_group);
        }
      }
    }

    llvm::MDNode* parallel_access = llvm::MDNode::get(
        func_.getContext(),
        {llvm::MDString::get(func_.getContext(), "llvm.loop.parallel_accesses"),
         access_group});
    llvm::MDNode* loop_vectorize = llvm::MDNode::get(
        func_.getContext(),
        {llvm::MDString::get(func_.getContext(), "llvm.loop.vectorize.enable"),
         llvm::ConstantAsMetadata::get(
             llvm::ConstantInt::get(llvm::Type::getInt1Ty(func_.getContext()), 1))});
    llvm::MDNode* loop =
        llvm::MDNode::get(func_.getContext(), {nullptr, parallel_access, loop_vectorize});
    loop->replaceOperandWith(0, loop);
    loop_br->setMetadata(llvm::LLVMContext::MD_loop, loop);
  }
}

void LLVMLoopBuilder::loopContinueImpl(JITValue* condition) {
  CHECK(update_block_);
  if (condition) {
    auto for_body =
        llvm::BasicBlock::Create(func_.getContext(), ".Loop_Body_Continue", &func_);
    LLVMJITValue* llvm_condition = static_cast<LLVMJITValue*>(condition);

    builder_.CreateCondBr(llvm_condition->load(), update_block_, for_body);
    builder_.SetInsertPoint(for_body);
  } else {
    builder_.CreateBr(update_block_);
    builder_.SetInsertPoint(body_block_);
  }
}

};  // namespace cider::jitlib
