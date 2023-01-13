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

#include "exec/nextgen/jitlib/llvmjit/LLVMJITValue.h"

#include <llvm/IR/Value.h>

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/base/JITValueOperations.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "util/Logger.h"

namespace cider::jitlib {
JITValue& LLVMJITValue::assign(JITValue& value) {
  if (!is_variable_) {
    // return *this;
    LOG(FATAL) << "JITValue " << getValueName()
               << "is not a variable in LLVMJITValue::assign.";
  }
  store(static_cast<LLVMJITValue&>(value));
  return *this;
}

JITValuePointer LLVMJITValue::andOp(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL: {
      if (auto const_bool = llvm::dyn_cast<llvm::ConstantInt>(llvm_rh.llvm_value_)) {
        return const_bool->isOne() ? JITValuePointer(this) : JITValuePointer(&rh);
      }
      ans = getFunctionBuilder(parent_function_).CreateAnd(load(), llvm_rh.load());
      break;
    }
    default:
      LOG(FATAL) << "Invalid JITValue type for and operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, ans, "and", false);
}

JITValuePointer LLVMJITValue::orOp(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL: {
      if (auto const_bool = llvm::dyn_cast<llvm::ConstantInt>(llvm_rh.llvm_value_)) {
        return const_bool->isOne() ? JITValuePointer(&rh) : JITValuePointer(this);
      }
      ans = getFunctionBuilder(parent_function_).CreateOr(load(), llvm_rh.load());
      break;
    }
    default:
      LOG(FATAL) << "Invalid JITValue type for or operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, ans, "or", false);
}

JITValuePointer LLVMJITValue::notOp() {
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL: {
      if (auto const_bool = llvm::dyn_cast<llvm::ConstantInt>(llvm_value_)) {
        bool literal = const_bool->isOne() ? false : true;
        return parent_function_.createLiteral(JITTypeTag::BOOL, literal);
      }
      ans = getFunctionBuilder(parent_function_).CreateNot(load());
      break;
    }
    default:
      LOG(FATAL) << "Invalid JITValue type for not operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, ans, "not", false);
}

JITValuePointer LLVMJITValue::uminus() {
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateNeg(load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFNeg(load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for uminus operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }
  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "uminus", false);
}

JITValuePointer LLVMJITValue::mod(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "mod");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateSRem(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFRem(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for mod operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "mod", false);
}

JITValuePointer LLVMJITValue::modWithErrorCheck(JITValue& rh) {
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "mod");

  auto ifBuilder = parent_function_.createIfBuilder();
  ifBuilder->condition([&rh]() { return rh == 0; })
      ->ifTrue([this]() {
        getFunctionBuilder(parent_function_)
            .CreateRet(llvm::ConstantInt::get(
                llvm::Type::getInt32Ty(parent_function_.getLLVMContext()),
                ERROR_CODE::ERR_DIV_BY_ZERO));
      })
      ->build();

  return mod(rh);
}

JITValuePointer LLVMJITValue::div(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "div");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateSDiv(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFDiv(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for div operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "div", false);
}

JITValuePointer LLVMJITValue::divWithErrorCheck(JITValue& rh) {
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "div");

  auto ifBuilder = parent_function_.createIfBuilder();
  ifBuilder->condition([&rh]() { return rh == 0; })
      ->ifTrue([this]() {
        getFunctionBuilder(parent_function_)
            .CreateRet(llvm::ConstantInt::get(
                llvm::Type::getInt32Ty(parent_function_.getLLVMContext()),
                ERROR_CODE::ERR_DIV_BY_ZERO));
      })
      ->build();

  return div(rh);
}

JITValuePointer LLVMJITValue::mul(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "mul");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateMul(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFMul(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for mul operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "mul", false);
}

JITValuePointer LLVMJITValue::mulWithErrorCheck(JITValue& rh) {
  // only integer type need overflow check
  auto lhs_llvm_type = getLLVMType(getValueTypeTag(), parent_function_.getLLVMContext());
  if (!lhs_llvm_type->isIntegerTy()) {
    return mul(rh);
  }

  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "mul");

  // get compute function
  auto llvm_module = parent_function_.func_.getParent();
  auto func = llvm::Intrinsic::getDeclaration(
      llvm_module, llvm::Intrinsic::smul_with_overflow, lhs_llvm_type);

  // compute result and overflow
  auto ret_and_overflow =
      getFunctionBuilder(parent_function_)
          .CreateCall(func, std::vector<llvm::Value*>{load(), llvm_rh.load()});
  auto ret = getFunctionBuilder(parent_function_)
                 .CreateExtractValue(ret_and_overflow, std::vector<unsigned>{0});
  auto overflow = getFunctionBuilder(parent_function_)
                      .CreateExtractValue(ret_and_overflow, std::vector<unsigned>{1});
  auto overflow_value = makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, overflow, "overflow", false);

  // return error
  auto ifBuilder = parent_function_.createIfBuilder();
  ifBuilder->condition([&overflow_value]() { return overflow_value; })
      ->ifTrue([this]() {
        getFunctionBuilder(parent_function_)
            .CreateRet(llvm::ConstantInt::get(
                llvm::Type::getInt32Ty(parent_function_.getLLVMContext()),
                ERROR_CODE::ERR_OVERFLOW_OR_UNDERFLOW));
      })
      ->build();

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ret, "mul", false);
}

JITValuePointer LLVMJITValue::sub(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "sub");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateSub(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFSub(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for sub operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "sub", false);
}

JITValuePointer LLVMJITValue::subWithErrorCheck(JITValue& rh) {
  // only integer type need overflow check
  auto lhs_llvm_type = getLLVMType(getValueTypeTag(), parent_function_.getLLVMContext());
  if (!lhs_llvm_type->isIntegerTy()) {
    return sub(rh);
  }

  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "sub");

  // get compute function
  auto llvm_module = parent_function_.func_.getParent();
  auto func = llvm::Intrinsic::getDeclaration(
      llvm_module, llvm::Intrinsic::ssub_with_overflow, lhs_llvm_type);

  // compute result and overflow
  auto ret_and_overflow =
      getFunctionBuilder(parent_function_)
          .CreateCall(func, std::vector<llvm::Value*>{load(), llvm_rh.load()});
  auto ret = getFunctionBuilder(parent_function_)
                 .CreateExtractValue(ret_and_overflow, std::vector<unsigned>{0});
  auto overflow = getFunctionBuilder(parent_function_)
                      .CreateExtractValue(ret_and_overflow, std::vector<unsigned>{1});
  auto overflow_value = makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, overflow, "overflow", false);

  // return error
  auto ifBuilder = parent_function_.createIfBuilder();
  ifBuilder->condition([&overflow_value]() { return overflow_value; })
      ->ifTrue([this]() {
        getFunctionBuilder(parent_function_)
            .CreateRet(llvm::ConstantInt::get(
                llvm::Type::getInt32Ty(parent_function_.getLLVMContext()),
                ERROR_CODE::ERR_OVERFLOW_OR_UNDERFLOW));
      })
      ->build();

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ret, "sub", false);
}

JITValuePointer LLVMJITValue::add(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);

  if (JITTypeTag::POINTER == getValueTypeTag()) {
    auto rh_type = rh.getValueTypeTag();
    if (rh_type != JITTypeTag::INT8 && rh_type != JITTypeTag::INT16 &&
        rh_type != JITTypeTag::INT32 && rh_type != JITTypeTag::INT64) {
      LOG(FATAL) << "Invalid index type for pointer offset operation. Name="
                 << rh.getValueName() << ", Type=" << getJITTypeName(rh_type);
    }
  } else {
    checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "add");
  }

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateAdd(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFAdd(load(), llvm_rh.load());
      break;
    case JITTypeTag::POINTER:
      ans = getFunctionBuilder(parent_function_).CreateGEP(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for add operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(),
      parent_function_,
      ans,
      JITTypeTag::POINTER == getValueTypeTag() ? "add_ptr" : "add",
      false,
      getValueSubTypeTag());
}

JITValuePointer LLVMJITValue::addWithErrorCheck(JITValue& rh) {
  // only integer type need overflow check
  auto lhs_llvm_type = getLLVMType(getValueTypeTag(), parent_function_.getLLVMContext());
  if (!lhs_llvm_type->isIntegerTy()) {
    return add(rh);
  }

  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  if (JITTypeTag::POINTER == getValueTypeTag()) {
    auto rh_type = rh.getValueTypeTag();
    if (rh_type != JITTypeTag::INT8 && rh_type != JITTypeTag::INT16 &&
        rh_type != JITTypeTag::INT32 && rh_type != JITTypeTag::INT64) {
      LOG(FATAL) << "Invalid index type for pointer offset operation. Name="
                 << rh.getValueName() << ", Type=" << getJITTypeName(rh_type);
    }
  } else {
    checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "add");
  }

  // get compute function
  auto llvm_module = parent_function_.func_.getParent();
  auto func = llvm::Intrinsic::getDeclaration(
      llvm_module, llvm::Intrinsic::sadd_with_overflow, lhs_llvm_type);

  // compute result and overflow
  auto ret_and_overflow =
      getFunctionBuilder(parent_function_)
          .CreateCall(func, std::vector<llvm::Value*>{load(), llvm_rh.load()});
  auto ret = getFunctionBuilder(parent_function_)
                 .CreateExtractValue(ret_and_overflow, std::vector<unsigned>{0});
  auto overflow = getFunctionBuilder(parent_function_)
                      .CreateExtractValue(ret_and_overflow, std::vector<unsigned>{1});
  auto overflow_value = makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, overflow, "overflow", false);

  // return error
  auto ifBuilder = parent_function_.createIfBuilder();
  ifBuilder->condition([&overflow_value]() { return overflow_value; })
      ->ifTrue([this]() {
        getFunctionBuilder(parent_function_)
            .CreateRet(llvm::ConstantInt::get(
                llvm::Type::getInt32Ty(parent_function_.getLLVMContext()),
                ERROR_CODE::ERR_OVERFLOW_OR_UNDERFLOW));
      })
      ->build();

  return makeJITValuePointer<LLVMJITValue>(
      getValueTypeTag(),
      parent_function_,
      ret,
      JITTypeTag::POINTER == getValueTypeTag() ? "add_ptr" : "add",
      false,
      getValueSubTypeTag());
}

JITValuePointer LLVMJITValue::createCmpInstruction(llvm::CmpInst::Predicate ICmpType,
                                                   llvm::CmpInst::Predicate FCmpType,
                                                   JITValue& rh,
                                                   const char* value) {
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), value);
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL:
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_)
                .CreateICmp(ICmpType, load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_)
                .CreateFCmp(FCmpType, load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for compare operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::BOOL, parent_function_, ans, value, false);
}

JITValuePointer LLVMJITValue::eq(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_EQ, llvm::CmpInst::FCMP_OEQ, rh, "eq");
}

JITValuePointer LLVMJITValue::ne(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_NE, llvm::CmpInst::FCMP_ONE, rh, "ne");
}

JITValuePointer LLVMJITValue::lt(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SLT, llvm::CmpInst::FCMP_OLT, rh, "lt");
}

JITValuePointer LLVMJITValue::le(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SLE, llvm::CmpInst::FCMP_OLE, rh, "le");
}

JITValuePointer LLVMJITValue::gt(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SGT, llvm::CmpInst::FCMP_OGT, rh, "gt");
}

JITValuePointer LLVMJITValue::ge(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SGE, llvm::CmpInst::FCMP_OGE, rh, "ge");
}

void LLVMJITValue::checkOprandsType(JITTypeTag lh, JITTypeTag rh, const char* op) {
  if (lh != rh) {
    LOG(FATAL) << "Oprands type doesn't match in LLVMJITValue operator " << op
               << " lh=" << getJITTypeName(lh) << ", rh=" << getJITTypeName(rh) << ".";
  }
}

llvm::Value* LLVMJITValue::load() {
  if (is_variable_) {
    return getFunctionBuilder(parent_function_).CreateLoad(llvm_value_, false);
  } else {
    return llvm_value_;
  }
}

llvm::Value* LLVMJITValue::store(LLVMJITValue& rh) {
  if (is_variable_) {
    return getFunctionBuilder(parent_function_)
        .CreateStore(rh.load(), llvm_value_, false);
  }
  return nullptr;
}

JITValuePointer LLVMJITValue::castPointerSubType(JITTypeTag type_tag) {
  if (JITTypeTag::POINTER != getValueTypeTag()) {
    LOG(FATAL) << "Invalid operation to cast subtype of a non-POINTER JITValue. Type="
               << getJITTypeName(getValueTypeTag());
  }
  llvm::Value* new_llvm_value =
      getFunctionBuilder(parent_function_)
          .CreateBitCast(load(),
                         getLLVMPtrType(type_tag, parent_function_.getLLVMContext()));
  return makeJITValuePointer<LLVMJITValue>(
      JITTypeTag::POINTER, parent_function_, new_llvm_value, "cast_ptr", false, type_tag);
}

JITValuePointer LLVMJITValue::castJITValuePrimitiveType(JITTypeTag target_jit_tag) {
  llvm::Type* source_type =
      getLLVMType(getValueTypeTag(), parent_function_.getLLVMContext());
  llvm::Type* target_type =
      getLLVMType(target_jit_tag, parent_function_.getLLVMContext());
  CHECK(source_type && target_type);
  llvm::Value* source_lv = load();
  llvm::Value* target_lv = nullptr;
  if (source_type->isIntegerTy() && target_type->isIntegerTy()) {
    target_lv =
        getFunctionBuilder(parent_function_).CreateIntCast(source_lv, target_type, true);
  } else if (source_type->isIntegerTy() && target_type->isFloatingPointTy()) {
    target_lv = getFunctionBuilder(parent_function_).CreateSIToFP(source_lv, target_type);
  } else if (source_type->isFloatingPointTy() && target_type->isIntegerTy()) {
    target_lv = getFunctionBuilder(parent_function_).CreateFPToSI(source_lv, target_type);
  } else if (source_type->isFloatingPointTy() && target_type->isFloatingPointTy()) {
    target_lv = getFunctionBuilder(parent_function_).CreateFPCast(source_lv, target_type);
  }
  CHECK(target_lv) << "error when cast JITValue type from:"
                   << getJITTypeName(getValueTypeTag())
                   << " into type:" << getJITTypeName(target_jit_tag);
  return makeJITValuePointer<LLVMJITValue>(target_jit_tag,
                                           parent_function_,
                                           target_lv,
                                           "cast_val",
                                           false,
                                           JITTypeTag::INVALID);
}

JITValuePointer LLVMJITValue::dereference() {
  if (JITTypeTag::POINTER != getValueTypeTag()) {
    LOG(FATAL) << "Invalid operation to dereference of a non-POINTER JITValue. Type="
               << getJITTypeName(getValueTypeTag());
  }

  return makeJITValuePointer<LLVMJITValue>(getValueSubTypeTag(),
                                           parent_function_,
                                           llvm_value_,
                                           "dereference_ptr",
                                           true,
                                           JITTypeTag::INVALID);
}

JITValuePointer LLVMJITValue::getElemAt(JITValue& index) {
  if (JITTypeTag::POINTER != getValueTypeTag()) {
    LOG(FATAL) << "Invalid operation to get elements from a non-POINTER JITValue. Type="
               << getJITTypeName(getValueTypeTag());
  }
  auto element_ptr = add(index);
  auto& llvm_element_ptr = static_cast<LLVMJITValue&>(*element_ptr);

  return llvm_element_ptr.dereference();
}

void LLVMJITValue::setName(const std::string& name) {
  value_name_ = name;
  llvm_value_->setName(name);
}

};  // namespace cider::jitlib
