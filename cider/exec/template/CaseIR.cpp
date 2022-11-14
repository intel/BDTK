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

#include "CodeGenerator.h"
#include "Execute.h"

std::vector<llvm::Value*> CodeGenerator::codegen(const Analyzer::CaseExpr* case_expr,
                                                 const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto case_ti = case_expr->get_type_info();
  llvm::Type* case_llvm_type = nullptr;
  bool is_real_str = false;
  if (case_ti.is_integer() || case_ti.is_time() || case_ti.is_decimal()) {
    case_llvm_type = get_int_type(get_bit_width(case_ti), cgen_state_->context_);
  } else if (case_ti.is_fp()) {
    case_llvm_type = case_ti.get_type() == kFLOAT
                         ? llvm::Type::getFloatTy(cgen_state_->context_)
                         : llvm::Type::getDoubleTy(cgen_state_->context_);
  } else if (case_ti.is_string()) {
    if (case_ti.get_compression() == kENCODING_DICT) {
      case_llvm_type =
          get_int_type(8 * case_ti.get_logical_size(), cgen_state_->context_);
    } else {
      is_real_str = true;
      case_llvm_type = get_int_type(64, cgen_state_->context_);
    }
  } else if (case_ti.is_boolean()) {
    case_llvm_type = get_int_type(8 * case_ti.get_logical_size(), cgen_state_->context_);
  }
  CHECK(case_llvm_type);
  const auto& else_ti = case_expr->get_else_expr()->get_type_info();
  CHECK_EQ(else_ti.get_type(), case_ti.get_type());
  llvm::Value* case_val = codegenCase(case_expr, case_llvm_type, is_real_str, co);
  std::vector<llvm::Value*> ret_vals{case_val};
  if (is_real_str) {
    ret_vals.push_back(cgen_state_->emitCall("extract_str_ptr", {case_val}));
    ret_vals.push_back(cgen_state_->emitCall("extract_str_len", {case_val}));
  }
  return ret_vals;
}

llvm::Value* CodeGenerator::codegenCase(const Analyzer::CaseExpr* case_expr,
                                        llvm::Type* case_llvm_type,
                                        const bool is_real_str,
                                        const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  // Here the linear control flow will diverge and expressions cached during the
  // code branch code generation (currently just column decoding) are not going
  // to be available once we're done generating the case. Take a snapshot of
  // the cache with FetchCacheAnchor and restore it once we're done with CASE.
  Executor::FetchCacheAnchor anchor(cgen_state_);
  const auto& expr_pair_list = case_expr->get_expr_pair_list();
  std::vector<llvm::Value*> then_lvs;
  std::vector<llvm::BasicBlock*> then_bbs;
  const auto end_bb = llvm::BasicBlock::Create(
      cgen_state_->context_, "end_case", cgen_state_->current_func_);
  for (const auto& expr_pair : expr_pair_list) {
    Executor::FetchCacheAnchor branch_anchor(cgen_state_);
    const auto when_lv = toBool(codegen(expr_pair.first.get(), true, co).front());
    const auto cmp_bb = cgen_state_->ir_builder_.GetInsertBlock();
    const auto then_bb = llvm::BasicBlock::Create(cgen_state_->context_,
                                                  "then_case",
                                                  cgen_state_->current_func_,
                                                  /*insert_before=*/end_bb);
    cgen_state_->ir_builder_.SetInsertPoint(then_bb);
    auto then_bb_lvs = codegen(expr_pair.second.get(), true, co);
    if (is_real_str) {
      if (then_bb_lvs.size() == 3) {
        then_lvs.push_back(
            cgen_state_->emitCall("string_pack", {then_bb_lvs[1], then_bb_lvs[2]}));
      } else {
        then_lvs.push_back(then_bb_lvs.front());
      }
    } else {
      CHECK_EQ(size_t(1), then_bb_lvs.size());
      then_lvs.push_back(then_bb_lvs.front());
    }
    then_bbs.push_back(cgen_state_->ir_builder_.GetInsertBlock());
    cgen_state_->ir_builder_.CreateBr(end_bb);
    const auto when_bb = llvm::BasicBlock::Create(
        cgen_state_->context_, "when_case", cgen_state_->current_func_);
    cgen_state_->ir_builder_.SetInsertPoint(cmp_bb);
    cgen_state_->ir_builder_.CreateCondBr(when_lv, then_bb, when_bb);
    cgen_state_->ir_builder_.SetInsertPoint(when_bb);
  }
  const auto else_expr = case_expr->get_else_expr();
  CHECK(else_expr);
  auto else_lvs = codegen(else_expr, true, co);
  llvm::Value* else_lv{nullptr};
  if (else_lvs.size() == 3) {
    else_lv = cgen_state_->emitCall("string_pack", {else_lvs[1], else_lvs[2]});
  } else {
    else_lv = else_lvs.front();
  }
  CHECK(else_lv);
  auto else_bb = cgen_state_->ir_builder_.GetInsertBlock();
  cgen_state_->ir_builder_.CreateBr(end_bb);
  cgen_state_->ir_builder_.SetInsertPoint(end_bb);
  auto then_phi =
      cgen_state_->ir_builder_.CreatePHI(case_llvm_type, expr_pair_list.size() + 1);
  CHECK_EQ(then_bbs.size(), then_lvs.size());
  for (size_t i = 0; i < then_bbs.size(); ++i) {
    then_phi->addIncoming(then_lvs[i], then_bbs[i]);
  }
  then_phi->addIncoming(else_lv, else_bb);
  return then_phi;
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenCaseExpr(
    const Analyzer::CaseExpr* case_expr,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto case_ti = case_expr->get_type_info();
  llvm::Type* case_llvm_type = nullptr;
  bool is_real_str = false;
  if (case_ti.is_integer() || case_ti.is_time() || case_ti.is_decimal()) {
    case_llvm_type = get_int_type(get_bit_width(case_ti), cgen_state_->context_);
  } else if (case_ti.is_fp()) {
    case_llvm_type = case_ti.get_type() == kFLOAT
                         ? llvm::Type::getFloatTy(cgen_state_->context_)
                         : llvm::Type::getDoubleTy(cgen_state_->context_);
  } else if (case_ti.is_string()) {
    if (case_ti.get_compression() == kENCODING_DICT) {
      case_llvm_type =
          get_int_type(8 * case_ti.get_logical_size(), cgen_state_->context_);
    } else {
      is_real_str = true;
      case_llvm_type = get_int_type(64, cgen_state_->context_);
    }
  } else if (case_ti.is_boolean()) {
    case_llvm_type = get_int_type(8 * case_ti.get_logical_size(), cgen_state_->context_);
  } else {
    CIDER_THROW(CiderUnsupportedException,
                fmt::format("case type is {}, not support", case_ti.get_type()));
  }
  CHECK(case_llvm_type);
  const auto& else_ti = case_expr->get_else_expr()->get_type_info();
  CHECK_EQ(else_ti.get_type(), case_ti.get_type());
  std::unique_ptr<CodegenColValues> case_val =
      codegenCaseExpr(case_expr, case_llvm_type, is_real_str, co);
  if (is_real_str) {
    // FIXME(haiwei): [POAE7-2457] ,blocking by [POAE7-2415]
    /*CIDER_THROW(CiderCompileException,
                "String type case when is not currently supported.");*/
    auto case_expr = dynamic_cast<FixedSizeColValues*>(case_val.get());
    std::vector<llvm::Value*> ret_vals{case_expr->getValue()};
    ret_vals.push_back(cgen_state_->emitCall("extract_str_ptr", {case_expr->getValue()}));
    ret_vals.push_back(cgen_state_->emitCall("extract_str_len", {case_expr->getValue()}));
    return std::make_unique<MultipleValueColValues>(ret_vals, case_expr->getNull());
  } else {
    return case_val;
  }
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenCaseExpr(
    const Analyzer::CaseExpr* case_expr,
    llvm::Type* case_llvm_type,
    const bool is_real_str,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  // Here the linear control flow will diverge and expressions cached during the
  // code branch code generation (currently just column decoding) are not going
  // to be available once we're done generating the case. Take a snapshot of
  // the cache with FetchCacheAnchor and restore it once we're done with CASE.
  Executor::FetchCacheAnchor anchor(cgen_state_);
  const auto& expr_pair_list = case_expr->get_expr_pair_list();
  std::vector<llvm::Value*> then_lvs;
  std::vector<llvm::Value*> then_null_lvs;
  std::vector<llvm::BasicBlock*> then_bbs;
  llvm::Value* when_null_lv = nullptr;
  const auto end_bb = llvm::BasicBlock::Create(
      cgen_state_->context_, "end_case", cgen_state_->current_func_);
  for (const auto& expr_pair : expr_pair_list) {
    Executor::FetchCacheAnchor branch_anchor(cgen_state_);
    const auto if_expr_ptr = codegen(expr_pair.first.get(), co, true);
    auto if_expr = dynamic_cast<FixedSizeColValues*>(if_expr_ptr.get());
    CHECK(if_expr);
    const auto when_lv = toBool(if_expr->getValue());
    when_null_lv = if_expr->getNull();
    CHECK(when_lv);
    llvm::Value* final_when_lv = nullptr;
    if (when_null_lv) {
      llvm::Value* filter_lv = cgen_state_->llBool(true);
      filter_lv = cgen_state_->ir_builder_.CreateAnd(
          filter_lv, cgen_state_->ir_builder_.CreateNot(when_null_lv));
      final_when_lv = cgen_state_->ir_builder_.CreateAnd(filter_lv, when_lv);
    } else {
      final_when_lv = when_lv;
    }
    const auto cmp_bb = cgen_state_->ir_builder_.GetInsertBlock();
    const auto then_bb = llvm::BasicBlock::Create(cgen_state_->context_,
                                                  "then_case",
                                                  cgen_state_->current_func_,
                                                  /*insert_before=*/end_bb);
    cgen_state_->ir_builder_.SetInsertPoint(then_bb);
    const auto then_expr_ptr = codegen(expr_pair.second.get(), co, true);
    if (is_real_str) {
      // FIXME(haiwei): [POAE7-2457] ,blocking by [POAE7-2415]
      /*CIDER_THROW(CiderCompileException,
                  "String type case when is not currently supported.");*/
      auto then_multiple_expr =
          dynamic_cast<MultipleValueColValues*>(then_expr_ptr.get());
      CHECK(then_multiple_expr);
      auto then_bb_lvs = then_multiple_expr->getValues();
      auto then_bb_null_lv = then_multiple_expr->getNull();
      if (then_bb_lvs.size() == 3) {
        then_lvs.push_back(
            cgen_state_->emitCall("string_pack", {then_bb_lvs[1], then_bb_lvs[2]}));
      } else {
        then_lvs.push_back(then_bb_lvs.front());
      }
      if (then_bb_null_lv) {
        then_null_lvs.push_back(then_bb_null_lv);
      }
    } else {
      auto then_fixed_expr = dynamic_cast<FixedSizeColValues*>(then_expr_ptr.get());
      CHECK(then_fixed_expr);
      auto then_bb_lv = then_fixed_expr->getValue();
      auto then_bb_null_lv = then_fixed_expr->getNull();
      CHECK(then_bb_lv);
      then_lvs.push_back(then_bb_lv);
      if (then_bb_null_lv) {
        then_null_lvs.push_back(then_bb_null_lv);
      }
    }
    then_bbs.push_back(cgen_state_->ir_builder_.GetInsertBlock());
    cgen_state_->ir_builder_.CreateBr(end_bb);
    const auto when_bb = llvm::BasicBlock::Create(
        cgen_state_->context_, "when_case", cgen_state_->current_func_);
    cgen_state_->ir_builder_.SetInsertPoint(cmp_bb);
    cgen_state_->ir_builder_.CreateCondBr(final_when_lv, then_bb, when_bb);
    cgen_state_->ir_builder_.SetInsertPoint(when_bb);
  }
  const auto else_expr = case_expr->get_else_expr();
  CHECK(else_expr);
  const auto else_expr_ptr = codegen(else_expr, co, true);
  llvm::Value* else_lv{nullptr};
  llvm::Value* else_null_lv{nullptr};
  if (is_real_str) {
    // FIXME(haiwei): [POAE7-2457] ,blocking by [POAE7-2415]
    /*CIDER_THROW(CiderCompileException,
                "String type case when is not currently supported.");*/
    auto else_multiple_expr = dynamic_cast<MultipleValueColValues*>(else_expr_ptr.get());
    CHECK(else_multiple_expr);
    auto else_lvs = else_multiple_expr->getValues();
    if (else_lvs.size() == 3) {
      else_lv = cgen_state_->emitCall("string_pack", {else_lvs[1], else_lvs[2]});
    } else {
      else_lv = else_lvs.front();
    }
    else_null_lv = else_multiple_expr->getNull();
  } else {
    auto else_expr_v = dynamic_cast<FixedSizeColValues*>(else_expr_ptr.get());
    CHECK(else_expr_v);
    else_lv = else_expr_v->getValue();
    else_null_lv = else_expr_v->getNull();
  }
  CHECK(else_lv);
  auto else_bb = cgen_state_->ir_builder_.GetInsertBlock();
  cgen_state_->ir_builder_.CreateBr(end_bb);
  cgen_state_->ir_builder_.SetInsertPoint(end_bb);
  auto then_phi =
      cgen_state_->ir_builder_.CreatePHI(case_llvm_type, expr_pair_list.size() + 1);
  CHECK_EQ(then_bbs.size(), then_lvs.size());
  for (size_t i = 0; i < then_bbs.size(); ++i) {
    then_phi->addIncoming(then_lvs[i], then_bbs[i]);
  }
  then_phi->addIncoming(else_lv, else_bb);
  llvm::PHINode* then_null_phi = nullptr;
  if (then_null_lvs.size() > 0 && else_null_lv) {
    then_null_phi = cgen_state_->ir_builder_.CreatePHI(
        llvm::Type::getInt1Ty(cgen_state_->context_), expr_pair_list.size() + 1);
    CHECK_EQ(then_bbs.size(), then_null_lvs.size());
    for (size_t i = 0; i < then_bbs.size(); ++i) {
      then_null_phi->addIncoming(then_null_lvs[i], then_bbs[i]);
    }
    then_null_phi->addIncoming(else_null_lv, else_bb);
  }
  if (then_null_phi) {
    return std::make_unique<FixedSizeColValues>(then_phi, then_null_phi);
  } else {
    return std::make_unique<FixedSizeColValues>(then_phi, when_null_lv);
  }
}
