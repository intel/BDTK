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

#include <typeinfo>

#include "exec/plan/parser/ParserNode.h"

extern bool g_enable_watchdog;
extern bool g_enable_overlaps_hashjoin;

namespace {

llvm::CmpInst::Predicate llvm_icmp_pred(const SQLOps op_type) {
  switch (op_type) {
    case kEQ:
      return llvm::ICmpInst::ICMP_EQ;
    case kNE:
      return llvm::ICmpInst::ICMP_NE;
    case kLT:
      return llvm::ICmpInst::ICMP_SLT;
    case kGT:
      return llvm::ICmpInst::ICMP_SGT;
    case kLE:
      return llvm::ICmpInst::ICMP_SLE;
    case kGE:
      return llvm::ICmpInst::ICMP_SGE;
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("op_type is {}", op_type));
  }
}

std::string icmp_name(const SQLOps op_type) {
  switch (op_type) {
    case kEQ:
      return "eq";
    case kNE:
      return "ne";
    case kLT:
      return "lt";
    case kGT:
      return "gt";
    case kLE:
      return "le";
    case kGE:
      return "ge";
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("op_type is {}", op_type));
  }
}

std::string icmp_arr_name(const SQLOps op_type) {
  switch (op_type) {
    case kEQ:
      return "eq";
    case kNE:
      return "ne";
    case kLT:
      return "gt";
    case kGT:
      return "lt";
    case kLE:
      return "ge";
    case kGE:
      return "le";
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("op_type is {}", op_type));
  }
}

llvm::CmpInst::Predicate llvm_fcmp_pred(const SQLOps op_type) {
  switch (op_type) {
    case kEQ:
      return llvm::CmpInst::FCMP_OEQ;
    case kNE:
      return llvm::CmpInst::FCMP_ONE;
    case kLT:
      return llvm::CmpInst::FCMP_OLT;
    case kGT:
      return llvm::CmpInst::FCMP_OGT;
    case kLE:
      return llvm::CmpInst::FCMP_OLE;
    case kGE:
      return llvm::CmpInst::FCMP_OGE;
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("op_type is {}", op_type));
  }
}

}  // namespace

namespace {

std::string string_cmp_func(const SQLOps optype) {
  switch (optype) {
    case kLT:
      return "string_lt";
    case kLE:
      return "string_le";
    case kGT:
      return "string_gt";
    case kGE:
      return "string_ge";
    case kEQ:
      return "string_eq";
    case kNE:
      return "string_ne";
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("op_type is {}", optype));
  }
}

std::shared_ptr<Analyzer::BinOper> lower_bw_eq(const Analyzer::BinOper* bw_eq) {
  const auto eq_oper =
      std::make_shared<Analyzer::BinOper>(bw_eq->get_type_info(),
                                          bw_eq->get_contains_agg(),
                                          kEQ,
                                          bw_eq->get_qualifier(),
                                          bw_eq->get_own_left_operand(),
                                          bw_eq->get_own_right_operand());
  const auto lhs_is_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kISNULL, bw_eq->get_own_left_operand());
  const auto rhs_is_null = std::make_shared<Analyzer::UOper>(
      kBOOLEAN, kISNULL, bw_eq->get_own_right_operand());
  const auto both_are_null =
      Parser::OperExpr::normalize(kAND, kONE, lhs_is_null, rhs_is_null);
  const auto bw_eq_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(
      Parser::OperExpr::normalize(kOR, kONE, eq_oper, both_are_null));
  CHECK(bw_eq_oper);
  return bw_eq_oper;
}

std::shared_ptr<Analyzer::BinOper> lower_bw_ne(const Analyzer::BinOper* bw_ne) {
  const auto ne_oper =
      std::make_shared<Analyzer::BinOper>(bw_ne->get_type_info(),
                                          bw_ne->get_contains_agg(),
                                          kNE,
                                          bw_ne->get_qualifier(),
                                          bw_ne->get_own_left_operand(),
                                          bw_ne->get_own_right_operand());
  const auto lhs_is_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kISNULL, bw_ne->get_own_left_operand());
  const auto rhs_is_null = std::make_shared<Analyzer::UOper>(
      kBOOLEAN, kISNULL, bw_ne->get_own_right_operand());
  const auto lhs_is_not_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kNOT, lhs_is_null);
  const auto rhs_is_not_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kNOT, rhs_is_null);
  const auto only_one_null =
      std::dynamic_pointer_cast<Analyzer::BinOper>(Parser::OperExpr::normalize(
          kOR,
          kONE,
          Parser::OperExpr::normalize(kAND, kONE, lhs_is_null, rhs_is_not_null),
          Parser::OperExpr::normalize(kAND, kONE, rhs_is_null, lhs_is_not_null)));

  const auto bw_ne_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(
      Parser::OperExpr::normalize(kOR, kONE, ne_oper, only_one_null));
  return bw_ne_oper;
}

std::shared_ptr<Analyzer::BinOper> make_eq(const std::shared_ptr<Analyzer::Expr>& lhs,
                                           const std::shared_ptr<Analyzer::Expr>& rhs,
                                           const SQLOps optype) {
  CHECK(IS_EQUIVALENCE(optype));
  // Sides of a tuple equality are stripped of cast operators to simplify the logic
  // in the hash table construction algorithm. Add them back here.
  auto eq_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(
      Parser::OperExpr::normalize(optype, kONE, lhs, rhs));
  CHECK(eq_oper);
  return optype == kBW_EQ ? lower_bw_eq(eq_oper.get()) : eq_oper;
}

// Convert a column tuple equality expression back to a conjunction of comparisons
// so that it can be handled by the regular code generation methods.
std::shared_ptr<Analyzer::BinOper> lower_multicol_compare(
    const Analyzer::BinOper* multicol_compare) {
  const auto left_tuple_expr = dynamic_cast<const Analyzer::ExpressionTuple*>(
      multicol_compare->get_left_operand());
  const auto right_tuple_expr = dynamic_cast<const Analyzer::ExpressionTuple*>(
      multicol_compare->get_right_operand());
  CHECK(left_tuple_expr && right_tuple_expr);
  const auto& left_tuple = left_tuple_expr->getTuple();
  const auto& right_tuple = right_tuple_expr->getTuple();
  CHECK_EQ(left_tuple.size(), right_tuple.size());
  CHECK_GT(left_tuple.size(), size_t(1));
  auto acc =
      make_eq(left_tuple.front(), right_tuple.front(), multicol_compare->get_optype());
  for (size_t i = 1; i < left_tuple.size(); ++i) {
    auto crt = make_eq(left_tuple[i], right_tuple[i], multicol_compare->get_optype());
    const bool not_null =
        acc->get_type_info().get_notnull() && crt->get_type_info().get_notnull();
    acc = makeExpr<Analyzer::BinOper>(
        SQLTypeInfo(kBOOLEAN, not_null), false, kAND, kONE, acc, crt);
  }
  return acc;
}

void check_array_comp_cond(const Analyzer::BinOper* bin_oper) {
  auto lhs_cv = dynamic_cast<const Analyzer::ColumnVar*>(bin_oper->get_left_operand());
  auto rhs_cv = dynamic_cast<const Analyzer::ColumnVar*>(bin_oper->get_right_operand());
  auto comp_op = IS_COMPARISON(bin_oper->get_optype());
  if (lhs_cv && rhs_cv && comp_op) {
    auto lhs_ti = lhs_cv->get_type_info();
    auto rhs_ti = rhs_cv->get_type_info();
    if (lhs_ti.is_array() && rhs_ti.is_array()) {
      CIDER_THROW(
          CiderCompileException,
          "Comparing two full array columns is not supported yet. Please consider "
          "rewriting the full array comparison to a comparison between indexed array "
          "columns (i.e., arr1[1] {<, <=, >, >=} arr2[1]).");
    }
  }
  auto lhs_bin_oper =
      dynamic_cast<const Analyzer::BinOper*>(bin_oper->get_left_operand());
  auto rhs_bin_oper =
      dynamic_cast<const Analyzer::BinOper*>(bin_oper->get_right_operand());
  // we can do (non-)equivalence check of two encoded string
  // even if they are (indexed) array cols
  auto theta_comp = IS_COMPARISON(bin_oper->get_optype()) &&
                    !IS_EQUIVALENCE(bin_oper->get_optype()) &&
                    bin_oper->get_optype() != SQLOps::kNE;
  if (lhs_bin_oper && rhs_bin_oper && theta_comp &&
      lhs_bin_oper->get_optype() == SQLOps::kARRAY_AT &&
      rhs_bin_oper->get_optype() == SQLOps::kARRAY_AT) {
    auto lhs_arr_cv =
        dynamic_cast<const Analyzer::ColumnVar*>(lhs_bin_oper->get_left_operand());
    auto lhs_arr_idx =
        dynamic_cast<const Analyzer::Constant*>(lhs_bin_oper->get_right_operand());
    auto rhs_arr_cv =
        dynamic_cast<const Analyzer::ColumnVar*>(rhs_bin_oper->get_left_operand());
    auto rhs_arr_idx =
        dynamic_cast<const Analyzer::Constant*>(rhs_bin_oper->get_right_operand());
    if (lhs_arr_cv && rhs_arr_cv && lhs_arr_idx && rhs_arr_idx &&
        ((lhs_arr_cv->get_type_info().is_array() &&
          lhs_arr_cv->get_type_info().get_subtype() == SQLTypes::kTEXT) ||
         (rhs_arr_cv->get_type_info().is_string() &&
          rhs_arr_cv->get_type_info().get_subtype() == SQLTypes::kTEXT))) {
      CIDER_THROW(CiderCompileException,
                  "Comparison between string array columns is not supported yet.");
    }
  }
}

}  // namespace

llvm::Value* CodeGenerator::codegenCmp(const Analyzer::BinOper* bin_oper,
                                       const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto qualifier = bin_oper->get_qualifier();
  const auto lhs = bin_oper->get_left_operand();
  const auto rhs = bin_oper->get_right_operand();
  if (dynamic_cast<const Analyzer::ExpressionTuple*>(lhs)) {
    CHECK(dynamic_cast<const Analyzer::ExpressionTuple*>(rhs));
    const auto lowered = lower_multicol_compare(bin_oper);
    const auto lowered_lvs = codegen(lowered.get(), true, co);
    CHECK_EQ(size_t(1), lowered_lvs.size());
    return lowered_lvs.front();
  }
  const auto optype = bin_oper->get_optype();
  if (optype == kBW_EQ or optype == kBW_NE) {
    return optype == kBW_EQ ? codegenLogical(lower_bw_eq(bin_oper).get(), co)
                            : codegenLogical(lower_bw_ne(bin_oper).get(), co);
  }
  if (is_unnest(lhs) || is_unnest(rhs)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in comparisons");
  }
  check_array_comp_cond(bin_oper);
  const auto& lhs_ti = lhs->get_type_info();
  const auto& rhs_ti = rhs->get_type_info();

  if (lhs_ti.is_string() && rhs_ti.is_string() &&
      !(IS_EQUIVALENCE(optype) || optype == kNE)) {
    auto cmp_str = codegenStrCmp(optype,
                                 qualifier,
                                 bin_oper->get_own_left_operand(),
                                 bin_oper->get_own_right_operand(),
                                 co);
    if (cmp_str) {
      return cmp_str;
    }
  }

  if (lhs_ti.is_decimal()) {
    auto cmp_decimal_const =
        codegenCmpDecimalConst(optype, qualifier, lhs, lhs_ti, rhs, co);
    if (cmp_decimal_const) {
      return cmp_decimal_const;
    }
  }
  auto lhs_lvs = codegen(lhs, true, co);
  return codegenCmp(optype, qualifier, lhs_lvs, lhs_ti, rhs, co);
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenCmpFun(
    const Analyzer::BinOper* bin_oper,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto lhs = bin_oper->get_left_operand();
  const auto rhs = bin_oper->get_right_operand();

  // TBD: Multi-Col Comparison.
  // TBD: Bitwise EQ.
  if (is_unnest(lhs) || is_unnest(rhs)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in comparisons");
  }
  // TODO: String, Array support.
  // TODO: Decimal constant support.
  const auto& lhs_ti = lhs->get_type_info();
  const auto& rhs_ti = rhs->get_type_info();
  CHECK_EQ(lhs_ti.get_type(), rhs_ti.get_type());

  auto lhs_lv = codegen(lhs, co, true);
  auto rhs_lv = codegen(rhs, co, true);

  auto lhs_nullable = dynamic_cast<NullableColValues*>(lhs_lv.get());
  auto rhs_nullable = dynamic_cast<NullableColValues*>(rhs_lv.get());
  llvm::Value* null = nullptr;

  if (lhs_nullable && rhs_nullable) {
    if (lhs_nullable->getNull() && rhs_nullable->getNull()) {
      null = cgen_state_->ir_builder_.CreateOr(lhs_nullable->getNull(),
                                               rhs_nullable->getNull());
    } else {
      null = lhs_nullable->getNull() ? lhs_nullable->getNull() : rhs_nullable->getNull();
    }
  } else if (lhs_nullable || rhs_nullable) {
    null = lhs_nullable ? lhs_nullable->getNull() : rhs_nullable->getNull();
  }

  switch (lhs_ti.get_type()) {
    case kVARCHAR:
    case kTEXT:
    case kCHAR:
      return codegenVarcharCmpFun(bin_oper, lhs_lv.get(), rhs_lv.get(), null);
    default:
      return codegenFixedSizeColCmpFun(bin_oper, lhs_lv.get(), rhs_lv.get(), null);
  }
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenFixedSizeColCmpFun(
    const Analyzer::BinOper* bin_oper,
    CodegenColValues* lhs,
    CodegenColValues* rhs,
    llvm::Value* null) {
  AUTOMATIC_IR_METADATA(cgen_state_);

  auto lhs_fixsize = dynamic_cast<FixedSizeColValues*>(lhs);
  auto rhs_fixsize = dynamic_cast<FixedSizeColValues*>(rhs);

  CHECK(lhs_fixsize && rhs_fixsize);

  auto lh_value = lhs_fixsize->getValue(), rh_value = rhs_fixsize->getValue();
  CHECK(lh_value && rh_value);
  llvm::Value* value =
      lh_value->getType()->isIntegerTy()
          ? cgen_state_->ir_builder_.CreateICmp(
                llvm_icmp_pred(bin_oper->get_optype()), lh_value, rh_value)
          : cgen_state_->ir_builder_.CreateFCmp(
                llvm_fcmp_pred(bin_oper->get_optype()), lh_value, rh_value);

  return std::make_unique<FixedSizeColValues>(value, null);
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenVarcharCmpFun(
    const Analyzer::BinOper* bin_oper,
    CodegenColValues* lhs,
    CodegenColValues* rhs,
    llvm::Value* null) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  auto lhs_fixsize = dynamic_cast<TwoValueColValues*>(lhs);
  CHECK(lhs_fixsize);
  auto rhs_fixsize = dynamic_cast<TwoValueColValues*>(rhs);
  CHECK(rhs_fixsize);

  llvm::Value* value = cgen_state_->emitCall("string_eq",
                                             {lhs_fixsize->getValueAt(0),
                                              lhs_fixsize->getValueAt(1),
                                              rhs_fixsize->getValueAt(0),
                                              rhs_fixsize->getValueAt(1)});
  return std::make_unique<FixedSizeColValues>(value, null);
}

llvm::Value* CodeGenerator::codegenOverlaps(const SQLOps optype,
                                            const SQLQualifier qualifier,
                                            const std::shared_ptr<Analyzer::Expr> lhs,
                                            const std::shared_ptr<Analyzer::Expr> rhs,
                                            const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto lhs_ti = lhs->get_type_info();
  if (g_enable_overlaps_hashjoin) {
    // failed to build a suitable hash table. short circuit the overlaps expression by
    // always returning true. this will fall into the ST_Contains check, which will do
    // overlaps checks before the heavier contains computation.
    VLOG(1) << "Failed to build overlaps hash table, short circuiting overlaps operator.";
    return llvm::ConstantInt::get(get_int_type(8, cgen_state_->context_), true);
  }

  CHECK(false) << "Unsupported type for overlaps operator: " << lhs_ti.get_type_name();
  return nullptr;
}

llvm::Value* CodeGenerator::codegenStrCmp(const SQLOps optype,
                                          const SQLQualifier qualifier,
                                          const std::shared_ptr<Analyzer::Expr> lhs,
                                          const std::shared_ptr<Analyzer::Expr> rhs,
                                          const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto lhs_ti = lhs->get_type_info();
  const auto rhs_ti = rhs->get_type_info();

  CHECK(lhs_ti.is_string());
  CHECK(rhs_ti.is_string());

  const auto null_check_suffix = get_null_check_suffix(lhs_ti, rhs_ti);
  if (lhs_ti.get_compression() == kENCODING_DICT &&
      rhs_ti.get_compression() == kENCODING_DICT) {
    if (lhs_ti.get_comp_param() == rhs_ti.get_comp_param()) {
      // Both operands share a dictionary

      // check if query is trying to compare a columnt against literal

      auto ir = codegenDictStrCmp(lhs, rhs, optype, co);
      if (ir) {
        return ir;
      }
    } else {
      // Both operands don't share a dictionary
      return nullptr;
    }
  }
  return nullptr;
}

llvm::Value* CodeGenerator::codegenCmpDecimalConst(const SQLOps optype,
                                                   const SQLQualifier qualifier,
                                                   const Analyzer::Expr* lhs,
                                                   const SQLTypeInfo& lhs_ti,
                                                   const Analyzer::Expr* rhs,
                                                   const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  auto u_oper = dynamic_cast<const Analyzer::UOper*>(lhs);
  if (!u_oper || u_oper->get_optype() != kCAST) {
    return nullptr;
  }
  auto rhs_constant = dynamic_cast<const Analyzer::Constant*>(rhs);
  if (!rhs_constant) {
    return nullptr;
  }
  const auto operand = u_oper->get_operand();
  const auto& operand_ti = operand->get_type_info();
  if (operand_ti.is_decimal() && operand_ti.get_scale() < lhs_ti.get_scale()) {
    // lhs decimal type has smaller scale
  } else if (operand_ti.is_integer() && 0 < lhs_ti.get_scale()) {
    // lhs is integer, no need to scale it all the way up to the cmp expr scale
  } else {
    return nullptr;
  }

  auto scale_diff = lhs_ti.get_scale() - operand_ti.get_scale() - 1;
  int64_t bigintval = rhs_constant->get_constval().bigintval;
  bool negative = false;
  if (bigintval < 0) {
    negative = true;
    bigintval = -bigintval;
  }
  int64_t truncated_decimal = bigintval / exp_to_scale(scale_diff);
  int64_t decimal_tail = bigintval % exp_to_scale(scale_diff);
  if (truncated_decimal % 10 == 0 && decimal_tail > 0) {
    truncated_decimal += 1;
  }
  SQLTypeInfo new_ti = SQLTypeInfo(
      kDECIMAL, 19, lhs_ti.get_scale() - scale_diff, operand_ti.get_notnull());
  if (negative) {
    truncated_decimal = -truncated_decimal;
  }
  Datum d;
  d.bigintval = truncated_decimal;
  const auto new_rhs_lit =
      makeExpr<Analyzer::Constant>(new_ti, rhs_constant->get_is_null(), d);
  const auto operand_lv = codegen(operand, true, co).front();
  const auto lhs_lv = codegenCast(operand_lv, operand_ti, new_ti, false, co);
  return codegenCmp(optype, qualifier, {lhs_lv}, new_ti, new_rhs_lit.get(), co);
}

llvm::Value* CodeGenerator::codegenCmp(const SQLOps optype,
                                       const SQLQualifier qualifier,
                                       std::vector<llvm::Value*> lhs_lvs,
                                       const SQLTypeInfo& lhs_ti,
                                       const Analyzer::Expr* rhs,
                                       const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(IS_COMPARISON(optype));
  const auto& rhs_ti = rhs->get_type_info();
  if (rhs_ti.is_array()) {
    return codegenQualifierCmp(optype, qualifier, lhs_lvs, rhs, co);
  }
  auto rhs_lvs = codegen(rhs, true, co);
  CHECK_EQ(kONE, qualifier);
  CHECK((lhs_ti.get_type() == rhs_ti.get_type()) ||
        (lhs_ti.is_string() && rhs_ti.is_string()));
  const auto null_check_suffix = get_null_check_suffix(lhs_ti, rhs_ti);
  if (lhs_ti.is_integer() || lhs_ti.is_decimal() || lhs_ti.is_time() ||
      lhs_ti.is_boolean() || lhs_ti.is_string() || lhs_ti.is_timeinterval()) {
    if (lhs_ti.is_string()) {
      CHECK(rhs_ti.is_string());
      CHECK_EQ(lhs_ti.get_compression(), rhs_ti.get_compression());
      if (lhs_ti.get_compression() == kENCODING_NONE) {
        // unpack pointer + length if necessary
        if (lhs_lvs.size() != 3) {
          CHECK_EQ(size_t(1), lhs_lvs.size());
          lhs_lvs.push_back(cgen_state_->emitCall("extract_str_ptr", {lhs_lvs.front()}));
          lhs_lvs.push_back(cgen_state_->emitCall("extract_str_len", {lhs_lvs.front()}));
        }
        if (rhs_lvs.size() != 3) {
          CHECK_EQ(size_t(1), rhs_lvs.size());
          rhs_lvs.push_back(cgen_state_->emitCall("extract_str_ptr", {rhs_lvs.front()}));
          rhs_lvs.push_back(cgen_state_->emitCall("extract_str_len", {rhs_lvs.front()}));
        }
        std::vector<llvm::Value*> str_cmp_args{
            lhs_lvs[1], lhs_lvs[2], rhs_lvs[1], rhs_lvs[2]};
        if (!null_check_suffix.empty()) {
          str_cmp_args.push_back(
              cgen_state_->inlineIntNull(SQLTypeInfo(kBOOLEAN, false)));
        }
        return cgen_state_->emitCall(
            string_cmp_func(optype) + (null_check_suffix.empty() ? "" : "_nullable"),
            str_cmp_args);
      } else {
        CHECK(optype == kEQ || optype == kNE);
      }
    }

    if (lhs_ti.is_boolean() && rhs_ti.is_boolean()) {
      auto& lhs_lv = lhs_lvs.front();
      auto& rhs_lv = rhs_lvs.front();
      CHECK(lhs_lv->getType()->isIntegerTy());
      CHECK(rhs_lv->getType()->isIntegerTy());
      if (lhs_lv->getType()->getIntegerBitWidth() <
          rhs_lv->getType()->getIntegerBitWidth()) {
        lhs_lv =
            cgen_state_->castToTypeIn(lhs_lv, rhs_lv->getType()->getIntegerBitWidth());
      } else {
        rhs_lv =
            cgen_state_->castToTypeIn(rhs_lv, lhs_lv->getType()->getIntegerBitWidth());
      }
    }

    return null_check_suffix.empty()
               ? cgen_state_->ir_builder_.CreateICmp(
                     llvm_icmp_pred(optype), lhs_lvs.front(), rhs_lvs.front())
               : cgen_state_->emitCall(
                     icmp_name(optype) + "_" + numeric_type_name(lhs_ti) +
                         null_check_suffix,
                     {lhs_lvs.front(),
                      rhs_lvs.front(),
                      cgen_state_->llInt(inline_int_null_val(lhs_ti)),
                      cgen_state_->inlineIntNull(SQLTypeInfo(kBOOLEAN, false))});
  }
  if (lhs_ti.get_type() == kFLOAT || lhs_ti.get_type() == kDOUBLE) {
    return null_check_suffix.empty()
               ? cgen_state_->ir_builder_.CreateFCmp(
                     llvm_fcmp_pred(optype), lhs_lvs.front(), rhs_lvs.front())
               : cgen_state_->emitCall(
                     icmp_name(optype) + "_" + numeric_type_name(lhs_ti) +
                         null_check_suffix,
                     {lhs_lvs.front(),
                      rhs_lvs.front(),
                      lhs_ti.get_type() == kFLOAT ? cgen_state_->llFp(NULL_FLOAT)
                                                  : cgen_state_->llFp(NULL_DOUBLE),
                      cgen_state_->inlineIntNull(SQLTypeInfo(kBOOLEAN, false))});
  }
  CHECK(false);
  return nullptr;
}

llvm::Value* CodeGenerator::codegenQualifierCmp(const SQLOps optype,
                                                const SQLQualifier qualifier,
                                                std::vector<llvm::Value*> lhs_lvs,
                                                const Analyzer::Expr* rhs,
                                                const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto& rhs_ti = rhs->get_type_info();
  const Analyzer::Expr* arr_expr{rhs};
  if (dynamic_cast<const Analyzer::UOper*>(rhs)) {
    const auto cast_arr = static_cast<const Analyzer::UOper*>(rhs);
    CHECK_EQ(kCAST, cast_arr->get_optype());
    arr_expr = cast_arr->get_operand();
  }
  const auto& arr_ti = arr_expr->get_type_info();
  const auto& elem_ti = arr_ti.get_elem_type();
  auto rhs_lvs = codegen(arr_expr, true, co);
  CHECK_NE(kONE, qualifier);
  std::string fname{std::string("array_") + (qualifier == kANY ? "any" : "all") + "_" +
                    icmp_arr_name(optype)};
  const auto& target_ti = rhs_ti.get_elem_type();
  const bool is_real_string{target_ti.is_string() &&
                            target_ti.get_compression() != kENCODING_DICT};
  if (is_real_string) {
    if (g_enable_watchdog) {
      CIDER_THROW(CiderWatchdogException,
                  "Comparison between a dictionary-encoded and a none-encoded string "
                  "would be slow");
    }
    CHECK_EQ(kENCODING_NONE, target_ti.get_compression());
    fname += "_str";
  }
  if (elem_ti.is_integer() || elem_ti.is_boolean() || elem_ti.is_string() ||
      elem_ti.is_decimal()) {
    fname += ("_" + numeric_type_name(elem_ti));
  } else {
    CHECK(elem_ti.is_fp());
    fname += elem_ti.get_type() == kDOUBLE ? "_double" : "_float";
  }
  if (is_real_string) {
    CHECK_EQ(size_t(3), lhs_lvs.size());
    return cgen_state_->emitExternalCall(
        fname,
        get_int_type(1, cgen_state_->context_),
        {rhs_lvs.front(),
         posArg(arr_expr),
         lhs_lvs[1],
         lhs_lvs[2],
         cgen_state_->llInt(int64_t(executor()->getStringDictionaryProxy(
             elem_ti.get_comp_param(), executor()->getRowSetMemoryOwner(), true))),
         cgen_state_->inlineIntNull(elem_ti)});
  }
  if (target_ti.is_integer() || target_ti.is_boolean() || target_ti.is_string() ||
      target_ti.is_decimal()) {
    fname += ("_" + numeric_type_name(target_ti));
  } else {
    CHECK(target_ti.is_fp());
    fname += target_ti.get_type() == kDOUBLE ? "_double" : "_float";
  }
  return cgen_state_->emitExternalCall(
      fname,
      get_int_type(1, cgen_state_->context_),
      {rhs_lvs.front(),
       posArg(arr_expr),
       lhs_lvs.front(),
       elem_ti.is_fp() ? static_cast<llvm::Value*>(cgen_state_->inlineFpNull(elem_ti))
                       : static_cast<llvm::Value*>(cgen_state_->inlineIntNull(elem_ti))});
}
