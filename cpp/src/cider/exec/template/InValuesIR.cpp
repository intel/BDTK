/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include <future>
#include <memory>

// will deprecate
llvm::Value* CodeGenerator::codegen(const Analyzer::InValues* expr,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto in_arg = expr->get_arg();
  if (is_unnest(in_arg)) {
    CIDER_THROW(CiderCompileException, "IN not supported for unnested expressions");
  }
  const auto& expr_ti = expr->get_type_info();
  CHECK(expr_ti.is_boolean());
  const auto lhs_lvs = codegen(in_arg, true, co);
  llvm::Value* result{nullptr};
  if (expr_ti.get_notnull()) {
    result = llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_),
                                    false);
  } else {
    result = cgen_state_->llInt(int8_t(0));
  }
  CHECK(result);
  if (co.hoist_literals) {  // TODO(alex): remove this constraint
    auto in_vals_bitmap = createInValuesBitmap(expr, co);
    if (in_vals_bitmap) {
      if (in_vals_bitmap->isEmpty()) {
        return in_vals_bitmap->hasNull()
                   ? cgen_state_->inlineIntNull(SQLTypeInfo(kBOOLEAN, false))
                   : result;
      }
      CHECK_EQ(size_t(1), lhs_lvs.size());
      return cgen_state_->addInValuesBitmap(in_vals_bitmap)
          ->codegen(lhs_lvs.front(), executor());
    }
  }
  if (expr_ti.get_notnull()) {
    for (auto in_val : expr->get_value_list()) {
      result = cgen_state_->ir_builder_.CreateOr(
          result,
          toBool(
              codegenCmp(kEQ, kONE, lhs_lvs, in_arg->get_type_info(), in_val.get(), co)));
    }
  } else {
    for (auto in_val : expr->get_value_list()) {
      const auto crt =
          codegenCmp(kEQ, kONE, lhs_lvs, in_arg->get_type_info(), in_val.get(), co);
      result = cgen_state_->emitCall("logical_or",
                                     {result, crt, cgen_state_->inlineIntNull(expr_ti)});
    }
  }
  return result;
}

// For Cider Data Format
std::unique_ptr<CodegenColValues> CodeGenerator::codegenInValues(
    const Analyzer::InValues* expr,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto in_arg = expr->get_arg();
  if (is_unnest(in_arg)) {
    CIDER_THROW(CiderCompileException, "IN not supported for unnested expressions");
  }
  if (in_arg->get_type_info().is_string()) {
    return codegenInValuesString(expr, co);
  }
  const auto& expr_ti = expr->get_type_info();
  CHECK(expr_ti.is_boolean());
  auto lhs_lv = codegen(in_arg, co, true);
  auto lhs_fixsize = dynamic_cast<FixedSizeColValues*>(lhs_lv.get());
  CHECK(lhs_fixsize);
  auto lhs_value = lhs_fixsize->getValue();
  auto lhs_null = lhs_fixsize->getNull();
  llvm::Value* result{nullptr};
  if (expr_ti.get_notnull()) {
    result = llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_),
                                    false);
  } else {
    result = cgen_state_->llBool(false);
  }
  CHECK(result);
  if (co.hoist_literals) {  // TODO(alex): remove this constraint
    auto in_vals_bitmap = createInValuesBitmapArrow(expr, co);
    if (in_vals_bitmap) {
      if (in_vals_bitmap->isEmpty()) {
        return in_vals_bitmap->hasNull()
                   ? std::make_unique<FixedSizeColValues>(
                         cgen_state_->inlineIntNull(SQLTypeInfo(kBOOLEAN, false)),
                         lhs_null)
                   : std::make_unique<FixedSizeColValues>(result, lhs_null);
      }
      return cgen_state_->addInValuesBitmap(in_vals_bitmap)
          ->codegen(lhs_value, lhs_null, executor());
    } else {
      LOG(INFO) << "Bitmap not created, switch to using OR.";
    }
  }
  for (auto in_val : expr->get_value_list()) {
    auto crt = codegenCmpFun(
        kEQ, kONE, lhs_value, lhs_null, in_arg->get_type_info(), in_val.get(), co);
    auto crt_fixsize = dynamic_cast<FixedSizeColValues*>(crt.get());

    result = cgen_state_->ir_builder_.CreateOr(result, toBool(crt_fixsize->getValue()));
  }
  return std::make_unique<FixedSizeColValues>(result, lhs_null);
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenInValuesString(
    const Analyzer::InValues* expr,
    const CompilationOptions& co) {
  const auto in_arg = expr->get_arg();
  const auto& expr_ti = expr->get_type_info();
  CHECK(expr_ti.is_boolean());
  auto lhs_lv = codegen(in_arg, co, true);
  auto lhs_fixsize = dynamic_cast<TwoValueColValues*>(lhs_lv.get());
  CHECK(lhs_fixsize);
  auto lhs_str_ptr = lhs_fixsize->getValueAt(0);
  auto lhs_str_len = lhs_fixsize->getValueAt(1);
  auto lhs_null = lhs_fixsize->getNull();
  llvm::Value* result{nullptr};
  if (expr_ti.get_notnull()) {
    result = llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_),
                                    false);
  } else {
    result = cgen_state_->llBool(false);
  }
  CHECK(result);

  if (co.hoist_literals) {  // TODO(alex): remove this constraint
    auto in_vals_bitmap = createInValuesBitmapArrow(expr, co);
    if (in_vals_bitmap) {
      if (in_vals_bitmap->isEmpty()) {
        return in_vals_bitmap->hasNull()
                   ? std::make_unique<FixedSizeColValues>(
                         cgen_state_->inlineIntNull(SQLTypeInfo(kBOOLEAN, false)),
                         lhs_null)
                   : std::make_unique<FixedSizeColValues>(result, lhs_null);
      }
      const int64_t cider_string_hasher_handle = 0;
      auto cider_string_hasher_handle_lv = cgen_state_->llInt(cider_string_hasher_handle);
      auto id_val = cgen_state_->emitExternalCall(
          "look_up_string_id_from_hasher",
          get_int_type(64, cgen_state_->context_),
          {lhs_str_ptr, lhs_str_len, cider_string_hasher_handle_lv});
      return cgen_state_->addInValuesBitmap(in_vals_bitmap)
          ->codegen(id_val, lhs_null, executor());
    } else {
      LOG(INFO) << "Bitmap not created, switch to using OR.";
    }
  }

  for (auto in_val : expr->get_value_list()) {
    auto crt = codegenStringEq(lhs_lv.get(), codegen(in_val.get(), co, true).get());
    auto crt_fixsize = dynamic_cast<FixedSizeColValues*>(crt.get());

    result = cgen_state_->ir_builder_.CreateOr(result, toBool(crt_fixsize->getValue()));
  }
  return std::make_unique<FixedSizeColValues>(result, lhs_null);
}

llvm::Value* CodeGenerator::codegen(const Analyzer::InIntegerSet* in_integer_set,
                                    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto in_arg = in_integer_set->get_arg();
  if (is_unnest(in_arg)) {
    CIDER_THROW(CiderCompileException, "IN not supported for unnested expressions");
  }
  const auto& ti = in_integer_set->get_arg()->get_type_info();
  const auto needle_null_val = inline_int_null_val(ti);
  if (!co.hoist_literals) {
    // We never run without literal hoisting in real world scenarios, this avoids a crash
    // when testing.
    CIDER_THROW(CiderCompileException,
                "IN subquery with many right-hand side values not supported when literal "
                "hoisting is disabled");
  }
  auto in_vals_bitmap = std::make_unique<InValuesBitmap>(in_integer_set->get_value_list(),
                                                         needle_null_val,
                                                         Data_Namespace::CPU_LEVEL,
                                                         1,
                                                         executor()->getBufferProvider());
  const auto& in_integer_set_ti = in_integer_set->get_type_info();
  CHECK(in_integer_set_ti.is_boolean());
  const auto lhs_lvs = codegen(in_arg, true, co);
  llvm::Value* result{nullptr};
  if (in_integer_set_ti.get_notnull()) {
    result = llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_),
                                    false);
  } else {
    result = cgen_state_->llInt(int8_t(0));
  }
  CHECK(result);
  CHECK_EQ(size_t(1), lhs_lvs.size());
  return cgen_state_->addInValuesBitmap(in_vals_bitmap)
      ->codegen(lhs_lvs.front(), executor());
}

std::unique_ptr<InValuesBitmap> CodeGenerator::createInValuesBitmap(
    const Analyzer::InValues* in_values,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto& value_list = in_values->get_value_list();
  const auto val_count = value_list.size();
  const auto& ti = in_values->get_arg()->get_type_info();
  if (!(ti.is_integer() || (ti.is_string() && ti.get_compression() == kENCODING_DICT))) {
    return nullptr;
  }
  const auto sdp = ti.is_string() ? executor()->getCiderStringDictionaryProxy() : nullptr;
  // FIXME: (yma11) in heavydb, use bitmap only when values count > 3
  // and switch to normal OR op if not. We make this change because of
  // type info check will fail for case substring(col, const_1, const_2) = "value"
  // as substring has SQLTypeInfo(kTEXT, kENCODING_DICT, 0, kNULLT) while
  // const_1/const_2 has SQLTypeInfo(kVARCHAR, kENCODING_NONE, 0, kNULLT)
  if (val_count > 3 || ti.is_string()) {
    using ListIterator = decltype(value_list.begin());
    std::vector<int64_t> values;
    const auto needle_null_val = inline_int_null_val(ti);
    const auto do_work = [&](std::vector<int64_t>& out_vals,
                             const ListIterator start,
                             const ListIterator end) -> bool {
      for (auto val_it = start; val_it != end; ++val_it) {
        const auto& in_val = *val_it;
        const auto in_val_const =
            dynamic_cast<const Analyzer::Constant*>(extract_cast_arg(in_val.get()));
        if (!in_val_const) {
          return false;
        }
        const auto& in_val_ti = in_val->get_type_info();
        // For string type, we don't need in_val_ti = ti, like substr() vs varchar
        CHECK(ti.is_string() || in_val_ti == ti ||
              get_nullable_type_info(in_val_ti) == ti);
        if (ti.is_string()) {
          CHECK(sdp);
          const auto string_id =
              in_val_const->get_is_null()
                  ? needle_null_val
                  : sdp->getOrAddTransient(*in_val_const->get_constval().stringval);
          if (string_id != StringDictionary::INVALID_STR_ID) {
            out_vals.push_back(string_id);
          }
        } else {
          out_vals.push_back(
              CodeGenerator::codegenIntConst(in_val_const, cgen_state_)->getSExtValue());
        }
      }
      return true;
    };
    do_work(std::ref(values), value_list.begin(), value_list.end());
    try {
      return std::make_unique<InValuesBitmap>(values,
                                              needle_null_val,
                                              Data_Namespace::CPU_LEVEL,
                                              1,
                                              executor()->getBufferProvider());
    } catch (...) {
      return nullptr;
    }
  }
  return nullptr;
}

// string handling is different compare to createInValuesBitmap
std::unique_ptr<InValuesBitmap> CodeGenerator::createInValuesBitmapArrow(
    const Analyzer::InValues* in_values,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto& value_list = in_values->get_value_list();
  const auto val_count = value_list.size();
  const auto& ti = in_values->get_arg()->get_type_info();
  if (!(ti.is_integer() || (ti.is_string() && ti.get_compression() == kENCODING_DICT))) {
    return nullptr;
  }
  const auto stringHasher = nullptr;
  if (val_count > 3) {
    using ListIterator = decltype(value_list.begin());
    std::vector<int64_t> values;
    const auto do_work = [&](std::vector<int64_t>& out_vals,
                             const ListIterator start,
                             const ListIterator end) -> bool {
      for (auto val_it = start; val_it != end; ++val_it) {
        const auto& in_val = *val_it;
        const auto in_val_const =
            dynamic_cast<const Analyzer::Constant*>(extract_cast_arg(in_val.get()));
        if (!in_val_const) {
          return false;
        }
        const auto& in_val_ti = in_val->get_type_info();
        // For string type, we don't need in_val_ti = ti, like substr() vs varchar
        CHECK(ti.is_string() || in_val_ti == ti ||
              get_nullable_type_info(in_val_ti) == ti);
        if (ti.is_string()) {
          std::string str_val = *in_val_const->get_constval().stringval;
          const auto string_id = 0;
        } else {
          out_vals.push_back(
              CodeGenerator::codegenIntConst(in_val_const, cgen_state_)->getSExtValue());
        }
      }
      return true;
    };
    do_work(std::ref(values), value_list.begin(), value_list.end());
    try {
      return std::make_unique<InValuesBitmap>(values,
                                              0,
                                              Data_Namespace::CPU_LEVEL,
                                              1,
                                              executor()->getBufferProvider());
    } catch (...) {
      return nullptr;
    }
  }
  return nullptr;
}
