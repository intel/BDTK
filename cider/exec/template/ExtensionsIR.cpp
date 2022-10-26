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
#include "function/ExtensionFunctionsBinding.h"
#include "function/ExtensionFunctionsWhitelist.h"
#include "function/FunctionHelper.h"
#include "function/scalar/ExtensionFunctions.hpp"

#include <tuple>

extern std::unique_ptr<llvm::Module> udf_cpu_module;

namespace {

llvm::StructType* get_buffer_struct_type(CgenState* cgen_state,
                                         const std::string& ext_func_name,
                                         size_t param_num,
                                         llvm::Type* elem_type,
                                         bool has_is_null) {
  CHECK(elem_type);
  CHECK(elem_type->isPointerTy());
  llvm::StructType* generated_struct_type =
      (has_is_null ? llvm::StructType::get(cgen_state->context_,
                                           {elem_type,
                                            llvm::Type::getInt64Ty(cgen_state->context_),
                                            llvm::Type::getInt8Ty(cgen_state->context_)},
                                           false)
                   : llvm::StructType::get(
                         cgen_state->context_,
                         {elem_type, llvm::Type::getInt64Ty(cgen_state->context_)},
                         false));
  llvm::Function* udf_func = cgen_state->module_->getFunction(ext_func_name);
  if (udf_func) {
    // Compare expected array struct type with type from the function
    // definition from the UDF module, but use the type from the
    // module
    llvm::FunctionType* udf_func_type = udf_func->getFunctionType();
    CHECK_LE(param_num, udf_func_type->getNumParams());
    llvm::Type* param_pointer_type = udf_func_type->getParamType(param_num);
    CHECK(param_pointer_type->isPointerTy());
    llvm::Type* param_type = param_pointer_type->getPointerElementType();
    CHECK(param_type->isStructTy());
    llvm::StructType* struct_type = llvm::cast<llvm::StructType>(param_type);
    CHECK_GE(struct_type->getStructNumElements(),
             generated_struct_type->getStructNumElements())
        << serialize_llvm_object(struct_type);

    const auto expected_elems = generated_struct_type->elements();
    const auto current_elems = struct_type->elements();
    for (size_t i = 0; i < expected_elems.size(); i++) {
      CHECK_EQ(expected_elems[i], current_elems[i])
          << "[" << ::toString(expected_elems[i]) << ", " << ::toString(current_elems[i])
          << "]";
    }

    if (struct_type->isLiteral()) {
      return struct_type;
    }

    llvm::StringRef struct_name = struct_type->getStructName();
#if LLVM_VERSION_MAJOR >= 12
    return struct_type->getTypeByName(cgen_state->context_, struct_name);
#else
    return cgen_state->module_->getTypeByName(struct_name);
#endif
  }
  return generated_struct_type;
}

llvm::Type* ext_arg_type_to_llvm_type(const ExtArgumentType ext_arg_type,
                                      llvm::LLVMContext& ctx) {
  switch (ext_arg_type) {
    case ExtArgumentType::Bool:  // pass thru to Int8
    case ExtArgumentType::Int8:
      return get_int_type(8, ctx);
    case ExtArgumentType::Int16:
      return get_int_type(16, ctx);
    case ExtArgumentType::Int32:
      return get_int_type(32, ctx);
    case ExtArgumentType::Int64:
      return get_int_type(64, ctx);
    case ExtArgumentType::Float:
      return llvm::Type::getFloatTy(ctx);
    case ExtArgumentType::Double:
      return llvm::Type::getDoubleTy(ctx);
    case ExtArgumentType::ArrayInt64:
    case ExtArgumentType::ArrayInt32:
    case ExtArgumentType::ArrayInt16:
    case ExtArgumentType::ArrayBool:
    case ExtArgumentType::ArrayInt8:
    case ExtArgumentType::ArrayDouble:
    case ExtArgumentType::ArrayFloat:
    case ExtArgumentType::ColumnInt64:
    case ExtArgumentType::ColumnInt32:
    case ExtArgumentType::ColumnInt16:
    case ExtArgumentType::ColumnBool:
    case ExtArgumentType::ColumnInt8:
    case ExtArgumentType::ColumnDouble:
    case ExtArgumentType::ColumnFloat:
    case ExtArgumentType::TextEncodingNone:
    case ExtArgumentType::ColumnListInt64:
    case ExtArgumentType::ColumnListInt32:
    case ExtArgumentType::ColumnListInt16:
    case ExtArgumentType::ColumnListBool:
    case ExtArgumentType::ColumnListInt8:
    case ExtArgumentType::ColumnListDouble:
    case ExtArgumentType::ColumnListFloat:
      return llvm::Type::getVoidTy(ctx);
    default:
      CHECK(false);
  }
  CHECK(false);
  return nullptr;
}

inline SQLTypeInfo get_sql_type_from_llvm_type(const llvm::Type* ll_type) {
  CHECK(ll_type);
  const auto bits = ll_type->getPrimitiveSizeInBits();

  if (ll_type->isFloatingPointTy()) {
    switch (bits) {
      case 32:
        return SQLTypeInfo(kFLOAT, false);
      case 64:
        return SQLTypeInfo(kDOUBLE, false);
      default:
        LOG(FATAL) << "Unsupported llvm floating point type: " << bits
                   << ", only 32 and 64 bit floating point is supported.";
    }
  } else {
    switch (bits) {
      case 1:
        return SQLTypeInfo(kBOOLEAN, false);
      case 8:
        return SQLTypeInfo(kTINYINT, false);
      case 16:
        return SQLTypeInfo(kSMALLINT, false);
      case 32:
        return SQLTypeInfo(kINT, false);
      case 64:
        return SQLTypeInfo(kBIGINT, false);
      default:
        LOG(FATAL) << "Unrecognized llvm type for SQL type: "
                   << bits;  // TODO let's get the real name here
    }
  }
  UNREACHABLE();
  return SQLTypeInfo();
}

inline llvm::Type* get_llvm_type_from_sql_array_type(const SQLTypeInfo ti,
                                                     llvm::LLVMContext& ctx) {
  CHECK(ti.is_buffer());
  if (ti.is_bytes()) {
    return llvm::Type::getInt8PtrTy(ctx);
  }

  const auto& elem_ti = ti.get_elem_type();
  if (elem_ti.is_fp()) {
    switch (elem_ti.get_size()) {
      case 4:
        return llvm::Type::getFloatPtrTy(ctx);
      case 8:
        return llvm::Type::getDoublePtrTy(ctx);
    }
  }

  if (elem_ti.is_boolean()) {
    return llvm::Type::getInt8PtrTy(ctx);
  }

  CHECK(elem_ti.is_integer());
  switch (elem_ti.get_size()) {
    case 1:
      return llvm::Type::getInt8PtrTy(ctx);
    case 2:
      return llvm::Type::getInt16PtrTy(ctx);
    case 4:
      return llvm::Type::getInt32PtrTy(ctx);
    case 8:
      return llvm::Type::getInt64PtrTy(ctx);
  }

  UNREACHABLE();
  return nullptr;
}

bool ext_func_call_requires_nullcheck(const Analyzer::FunctionOper* function_oper) {
  const auto& func_ti = function_oper->get_type_info();
  for (size_t i = 0; i < function_oper->getArity(); ++i) {
    const auto arg = function_oper->getArg(i);
    const auto& arg_ti = arg->get_type_info();
    if ((func_ti.is_array() && arg_ti.is_array()) ||
        (func_ti.is_bytes() && arg_ti.is_bytes())) {
      // If the function returns an array and any of the arguments are arrays, allow NULL
      // scalars.
      // TODO: Make this a property of the FunctionOper following `RETURN NULL ON NULL`
      // semantics.
      return false;
    } else if (!arg_ti.get_notnull() && !arg_ti.is_buffer()) {
      return true;
    } else {
      continue;
    }
  }
  return false;
}

}  // namespace

llvm::Value* CodeGenerator::codegenFunctionOper(
    const Analyzer::FunctionOper* function_oper,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  ExtensionFunction ext_func_sig = [=]() {
    try {
      return bind_function(function_oper);
    } catch (CiderCompileException& e) {
      LOG(WARNING) << "codegenFunctionOper[CPU]: " << e.what();
      throw;
    }
  }();

  const auto& ret_ti = function_oper->get_type_info();
  CHECK(ret_ti.is_integer() || ret_ti.is_fp() || ret_ti.is_boolean() ||
        ret_ti.is_buffer());
  auto ret_ty = ext_arg_type_to_llvm_type(ext_func_sig.getRet(), cgen_state_->context_);
  const auto current_bb = cgen_state_->ir_builder_.GetInsertBlock();
  for (auto it : cgen_state_->ext_call_cache_) {
    if (*it.foper == *function_oper) {
      auto inst = llvm::dyn_cast<llvm::Instruction>(it.lv);
      if (inst && inst->getParent() == current_bb) {
        return it.lv;
      }
    }
  }
  std::vector<llvm::Value*> orig_arg_lvs;
  std::vector<size_t> orig_arg_lvs_index;
  std::unordered_map<llvm::Value*, llvm::Value*> const_arr_size;

  for (size_t i = 0; i < function_oper->getArity(); ++i) {
    orig_arg_lvs_index.push_back(orig_arg_lvs.size());
    const auto arg = function_oper->getArg(i);
    const auto arg_cast = dynamic_cast<const Analyzer::UOper*>(arg);
    const auto arg0 =
        (arg_cast && arg_cast->get_optype() == kCAST) ? arg_cast->get_operand() : arg;
    const auto array_expr_arg = dynamic_cast<const Analyzer::ArrayExpr*>(arg0);
    auto is_local_alloc =
        ret_ti.is_buffer() || (array_expr_arg && array_expr_arg->isLocalAlloc());
    const auto& arg_ti = arg->get_type_info();
    const auto arg_lvs = codegen(arg, true, co);
    if (arg_ti.is_bytes()) {
      CHECK_EQ(size_t(3), arg_lvs.size());
      /* arg_lvs contains:
         c = string_decode(&col_buf0, pos)
         ptr = extract_str_ptr(c)
         sz = extract_str_len(c)
      */
      for (size_t j = 0; j < arg_lvs.size(); j++) {
        orig_arg_lvs.push_back(arg_lvs[j]);
      }
    } else {
      if (arg_lvs.size() > 1) {
        CHECK(arg_ti.is_array());
        CHECK_EQ(size_t(2), arg_lvs.size());
        const_arr_size[arg_lvs.front()] = arg_lvs.back();
      } else {
        CHECK_EQ(size_t(1), arg_lvs.size());
        /* arg_lvs contains:
             &col_buf1
         */
        if (is_local_alloc && arg_ti.get_size() > 0) {
          const_arr_size[arg_lvs.front()] = cgen_state_->llInt(arg_ti.get_size());
        }
      }
      orig_arg_lvs.push_back(arg_lvs.front());
    }
  }
  // The extension function implementations don't handle NULL, they work under
  // the assumption that the inputs are validated before calling them. Generate
  // code to do the check at the call site: if any argument is NULL, return NULL
  // without calling the function at all.
  const auto [bbs, null_buffer_ptr] = beginArgsNullcheck(function_oper, orig_arg_lvs);
  CHECK_GE(orig_arg_lvs.size(), function_oper->getArity());
  // Arguments must be converted to the types the extension function can handle.
  auto args = codegenFunctionOperCastArgs(
      function_oper, &ext_func_sig, orig_arg_lvs, orig_arg_lvs_index, const_arr_size, co);

  llvm::Value* buffer_ret{nullptr};
  if (ret_ti.is_buffer()) {
    // codegen buffer return as first arg
    CHECK(ret_ti.is_array() || ret_ti.is_bytes());
    ret_ty = llvm::Type::getVoidTy(cgen_state_->context_);
    const auto struct_ty = get_buffer_struct_type(
        cgen_state_,
        function_oper->getName(),
        0,
        get_llvm_type_from_sql_array_type(ret_ti, cgen_state_->context_),
        /* has_is_null = */ ret_ti.is_array() || ret_ti.is_bytes());
    buffer_ret = cgen_state_->ir_builder_.CreateAlloca(struct_ty);
    args.insert(args.begin(), buffer_ret);
  }

  const auto ext_call = cgen_state_->emitExternalCall(
      ext_func_sig.getName(), ret_ty, args, {}, ret_ti.is_buffer());
  auto ext_call_nullcheck = endArgsNullcheck(
      bbs, ret_ti.is_buffer() ? buffer_ret : ext_call, null_buffer_ptr, function_oper);

  // Cast the return of the extension function to match the FunctionOper
  if (!(ret_ti.is_buffer())) {
    const auto extension_ret_ti = get_sql_type_from_llvm_type(ret_ty);
    if (bbs.args_null_bb &&
        extension_ret_ti.get_type() != function_oper->get_type_info().get_type() &&
        // Skip i1-->i8 casts for ST_ functions.
        // function_oper ret type is i1, extension ret type is 'upgraded' to i8
        // during type deserialization to 'handle' NULL returns, hence i1-->i8.
        // ST_ functions can't return NULLs, we just need to check arg nullness
        // and if any args are NULL then ST_ function is not called
        function_oper->getName().substr(0, 3) != std::string("ST_")) {
      ext_call_nullcheck = codegenCast(ext_call_nullcheck,
                                       extension_ret_ti,
                                       function_oper->get_type_info(),
                                       false,
                                       co);
    }
  }

  cgen_state_->ext_call_cache_.push_back({function_oper, ext_call_nullcheck});
  return ext_call_nullcheck;
}

std::unique_ptr<CodegenColValues> CodeGenerator::codegenFunctionOp(
    const Analyzer::FunctionOper* function_oper,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  ExtensionFunction ext_func_sig = [=]() {
    try {
      return bind_function(function_oper);
    } catch (CiderCompileException& e) {
      LOG(WARNING) << "codegenFunctionOper[CPU]: " << e.what();
      throw;
    }
  }();

  const auto& ret_ti = function_oper->get_type_info();
  // NOTE: string/array related FunctionOp not supported
  CHECK(ret_ti.is_integer() || ret_ti.is_fp() || ret_ti.is_boolean());
  auto ret_ty = ext_arg_type_to_llvm_type(ext_func_sig.getRet(), cgen_state_->context_);
  const auto current_bb = cgen_state_->ir_builder_.GetInsertBlock();
  std::vector<llvm::Value*> orig_arg_lv_values;
    std::vector<llvm::Value*> orig_arg_lv_nulls;
  std::vector<size_t> orig_arg_lvs_index;
  std::unordered_map<llvm::Value*, llvm::Value*> const_arr_size;

  for (size_t i = 0; i < function_oper->getArity(); ++i) {
    orig_arg_lvs_index.push_back(orig_arg_lv_values.size());
    const auto arg = function_oper->getArg(i);
    const auto arg_cast = dynamic_cast<const Analyzer::UOper*>(arg);
    const auto arg0 =
        (arg_cast && arg_cast->get_optype() == kCAST) ? arg_cast->get_operand() : arg;
    const auto array_expr_arg = dynamic_cast<const Analyzer::ArrayExpr*>(arg0);
    auto is_local_alloc =
        ret_ti.is_buffer() || (array_expr_arg && array_expr_arg->isLocalAlloc());
    const auto& arg_ti = arg->get_type_info();
    auto arg_lv = codegen(arg, co, true);
    auto arg_lv_fixedsize = dynamic_cast<FixedSizeColValues*>(arg_lv.get());
    // TODO: (yma11) add support for bytes/array arguments
    CHECK(arg_lv_fixedsize);
    orig_arg_lv_values.emplace_back(arg_lv_fixedsize->getValue());
    orig_arg_lv_nulls.emplace_back(arg_lv_fixedsize->getNull());
  }
  llvm::Value* null{nullptr};
  bool is_nullable = ext_func_call_requires_nullcheck(function_oper);
  // null is true when at least one argument is null.
  if (is_nullable) {
    null = codegenFunctionOperNullArgForArrow(function_oper, orig_arg_lv_nulls);
  } else {
    null = llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_),
                                  false);
  }
  // Arguments must be converted to the types the extension function can handle.
  auto args = codegenFunctionOperCastArgsForArrow(
      function_oper, &ext_func_sig, orig_arg_lv_values, orig_arg_lvs_index, const_arr_size, co);
  auto ext_call = cgen_state_->emitExternalCall(
      ext_func_sig.getName(), ret_ty, args, {}, ret_ti.is_buffer());
  // Cast the return of the extension function to match the FunctionOper
  if (!(ret_ti.is_buffer())) {
    const auto extension_ret_ti = get_sql_type_from_llvm_type(ret_ty);
    if (is_nullable &&
        extension_ret_ti.get_type() != function_oper->get_type_info().get_type()) {
      // TODO: (yma11) need to switch to new codegenCast for arrow
      ext_call = codegenCast(
          ext_call, extension_ret_ti, function_oper->get_type_info(), false, co);
    }
  }
  cgen_state_->ext_call_cache_.push_back({function_oper, ext_call});
  return std::make_unique<FixedSizeColValues>(ext_call, null);
}

// Start the control flow needed for a call site check of NULL arguments.
std::tuple<CodeGenerator::ArgNullcheckBBs, llvm::Value*>
CodeGenerator::beginArgsNullcheck(const Analyzer::FunctionOper* function_oper,
                                  const std::vector<llvm::Value*>& orig_arg_lvs) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  llvm::BasicBlock* args_null_bb{nullptr};
  llvm::BasicBlock* args_notnull_bb{nullptr};
  llvm::BasicBlock* orig_bb = cgen_state_->ir_builder_.GetInsertBlock();
  llvm::Value* null_array_alloca{nullptr};
  // Only generate the check if required (at least one argument must be nullable).
  if (ext_func_call_requires_nullcheck(function_oper)) {
    const auto func_ti = function_oper->get_type_info();
    if (func_ti.is_buffer()) {
      const auto arr_struct_ty = get_buffer_struct_type(
          cgen_state_,
          function_oper->getName(),
          0,
          get_llvm_type_from_sql_array_type(func_ti, cgen_state_->context_),
          func_ti.is_array() || func_ti.is_bytes());
      null_array_alloca = cgen_state_->ir_builder_.CreateAlloca(arr_struct_ty);
    }
    const auto args_notnull_lv = cgen_state_->ir_builder_.CreateNot(
        codegenFunctionOperNullArg(function_oper, orig_arg_lvs));
    args_notnull_bb = llvm::BasicBlock::Create(
        cgen_state_->context_, "args_notnull", cgen_state_->current_func_);
    args_null_bb = llvm::BasicBlock::Create(
        cgen_state_->context_, "args_null", cgen_state_->current_func_);
    cgen_state_->ir_builder_.CreateCondBr(args_notnull_lv, args_notnull_bb, args_null_bb);
    cgen_state_->ir_builder_.SetInsertPoint(args_notnull_bb);
  }
  return std::make_tuple(
      CodeGenerator::ArgNullcheckBBs{args_null_bb, args_notnull_bb, orig_bb},
      null_array_alloca);
}

// Wrap up the control flow needed for NULL argument handling.
llvm::Value* CodeGenerator::endArgsNullcheck(
    const ArgNullcheckBBs& bbs,
    llvm::Value* fn_ret_lv,
    llvm::Value* null_array_ptr,
    const Analyzer::FunctionOper* function_oper) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  if (bbs.args_null_bb) {
    CHECK(bbs.args_notnull_bb);
    cgen_state_->ir_builder_.CreateBr(bbs.args_null_bb);
    cgen_state_->ir_builder_.SetInsertPoint(bbs.args_null_bb);

    llvm::PHINode* ext_call_phi{nullptr};
    llvm::Value* null_lv{nullptr};
    const auto func_ti = function_oper->get_type_info();
    if (!func_ti.is_buffer()) {
      // The pre-cast SQL equivalent of the type returned by the extension function.
      const auto extension_ret_ti = get_sql_type_from_llvm_type(fn_ret_lv->getType());

      ext_call_phi = cgen_state_->ir_builder_.CreatePHI(
          extension_ret_ti.is_fp()
              ? get_fp_type(extension_ret_ti.get_size() * 8, cgen_state_->context_)
              : get_int_type(extension_ret_ti.get_size() * 8, cgen_state_->context_),
          2);

      null_lv =
          extension_ret_ti.is_fp()
              ? static_cast<llvm::Value*>(cgen_state_->inlineFpNull(extension_ret_ti))
              : static_cast<llvm::Value*>(cgen_state_->inlineIntNull(extension_ret_ti));
    } else {
      const auto arr_struct_ty = get_buffer_struct_type(
          cgen_state_,
          function_oper->getName(),
          0,
          get_llvm_type_from_sql_array_type(func_ti, cgen_state_->context_),
          true);
      ext_call_phi =
          cgen_state_->ir_builder_.CreatePHI(llvm::PointerType::get(arr_struct_ty, 0), 2);

      CHECK(null_array_ptr);
      const auto arr_null_bool =
          cgen_state_->ir_builder_.CreateStructGEP(arr_struct_ty, null_array_ptr, 2);
      cgen_state_->ir_builder_.CreateStore(
          llvm::ConstantInt::get(get_int_type(8, cgen_state_->context_), 1),
          arr_null_bool);

      const auto arr_null_size =
          cgen_state_->ir_builder_.CreateStructGEP(arr_struct_ty, null_array_ptr, 1);
      cgen_state_->ir_builder_.CreateStore(
          llvm::ConstantInt::get(get_int_type(64, cgen_state_->context_), 0),
          arr_null_size);
    }
    ext_call_phi->addIncoming(fn_ret_lv, bbs.args_notnull_bb);
    ext_call_phi->addIncoming(func_ti.is_buffer() ? null_array_ptr : null_lv,
                              bbs.orig_bb);

    return ext_call_phi;
  }
  return fn_ret_lv;
}

namespace {

bool call_requires_custom_type_handling(const Analyzer::FunctionOper* function_oper) {
  const auto& ret_ti = function_oper->get_type_info();
  if (!ret_ti.is_integer() && !ret_ti.is_fp()) {
    return true;
  }
  for (size_t i = 0; i < function_oper->getArity(); ++i) {
    const auto arg = function_oper->getArg(i);
    const auto& arg_ti = arg->get_type_info();
    if (!arg_ti.is_integer() && !arg_ti.is_fp()) {
      return true;
    }
  }
  return false;
}

}  // namespace

llvm::Value* CodeGenerator::codegenFunctionOperWithCustomTypeHandling(
    const Analyzer::FunctionOperWithCustomTypeHandling* function_oper,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  if (call_requires_custom_type_handling(function_oper)) {
    // Some functions need the return type to be the same as the input type.
    if (function_oper->getName() == "FLOOR" || function_oper->getName() == "CEIL") {
      CHECK_EQ(size_t(1), function_oper->getArity());
      const auto arg = function_oper->getArg(0);
      const auto& arg_ti = arg->get_type_info();
      CHECK(arg_ti.is_decimal());
      const auto arg_lvs = codegen(arg, true, co);
      CHECK_EQ(size_t(1), arg_lvs.size());
      const auto arg_lv = arg_lvs.front();
      CHECK(arg_lv->getType()->isIntegerTy(64));
      CodeGenerator::ArgNullcheckBBs bbs;
      std::tie(bbs, std::ignore) = beginArgsNullcheck(function_oper, {arg_lvs});
      const std::string func_name =
          (function_oper->getName() == "FLOOR") ? "decimal_floor" : "decimal_ceil";
      const auto covar_result_lv = cgen_state_->emitCall(
          func_name, {arg_lv, cgen_state_->llInt(exp_to_scale(arg_ti.get_scale()))});
      const auto ret_ti = function_oper->get_type_info();
      CHECK(ret_ti.is_decimal());
      CHECK_EQ(0, ret_ti.get_scale());
      const auto result_lv = cgen_state_->ir_builder_.CreateSDiv(
          covar_result_lv, cgen_state_->llInt(exp_to_scale(arg_ti.get_scale())));
      return endArgsNullcheck(bbs, result_lv, nullptr, function_oper);
    } else if (function_oper->getName() == "ROUND" &&
               function_oper->getArg(0)->get_type_info().is_decimal()) {
      CHECK_EQ(size_t(2), function_oper->getArity());

      const auto arg0 = function_oper->getArg(0);
      const auto& arg0_ti = arg0->get_type_info();
      const auto arg0_lvs = codegen(arg0, true, co);
      CHECK_EQ(size_t(1), arg0_lvs.size());
      const auto arg0_lv = arg0_lvs.front();
      CHECK(arg0_lv->getType()->isIntegerTy(64));

      const auto arg1 = function_oper->getArg(1);
      const auto& arg1_ti = arg1->get_type_info();
      CHECK(arg1_ti.is_integer());
      const auto arg1_lvs = codegen(arg1, true, co);
      auto arg1_lv = arg1_lvs.front();
      if (arg1_ti.get_type() != kINT) {
        arg1_lv = codegenCast(arg1_lv, arg1_ti, SQLTypeInfo(kINT, true), false, co);
      }

      CodeGenerator::ArgNullcheckBBs bbs0;
      std::tie(bbs0, std::ignore) =
          beginArgsNullcheck(function_oper, {arg0_lv, arg1_lvs.front()});

      const std::string func_name = "Round__4";
      const auto ret_ti = function_oper->get_type_info();
      CHECK(ret_ti.is_decimal());
      const auto result_lv = cgen_state_->emitExternalCall(
          func_name,
          get_int_type(64, cgen_state_->context_),
          {arg0_lv, arg1_lv, cgen_state_->llInt(arg0_ti.get_scale())});

      return endArgsNullcheck(bbs0, result_lv, nullptr, function_oper);
    }
    CIDER_THROW(
        CiderCompileException,
        "Type combination not supported for function " + function_oper->getName());
  }
  return codegenFunctionOper(function_oper, co);
}

// Generates code which returns true if at least one of the arguments is NULL.
llvm::Value* CodeGenerator::codegenFunctionOperNullArgForArrow(
    const Analyzer::FunctionOper* function_oper,
    const std::vector<llvm::Value*>& orig_arg_lv_nulls) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  llvm::Value* one_arg_null =
      llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_), false);
  size_t physical_coord_cols = 0;
  for (size_t i = 0, j = 0; i < function_oper->getArity();
       ++i, j += std::max(size_t(1), physical_coord_cols)) {
    const auto arg = function_oper->getArg(i);
    const auto& arg_ti = arg->get_type_info();
    physical_coord_cols = arg_ti.get_physical_coord_cols();
    if (arg_ti.get_notnull()) {
      continue;
    }
    // TODO:(yma11) add string support
    CHECK(arg_ti.is_number() or arg_ti.is_boolean());
    one_arg_null =
        cgen_state_->ir_builder_.CreateOr(one_arg_null, orig_arg_lv_nulls[j]);
  }
  return one_arg_null;
}

// Generates code which returns true iff at least one of the arguments is NULL.
llvm::Value* CodeGenerator::codegenFunctionOperNullArg(
    const Analyzer::FunctionOper* function_oper,
    const std::vector<llvm::Value*>& orig_arg_lvs) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  llvm::Value* one_arg_null =
      llvm::ConstantInt::get(llvm::IntegerType::getInt1Ty(cgen_state_->context_), false);
  size_t physical_coord_cols = 0;
  for (size_t i = 0, j = 0; i < function_oper->getArity();
       ++i, j += std::max(size_t(1), physical_coord_cols)) {
    const auto arg = function_oper->getArg(i);
    const auto& arg_ti = arg->get_type_info();
    physical_coord_cols = arg_ti.get_physical_coord_cols();
    if (arg_ti.get_notnull()) {
      continue;
    }
    if (arg_ti.is_buffer()) {
      auto fname = "array_is_null";
      auto is_null_lv = cgen_state_->emitExternalCall(
          fname, get_int_type(1, cgen_state_->context_), {orig_arg_lvs[j], posArg(arg)});
      one_arg_null = cgen_state_->ir_builder_.CreateOr(one_arg_null, is_null_lv);
      continue;
    }
    CHECK(arg_ti.is_number() or arg_ti.is_boolean());
    one_arg_null = cgen_state_->ir_builder_.CreateOr(
        one_arg_null, codegenIsNullNumber(orig_arg_lvs[j], arg_ti));
  }
  return one_arg_null;
}

std::pair<llvm::Value*, llvm::Value*> CodeGenerator::codegenArrayBuff(
    llvm::Value* chunk,
    llvm::Value* row_pos,
    SQLTypes array_type,
    bool cast_and_extend) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto elem_ti =
      SQLTypeInfo(
          SQLTypes::kARRAY, 0, 0, false, EncodingType::kENCODING_NONE, 0, array_type)
          .get_elem_type();

  auto buff = cgen_state_->emitExternalCall(
      "array_buff", llvm::Type::getInt32PtrTy(cgen_state_->context_), {chunk, row_pos});

  auto len = cgen_state_->emitExternalCall(
      "array_size",
      get_int_type(32, cgen_state_->context_),
      {chunk, row_pos, cgen_state_->llInt(log2_bytes(elem_ti.get_logical_size()))});

  if (cast_and_extend) {
    buff = castArrayPointer(buff, elem_ti);
    len =
        cgen_state_->ir_builder_.CreateZExt(len, get_int_type(64, cgen_state_->context_));
  }

  return std::make_pair(buff, len);
}

void CodeGenerator::codegenBufferArgs(const std::string& ext_func_name,
                                      size_t param_num,
                                      llvm::Value* buffer_buf,
                                      llvm::Value* buffer_size,
                                      llvm::Value* buffer_null,
                                      std::vector<llvm::Value*>& output_args) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(buffer_buf);
  CHECK(buffer_size);

  auto buffer_abstraction = get_buffer_struct_type(
      cgen_state_, ext_func_name, param_num, buffer_buf->getType(), !!(buffer_null));
  auto alloc_mem = cgen_state_->ir_builder_.CreateAlloca(buffer_abstraction);

  auto buffer_buf_ptr =
      cgen_state_->ir_builder_.CreateStructGEP(buffer_abstraction, alloc_mem, 0);
  cgen_state_->ir_builder_.CreateStore(buffer_buf, buffer_buf_ptr);

  auto buffer_size_ptr =
      cgen_state_->ir_builder_.CreateStructGEP(buffer_abstraction, alloc_mem, 1);
  cgen_state_->ir_builder_.CreateStore(buffer_size, buffer_size_ptr);

  if (buffer_null) {
    auto bool_extended_type = llvm::Type::getInt8Ty(cgen_state_->context_);
    auto buffer_null_extended =
        cgen_state_->ir_builder_.CreateZExt(buffer_null, bool_extended_type);
    auto buffer_is_null_ptr =
        cgen_state_->ir_builder_.CreateStructGEP(buffer_abstraction, alloc_mem, 2);
    cgen_state_->ir_builder_.CreateStore(buffer_null_extended, buffer_is_null_ptr);
  }
  output_args.push_back(alloc_mem);
}

// Generate CAST operations for arguments in `orig_arg_lvs` to the types required by
// `ext_func_sig`.
std::vector<llvm::Value*> CodeGenerator::codegenFunctionOperCastArgsForArrow(
    const Analyzer::FunctionOper* function_oper,
    const ExtensionFunction* ext_func_sig,
    const std::vector<llvm::Value*>& orig_arg_lv_values,
    const std::vector<size_t>& orig_arg_lvs_index,
    const std::unordered_map<llvm::Value*, llvm::Value*>& const_arr_size,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(ext_func_sig);
  const auto& ext_func_args = ext_func_sig->getArgs();
  CHECK_LE(function_oper->getArity(), ext_func_args.size());
  const auto func_ti = function_oper->get_type_info();
  std::vector<llvm::Value*> args;
  // NOTE: following variables are defined also for complicated cases like bytes/array
  // related functions, we keep it for future use
  // TODO: (yma11) add support for bytes/array functionsl
  /*
    i: argument in RA for the function operand
    j: extra offset in ext_func_args
    k: origin_arg_lvs counter, equal to orig_arg_lvs_index[i]
    ij: ext_func_args counter, equal to i + j
    dj: offset when UDF implementation first argument corresponds to return value
   */
  for (size_t i = 0, j = 0, dj = (func_ti.is_buffer() ? 1 : 0);
       i < function_oper->getArity();
       ++i) {
    size_t k = orig_arg_lvs_index[i];
    size_t ij = i + j;
    const auto arg = function_oper->getArg(i);
    const auto ext_func_arg = ext_func_args[ij];
    const auto& arg_ti = arg->get_type_info();
    llvm::Value* arg_lv{nullptr};
    CHECK(is_ext_arg_type_scalar(ext_func_arg));
    const auto arg_target_ti = ext_arg_type_to_type_info(ext_func_arg);
    if (arg_ti.get_type() != arg_target_ti.get_type()) {
      // TODO: (yma11) need to switch to new codegenCast for arrow
      arg_lv = codegenCast(
          orig_arg_lv_values[k], arg_ti, arg_target_ti, false, co);
    } else {
      arg_lv = orig_arg_lv_values[k];
    }
    CHECK_EQ(arg_lv->getType(),
             ext_arg_type_to_llvm_type(ext_func_arg, cgen_state_->context_));
    args.push_back(arg_lv);
  }
  return args;
}

// Generate CAST operations for arguments in `orig_arg_lvs` to the types required by
// `ext_func_sig`.
std::vector<llvm::Value*> CodeGenerator::codegenFunctionOperCastArgs(
    const Analyzer::FunctionOper* function_oper,
    const ExtensionFunction* ext_func_sig,
    const std::vector<llvm::Value*>& orig_arg_lvs,
    const std::vector<size_t>& orig_arg_lvs_index,
    const std::unordered_map<llvm::Value*, llvm::Value*>& const_arr_size,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  CHECK(ext_func_sig);
  const auto& ext_func_args = ext_func_sig->getArgs();
  CHECK_LE(function_oper->getArity(), ext_func_args.size());
  const auto func_ti = function_oper->get_type_info();
  std::vector<llvm::Value*> args;
  /*
    i: argument in RA for the function operand
    j: extra offset in ext_func_args
    k: origin_arg_lvs counter, equal to orig_arg_lvs_index[i]
    ij: ext_func_args counter, equal to i + j
    dj: offset when UDF implementation first argument corresponds to return value
   */
  for (size_t i = 0, j = 0, dj = (func_ti.is_buffer() ? 1 : 0);
       i < function_oper->getArity();
       ++i) {
    size_t k = orig_arg_lvs_index[i];
    size_t ij = i + j;
    const auto arg = function_oper->getArg(i);
    const auto ext_func_arg = ext_func_args[ij];
    const auto& arg_ti = arg->get_type_info();
    llvm::Value* arg_lv{nullptr};
    if (arg_ti.is_bytes()) {
      CHECK(ext_func_arg == ExtArgumentType::TextEncodingNone)
          << ::toString(ext_func_arg);
      const auto ptr_lv = orig_arg_lvs[k + 1];
      const auto len_lv = orig_arg_lvs[k + 2];
      auto& builder = cgen_state_->ir_builder_;
      auto string_buf_arg = builder.CreatePointerCast(
          ptr_lv, llvm::Type::getInt8PtrTy(cgen_state_->context_));
      auto string_size_arg =
          builder.CreateZExt(len_lv, get_int_type(64, cgen_state_->context_));
      codegenBufferArgs(ext_func_sig->getName(),
                        ij + dj,
                        string_buf_arg,
                        string_size_arg,
                        nullptr,
                        args);
    } else if (arg_ti.is_array()) {
      bool const_arr = (const_arr_size.count(orig_arg_lvs[k]) > 0);
      const auto elem_ti = arg_ti.get_elem_type();
      // TODO: switch to fast fixlen variants
      const auto ptr_lv = (const_arr)
                              ? orig_arg_lvs[k]
                              : cgen_state_->emitExternalCall(
                                    "array_buff",
                                    llvm::Type::getInt8PtrTy(cgen_state_->context_),
                                    {orig_arg_lvs[k], posArg(arg)});
      const auto len_lv =
          (const_arr) ? const_arr_size.at(orig_arg_lvs[k])
                      : cgen_state_->emitExternalCall(
                            "array_size",
                            get_int_type(32, cgen_state_->context_),
                            {orig_arg_lvs[k],
                             posArg(arg),
                             cgen_state_->llInt(log2_bytes(elem_ti.get_logical_size()))});

      if (is_ext_arg_type_pointer(ext_func_arg)) {
        args.push_back(castArrayPointer(ptr_lv, elem_ti));
        args.push_back(cgen_state_->ir_builder_.CreateZExt(
            len_lv, get_int_type(64, cgen_state_->context_)));
        j++;
      } else if (is_ext_arg_type_array(ext_func_arg)) {
        auto array_buf_arg = castArrayPointer(ptr_lv, elem_ti);
        auto& builder = cgen_state_->ir_builder_;
        auto array_size_arg =
            builder.CreateZExt(len_lv, get_int_type(64, cgen_state_->context_));
        auto array_null_arg =
            cgen_state_->emitExternalCall("array_is_null",
                                          get_int_type(1, cgen_state_->context_),
                                          {orig_arg_lvs[k], posArg(arg)});
        codegenBufferArgs(ext_func_sig->getName(),
                          ij + dj,
                          array_buf_arg,
                          array_size_arg,
                          array_null_arg,
                          args);
      } else {
        UNREACHABLE();
      }
    } else {
      CHECK(is_ext_arg_type_scalar(ext_func_arg));
      const auto arg_target_ti = ext_arg_type_to_type_info(ext_func_arg);
      if (arg_ti.get_type() != arg_target_ti.get_type()) {
        arg_lv = codegenCast(orig_arg_lvs[k], arg_ti, arg_target_ti, false, co);
      } else {
        arg_lv = orig_arg_lvs[k];
      }
      CHECK_EQ(arg_lv->getType(),
               ext_arg_type_to_llvm_type(ext_func_arg, cgen_state_->context_));
      args.push_back(arg_lv);
    }
  }
  return args;
}

llvm::Value* CodeGenerator::castArrayPointer(llvm::Value* ptr,
                                             const SQLTypeInfo& elem_ti) {
  AUTOMATIC_IR_METADATA(cgen_state_);
  if (elem_ti.get_type() == kFLOAT) {
    return cgen_state_->ir_builder_.CreatePointerCast(
        ptr, llvm::Type::getFloatPtrTy(cgen_state_->context_));
  }
  if (elem_ti.get_type() == kDOUBLE) {
    return cgen_state_->ir_builder_.CreatePointerCast(
        ptr, llvm::Type::getDoublePtrTy(cgen_state_->context_));
  }
  CHECK(elem_ti.is_integer() || elem_ti.is_boolean() ||
        (elem_ti.is_string() && elem_ti.get_compression() == kENCODING_DICT));
  switch (elem_ti.get_size()) {
    case 1:
      return cgen_state_->ir_builder_.CreatePointerCast(
          ptr, llvm::Type::getInt8PtrTy(cgen_state_->context_));
    case 2:
      return cgen_state_->ir_builder_.CreatePointerCast(
          ptr, llvm::Type::getInt16PtrTy(cgen_state_->context_));
    case 4:
      return cgen_state_->ir_builder_.CreatePointerCast(
          ptr, llvm::Type::getInt32PtrTy(cgen_state_->context_));
    case 8:
      return cgen_state_->ir_builder_.CreatePointerCast(
          ptr, llvm::Type::getInt64PtrTy(cgen_state_->context_));
    default:
      CHECK(false);
  }
  return nullptr;
}
