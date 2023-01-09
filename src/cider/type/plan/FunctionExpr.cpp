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

#include "type/plan/FunctionExpr.h"
#include "function/ExtensionFunctionsBinding.h"
#include "function/ExtensionFunctionsWhitelist.h"
#include "type/data/sqltypes.h"
#include "type/plan/UnaryExpr.h"

namespace Analyzer {

void FunctionOper::collect_rte_idx(std::set<int>& rte_idx_set) const {
  for (unsigned i = 0; i < getArity(); i++) {
    const auto expr = getArg(i);
    expr->collect_rte_idx(rte_idx_set);
  }
}

void FunctionOper::collect_column_var(
    std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>& colvar_set,
    bool include_agg) const {
  for (unsigned i = 0; i < getArity(); i++) {
    const auto expr = getArg(i);
    expr->collect_column_var(colvar_set, include_agg);
  }
}

std::shared_ptr<Analyzer::Expr> FunctionOper::deep_copy() const {
  std::vector<std::shared_ptr<Analyzer::Expr>> args_copy;
  for (size_t i = 0; i < getArity(); ++i) {
    args_copy.push_back(getArg(i)->deep_copy());
  }
  return makeExpr<Analyzer::FunctionOper>(type_info, getName(), args_copy);
}

bool FunctionOper::operator==(const Expr& rhs) const {
  if (type_info != rhs.get_type_info()) {
    return false;
  }
  const auto rhs_func_oper = dynamic_cast<const FunctionOper*>(&rhs);
  if (!rhs_func_oper) {
    return false;
  }
  if (getName() != rhs_func_oper->getName()) {
    return false;
  }
  if (getArity() != rhs_func_oper->getArity()) {
    return false;
  }
  for (size_t i = 0; i < getArity(); ++i) {
    if (!(*getArg(i) == *(rhs_func_oper->getArg(i)))) {
      return false;
    }
  }
  return true;
}

std::string FunctionOper::toString() const {
  std::string str{"(" + name_ + " "};
  for (const auto& arg : args_) {
    str += arg->toString();
  }
  str += ")";
  return str;
}

std::shared_ptr<Analyzer::Expr> FunctionOperWithCustomTypeHandling::deep_copy() const {
  std::vector<std::shared_ptr<Analyzer::Expr>> args_copy;
  for (size_t i = 0; i < getArity(); ++i) {
    args_copy.push_back(getArg(i)->deep_copy());
  }
  return makeExpr<Analyzer::FunctionOperWithCustomTypeHandling>(
      type_info, getName(), args_copy);
}

bool FunctionOperWithCustomTypeHandling::operator==(const Expr& rhs) const {
  if (type_info != rhs.get_type_info()) {
    return false;
  }
  const auto rhs_func_oper =
      dynamic_cast<const FunctionOperWithCustomTypeHandling*>(&rhs);
  if (!rhs_func_oper) {
    return false;
  }
  if (getName() != rhs_func_oper->getName()) {
    return false;
  }
  if (getArity() != rhs_func_oper->getArity()) {
    return false;
  }
  for (size_t i = 0; i < getArity(); ++i) {
    if (!(*getArg(i) == *(rhs_func_oper->getArg(i)))) {
      return false;
    }
  }
  return true;
}

inline JITTypeTag ext_arg_type_to_JIT_type_tag(const ExtArgumentType ext_arg_type) {
  switch (ext_arg_type) {
    case ExtArgumentType::Bool:  // pass thru to Int8
    case ExtArgumentType::Int8:
      return JITTypeTag::INT8;
    case ExtArgumentType::Int16:
      return JITTypeTag::INT16;
    case ExtArgumentType::Int32:
      return JITTypeTag::INT32;
    case ExtArgumentType::Int64:
      return JITTypeTag::INT64;
    case ExtArgumentType::Float:
      return JITTypeTag::FLOAT;
    case ExtArgumentType::Double:
      return JITTypeTag::DOUBLE;
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
      return JITTypeTag::VOID;
    default:
      CHECK(false);
  }
  CHECK(false);
  return JITTypeTag::INVALID;
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

// Generates code which returns true if at least one of the arguments is NULL.
JITValuePointer codegenFunctionOperNullArgForArrow(
    JITFunction& func,
    const Analyzer::FunctionOper* function_oper,
    const std::vector<JITValue*>& orig_arg_lv_nulls) {
  auto one_arg_null = func.createVariable(JITTypeTag::BOOL, "one_arg_null", false);
  size_t physical_coord_cols = 0;
  for (size_t i = 0, j = 0; i < function_oper->getArity();
       ++i, j += std::max(size_t(1), physical_coord_cols)) {
    const auto arg = function_oper->getArg(i);
    const auto& arg_ti = arg->get_type_info();
    physical_coord_cols = arg_ti.get_physical_coord_cols();
    if (arg_ti.get_notnull()) {
      continue;
    }
    CHECK(arg_ti.is_number() or arg_ti.is_boolean());
    one_arg_null = one_arg_null->orOp(*orig_arg_lv_nulls[j]);
  }
  return one_arg_null;
}

std::shared_ptr<Analyzer::Expr> expr_rewrite_to_cast(
    const std::shared_ptr<Analyzer::Expr> orig_expr,
    SQLTypes target_type) {
  return std::make_shared<Analyzer::UOper>(
      SQLTypeInfo(target_type), false, kCAST, orig_expr);
}

JITExprValue& FunctionOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  ExtensionFunction ext_func_sig = [=]() {
    try {
      return bind_function(this);
    } catch (CiderCompileException& e) {
      LOG(WARNING) << "codegenFunctionOper[CPU]: " << e.what();
      throw;
    }
  }();
  const auto& ret_ti = this->get_type_info();
  CHECK(ret_ti.is_integer() || ret_ti.is_fp() || ret_ti.is_boolean());
  auto ret_ty = ext_arg_type_to_JIT_type_tag(ext_func_sig.getRet());
  std::vector<JITValue*> arg_lv_values;
  std::vector<JITValue*> arg_lv_nulls;

  const auto& ext_func_args = ext_func_sig.getArgs();
  for (size_t i = 0; i < this->getArity(); ++i) {
    // target arg type
    const auto ext_func_arg = ext_func_args[i];
    const auto arg_target_ti = ext_arg_type_to_type_info(ext_func_arg);

    // origin arg type
    const auto arg = this->getArg(i);
    const auto arg_ptr = this->getOwnArg(i);
    const auto& arg_ti = arg->get_type_info();

    // Arguments must be converted to the types the extension function can handle.
    Analyzer::Expr* arg_expr = nullptr;
    // need cast
    if (arg_ti.get_type() != arg_target_ti.get_type()) {
      auto arg_casted_ptr = expr_rewrite_to_cast(arg_ptr, arg_target_ti.get_type());
      arg_expr = arg_casted_ptr.get();
      if (!is_rewritten_) {
        rewrote_args_.push_back(arg_casted_ptr);
      } else {
        arg_expr = const_cast<Analyzer::Expr*>(this->getReworteArg(i));
      }
      // no need to cast
    } else {
      arg_expr = const_cast<Analyzer::Expr*>(arg);
      if (!is_rewritten_) {
        rewrote_args_.push_back(arg_ptr);
      }
    }
    auto arg_lv = arg_expr->codegen(context);
    auto arg_lv_fixedsize = FixSizeJITExprValue(arg_lv);
    arg_lv_values.emplace_back(arg_lv_fixedsize.getValue().get());
    arg_lv_nulls.emplace_back(arg_lv_fixedsize.getNull().get());
  }

  // get null value
  JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "ret_null", false);
  bool is_nullable = ext_func_call_requires_nullcheck(this);
  // null is true when at least one argument is null.
  if (is_nullable) {
    null = codegenFunctionOperNullArgForArrow(func, this, arg_lv_nulls);
  }

  // Cast the return of the extension function to match the FunctionOper
  if (!(ret_ti.is_buffer()) && !is_rewritten_) {
    const auto extension_ret_ti = ext_arg_type_to_type_info(ext_func_sig.getRet());
    if (is_nullable && extension_ret_ti.get_type() != this->get_type_info().get_type()) {
      is_rewritten_ = true;
      std::shared_ptr<Analyzer::Expr> this_ptr(nullptr);
      this_ptr.reset(this);
      auto ret = expr_rewrite_to_cast(this_ptr, this->get_type_info().get_type())
                     ->codegen(context);
      auto ret_fixedsize = FixSizeJITExprValue(ret);
      return set_expr_value(ret_fixedsize.getNull(), ret_fixedsize.getValue());
    }
  }
  boost::container::small_vector<JITValue*, 8> args;
  for (auto arg : arg_lv_values) {
    args.push_back(arg);
  }
  auto ext_call = func.emitRuntimeFunctionCall(
      ext_func_sig.getName(),
      JITFunctionEmitDescriptor{.ret_type = ret_ty, .params_vector = args});
  return set_expr_value(null, ext_call);
}

}  // namespace Analyzer
