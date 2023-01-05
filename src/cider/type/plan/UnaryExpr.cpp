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
#include "UnaryExpr.h"
#include "exec/template/Execute.h"

namespace Analyzer {
using namespace cider::jitlib;

void codegenCastOverflowCheck(CodegenContext& context,
                              JITValuePointer operand_val,
                              JITValuePointer null_val,
                              const SQLTypeInfo& operand_ti,
                              const SQLTypeInfo& target_ti) {
  bool integer_ovf =
      target_ti.is_integer() && operand_ti.get_size() >= target_ti.get_size();
  if (context.getCodegenOptions().needs_error_check && integer_ovf) {
    JITFunction& func = *context.getJITFunction();
    auto jit_target_tag = getJITTypeTag(target_ti.get_type());
    auto jit_source_tag = getJITTypeTag(operand_ti.get_type());
    JITValuePointer type_max_val =
        func.createLiteral(jit_target_tag, get_max_value(target_ti.get_type()))
            ->castJITValuePrimitiveType(jit_source_tag);
    JITValuePointer type_min_val =
        func.createLiteral(jit_target_tag, get_min_value(target_ti.get_type()))
            ->castJITValuePrimitiveType(jit_source_tag);
    // overflow check before cast
    func.createIfBuilder()
        ->condition([&]() {
          return !null_val && (operand_val > type_max_val || operand_val <= type_min_val);
        })
        ->ifTrue([&]() {
          func.createReturn(func.createLiteral(JITTypeTag::INT32,
                                               ERROR_CODE::ERR_OVERFLOW_OR_UNDERFLOW));
        })
        ->build();
  }
}
JITValuePointer codegenCastBetweenDateAndTime(CodegenContext& context,
                                              JITValuePointer operand_val,
                                              const SQLTypeInfo& operand_ti,
                                              const SQLTypeInfo& ti) {
  JITFunction& func = *context.getJITFunction();
  const int64_t operand_width = getTypeBytes(operand_ti.get_type());
  const int64_t target_width = getTypeBytes(ti.get_type());
  int64_t dim_scaled = DateTimeUtils::get_timestamp_precision_scale(
      abs(operand_ti.get_dimension() - ti.get_dimension()));
  JITValuePointer cast_scaled =
      func.createLiteral(JITTypeTag::INT64, dim_scaled * kSecondsInOneDay);
  if (target_width == operand_width) {
    return operand_val;
  } else if (target_width > operand_width) {
    JITValuePointer cast_val =
        operand_val->castJITValuePrimitiveType(getJITTypeTag(ti.get_type()));
    return cast_val * cast_scaled;
  } else {
    JITValuePointer trunc_val = func.emitRuntimeFunctionCall(
        "floor_div_lhs",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT64,
            .params_vector = {operand_val.get(), cast_scaled.get()}});
    return trunc_val->castJITValuePrimitiveType(getJITTypeTag(ti.get_type()));
  }
}

JITValuePointer codegenCastBetweenTime(CodegenContext& context,
                                       JITValuePointer operand_val,
                                       const SQLTypeInfo& operand_ti,
                                       const SQLTypeInfo& target_ti) {
  JITFunction& func = *context.getJITFunction();
  const auto operand_dimen = operand_ti.get_dimension();
  const auto target_dimen = target_ti.get_dimension();
  JITValuePointer cast_scaled = func.createLiteral(
      JITTypeTag::INT64,
      DateTimeUtils::get_timestamp_precision_scale(abs(operand_dimen - target_dimen)));
  if (operand_dimen == target_dimen) {
    return operand_val;
  } else if (operand_dimen < target_dimen) {
    return operand_val * cast_scaled;
  } else {
    return func.emitRuntimeFunctionCall(
        "floor_div_lhs",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT64,
            .params_vector = {operand_val.get(), cast_scaled.get()}});
  }
}

JITExprValue& UOper::codegen(CodegenContext& context) {
  if (auto& expr_var = get_expr_value()) {
    return expr_var;
  }
  auto operand = const_cast<Analyzer::Expr*>(get_operand());
  if (is_unnest(operand) || is_unnest(operand)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in UOper");
  }
  const auto& operand_ti = operand->get_type_info();
  if (operand_ti.is_decimal()) {
    CIDER_THROW(CiderCompileException, "Decimal not supported in Uoper codegen now.");
  }

  switch (get_optype()) {
    case kISNULL:
    case kISNOTNULL: {
      return codegenIsNull(context, operand, get_optype());
    }
    case kNOT: {
      return codegenNot(context, operand);
    }
    case kCAST: {
      return codegenCast(context, operand);
    }
    case kUMINUS: {
      return codegenUminus(context, operand);
    }
    default:
      UNIMPLEMENTED();
  }
}

JITExprValue& UOper::codegenIsNull(CodegenContext& context,
                                   Analyzer::Expr* operand,
                                   SQLOps optype) {
  JITFunction& func = *context.getJITFunction();
  // For ISNULL, the null will always be false
  const auto& ti = get_type_info();
  const auto type = ti.is_decimal() ? decimal_to_int_type(ti) : ti.get_type();

  // for IS NULL / IS NOT NULL, we only need the null info (getNull())
  JITExprValueAdaptor operand_val(operand->codegen(context));

  if (optype == kISNOTNULL) {
    // is not null
    auto null_value = operand_val.getNull()->notOp();
    return set_expr_value(func.createLiteral(getJITTag(type), false), null_value);
  } else {
    // is null
    auto null_value = operand_val.getNull();
    return set_expr_value(func.createLiteral(getJITTag(type), false), null_value);
  }
}

JITExprValue& UOper::codegenNot(CodegenContext& context, Analyzer::Expr* operand) {
  const auto& ti = get_type_info();

  // should be bool, or otherwise will throw in notOp()
  FixSizeJITExprValue operand_val(operand->codegen(context));

  CHECK(operand_val.getNull().get());
  return set_expr_value(operand_val.getNull(), operand_val.getValue()->notOp());
}

JITExprValue& UOper::codegenUminus(CodegenContext& context, Analyzer::Expr* operand) {
  // should be fixedSize type
  FixSizeJITExprValue operand_val(operand->codegen(context));
  CHECK(operand_val.getNull().get());
  return set_expr_value(operand_val.getNull(), -operand_val.getValue());
}

JITValuePointer codegenCastFixedSize(CodegenContext& context,
                                     JITValuePointer operand_val,
                                     const SQLTypeInfo& operand_ti,
                                     const SQLTypeInfo& target_ti) {
  JITFunction& func = *context.getJITFunction();
  JITTypeTag ti_jit_tag = getJITTypeTag(target_ti.get_type());
  if (operand_ti.get_type() == kDATE || target_ti.get_type() == kDATE) {
    return codegenCastBetweenDateAndTime(context, operand_val, operand_ti, target_ti);
  } else if (operand_ti.get_type() == kTIMESTAMP && target_ti.get_type() == kTIMESTAMP) {
    return codegenCastBetweenTime(context, operand_val, operand_ti, target_ti);
  } else if (operand_ti.is_integer()) {
    CHECK(target_ti.is_fp() || target_ti.is_integer());
    return operand_val->castJITValuePrimitiveType(ti_jit_tag);
  } else if (operand_ti.is_fp()) {
    if (target_ti.is_fp()) {
      return operand_val->castJITValuePrimitiveType(ti_jit_tag);
    }
    CHECK(target_ti.is_integer());
    // Round by adding/subtracting 0.5 before fptosi.
    auto round_val =
        func.createVariable(getJITTypeTag(operand_ti.get_type()), "fp_round_val");
    func.createIfBuilder()
        ->condition([&]() { return operand_val < 0; })
        ->ifTrue([&]() { round_val = operand_val - 0.5; })
        ->ifFalse([&]() { round_val = operand_val + 0.5; })
        ->build();
    return round_val->castJITValuePrimitiveType(ti_jit_tag);
  }
  CIDER_THROW(CiderCompileException,
              fmt::format("cast type:{} into type:{} not support yet",
                          operand_ti.get_type_name(),
                          target_ti.get_type_name()));
}

JITValuePointer codegenCastNumericToString(CodegenContext& context,
                                           JITValuePointer operand_val,
                                           JITValuePointer string_heap_val,
                                           const SQLTypeInfo& operand_ti) {
  JITFunction& func = *context.getJITFunction();
  std::string fn_call{"gen_string_from_"};
  auto args_desc = JITFunctionEmitDescriptor{
      .ret_type = JITTypeTag::INT64,
      .params_vector = {operand_val.get(), string_heap_val.get()}};
  auto dim_val = func.createLiteral(JITTypeTag::INT32, operand_ti.get_dimension());
  const auto operand_type = operand_ti.get_type();
  switch (operand_type) {
    case kBOOLEAN:
      fn_call += "bool";
      break;
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT:
    case kFLOAT:
    case kDOUBLE:
      fn_call += to_lower(toString(operand_type));
      break;
    case kTIME:
      fn_call += "time";
      args_desc.params_vector.push_back(dim_val.get());
      break;
    case kTIMESTAMP:
      fn_call += "timestamp";
      args_desc.params_vector.push_back(dim_val.get());
      break;
    case kDATE:
      fn_call += "date";
      break;
    default:
      CIDER_THROW(CiderCompileException,
                  "Unimplemented type for string cast from " + toString(operand_type));
  }
  return func.emitRuntimeFunctionCall(fn_call, args_desc);
}

JITValuePointer codegenCastStringToNumeric(CodegenContext& context,
                                           JITValuePointer str_val,
                                           JITValuePointer str_len,
                                           const SQLTypeInfo& target_ti) {
  JITFunction& func = *context.getJITFunction();
  std::string func_call = "convert_string_to_";
  auto args_desc =
      JITFunctionEmitDescriptor{.ret_type = getJITTypeTag(target_ti.get_type()),
                                .params_vector = {str_val.get(), str_len.get()}};
  auto dim_val = func.createLiteral(JITTypeTag::INT32, target_ti.get_dimension());
  const auto target_type = target_ti.get_type();
  switch (target_type) {
    case kBOOLEAN:
      func_call += "bool";
      break;
    case kBIGINT:
      func_call += "bigint";
      break;
    case kINT:
      func_call += "int";
      break;
    case kSMALLINT:
      func_call += "smallint";
      break;
    case kTINYINT:
      func_call += "tinyint";
      break;
    case kFLOAT:
      func_call += "float";
      break;
    case kDOUBLE:
      func_call += "double";
      break;
    case kTIME:
      func_call += "time";
      args_desc.params_vector.push_back(dim_val.get());
      break;
    case kTIMESTAMP:
      func_call += "timestamp";
      args_desc.params_vector.push_back(dim_val.get());
      break;
    case kDATE:
      func_call += "date";
      break;
    default:
      CIDER_THROW(CiderCompileException,
                  "Unimplemented type for string cast to " + toString(target_type));
  }
  return func.emitRuntimeFunctionCall(func_call, args_desc);
}

JITExprValue& UOper::codegenCast(CodegenContext& context, Analyzer::Expr* operand) {
  JITFunction& func = *context.getJITFunction();
  const auto& target_ti = get_type_info();
  const auto& operand_ti = operand->get_type_info();
  if (operand_ti.is_string() && target_ti.is_string()) {
    // only supports casting from varchar to varchar
    VarSizeJITExprValue operand_val(operand->codegen(context));
    return set_expr_value(
        operand_val.getNull(), operand_val.getLength(), operand_val.getValue());
  } else if (target_ti.is_string()) {
    FixSizeJITExprValue operand_val(operand->codegen(context));
    auto string_heap_ptr = func.emitRuntimeFunctionCall(
        "get_query_context_string_heap_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .ret_sub_type = JITTypeTag::INT8,
                                  .params_vector = {func.getArgument(0).get()}});
    JITValuePointer ptr_and_len = codegenCastNumericToString(
        context, operand_val.getValue(), string_heap_ptr, operand_ti);
    auto ret_ptr = func.emitRuntimeFunctionCall(
        "extract_str_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .params_vector = {ptr_and_len.get()}});
    auto ret_len = func.emitRuntimeFunctionCall(
        "extract_str_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {ptr_and_len.get()}});
    return set_expr_value(operand_val.getNull(), ret_len, ret_ptr);
  } else if (operand_ti.is_string()) {
    auto str_val = VarSizeJITExprValue(operand->codegen(context));
    return set_expr_value(
        str_val.getNull(),
        codegenCastStringToNumeric(
            context, str_val.getValue(), str_val.getLength(), target_ti));
  } else {
    // cast between numeric type
    // If arg type is same as target type, erase the cast directly
    if (get_type_info().get_type() == get_operand()->get_type_info().get_type()) {
      return set_expr_value(operand_val.getNull(), operand_val.getValue());
    }
    FixSizeJITExprValue operand_val(operand->codegen(context));
    codegenCastOverflowCheck(
        context, operand_val.getValue(), operand_val.getNull(), operand_ti, target_ti);
    return set_expr_value(
        operand_val.getNull(),
        codegenCastFixedSize(context, operand_val.getValue(), operand_ti, target_ti));
  }
}

std::shared_ptr<Analyzer::Expr> UOper::deep_copy() const {
  return makeExpr<UOper>(type_info, contains_agg, optype, operand->deep_copy());
}

std::shared_ptr<Analyzer::Expr> UOper::add_cast(const SQLTypeInfo& new_type_info) {
  if (optype != kCAST) {
    return Expr::add_cast(new_type_info);
  }
  if (type_info.is_string() && new_type_info.is_string() &&
      new_type_info.get_compression() == kENCODING_DICT &&
      type_info.get_compression() == kENCODING_NONE) {
    const SQLTypeInfo oti = operand->get_type_info();
    if (oti.is_string() && oti.get_compression() == kENCODING_DICT &&
        (oti.get_comp_param() == new_type_info.get_comp_param() ||
         oti.get_comp_param() == TRANSIENT_DICT(new_type_info.get_comp_param()))) {
      auto result = operand;
      operand = nullptr;
      return result;
    }
  }
  return Expr::add_cast(new_type_info);
}

void UOper::check_group_by(
    const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const {
  operand->check_group_by(groupby);
}

void UOper::group_predicates(std::list<const Expr*>& scan_predicates,
                             std::list<const Expr*>& join_predicates,
                             std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  operand->collect_rte_idx(rte_idx_set);
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}

bool UOper::operator==(const Expr& rhs) const {
  if (typeid(rhs) != typeid(UOper)) {
    return false;
  }
  const UOper& rhs_uo = dynamic_cast<const UOper&>(rhs);
  return optype == rhs_uo.get_optype() && *operand == *rhs_uo.get_operand();
}

std::string UOper::toString() const {
  std::string op;
  switch (optype) {
    case kNOT:
      op = "NOT ";
      break;
    case kUMINUS:
      op = "- ";
      break;
    case kISNULL:
      op = "IS NULL ";
      break;
    case kEXISTS:
      op = "EXISTS ";
      break;
    case kCAST:
      op = "CAST " + type_info.get_type_name() + "(" +
           std::to_string(type_info.get_precision()) + "," +
           std::to_string(type_info.get_scale()) + ") " +
           type_info.get_compression_name() + "(" +
           std::to_string(type_info.get_comp_param()) + ") ";
      break;
    case kUNNEST:
      op = "UNNEST ";
      break;
    default:
      break;
  }
  return "(" + op + operand->toString() + ") ";
}

void UOper::find_expr(bool (*f)(const Expr*), std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  operand->find_expr(f, expr_list);
}

}  // namespace Analyzer
