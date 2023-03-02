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

#include "type/plan/StringOpExpr.h"

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/base/JITValue.h"

namespace Analyzer {
using namespace cider::exec::nextgen;
using LiteralArgMap = std::map<size_t, std::pair<SQLTypes, Datum>>;

LiteralArgMap StringOper::getLiteralArgs() const {
  LiteralArgMap literal_arg_map;
  const auto num_args = getArity();
  for (size_t idx = 0; idx < num_args; ++idx) {
    const auto constant_arg_expr = dynamic_cast<const Analyzer::Constant*>(getArg(idx));
    if (constant_arg_expr) {
      literal_arg_map.emplace(
          std::make_pair(idx,
                         std::make_pair(constant_arg_expr->get_type_info().get_type(),
                                        constant_arg_expr->get_constval())));
    }
  }
  return literal_arg_map;
}

// StringOper Base Class

void StringOper::group_predicates(std::list<const Expr*>& scan_predicates,
                                  std::list<const Expr*>& join_predicates,
                                  std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  for (const auto& arg : args_) {
    arg->collect_rte_idx(rte_idx_set);
  }
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}

std::shared_ptr<Analyzer::Expr> StringOper::rewrite_with_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::vector<std::shared_ptr<Analyzer::Expr>> rewritten_args;
  for (const auto& arg : args_) {
    rewritten_args.emplace_back(arg->rewrite_with_targetlist(tlist));
  }
  return makeExpr<StringOper>(kind_, rewritten_args);
}

std::shared_ptr<Analyzer::Expr> StringOper::rewrite_with_child_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::vector<std::shared_ptr<Analyzer::Expr>> rewritten_args;
  for (const auto& arg : args_) {
    rewritten_args.emplace_back(arg->rewrite_with_child_targetlist(tlist));
  }
  return makeExpr<StringOper>(kind_, rewritten_args);
}

std::shared_ptr<Analyzer::Expr> StringOper::rewrite_agg_to_var(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::vector<std::shared_ptr<Analyzer::Expr>> rewritten_args;
  for (const auto& arg : args_) {
    rewritten_args.emplace_back(arg->rewrite_agg_to_var(tlist));
  }
  return makeExpr<StringOper>(kind_, rewritten_args);
}

void StringOper::find_expr(bool (*f)(const Expr*),
                           std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  for (const auto& arg : args_) {
    arg->find_expr(f, expr_list);
  }
}
void StringOper::collect_rte_idx(std::set<int>& rte_idx_set) const {
  for (const auto& arg : args_) {
    arg->collect_rte_idx(rte_idx_set);
  }
}

void StringOper::collect_column_var(
    std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>& colvar_set,
    bool include_agg) const {
  for (const auto& arg : args_) {
    arg->collect_column_var(colvar_set, include_agg);
  }
}

std::shared_ptr<Analyzer::Expr> StringOper::deep_copy() const {
  std::vector<std::shared_ptr<Analyzer::Expr>> args_copy;
  for (const auto& arg : args_) {
    args_copy.emplace_back(arg->deep_copy());
  }
  std::vector<std::shared_ptr<Analyzer::Expr>> chained_string_op_exprs_copy;
  for (const auto& chained_string_op_expr : chained_string_op_exprs_) {
    chained_string_op_exprs_copy.emplace_back(chained_string_op_expr->deep_copy());
  }
  return makeExpr<Analyzer::StringOper>(kind_,
                                        get_type_info(),
                                        std::move(args_copy),
                                        std::move(chained_string_op_exprs_copy));
}

bool StringOper::operator==(const Expr& rhs) const {
  const auto rhs_string_oper = dynamic_cast<const StringOper*>(&rhs);

  if (!rhs_string_oper) {
    return false;
  }

  if (get_kind() != rhs_string_oper->get_kind()) {
    return false;
  }
  if (getArity() != rhs_string_oper->getArity()) {
    return false;
  }

  for (size_t i = 0; i < getArity(); ++i) {
    if (!(*getArg(i) == *(rhs_string_oper->getArg(i)))) {
      return false;
    }
  }
  if (chained_string_op_exprs_.size() !=
      rhs_string_oper->chained_string_op_exprs_.size()) {
    return false;
  }
  for (size_t i = 0; i < chained_string_op_exprs_.size(); ++i) {
    if (!(*(chained_string_op_exprs_[i]) ==
          *(rhs_string_oper->chained_string_op_exprs_[i]))) {
      return false;
    }
  }
  return true;
}

std::string StringOper::toString() const {
  std::string str{"(" + ::toString(kind_) + " "};
  for (const auto& arg : args_) {
    str += arg->toString();
  }
  str += ")";
  return str;
}

bool StringOper::hasSingleDictEncodedColInput() const {
  auto comparator = Analyzer::ColumnVar::colvar_comp;
  std::set<const Analyzer::ColumnVar*,
           bool (*)(const Analyzer::ColumnVar*, const Analyzer::ColumnVar*)>
      colvar_set(comparator);
  collect_column_var(colvar_set, true);
  if (colvar_set.size() != 1UL) {
    return false;
  }
  auto col_expr_ptr = *colvar_set.begin();
  CHECK(col_expr_ptr);
  return col_expr_ptr->get_type_info().is_dict_encoded_string();
}

std::vector<size_t> StringOper::getLiteralArgIndexes() const {
  std::vector<size_t> literal_arg_indexes;
  const auto num_args = args_.size();
  for (size_t idx = 0; idx < num_args; ++idx) {
    if (dynamic_cast<const Analyzer::Constant*>(args_[idx].get())) {
      literal_arg_indexes.emplace_back(idx);
    }
  }
  return literal_arg_indexes;
}

SQLTypeInfo StringOper::get_return_type(
    const SqlStringOpKind kind,
    const std::vector<std::shared_ptr<Analyzer::Expr>>& args) {
  CHECK_NE(kind, SqlStringOpKind::TRY_STRING_CAST)
      << "get_return_type for TRY_STRING_CAST disallowed.";
  if (kind == SqlStringOpKind::CHAR_LENGTH) {
    // ret-type of char_length should be int64
    return SQLTypeInfo(kBIGINT, args[0]->get_type_info().get_notnull());
  }
  if (args.empty()) {
    return SQLTypeInfo(kNULLT);
  } else if (dynamic_cast<const Analyzer::Constant*>(args[0].get())) {
    // Constant literal first argument
    return args[0]->get_type_info();
  } else if (args[0]->get_type_info().is_none_encoded_string()) {
    // None-encoded text column argument
    // Note that whether or not this is allowed is decided separately
    // in check_operand_types
    // If here, we have a dict-encoded column arg
    return SQLTypeInfo(
        kTEXT, 0, 0, args[0]->get_type_info().get_notnull(), kENCODING_DICT, 0, kNULLT);
  } else {
    return SQLTypeInfo(args[0]->get_type_info());  // nullable by default
  }
}

void StringOper::check_operand_types(
    const size_t min_args,
    const std::vector<OperandTypeFamily>& expected_type_families,
    const std::vector<std::string>& arg_names,
    const bool dict_encoded_cols_only,
    const bool cols_first_arg_only) const {
  std::ostringstream oss;
  const size_t num_args = args_.size();
  CHECK_EQ(expected_type_families.size(), arg_names.size());
  if (num_args < min_args || num_args > expected_type_families.size()) {
    oss << "Error instantiating " << ::toString(get_kind()) << " operator. ";
    oss << "Expected " << expected_type_families.size() << " arguments, but received "
        << num_args << ".";
  }
  for (size_t arg_idx = 0; arg_idx < num_args; ++arg_idx) {
    const auto& expected_type_family = expected_type_families[arg_idx];
    // We need to remove any casts that Calcite may add to try the right operand type,
    // even if we don't support them. Need to check how this works with casts we do
    // support.
    auto arg_ti = args_[arg_idx]->get_type_info();
    const auto decasted_arg = remove_cast(args_[arg_idx]);
    const bool is_arg_constant =
        dynamic_cast<const Analyzer::Constant*>(decasted_arg.get()) != nullptr;
    const bool is_arg_column_var =
        dynamic_cast<const Analyzer::ColumnVar*>(decasted_arg.get()) != nullptr;
    const bool is_arg_string_oper =
        dynamic_cast<const Analyzer::StringOper*>(decasted_arg.get()) != nullptr;
    if (!(is_arg_constant || is_arg_column_var || is_arg_string_oper)) {
      oss << "Error instantiating " << ::toString(get_kind()) << " operator. "
          << "Currently only constant, column, or other string operator arguments "
          << "are allowed as inputs.";
      CIDER_THROW(CiderCompileException, oss.str());
    }
    auto decasted_arg_ti = decasted_arg->get_type_info();
    // We need to prevent any non-string type from being casted to a string, but can
    // permit non-integer types being casted to integers Todo: Find a cleaner way to
    // handle this (we haven't validated any of the casts that calcite has given us at the
    // point of RelAlgTranslation)
    if (arg_ti != decasted_arg_ti &&
        ((arg_ti.is_string() && !decasted_arg_ti.is_string()) ||
         (arg_ti.is_integer() && decasted_arg_ti.is_string()))) {
      arg_ti = decasted_arg_ti;
    }

    if (cols_first_arg_only && !is_arg_constant && arg_idx >= 1UL) {
      if (get_kind() != SqlStringOpKind::CONCAT) {
        oss << "Error instantiating " << ::toString(get_kind()) << " operator. "
            << "Currently only column inputs allowed for the primary argument, "
            << "but a column input was received for argument " << arg_idx + 1 << ".";
        CIDER_THROW(CiderCompileException, oss.str());
      }
    }
    switch (expected_type_family) {
      case OperandTypeFamily::STRING_FAMILY: {
        // do not check currently
        break;
      }
      case OperandTypeFamily::INT_FAMILY: {
        if (!IS_INTEGER(arg_ti.get_type())) {
          oss << "Error instantiating " << ::toString(get_kind()) << " operator. "
              << "Expected integer type for argument " << arg_idx + 1 << " ("
              << arg_names[arg_idx] << ").";
          CIDER_THROW(CiderCompileException, oss.str());
          break;
        }
        if (!is_arg_constant) {
          oss << "Error instantiating " << ::toString(get_kind()) << " operator. "
              << "Currently only text-encoded dictionary column inputs are "
              << "allowed, but an integer-type column was provided.";
          CIDER_THROW(CiderCompileException, oss.str());
          break;
        }
        break;
      }
    }
  }
}

// SubstringStringOper: SUBSTRING
std::shared_ptr<Analyzer::Expr> SubstringStringOper::deep_copy() const {
  return makeExpr<Analyzer::SubstringStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& SubstringStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // 1. decode parameters
  auto arg = const_cast<Analyzer::Expr*>(getArg(0));
  auto pos = const_cast<Analyzer::Expr*>(getArg(1));
  auto len = const_cast<Analyzer::Expr*>(getArg(2));

  CHECK(arg->get_type_info().is_string());

  auto arg_val = VarSizeJITExprValue(arg->codegen(context));
  auto pos_val = FixSizeJITExprValue(pos->codegen(context));
  auto len_val = FixSizeJITExprValue(len->codegen(context));

  auto if_builder = func.createIfBuilder();
  std::string fn_name = "cider_substring_extra";
  if (isOutput()) {
    auto ret_ptr_and_len = func.createVariable(JITTypeTag::INT64, "ptr_and_len", 0);
    if_builder
        ->condition([&]() {
          auto condition = arg_val.getNull();
          return condition;
        })
        ->ifFalse([&]() {
          // format parameters
          auto pos_param = func.emitRuntimeFunctionCall(
              "format_substring_pos",
              JITFunctionEmitDescriptor{
                  .ret_type = JITTypeTag::INT32,
                  .params_vector = {pos_val.getValue()
                                        ->castJITValuePrimitiveType(JITTypeTag::INT32)
                                        .get(),
                                    arg_val.getLength().get()}});
          auto len_param = func.emitRuntimeFunctionCall(
              "format_substring_len",
              JITFunctionEmitDescriptor{
                  .ret_type = JITTypeTag::INT32,
                  .params_vector = {pos_param.get(),
                                    arg_val.getLength().get(),
                                    len_val.getValue()
                                        ->castJITValuePrimitiveType(JITTypeTag::INT32)
                                        .get()}});

          // get string heap ptr
          auto string_heap_ptr = func.emitRuntimeFunctionCall(
              "get_query_context_string_heap_ptr",
              JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                        .ret_sub_type = JITTypeTag::INT8,
                                        .params_vector = {func.getArgument(0).get()}});

          auto emit_desc =
              JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT64,
                                        .params_vector = {string_heap_ptr.get(),
                                                          arg_val.getValue().get(),
                                                          pos_param.get(),
                                                          len_param.get()}};
          *ret_ptr_and_len = *func.emitRuntimeFunctionCall(fn_name, emit_desc);
        })
        ->build();
    return set_expr_value(arg_val.getNull(), ret_ptr_and_len);
  } else {
    auto ret_ptr_int64 = func.createVariable(JITTypeTag::INT64);
    auto ret_len = func.createVariable(JITTypeTag::INT32);
    if_builder
        ->condition([&]() {
          auto condition = arg_val.getNull();
          return condition;
        })
        ->ifFalse([&]() {
          // format parameters
          auto pos_param = func.emitRuntimeFunctionCall(
              "format_substring_pos",
              JITFunctionEmitDescriptor{
                  .ret_type = JITTypeTag::INT32,
                  .params_vector = {pos_val.getValue()
                                        ->castJITValuePrimitiveType(JITTypeTag::INT32)
                                        .get(),
                                    arg_val.getLength().get()}});
          auto len_param = func.emitRuntimeFunctionCall(
              "format_substring_len",
              JITFunctionEmitDescriptor{
                  .ret_type = JITTypeTag::INT32,
                  .params_vector = {pos_param.get(),
                                    arg_val.getLength().get(),
                                    len_val.getValue()
                                        ->castJITValuePrimitiveType(JITTypeTag::INT32)
                                        .get()}});

          // get string heap ptr
          auto string_heap_ptr = func.emitRuntimeFunctionCall(
              "get_query_context_string_heap_ptr",
              JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                        .ret_sub_type = JITTypeTag::INT8,
                                        .params_vector = {func.getArgument(0).get()}});
          *ret_len = *len_param;

          auto ret_ptr = func.emitRuntimeFunctionCall(
              fn_name + "_ptr",
              JITFunctionEmitDescriptor{
                  .ret_type = JITTypeTag::POINTER,
                  .params_vector = {arg_val.getValue().get(), pos_param.get()}});
          *ret_ptr_int64 = *func.emitRuntimeFunctionCall(
              "cast_ptr_to_int64",
              JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT64,
                                        .params_vector = {ret_ptr.get()}});
        })
        ->build();
    return set_expr_value(
        arg_val.getNull(),
        ret_len,
        func.emitRuntimeFunctionCall(
            "cast_int64_to_ptr",
            JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                      .params_vector = {ret_ptr_int64.get()}}));
  }
}

// LowerStringOper: LOWER
std::shared_ptr<Analyzer::Expr> LowerStringOper::deep_copy() const {
  return makeExpr<Analyzer::LowerStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& LowerStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode parameters
  auto arg = const_cast<Analyzer::Expr*>(getArg(0));
  CHECK(arg->get_type_info().is_string());
  auto arg_val = VarSizeJITExprValue(arg->codegen(context));

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name = "cider_ascii_lower";

  if (isOutput()) {
    // call external function
    auto emit_desc = JITFunctionEmitDescriptor{
        .ret_type = JITTypeTag::INT64,
        .params_vector = {
            string_heap_ptr.get(), arg_val.getValue().get(), arg_val.getLength().get()}};
    auto ptr_and_len = func.emitRuntimeFunctionCall(fn_name, emit_desc);

    return set_expr_value(arg_val.getNull(), ptr_and_len);
  } else {
    auto ret_len = func.emitRuntimeFunctionCall(
        fn_name + "_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {arg_val.getLength().get()}});
    auto ptr = func.emitRuntimeFunctionCall(
        "allocate_from_string_heap",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .params_vector = {string_heap_ptr.get(), ret_len.get()}});
    auto ret_ptr = func.emitRuntimeFunctionCall(
        fn_name + "_ptr",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::VOID,
            .params_vector = {
                ptr.get(), arg_val.getValue().get(), arg_val.getLength().get()}});
    return set_expr_value(arg_val.getNull(), ret_len, ptr);
  }
}

// UpperStringOper: UPPER
std::shared_ptr<Analyzer::Expr> UpperStringOper::deep_copy() const {
  return makeExpr<Analyzer::UpperStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& UpperStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode parameters
  auto arg = const_cast<Analyzer::Expr*>(getArg(0));
  CHECK(arg->get_type_info().is_string());
  auto arg_val = VarSizeJITExprValue(arg->codegen(context));

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name = "cider_ascii_upper";

  if (isOutput()) {
    // call external function
    auto emit_desc = JITFunctionEmitDescriptor{
        .ret_type = JITTypeTag::INT64,
        .params_vector = {
            string_heap_ptr.get(), arg_val.getValue().get(), arg_val.getLength().get()}};
    auto ptr_and_len = func.emitRuntimeFunctionCall(fn_name, emit_desc);

    return set_expr_value(arg_val.getNull(), ptr_and_len);
  } else {
    auto ret_len = func.emitRuntimeFunctionCall(
        fn_name + "_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {arg_val.getLength().get()}});
    auto ptr = func.emitRuntimeFunctionCall(
        "allocate_from_string_heap",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .params_vector = {string_heap_ptr.get(), ret_len.get()}});
    auto ret_ptr = func.emitRuntimeFunctionCall(
        fn_name + "_ptr",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::VOID,
            .params_vector = {
                ptr.get(), arg_val.getValue().get(), arg_val.getLength().get()}});
    return set_expr_value(arg_val.getNull(), ret_len, ptr);
  }
}

// CharLengthStringOp: CHAR_LENGTH
std::shared_ptr<Analyzer::Expr> CharLengthStringOper::deep_copy() const {
  return makeExpr<Analyzer::CharLengthStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& CharLengthStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode parameters
  auto arg = const_cast<Analyzer::Expr*>(getArg(0));
  CHECK(arg->get_type_info().is_string());
  auto arg_val = VarSizeJITExprValue(arg->codegen(context));

  // directly return str_len, but need to cast it to INT64
  return set_expr_value(
      arg_val.getNull(),
      arg_val.getLength()->castJITValuePrimitiveType(JITTypeTag::INT64));
}

// ConcatStringOper: CONCAT
std::shared_ptr<Analyzer::Expr> ConcatStringOper::deep_copy() const {
  return makeExpr<Analyzer::ConcatStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& ConcatStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode input args
  auto lhs = const_cast<Analyzer::Expr*>(getArg(0));
  auto rhs = const_cast<Analyzer::Expr*>(getArg(1));

  CHECK(lhs->get_type_info().is_string());
  CHECK(rhs->get_type_info().is_string());

  auto lhs_val = VarSizeJITExprValue(lhs->codegen(context));
  auto rhs_val = VarSizeJITExprValue(rhs->codegen(context));

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name =
      get_kind() == SqlStringOpKind::CONCAT ? "cider_concat" : "cider_rconcat";
  if (isOutput()) {
    // call external function
    auto emit_desc =
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT64,
                                  .params_vector = {string_heap_ptr.get(),
                                                    lhs_val.getValue().get(),
                                                    lhs_val.getLength().get(),
                                                    rhs_val.getValue().get(),
                                                    rhs_val.getLength().get()}};

    auto ptr_and_len = func.emitRuntimeFunctionCall(fn_name, emit_desc);
    return set_expr_value(lhs_val.getNull() || rhs_val.getNull(), ptr_and_len);
  } else {
    // TODO (YBRua): deprecate cider_rconcat after full migration to nextgen
    // rconcat is a workaround used in template codegen for cases such as constant || var

    auto ret_len = func.emitRuntimeFunctionCall(
        fn_name + "_len",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT32,
            .params_vector = {lhs_val.getLength().get(), rhs_val.getLength().get()}});
    auto ptr = func.emitRuntimeFunctionCall(
        "allocate_from_string_heap",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .params_vector = {string_heap_ptr.get(), ret_len.get()}});
    auto ret_ptr = func.emitRuntimeFunctionCall(
        fn_name + "_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::VOID,
                                  .params_vector = {ptr.get(),
                                                    lhs_val.getValue().get(),
                                                    lhs_val.getLength().get(),
                                                    rhs_val.getValue().get(),
                                                    rhs_val.getLength().get()}});
    return set_expr_value(lhs_val.getNull() || rhs_val.getNull(), ret_len, ptr);
  }
}  // namespace Analyzer

// to be deprecated. Can be removed after full migration to nextgen
bool ConcatStringOper::isLiteralOrCastLiteral(const Analyzer::Expr* operand) {
  // literals may exist in a CAST op (casted from fixedchar to varchar)
  auto literal_arg = dynamic_cast<const Analyzer::Constant*>(remove_cast(operand));
  if (literal_arg) {
    // is a literal or a casted literal
    return true;
  }
  return false;
}

// to be deprecated. Can be removed after full migration to nextgen.
// Anything else related with RCONCAT can also be removed after migration to nextgen
SqlStringOpKind ConcatStringOper::getConcatOpKind(
    const std::vector<std::shared_ptr<Analyzer::Expr>>& operands) {
  CHECK_EQ(operands.size(), 2);
  auto is_constant_arg0 = isLiteralOrCastLiteral(operands[0].get());
  auto is_constant_arg1 = isLiteralOrCastLiteral(operands[1].get());

  if (is_constant_arg1) {
    // concat(col, literal) or concat(literal, literal)
    return SqlStringOpKind::CONCAT;
  } else if (is_constant_arg0) {
    // concat(literal, col)
    return SqlStringOpKind::RCONCAT;
  } else {
    return SqlStringOpKind::CONCAT;
    // NOTE: (YBRua) the error check here is not needed in nextgen, because nextgen
    // supports concatenating two variable inputs. However, templated codegen still does
    // not support two variable inputs and will have errors in these cases
    // CIDER_THROW(CiderCompileException,
    //             "concat() currently does not support two variable operands.");
  }
}

// to be deprecated. Can be removed after full migration to nextgen
std::vector<std::shared_ptr<Analyzer::Expr>> ConcatStringOper::rearrangeOperands(
    const std::vector<std::shared_ptr<Analyzer::Expr>>& operands) {
  // ensures non-literal operand (if any) is not at arg1
  // as stringops expect non-literals to be the first arg at runtime
  CHECK_EQ(operands.size(), 2);
  auto is_constant_arg0 = isLiteralOrCastLiteral(operands[0].get());
  auto is_constant_arg1 = isLiteralOrCastLiteral(operands[1].get());

  if (is_constant_arg1) {
    // concat(col, literal) or concat(literal, literal)
    return {operands[0], remove_cast(operands[1].get())->deep_copy()};
  } else if (is_constant_arg0) {
    // concat(literal, col)
    return {operands[1], remove_cast(operands[0].get())->deep_copy()};
  } else {
    return operands;
    // error check is disabled for the same reason as above
    // CIDER_THROW(CiderCompileException,
    //             "concat() currently does not support two variable operands.");
  }
}

// TrimStringOper: LTRIM / TRIM / RTRIM
std::shared_ptr<Analyzer::Expr> TrimStringOper::deep_copy() const {
  return makeExpr<Analyzer::TrimStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& TrimStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode input args
  auto input = const_cast<Analyzer::Expr*>(getArg(0));
  auto trim_char = const_cast<Analyzer::Expr*>(getArg(1));

  CHECK(input->get_type_info().is_string());
  CHECK(trim_char->get_type_info().is_string());

  auto trim_char_literal = dynamic_cast<Analyzer::Constant*>(trim_char);
  if (!trim_char_literal) {
    CIDER_THROW(CiderUnsupportedException, "argument 1 of TRIM() must be literal");
  }

  auto input_val = VarSizeJITExprValue(input->codegen(context));

  // register trim chars to context
  std::string trim_char_val = *trim_char_literal->get_constval().stringval;
  int trim_char_map_idx = context.registerTrimStringOperCharMap(trim_char_val);

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  // get runtime trim_char_map ptr
  auto trim_char_map_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_trim_char_map_by_id",
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::POINTER,
          .ret_sub_type = JITTypeTag::INT8,
          .params_vector = {
              func.getArgument(0).get(),
              func.createLiteral<int32_t>(JITTypeTag::INT32, trim_char_map_idx).get()}});

  // call external function
  bool do_ltrim =
      get_kind() == SqlStringOpKind::LTRIM || get_kind() == SqlStringOpKind::TRIM;
  bool do_rtrim =
      get_kind() == SqlStringOpKind::RTRIM || get_kind() == SqlStringOpKind::TRIM;
  std::string fn_name = "cider_trim";
  if (isOutput()) {
    auto ptr_and_len = func.emitRuntimeFunctionCall(
        fn_name,
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT64,
            .params_vector = {
                string_heap_ptr.get(),
                input_val.getValue().get(),
                input_val.getLength().get(),
                trim_char_map_ptr.get(),
                func.createLiteral<bool>(JITTypeTag::BOOL, do_ltrim).get(),
                func.createLiteral<bool>(JITTypeTag::BOOL, do_rtrim).get()}});

    return set_expr_value(input_val.getNull(), ptr_and_len);
  } else {
    auto start_val = func.emitRuntimeFunctionCall(
        fn_name + "_start",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT32,
            .params_vector = {
                input_val.getValue().get(),
                input_val.getLength().get(),
                trim_char_map_ptr.get(),
                func.createLiteral<bool>(JITTypeTag::BOOL, do_ltrim).get()}});
    auto ret_len = func.emitRuntimeFunctionCall(
        fn_name + "_len",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT32,
            .params_vector = {
                input_val.getValue().get(),
                input_val.getLength().get(),
                trim_char_map_ptr.get(),
                func.createLiteral<bool>(JITTypeTag::BOOL, do_ltrim).get(),
                func.createLiteral<bool>(JITTypeTag::BOOL, do_rtrim).get()}});

    auto ptr = func.emitRuntimeFunctionCall(
        "allocate_from_string_heap",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .params_vector = {string_heap_ptr.get(), ret_len.get()}});
    auto ret_ptr = func.emitRuntimeFunctionCall(
        fn_name + "_ptr",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::VOID,
            .params_vector = {
                ptr.get(), input_val.getValue().get(), start_val.get(), ret_len.get()}});
    return set_expr_value(input_val.getNull(), ret_len, ptr);
  }
}

std::shared_ptr<Analyzer::Expr> SplitPartStringOper::deep_copy() const {
  return makeExpr<Analyzer::SplitPartStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& SplitPartStringOper::codegen(CodegenContext& context) {
  CHECK_GE(getArity(), 3);
  CHECK_LE(getArity(), 4);
  JITFunction& func = *context.getJITFunction();
  // decode input args
  auto input = const_cast<Analyzer::Expr*>(getArg(0));
  auto delimiter = const_cast<Analyzer::Expr*>(getArg(1));

  auto input_val = VarSizeJITExprValue(input->codegen(context));

  auto delimiter_literal = dynamic_cast<Analyzer::Constant*>(delimiter);
  auto delimiter_val = VarSizeJITExprValue(delimiter->codegen(context));

  int limit_val = 0;
  int splitpart_val = 0;
  if (getArity() == 3) {  // no limit
    splitpart_val =
        dynamic_cast<const Analyzer::Constant*>(getArg(2))->get_constval().intval;
  } else if (getArity() == 4) {
    limit_val = dynamic_cast<const Analyzer::Constant*>(getArg(2))->get_constval().intval;
    splitpart_val =
        dynamic_cast<const Analyzer::Constant*>(getArg(3))->get_constval().intval;
  }
  bool reverse = splitpart_val < 0;
  splitpart_val = splitpart_val == 0 ? 1 : std::abs(splitpart_val);

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name = "cider_split";
  auto ptr_and_len = func.emitRuntimeFunctionCall(
      fn_name,
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::INT64,
          .params_vector = {
              string_heap_ptr.get(),
              input_val.getValue().get(),
              input_val.getLength().get(),
              delimiter_val.getValue().get(),
              delimiter_val.getLength().get(),
              func.createLiteral<bool>(JITTypeTag::BOOL, reverse).get(),
              func.createLiteral<int>(JITTypeTag::INT32, limit_val).get(),
              func.createLiteral<int>(JITTypeTag::INT32, splitpart_val).get()}});
  if (isOutput()) {
    return set_expr_value(input_val.getNull(), ptr_and_len);
  } else {
    // decode result
    auto ret_ptr = func.emitRuntimeFunctionCall(
        "extract_string_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .params_vector = {ptr_and_len.get()}});
    auto ret_len = func.emitRuntimeFunctionCall(
        "extract_string_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {ptr_and_len.get()}});
    return set_expr_value(input_val.getNull(), ret_len, ret_ptr);
  }
}

std::shared_ptr<Analyzer::Expr> RegexpReplaceStringOper::deep_copy() const {
  return makeExpr<Analyzer::RegexpReplaceStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& RegexpReplaceStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode input args
  auto input = const_cast<Analyzer::Expr*>(getArg(0));
  auto regex_pattern = const_cast<Analyzer::Expr*>(getArg(1));
  auto replace = const_cast<Analyzer::Expr*>(getArg(2));

  auto input_val = VarSizeJITExprValue(input->codegen(context));

  auto regex_pattern_literal = dynamic_cast<Analyzer::Constant*>(regex_pattern);
  auto regex_pattern_val = VarSizeJITExprValue(regex_pattern_literal->codegen(context));

  auto replace_literal = dynamic_cast<Analyzer::Constant*>(replace);
  auto replace_val = VarSizeJITExprValue(replace_literal->codegen(context));

  int start_pos_val =
      dynamic_cast<const Analyzer::Constant*>(getArg(3))->get_constval().intval;
  int occurence_val =
      dynamic_cast<const Analyzer::Constant*>(getArg(4))->get_constval().intval;

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name = "cider_regexp_replace";
  auto ptr_and_len = func.emitRuntimeFunctionCall(
      fn_name,
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::INT64,
          .params_vector = {
              string_heap_ptr.get(),
              input_val.getValue().get(),
              input_val.getLength().get(),
              regex_pattern_val.getValue().get(),
              regex_pattern_val.getLength().get(),
              replace_val.getValue().get(),
              replace_val.getLength().get(),
              func.createLiteral<int>(JITTypeTag::INT32, start_pos_val).get(),
              func.createLiteral<int>(JITTypeTag::INT32, occurence_val).get()}});
  if (isOutput()) {
    return set_expr_value(input_val.getNull(), ptr_and_len);
  } else {
    // decode result
    auto ret_ptr = func.emitRuntimeFunctionCall(
        "extract_string_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .params_vector = {ptr_and_len.get()}});
    auto ret_len = func.emitRuntimeFunctionCall(
        "extract_string_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {ptr_and_len.get()}});
    return set_expr_value(input_val.getNull(), ret_len, ret_ptr);
  }
}

std::shared_ptr<Analyzer::Expr> RegexpExtractStringOper::deep_copy() const {
  return makeExpr<Analyzer::RegexpExtractStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& RegexpExtractStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode input args
  auto input = const_cast<Analyzer::Expr*>(getArg(0));
  auto regex_pattern = const_cast<Analyzer::Expr*>(getArg(1));

  auto input_val = VarSizeJITExprValue(input->codegen(context));

  auto regex_pattern_literal = dynamic_cast<Analyzer::Constant*>(regex_pattern);
  auto regex_pattern_val = VarSizeJITExprValue(regex_pattern_literal->codegen(context));

  int group_val =
      dynamic_cast<const Analyzer::Constant*>(getArg(2))->get_constval().intval;

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name = "cider_regexp_extract";
  auto ptr_and_len = func.emitRuntimeFunctionCall(
      fn_name,
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::INT64,
          .params_vector = {
              string_heap_ptr.get(),
              input_val.getValue().get(),
              input_val.getLength().get(),
              regex_pattern_val.getValue().get(),
              regex_pattern_val.getLength().get(),
              func.createLiteral<int>(JITTypeTag::INT32, group_val).get()}});
  if (isOutput()) {
    return set_expr_value(input_val.getNull(), ptr_and_len);
  } else {
    // decode result
    auto ret_ptr = func.emitRuntimeFunctionCall(
        "extract_string_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .params_vector = {ptr_and_len.get()}});
    auto ret_len = func.emitRuntimeFunctionCall(
        "extract_string_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {ptr_and_len.get()}});
    return set_expr_value(input_val.getNull(), ret_len, ret_ptr);
  }
}

std::shared_ptr<Analyzer::Expr> RegexpSubstrStringOper::deep_copy() const {
  return makeExpr<Analyzer::RegexpSubstrStringOper>(
      std::dynamic_pointer_cast<Analyzer::StringOper>(StringOper::deep_copy()));
}

JITExprValue& RegexpSubstrStringOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // decode input args
  auto input = const_cast<Analyzer::Expr*>(getArg(0));
  auto regex_pattern = const_cast<Analyzer::Expr*>(getArg(1));

  auto input_val = VarSizeJITExprValue(input->codegen(context));

  auto regex_pattern_literal = dynamic_cast<Analyzer::Constant*>(regex_pattern);
  auto regex_pattern_val = VarSizeJITExprValue(regex_pattern_literal->codegen(context));

  int start_pos_val =
      dynamic_cast<const Analyzer::Constant*>(getArg(2))->get_constval().intval;
  int occurence_val =
      dynamic_cast<const Analyzer::Constant*>(getArg(3))->get_constval().intval;

  // get string heap ptr
  auto string_heap_ptr = func.emitRuntimeFunctionCall(
      "get_query_context_string_heap_ptr",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                .ret_sub_type = JITTypeTag::INT8,
                                .params_vector = {func.getArgument(0).get()}});
  std::string fn_name = "cider_regexp_substring";
  auto ptr_and_len = func.emitRuntimeFunctionCall(
      fn_name,
      JITFunctionEmitDescriptor{
          .ret_type = JITTypeTag::INT64,
          .params_vector = {
              string_heap_ptr.get(),
              input_val.getValue().get(),
              input_val.getLength().get(),
              regex_pattern_val.getValue().get(),
              regex_pattern_val.getLength().get(),
              func.createLiteral<int>(JITTypeTag::INT32, occurence_val).get(),
              func.createLiteral<int>(JITTypeTag::INT32, start_pos_val).get()}});
  if (isOutput()) {
    return set_expr_value(input_val.getNull(), ptr_and_len);
  } else {
    // decode result
    auto ret_ptr = func.emitRuntimeFunctionCall(
        "extract_string_ptr",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .params_vector = {ptr_and_len.get()}});
    auto ret_len = func.emitRuntimeFunctionCall(
        "extract_string_len",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                  .params_vector = {ptr_and_len.get()}});
    return set_expr_value(input_val.getNull(), ret_len, ret_ptr);
  }
}

}  // namespace Analyzer
