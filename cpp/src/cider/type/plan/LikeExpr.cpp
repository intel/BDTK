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
#include "type/plan/LikeExpr.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "type/plan/ConstantExpr.h"
#include "type/plan/Utils.h"  // for is_unnest

namespace Analyzer {
using namespace cider::jitlib;

JITExprValue& LikeExpr::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  if (auto expr_val = get_expr_value()) {
    return expr_val;
  }

  auto arg = const_cast<Analyzer::Expr*>(get_arg());
  auto pattern = const_cast<Analyzer::Expr*>(get_like_expr());
  auto escape = const_cast<Analyzer::Expr*>(get_escape_expr());

  if (is_unnest(extract_cast_arg(arg))) {
    CIDER_THROW(CiderCompileException, "LIKE does not support unnested exprssions");
  }

  CHECK(arg->get_type_info().is_string());
  CHECK(pattern->get_type_info().is_string());

  auto arg_val = VarSizeJITExprValue(arg->codegen(context));
  auto pattern_val = VarSizeJITExprValue(pattern->codegen(context));

  std::string fn_name{get_is_ilike() ? "string_ilike" : "string_like"};
  auto emit_desc =
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::BOOL,
                                .params_vector = {arg_val.getValue().get(),
                                                  arg_val.getLength().get(),
                                                  pattern_val.getValue().get(),
                                                  pattern_val.getLength().get()}};

  auto escape_char = char{'\\'};
  if (escape) {
    auto escape_char_expr = dynamic_cast<Analyzer::Constant*>(escape);
    CHECK(escape_char_expr);
    CHECK(escape_char_expr->get_type_info().is_string());
    CHECK_EQ(size_t(1), escape_char_expr->get_constval().stringval->size());
    escape_char = (*escape_char_expr->get_constval().stringval)[0];
  }
  // put escape_char_val here to keep it alive until codegen complete
  auto escape_char_val = func.createLiteral(JITTypeTag::INT8, int8_t(escape_char));

  if (get_is_simple()) {
    fn_name += "_simple";
  } else {
    // for non-simple string like ops, append escape char
    emit_desc.params_vector.push_back(escape_char_val.get());
  }

  return set_expr_value(arg_val.getNull(),
                        func.emitRuntimeFunctionCall(fn_name, emit_desc));
}

std::shared_ptr<Analyzer::Expr> LikeExpr::deep_copy() const {
  return makeExpr<LikeExpr>(arg->deep_copy(),
                            like_expr->deep_copy(),
                            escape_expr ? escape_expr->deep_copy() : nullptr,
                            is_ilike,
                            is_simple);
}

bool LikeExpr::operator==(const Expr& rhs) const {
  if (typeid(rhs) != typeid(LikeExpr)) {
    return false;
  }
  const LikeExpr& rhs_lk = dynamic_cast<const LikeExpr&>(rhs);
  if (!(*arg == *rhs_lk.get_arg()) || !(*like_expr == *rhs_lk.get_like_expr()) ||
      is_ilike != rhs_lk.get_is_ilike()) {
    return false;
  }
  if (escape_expr.get() == rhs_lk.get_escape_expr()) {
    return true;
  }
  if (escape_expr != nullptr && rhs_lk.get_escape_expr() != nullptr &&
      *escape_expr == *rhs_lk.get_escape_expr()) {
    return true;
  }
  return false;
}

std::string LikeExpr::toString() const {
  std::string str{"(LIKE "};
  str += arg->toString();
  str += like_expr->toString();
  if (escape_expr) {
    str += escape_expr->toString();
  }
  str += ") ";
  return str;
}

void LikeExpr::find_expr(bool (*f)(const Expr*),
                         std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  arg->find_expr(f, expr_list);
  like_expr->find_expr(f, expr_list);
  if (escape_expr != nullptr) {
    escape_expr->find_expr(f, expr_list);
  }
}

// to be deprecated
void LikeExpr::group_predicates(std::list<const Expr*>& scan_predicates,
                                std::list<const Expr*>& join_predicates,
                                std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  arg->collect_rte_idx(rte_idx_set);
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}
}  // namespace Analyzer
