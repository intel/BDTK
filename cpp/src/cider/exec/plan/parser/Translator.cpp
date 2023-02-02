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

#include "Translator.h"
#include "exec/template/ExpressionRewrite.h"

QualsConjunctiveForm qual_to_conjunctive_form(
    const std::shared_ptr<Analyzer::Expr> qual_expr) {
  CHECK(qual_expr);
  auto bin_oper = std::dynamic_pointer_cast<const Analyzer::BinOper>(qual_expr);
  // skip translate if its left operand is substr()
  if (bin_oper &&
      dynamic_cast<const Analyzer::SubstringStringOper*>(bin_oper->get_left_operand())) {
    int rte_idx{0};
    const auto simple_qual = bin_oper->normalize_simple_predicate(rte_idx);
    return simple_qual ? QualsConjunctiveForm{{simple_qual}, {}}
                       : QualsConjunctiveForm{{}, {qual_expr}};
  }

  if (!bin_oper) {
    const auto rewritten_qual_expr = rewrite_expr(qual_expr.get());
    auto up_oper = std::dynamic_pointer_cast<const Analyzer::UOper>(qual_expr);
    if (up_oper && up_oper->get_optype() == kNOT) {
      return {{rewritten_qual_expr ? rewritten_qual_expr : qual_expr}, {}};
    }
    return {{}, {rewritten_qual_expr ? rewritten_qual_expr : qual_expr}};
  }

  if (bin_oper->get_optype() == kAND) {
    const auto lhs_cf = qual_to_conjunctive_form(bin_oper->get_own_left_operand());
    const auto rhs_cf = qual_to_conjunctive_form(bin_oper->get_own_right_operand());
    auto simple_quals = lhs_cf.simple_quals;
    simple_quals.insert(
        simple_quals.end(), rhs_cf.simple_quals.begin(), rhs_cf.simple_quals.end());
    auto quals = lhs_cf.quals;
    quals.insert(quals.end(), rhs_cf.quals.begin(), rhs_cf.quals.end());
    return {simple_quals, quals};
  }
  int rte_idx{0};
  const auto simple_qual = bin_oper->normalize_simple_predicate(rte_idx);
  return simple_qual ? QualsConjunctiveForm{{simple_qual}, {}}
                     : QualsConjunctiveForm{{}, {qual_expr}};
}
