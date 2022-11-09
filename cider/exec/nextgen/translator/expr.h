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

#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_EXPR_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_EXPR_H

#include <llvm/IR/Value.h>

#include "exec/nextgen/jitlib/base/JITTuple.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/translator/dummy.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::translator {
using namespace cider::jitlib;

class ExprGenerator {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;

  ExprGenerator(JITFunction* func) : func_(func) {}

  // std::vector<JITValuePointer> codegen(const std::vector<ExprPtr> exprs, const
  // JITTuple& input) {
  //   JITTuple next_input = input;
  //   for (const auto& expr : exprs) {
  //     auto vals = codegen(expr.get(), next_input);
  //     // merge vals into next_input
  //   }
  //   return next_input;
  // }
  // Generates IR value(s) for the given analyzer expression.
  JITExprValue& codegen(Analyzer::Expr* expr);

 protected:
  JITExprValue& codegenBinOper(Analyzer::BinOper*);
  JITExprValue& codegenColumnExpr(Analyzer::ColumnVar* col_var);

  // JITExprValue& codegenArithFun(Analyzer::BinOper* bin_oper);
  // JITExprValue& codegenCmpFun(Analyzer::BinOper* bin_oper);
  JITExprValue& codegenFixedSizeColArithFun(Analyzer::BinOper* bin_oper,
                                            JITValue& lhs,
                                            JITValue& rhs,
                                            JITValue& null);
  JITExprValue& codegenFixedSizeColCmpFun(Analyzer::BinOper* bin_oper,
                                          JITValue& lhs,
                                          JITValue& rhs,
                                          JITValue& null);
  JITExprValue& codegenConstantExpr(Analyzer::Constant*);

  // JITTuple codegenCaseExpr(const Analyzer::CaseExpr*);

  // JITTuple codegenCaseExpr(const Analyzer::CaseExpr*,
  //                          llvm::Type* case_llvm_type,
  //                          const bool is_real_str);

  // JITTuple codegenUOper(const Analyzer::UOper*);

  // JITTuple codegenFixedLengthColVar(const Analyzer::ColumnVar* col_var,
  //                                   llvm::Value* col_byte_stream,
  //                                   llvm::Value* pos_arg);
 private:
  JITFunction* func_;
  // just for unreachable branch return;
  JITExprValue fake_val_;
};

}  // namespace cider::exec::nextgen::translator
#endif
