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

#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_EXPR_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_EXPR_H

#include <llvm/IR/Value.h>

#include "dummy.h"
#include "type/plan/Analyzer.h"

class ExprGenerator {
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;

 public:
  // Generates IR value(s) for the given analyzer expression.
  JITTuple codegen(const Analyzer::Expr* expr, const JITTuple& input);
  JITTuple codegen(const std::vector<ExprPtr> exprs, const JITTuple& input) {
    JITTuple next_input = input;
    for (const auto& expr : exprs) {
      auto vals = codegen(expr.get(), next_input);
      // merge vals into next_input
    }
    return next_input;
  }

 protected:
  JITTuple codegenBinOper(const Analyzer::BinOper*, const JITTuple& input);
  JITTuple codegenColumnExpr(const Analyzer::ColumnVar* col_var, const JITTuple& input);

  JITTuple codegenCmpFun(const Analyzer::BinOper* bin_oper, const JITTuple& input);
  JITTuple codegenFixedSizeColCmpFun(const Analyzer::BinOper* bin_oper,
                                     llvm::Value* lhs,
                                     llvm::Value* rhs,
                                     llvm::Value* null);
  // JITTuple codegenConstantExpr(const Analyzer::Constant*, const JITTuple& input);

  // JITTuple codegenCaseExpr(const Analyzer::CaseExpr*, const JITTuple& input);

  // JITTuple codegenCaseExpr(const Analyzer::CaseExpr*,
  //                          llvm::Type* case_llvm_type,
  //                          const bool is_real_str,
  //                          const JITTuple& input);

  // JITTuple codegenUOper(const Analyzer::UOper*, const JITTuple& input);

  // JITTuple codegenFixedLengthColVar(const Analyzer::ColumnVar* col_var,
  //                                   llvm::Value* col_byte_stream,
  //                                   llvm::Value* pos_arg,
  //                                   const JITTuple& input);
};

#endif
