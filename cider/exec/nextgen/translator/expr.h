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

#include "exec/nextgen/translator/dummy.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::translator {
using namespace cider::jitlib;

class ExprGenerator {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;

  ExprGenerator(JITFunction* func) : func_(func) {}

  // Generates IR value(s) for the given analyzer expression.
  JITExprValue& codegen(Analyzer::Expr* expr);

 protected:
  JITExprValue& codegenBinOper(Analyzer::BinOper*);
  JITExprValue& codegenColumnExpr(Analyzer::ColumnVar* col_var);

  JITExprValue& codegenFixedSizeColArithFun(Analyzer::BinOper* bin_oper,
                                            JITValue& lhs,
                                            JITValue& rhs);
  JITExprValue& codegenFixedSizeColCmpFun(Analyzer::BinOper* bin_oper,
                                          JITValue& lhs,
                                          JITValue& rhs);
  JITExprValue& codegenConstantExpr(Analyzer::Constant*);

  // JITExprValue& codegenCaseExpr(const Analyzer::CaseExpr*);

  // JITExprValue& codegenCaseExpr(const Analyzer::CaseExpr*,
  //                          llvm::Type* case_llvm_type,
  //                          const bool is_real_str);

  // JITExprValue& codegenUOper(const Analyzer::UOper*);

  // JITExprValue& codegenFixedLengthColVar(const Analyzer::ColumnVar* col_var,
  //                                   llvm::Value* col_byte_stream,
  //                                   llvm::Value* pos_arg);

  JITTypeTag getJITTag(const SQLTypes& st) {
    switch (st) {
      case kBOOLEAN:
        return JITTypeTag::BOOL;
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kTIME:
      case kTIMESTAMP:
      case kDATE:
      case kINTERVAL_DAY_TIME:
      case kINTERVAL_YEAR_MONTH:
        return JITTypeTag::INT32;
      case kFLOAT:
        return JITTypeTag::FLOAT;
      case kDOUBLE:
        return JITTypeTag::DOUBLE;
      case kVARCHAR:
      case kCHAR:
      case kTEXT:
        UNIMPLEMENTED();
      case kNULLT:
      default:
        return JITTypeTag::INVALID;
    }
    UNREACHABLE();
  }

  JITTypeTag getJITTag(const Analyzer::Expr* col_var) {
    CHECK(col_var);
    const auto& col_ti = col_var->get_type_info();
    return getJITTag(col_ti.get_type());
  }

 private:
  JITFunction* func_;
  // just for unreachable branch return;
  JITExprValue fake_val_;
};

}  // namespace cider::exec::nextgen::translator
#endif
