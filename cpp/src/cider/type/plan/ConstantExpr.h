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
#ifndef TYPE_PLAN_CONSTANT_EXPR_H
#define TYPE_PLAN_CONSTANT_EXPR_H

#include <list>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "exec/nextgen/context/CodegenContext.h"
#include "type/data/sqltypes.h"
#include "type/plan/Expr.h"

namespace Analyzer {

/*
 * @type Constant
 * @brief expression for a constant value
 */
class Constant : public Expr {
 public:
  Constant(SQLTypes t, bool n) : Expr(t, !n), is_null(n) {
    if (n) {
      set_null_value();
    } else {
      type_info.set_notnull(true);
    }
    initAutoVectorizeFlag();
  }
  Constant(SQLTypes t, bool n, Datum v) : Expr(t, !n), is_null(n), constval(v) {
    if (n) {
      set_null_value();
    } else {
      type_info.set_notnull(true);
    }
    initAutoVectorizeFlag();
  }
  Constant(const SQLTypeInfo& ti, bool n, Datum v) : Expr(ti), is_null(n), constval(v) {
    if (n) {
      set_null_value();
    } else {
      type_info.set_notnull(true);
    }
    initAutoVectorizeFlag();
  }
  Constant(const SQLTypeInfo& ti,
           bool n,
           const std::list<std::shared_ptr<Analyzer::Expr>>& l)
      : Expr(ti), is_null(n), constval(Datum{0}), value_list(l) {
    initAutoVectorizeFlag();
  }
  ~Constant() override;
  bool get_is_null() const { return is_null; }
  Datum get_constval() const { return constval; }
  void set_constval(Datum d) { constval = d; }
  const std::list<std::shared_ptr<Analyzer::Expr>>& get_value_list() const {
    return value_list;
  }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  std::shared_ptr<Analyzer::Expr> add_cast(const SQLTypeInfo& new_type_info) override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

  JITExprValue& codegen(CodegenContext& context) override;

  ExprPtrRefVector get_children_reference() override { return {}; }

 private:
  bool is_null;    // constant is NULL
  Datum constval;  // the constant value
  const std::list<std::shared_ptr<Analyzer::Expr>> value_list;
  void cast_number(const SQLTypeInfo& new_type_info);
  void cast_string(const SQLTypeInfo& new_type_info);
  void cast_from_string(const SQLTypeInfo& new_type_info);
  void cast_to_string(const SQLTypeInfo& new_type_info);
  void do_cast(const SQLTypeInfo& new_type_info);
  void set_null_value();
  void initAutoVectorizeFlag() {
    switch (type_info.get_type()) {
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE: {
        auto_vectorizable_ = true;
        break;
      }
      default:
        auto_vectorizable_ = false;
    }
  }
};

}  // namespace Analyzer

#endif
