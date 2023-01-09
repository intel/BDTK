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

#ifndef TYPE_PLAN_FUNCTION_EXPR_H
#define TYPE_PLAN_FUNCTION_EXPR_H

#include "type/plan/Expr.h"

namespace Analyzer {
/*
 * @type FunctionOper
 * @brief
 */

class FunctionOper : public Expr {
 public:
  FunctionOper(const SQLTypeInfo& ti,
               const std::string& name,
               const std::vector<std::shared_ptr<Analyzer::Expr>>& args)
      : Expr(ti, false), name_(name), args_(args) {}

  std::string getName() const { return name_; }

  size_t getArity() const { return args_.size(); }

  const Analyzer::Expr* getArg(const size_t i) const {
    CHECK_LT(i, args_.size());
    return args_[i].get();
  }

  const Analyzer::Expr* getReworteArg(const size_t i) const {
    CHECK_LT(i, rewrote_args_.size());
    CHECK_EQ(args_.size(), rewrote_args_.size());
    return rewrote_args_[i].get();
  }

  std::shared_ptr<Analyzer::Expr> getOwnArg(const size_t i) const {
    CHECK_LT(i, args_.size());
    return args_[i];
  }

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  JITExprValue& codegen(CodegenContext& context) override;

  ExprPtrRefVector get_children_reference() override {
    ExprPtrRefVector children_ref;
    for (auto&& arg : args_) {
      children_ref.push_back(const_cast<ExprPtr*>(&arg));
    }
    return children_ref;
  }

 private:
  const std::string name_;
  const std::vector<std::shared_ptr<Analyzer::Expr>> args_;
  std::vector<std::shared_ptr<Analyzer::Expr>> rewrote_args_;  // casted args
  bool is_rewritten_ = false;
};

class FunctionOperWithCustomTypeHandling : public FunctionOper {
 public:
  FunctionOperWithCustomTypeHandling(
      const SQLTypeInfo& ti,
      const std::string& name,
      const std::vector<std::shared_ptr<Analyzer::Expr>>& args)
      : FunctionOper(ti, name, args) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  bool operator==(const Expr& rhs) const override;
};
}  // namespace Analyzer

#endif
