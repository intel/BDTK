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
#ifndef TYPE_PLAN_COLUMN_EXPR_H
#define TYPE_PLAN_COLUMN_EXPR_H

#include <list>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "exec/nextgen/context/CodegenContext.h"
#include "type/data/sqltypes.h"
#include "type/plan/Expr.h"
#include "type/schema/ColumnInfo.h"

namespace Analyzer {
/*
 * @type ColumnVar
 * @brief expression that evaluates to the value of a column in a given row from a base
 * table. It is used in parse trees and is only used in Scan nodes in a query plan for
 * scanning a table while Var nodes are used for all other plans.
 */
class ColumnVar : public Expr {
 public:
  ColumnVar(ColumnInfoPtr col_info, int nest_level)
      : Expr(col_info->type), rte_idx(nest_level), col_info_(std::move(col_info)) {
    is_column_var_ = true;
    initAutoVectorizeFlag();
  }
  explicit ColumnVar(const SQLTypeInfo& ti)
      : Expr(ti)
      , rte_idx(-1)
      , col_info_(std::make_shared<ColumnInfo>(-1, 0, 0, "", ti, false)) {
    is_column_var_ = true;
    initAutoVectorizeFlag();
  }
  ColumnVar(const SQLTypeInfo& ti,
            int table_id,
            int col_id,
            int nest_level,
            bool is_virtual = false)
      : Expr(ti)
      , rte_idx(nest_level)
      , col_info_(
            std::make_shared<ColumnInfo>(-1, table_id, col_id, "", ti, is_virtual)) {
    is_column_var_ = true;
    initAutoVectorizeFlag();
  }
  int get_db_id() const { return col_info_->db_id; }
  int get_table_id() const { return col_info_->table_id; }
  int get_column_id() const { return col_info_->column_id; }
  int get_rte_idx() const { return rte_idx; }
  ColumnInfoPtr get_column_info() const { return col_info_; }
  bool is_virtual() const { return col_info_->is_rowid; }
  EncodingType get_compression() const { return type_info.get_compression(); }
  int get_comp_param() const { return type_info.get_comp_param(); }
  void set_type_info(const SQLTypeInfo& ti) override {
    if (type_info != ti) {
      col_info_ = std::make_shared<ColumnInfo>(col_info_->db_id,
                                               col_info_->table_id,
                                               col_info_->column_id,
                                               col_info_->name,
                                               ti,
                                               col_info_->is_rowid);
      type_info = ti;
    }
  }
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    rte_idx_set.insert(rte_idx);
  }
  static bool colvar_comp(const ColumnVar* l, const ColumnVar* r) {
    return l->get_table_id() < r->get_table_id() ||
           (l->get_table_id() == r->get_table_id() &&
            l->get_column_id() < r->get_column_id());
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    colvar_set.insert(this);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

 public:
  JITExprValue& codegen(CodegenContext& context) override;

  ExprPtrRefVector get_children_reference() override { return {}; }

 protected:
  int rte_idx;  // 0-based range table index, used for table ordering in multi-joins
  ColumnInfoPtr col_info_;

 private:
  void initAutoVectorizeFlag();
};

class OutputColumnVar : public Expr {
 public:
  explicit OutputColumnVar(const std::shared_ptr<ColumnVar>& col)
      : Expr(col->get_type_info()), col_(col) {}

  JITExprValue& codegen(CodegenContext& context) override;

  ExprPtrRefVector get_children_reference() override { return {&col_}; }

 private:
  ExprPtr col_;
};

/*
 * @type Var
 * @brief expression that evaluates to the value of a column in a given row generated
 * from a query plan node.  It is only used in plan nodes above Scan nodes.
 * The row can be produced by either the inner or the outer plan in case of a join.
 * It inherits from ColumnVar to keep track of the lineage through the plan nodes.
 * The table_id will be set to 0 if the Var does not correspond to an original column
 * value.
 */
class Var : public ColumnVar {
 public:
  enum WhichRow { kINPUT_OUTER, kINPUT_INNER, kOUTPUT, kGROUPBY };
  Var(const SQLTypeInfo& ti, int r, int c, int i, bool is_virtual, WhichRow o, int v)
      : ColumnVar(ti, r, c, i, is_virtual), which_row(o), varno(v) {}
  Var(ColumnInfoPtr col_info, int i, WhichRow o, int v)
      : ColumnVar(col_info, i), which_row(o), varno(v) {}
  Var(const SQLTypeInfo& ti, WhichRow o, int v) : ColumnVar(ti), which_row(o), varno(v) {}
  WhichRow get_which_row() const { return which_row; }
  void set_which_row(WhichRow r) { which_row = r; }
  int get_varno() const { return varno; }
  void set_varno(int n) { varno = n; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  std::string toString() const override;
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    rte_idx_set.insert(-1);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return deep_copy();
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return deep_copy();
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;

 private:
  WhichRow which_row;  // indicate which row this Var should project from.  It can be
                       // from the outer input plan or the inner input plan (for joins)
                       // or the output row in the current plan.
  int varno;           // the column number in the row.  1-based
};

}  // namespace Analyzer

#endif
