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

#include "CodeGenerator.h"
#include "ScalarExprVisitor.h"

namespace {

class UsedColumnExpressions : public ScalarExprVisitor<ScalarCodeGenerator::ColumnMap> {
 protected:
  ScalarCodeGenerator::ColumnMap visitColumnVar(
      const Analyzer::ColumnVar* column) const override {
    ScalarCodeGenerator::ColumnMap m;
    InputColDescriptor input_desc(column->get_column_info(), column->get_rte_idx());
    m.emplace(input_desc,
              std::static_pointer_cast<Analyzer::ColumnVar>(column->deep_copy()));
    return m;
  }

  ScalarCodeGenerator::ColumnMap aggregateResult(
      const ScalarCodeGenerator::ColumnMap& aggregate,
      const ScalarCodeGenerator::ColumnMap& next_result) const override {
    auto result = aggregate;
    result.insert(next_result.begin(), next_result.end());
    return result;
  }
};

std::vector<InputTableInfo> g_table_infos;

llvm::Type* llvm_type_from_sql(const SQLTypeInfo& ti, llvm::LLVMContext& ctx) {
  switch (ti.get_type()) {
    case kINT: {
      return get_int_type(32, ctx);
    }
    default: {
      LOG(ERROR) << "Unsupported type";
      return nullptr;  // satisfy -Wreturn-type
    }
  }
}

}  // namespace

ScalarCodeGenerator::ColumnMap ScalarCodeGenerator::prepare(const Analyzer::Expr* expr) {
  UsedColumnExpressions visitor;
  const auto used_columns = visitor.visit(expr);
  std::list<std::shared_ptr<const InputColDescriptor>> global_col_ids;
  for (const auto& used_column : used_columns) {
    global_col_ids.push_back(std::make_shared<InputColDescriptor>(used_column.first));
  }
  plan_state_->allocateLocalColumnIds(global_col_ids);
  return used_columns;
}

ScalarCodeGenerator::CompiledExpression ScalarCodeGenerator::compile(
    const Analyzer::Expr* expr,
    const bool fetch_columns,
    const CompilationOptions& co) {
  own_plan_state_ =
      std::make_unique<PlanState>(false, std::vector<InputTableInfo>{}, nullptr);
  plan_state_ = own_plan_state_.get();
  const auto used_columns = prepare(expr);
  std::vector<llvm::Type*> arg_types(plan_state_->global_to_local_col_ids_.size() + 1);
  std::vector<std::shared_ptr<Analyzer::ColumnVar>> inputs(arg_types.size() - 1);
  auto& ctx = module_->getContext();
  for (const auto& kv : plan_state_->global_to_local_col_ids_) {
    size_t arg_idx = kv.second;
    CHECK_LT(arg_idx, arg_types.size());
    const auto it = used_columns.find(kv.first);
    const auto col_expr = it->second;
    inputs[arg_idx] = col_expr;
    const auto& ti = col_expr->get_type_info();
    arg_types[arg_idx + 1] = llvm_type_from_sql(ti, ctx);
  }
  arg_types[0] =
      llvm::PointerType::get(llvm_type_from_sql(expr->get_type_info(), ctx), 0);
  auto ft = llvm::FunctionType::get(get_int_type(32, ctx), arg_types, false);
  auto scalar_expr_func = llvm::Function::Create(
      ft, llvm::Function::ExternalLinkage, "scalar_expr", module_.get());
  auto bb_entry = llvm::BasicBlock::Create(ctx, ".entry", scalar_expr_func, 0);
  own_cgen_state_ = std::make_unique<CgenState>(g_table_infos.size(), false);
  own_cgen_state_->module_ = module_.get();
  own_cgen_state_->row_func_ = own_cgen_state_->current_func_ = scalar_expr_func;
  own_cgen_state_->ir_builder_.SetInsertPoint(bb_entry);
  cgen_state_ = own_cgen_state_.get();
  AUTOMATIC_IR_METADATA(cgen_state_);
  const auto expr_lvs = codegen(expr, fetch_columns, co);
  CHECK_EQ(expr_lvs.size(), size_t(1));
  cgen_state_->ir_builder_.CreateStore(expr_lvs.front(),
                                       cgen_state_->row_func_->arg_begin());
  cgen_state_->ir_builder_.CreateRet(ll_int<int32_t>(0, ctx));
  return {scalar_expr_func, nullptr, inputs};
}

std::vector<void*> ScalarCodeGenerator::generateNativeCode(
    const CompiledExpression& compiled_expression,
    const CompilationOptions& co) {
  CHECK(module_ && !execution_engine_.get()) << "Invalid code generator state";
  module_.release();
  execution_engine_ =
      generateNativeCPUCode(compiled_expression.func, {compiled_expression.func}, co);
  return {execution_engine_->getPointerToFunction(compiled_expression.func)};
}

std::vector<llvm::Value*> ScalarCodeGenerator::codegenColumn(
    const Analyzer::ColumnVar* column,
    const bool fetch_column,
    const CompilationOptions& co) {
  int arg_idx = plan_state_->getLocalColumnId(column, fetch_column);
  CHECK_LT(static_cast<size_t>(arg_idx), cgen_state_->row_func_->arg_size());
  llvm::Value* arg = cgen_state_->row_func_->arg_begin() + arg_idx + 1;
  return {arg};
}
