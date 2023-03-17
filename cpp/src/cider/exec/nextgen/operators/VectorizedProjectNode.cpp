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

#include "exec/nextgen/operators/VectorizedProjectNode.h"

#include <numeric>

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/operators/ColumnToRowNode.h"
#include "exec/nextgen/operators/RowToColumnNode.h"
#include "exec/nextgen/utils/ExprUtils.h"
#include "exec/nextgen/utils/JITExprValue.h"
#include "type/plan/ConstantExpr.h"

namespace cider::exec::nextgen::operators {

using namespace jitlib;

class UnionFindSet {
 public:
  explicit UnionFindSet(size_t init_union_num)
      : parent_(init_union_num), union_num_(init_union_num) {
    std::iota(parent_.begin(), parent_.end(), 0);
  }

  size_t getUnionNum() const { return union_num_; }

  size_t find(size_t index) {
    size_t parent = index;
    while (parent != parent_[parent]) {
      parent = parent_[parent];
    }
    parent_[index] = parent;
    return parent;
  }

  // bool Criteria(size_t elem1, size_t elem2), returns true if elem1 has higher priority
  // to become a root.
  template <
      typename Criteria,
      typename std::enable_if_t<std::is_invocable_r_v<bool, Criteria, size_t, size_t>,
                                bool> = true>
  void connect(size_t elem1, size_t elem2, Criteria&& criteria) {
    elem1 = find(elem1);
    elem2 = find(elem2);
    if (elem1 != elem2) {
      if (!criteria(elem1, elem2)) {
        std::swap(elem1, elem2);
      }
      parent_[elem2] = elem1;
      --union_num_;
    }
  }

 private:
  std::vector<size_t> parent_;
  size_t union_num_;
};

class NullableGuard {
 public:
  explicit NullableGuard(ExprPtrVector& exprs) {
    utils::RecursiveFunctor traverser{[this](auto&& traverser, ExprPtr& expr) -> void {
      if (expr->getNullable()) {
        nullable_ptrs_.insert(expr.get());
        expr->setNullable(false);
      }
      auto children = expr->get_children_reference();
      for (auto child : children) {
        traverser(*child);
      }
    }};

    for (auto& expr : exprs) {
      traverser(expr);
    }
  }

  ~NullableGuard() {
    for (auto ptr : nullable_ptrs_) {
      ptr->setNullable(true);
    }
  }

 private:
  std::unordered_set<ExprPtr::element_type*> nullable_ptrs_;
};

// Spilt expression tree into sub-trees as three types like BoolIOTree, BoolOutputTree,
// NoBoolTree. All of the sub-trees are sorted in topological order and all of input exprs
// are ordered.
class ExpressionTreeSpilter {
  using ExprGroup = VectorizedProjectTranslator::ExprsGroup;
  using TreeInputs = std::pair<bool, ExprPtrVector>;

 public:
  explicit ExpressionTreeSpilter(const ExprPtrVector& exprs) {
    for (auto&& expr : exprs) {
      addExprTrees(expr);
    }
  }

  void addExprTrees(const ExprPtr& expr) {
    utils::RecursiveFunctor traverser{
        [this](auto&& traverser, const ExprPtr& expr) -> TreeInputs {
          CHECK(expr);

          auto&& children = expr->get_children_reference();

          if (children.empty()) {
            return spilt(expr, {});
          }

          std::vector<TreeInputs> children_inputs;
          children_inputs.reserve(children.size());
          for (auto&& child : children) {
            children_inputs.emplace_back(traverser(*child));
          }

          return spilt(expr, children_inputs);
        }};

    auto inputs = traverser(expr);
    sub_expr_trees_.emplace_back(ExprGroup{.exprs = {expr},
                                           .input_exprs = std::move(inputs.second),
                                           .bool_input = inputs.first});
  }

  const std::vector<ExprGroup>& getSubExprTrees() const { return sub_expr_trees_; }

 private:
  TreeInputs spilt(const ExprPtr& curr, const std::vector<TreeInputs>& child_inputs) {
    // Just pass leaf value.
    if (child_inputs.empty()) {
      if (dynamic_cast<Analyzer::Constant*>(curr.get())) {
        return {false, {}};
      } else {
        return {curr->get_type_info().get_type() == kBOOLEAN, {curr}};
      }
    }

    auto combined_sorted_inputs = ExpressionTreeSpilter::combineInputExprs(child_inputs);
    switch (curr->get_type_info().get_type()) {
      case kBOOLEAN: {
        if (std::all_of(child_inputs.begin(),
                        child_inputs.end(),
                        [](const TreeInputs& child) { return child.first; })) {
          // All of chidren output Bool values, just pass them.
          return {true, std::move(combined_sorted_inputs)};
        } else {
          // There are non-Bool outputs of children, split current expression tree.
          sub_expr_trees_.emplace_back(
              ExprGroup{.exprs = {curr},
                        .input_exprs = std::move(combined_sorted_inputs),
                        .bool_input = false});

          return {true, {curr}};
        }
      }
      default: {
        return {false, std::move(combined_sorted_inputs)};
      }
    }
  }

  // Combine and sort input exprs of sub-trees, and remove duplicate items.
  static std::vector<ExprPtr> combineInputExprs(const std::vector<TreeInputs>& exprs) {
    size_t num = 0;
    for (auto&& expr : exprs) {
      num += expr.second.size();
    }
    std::vector<ExprPtr> ans;
    ans.reserve(num);

    for (auto&& expr : exprs) {
      ans.insert(ans.end(), expr.second.begin(), expr.second.end());
    }
    std::sort(ans.begin(), ans.end());
    ans.erase(std::unique(ans.begin(), ans.end()), ans.end());

    return ans;
  }

 private:
  std::vector<ExprGroup> sub_expr_trees_;
};

TranslatorPtr VectorizedProjectNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<VectorizedProjectTranslator>(shared_from_this(), succ);
}

void VectorizedProjectTranslator::consume(context::CodegenContext& context) {
  codegen(context, [this](context::CodegenContext& context) {
    if (successor_) {
      successor_->consume(context);
    }
  });
}

std::vector<VectorizedProjectTranslator::ExprsGroup>
VectorizedProjectTranslator::groupOutputExprs() {
  auto&& [_, exprs] = node_->getOutputExprs();

  // Collect all sub-trees. The input columns are sorted.
  ExpressionTreeSpilter expression_tree_spilter(exprs);
  auto&& expr_trees = expression_tree_spilter.getSubExprTrees();

  // Group output exprs and generate their code in same loops. If a input expression set
  // of a output expr is a subset of another one, the output exprs should be in a group.
  UnionFindSet expr_groups(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    for (size_t j = i + 1; j < exprs.size(); ++j) {
      const ExprsGroup& expr_i = expr_trees[i];
      const ExprsGroup& expr_j = expr_trees[j];
      if (utils::isSubsetExprVector(expr_i.input_exprs, expr_j.input_exprs) ||
          utils::isSubsetExprVector(expr_j.input_exprs, expr_i.input_exprs)) {
        // TBD: Whether need to check rings.
        expr_groups.connect(i, j, [&](size_t a, size_t b) {
          return expr_trees[a].input_exprs.size() >= expr_trees[b].input_exprs.size();
        });
      }
    }
  }

  // Generate ExprsGroup
  std::unordered_map<size_t, ExprsGroup> groups_map;
  for (size_t i = 0; i < exprs.size(); ++i) {
    size_t group_root = expr_groups.find(i);
    auto&& val = groups_map[group_root];
    val.exprs.emplace_back(exprs[i]);
    if (group_root == i) {
      val.input_exprs = expr_trees[i].input_exprs;
      val.bool_input = expr_trees[i].bool_input;
    }
  }

  // TODO: Values generation sequence refactor.
  std::vector<ExprsGroup> res;
  res.reserve(expr_groups.getUnionNum());
  for (auto& item : groups_map) {
    res.emplace_back(std::move(item.second));
  }

  return res;
}

jitlib::JITValuePointer& getNullBuffer(context::CodegenContext& context,
                                       const ExprPtr& expr) {
  CHECK(expr->getLocalIndex());
  auto&& [_, values] = context.getArrowArrayValues(expr->getLocalIndex());
  return utils::JITExprValueAdaptor(values).getNull();
}

jitlib::JITValuePointer& allocateNullBuffer(context::CodegenContext& context,
                                            jitlib::JITValuePointer& len,
                                            const ExprPtr& expr) {
  CHECK(expr->getLocalIndex());
  auto& arrow_array_values = context.getArrowArrayValues(expr->getLocalIndex());

  utils::JITExprValueAdaptor nullable_values(arrow_array_values.second);
  nullable_values.setNull(context.getJITFunction()->createLocalJITValue([&]() {
    return context::codegen_utils::allocateArrowArrayBuffer(
        arrow_array_values.first, 0, len, SQLTypes::kTINYINT);
  }));

  return nullable_values.getNull();
}

void generateNonBoolInputExprsGroupCode(context::CodegenContext& context,
                                        VectorizedProjectTranslator::ExprsGroup& group) {
  // Utilize C2R and R2C to generate column load and store.
  auto c2r_node = createOpNode<ColumnToRowNode>(group.input_exprs, true);
  auto c2r_translator = c2r_node->toTranslator();
  auto r2c_node = createOpNode<RowToColumnNode>(group.exprs, c2r_node.get());
  auto r2c_translator = r2c_node->toTranslator();

  {
    // Temporarily set nullable of all exprs in output expr tree as false.
    NullableGuard null_guard(group.exprs);

    c2r_translator->codegen(context,
                            [&group, &r2c_translator](context::CodegenContext& context) {
                              // Generate Project code.
                              for (auto& expr : group.exprs) {
                                expr->codegen(context);
                              }
                              r2c_translator->consume(context);
                            });
  }

  // Process null vector.
  // TBD   (bigPYJ1151): Currently, only support trival null processing (e.g. merge all
  // leaf exprs' null vector by bitwise-and).
  // TODO (bigPYJ1151): Specialize more null vector 'and' primary function to reduce
  // function-call overhead and load/store inst.
  for (auto& expr : group.exprs) {
    if (!expr->getNullable()) {
      continue;
    }
    auto input_col = utils::collectColumnVars({expr});
    input_col.erase(std::remove_if(input_col.begin(),
                                   input_col.end(),
                                   [](ExprPtr& expr) { return !expr->getNullable(); }),
                    input_col.end());
    CHECK(!input_col.empty());

    auto output_null = allocateNullBuffer(context, c2r_node->getColumnRowNum(), expr);
    size_t input_index = 0;
    if (input_col.size() < 2) {
      context::codegen_utils::bitBufferMemcpy(
          output_null, getNullBuffer(context, input_col[0]), c2r_node->getColumnRowNum());
      input_index += 1;
    } else {
      context::codegen_utils::bitBufferAnd(output_null,
                                           getNullBuffer(context, input_col[0]),
                                           getNullBuffer(context, input_col[1]),
                                           c2r_node->getColumnRowNum());
      input_index += 2;
    }

    for (; input_index < input_col.size(); ++input_index) {
      context::codegen_utils::bitBufferAnd(output_null,
                                           output_null,
                                           getNullBuffer(context, input_col[input_index]),
                                           c2r_node->getColumnRowNum());
    }
  }
}

void allocateBitwiseBuffer(context::CodegenContext& ctx,
                           ExprPtr& expr,
                           JITValuePointer& length) {
  size_t local_index = expr->getLocalIndex();
  CHECK(local_index);

  auto& batch_values = ctx.getArrowArrayValues(local_index);
  auto& arrow_array = batch_values.first;
  utils::FixSizeJITExprValue values(batch_values.second);

  auto data_buffer =
      context::codegen_utils::allocateArrowArrayBuffer(arrow_array, 1, length, kTINYINT);
  values.setValue(data_buffer);

  if (expr->getNullable()) {
    auto null_buffer = context::codegen_utils::allocateArrowArrayBuffer(
        arrow_array, 0, length, kTINYINT);
    values.setNull(null_buffer);
  }
}

void generateBoolInputExprsGroupCode(context::CodegenContext& context,
                                     VectorizedProjectTranslator::ExprsGroup& group) {
  auto&& func = context.getJITFunction();

  auto index = func->createVariable(JITTypeTag::INT64, "bool_loop_index", 0);
  auto input_len = context.getInputLength();

  auto bool_loop_length = (input_len + 7) / 8;

  for (auto& target : group.exprs) {
    // TODO (bigPYJ1151): Register Batch for intermediate targets.
    allocateBitwiseBuffer(context, target, bool_loop_length);
  }

  func->createLoopBuilder()
      ->condition([&index, &bool_loop_length]() { return index < bool_loop_length; })
      ->loop([&group, &index, &context, &func](LoopBuilder*) {
        // Read inputs
        for (auto& input : group.input_exprs) {
          size_t arrow_array_local_index = input->getLocalIndex();
          CHECK(arrow_array_local_index);
          auto&& [batch, buffers] = context.getArrowArrayValues(arrow_array_local_index);
          utils::FixSizeJITExprValue fixsize_values(buffers);

          auto i8_data_buffer =
              fixsize_values.getValue()->castPointerSubType(JITTypeTag::INT8);
          auto i8_data = i8_data_buffer[index];

          JITValuePointer i8_null(nullptr);
          if (input->getNullable()) {
            // Nullable
            auto i8_null_buffer =
                fixsize_values.getNull()->castPointerSubType(JITTypeTag::INT8);
            i8_null.replace(~i8_null_buffer[index]);
          } else {
            // Not nullable
            i8_null.replace(func->createLiteral(JITTypeTag::INT8, 0));
          }
          input->set_expr_value(i8_null, i8_data);
        }

        // Generate expressions
        for (auto& target : group.exprs) {
          target->codegen(context);
        }

        // Write outputs
        for (auto& target : group.exprs) {
          size_t arrow_array_local_index = target->getLocalIndex();
          CHECK(arrow_array_local_index);
          auto&& [batch, buffers] = context.getArrowArrayValues(arrow_array_local_index);
          utils::FixSizeJITExprValue fixsize_values(buffers),
              target_values(target->get_expr_value());

          auto data_buffer = fixsize_values.getValue();
          data_buffer[index] = *target_values.getValue();
          if (target->getNullable()) {
            auto null_buffer = fixsize_values.getNull();
            null_buffer[index] = ~target_values.getNull();
          }
        }
      })
      ->setNoAlias(true)
      ->update([&index]() { index = index + 1; })
      ->build();

  // Set length
  for (auto& target : group.exprs) {
    size_t arrow_array_local_index = target->getLocalIndex();
    CHECK(arrow_array_local_index);
    auto&& [batch, _] = context.getArrowArrayValues(arrow_array_local_index);
    context::codegen_utils::setArrowArrayLength(batch, input_len);
  }
}

void VectorizedProjectTranslator::generateExprsGroupCode(
    context::CodegenContext& context,
    std::vector<ExprsGroup>& exprs_groups) {
  for (auto& group : exprs_groups) {
    if (group.bool_input) {
      generateBoolInputExprsGroupCode(context, group);
    } else {
      generateNonBoolInputExprsGroupCode(context, group);
    }
  }
}

void VectorizedProjectTranslator::codegenImpl(SuccessorEmitter successor_wrapper,
                                              context::CodegenContext& context,
                                              void* successor) {
  // Group output exprs
  auto exprs_groups = groupOutputExprs();

  generateExprsGroupCode(context, exprs_groups);

  successor_wrapper(successor, context);
}

}  // namespace cider::exec::nextgen::operators
