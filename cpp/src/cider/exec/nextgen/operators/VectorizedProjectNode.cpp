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
// NormalTree. All of the sub-trees are sorted in topological order and all of input exprs
// are ordered.
class ExpressionTreeSpilter {
  using ExprGroup = VectorizedProjectTranslator::ExprsGroup;
  using ExprGroupType = VectorizedProjectTranslator::ExprsGroupType;
  struct TreeInputs {
    bool bool_input;
    ExprPtr root;
    ExprPtrVector inputs;
  };

 public:
  explicit ExpressionTreeSpilter(const ExprPtrVector& exprs) {
    for (auto&& expr : exprs) {
      addExprTrees(expr);
    }
  }

  void addExprTrees(const ExprPtr& expr) {
    utils::RecursiveFunctor traverser{
        [this](auto&& traverser, const ExprPtr& expr, bool is_root) -> TreeInputs {
          CHECK(expr);

          auto&& children = expr->get_children_reference();

          if (children.empty()) {
            return spilt(expr, {}, is_root);
          }

          std::vector<TreeInputs> children_inputs;
          children_inputs.reserve(children.size());
          for (auto&& child : children) {
            children_inputs.emplace_back(traverser(*child, false));
          }

          return spilt(expr, children_inputs, is_root);
        }};

    auto inputs = traverser(expr, true);
    sub_expr_trees_.emplace_back(
        ExprGroup{.exprs = {expr},
                  .input_exprs = std::move(inputs.inputs),
                  .type = getExprTreeType(expr, inputs.bool_input)});
  }

  std::vector<ExprGroup>& getSubExprTrees() { return sub_expr_trees_; }

 private:
  static ExprGroupType getExprTreeType(const ExprPtr& expr, bool bool_input) {
    if (bool_input) {
      if (expr->get_type_info().get_type() == kBOOLEAN) {
        return ExprGroupType::BoolIOTree;
      } else {
        LOG(ERROR) << "Invalid expression tree in ExpressionTreeSpilter, expr="
                   << expr->toString();
        return ExprGroupType::InvalidTree;
      }
    } else {
      if (expr->get_type_info().get_type() == kBOOLEAN) {
        return ExprGroupType::BoolOutputTree;
      } else {
        return ExprGroupType::NormalTree;
      }
    }
  }

  TreeInputs spilt(const ExprPtr& curr,
                   const std::vector<TreeInputs>& child_inputs,
                   bool is_root) {
    // Just pass leaf value.
    if (child_inputs.empty()) {
      if (dynamic_cast<Analyzer::Constant*>(curr.get())) {
        return {false, curr, {}};
      } else {
        return {curr->get_type_info().get_type() == kBOOLEAN, curr, {curr}};
      }
    }

    switch (curr->get_type_info().get_type()) {
      case kBOOLEAN: {
        if (std::all_of(child_inputs.begin(),
                        child_inputs.end(),
                        [](const TreeInputs& child) { return child.bool_input; })) {
          // All of chidren output Bool values, just pass them.
          return {true, curr, ExpressionTreeSpilter::combineInputExprs(child_inputs)};
        } else if (curr->isTrivialNullProcess() &&
                   std::all_of(
                       child_inputs.begin(),
                       child_inputs.end(),
                       [](const TreeInputs& child) { return !child.bool_input; })) {
          // All of input are not Bool values, try to pass them.
          return {false, curr, ExpressionTreeSpilter::combineInputExprs(child_inputs)};
        } else {
          // Both of BoolIO and BoolOutput subtrees exist in the children or curr expr has
          // non-trivial null process procedure, just split the BoolOutput subtrees.
          std::vector<TreeInputs> new_child_inputs;
          new_child_inputs.reserve(child_inputs.size());
          for (auto& child : child_inputs) {
            if (!child.bool_input) {
              sub_expr_trees_.emplace_back(
                  ExprGroup{.exprs = {child.root},
                            .input_exprs = child.inputs,
                            .type = ExprGroupType::BoolOutputTree});

              new_child_inputs.emplace_back(TreeInputs{
                  .bool_input = true, .root = child.root, .inputs = {child.root}});
            } else {
              new_child_inputs.emplace_back(child);
            }
          }

          CHECK(std::all_of(new_child_inputs.begin(),
                            new_child_inputs.end(),
                            [](const TreeInputs& child) { return child.bool_input; }));

          return {true, curr, ExpressionTreeSpilter::combineInputExprs(new_child_inputs)};
        }
      }
      default: {
        return {false, curr, ExpressionTreeSpilter::combineInputExprs(child_inputs)};
      }
    }
  }

  // Combine and sort input exprs of sub-trees, and remove duplicate items.
  static std::vector<ExprPtr> combineInputExprs(const std::vector<TreeInputs>& exprs) {
    size_t num = 0;
    for (auto&& expr : exprs) {
      num += expr.inputs.size();
    }
    std::vector<ExprPtr> ans;
    ans.reserve(num);

    for (auto&& expr : exprs) {
      ans.insert(ans.end(), expr.inputs.begin(), expr.inputs.end());
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
  UnionFindSet expr_groups(expr_trees.size());
  for (size_t i = 0; i < expr_trees.size(); ++i) {
    for (size_t j = i + 1; j < expr_trees.size(); ++j) {
      const ExprsGroup& expr_i = expr_trees[i];
      const ExprsGroup& expr_j = expr_trees[j];

      if (expr_i.type != expr_j.type) {
        continue;
      }

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
  std::vector<size_t> group_root_index(expr_trees.size());
  for (size_t i = 0; i < expr_trees.size(); ++i) {
    size_t group_root = expr_groups.find(i);
    group_root_index[i] = group_root;
    if (i != group_root) {
      expr_trees[group_root].exprs.emplace_back(expr_trees[i].exprs.front());
    }
  }
  std::sort(group_root_index.begin(), group_root_index.end());
  group_root_index.erase(std::unique(group_root_index.begin(), group_root_index.end()),
                         group_root_index.end());
  CHECK_EQ(group_root_index.size(), expr_groups.getUnionNum());

  // Gather non-ColumnVar input expressions users.
  std::unordered_map<ExprPtr::element_type*, std::vector<size_t>> indermediate_expr_users;
  for (size_t root_index = 0; root_index < group_root_index.size(); ++root_index) {
    for (auto& input : expr_trees[group_root_index[root_index]].input_exprs) {
      if (!input->isColumnVar()) {
        indermediate_expr_users[input.get()].emplace_back(root_index);
      }
    }
  }

  // Sort ExprsGroup as proper generation sequence
  if (!indermediate_expr_users.empty()) {
    std::vector<std::vector<size_t>> adjcent_map(group_root_index.size());
    std::vector<size_t> degree(group_root_index.size(), 0);
    for (size_t root_index = 0; root_index < group_root_index.size(); ++root_index) {
      for (auto& output : expr_trees[group_root_index[root_index]].exprs) {
        if (auto iter = indermediate_expr_users.find(output.get());
            iter != indermediate_expr_users.end()) {
          for (size_t user_root_index : iter->second) {
            ++degree[user_root_index];
            adjcent_map[root_index].push_back(user_root_index);
          }
        }
      }
    }

    std::vector<size_t> avaliable_index;
    avaliable_index.reserve(group_root_index.size());
    while (avaliable_index.size() != group_root_index.size()) {
      size_t prev_size = avaliable_index.size();
      for (size_t index = 0; index < degree.size(); ++index) {
        if (degree[index] == 0) {
          avaliable_index.push_back(index);
          for (size_t user : adjcent_map[index]) {
            --degree[user];
          }
          degree[index] = 1;
        }
      }
      if (prev_size == avaliable_index.size()) {
        LOG(FATAL) << "ExprsGroups contain circular dependencies in "
                      "VectorizedProjectTranslator.";
      }
    }

    std::vector<size_t> new_group_root_index(group_root_index.size());
    for (size_t i = 0; i < avaliable_index.size(); ++i) {
      new_group_root_index[i] = group_root_index[avaliable_index[i]];
    }
    group_root_index = std::move(new_group_root_index);
  }

  std::vector<ExprsGroup> res;
  res.reserve(expr_groups.getUnionNum());
  for (size_t index : group_root_index) {
    res.emplace_back(std::move(expr_trees[index]));
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
  auto r2c_node = createOpNode<RowToColumnNode>(group.exprs, c2r_node.get(), false);
  auto r2c_translator = r2c_node->toTranslator();

  for (auto& target : group.exprs) {
    // Register Batch for intermediate results
    if (target->getLocalIndex() == 0) {
      auto output_arrow_array =
          context.registerBatch(SQLTypeInfo(target->get_type_info().get_type(), false),
                                "intermediate_output",
                                true);
      // Binding target exprs with JITValues.
      target->setLocalIndex(context.appendArrowArrayValues(
          output_arrow_array, utils::JITExprValue(0, JITExprValueType::BATCH)));
    }
  }

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
    if (!input_col.empty()) {
      auto output_null = allocateNullBuffer(context, context.getInputLength(), expr);
      size_t input_index = 0;
      if (input_col.size() < 2) {
        context::codegen_utils::bitBufferMemcpy(
            output_null, getNullBuffer(context, input_col[0]), context.getInputLength());
        input_index += 1;
      } else {
        context::codegen_utils::bitBufferAnd(output_null,
                                             getNullBuffer(context, input_col[0]),
                                             getNullBuffer(context, input_col[1]),
                                             context.getInputLength());
        input_index += 2;
      }

      for (; input_index < input_col.size(); ++input_index) {
        context::codegen_utils::bitBufferAnd(
            output_null,
            output_null,
            getNullBuffer(context, input_col[input_index]),
            context.getInputLength());
      }
    }
  }

  // Convert byte to bit for Boolen outputs.
  for (auto& output : group.exprs) {
    if (output->get_type_info().get_type() == kBOOLEAN) {
      size_t local_index = output->getLocalIndex();
      CHECK(local_index);

      auto& batch_values = context.getArrowArrayValues(local_index);
      utils::FixSizeJITExprValue values(batch_values.second);

      context::codegen_utils::convertByteBoolToBit(
          values.getValue(), values.getValue(), context.getInputLength());
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
  auto buffer_bytes_length = (bool_loop_length + 31) / 32 * 32;

  for (auto& target : group.exprs) {
    // TODO (bigPYJ1151): Register Batch for intermediate targets.
    allocateBitwiseBuffer(context, target, buffer_bytes_length);
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

  // Set length.
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
    switch (group.type) {
      case ExprsGroupType::BoolIOTree:
        generateBoolInputExprsGroupCode(context, group);
        continue;
      case ExprsGroupType::NormalTree:
      case ExprsGroupType::BoolOutputTree:
        generateNonBoolInputExprsGroupCode(context, group);
        continue;
      default:
        LOG(FATAL) << "Invalid expression group in VectorizedProjectTranslator.";
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
