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
#include "exec/template/CodegenColValues.h"

namespace cider::exec::nextgen::operators {

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

  // Collect input ColumnVars of each output exprs respectively.
  std::vector<ExprPtrVector> input_columnvars(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    ExprPtrVector columnvars = utils::collectColumnVars({exprs[i]});
    std::sort(columnvars.begin(), columnvars.end());
    input_columnvars[i] = std::move(columnvars);
  }

  // Group output exprs and generate their code in same loops. If a input ColumnVars set
  // of a output expr is a subset of another one, the output exprs should be in a group.
  UnionFindSet expr_groups(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    for (size_t j = i + 1; j < exprs.size(); ++j) {
      if (utils::isSubsetExprVector(input_columnvars[i], input_columnvars[j]) ||
          utils::isSubsetExprVector(input_columnvars[j], input_columnvars[i])) {
        expr_groups.connect(i, j, [&input_columnvars](size_t a, size_t b) {
          return input_columnvars[a] >= input_columnvars[b];
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
      val.input_exprs = input_columnvars[i];
    }
  }

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

void VectorizedProjectTranslator::generateExprsGroupCode(
    context::CodegenContext& context,
    std::vector<ExprsGroup>& exprs_groups) {
  for (auto& group : exprs_groups) {
    // Utilize C2R and R2C to generate column load and store.
    auto c2r_node = createOpNode<ColumnToRowNode>(group.input_exprs);
    auto c2r_translator = c2r_node->toTranslator();
    auto r2c_node = createOpNode<RowToColumnNode>(group.exprs, c2r_node.get());
    auto r2c_translator = r2c_node->toTranslator();

    {
      // Temporarily set nullable of all exprs in output expr tree as false.
      NullableGuard null_guard(group.exprs);

      c2r_translator->codegen(
          context, [&group, &r2c_translator](context::CodegenContext& context) {
            // Generate Project code.
            for (auto& expr : group.exprs) {
              expr->codegen(context);
            }
            r2c_translator->consume(context);
          });
    }

    // Process null vector.
    // TBD (bigPYJ1151): Currently, only support trival null processing (e.g. merge all
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
        context::codegen_utils::bitBufferMemcpy(output_null,
                                                getNullBuffer(context, input_col[0]),
                                                c2r_node->getColumnRowNum());
        input_index += 1;
      } else {
        context::codegen_utils::bitBufferAnd(output_null,
                                             getNullBuffer(context, input_col[0]),
                                             getNullBuffer(context, input_col[1]),
                                             c2r_node->getColumnRowNum());
        input_index += 2;
      }

      for (; input_index < input_col.size(); ++input_index) {
        context::codegen_utils::bitBufferAnd(
            output_null,
            output_null,
            getNullBuffer(context, input_col[input_index]),
            c2r_node->getColumnRowNum());
      }
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
