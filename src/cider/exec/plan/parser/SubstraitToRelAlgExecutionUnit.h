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

/**
 * @file    SubstraitToRelAlgExecutionUnit.h
 * @brief   Translate Substrait plan to RelAlgExecutionUnit
 **/

#pragma once

#include <cstdint>
#include <unordered_set>
#include "BaseContext.h"
#include "ConverterHelper.h"
#include "GeneratorContext.h"
#include "cider/CiderTableSchema.h"
#include "exec/plan/parser/SubstraitToAnalyzerExpr.h"
#include "exec/template/RelAlgExecutionUnit.h"
#include "include/cider/CiderSupportPlatType.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

namespace generator {

enum FrontendEngine { PRESTO, VELOX };

enum ExprType { FilterExpr, ProjectExpr, AggregationExpr };
static const std::string enum_str[] = {"FilterExpr",  // NOLINT
                                       "ProjectExpr",
                                       "AggregationExpr"};

class SubstraitToRelAlgExecutionUnit {
 public:
  explicit SubstraitToRelAlgExecutionUnit(
      const substrait::Plan& plan,
      const PlatformType from_platform = PlatformType::PrestoPlatform)
      : toAnalyzerExprConverter_(from_platform)
      , ctx_{nullptr}
      , input_table_schemas_{}
      , plan_(plan)
      , output_cider_table_schema_(nullptr) {}

  explicit SubstraitToRelAlgExecutionUnit(
      const PlatformType from_platform = PlatformType::PrestoPlatform)
      : toAnalyzerExprConverter_(from_platform)
      , ctx_{nullptr}
      , input_table_schemas_{}
      , plan_(substrait::Plan())
      , output_cider_table_schema_(nullptr) {}

  std::shared_ptr<RelAlgExecutionUnit> createRelAlgExecutionUnit(
      const std::vector<substrait::Expression*> exprs,
      const substrait::NamedStruct& schema,
      const std::vector<
          substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
          func_infos,
      const ExprType& expr_type);

  /**
   * public API for generating RelAlgExecutionUnit with substrait plan as input
   * @param plan: substrait plan
   * @return RelAlgExecutionUnit: input for future compilation
   */
  RelAlgExecutionUnit createRelAlgExecutionUnit();

  /**
   * Reconstructs struct type from a series of flatten types
   * @param index: the index of the first target exprssion that belongs to a struct type
   * @param length: the number of flatten types that make up the struct type
   */
  substrait::Type reconstructStructType(size_t index, size_t length);

  /**
   * return the output table schema info
   * @param plan: substrait plan
   */
  std::shared_ptr<CiderTableSchema> getOutputCiderTableSchema();

  const std::vector<CiderTableSchema> getInputCiderTableSchema() const {
    return input_table_schemas_;
  }

 private:
  /**
   * called by createRelAlgExecutionUnit() which will generate final RelAlgExecutionUnit
   * based on returned GeneratorContext.
   * generally, when visiting each rel node, GeneratorContext will be updated:
   * 1) update correspoding expr group, like quals, target_exprs, groupby_exprs, etc.
   *    AggregateRel --> target_exprs, groupby_exprs
   *    FilterRel    --> quals, simple_quals
   *    ProjectRel   --> target_exprs
   *    SortRel      --> sort_info
   * 2) update the index reference in each expression to eventually flat the expression
   *    with root named_struct, need pay attention to Analyzer::Var and
   * Analyzer::ColumnVar
   */
  void updateGeneratorContext(const substrait::Rel& rel_node,
                              const std::unordered_map<int, std::string>& function_map);

  // Postorder traversal to get all Relnode, use this traversal result to generate ctx
  void getRelNodesInPostOder(
      const substrait::Rel& rel_node,
      std::vector<std::pair<substrait::Rel, bool>>& rel_vec,
      std::unordered_set<substrait::Rel::RelTypeCase>& rel_type_set,
      bool is_join_right_node = false);

  // generate ctx elements base on rel node of substrait-json
  void generateCtxElements(
      const std::unordered_set<substrait::Rel::RelTypeCase>& rel_type_set,
      std::vector<std::shared_ptr<BaseContext>>& ctx_elements_vec);

  Substrait2AnalyzerExprConverter toAnalyzerExprConverter_;
  std::shared_ptr<GeneratorContext> ctx_;
  std::vector<CiderTableSchema> input_table_schemas_;
  const substrait::Plan& plan_;
  std::shared_ptr<CiderTableSchema> output_cider_table_schema_;
};
}  // namespace generator
