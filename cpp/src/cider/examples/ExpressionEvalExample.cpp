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
#include "exec/plan/builder/SubstraitExprBuilder.h"
#include "exec/plan/parser/LiteralUtils.h"
#include "exec/plan/parser/TypeUtils.h"
// #include "exec/plan/validator/CiderPlanValidator.h"
#include "substrait/algebra.pb.h"
#include "substrait/function.pb.h"
#include "substrait/type.pb.h"

int main(int argc, char** argv) {
  // Example 1: generate for expression "a * b + b"
  SubstraitExprBuilder builder({"a", "b"},
                               {CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false),
                                CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false)});
  ::substrait::Expression* field0 = builder.makeFieldReference(0);
  ::substrait::Expression* field1 = builder.makeFieldReference("b");
  ::substrait::Expression* multiply_expr = builder.makeScalarExpr(
      "multiply", {field0, field1}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  ::substrait::Expression* add_expr = builder.makeScalarExpr(
      "add", {multiply_expr, field1}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));

  // TODO: (yma11) enable following code when ExpressionEvaluator implemented
  /*
  ::substrait::ExtendedExpression* ext_expr = builder.build({{add_expr, {"add_res"}}});
  // Make batch for evaluation, data should be transferred from frontend in real case
  auto&& [schema, array] =
      ArrowArrayBuilder()
          .setRowNum(2)
          .addColumn<int64_t>("a", CREATE_SUBSTRAIT_TYPE(I64), {1, 4})
          .addColumn<int64_t>("b", CREATE_SUBSTRAIT_TYPE(I64), {4, 3})
          .build();
  auto&& [expected_schema, expected_array] =
      ArrowArrayBuilder()
          .setRowNum(2)
          .addColumn<int64_t>("c", CREATE_SUBSTRAIT_TYPE(I64), {8, 15})
          .build();
  auto allocator = std::make_shared<CiderDefaultAllocator>();
  ExpressionEvaluator evaluator({ext_expr},
                                std::make_shared<ExprEvaluatorContext>(allocator));
  struct ArrowArray out_array;
  struct ArrowSchema out_schema;
  evaluator.eval(array, schema, out_array, out_schema);
  // Verify result
  assert(CiderArrowChecker::checkArrowEq(
      expected_array, out_array, expected_schema, out_schema));
  */

  // Example 2 : generate for expression "a * b + 10"
  SubstraitExprBuilder inc_builder({"a", "b"},
                                   {CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false),
                                    CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false)});
  ::substrait::Expression* field2 = inc_builder.makeFieldReference("a");
  ::substrait::Expression* field3 = inc_builder.makeFieldReference("b");
  ::substrait::Expression* field4 = CREATE_LITERAL(I64, "10");
  ::substrait::NamedStruct* inc_schema = inc_builder.getSchema();
  ::substrait::Expression* multiply_expr1 = inc_builder.makeScalarExpr(
      "multiply", {field2, field3}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  ::substrait::Expression* add_expr1 = inc_builder.makeScalarExpr(
      "add", {multiply_expr1, field4}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  /*
  ::substrait::ExtendedExpression* ext_expr1 = builder.build({{add_expr1, {"add_res"}}});
  // Evaluate same batch as above
  ExpressionEvaluator evaluator1({ext_expr1},
                                 std::make_shared<ExprEvaluatorContext>(allocator));
  struct ArrowArray out_array_1;
  struct ArrowSchema out_schema_1;
  evaluator1.eval(array, schema, out_array_1, out_schema_1);
  auto&& [expected_schema_1, expected_array_1] =
      ArrowArrayBuilder()
          .setRowNum(2)
          .addColumn<int64_t>("c", CREATE_SUBSTRAIT_TYPE(I64), {14, 22})
          .build();
  // Verify result
  assert(CiderArrowChecker::checkArrowEq(
      expected_array_1, out_array_1, expected_schema_1, out_schema_1));
  */
}
