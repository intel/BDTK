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

  // TODO: (yma11) build extended expression and replace old evalulator API
  // ::substrait::ExtendedExpression* ext_expr = builder.build({{add_expr, {"add_res"}}});
  // Make batch for evaluation, data should be transferred from frontend in real case
    // TODO : (yma11) switch to new expr evaluation API
//   auto input_batch = CiderBatchBuilder()
//                          .setRowNum(2)
//                          .addColumn<int64_t>("a", CREATE_SUBSTRAIT_TYPE(I64), {1, 4})
//                          .addColumn<int64_t>("b", CREATE_SUBSTRAIT_TYPE(I64), {4, 3})
//                          .build();
//   auto expect_batch = CiderBatchBuilder()
//                           .setRowNum(2)
//                           .addColumn<int64_t>("0", CREATE_SUBSTRAIT_TYPE(I64), {8, 15})
//                           .build();
//   auto allocator = std::make_shared<CiderDefaultAllocator>();
//   CiderExprEvaluator evaluator({add_expr},
//                                builder.funcsInfo(),
//                                builder.getSchema(),
//                                allocator,
//                                generator::ExprType::ProjectExpr);
//   auto out_batch = evaluator.eval(input_batch);
//   // Verify result
//   assert(CiderBatchChecker::checkEq(std::make_shared<CiderBatch>(expect_batch),
//                                     std::make_shared<CiderBatch>(out_batch)));

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
  // ::substrait::ExtendedExpression* ext_expr = builder.build({{add_expr1,
  // {"add_res"}}}); Evaluate same batch as above
    // TODO : (yma11) switch to new expr evaluation API
//   CiderExprEvaluator evaluator1({add_expr1},
//                                 inc_builder.funcsInfo(),
//                                 inc_schema,
//                                 allocator,
//                                 generator::ExprType::ProjectExpr);
//   auto out_batch1 = evaluator1.eval(input_batch);
//   // Verify result
//   auto expected_batch1 =
//       CiderBatchBuilder()
//           .setRowNum(2)
//           .addColumn<int64_t>("0", CREATE_SUBSTRAIT_TYPE(I64), {14, 22})
//           .build();
//   assert(CiderBatchChecker::checkEq(std::make_shared<CiderBatch>(expected_batch1),
//                                     std::make_shared<CiderBatch>(out_batch1)));
  // Example 2 : generate for expression "sum(a), min(b)"
  SubstraitExprBuilder agg_builder({"a", "b"},
                                   {CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false),
                                    CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false)});
  ::substrait::Expression* arg_1 = agg_builder.makeFieldReference("a");
  ::substrait::Expression* arg_2 = agg_builder.makeFieldReference("b");
  ::substrait::AggregateFunction* sum =
      agg_builder.makeAggExpr("sum", {arg_1}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  ::substrait::AggregateFunction* min =
      agg_builder.makeAggExpr("min", {arg_2}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  // ::substrait::ExtendedExpression* ext_expr = builder.build({{sum, {"sum_a"}}, {min,
  // {"min_b"}}});
}
