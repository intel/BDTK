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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <string>
#include "TestHelpers.h"
#include "cider/CiderCompileModule.h"
#include "exec/module/CiderExprEvaluator.h"
#include "tests/utils/CiderBatchBuilder.h"
#include "tests/utils/CiderBatchChecker.h"
#include "util/Logger.h"

#include "exec/plan/builder/SubstraitExprBuilder.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"

static const auto allocator = std::make_shared<CiderDefaultAllocator>();

TEST(ExpressionEvalTest, Add_I64_I64) {
  // should be expression or Measure
  std::string expr_json = R"({
            "scalar_function": {
                    "functionReference": 1,
                    "arguments": [{ "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 0
                          }
                        },
                        "rootReference": {
                        }
                      }}
                    }, { "value": {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 1
                          }
                        },
                        "rootReference": {
                        }
                      }}
                    }],
                    "outputType": {
                      "i64": {
                        "typeVariationReference": 0,
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }
                  }
   })";
  std::string func_json = R"({
        "extensionUriReference": 2,
        "functionAnchor": 1,
        "name": "add:opt_i64_i64"
    })";
  std::string schema_json = R"(
        {
         "names": [
          "c0",
          "c1"
         ],
         "struct": {
          "types": [
           {
            "i64": {
             "type_variation_reference": 0,
             "nullability": "NULLABILITY_REQUIRED"
            }
           },
           {
            "i64": {
             "type_variation_reference": 0,
             "nullability": "NULLABILITY_REQUIRED"
            }
           }
          ],
          "type_variation_reference": 0,
          "nullability": "NULLABILITY_REQUIRED"
         }
        })";
  ::substrait::Expression sub_expr;
  ::substrait::NamedStruct schema;
  ::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction func_info;
  google::protobuf::util::JsonStringToMessage(expr_json, &sub_expr);
  google::protobuf::util::JsonStringToMessage(schema_json, &schema);
  google::protobuf::util::JsonStringToMessage(func_json, &func_info);
  // make batch and process
  auto input_batch = CiderBatchBuilder()
                         .setRowNum(2)
                         .addColumn<int64_t>("c0", CREATE_SUBSTRAIT_TYPE(I64), {1, 2})
                         .addColumn<int64_t>("c1", CREATE_SUBSTRAIT_TYPE(I64), {2, 3})
                         .build();
  auto expect_batch = CiderBatchBuilder()
                          .setRowNum(2)
                          .addColumn<int64_t>("0", CREATE_SUBSTRAIT_TYPE(I64), {3, 5})
                          .build();
  EXPECT_EQ(2, input_batch.row_num());

  CiderExprEvaluator evaluator(
      {&sub_expr}, {&func_info}, &schema, allocator, ExprType::ProjectExpr);
  auto out_batch1 = evaluator.eval(input_batch);
  auto out_batch2 = evaluator.eval(input_batch);
  auto expect_batch_ptr = std::make_shared<CiderBatch>(expect_batch);
  EXPECT_TRUE(CiderBatchChecker::checkEq(expect_batch_ptr,
                                         std::make_shared<CiderBatch>(out_batch1)));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expect_batch_ptr,
                                         std::make_shared<CiderBatch>(out_batch2)));
}

TEST(ExpressionEvalTest, Complex_I32_I32) {
  // Expression: a * b + b
  SubstraitExprBuilder builder({"a", "b"},
                               {CREATE_SUBSTRAIT_TYPE_FULL_PTR(I32, false),
                                CREATE_SUBSTRAIT_TYPE_FULL_PTR(I32, false)});

  ::substrait::Expression* field1 = builder.makeFieldReference(0);
  ::substrait::Expression* field2 = builder.makeFieldReference(1);
  ::substrait::Expression* multiply_expr = builder.makeScalarExpr(
      "multiply", {field1, field2}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I32, false));

  ::substrait::Expression* add_expr = builder.makeScalarExpr(
      "add", {multiply_expr, field2}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I32, false));
  auto input_batch = CiderBatchBuilder()
                         .setRowNum(2)
                         .addColumn<int64_t>("a", CREATE_SUBSTRAIT_TYPE(I32), {1, 4})
                         .addColumn<int64_t>("b", CREATE_SUBSTRAIT_TYPE(I32), {4, 3})
                         .build();
  auto expect_batch = CiderBatchBuilder()
                          .setRowNum(2)
                          .addColumn<int64_t>("0", CREATE_SUBSTRAIT_TYPE(I32), {8, 15})
                          .build();
  CiderExprEvaluator evaluator({add_expr},
                               builder.funcsInfo(),
                               builder.getSchema(),
                               allocator,
                               ExprType::ProjectExpr);
  auto out_batch1 = evaluator.eval(input_batch);
  auto out_batch2 = evaluator.eval(input_batch);
  auto expect_batch_ptr = std::make_shared<CiderBatch>(expect_batch);
  EXPECT_TRUE(CiderBatchChecker::checkEq(expect_batch_ptr,
                                         std::make_shared<CiderBatch>(out_batch1)));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expect_batch_ptr,
                                         std::make_shared<CiderBatch>(out_batch2)));
}

TEST(ExpressionEvalTest, GT_I64_I64) {
  // Expression: a > b
  SubstraitExprBuilder builder({"a", "b"},
                               {CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false),
                                CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false)});
  ::substrait::Expression* field0 = builder.makeFieldReference(0);
  ::substrait::Expression* field1 = builder.makeFieldReference(1);
  ::substrait::Expression* gt_expr = builder.makeScalarExpr(
      "gt", {field0, field1}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(Bool, false));
  auto input_batch = CiderBatchBuilder()
                         .setRowNum(2)
                         .addColumn<int64_t>("a", CREATE_SUBSTRAIT_TYPE(I64), {1, 4})
                         .addColumn<int64_t>("b", CREATE_SUBSTRAIT_TYPE(I64), {4, 3})
                         .build();
  auto expect_batch = CiderBatchBuilder()
                          .setRowNum(1)
                          .addColumn<int64_t>("a", CREATE_SUBSTRAIT_TYPE(I64), {4})
                          .addColumn<int64_t>("b", CREATE_SUBSTRAIT_TYPE(I64), {3})
                          .build();
  CiderExprEvaluator evaluator({gt_expr},
                               builder.funcsInfo(),
                               builder.getSchema(),
                               allocator,
                               ExprType::FilterExpr);
  auto out_batch1 = evaluator.eval(input_batch);
  auto out_batch2 = evaluator.eval(input_batch);
  auto expect_batch_ptr = std::make_shared<CiderBatch>(expect_batch);
  EXPECT_TRUE(CiderBatchChecker::checkEq(expect_batch_ptr,
                                         std::make_shared<CiderBatch>(out_batch1)));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expect_batch_ptr,
                                         std::make_shared<CiderBatch>(out_batch2)));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
