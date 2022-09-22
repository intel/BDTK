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

#include <gtest/gtest.h>
#include "function/ExtensionFunctionsWhitelist.h"
#include "function/FunctionLookup.h"
#include "function/SubstraitFunctionMappings.h"
#include "function/extensions/BasicFunctionLookUpContext.h"

class FunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mappings_ptr = std::make_shared<const SubstraitFunctionMappings>();
    function_lookup_ptr = std::make_shared<FunctionLookup>(mappings_ptr);
  }

 public:
  SubstraitFunctionMappingsPtr mappings_ptr;
  FunctionLookupPtr function_lookup_ptr;
};

TEST_F(FunctionLookupTest, functionLookupPrestoExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
  function_signature.func_sig = "between:double_double_double";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::FunctionOper);
  }
}

TEST_F(FunctionLookupTest, functionLookupPrestoIntentionAggTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
  function_signature.func_sig = "sum:double_double";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  if (function_descriptor_ptr->agg_op_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->agg_op_type_ptr) == SQLAgg::kSUM);
  }
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::AggExpr);
  }
}

TEST_F(FunctionLookupTest, functionLookupPrestoIntentionScalarTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
  function_signature.func_sig = "equal:any1_any1";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_NE(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  if (function_descriptor_ptr->scalar_op_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->scalar_op_type_ptr) == SQLOps::kEQ);
  }
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::BinOper);
  }
}

TEST_F(FunctionLookupTest, functionLookupPrestoUnregisteredTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
  function_signature.func_sig = "between_unregisterd:double_double_double";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
}

TEST_F(FunctionLookupTest, functionLookupSubstraitExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "substrait";
  function_signature.func_sig = "between:double_double_double";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::FunctionOper);
  }
}

TEST_F(FunctionLookupTest, functionLookupSubstraitIntentionAggTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "substrait";
  function_signature.func_sig = "sum:double_double";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  if (function_descriptor_ptr->agg_op_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->agg_op_type_ptr) == SQLAgg::kSUM);
  }
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::AggExpr);
  }
}

TEST_F(FunctionLookupTest, functionLookupSubstraitIntentionScalarTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "substrait";
  function_signature.func_sig = "equal:any1_any1";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_NE(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  if (function_descriptor_ptr->scalar_op_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->scalar_op_type_ptr) == SQLOps::kEQ);
  }
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::BinOper);
  }
}

TEST_F(FunctionLookupTest, functionLookupSubstraitUnregisteredTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "substrait";
  function_signature.func_sig = "between_unregisterd:double_double_double";
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with any type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
}

TEST_F(FunctionLookupTest, functionLookupSparkExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "spark";
  function_signature.func_sig = "between:double_double_double";

  EXPECT_THROW(
      {
        try {
          auto function_descriptor_ptr =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ("spark function look up is not yet supported", e.what());
          throw;
        }
      },
      std::runtime_error);
}

TEST_F(FunctionLookupTest, functionLookupSparkIntentionAggTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "spark";
  function_signature.func_sig = "sum:double_double";

  EXPECT_THROW(
      {
        try {
          auto function_descriptor_ptr =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ("spark function look up is not yet supported", e.what());
          throw;
        }
      },
      std::runtime_error);
}

TEST_F(FunctionLookupTest, functionLookupSparkIntentionScalarTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "spark";
  function_signature.func_sig = "like:string_string";

  EXPECT_THROW(
      {
        try {
          auto function_descriptor_ptr =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ("spark function look up is not yet supported", e.what());
          throw;
        }
      },
      std::runtime_error);
}

TEST_F(FunctionLookupTest, functionLookupSparkUnregisteredTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "spark";
  function_signature.func_sig = "between_unregisterd:double_double_double";

  EXPECT_THROW(
      {
        try {
          auto function_descriptor_ptr =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ("spark function look up is not yet supported", e.what());
          throw;
        }
      },
      std::runtime_error);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
