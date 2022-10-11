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
#include "cider/CiderException.h"
#include "function/ExtensionFunctionsWhitelist.h"
#include "function/FunctionLookup.h"
#include "function/SubstraitFunctionCiderMappings.h"
#include "function/substrait/SubstraitFunctionLookup.h"
#include "function/substrait/SubstraitType.h"
#include "function/substrait/VeloxToSubstraitMappings.h"

class SubstraitFunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    function_lookup_ptr =
        std::make_shared<FunctionLookup>(PlatformType::SubstraitPlatform);
  }

  bool containsTips(const std::string& exception_str, const std::string& expect_str) {
    if (exception_str.find(expect_str) != std::string::npos) {
      return true;
    }
    return false;
  }

 public:
  FunctionLookupPtr function_lookup_ptr;
};

class PrestoFunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    function_lookup_ptr = std::make_shared<FunctionLookup>(PlatformType::PrestoPlatform);
  }

 public:
  FunctionLookupPtr function_lookup_ptr;
};

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenDoubleTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenI8Test) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI8>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI8>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI8>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenI16Test) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI16>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI16>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI16>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionAggTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "avg";
  function_signature.arguments = {
      cider::function::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kAVG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "equal";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI32>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoUnregisteredTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between_unregisterd";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kUNDEFINED_EXPR);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitIntentionAggTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "avg";
  function_signature.arguments = {
      cider::function::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kAVG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitIntentionScalarTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "equal";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI32>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitUnregisteredTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "between_unregisterd";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kUNDEFINED_EXPR);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSparkExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SparkPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();

  EXPECT_THROW(
      {
        try {
          auto function_descriptor =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const CiderCompileException& e) {
          EXPECT_TRUE(containsTips(
              e.what(),
              "Platform of target function is 2, mismatched with registered platform 0"));
          throw;
        }
      },
      CiderCompileException);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSparkIntentionAggTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SparkPlatform;
  function_signature.func_name = "avg";
  function_signature.arguments = {
      cider::function::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>();

  EXPECT_THROW(
      {
        try {
          auto function_descriptor =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const CiderCompileException& e) {
          EXPECT_TRUE(containsTips(
              e.what(),
              "Platform of target function is 2, mismatched with registered platform 0"));
          throw;
        }
      },
      CiderCompileException);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSparkIntentionScalarTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SparkPlatform;
  function_signature.func_name = "equal";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI32>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();

  EXPECT_THROW(
      {
        try {
          auto function_descriptor =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const CiderCompileException& e) {
          EXPECT_TRUE(containsTips(
              e.what(),
              "Platform of target function is 2, mismatched with registered platform 0"));
          throw;
        }
      },
      CiderCompileException);
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSparkUnregisteredTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SparkPlatform;
  function_signature.func_name = "between_unregisterd";
  function_signature.arguments = {
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>();

  EXPECT_THROW(
      {
        try {
          auto function_descriptor =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const CiderCompileException& e) {
          EXPECT_TRUE(containsTips(
              e.what(),
              "Platform of target function is 2, mismatched with registered platform 0"));
          throw;
        }
      },
      CiderCompileException);
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
