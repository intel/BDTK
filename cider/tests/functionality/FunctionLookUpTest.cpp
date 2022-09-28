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
#include "function/SubstraitFunctionCiderMappings.h"
#include "function/substrait/SubstraitFunctionLookup.h"
#include "function/substrait/SubstraitType.h"
#include "function/substrait/VeloxToSubstraitMappings.h"

class FunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mappings_ptr = std::make_shared<const SubstraitFunctionCiderMappings>();
    function_lookup_ptr = std::make_shared<FunctionLookup>(mappings_ptr);
  }

 public:
  SubstraitFunctionCiderMappingsPtr mappings_ptr;
  FunctionLookupPtr function_lookup_ptr;
};

class SubstraitFunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    extension_ = cider::function::substrait::SubstraitExtension::loadExtension();
    mappings_ = std::make_shared<
        const cider::function::substrait::VeloxToSubstraitFunctionMappings>();
    scalarFunctionLookup_ =
        std::make_shared<cider::function::substrait::SubstraitScalarFunctionLookup>(
            extension_, mappings_);
    aggregateFunctionLookup_ =
        std::make_shared<cider::function::substrait::SubstraitAggregateFunctionLookup>(
            extension_, mappings_);
    const auto& testExtension =
        cider::function::substrait::SubstraitExtension::loadExtension(
            {getDataPath() + "functions_test.yaml"});
    testScalarFunctionLookup_ =
        std::make_shared<cider::function::substrait::SubstraitScalarFunctionLookup>(
            testExtension, mappings_);
  }

  void assertTestSignature(
      const std::string& name,
      const std::vector<cider::function::substrait::SubstraitTypePtr>& arguments,
      const cider::function::substrait::SubstraitTypePtr& returnType,
      const std::string& outputSignature) {
    const auto& functionSignature =
        cider::function::substrait::SubstraitFunctionSignature::of(
            name, arguments, returnType);
    const auto& functionOption =
        testScalarFunctionLookup_->lookupFunction(functionSignature);

    ASSERT_TRUE(functionOption.has_value());
    ASSERT_EQ(functionOption.value()->anchor().key, outputSignature);
  }

 private:
  static std::string getDataPath() {
    const std::string absolute_path = __FILE__;
    auto const pos = absolute_path.find_last_of('/');
    return absolute_path.substr(0, pos) + "/data/";
  }

  cider::function::substrait::SubstraitExtensionPtr extension_;
  cider::function::substrait::SubstraitFunctionMappingsPtr mappings_;
  cider::function::substrait::SubstraitScalarFunctionLookupPtr scalarFunctionLookup_;
  cider::function::substrait::SubstraitAggregateFunctionLookupPtr
      aggregateFunctionLookup_;
  cider::function::substrait::SubstraitScalarFunctionLookupPtr testScalarFunctionLookup_;
};

TEST_F(FunctionLookupTest, functionLookupPrestoExtentionBetweenDoubleTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::FunctionOper);
  }
}

TEST_F(FunctionLookupTest, functionLookupPrestoExtentionBetweenI8Test) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->op_support_expr_type_ptr) ==
                OpSupportExprType::FunctionOper);
  }
}

TEST_F(FunctionLookupTest, functionLookupPrestoExtentionBetweenI16Test) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
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
  function_signature.func_name = "avg";
  function_signature.arguments = {
      cider::function::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>();
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  if (function_descriptor_ptr->agg_op_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->agg_op_type_ptr) == SQLAgg::kAVG);
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
}

TEST_F(FunctionLookupTest, functionLookupSubstraitExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "substrait";
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
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
  function_signature.func_name = "avg";
  function_signature.arguments = {
      cider::function::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>();
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_NE(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  if (function_descriptor_ptr->agg_op_type_ptr != nullptr) {
    ASSERT_TRUE(*(function_descriptor_ptr->agg_op_type_ptr) == SQLAgg::kAVG);
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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

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
  auto function_descriptor_ptr = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor_ptr->scalar_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->agg_op_type_ptr, nullptr);
  ASSERT_EQ(function_descriptor_ptr->op_support_expr_type_ptr, nullptr);
}

TEST_F(FunctionLookupTest, functionLookupSparkExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "spark";
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
  function_signature.func_name = "avg";
  function_signature.arguments = {
      cider::function::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kFp64>>();

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
          auto function_descriptor_ptr =
              function_lookup_ptr->lookupFunction(function_signature);
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ("spark function look up is not yet supported", e.what());
          throw;
        }
      },
      std::runtime_error);
}

TEST_F(SubstraitFunctionLookupTest, substraitFunctionLookupTest) {
  assertTestSignature(
      "test",
      {
          std::make_shared<const cider::function::substrait::SubstraitScalarType<
              cider::function::substrait::SubstraitTypeKind::kFp32>>(),
          std::make_shared<const cider::function::substrait::SubstraitScalarType<
              cider::function::substrait::SubstraitTypeKind::kFp32>>(),
      },
      std::make_shared<const cider::function::substrait::SubstraitScalarType<
          cider::function::substrait::SubstraitTypeKind::kBool>>(),
      "test:fp32_fp32");
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
