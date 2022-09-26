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
#include "../thirdparty/velox/velox/substrait/SubstraitFunctionLookup.h"
#include "../thirdparty/velox/velox/substrait/SubstraitType.h"
#include "../thirdparty/velox/velox/substrait/VeloxToSubstraitMappings.h"
#include "function/ExtensionFunctionsWhitelist.h"
#include "function/FunctionLookup.h"
#include "function/SubstraitFunctionMappings.h"

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

class SubstraitFunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    extension_ = facebook::velox::substrait::SubstraitExtension::loadExtension();
    mappings_ = std::make_shared<
        const facebook::velox::substrait::VeloxToSubstraitFunctionMappings>();
    scalarFunctionLookup_ =
        std::make_shared<facebook::velox::substrait::SubstraitScalarFunctionLookup>(
            extension_, mappings_);
    aggregateFunctionLookup_ =
        std::make_shared<facebook::velox::substrait::SubstraitAggregateFunctionLookup>(
            extension_, mappings_);
    const auto& testExtension =
        facebook::velox::substrait::SubstraitExtension::loadExtension(
            {getDataPath() + "functions_test.yaml"});
    testScalarFunctionLookup_ =
        std::make_shared<facebook::velox::substrait::SubstraitScalarFunctionLookup>(
            testExtension, mappings_);
  }

  void testScalarFunctionLookup(
      const std::string& name,
      const std::vector<facebook::velox::substrait::SubstraitTypePtr>& arguments,
      const facebook::velox::substrait::SubstraitTypePtr& returnType,
      const std::string& outputSignature) {
    const auto& functionSignature =
        facebook::velox::substrait::SubstraitFunctionSignature::of(
            name, arguments, returnType);
    const auto& functionOption = scalarFunctionLookup_->lookupFunction(functionSignature);

    ASSERT_TRUE(functionOption.has_value());
    ASSERT_EQ(functionOption.value()->anchor().key, outputSignature);
  }

  void testAggregateFunctionLookup(
      const std::string& name,
      const std::vector<facebook::velox::substrait::SubstraitTypePtr>& arguments,
      const facebook::velox::substrait::SubstraitTypePtr& returnType,
      const std::string& outputSignature) {
    const auto& functionSignature =
        facebook::velox::substrait::SubstraitFunctionSignature::of(
            name, arguments, returnType);
    const auto& functionOption =
        aggregateFunctionLookup_->lookupFunction(functionSignature);

    ASSERT_TRUE(functionOption.has_value());
    ASSERT_EQ(functionOption.value()->anchor().key, outputSignature);
  }

  void assertTestSignature(
      const std::string& name,
      const std::vector<facebook::velox::substrait::SubstraitTypePtr>& arguments,
      const facebook::velox::substrait::SubstraitTypePtr& returnType,
      const std::string& outputSignature) {
    const auto& functionSignature =
        facebook::velox::substrait::SubstraitFunctionSignature::of(
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

  facebook::velox::substrait::SubstraitExtensionPtr extension_;
  facebook::velox::substrait::SubstraitFunctionMappingsPtr mappings_;
  facebook::velox::substrait::SubstraitScalarFunctionLookupPtr scalarFunctionLookup_;
  facebook::velox::substrait::SubstraitAggregateFunctionLookupPtr
      aggregateFunctionLookup_;
  facebook::velox::substrait::SubstraitScalarFunctionLookupPtr testScalarFunctionLookup_;
};

TEST_F(FunctionLookupTest, functionLookupPrestoExtentionBetweenDoubleTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = "presto";
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI8>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI8>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI8>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI16>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI16>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI16>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      facebook::velox::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI32>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI32>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      facebook::velox::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI32>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI32>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();
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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();

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
      facebook::velox::substrait::SubstraitType::decode("struct<fp64,i64>")};
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>();

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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI32>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kI32>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();

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
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kFp64>>(),
  };
  function_signature.returnType =
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>();

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

TEST_F(SubstraitFunctionLookupTest, test) {
  assertTestSignature(
      "test",
      {
          std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
              facebook::velox::substrait::SubstraitTypeKind::kFp32>>(),
          std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
              facebook::velox::substrait::SubstraitTypeKind::kFp32>>(),
      },
      std::make_shared<const facebook::velox::substrait::SubstraitScalarType<
          facebook::velox::substrait::SubstraitTypeKind::kBool>>(),
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
