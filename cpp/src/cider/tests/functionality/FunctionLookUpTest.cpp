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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "cider/CiderException.h"
#include "function/ExtensionFunctionsWhitelist.h"
#include "function/FunctionLookupEngine.h"
#include "function/SubstraitFunctionCiderMappings.h"

class SubstraitFunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    function_lookup_ptr =
        FunctionLookupEngine::getInstance(PlatformType::SubstraitPlatform);
  }

  bool containsTips(const std::string& exception_str, const std::string& expect_str) {
    if (exception_str.find(expect_str) != std::string::npos) {
      return true;
    }
    return false;
  }

 public:
  FunctionLookupEnginePtr function_lookup_ptr = nullptr;
};

class PrestoFunctionLookupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    function_lookup_ptr = FunctionLookupEngine::getInstance(PlatformType::PrestoPlatform);
  }

 public:
  FunctionLookupEnginePtr function_lookup_ptr = nullptr;
};

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenDoubleTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenDoubleTest2) {
  const std::string function_signature_str = "between__3:fp64_fp64_fp64";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenI8Test1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI8>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI8>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI8>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenI8Test2) {
  const std::string function_signature_str = "between__3:i8_i8_i8";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenI16Test1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI16>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI16>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI16>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoExtentionBetweenI16Test2) {
  const std::string function_signature_str = "between:i64_i64_i64";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionAggTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "avg";
  function_signature.arguments = {io::substrait::Type::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kAVG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "avg");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionAggTest2) {
  const std::string function_signature_str = "avg:struct<fp64,i64>";
  const std::string return_type = "fp64";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kAVG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "avg");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionAggTest3) {
  const std::string function_signature_str = "count:i64";
  const std::string return_type = "i64";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kCOUNT);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "count");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionAggTest4) {
  const std::string function_signature_str = "count:opt";
  const std::string return_type = "i64";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kCOUNT);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "count");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionAggTest5) {
  const std::string function_signature_str = "count";
  const std::string return_type = "i64";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kCOUNT);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "count");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "equal";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "equal");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest2) {
  const std::string function_signature_str = "equal:i32_i32";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "equal");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest3) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "eq";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "equal");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest4) {
  const std::string function_signature_str = "eq:i32_i32";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "equal");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest5) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "substr";
  function_signature.arguments = {
      std::make_shared<
          const io::substrait::ScalarType<io::substrait::TypeKind::kString>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
  };
  function_signature.return_type = std::make_shared<
      const io::substrait::ScalarType<io::substrait::TypeKind::kString>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::SUBSTRING);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type,
            OpSupportExprType::kSUBSTRING_STRING_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "substring");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest6) {
  const std::string function_signature_str = "substr:string_i32_i32";
  const std::string return_type = "string";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::SUBSTRING);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type,
            OpSupportExprType::kSUBSTRING_STRING_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "substring");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest7) {
  const std::string function_signature_str = "char_length:vchar";
  const std::string return_type = "i64";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::CHAR_LENGTH);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type,
            OpSupportExprType::kCHAR_LENGTH_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "char_length");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest8) {
  const std::string function_signature_str = "substr:varchar<L1>_i32_i32";
  const std::string return_type = "varchar<L1>";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::SUBSTRING);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type,
            OpSupportExprType::kSUBSTRING_STRING_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "substring");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest9) {
  const std::string function_signature_str = "substr:vchar_i32_i32";
  const std::string return_type = "varchar<L1>";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::SUBSTRING);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type,
            OpSupportExprType::kSUBSTRING_STRING_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "substring");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest10) {
  const std::string function_signature_str = "substr:str_i64_i64";
  const std::string return_type = "str";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::SUBSTRING);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type,
            OpSupportExprType::kSUBSTRING_STRING_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "substring");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest11) {
  const std::string function_signature_str = "in:str_str";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kIN);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kIN_VALUES);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "in");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest12) {
  const std::string function_signature_str = "in:str_list";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kIN);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kIN_VALUES);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "in");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoIntentionScalarTest13) {
  const std::string function_signature_str = "in:str_list<string>";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kIN);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kIN_VALUES);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "in");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoUnregisteredTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::PrestoPlatform;
  function_signature.func_name = "between_unregisterd";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kUNDEFINED_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, false);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between_unregisterd");
}

TEST_F(PrestoFunctionLookupTest, functionLookupPrestoUnregisteredTest2) {
  const std::string function_signature_str = "between_unregisterd:fp64_fp64_fp64";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kUNDEFINED_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, false);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between_unregisterd");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitExtentionTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitExtentionTest2) {
  const std::string function_signature_str = "between__3:fp64_fp64_fp64";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kFUNCTION_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between__3");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitIntentionAggTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "avg";
  function_signature.arguments = {io::substrait::Type::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kAVG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "avg");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitIntentionAggTest2) {
  const std::string function_signature_str = "avg:struct<fp64,i64>";
  const std::string return_type = "fp64";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kAVG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kAGG_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "avg");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitIntentionScalarTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "equal";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "equal");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitIntentionScalarTest2) {
  const std::string function_signature_str = "equal:i32_i32";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kEQ);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kBIN_OPER);
  ASSERT_EQ(function_descriptor.is_cider_support_function, true);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "equal");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitUnregisteredTest1) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SubstraitPlatform;
  function_signature.func_name = "between_unregisterd";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  auto function_descriptor = function_lookup_ptr->lookupFunction(function_signature);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kUNDEFINED_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, false);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between_unregisterd");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSubstraitUnregisteredTest2) {
  const std::string function_signature_str = "between_unregisterd:fp64_fp64_fp64";
  const std::string return_type = "boolean";
  auto function_descriptor =
      function_lookup_ptr->lookupFunction(function_signature_str, return_type);

  // it should match with the correct type
  ASSERT_EQ(function_descriptor.scalar_op_type, SQLOps::kUNDEFINED_OP);
  ASSERT_EQ(function_descriptor.string_op_type, SqlStringOpKind::kUNDEFINED_STRING_OP);
  ASSERT_EQ(function_descriptor.agg_op_type, SQLAgg::kUNDEFINED_AGG);
  ASSERT_EQ(function_descriptor.op_support_expr_type, OpSupportExprType::kUNDEFINED_EXPR);
  ASSERT_EQ(function_descriptor.is_cider_support_function, false);
  ASSERT_EQ(function_descriptor.func_sig.func_name, "between_unregisterd");
}

TEST_F(SubstraitFunctionLookupTest, functionLookupSparkExtentionTest) {
  FunctionSignature function_signature;
  function_signature.from_platform = PlatformType::SparkPlatform;
  function_signature.func_name = "between__3";
  function_signature.arguments = {
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();

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
  function_signature.arguments = {io::substrait::Type::decode("struct<fp64,i64>")};
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>();

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
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kI32>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();

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
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kFp64>>(),
  };
  function_signature.return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();

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
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
