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

#ifndef CIDER_FUNCTION_SUBSTRAITFUNCTIONMAPPINGS_H
#define CIDER_FUNCTION_SUBSTRAITFUNCTIONMAPPINGS_H

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include "type/plan/Analyzer.h"
#include "util/sqldefs.h"

using FunctionSQLScalarOpsMappings = std::unordered_map<std::string, SQLOps>;
using FunctionSQLStringOpsMappings = std::unordered_map<std::string, SqlStringOpKind>;
using FunctionSQLAggOpsMappings = std::unordered_map<std::string, SQLAgg>;
using FunctionSQLOpSupportTypeMappings =
    std::unordered_map<std::string, OpSupportExprType>;

class SubstraitFunctionCiderMappings {
 public:
  const SQLOps getFunctionScalarOp(const std::string& function_name) const {
    const auto& scalar_op_map = scalarMappings();
    auto iter = scalar_op_map.find(function_name);
    if (iter == scalar_op_map.end()) {
      return SQLOps::kUNDEFINED_OP;
    }
    return iter->second;
  }

  const SqlStringOpKind getFunctionStringOp(const std::string& function_name) const {
    const auto& string_op_map = stringMappings();
    auto iter = string_op_map.find(function_name);
    if (iter == string_op_map.end()) {
      return SqlStringOpKind::kUNDEFINED_STRING_OP;
    }
    return iter->second;
  }

  const SQLAgg getFunctionAggOp(const std::string& function_name) const {
    const auto& agg_op_map = aggregateMappings();
    auto iter = agg_op_map.find(function_name);
    if (iter == agg_op_map.end()) {
      return SQLAgg::kUNDEFINED_AGG;
    }
    return iter->second;
  }

  const OpSupportExprType getFunctionOpSupportType(
      const std::string& function_name) const {
    const auto& op_support_map = opsSupportTypeMappings();
    auto iter = op_support_map.find(function_name);
    if (iter == op_support_map.end()) {
      return OpSupportExprType::kUNDEFINED_EXPR;
    }
    return iter->second;
  }

 private:
  // scalar function name and sql-ops mapping.
  virtual const FunctionSQLScalarOpsMappings& scalarMappings() const {
    static const FunctionSQLScalarOpsMappings mapping{
        {"lt", SQLOps::kLT},
        {"and", SQLOps::kAND},
        {"or", SQLOps::kOR},
        {"not", SQLOps::kNOT},
        {"gt", SQLOps::kGT},
        {"equal", SQLOps::kEQ},
        {"not_equal", SQLOps::kNE},
        {"gte", SQLOps::kGE},
        {"lte", SQLOps::kLE},
        {"multiply", SQLOps::kMULTIPLY},
        {"divide", SQLOps::kDIVIDE},
        {"add", SQLOps::kPLUS},
        {"subtract", SQLOps::kMINUS},
        {"modulus", SQLOps::kMODULO},
        {"is_not_null", SQLOps::kISNOTNULL},
        {"is_null", SQLOps::kISNULL},
        {"is_not_distinct_from", SQLOps::kBW_EQ},
        {"is_distinct_from", SQLOps::kBW_NE},
        {"cast", SQLOps::kCAST},
        {"in", SQLOps::kIN},
    };
    return mapping;
  }

  // string function name and sql-ops mapping.
  virtual const FunctionSQLStringOpsMappings& stringMappings() const {
    static const FunctionSQLStringOpsMappings mapping{
        {"lower", SqlStringOpKind::LOWER},
        {"upper", SqlStringOpKind::UPPER},
        {"initcap", SqlStringOpKind::INITCAP},
        {"reverse", SqlStringOpKind::REVERSE},
        {"repeat", SqlStringOpKind::REPEAT},
        {"||", SqlStringOpKind::CONCAT},
        {"lpad", SqlStringOpKind::LPAD},
        {"rpad", SqlStringOpKind::RPAD},
        {"trim", SqlStringOpKind::TRIM},
        {"ltrim", SqlStringOpKind::LTRIM},
        {"rtrim", SqlStringOpKind::RTRIM},
        {"substring", SqlStringOpKind::SUBSTRING},
        {"overlay", SqlStringOpKind::OVERLAY},
        {"replace", SqlStringOpKind::REPLACE},
        {"regexp_replace", SqlStringOpKind::REGEXP_REPLACE},
        {"regexp_substr", SqlStringOpKind::REGEXP_SUBSTR},
        {"regexp_match", SqlStringOpKind::REGEXP_SUBSTR},
        {"regexp_match_substring", SqlStringOpKind::REGEXP_SUBSTR},
        {"regexp_extract", SqlStringOpKind::REGEXP_EXTRACT},
        {"json_value", SqlStringOpKind::JSON_VALUE},
        {"base64_encode", SqlStringOpKind::BASE64_ENCODE},
        {"base64_decode", SqlStringOpKind::BASE64_DECODE},
        {"try_cast", SqlStringOpKind::TRY_STRING_CAST},
        {"char_length", SqlStringOpKind::CHAR_LENGTH},
        {"string_split", SqlStringOpKind::STRING_SPLIT},
        {"split_part", SqlStringOpKind::SPLIT_PART},  // split_part()
        {"split", SqlStringOpKind::STRING_SPLIT}      // split(input, delimiter, limit)
    };
    return mapping;
  }

  // aggregate function names and sql-agg mapping.
  virtual const FunctionSQLAggOpsMappings& aggregateMappings() const {
    static const FunctionSQLAggOpsMappings mapping{
        {"sum", SQLAgg::kSUM},
        {"min", SQLAgg::kMIN},
        {"max", SQLAgg::kMAX},
        {"avg", SQLAgg::kAVG},
        {"count", SQLAgg::kCOUNT},
    };
    return mapping;
  }

  // scalar and agg function and expr-type mapping.
  virtual const FunctionSQLOpSupportTypeMappings& opsSupportTypeMappings() const {
    static const FunctionSQLOpSupportTypeMappings mapping{
        {"sum", OpSupportExprType::kAGG_EXPR},
        {"min", OpSupportExprType::kAGG_EXPR},
        {"max", OpSupportExprType::kAGG_EXPR},
        {"avg", OpSupportExprType::kAGG_EXPR},
        {"count", OpSupportExprType::kAGG_EXPR},
        {"lt", OpSupportExprType::kBIN_OPER},
        {"and", OpSupportExprType::kU_OPER},
        {"or", OpSupportExprType::kU_OPER},
        {"not", OpSupportExprType::kU_OPER},
        {"gt", OpSupportExprType::kBIN_OPER},
        {"equal", OpSupportExprType::kBIN_OPER},
        {"not_equal", OpSupportExprType::kBIN_OPER},
        {"gte", OpSupportExprType::kBIN_OPER},
        {"lte", OpSupportExprType::kBIN_OPER},
        {"multiply", OpSupportExprType::kBIN_OPER},
        {"divide", OpSupportExprType::kBIN_OPER},
        {"add", OpSupportExprType::kBIN_OPER},
        {"subtract", OpSupportExprType::kBIN_OPER},
        {"modulus", OpSupportExprType::kBIN_OPER},
        {"is_not_null", OpSupportExprType::kIS_NOT_NULL},
        {"is_null", OpSupportExprType::kU_OPER},
        {"is_not_distinct_from", OpSupportExprType::kBIN_OPER},
        {"is_distinct_from", OpSupportExprType::kBIN_OPER},
        {"cast", OpSupportExprType::kU_OPER},
        {"in", OpSupportExprType::kIN_VALUES},
        {"extract", OpSupportExprType::kEXTRACT_EXPR},
        {"year", OpSupportExprType::kEXTRACT_EXPR},
        {"quarter", OpSupportExprType::kEXTRACT_EXPR},
        {"month", OpSupportExprType::kEXTRACT_EXPR},
        {"day", OpSupportExprType::kEXTRACT_EXPR},
        {"quarterday", OpSupportExprType::kEXTRACT_EXPR},
        {"hour", OpSupportExprType::kEXTRACT_EXPR},
        {"minute", OpSupportExprType::kEXTRACT_EXPR},
        {"second", OpSupportExprType::kEXTRACT_EXPR},
        {"millisecond", OpSupportExprType::kEXTRACT_EXPR},
        {"microsecond", OpSupportExprType::kEXTRACT_EXPR},
        {"nanosecond", OpSupportExprType::kEXTRACT_EXPR},
        {"day_of_week", OpSupportExprType::kEXTRACT_EXPR},
        {"isodow", OpSupportExprType::kEXTRACT_EXPR},
        {"day_of_year", OpSupportExprType::kEXTRACT_EXPR},
        {"epoch", OpSupportExprType::kEXTRACT_EXPR},
        {"week", OpSupportExprType::kEXTRACT_EXPR},
        {"week_sunday", OpSupportExprType::kEXTRACT_EXPR},
        {"week_saturday", OpSupportExprType::kEXTRACT_EXPR},
        {"dateepoch", OpSupportExprType::kEXTRACT_EXPR},
        {"coalesce", OpSupportExprType::kCASE_EXPR},
        {"substring", OpSupportExprType::kSUBSTRING_STRING_OPER},
        {"lower", OpSupportExprType::kLOWER_STRING_OPER},
        {"upper", OpSupportExprType::kUPPER_STRING_OPER},
        {"trim", OpSupportExprType::kTRIM_STRING_OPER},
        {"ltrim", OpSupportExprType::kTRIM_STRING_OPER},
        {"rtrim", OpSupportExprType::kTRIM_STRING_OPER},
        {"concat", OpSupportExprType::kCONCAT_STRING_OPER},
        {"||", OpSupportExprType::kCONCAT_STRING_OPER},
        {"char_length", OpSupportExprType::kCHAR_LENGTH_OPER},
        {"regexp_replace", OpSupportExprType::kREGEXP_REPLACE_OPER},
        {"regexp_extract", OpSupportExprType::kREGEXP_EXTRACT_OPER},
        {"regexp_match_substring", OpSupportExprType::kREGEXP_SUBSTR_OPER},
        {"like", OpSupportExprType::kLIKE_EXPR},
        {"split_part", OpSupportExprType::kSPLIT_PART_OPER},
        {"string_split", OpSupportExprType::kSTRING_SPLIT_OPER},
        {"split", OpSupportExprType::kSTRING_SPLIT_OPER}};
    return mapping;
  }
};

using SubstraitFunctionCiderMappingsPtr =
    std::shared_ptr<const SubstraitFunctionCiderMappings>;

#endif  // CIDER_FUNCTION_SUBSTRAITFUNCTIONMAPPINGS_H
