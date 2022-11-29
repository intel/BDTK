/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
#ifndef SQLDEFS_H
#define SQLDEFS_H

// must not change the order without keeping the array in OperExpr::to_string
// in sync.
enum SQLOps {
  kEQ = 0,
  // BitWise Equal <=> a = b or (a is null and b is null)
  // The only difference between EQ and BW_EQ is that BW_EQ will check nullability of both
  // sides while EQ won't.
  kBW_EQ = 1,
  kNE = 2,
  kLT = 3,
  kGT = 4,
  kLE = 5,
  kGE = 6,
  kAND = 7,
  kOR = 8,
  kNOT = 9,
  kMINUS = 10,
  kPLUS = 11,
  kMULTIPLY = 12,
  kDIVIDE = 13,
  kMODULO = 14,
  kUMINUS = 15,
  kISNULL = 16,
  kISNOTNULL = 17,
  kEXISTS = 18,
  kCAST = 19,
  kARRAY_AT = 20,
  kUNNEST = 21,
  kFUNCTION = 22,
  kIN = 23,
  kBW_NE = 24,
  kUNDEFINED_OP = 25,
};

#define IS_COMPARISON(X)                                                    \
  ((X) == kEQ || (X) == kBW_EQ || (X) == kNE || (X) == kLT || (X) == kGT || \
   (X) == kLE || (X) == kGE || (X) == kBW_NE)
#define IS_LOGIC(X) ((X) == kAND || (X) == kOR)
#define IS_ARITHMETIC(X) \
  ((X) == kMINUS || (X) == kPLUS || (X) == kMULTIPLY || (X) == kDIVIDE || (X) == kMODULO)
#define COMMUTE_COMPARISON(X) \
  ((X) == kLT ? kGT : (X) == kLE ? kGE : (X) == kGT ? kLT : (X) == kGE ? kLE : (X))
#define IS_UNARY(X) \
  ((X) == kNOT || (X) == kUMINUS || (X) == kISNULL || (X) == kEXISTS || (X) == kCAST)
#define IS_EQUIVALENCE(X) ((X) == kEQ || (X) == kBW_EQ)

enum SQLQualifier { kONE, kANY, kALL };

enum SQLAgg {
  kAVG = 0,
  kMIN = 1,
  kMAX = 2,
  kSUM = 3,
  kCOUNT = 4,
  kAPPROX_COUNT_DISTINCT = 5,
  kAPPROX_QUANTILE = 6,
  kSAMPLE = 7,
  kSINGLE_VALUE = 8,
  kUNDEFINED_AGG = 9,
};

enum class SqlStringOpKind {
  /* Unary */
  LOWER = 1,
  UPPER,
  INITCAP,
  REVERSE,
  CHAR_LENGTH,
  /* Binary */
  REPEAT,
  CONCAT,
  RCONCAT,
  /* Ternary */
  LPAD,
  RPAD,
  TRIM,
  LTRIM,
  RTRIM,
  SUBSTRING,
  OVERLAY,
  REPLACE,
  SPLIT_PART,
  STRING_SPLIT,
  REGEXP_EXTRACT,
  REGEXP_SUBSTR,
  REGEXP_REPLACE,
  JSON_VALUE,
  BASE64_ENCODE,
  BASE64_DECODE,
  TRY_STRING_CAST,
  INVALID,
  kUNDEFINED_STRING_OP
};

enum class SqlWindowFunctionKind {
  ROW_NUMBER,
  RANK,
  DENSE_RANK,
  PERCENT_RANK,
  CUME_DIST,
  NTILE,
  LAG,
  LEAD,
  FIRST_VALUE,
  LAST_VALUE,
  AVG,
  MIN,
  MAX,
  SUM,
  COUNT,
  SUM_INTERNAL  // For deserialization from Calcite only. Gets rewritten to a regular SUM.
};

enum SQLStmtType { kSELECT, kUPDATE, kINSERT, kDELETE, kCREATE_TABLE };

enum ViewRefreshOption { kMANUAL = 0, kAUTO = 1, kIMMEDIATE = 2 };

enum class JoinType { INNER, LEFT, SEMI, ANTI, INVALID };

#include <string>
#include <unordered_map>
#include "util/Logger.h"

inline std::string toString(const JoinType& join_type) {
  switch (join_type) {
    case JoinType::INNER:
      return "INNER";
    case JoinType::LEFT:
      return "LEFT";
    case JoinType::SEMI:
      return "SEMI";
    case JoinType::ANTI:
      return "ANTI";
    default:
      return "INVALID";
  }
}

inline std::string toString(const SQLQualifier& qualifier) {
  switch (qualifier) {
    case kONE:
      return "ONE";
    case kANY:
      return "ANY";
    case kALL:
      return "ALL";
  }
  LOG(FATAL) << "Invalid SQLQualifier: " << qualifier;
  return "";
}

inline std::string toString(const SQLAgg& kind) {
  switch (kind) {
    case kAVG:
      return "AVG";
    case kMIN:
      return "MIN";
    case kMAX:
      return "MAX";
    case kSUM:
      return "SUM";
    case kCOUNT:
      return "COUNT";
    case kAPPROX_COUNT_DISTINCT:
      return "APPROX_COUNT_DISTINCT";
    case kAPPROX_QUANTILE:
      return "APPROX_PERCENTILE";
    case kSAMPLE:
      return "SAMPLE";
    case kSINGLE_VALUE:
      return "SINGLE_VALUE";
  }
  LOG(FATAL) << "Invalid aggregate kind: " << kind;
  return "";
}

inline std::string toString(const SQLOps& op) {
  switch (op) {
    case kEQ:
      return "EQ";
    case kBW_EQ:
      return "BW_EQ";
    case kNE:
      return "NE";
    case kLT:
      return "LT";
    case kGT:
      return "GT";
    case kLE:
      return "LE";
    case kGE:
      return "GE";
    case kAND:
      return "AND";
    case kOR:
      return "OR";
    case kNOT:
      return "NOT";
    case kMINUS:
      return "MINUS";
    case kPLUS:
      return "PLUS";
    case kMULTIPLY:
      return "MULTIPLY";
    case kDIVIDE:
      return "DIVIDE";
    case kMODULO:
      return "MODULO";
    case kUMINUS:
      return "UMINUS";
    case kISNULL:
      return "ISNULL";
    case kISNOTNULL:
      return "ISNOTNULL";
    case kEXISTS:
      return "EXISTS";
    case kCAST:
      return "CAST";
    case kARRAY_AT:
      return "ARRAY_AT";
    case kUNNEST:
      return "UNNEST";
    case kFUNCTION:
      return "FUNCTION";
    case kIN:
      return "IN";
    case kBW_NE:
      return "BW_NE";
  }
  LOG(FATAL) << "Invalid operation kind: " << op;
  return "";
}

inline std::ostream& operator<<(std::ostream& os, const SqlStringOpKind kind) {
  switch (kind) {
    case SqlStringOpKind::LOWER:
      return os << "LOWER";
    case SqlStringOpKind::UPPER:
      return os << "UPPER";
    case SqlStringOpKind::INITCAP:
      return os << "INITCAP";
    case SqlStringOpKind::REVERSE:
      return os << "REVERSE";
    case SqlStringOpKind::REPEAT:
      return os << "REPEAT";
    case SqlStringOpKind::CONCAT:
      return os << "CONCAT";
    case SqlStringOpKind::RCONCAT:
      return os << "RCONCAT";
    case SqlStringOpKind::LPAD:
      return os << "LPAD";
    case SqlStringOpKind::RPAD:
      return os << "RPAD";
    case SqlStringOpKind::TRIM:
      return os << "TRIM";
    case SqlStringOpKind::LTRIM:
      return os << "LTRIM";
    case SqlStringOpKind::RTRIM:
      return os << "RTRIM";
    case SqlStringOpKind::SUBSTRING:
      return os << "SUBSTRING";
    case SqlStringOpKind::OVERLAY:
      return os << "OVERLAY";
    case SqlStringOpKind::REPLACE:
      return os << "REPLACE";
    case SqlStringOpKind::SPLIT_PART:
      return os << "SPLIT_PART";
    case SqlStringOpKind::REGEXP_REPLACE:
      return os << "REGEXP_REPLACE";
    case SqlStringOpKind::REGEXP_SUBSTR:
      return os << "REGEXP_SUBSTR";
    case SqlStringOpKind::REGEXP_EXTRACT:
      return os << "REGEXP_EXTRACT";
    case SqlStringOpKind::JSON_VALUE:
      return os << "JSON_VALUE";
    case SqlStringOpKind::BASE64_ENCODE:
      return os << "BASE64_ENCODE";
    case SqlStringOpKind::BASE64_DECODE:
      return os << "BASE64_DECODE";
    case SqlStringOpKind::TRY_STRING_CAST:
      return os << "TRY_STRING_CAST";
    case SqlStringOpKind::INVALID:
      return os << "INVALID";
    case SqlStringOpKind::kUNDEFINED_STRING_OP:
      return os << "kUNDEFINED_STRING_OP";
  }
  LOG(FATAL) << "Invalid string operation";
  // Make compiler happy
  return os << "INVALID";
}

inline SqlStringOpKind name_to_string_op_kind(const std::string& func_name) {
  const static std::unordered_map<std::string, SqlStringOpKind> name_to_string_map{
      {"LOWER", SqlStringOpKind::LOWER},
      {"UPPER", SqlStringOpKind::UPPER},
      {"INITCAP", SqlStringOpKind::INITCAP},
      {"REVERSE", SqlStringOpKind::REVERSE},
      {"REPEAT", SqlStringOpKind::REPEAT},
      {"||", SqlStringOpKind::CONCAT},
      {"CONCAT", SqlStringOpKind::CONCAT},
      {"LPAD", SqlStringOpKind::LPAD},
      {"RPAD", SqlStringOpKind::RPAD},
      {"TRIM", SqlStringOpKind::TRIM},
      {"LTRIM", SqlStringOpKind::LTRIM},
      {"RTRIM", SqlStringOpKind::RTRIM},
      {"SUBSTRING", SqlStringOpKind::SUBSTRING},
      {"OVERLAY", SqlStringOpKind::OVERLAY},
      {"REPLACE", SqlStringOpKind::REPLACE},
      // split_part is a presto extension
      {"SPLIT_PART", SqlStringOpKind::SPLIT_PART},
      // split with limit is a presto extension
      {"STRING_SPLIT", SqlStringOpKind::STRING_SPLIT},
      {"SPLIT", SqlStringOpKind::STRING_SPLIT},
      {"REGEXP_REPLACE", SqlStringOpKind::REGEXP_REPLACE},
      {"REGEXP_SUBSTR", SqlStringOpKind::REGEXP_SUBSTR},
      {"REGEXP_MATCH_SUBSTRING", SqlStringOpKind::REGEXP_SUBSTR},
      {"REGEXP_EXTRACT", SqlStringOpKind::REGEXP_EXTRACT},
      {"JSON_VALUE", SqlStringOpKind::JSON_VALUE},
      {"BASE64_ENCODE", SqlStringOpKind::BASE64_ENCODE},
      {"BASE64_DECODE", SqlStringOpKind::BASE64_DECODE},
      {"TRY_CAST", SqlStringOpKind::TRY_STRING_CAST},
      {"LENGTH", SqlStringOpKind::CHAR_LENGTH},
      {"CHAR_LENGTH", SqlStringOpKind::CHAR_LENGTH}};

  auto op_kind_entry = name_to_string_map.find(func_name);

  if (op_kind_entry == name_to_string_map.end()) {
    LOG(FATAL) << "Invalid string function " << func_name << ".";
    return SqlStringOpKind::INVALID;
  }

  return op_kind_entry->second;
}

inline std::string toString(const SqlStringOpKind& kind) {
  switch (kind) {
    case SqlStringOpKind::LOWER:
      return "LOWER";
    case SqlStringOpKind::UPPER:
      return "UPPER";
    case SqlStringOpKind::INITCAP:
      return "INITCAP";
    case SqlStringOpKind::REVERSE:
      return "REVERSE";
    case SqlStringOpKind::REPEAT:
      return "REPEAT";
    case SqlStringOpKind::CONCAT:
    case SqlStringOpKind::RCONCAT:
      return "||";
    case SqlStringOpKind::LPAD:
      return "LPAD";
    case SqlStringOpKind::RPAD:
      return "RPAD";
    case SqlStringOpKind::TRIM:
      return "TRIM";
    case SqlStringOpKind::LTRIM:
      return "LTRIM";
    case SqlStringOpKind::RTRIM:
      return "RTRIM";
    case SqlStringOpKind::SUBSTRING:
      return "SUBSTRING";
    case SqlStringOpKind::OVERLAY:
      return "OVERLAY";
    case SqlStringOpKind::REPLACE:
      return "REPLACE";
    case SqlStringOpKind::SPLIT_PART:
      return "SPLIT_PART";
    case SqlStringOpKind::REGEXP_REPLACE:
      return "REGEXP_REPLACE";
    case SqlStringOpKind::REGEXP_SUBSTR:
      return "REGEXP_SUBSTR";
    case SqlStringOpKind::REGEXP_EXTRACT:
      return "REGEXP_EXTRACT";
    case SqlStringOpKind::JSON_VALUE:
      return "JSON_VALUE";
    case SqlStringOpKind::BASE64_ENCODE:
      return "BASE64_ENCODE";
    case SqlStringOpKind::BASE64_DECODE:
      return "BASE64_DECODE";
    case SqlStringOpKind::TRY_STRING_CAST:
      return "TRY_STRING_CAST";
    default:
      LOG(FATAL) << "Invalid string operation";
  }
  return "";
}

inline std::string toString(const SqlWindowFunctionKind& kind) {
  switch (kind) {
    case SqlWindowFunctionKind::ROW_NUMBER:
      return "ROW_NUMBER";
    case SqlWindowFunctionKind::RANK:
      return "RANK";
    case SqlWindowFunctionKind::DENSE_RANK:
      return "DENSE_RANK";
    case SqlWindowFunctionKind::PERCENT_RANK:
      return "PERCENT_RANK";
    case SqlWindowFunctionKind::CUME_DIST:
      return "CUME_DIST";
    case SqlWindowFunctionKind::NTILE:
      return "NTILE";
    case SqlWindowFunctionKind::LAG:
      return "LAG";
    case SqlWindowFunctionKind::LEAD:
      return "LEAD";
    case SqlWindowFunctionKind::FIRST_VALUE:
      return "FIRST_VALUE";
    case SqlWindowFunctionKind::LAST_VALUE:
      return "LAST_VALUE";
    case SqlWindowFunctionKind::AVG:
      return "AVG";
    case SqlWindowFunctionKind::MIN:
      return "MIN";
    case SqlWindowFunctionKind::MAX:
      return "MAX";
    case SqlWindowFunctionKind::SUM:
      return "SUM";
    case SqlWindowFunctionKind::COUNT:
      return "COUNT";
    case SqlWindowFunctionKind::SUM_INTERNAL:
      return "SUM_INTERNAL";
  }
  LOG(FATAL) << "Invalid window function kind.";
  return "";
}

#endif  // SQLDEFS_H
