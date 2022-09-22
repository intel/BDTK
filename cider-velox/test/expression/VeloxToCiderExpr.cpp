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

#include "VeloxToCiderExpr.h"
#include <cstdint>
#include "exec/plan/parser/ConverterHelper.h"
#include "type/data/sqltypes.h"

using namespace facebook::velox::core;
using facebook::velox::TypeKind;

namespace facebook::velox::plugin {

namespace {
// convert velox sqltype to cider SQLTypeInfo(SQLTypes t, int d/p, int s,
// bool n, EncodingType c, int p, SQLTypes st)
SQLTypeInfo getCiderType(const std::shared_ptr<const velox::Type>& expr_type,
                         bool isNullable) {
  // SQLTypeInfo notnull has an opposite value against isNullable
  bool notNull = !isNullable;
  switch (expr_type->kind()) {
    case TypeKind::BOOLEAN:
      return SQLTypeInfo(SQLTypes::kBOOLEAN, notNull);
    case TypeKind::DOUBLE:
      return SQLTypeInfo(SQLTypes::kDOUBLE, notNull);
    case TypeKind::INTEGER:
      return SQLTypeInfo(SQLTypes::kINT, notNull);
    case TypeKind::BIGINT:
      return SQLTypeInfo(SQLTypes::kBIGINT, notNull);
    case TypeKind::TIMESTAMP:
      return SQLTypeInfo(SQLTypes::kTIMESTAMP, notNull);
    default:
      VELOX_UNSUPPORTED(expr_type->toString() + " is not yet supported.");
  }
}

bool isSupportedScalarFunction(std::string functionName) {
  return functionName == "gt" || functionName == "lt" || functionName == "gte" ||
         functionName == "lte" || functionName == "eq" || functionName == "neq" ||
         functionName == "and" || functionName == "or" || functionName == "multiply" ||
         functionName == "plus" || functionName == "modulus";
}

bool isSupportedAggFuncion(std::string functionName) {
  return functionName == "sum" || functionName == "avg" || functionName == "count";
}

bool isBinOp(std::shared_ptr<const CallTypedExpr> vExpr) {
  return vExpr->inputs().size() == 2;
}

bool isUOp(std::shared_ptr<const CallTypedExpr> vExpr) {
  return vExpr->inputs().size() == 1;
}

bool hasNoArg(std::shared_ptr<const CallTypedExpr> vExpr) {
  return vExpr->inputs().size() == 1;
}

}  // namespace

std::vector<std::shared_ptr<const Analyzer::Expr>>
VeloxToCiderExprConverter::toCiderExprs(
    std::vector<std::shared_ptr<const ITypedExpr>> v_exprs,
    std::unordered_map<std::string, int> col_info) const {
  std::vector<std::shared_ptr<const Analyzer::Expr>> c_exprs;
  for (auto v_expr : v_exprs) {
    c_exprs.push_back(toCiderExpr(v_expr, col_info));
  }
  return c_exprs;
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const ITypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  try {
    if (auto constant = std::dynamic_pointer_cast<const ConstantTypedExpr>(vExpr)) {
      return toCiderExpr(constant);
    }
    if (auto field_expr = std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr)) {
      return toCiderExpr(field_expr, colInfo);
    }
    if (auto call_expr = std::dynamic_pointer_cast<const CallTypedExpr>(vExpr)) {
      return toCiderExpr(call_expr, colInfo);
    }
    if (auto cast_expr = std::dynamic_pointer_cast<const CastTypedExpr>(vExpr)) {
      return toCiderExpr(cast_expr, colInfo);
    }
    return nullptr;
  } catch (VeloxException& error) {
    std::cout << error.what();
    return nullptr;
  }
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const ConstantTypedExpr> vExpr) const {
  // TOTO: update this based on RelAlgTranslator::translateLiteral
  auto type_kind = vExpr->type()->kind();
  auto cider_type = getCiderType(vExpr->type(), false);
  auto value = vExpr->value();
  // referring sqltypes.h/Datum
  Datum constant_value;
  switch (type_kind) {
    case TypeKind::BOOLEAN:
      constant_value.boolval = value.value<TypeKind::BOOLEAN>();
      break;
    case TypeKind::DOUBLE:
      constant_value.doubleval = value.value<TypeKind::DOUBLE>();
      break;
    case TypeKind::INTEGER:
      constant_value.intval = value.value<TypeKind::INTEGER>();
      break;
    case TypeKind::BIGINT:
      constant_value.bigintval = value.value<TypeKind::BIGINT>();
      break;
    default:
      VELOX_UNSUPPORTED(vExpr->type()->toString() + " is not yet supported.");
  }
  return std::make_shared<Analyzer::Constant>(cider_type, false, constant_value);
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const FieldAccessTypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  // inputs for FieldAccessTypedExpr is not useful for cider?
  auto it = colInfo.find(vExpr->name());
  if (it == colInfo.end()) {
    VELOX_USER_FAIL("can't get column index for column " + vExpr->name());
  }
  auto colIndex = it->second;
  // Can't get isNullable info from velox expr, we set it default to true
  auto colType = getCiderType(vExpr->type(), true);
  // No table id info from ConnectorTableHandle, so use faked table id 100
  return std::make_shared<Analyzer::ColumnVar>(colType, 100, colIndex, 0);
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const CastTypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  // currently only support one expr cast, see Analyzer::UOper
  auto inputs = vExpr->inputs();
  CHECK_EQ(inputs.size(), 1);
  return std::make_shared<Analyzer::UOper>(getCiderType(vExpr->type(), false),
                                           false,
                                           SQLOps::kCAST,
                                           toCiderExpr(inputs[0], colInfo));
}

std::shared_ptr<Analyzer::Expr> VeloxToCiderExprConverter::toCiderExpr(
    std::shared_ptr<const CallTypedExpr> vExpr,
    std::unordered_map<std::string, int> colInfo) const {
  // common scalar funtions with 2 arguments
  if (isSupportedScalarFunction(vExpr->name()) && isBinOp(vExpr)) {
    auto type = getCiderType(vExpr->type(), false);
    auto inputs = vExpr->inputs();
    SQLQualifier qualifier = SQLQualifier::kONE;
    auto leftExpr = toCiderExpr(inputs[0], colInfo);
    auto rightExpr = toCiderExpr(inputs[1], colInfo);
    if (leftExpr && rightExpr) {
      return std::make_shared<Analyzer::BinOper>(type,
                                                 false,
                                                 generator::getCiderSqlOps(vExpr->name()),
                                                 qualifier,
                                                 leftExpr,
                                                 rightExpr);
    }
    return nullptr;
  }
  // between needs change from between(ROW["c1"],0.6,1.6)
  // to AND(GE(ROW['c1'], 0.6), LE(ROW['c1'], 1.6))
  // note that time related type not supported now
  if (vExpr->name() == "between") {
    auto type = getCiderType(vExpr->type(), false);
    // should have 3 inputs
    auto inputs = vExpr->inputs();
    CHECK_EQ(inputs.size(), 3);
    SQLQualifier qualifier = SQLQualifier::kONE;
    return std::make_shared<Analyzer::BinOper>(
        type,
        false,
        SQLOps::kAND,
        qualifier,
        std::make_shared<Analyzer::BinOper>(type,
                                            false,
                                            SQLOps::kGE,
                                            qualifier,
                                            toCiderExpr(inputs[0], colInfo),
                                            toCiderExpr(inputs[1], colInfo)),
        std::make_shared<Analyzer::BinOper>(type,
                                            false,
                                            SQLOps::kLE,
                                            qualifier,
                                            toCiderExpr(inputs[0], colInfo),
                                            toCiderExpr(inputs[2], colInfo)));
  }
  // agg cases, refer RelAlgTranslator::translateAggregateRex
  // common agg function with one arguments
  if (isSupportedAggFuncion(vExpr->name()) && isUOp(vExpr)) {
    // get target_expr which bypass the project mask
    // note that we only support one arg agg here
    // we returned a agg_expr with Analyzer::ColumnVar as its expr and then
    // replace this mask with actual detailed expr in agg node
    if (auto agg_field =
            std::dynamic_pointer_cast<const FieldAccessTypedExpr>(vExpr->inputs()[0])) {
      SQLAgg agg_kind = generator::getCiderAggOp(vExpr->name());
      auto mask = agg_field->name();
      auto arg_expr = toCiderExpr(vExpr->inputs()[0], colInfo);
      std::shared_ptr<Analyzer::Constant> const_arg;  // 2nd aggregate parameter
      SQLTypeInfo agg_type = getCiderType(vExpr->type(), false);
      // In some cases, output type of frontend framework agg functions is inconsistent
      // with cider, such in partial avg, velox uses a rule that sum(int)->double while
      // in cider, it always uses sum(int)->bigint. So at plan side, we follow cider rule
      // to assure compile pass but when fetching result, we need convert back to double
      // to provide correct result for frontend framework./*  */
      return std::make_shared<Analyzer::AggExpr>(
          agg_type, agg_kind, arg_expr, false, const_arg);
    } else {
      VELOX_UNREACHABLE("agg should happen on specific column.");
    }
  }
  // count(*)
  if (isSupportedAggFuncion(vExpr->name()) && hasNoArg(vExpr)) {
    SQLAgg agg_kind = generator::getCiderAggOp(vExpr->name());
    std::shared_ptr<Analyzer::Constant> const_arg;  // 2nd aggregate parameter
    SQLTypeInfo agg_type = getCiderType(vExpr->type(), false);
    return std::make_shared<Analyzer::AggExpr>(
        agg_type, agg_kind, nullptr, false, const_arg);
  }
  return nullptr;
}
}  // namespace facebook::velox::plugin
