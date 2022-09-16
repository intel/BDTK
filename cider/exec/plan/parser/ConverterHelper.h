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

/**
 * @file    ConverterHelper.h
 * @brief   Provide utils methods for Substrait plan Translation
 **/

#pragma once

#include <string>
#include <unordered_map>
#include "CiderSort.h"
#include "exec/plan/parser/Translator.h"
#include "exec/template/AggregatedColRange.h"
#include "exec/template/ExpressionRewrite.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "type/data/sqltypes.h"
#include "type/plan/Analyzer.h"
#include "util/sqldefs.h"

namespace generator {
const int fake_db_id = 100;
const int fake_table_id = 100;
// dummy value which are not actually used when do context update
const int dummy_col_index = -1;
const int dummy_table_id = -1;
const int dummy_rte_idx = 1;

/**
 * Registers all supported extention functions
 */
void registerExtensionFunctions();

/**
 * generate function map according to substrait plan
 * @param plan: substrait plan
 * @return std::unordered_map: function map <function_id, function_name>
 */
std::unordered_map<int, std::string> getFunctionMap(const substrait::Plan& plan);

/**
 * function lookup
 * @param function_map: function map <function_id, function_name>
 * @param function_reference: function_id
 * @return std::string: function_name
 */
std::string getFunctionName(const std::unordered_map<int, std::string> function_map,
                            int function_reference);

/**
 * convert substrait type to cider SQLTypeInfo(SQLTypes t, int d/p, int s,
 * bool n, EncodingType c, int p, SQLTypes st)
 */
SQLTypeInfo getSQLTypeInfo(const substrait::Type& s_type);

/**
 * get SQLTypeInfo for substrait literal
 */
SQLTypeInfo getSQLTypeInfo(const substrait::Expression_Literal& s_literal_expr);

SQLTypeInfo getCiderAggType(const SQLAgg agg_kind, const Analyzer::Expr* arg_expr);

/**
 * type conversion from cider to substrait
 * used in SubstraitToRelAlgExecutionUnit.cpp
 */
substrait::Type getSubstraitType(const SQLTypeInfo& type_info);

/**
 * substrait operation conversion
 */
SQLOps getCiderSqlOps(const std::string op);
SQLAgg getCiderAggOp(const std::string op);

/**
 * Verifies whether this `function` is registered as extension function.
 * Function name and argument types are used for finding the best matching function.
 */
bool isExtensionFunction(const std::string& function,
                         std::vector<std::shared_ptr<Analyzer::Expr>> args);

/**
 * ouput columns size of rel node
 */
int getSizeOfOutputColumns(const substrait::Rel& rel_node);

/**
 * get depth of left join, which will help decide the nest level, fake_table_id, etc
 */
int getLeftJoinDepth(const substrait::Plan& plan);

/**
 * get cider join type from substrit join type
 */
JoinType getCiderJoinType(const substrait::JoinRel_JoinType& s_join_type);

/**
 * convert shared_ptr to normal ptr to align with RelAlgExecutionUnit definition
 */
Analyzer::Expr* getExpr(std::shared_ptr<Analyzer::Expr> expr,
                        bool is_partial_avg = false);

std::unordered_map<int, std::string> getFunctionMap(
    const std::vector<
        substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*> func_infos);

void parseSubstraitSortfieldSortkind2sortfiledType(
    const substrait::SortField_SortDirection& direction,
    SortDirection& sort_dir,
    NullSortedPosition& nulls_pos);

}  // namespace generator
