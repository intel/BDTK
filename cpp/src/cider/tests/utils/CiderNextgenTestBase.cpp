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

#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/CiderArrowChecker.h"

namespace cider::test::util {

void CiderNextgenTestBase::assertQuery(const std::string& sql,
                                       const std::string& json_file,
                                       const bool ignore_order) {
  auto duck_res = duckdb_query_runner_.runSql(sql);
  auto duck_res_arrow = DuckDbResultConvertor::fetchDataToArrow(duck_res);

  ArrowArray input_array;
  ArrowSchema input_schema;
  QueryArrowDataGenerator::cloneChildren(
      *input_schema_, input_schema, *input_array_, input_array);

  ArrowArray output_array;
  ArrowSchema output_schema;
  // By default, SQL statement is used to generate Substrait plan through Isthmus.
  // However, in some cases, the conversion result doesn't meet our expectations.
  // For example, for `between and` case, Isthmus will translate it into `>=` and `<=`,
  // rather than `between` function.
  // As a result, in this case, we need feed a json file, which is delivered by Velox and
  // will be used to generate Substrait plan.
  auto file_or_sql = json_file.size() ? json_file : sql;
  cider_nextgen_query_runner_->runQueryOneBatch(file_or_sql,
                                                input_array,
                                                input_schema,
                                                output_array,
                                                output_schema,
                                                codegen_options_);
  if (0 == duck_res_arrow.size()) {
    // result is empty.
    CHECK_EQ(0, output_array.length);
  } else {
    if (ignore_order) {
      EXPECT_TRUE(
          CiderArrowChecker::checkArrowEqIgnoreOrder(duck_res_arrow[0].first.get(),
                                                     &output_array,
                                                     duck_res_arrow[0].second.get(),
                                                     &output_schema));
    } else {
      EXPECT_TRUE(CiderArrowChecker::checkArrowEq(duck_res_arrow[0].first.get(),
                                                  &output_array,
                                                  duck_res_arrow[0].second.get(),
                                                  &output_schema));
    }
  }
  if (output_array.length != 0) {
    output_array.release(&output_array);
    output_schema.release(&output_schema);
  }
}

void CiderJoinNextgenTestBase::assertJoinQuery(const std::string& sql,
                                               const std::string& json_file,
                                               const bool ignore_order) {
  auto duck_res = duckdb_query_runner_.runSql(sql);
  auto duck_res_arrow = DuckDbResultConvertor::fetchDataToArrow(duck_res);

  ArrowArray input_array;
  ArrowSchema input_schema;
  QueryArrowDataGenerator::cloneChildren(
      *input_schema_, input_schema, *input_array_, input_array);

  struct ArrowArray output_array;
  struct ArrowSchema output_schema;
  // By default, SQL statement is used to generate Substrait plan through Isthmus.
  // However, in some cases, the conversion result doesn't meet our expectations.
  // For example, for `between and` case, Isthmus will translate it into `>=` and `<=`,
  // rather than `between` function.
  // As a result, in this case, we need feed a json file, which is delivered by Velox and
  // will be used to generate Substrait plan.
  auto file_or_sql = json_file.size() ? json_file : sql;
  cider_nextgen_query_runner_->runJoinQueryOneBatch(file_or_sql,
                                                    input_array,
                                                    input_schema,
                                                    *build_array_,
                                                    *build_schema_,
                                                    output_array,
                                                    output_schema,
                                                    codegen_options_);
  if (0 == duck_res_arrow.size()) {
    // result is empty.
    CHECK_EQ(0, output_array.length);
  } else {
    if (ignore_order) {
      EXPECT_TRUE(
          CiderArrowChecker::checkArrowEqIgnoreOrder(duck_res_arrow[0].first.get(),
                                                     &output_array,
                                                     duck_res_arrow[0].second.get(),
                                                     &output_schema));
    } else {
      EXPECT_TRUE(CiderArrowChecker::checkArrowEq(duck_res_arrow[0].first.get(),
                                                  &output_array,
                                                  duck_res_arrow[0].second.get(),
                                                  &output_schema));
    }
  }

  output_array.release(&output_array);
  output_schema.release(&output_schema);
}

bool CiderNextgenTestBase::executeIncorrectQuery(const std::string& wrong_sql) {
  try {
    ArrowArray input_array;
    ArrowSchema input_schema;
    QueryArrowDataGenerator::cloneChildren(
        *input_schema_, input_schema, *input_array_, input_array);

    ArrowArray output_array;
    ArrowSchema output_schema;
    cider_nextgen_query_runner_->runQueryOneBatch(wrong_sql,
                                                  input_array,
                                                  input_schema,
                                                  output_array,
                                                  output_schema,
                                                  codegen_options_);
  } catch (const CiderException& e) {
    LOG(ERROR) << e.what();
    return true;
  }
  return false;
}
void CiderNextgenTestBase::assertQuery(const std::string& sql,
                                       const ArrowArray* expect_array,
                                       const ArrowSchema* expect_schema,
                                       bool ignore_order) {
  ArrowArray input_array;
  ArrowSchema input_schema;
  QueryArrowDataGenerator::cloneChildren(
      *input_schema_, input_schema, *input_array_, input_array);

  ArrowArray output_array;
  ArrowSchema output_schema;
  auto file_or_sql = sql;
  cider_nextgen_query_runner_->runQueryOneBatch(file_or_sql,
                                                input_array,
                                                input_schema,
                                                output_array,
                                                output_schema,
                                                codegen_options_);

  if (ignore_order) {
    EXPECT_TRUE(CiderArrowChecker::checkArrowEqIgnoreOrder(
        expect_array, &output_array, expect_schema, &output_schema));
  } else {
    EXPECT_TRUE(CiderArrowChecker::checkArrowEq(
        expect_array, &output_array, expect_schema, &output_schema));
  }
  // }
}

}  // namespace cider::test::util
