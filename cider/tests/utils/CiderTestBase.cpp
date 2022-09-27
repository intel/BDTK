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

#include "tests/utils/CiderTestBase.h"
#include "CiderBatchChecker.h"

void CiderTestBase::assertQuery(const std::string& sql,
                                const std::string& json_file,
                                const bool ignore_order) {
  auto duck_res = duckDbQueryRunner_.runSql(sql);
  auto duck_res_batch = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);

  // By default, SQL statement is used to generate Substrait plan through Isthmus.
  // However, in some cases, the conversion result doesn't meet our expectations.
  // For example, for `between and` case, Isthmus will translate it into `>=` and `<=`,
  // rather than `between` function.
  // As a result, in this case, we need feed a json file, which is delivered by Velox and
  // will be used to generate Substrait plan.
  auto cider_input = json_file.size() ? json_file : sql;
  auto cider_res_batch = std::make_shared<CiderBatch>(
      ciderQueryRunner_.runQueryOneBatch(cider_input, input_[0]));
  EXPECT_TRUE(CiderBatchChecker::checkEq(duck_res_batch, cider_res_batch, ignore_order));
}

void CiderTestBase::assertQuery(const std::string& sql,
                                const std::shared_ptr<CiderBatch> expected_batch,
                                const bool ignore_order) {
  auto cider_res_batch =
      std::make_shared<CiderBatch>(ciderQueryRunner_.runQueryOneBatch(sql, input_[0]));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, cider_res_batch, ignore_order));
}
void CiderTestBase::assertQueryForCountDistinct(
    const std::string& sql,
    const std::vector<std::shared_ptr<CiderBatch>>& expected_batches) {
  auto res_batches = ciderQueryRunner_.runQueryForCountDistinct(sql, input_);
  std::vector<std::shared_ptr<CiderBatch>> res_vec;
  for (auto it = res_batches.begin(); it < res_batches.end(); it++) {
    res_vec.emplace_back(std::make_shared<CiderBatch>(std::move(*it)));
  }
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batches, res_vec));
}

void CiderTestBase::assertQueryIgnoreOrder(const std::string& sql,
                                           const std::string& json_file) {
  auto duck_res = duckDbQueryRunner_.runSql(sql);
  auto duck_res_batch = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);

  auto cider_input = json_file.size() ? json_file : sql;
  auto cider_res_batch = std::make_shared<CiderBatch>(
      ciderQueryRunner_.runQueryOneBatch(cider_input, input_[0]));

  EXPECT_TRUE(CiderBatchChecker::checkEq(duck_res_batch, cider_res_batch, true));
}

bool CiderTestBase::executeIncorrectQuery(const std::string& wrong_sql) {
  try {
    auto cider_res_batch = ciderQueryRunner_.runQueryOneBatch(wrong_sql, input_[0]);
  } catch (const CiderException& e) {
    LOG(ERROR) << e.what();
    return true;
  }
  return false;
}

void CiderJoinTestBase::assertJoinQuery(const std::string& sql,
                                        const std::string& json_file,
                                        const bool compare_value) {
  auto duck_res = duckDbQueryRunner_.runSql(sql);
  auto duck_res_batch = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);

  auto cider_input = json_file.size() ? json_file : sql;
  auto cider_res_batch = std::make_shared<CiderBatch>(
      ciderQueryRunner_.runJoinQueryOneBatch(cider_input, *input_[0], *build_table_));

  if (compare_value) {
    EXPECT_TRUE(CiderBatchChecker::checkEq(duck_res_batch, cider_res_batch, true));
  } else {
    // only check row_num, column_num and schema when we don't need to check value since
    // duck db in some cases will return wrong results.
    EXPECT_TRUE(duck_res_batch[0]->row_num() == cider_res_batch->row_num());
    EXPECT_TRUE(duck_res_batch[0]->column_num() == cider_res_batch->column_num());
    EXPECT_TRUE(duck_res_batch[0]->schema() && cider_res_batch->schema());
  }
}

void CiderJoinTestBase::assertJoinQueryRowEqual(const std::string& sql,
                                                const std::string& json_file,
                                                const bool ignore_order) {
  auto duck_res = duckDbQueryRunner_.runSql(sql);
  auto duck_res_batches = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);

  auto cider_input = json_file.size() ? json_file : sql;
  auto cider_res_batch = std::make_shared<CiderBatch>(
      ciderQueryRunner_.runJoinQueryOneBatch(cider_input, *input_[0], *build_table_));

  EXPECT_TRUE(CiderBatchChecker::checkEq(duck_res_batches, cider_res_batch, true));
}
