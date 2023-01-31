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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "CiderOperatorTestBase.h"
#include "CiderPlanBuilder.h"

using namespace facebook::velox::plugin::test;

#define ROW_TPYE_LIST TINYINT(), SMALLINT(), INTEGER(), BIGINT()

class CiderOperatorCompareOpTest : public CiderOperatorTestBase {
 public:
  // For Compare Op Test.
  void verifyCompareOp(const std::vector<RowVectorPtr>& batches,
                       std::vector<std::string> filters,
                       int numThreads = 1) {
    if (filters.size() > 0) {
      for (auto filter : filters) {
        std::string duckDbSql = "SELECT c0 FROM tmp WHERE " + filter;
        auto ciderPlan =
            CiderPlanBuilder().values(batches).filter(filter).project({"c0"}).planNode();

        verify(ciderPlan, duckDbSql);
      }
    }
  }
};

TEST_F(CiderOperatorCompareOpTest, compareOpForTest) {
  const std::vector<TypePtr> types_{ROW_TPYE_LIST};
  for (auto type : types_) {
    RowTypePtr rowType{ROW({"c0"}, {type})};
    std::vector<std::string> filters = {"c0 between 20000 and 30000",
                                        "c0 < 13",
                                        "c0 > 13",
                                        "c0 <= 13",
                                        "c0 >= 13",
                                        "c0 = 13",
                                        "c0 <> 13"};
    verifyCompareOp(generateTestBatch(rowType, false), filters);
    // Enable this after fix the null value problems.
    GTEST_SKIP();
    verifyCompareOp(generateTestBatch(rowType, true), filters);
  }
}

TEST_F(CiderOperatorCompareOpTest, compareOpForDoubleAndRealTest) {
  const std::vector<TypePtr> types_{DOUBLE(), REAL()};
  for (auto type : types_) {
    RowTypePtr rowType{ROW({"c0"}, {type})};
    std::vector<std::string> filters = {"c0 between 1.0 and 5.0",
                                        "c0 < 13.0",
                                        "c0 > 13.0",
                                        "c0 <= 13.0",
                                        "c0 >= 13.0",
                                        "c0 = 13.0",
                                        "c0 <> 13.0"};

    verifyCompareOp(generateTestBatch(rowType, false), filters);
    verifyCompareOp(generateTestBatch(rowType, true), filters);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
