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
#include "CiderOperatorTestBase.h"
#include "CiderPlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::plugin::test;

#define ROW_TPYE_LIST \
  BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE(), DATE()

class CiderDataTypeTest : public CiderOperatorTestBase {};

// Enable generateTestBatch(rowType, true) after fix the null value problems.
TEST_F(CiderDataTypeTest, basic) {
  const std::vector<TypePtr> types_{ROW_TPYE_LIST};
  for (auto type : types_) {
    RowTypePtr rowType{ROW({"c0", "c1"}, {type, type})};
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(rowType->names())
               .planNode(),
           "select * from tmp");
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(rowType->names())
               .planNode(),
           "select c0, c1 from tmp");
  }
}

TEST_F(CiderDataTypeTest, parallism) {
  const std::vector<TypePtr> types_{ROW_TPYE_LIST};
  for (auto type : types_) {
    RowTypePtr rowType{ROW({"c0", "c1"}, {type, type})};

    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(rowType->names())
               .planNode(),
           "select * from tmp",
           3);
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(rowType->names())
               .planNode(),
           "select c0, c1 from tmp",
           3);
  }
}

TEST_F(CiderDataTypeTest, mixture) {
  RowTypePtr rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"}, {ROW_TPYE_LIST})};
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, false))
             .project(rowType->names())
             .planNode(),
         "select * from tmp",
         3);
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, false))
             .project(rowType->names())
             .planNode(),
         "select c0, c1, c2, c3, c4, c5, c6, c7 from tmp");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
