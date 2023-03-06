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

using namespace facebook::velox::plugin::test;

class CiderOperatorFilterProjectOpTest : public CiderOperatorTestBase {};

TEST_F(CiderOperatorFilterProjectOpTest, asteriskProjectTest) {
  RowTypePtr rowType{ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
                         {TINYINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};
  std::string duckDbSql = " select * from tmp";

  verify(CiderPlanBuilder().values(generateTestBatch(rowType, false)).planNode(),
         duckDbSql);
  verify(CiderPlanBuilder().values(generateTestBatch(rowType, true)).planNode(),
         duckDbSql);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
