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
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include "exec/nextgen/parsers/Parser.h"
#include "exec/nextgen/transformer/Transformer.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "tests/utils/Utils.h"

class NextGenParserTest : public ::testing::Test {
 public:
  void executeTest(const std::string& sql) {
    auto json = RunIsthmus::processSql(sql, create_ddl_);
    ::substrait::Plan plan;
    google::protobuf::util::JsonStringToMessage(json, &plan);

    generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
    auto eu = substrait2eu.createRelAlgExecutionUnit();

    auto pipeline = cider::exec::nextgen::parsers::toOpPipeline(eu);
    EXPECT_EQ(pipeline.size(), 3);

    cider::exec::nextgen::transformer::Transformer transformer;
    auto translators = transformer.toTranslator(pipeline);
    EXPECT_NE(translators, nullptr);

    translators = translators->getSuccessor();
    EXPECT_NE(translators, nullptr);

    translators = translators->getSuccessor();
    EXPECT_NE(translators, nullptr);

    translators = translators->getSuccessor();
    EXPECT_EQ(translators, nullptr);
  }

 private:
  std::string create_ddl_ = "CREATE TABLE test(a BIGINT NOT NULL, b BIGINT, c BIGINT);";
};

TEST_F(NextGenParserTest, ParserTest) {
  executeTest("select (a+b)*(b-c) from test where a > b or b < c");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
